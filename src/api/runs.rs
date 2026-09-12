use std::fmt;
use std::time::Duration;

use base64::Engine as _;
use reqwest::blocking::Response;
use reqwest::header::{
    CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue, IF_NONE_MATCH, LOCATION,
};
use reqwest::{Method, StatusCode, Url};
use ring::digest::{SHA256, digest};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::generated::{apis, models};
use super::http_client::{HttpClient, SignedStorageRequestError, generated_configuration};
use super::http_util::{self, BoundedBodyError};
use super::problem::{
    self, ACCEPTED_MEDIA_TYPES, BAD_REQUEST, FORBIDDEN, JSON_MEDIA_TYPE, NOT_FOUND,
    PROBLEM_MEDIA_TYPE, UNAUTHORIZED,
};
use super::{HttpTransportPolicy, UnreachableCategory, classify_reqwest_error};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const CREATE_ATTEMPTS: usize = 2;
const TEXT_MEDIA_TYPE: &str = "text/plain; charset=utf-8";

pub(crate) type Run = models::Run;
pub(crate) type RunState = models::run::State;
pub(crate) type RunCreationAcceptance = models::RunCreationAcceptance;

pub(crate) struct CreateRunInput<'a> {
    pub(crate) project_id: &'a str,
    pub(crate) workflow_path: &'a str,
    pub(crate) source_branch: Option<&'a str>,
    pub(crate) display_name: Option<&'a str>,
    pub(crate) input_set_id: Option<&'a str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NamedTextInputMetadata {
    pub(crate) name: String,
    pub(crate) size_bytes: u64,
    pub(crate) sha256: [u8; 32],
}

pub(crate) struct TextInputSet {
    id: String,
    project_id: String,
    metadata: NamedTextInputMetadata,
    manifest_sha256: [u8; 32],
    open_deadline_at: OffsetDateTime,
}

impl TextInputSet {
    pub(crate) fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Debug for TextInputSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TextInputSet([redacted])")
    }
}

struct TextUploadCapability {
    url: Url,
    content_length: String,
    content_type: String,
    checksum_sha256: String,
}

impl fmt::Debug for TextUploadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TextUploadCapability([redacted])")
    }
}

pub(crate) struct RunApi<'a> {
    configuration: apis::configuration::Configuration,
    storage_transport: &'a HttpClient,
    transport_policy: HttpTransportPolicy,
}

impl<'a> RunApi<'a> {
    pub(crate) fn new(
        api_url: &str,
        access_token: &str,
        transport_policy: HttpTransportPolicy,
        storage_transport: &'a HttpClient,
    ) -> Result<Self, RunApiError> {
        let parsed = Url::parse(api_url).map_err(|_| RunApiError::InvalidEndpoint)?;
        if !transport_policy.permits(&parsed) {
            return Err(if parsed.scheme() == "http" {
                RunApiError::InsecureHttp
            } else {
                RunApiError::InvalidEndpoint
            });
        }
        if parsed.cannot_be_a_base() || parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(RunApiError::InvalidEndpoint);
        }
        let configuration =
            generated_configuration(api_url, access_token, transport_policy, REQUEST_TIMEOUT)
                .map_err(RunApiError::BuildClient)?;
        Ok(Self {
            configuration,
            storage_transport,
            transport_policy,
        })
    }

    pub(crate) fn create(
        &self,
        organization: &str,
        idempotency_key: &str,
        input: CreateRunInput<'_>,
    ) -> Result<RunCreationAcceptance, RunFailure> {
        let mut request = models::CreateRunRequest::new(
            input.project_id.to_owned(),
            input.workflow_path.to_owned(),
        );
        request.source_branch = input.source_branch.map(str::to_owned);
        request.display_name = input.display_name.map(str::to_owned);
        request.input_set_id = input.input_set_id.map(|id| Some(id.to_owned()));
        let endpoint = self.collection_endpoint(organization);
        let response =
            self.send_api_request(StatusCode::ACCEPTED, Some(idempotency_key), || {
                self.request(Method::POST, &endpoint)
                    .header("Idempotency-Key", idempotency_key)
                    .json(&request)
            })?;
        decode_create_response(response, organization, idempotency_key)
    }

    pub(crate) fn create_text_input_set(
        &self,
        organization: &str,
        idempotency_key: &str,
        project_id: &str,
        metadata: &NamedTextInputMetadata,
    ) -> Result<TextInputSet, RunFailure> {
        let size_bytes =
            i64::try_from(metadata.size_bytes).map_err(|_| RunFailure::InvalidInput)?;
        let text_entry =
            models::RunInputManifestEntry::Text(Box::new(models::RunInputTextEntry::new(
                models::run_input_text_entry::Kind::Text,
                size_bytes,
                lowercase_hex_digest(metadata.sha256),
            )));
        let request = models::CreateRunInputSetRequest::new(
            project_id.to_owned(),
            1,
            std::collections::HashMap::from([(metadata.name.clone(), text_entry)]),
        );
        let body = serde_json::to_vec(&request).map_err(|_| RunFailure::protocol(false))?;
        let response = self.input_api_request(InputApiRequest {
            organization,
            input_set_id: None,
            operation: InputOperation::Create,
            idempotency_key: Some(idempotency_key),
            body: Some(body),
        })?;
        let set: models::RunInputSet =
            serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
        let manifest_sha256 = text_manifest_digest(metadata);
        let set = validate_text_input_set(
            set,
            project_id,
            metadata,
            manifest_sha256,
            ExpectedInputSetState::Open,
        )?;
        let expected_location = format!(
            "/v1/organizations/{}/run-input-sets/{}",
            apis::urlencode(organization),
            apis::urlencode(&set.id)
        );
        require_exact_header(response.locations.iter(), &expected_location)?;
        let open_deadline_at =
            parse_timestamp(&set.open_deadline_at).ok_or_else(|| RunFailure::protocol(false))?;
        Ok(TextInputSet {
            id: set.id,
            project_id: project_id.to_owned(),
            metadata: metadata.clone(),
            manifest_sha256,
            open_deadline_at,
        })
    }

    pub(crate) fn issue_and_upload_text(
        &self,
        organization: &str,
        input_set: &TextInputSet,
        bytes: &[u8],
    ) -> Result<(), RunFailure> {
        if u64::try_from(bytes.len()).ok() != Some(input_set.metadata.size_bytes)
            || digest_bytes(bytes) != input_set.metadata.sha256
        {
            return Err(RunFailure::InvalidInput);
        }
        let body = serde_json::to_vec(&models::RunInputUploadCapabilityRequest::new(vec![
            text_member_id(&input_set.metadata.name),
        ]))
        .map_err(|_| RunFailure::protocol(false))?;
        let response = self.input_api_request(InputApiRequest {
            organization,
            input_set_id: Some(&input_set.id),
            operation: InputOperation::IssueUpload,
            idempotency_key: None,
            body: Some(body),
        })?;
        require_private_no_store(&response)?;
        let issued: models::RunInputUploadCapabilityResponse =
            serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
        let capability = validate_text_upload_capability(issued, input_set, self.transport_policy)?;
        self.upload_text(&capability, bytes)
    }

    pub(crate) fn seal_text_input_set(
        &self,
        organization: &str,
        idempotency_key: &str,
        input_set: &TextInputSet,
    ) -> Result<(), RunFailure> {
        let response = self.input_api_request(InputApiRequest {
            organization,
            input_set_id: Some(&input_set.id),
            operation: InputOperation::Seal,
            idempotency_key: Some(idempotency_key),
            body: None,
        })?;
        let set: models::RunInputSet =
            serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
        let set = validate_text_input_set(
            set,
            &input_set.project_id,
            &input_set.metadata,
            input_set.manifest_sha256,
            ExpectedInputSetState::Sealed,
        )?;
        if set.id != input_set.id {
            return Err(RunFailure::protocol(false));
        }
        Ok(())
    }

    fn input_api_request(
        &self,
        request: InputApiRequest<'_>,
    ) -> Result<ReceivedResponse, RunFailure> {
        let endpoint = input_endpoint(
            &self.configuration.base_path,
            request.organization,
            request.input_set_id,
            request.operation,
        )?;
        let response = self.send_api_request(
            request.operation.success_status(),
            request.idempotency_key,
            || {
                let mut builder = self.request(Method::POST, &endpoint);
                if let Some(idempotency_key) = request.idempotency_key {
                    builder = builder.header("Idempotency-Key", idempotency_key);
                }
                if let Some(body) = &request.body {
                    builder = builder
                        .header(CONTENT_TYPE, JSON_MEDIA_TYPE)
                        .body(body.clone());
                }
                builder
            },
        )?;
        if response.status == request.operation.success_status() {
            require_media_type(&response, JSON_MEDIA_TYPE, false)?;
            Ok(response)
        } else {
            Err(classify_failure(&response, RunOperation::InputMutation))
        }
    }

    fn send_api_request(
        &self,
        success_status: StatusCode,
        idempotency_key: Option<&str>,
        mut build: impl FnMut() -> reqwest::blocking::RequestBuilder,
    ) -> Result<ReceivedResponse, RunFailure> {
        let attempts = if idempotency_key.is_some() {
            CREATE_ATTEMPTS
        } else {
            1
        };
        let mut last_transport_failure = UnreachableCategory::Connection;
        for attempt in 0..attempts {
            let response = match build().send() {
                Ok(response) => response,
                Err(error) => {
                    let category = classify_reqwest_error(&error);
                    last_transport_failure = category;
                    if idempotency_key.is_some() && can_retry_transport(attempt, category) {
                        crate::timing::sleep(crate::timing::short_retry_delay());
                        continue;
                    }
                    return Err(RunFailure::Unreachable(category));
                }
            };
            let status = response.status();
            if status == success_status
                && let Some(idempotency_key) = idempotency_key
            {
                require_exact_header(
                    response.headers().get_all("Idempotency-Key").iter(),
                    idempotency_key,
                )?;
            }
            match receive_response(response) {
                Ok(response) => return Ok(response),
                Err(ReceiveError::TooLarge) => {
                    return Err(RunFailure::protocol(status == StatusCode::UNAUTHORIZED));
                }
                Err(ReceiveError::Transport(error))
                    if idempotency_key.is_some() && status == success_status =>
                {
                    let category = classify_reqwest_error(&error);
                    last_transport_failure = category;
                    if can_retry_transport(attempt, category) {
                        crate::timing::sleep(crate::timing::short_retry_delay());
                        continue;
                    }
                    return Err(RunFailure::Unreachable(category));
                }
                Err(ReceiveError::Transport(_)) if status == StatusCode::UNAUTHORIZED => {
                    return Err(RunFailure::protocol(true));
                }
                Err(ReceiveError::Transport(error)) => {
                    return Err(RunFailure::Unreachable(if status.is_server_error() {
                        UnreachableCategory::Server
                    } else {
                        classify_reqwest_error(&error)
                    }));
                }
            }
        }
        Err(RunFailure::Unreachable(last_transport_failure))
    }

    fn upload_text(
        &self,
        capability: &TextUploadCapability,
        bytes: &[u8],
    ) -> Result<(), RunFailure> {
        let mut headers = HeaderMap::with_capacity(4);
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&capability.content_length)
                .map_err(|_| RunFailure::protocol(false))?,
        );
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str(&capability.content_type)
                .map_err(|_| RunFailure::protocol(false))?,
        );
        headers.insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
        headers.insert(
            "x-amz-checksum-sha256",
            HeaderValue::from_str(&capability.checksum_sha256)
                .map_err(|_| RunFailure::protocol(false))?,
        );
        let status =
            self.storage_transport
                .signed_storage_put(&capability.url, headers, bytes, REQUEST_TIMEOUT)
                .map_err(|error| match error {
                    SignedStorageRequestError::Build
                    | SignedStorageRequestError::InvalidRequest => RunFailure::protocol(false),
                    SignedStorageRequestError::Unreachable(category) => {
                        RunFailure::Unreachable(category)
                    }
                })?;
        if status == StatusCode::PRECONDITION_FAILED {
            return Ok(());
        }
        if status.is_server_error() {
            return Err(RunFailure::Unreachable(UnreachableCategory::Server));
        }
        if !matches!(
            status,
            StatusCode::OK | StatusCode::CREATED | StatusCode::NO_CONTENT
        ) {
            return Err(RunFailure::InputUploadRejected);
        }
        Ok(())
    }

    pub(crate) fn get(&self, organization: &str, run_id: &str) -> Result<Run, RunFailure> {
        let endpoint = format!(
            "{}/{}",
            self.collection_endpoint(organization),
            apis::urlencode(run_id)
        );
        let response = self
            .request(Method::GET, &endpoint)
            .send()
            .map_err(|error| RunFailure::Unreachable(classify_reqwest_error(&error)))?;
        let status = response.status();
        let response = receive_response(response).map_err(|error| match error {
            ReceiveError::TooLarge => RunFailure::protocol(status == StatusCode::UNAUTHORIZED),
            ReceiveError::Transport(_) if status == StatusCode::UNAUTHORIZED => {
                RunFailure::protocol(true)
            }
            ReceiveError::Transport(error) => {
                RunFailure::Unreachable(if status.is_server_error() {
                    UnreachableCategory::Server
                } else {
                    classify_reqwest_error(&error)
                })
            }
        })?;
        decode_get_response(response, run_id)
    }

    fn collection_endpoint(&self, organization: &str) -> String {
        format!(
            "{}/v1/organizations/{}/runs",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(organization)
        )
    }

    fn request(&self, method: Method, endpoint: &str) -> reqwest::blocking::RequestBuilder {
        let mut request = self
            .configuration
            .client
            .request(method, endpoint)
            .header(reqwest::header::ACCEPT, ACCEPTED_MEDIA_TYPES);
        if let Some(user_agent) = &self.configuration.user_agent {
            request = request.header(reqwest::header::USER_AGENT, user_agent);
        }
        if let Some(access_token) = &self.configuration.bearer_access_token {
            request = request.bearer_auth(access_token);
        }
        request
    }
}

impl Drop for RunApi<'_> {
    fn drop(&mut self) {
        super::clear_generated_access_token(&mut self.configuration);
    }
}

struct InputApiRequest<'a> {
    organization: &'a str,
    input_set_id: Option<&'a str>,
    operation: InputOperation,
    idempotency_key: Option<&'a str>,
    body: Option<Vec<u8>>,
}

#[derive(Clone, Copy)]
enum InputOperation {
    Create,
    IssueUpload,
    Seal,
}

impl InputOperation {
    const fn success_status(self) -> StatusCode {
        match self {
            Self::Create => StatusCode::CREATED,
            Self::IssueUpload | Self::Seal => StatusCode::OK,
        }
    }
}

fn input_endpoint(
    base_path: &str,
    organization: &str,
    input_set_id: Option<&str>,
    operation: InputOperation,
) -> Result<String, RunFailure> {
    let mut endpoint = format!(
        "{}/v1/organizations/{}/run-input-sets",
        base_path.trim_end_matches('/'),
        apis::urlencode(organization)
    );
    match operation {
        InputOperation::Create if input_set_id.is_none() => {}
        InputOperation::IssueUpload | InputOperation::Seal => {
            let input_set_id = input_set_id.ok_or_else(|| RunFailure::protocol(false))?;
            endpoint.push('/');
            endpoint.push_str(&apis::urlencode(input_set_id));
            endpoint.push_str(match operation {
                InputOperation::IssueUpload => "/upload-capabilities",
                InputOperation::Seal => "/seal",
                InputOperation::Create => return Err(RunFailure::protocol(false)),
            });
        }
        InputOperation::Create => return Err(RunFailure::protocol(false)),
    }
    Ok(endpoint)
}

#[derive(Clone, Copy)]
enum ExpectedInputSetState {
    Open,
    Sealed,
}

fn validate_text_input_set(
    set: models::RunInputSet,
    project_id: &str,
    metadata: &NamedTextInputMetadata,
    manifest_sha256: [u8; 32],
    expected_state: ExpectedInputSetState,
) -> Result<models::RunInputSet, RunFailure> {
    let text = set.manifest.inputs.get(&metadata.name);
    let mut members = set.members.iter();
    let text_status = members.next();
    let created_at = parse_timestamp(&set.created_at);
    let open_deadline_at = parse_timestamp(&set.open_deadline_at);
    let expected_text_sha256 = lowercase_hex_digest(metadata.sha256);
    let expected_manifest_sha256 = lowercase_hex_digest(manifest_sha256);
    let expected_member_id = text_member_id(&metadata.name);
    let immutable_valid = crate::public_id::valid_typed_id(&set.id, "ris_")
        && crate::public_id::valid_typed_id(&set.organization_id, "org_")
        && set.project_id == project_id
        && set.bounds_profile == 1
        && set.manifest.schema_version == 1
        && set.manifest.inputs.len() == 1
        && text.is_some_and(|entry| match entry {
            models::RunInputManifestEntry::Text(text) => {
                text.kind == models::run_input_text_entry::Kind::Text
                    && u64::try_from(text.size_bytes).ok() == Some(metadata.size_bytes)
                    && text.sha256 == expected_text_sha256
            }
            models::RunInputManifestEntry::Attachments(_) => false,
        })
        && set.manifest_digest.algorithm
            == models::run_input_digest::Algorithm::RunInputDigestAlgorithmSha256
        && set.manifest_digest.value == expected_manifest_sha256
        && set.input_count == 1
        && set.attachment_count == 0
        && u64::try_from(set.aggregate_size_bytes).ok() == Some(metadata.size_bytes)
        && text_status.is_some_and(|member| member.member_id == expected_member_id)
        && members.next().is_none()
        && created_at
            .zip(open_deadline_at)
            .is_some_and(|(created, deadline)| created < deadline);
    let state_valid = match expected_state {
        ExpectedInputSetState::Open => {
            set.state == models::run_input_set::State::Open
                && set.sealed_at.is_none()
                && set.sealed_deadline_at.is_none()
                && text_status.is_some_and(|member| !member.upload_confirmed)
        }
        ExpectedInputSetState::Sealed => {
            let sealed_at = set.sealed_at.as_deref().and_then(parse_timestamp);
            let sealed_deadline_at = set.sealed_deadline_at.as_deref().and_then(parse_timestamp);
            set.state == models::run_input_set::State::Sealed
                && text_status.is_some_and(|member| member.upload_confirmed)
                && sealed_at
                    .zip(sealed_deadline_at)
                    .is_some_and(|(sealed, deadline)| sealed < deadline)
        }
    };
    if immutable_valid && state_valid {
        Ok(set)
    } else {
        Err(RunFailure::protocol(false))
    }
}

fn validate_text_upload_capability(
    issued: models::RunInputUploadCapabilityResponse,
    input_set: &TextInputSet,
    transport_policy: HttpTransportPolicy,
) -> Result<TextUploadCapability, RunFailure> {
    if issued.input_set_id != input_set.id
        || parse_timestamp(&issued.capability_expires_at)
            .is_none_or(|expires_at| expires_at > input_set.open_deadline_at)
    {
        return Err(RunFailure::protocol(false));
    }
    let mut members = issued.members.into_iter();
    let member = members.next().ok_or_else(|| RunFailure::protocol(false))?;
    if members.next().is_some() || member.member_id != text_member_id(&input_set.metadata.name) {
        return Err(RunFailure::protocol(false));
    }
    let expected_content_length = input_set.metadata.size_bytes.to_string();
    let expected_checksum =
        base64::engine::general_purpose::STANDARD.encode(input_set.metadata.sha256);
    let headers = member.required_headers;
    if headers.content_length != expected_content_length
        || headers.content_type != TEXT_MEDIA_TYPE
        || headers.if_none_match != models::run_input_upload_required_headers::IfNoneMatch::Star
        || headers.x_amz_checksum_sha256 != expected_checksum
    {
        return Err(RunFailure::protocol(false));
    }
    let url = Url::parse(&member.url).map_err(|_| RunFailure::protocol(false))?;
    if !transport_policy.permits(&url)
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_none()
    {
        return Err(RunFailure::protocol(false));
    }
    Ok(TextUploadCapability {
        url,
        content_length: headers.content_length,
        content_type: headers.content_type,
        checksum_sha256: headers.x_amz_checksum_sha256,
    })
}

fn require_private_no_store(response: &ReceivedResponse) -> Result<(), RunFailure> {
    let mut values = response.cache_controls.iter();
    let valid = values
        .next()
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("private, no-store"))
        && values.next().is_none();
    if valid {
        Ok(())
    } else {
        Err(RunFailure::protocol(false))
    }
}

fn text_manifest_digest(metadata: &NamedTextInputMetadata) -> [u8; 32] {
    let canonical = format!(
        "{{\"inputs\":{{\"{}\":{{\"kind\":\"text\",\"sha256\":\"{}\",\"sizeBytes\":{}}}}},\"schemaVersion\":1}}",
        metadata.name,
        lowercase_hex_digest(metadata.sha256),
        metadata.size_bytes
    );
    digest_bytes(canonical.as_bytes())
}

fn text_member_id(name: &str) -> String {
    format!("inputs/{name}")
}

fn digest_bytes(bytes: &[u8]) -> [u8; 32] {
    let digest = digest(&SHA256, bytes);
    let mut result = [0_u8; 32];
    result.copy_from_slice(digest.as_ref());
    result
}

fn lowercase_hex_digest(digest: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn parse_timestamp(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).ok()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RunFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    NotFound,
    Conflict,
    Unreachable(UnreachableCategory),
    InputUploadRejected,
    Protocol { credential_rejected: bool },
}

impl RunFailure {
    pub(crate) fn credential_rejected(&self) -> bool {
        matches!(
            self,
            Self::Unauthenticated
                | Self::Protocol {
                    credential_rejected: true
                }
        )
    }

    pub(crate) fn retryable_observation(&self) -> bool {
        matches!(
            self,
            Self::Unreachable(
                UnreachableCategory::Connection
                    | UnreachableCategory::Timeout
                    | UnreachableCategory::Server
            )
        )
    }

    fn protocol(credential_rejected: bool) -> Self {
        Self::Protocol {
            credential_rejected,
        }
    }
}

struct ReceivedResponse {
    status: StatusCode,
    content_type: Option<HeaderValue>,
    idempotency_keys: Vec<HeaderValue>,
    locations: Vec<HeaderValue>,
    cache_controls: Vec<HeaderValue>,
    body: Vec<u8>,
}

enum ReceiveError {
    TooLarge,
    Transport(reqwest::Error),
}

fn receive_response(response: Response) -> Result<ReceivedResponse, ReceiveError> {
    let status = response.status();
    let content_type = response.headers().get(CONTENT_TYPE).cloned();
    let idempotency_keys = response
        .headers()
        .get_all("Idempotency-Key")
        .iter()
        .cloned()
        .collect();
    let locations = response
        .headers()
        .get_all(LOCATION)
        .iter()
        .cloned()
        .collect();
    let cache_controls = response
        .headers()
        .get_all(CACHE_CONTROL)
        .iter()
        .cloned()
        .collect();
    let body = http_util::read_bounded_blocking_body(response).map_err(|error| match error {
        BoundedBodyError::TooLarge => ReceiveError::TooLarge,
        BoundedBodyError::Transport(error) => ReceiveError::Transport(error),
    })?;
    Ok(ReceivedResponse {
        status,
        content_type,
        idempotency_keys,
        locations,
        cache_controls,
        body,
    })
}

fn can_retry_transport(attempt: usize, category: UnreachableCategory) -> bool {
    attempt + 1 < CREATE_ATTEMPTS
        && matches!(
            category,
            UnreachableCategory::Connection | UnreachableCategory::Timeout
        )
}

fn decode_create_response(
    response: ReceivedResponse,
    organization: &str,
    expected_idempotency_key: &str,
) -> Result<RunCreationAcceptance, RunFailure> {
    if response.status != StatusCode::ACCEPTED {
        return Err(classify_failure(&response, RunOperation::Create));
    }
    require_media_type(&response, JSON_MEDIA_TYPE, false)?;
    require_exact_header(response.idempotency_keys.iter(), expected_idempotency_key)?;
    let acceptance: RunCreationAcceptance =
        serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
    validate_acceptance(acceptance, organization, &response.locations)
}

fn decode_get_response(
    response: ReceivedResponse,
    requested_run_id: &str,
) -> Result<Run, RunFailure> {
    if response.status != StatusCode::OK {
        return Err(classify_failure(&response, RunOperation::Get));
    }
    require_media_type(&response, JSON_MEDIA_TYPE, false)?;
    let run = serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
    validate_run(run, requested_run_id)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RunOperation {
    Create,
    Get,
    InputMutation,
}

fn classify_failure(response: &ReceivedResponse, operation: RunOperation) -> RunFailure {
    match response.status {
        StatusCode::BAD_REQUEST => validated_problem_failure(
            response,
            (operation != RunOperation::InputMutation).then_some(BAD_REQUEST),
            RunFailure::InvalidInput,
            false,
        ),
        StatusCode::UNAUTHORIZED => validated_problem_failure(
            response,
            Some(UNAUTHORIZED),
            RunFailure::Unauthenticated,
            true,
        ),
        StatusCode::FORBIDDEN => {
            validated_problem_failure(response, Some(FORBIDDEN), RunFailure::Forbidden, false)
        }
        StatusCode::NOT_FOUND => {
            validated_problem_failure(response, Some(NOT_FOUND), RunFailure::NotFound, false)
        }
        StatusCode::CONFLICT | StatusCode::GONE
            if matches!(
                operation,
                RunOperation::Create | RunOperation::InputMutation
            ) =>
        {
            validated_problem_failure(response, None, RunFailure::Conflict, false)
        }
        StatusCode::PAYLOAD_TOO_LARGE | StatusCode::UNSUPPORTED_MEDIA_TYPE
            if matches!(
                operation,
                RunOperation::Create | RunOperation::InputMutation
            ) =>
        {
            validated_problem_failure(response, None, RunFailure::InvalidInput, false)
        }
        status if status.is_server_error() => RunFailure::Unreachable(UnreachableCategory::Server),
        _ => RunFailure::protocol(false),
    }
}

fn validated_problem_failure(
    response: &ReceivedResponse,
    expected_type: Option<&str>,
    failure: RunFailure,
    credential_rejected: bool,
) -> RunFailure {
    match require_problem_type(response, expected_type, credential_rejected) {
        Ok(()) => failure,
        Err(error) => error,
    }
}

fn validate_acceptance(
    acceptance: RunCreationAcceptance,
    organization: &str,
    locations: &[HeaderValue],
) -> Result<RunCreationAcceptance, RunFailure> {
    if !crate::public_id::valid_typed_id(&acceptance.run_id, "run_") {
        return Err(RunFailure::protocol(false));
    }
    let expected_location = format!(
        "/v1/organizations/{}/runs/{}",
        apis::urlencode(organization),
        apis::urlencode(&acceptance.run_id)
    );
    require_exact_header(locations.iter(), &expected_location)?;
    Ok(acceptance)
}

fn validate_run(run: Run, requested_run_id: &str) -> Result<Run, RunFailure> {
    let workflow_source = &run.workflow_definition_source;
    let workspace_source = &run.primary_workspace_source;
    let inputs = &run.inputs;
    let valid = run.id == requested_run_id
        && crate::public_id::valid_typed_id(&run.id, "run_")
        && crate::public_id::valid_typed_id(&run.organization_id, "org_")
        && crate::public_id::valid_typed_id(&run.project_id, "prj_")
        && crate::public_id::valid_typed_id(&run.execution_spec_id, "xsp_")
        && crate::public_id::valid_typed_id(&run.current_attempt_id, "atm_")
        && run.version >= 1
        && run.current_attempt_number >= 1
        && valid_bounded_string(&run.source_branch, 1, 1024)
        && run
            .display_name
            .as_deref()
            .is_none_or(|name| valid_bounded_string(name, 1, 200))
        && crate::public_id::valid_typed_id(&workflow_source.repository_connection_id, "rpc_")
        && lowercase_hex(&workflow_source.commit_oid, 40)
        && valid_canonical_workflow_path(&workflow_source.workflow_path)
        && lowercase_hex(&workflow_source.workflow_source_closure_digest.value, 64)
        && crate::public_id::valid_typed_id(&workspace_source.repository_connection_id, "rpc_")
        && lowercase_hex(&workspace_source.commit_oid, 40)
        && inputs
            .input_set_id
            .as_deref()
            .is_none_or(|id| crate::public_id::valid_typed_id(id, "ris_"))
        && (0..=256).contains(&inputs.attachment_count)
        && (0..=268_435_456).contains(&inputs.aggregate_bytes)
        && valid_timestamp(&run.created_at)
        && valid_timestamp(&run.updated_at);
    if valid {
        Ok(run)
    } else {
        Err(RunFailure::protocol(false))
    }
}

fn require_exact_header<'a>(
    mut values: impl Iterator<Item = &'a HeaderValue>,
    expected: &str,
) -> Result<(), RunFailure> {
    if values.next().and_then(|value| value.to_str().ok()) == Some(expected)
        && values.next().is_none()
    {
        Ok(())
    } else {
        Err(RunFailure::protocol(false))
    }
}

fn require_problem_type(
    response: &ReceivedResponse,
    expected_type: Option<&str>,
    credential_rejected: bool,
) -> Result<(), RunFailure> {
    require_media_type(response, PROBLEM_MEDIA_TYPE, credential_rejected)?;
    let decoded = problem::decode(&response.body, response.status)
        .map_err(|_| RunFailure::protocol(credential_rejected))?;
    if expected_type.is_none_or(|expected| decoded.r#type == expected) {
        Ok(())
    } else {
        Err(RunFailure::protocol(credential_rejected))
    }
}

fn require_media_type(
    response: &ReceivedResponse,
    expected: &str,
    credential_rejected: bool,
) -> Result<(), RunFailure> {
    let actual = response
        .content_type
        .as_ref()
        .map(http_util::media_type)
        .transpose()
        .map_err(|_| RunFailure::protocol(credential_rejected))?;
    if actual.as_deref() == Some(expected) {
        Ok(())
    } else {
        Err(RunFailure::protocol(credential_rejected))
    }
}

fn valid_bounded_string(value: &str, minimum: usize, maximum: usize) -> bool {
    let length = value.chars().count();
    (minimum..=maximum).contains(&length)
}

fn valid_canonical_workflow_path(value: &str) -> bool {
    valid_bounded_string(value, 1, 4096)
        && !value.starts_with('/')
        && value
            .split('/')
            .all(|component| !component.is_empty() && component != "." && component != "..")
}

fn lowercase_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_timestamp(value: &str) -> bool {
    OffsetDateTime::parse(value, &Rfc3339).is_ok()
}

#[derive(Debug)]
pub(crate) enum RunApiError {
    InvalidEndpoint,
    InsecureHttp,
    BuildClient(reqwest::Error),
}

impl fmt::Display for RunApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                formatter,
                "the deployment API URL cannot form a Cloud run endpoint"
            ),
            Self::InsecureHttp => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            Self::BuildClient(error) => write!(formatter, "prepare Cloud run networking: {error}"),
        }
    }
}
