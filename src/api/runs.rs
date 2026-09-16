use std::fmt;
use std::time::Duration;

use reqwest::blocking::Response;
use reqwest::header::{CACHE_CONTROL, CONTENT_TYPE, HeaderValue, LOCATION};
use reqwest::{Method, StatusCode, Url};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::generated::{apis, models};
use super::http_client::{HttpClient, generated_configuration};
use super::http_util::{self, BoundedBodyError};
use super::problem::{
    self, ACCEPTED_MEDIA_TYPES, BAD_REQUEST, FORBIDDEN, JSON_MEDIA_TYPE, NOT_FOUND,
    PROBLEM_MEDIA_TYPE, UNAUTHORIZED,
};
use super::{HttpTransportPolicy, UnreachableCategory, classify_reqwest_error};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const CREATE_ATTEMPTS: usize = 2;

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

pub(crate) struct RunApi<'a> {
    pub(super) configuration: apis::configuration::Configuration,
    pub(super) storage_transport: &'a HttpClient,
    pub(super) transport_policy: HttpTransportPolicy,
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
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<RunCreationAcceptance, RunFailure> {
        let mut request = models::CreateRunRequest::new(
            input.project_id.to_owned(),
            input.workflow_path.to_owned(),
        );
        request.source_branch = input.source_branch.map(str::to_owned);
        request.display_name = input.display_name.map(str::to_owned);
        request.input_set_id = input.input_set_id.map(|id| Some(id.to_owned()));
        let endpoint = self.collection_endpoint(organization);
        let response = self.send_api_request(
            StatusCode::ACCEPTED,
            Some(idempotency_key),
            begin_dispatch,
            || {
                self.request(Method::POST, &endpoint)
                    .header("Idempotency-Key", idempotency_key)
                    .json(&request)
            },
        )?;
        decode_create_response(response, organization, idempotency_key)
    }

    pub(super) fn send_api_request(
        &self,
        success_status: StatusCode,
        idempotency_key: Option<&str>,
        begin_dispatch: impl Fn() -> bool,
        mut build: impl FnMut() -> reqwest::blocking::RequestBuilder,
    ) -> Result<ReceivedResponse, RunFailure> {
        let attempts = if idempotency_key.is_some() {
            CREATE_ATTEMPTS
        } else {
            1
        };
        let mut last_transport_failure = UnreachableCategory::Connection;
        for attempt in 0..attempts {
            if !begin_dispatch() {
                return Err(RunFailure::Interrupted);
            }
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

    pub(super) fn request(
        &self,
        method: Method,
        endpoint: &str,
    ) -> reqwest::blocking::RequestBuilder {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RunFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    NotFound,
    Conflict,
    Gone,
    Unreachable(UnreachableCategory),
    InputUploadRejected,
    InputDownloadRejected,
    Interrupted,
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

    pub(super) fn protocol(credential_rejected: bool) -> Self {
        Self::Protocol {
            credential_rejected,
        }
    }
}

pub(super) struct ReceivedResponse {
    pub(super) status: StatusCode,
    pub(super) content_type: Option<HeaderValue>,
    pub(super) idempotency_keys: Vec<HeaderValue>,
    pub(super) locations: Vec<HeaderValue>,
    pub(super) cache_controls: Vec<HeaderValue>,
    pub(super) body: Vec<u8>,
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
pub(super) enum RunOperation {
    Create,
    Get,
    Input,
}

pub(super) fn classify_failure(response: &ReceivedResponse, operation: RunOperation) -> RunFailure {
    match response.status {
        StatusCode::BAD_REQUEST => validated_problem_failure(
            response,
            (operation != RunOperation::Input).then_some(BAD_REQUEST),
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
        StatusCode::CONFLICT if matches!(operation, RunOperation::Create | RunOperation::Input) => {
            validated_problem_failure(response, None, RunFailure::Conflict, false)
        }
        StatusCode::GONE if matches!(operation, RunOperation::Create | RunOperation::Input) => {
            validated_problem_failure(response, None, RunFailure::Gone, false)
        }
        StatusCode::PAYLOAD_TOO_LARGE | StatusCode::UNSUPPORTED_MEDIA_TYPE
            if matches!(operation, RunOperation::Create | RunOperation::Input) =>
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

pub(super) fn require_exact_header<'a>(
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

pub(super) fn require_media_type(
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
