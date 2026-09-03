use std::fmt;
use std::io::{Read, Write};
use std::time::Duration;

use reqwest::blocking::{Client, Response};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use reqwest::{Method, StatusCode, Url};
use ring::digest::{Context as DigestContext, SHA256};
use serde::{Deserialize, Serialize};

use super::bearer_authorization;
use super::http_client::{HttpEndpointError, HttpTransportPolicy, categorized_dns_resolver};
use super::{UnreachableCategory, classify_reqwest_error, http_util};

const API_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const DOWNLOAD_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const JSON_MEDIA_TYPE: &str = "application/json";
const PROBLEM_MEDIA_TYPE: &str = "application/problem+json";
const ACCEPTED_MEDIA_TYPES: &str = "application/json, application/problem+json";
const MAXIMUM_BODY_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactMember {
    pub(crate) path: String,
    pub(crate) media_type: String,
    pub(crate) size_bytes: u64,
    pub(crate) sha256: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactInventoryPage {
    pub(crate) artifact_set_id: String,
    pub(crate) sealed_at: String,
    pub(crate) expires_at: String,
    pub(crate) member_count: usize,
    pub(crate) total_size_bytes: u64,
    pub(crate) members: Vec<ArtifactMember>,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ArtifactCapabilityMember {
    pub(crate) member: ArtifactMember,
    pub(crate) url: String,
}

impl fmt::Debug for ArtifactCapabilityMember {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ArtifactCapabilityMember([redacted])")
    }
}

impl fmt::Display for ArtifactCapabilityMember {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("artifact download capability [redacted]")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactCapabilities {
    pub(crate) artifact_set_id: String,
    pub(crate) expires_at: String,
    pub(crate) capability_expires_at: String,
    pub(crate) members: Vec<ArtifactCapabilityMember>,
}

pub(crate) trait ArtifactSource {
    fn inventory_page(
        &mut self,
        organization: &str,
        run_id: &str,
        limit: u16,
        cursor: Option<&str>,
    ) -> Result<ArtifactInventoryPage, ArtifactApiError>;

    fn issue_capabilities(
        &mut self,
        organization: &str,
        run_id: &str,
        paths: &[String],
    ) -> Result<ArtifactCapabilities, ArtifactApiError>;

    fn download(
        &mut self,
        capability: &ArtifactCapabilityMember,
        destination: &mut dyn Write,
    ) -> Result<DownloadedMember, ArtifactApiError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DownloadedMember {
    pub(crate) size_bytes: u64,
    pub(crate) sha256: [u8; 32],
}

pub(crate) struct ArtifactApi {
    api_url: String,
    authorization: HeaderValue,
    api_client: Client,
    download_client: Client,
    transport_policy: HttpTransportPolicy,
}

impl ArtifactApi {
    pub(crate) fn new(
        api_url: &str,
        access_token: &str,
        transport_policy: HttpTransportPolicy,
    ) -> Result<Self, ArtifactApiError> {
        crate::tls::install_provider();
        let base = Url::parse(api_url)
            .map_err(|_| ArtifactApiError::Endpoint(HttpEndpointError::Invalid))?;
        if !transport_policy.permits(&base) {
            return Err(ArtifactApiError::Endpoint(HttpEndpointError::InsecureHttp));
        }
        let authorization = bearer_authorization(access_token)
            .map_err(|_| ArtifactApiError::InvalidAuthorizationHeader)?;
        let api_client = build_http_client(
            transport_policy,
            API_REQUEST_TIMEOUT,
            Some(API_REQUEST_TIMEOUT),
        )?;
        let download_client = build_http_client(transport_policy, DOWNLOAD_CONNECT_TIMEOUT, None)?;
        Ok(Self {
            api_url: api_url.to_owned(),
            authorization,
            api_client,
            download_client,
            transport_policy,
        })
    }

    fn endpoint(&self, path: &[&str]) -> Result<Url, ArtifactApiError> {
        http_util::endpoint(&self.api_url, path)
            .map_err(|()| ArtifactApiError::Endpoint(HttpEndpointError::Invalid))
    }

    fn send_api(
        &self,
        method: Method,
        endpoint: Url,
        body: Option<Vec<u8>>,
    ) -> Result<Response, ArtifactApiError> {
        let mut request = self
            .api_client
            .request(method, endpoint)
            .header(ACCEPT, ACCEPTED_MEDIA_TYPES)
            .header(AUTHORIZATION, self.authorization.clone());
        if let Some(body) = body {
            request = request.header(CONTENT_TYPE, JSON_MEDIA_TYPE).body(body);
        }
        request.send().map_err(ArtifactApiError::transport)
    }
}

fn build_http_client(
    transport_policy: HttpTransportPolicy,
    connect_timeout: Duration,
    request_timeout: Option<Duration>,
) -> Result<Client, ArtifactApiError> {
    Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .dns_resolver(categorized_dns_resolver())
        .https_only(transport_policy == HttpTransportPolicy::HttpsOnly)
        .build()
        .map_err(|_| ArtifactApiError::Protocol {
            reason: "the HTTP client could not be created",
            credential_rejected: false,
        })
}

impl ArtifactSource for ArtifactApi {
    fn inventory_page(
        &mut self,
        organization: &str,
        run_id: &str,
        limit: u16,
        cursor: Option<&str>,
    ) -> Result<ArtifactInventoryPage, ArtifactApiError> {
        let mut endpoint = self.endpoint(&[
            "v1",
            "organizations",
            organization,
            "runs",
            run_id,
            "artifact-set",
        ])?;
        {
            let mut query = endpoint.query_pairs_mut();
            query.append_pair("limit", &limit.to_string());
            if let Some(cursor) = cursor {
                query.append_pair("cursor", cursor);
            }
        }
        let response = self.send_api(Method::GET, endpoint, None)?;
        let response = receive_api_response(response)?;
        match response.status {
            StatusCode::OK => {
                require_success_headers(&response)?;
                let wire: WireInventory = serde_json::from_slice(&response.body).map_err(|_| {
                    ArtifactApiError::protocol("the inventory body is invalid", false)
                })?;
                ArtifactInventoryPage::try_from(wire)
            }
            status => Err(classify_api_failure(status, &response)),
        }
    }

    fn issue_capabilities(
        &mut self,
        organization: &str,
        run_id: &str,
        paths: &[String],
    ) -> Result<ArtifactCapabilities, ArtifactApiError> {
        let endpoint = self.endpoint(&[
            "v1",
            "organizations",
            organization,
            "runs",
            run_id,
            "artifact-set",
            "download-capabilities",
        ])?;
        let body = serde_json::to_vec(&CapabilityRequest { paths }).map_err(|_| {
            ArtifactApiError::protocol("the capability request could not be serialized", false)
        })?;
        let response = self.send_api(Method::POST, endpoint, Some(body))?;
        let response = receive_api_response(response)?;
        match response.status {
            StatusCode::OK => {
                require_success_headers(&response)?;
                let wire: WireCapabilities =
                    serde_json::from_slice(&response.body).map_err(|_| {
                        ArtifactApiError::protocol("the capability body is invalid", false)
                    })?;
                ArtifactCapabilities::try_from(wire)
            }
            status => Err(classify_api_failure(status, &response)),
        }
    }

    fn download(
        &mut self,
        capability: &ArtifactCapabilityMember,
        destination: &mut dyn Write,
    ) -> Result<DownloadedMember, ArtifactApiError> {
        let url =
            Url::parse(&capability.url).map_err(|_| ArtifactApiError::CapabilityUnavailable)?;
        if !self.transport_policy.permits(&url)
            || url.username() != ""
            || url.password().is_some()
            || url.fragment().is_some()
            || url.query().is_none()
        {
            return Err(ArtifactApiError::CapabilityUnavailable);
        }
        let mut response = self
            .download_client
            .get(url)
            .header(ACCEPT, capability.member.media_type.as_str())
            .send()
            .map_err(|_| ArtifactApiError::CapabilityUnavailable)?;
        if response.status() != StatusCode::OK
            || response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                != Some(capability.member.media_type.as_str())
            || response
                .content_length()
                .is_some_and(|length| length != capability.member.size_bytes)
        {
            return Err(ArtifactApiError::CapabilityUnavailable);
        }
        let mut digest = DigestContext::new(&SHA256);
        let mut size_bytes = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = response
                .read(&mut buffer)
                .map_err(|_| ArtifactApiError::CapabilityUnavailable)?;
            if read == 0 {
                break;
            }
            size_bytes = size_bytes
                .checked_add(
                    u64::try_from(read).map_err(|_| ArtifactApiError::CapabilityUnavailable)?,
                )
                .ok_or(ArtifactApiError::CapabilityUnavailable)?;
            if size_bytes > capability.member.size_bytes {
                return Err(ArtifactApiError::CapabilityUnavailable);
            }
            digest.update(&buffer[..read]);
            destination
                .write_all(&buffer[..read])
                .map_err(|_| ArtifactApiError::LocalOutput)?;
        }
        let observed = digest.finish();
        let mut sha256 = [0_u8; 32];
        sha256.copy_from_slice(observed.as_ref());
        Ok(DownloadedMember { size_bytes, sha256 })
    }
}

#[derive(Debug)]
pub(crate) enum ArtifactApiError {
    Endpoint(HttpEndpointError),
    InvalidAuthorizationHeader,
    InvalidInput,
    Unauthenticated,
    Forbidden,
    NotFound,
    Gone,
    Unreachable(UnreachableCategory),
    CapabilityUnavailable,
    LocalOutput,
    Protocol {
        reason: &'static str,
        credential_rejected: bool,
    },
}

impl ArtifactApiError {
    pub(crate) fn credential_rejected(&self) -> bool {
        matches!(self, Self::Unauthenticated)
            || matches!(
                self,
                Self::Protocol {
                    credential_rejected: true,
                    ..
                }
            )
    }

    fn protocol(reason: &'static str, credential_rejected: bool) -> Self {
        Self::Protocol {
            reason,
            credential_rejected,
        }
    }

    fn transport(error: reqwest::Error) -> Self {
        if error.is_builder() {
            Self::protocol("the API request could not be constructed", false)
        } else {
            Self::Unreachable(classify_reqwest_error(&error))
        }
    }
}

impl fmt::Display for ArtifactApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Endpoint(HttpEndpointError::Invalid) => formatter.write_str("the deployment API URL cannot form an Artifact Set endpoint"),
            Self::Endpoint(HttpEndpointError::InsecureHttp) => formatter.write_str("the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"),
            Self::InvalidAuthorizationHeader => formatter.write_str("the stored access token cannot be represented as a bearer credential"),
            Self::InvalidInput => formatter.write_str("the Artifact Set request is invalid"),
            Self::Unauthenticated => formatter.write_str("Artifact Set download requires sign-in"),
            Self::Forbidden => formatter.write_str("Artifact Set download is not permitted for this account"),
            Self::NotFound => formatter.write_str("the run or Artifact Set was not found"),
            Self::Gone => formatter.write_str("the Artifact Set has expired"),
            Self::Unreachable(category) => write!(formatter, "the Artifact Set API is unreachable: {}", category.as_str()),
            Self::CapabilityUnavailable => formatter.write_str("an exact artifact download is unavailable"),
            Self::LocalOutput => formatter.write_str("write private artifact staging"),
            Self::Protocol { reason, .. } => write!(formatter, "the Artifact Set API response violates the public contract: {reason}"),
        }
    }
}

struct ReceivedResponse {
    status: StatusCode,
    content_type: Option<String>,
    cache_control: Option<String>,
    body: Vec<u8>,
}

fn receive_api_response(response: Response) -> Result<ReceivedResponse, ArtifactApiError> {
    let status = response.status();
    let credential_rejected = status == StatusCode::UNAUTHORIZED;
    if response
        .content_length()
        .is_some_and(|length| length > MAXIMUM_BODY_BYTES)
    {
        return Err(ArtifactApiError::protocol(
            "the response body exceeds 1 MiB",
            credential_rejected,
        ));
    }
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let cache_control = response
        .headers()
        .get("Cache-Control")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut body = Vec::new();
    response
        .take(MAXIMUM_BODY_BYTES + 1)
        .read_to_end(&mut body)
        .map_err(|_| ArtifactApiError::Unreachable(UnreachableCategory::Connection))?;
    if u64::try_from(body.len()).map_or(true, |length| length > MAXIMUM_BODY_BYTES) {
        return Err(ArtifactApiError::protocol(
            "the response body exceeds 1 MiB",
            credential_rejected,
        ));
    }
    Ok(ReceivedResponse {
        status,
        content_type,
        cache_control,
        body,
    })
}

fn require_success_headers(response: &ReceivedResponse) -> Result<(), ArtifactApiError> {
    if response.content_type.as_deref() != Some(JSON_MEDIA_TYPE) {
        return Err(ArtifactApiError::protocol(
            "the success Content-Type is invalid",
            false,
        ));
    }
    let no_store = response.cache_control.as_deref().is_some_and(|value| {
        value
            .split(',')
            .any(|directive| directive.trim().eq_ignore_ascii_case("no-store"))
    });
    if !no_store {
        return Err(ArtifactApiError::protocol(
            "the success response is cacheable",
            false,
        ));
    }
    Ok(())
}

fn classify_api_failure(status: StatusCode, response: &ReceivedResponse) -> ArtifactApiError {
    let credential_rejected = status == StatusCode::UNAUTHORIZED;
    if response.content_type.as_deref() != Some(PROBLEM_MEDIA_TYPE)
        || serde_json::from_slice::<serde_json::Value>(&response.body).is_err()
    {
        return ArtifactApiError::protocol(
            "the error response is not valid problem details",
            credential_rejected,
        );
    }
    match status {
        StatusCode::BAD_REQUEST
        | StatusCode::PAYLOAD_TOO_LARGE
        | StatusCode::UNSUPPORTED_MEDIA_TYPE => ArtifactApiError::InvalidInput,
        StatusCode::UNAUTHORIZED => ArtifactApiError::Unauthenticated,
        StatusCode::FORBIDDEN => ArtifactApiError::Forbidden,
        StatusCode::NOT_FOUND => ArtifactApiError::NotFound,
        StatusCode::GONE => ArtifactApiError::Gone,
        status if status.is_server_error() => {
            ArtifactApiError::Unreachable(UnreachableCategory::Server)
        }
        _ => ArtifactApiError::protocol(
            "the HTTP status is invalid for the operation",
            credential_rejected,
        ),
    }
}

#[derive(Serialize)]
struct CapabilityRequest<'a> {
    paths: &'a [String],
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireDigest {
    algorithm: String,
    value: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireMember {
    path: String,
    media_type: String,
    size_bytes: i64,
    digest: WireDigest,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireInventory {
    artifact_set_id: String,
    sealed_at: String,
    expires_at: String,
    member_count: usize,
    total_size_bytes: i64,
    members: Vec<WireMember>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireCapabilityMember {
    path: String,
    media_type: String,
    size_bytes: i64,
    digest: WireDigest,
    url: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireCapabilities {
    artifact_set_id: String,
    expires_at: String,
    capability_expires_at: String,
    members: Vec<WireCapabilityMember>,
}

impl TryFrom<WireMember> for ArtifactMember {
    type Error = ArtifactApiError;

    fn try_from(member: WireMember) -> Result<Self, Self::Error> {
        Ok(Self {
            path: member.path,
            media_type: member.media_type,
            size_bytes: u64::try_from(member.size_bytes)
                .map_err(|_| ArtifactApiError::protocol("a member size is invalid", false))?,
            sha256: decode_digest(member.digest)?,
        })
    }
}

impl TryFrom<WireInventory> for ArtifactInventoryPage {
    type Error = ArtifactApiError;

    fn try_from(page: WireInventory) -> Result<Self, Self::Error> {
        if page.member_count == 0
            || page.member_count > 4097
            || page.members.len() > 200
            || page.next_cursor.as_ref().is_some_and(String::is_empty)
        {
            return Err(ArtifactApiError::protocol(
                "the inventory bounds are invalid",
                false,
            ));
        }
        let sealed_at = parse_timestamp(&page.sealed_at)?;
        let expires_at = parse_timestamp(&page.expires_at)?;
        if expires_at <= sealed_at {
            return Err(ArtifactApiError::protocol(
                "the Artifact Set retention timestamps are invalid",
                false,
            ));
        }
        Ok(Self {
            artifact_set_id: page.artifact_set_id,
            sealed_at: page.sealed_at,
            expires_at: page.expires_at,
            member_count: page.member_count,
            total_size_bytes: u64::try_from(page.total_size_bytes).map_err(|_| {
                ArtifactApiError::protocol("the inventory byte total is invalid", false)
            })?,
            members: page
                .members
                .into_iter()
                .map(ArtifactMember::try_from)
                .collect::<Result<_, _>>()?,
            next_cursor: page.next_cursor,
        })
    }
}

impl TryFrom<WireCapabilities> for ArtifactCapabilities {
    type Error = ArtifactApiError;

    fn try_from(response: WireCapabilities) -> Result<Self, Self::Error> {
        if response.members.is_empty() || response.members.len() > 100 {
            return Err(ArtifactApiError::protocol(
                "the capability member count is invalid",
                false,
            ));
        }
        let expires_at = parse_timestamp(&response.expires_at)?;
        let capability_expires_at = parse_timestamp(&response.capability_expires_at)?;
        if capability_expires_at > expires_at {
            return Err(ArtifactApiError::protocol(
                "the capability exceeds Artifact Set retention",
                false,
            ));
        }
        let members = response
            .members
            .into_iter()
            .map(|member| {
                let artifact = ArtifactMember::try_from(WireMember {
                    path: member.path,
                    media_type: member.media_type,
                    size_bytes: member.size_bytes,
                    digest: member.digest,
                })?;
                Ok(ArtifactCapabilityMember {
                    member: artifact,
                    url: member.url,
                })
            })
            .collect::<Result<_, ArtifactApiError>>()?;
        Ok(Self {
            artifact_set_id: response.artifact_set_id,
            expires_at: response.expires_at,
            capability_expires_at: response.capability_expires_at,
            members,
        })
    }
}

fn decode_digest(digest: WireDigest) -> Result<[u8; 32], ArtifactApiError> {
    if digest.algorithm != "sha256" || digest.value.len() != 64 {
        return Err(ArtifactApiError::protocol(
            "a member digest is invalid",
            false,
        ));
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in digest.value.as_bytes().chunks_exact(2).enumerate() {
        let value = std::str::from_utf8(pair)
            .ok()
            .and_then(|value| u8::from_str_radix(value, 16).ok())
            .ok_or_else(|| ArtifactApiError::protocol("a member digest is invalid", false))?;
        decoded[index] = value;
    }
    Ok(decoded)
}

fn parse_timestamp(value: &str) -> Result<time::OffsetDateTime, ArtifactApiError> {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|_| ArtifactApiError::protocol("an Artifact Set timestamp is invalid", false))
}
