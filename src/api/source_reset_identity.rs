use std::fmt;
use std::time::Duration;

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use reqwest::{StatusCode, Url};
use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};

use super::http_client::{HttpClient, HttpEndpointError};
use super::http_util::{self, BoundedBodyError};
use super::{UnreachableCategory, classify_reqwest_error};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const JSON_MEDIA_TYPE: &str = "application/json";
const BOUNDARY_ID: &str = "cloud_git_source_v1_full_reset";
const EVIDENCE_DOMAIN: &[u8] = b"scherzo.cloud/source-reset-bootstrap-identity-evidence/v1\0";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SourceResetIdentityEvidence {
    pub(crate) schema_version: u8,
    pub(crate) boundary_id: String,
    pub(crate) identity_digest_encoding: String,
    pub(crate) human_identity_sha256: String,
    pub(crate) principal_id: String,
    pub(crate) issuer_configuration_sha256: String,
    pub(crate) preparation_manifest_sha256: String,
    pub(crate) observed_at: String,
    pub(crate) evidence_sha256: String,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum SourceResetIdentityOutcome {
    Evidence(SourceResetIdentityEvidence),
    Unauthenticated,
    Unavailable(UnreachableCategory),
}

#[derive(Debug)]
pub(crate) struct SourceResetIdentityError {
    kind: SourceResetIdentityErrorKind,
    credential_rejected: bool,
}

impl SourceResetIdentityError {
    pub(crate) fn credential_rejected(&self) -> bool {
        self.credential_rejected
    }

    fn local(kind: SourceResetIdentityErrorKind) -> Self {
        Self {
            kind,
            credential_rejected: false,
        }
    }

    fn protocol(reason: &'static str, credential_rejected: bool) -> Self {
        Self {
            kind: SourceResetIdentityErrorKind::Protocol(reason),
            credential_rejected,
        }
    }
}

#[derive(Debug)]
enum SourceResetIdentityErrorKind {
    Endpoint(HttpEndpointError),
    InvalidAuthorizationHeader,
    Protocol(&'static str),
}

impl fmt::Display for SourceResetIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            SourceResetIdentityErrorKind::Endpoint(HttpEndpointError::Invalid) => {
                write!(
                    formatter,
                    "the deployment API URL cannot form the source-reset identity endpoint"
                )
            }
            SourceResetIdentityErrorKind::Endpoint(HttpEndpointError::InsecureHttp) => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            SourceResetIdentityErrorKind::InvalidAuthorizationHeader => write!(
                formatter,
                "the stored access token cannot be represented as a bearer credential"
            ),
            SourceResetIdentityErrorKind::Protocol(reason) => write!(
                formatter,
                "source-reset identity response violates its closed contract: {reason}"
            ),
        }
    }
}

pub(crate) fn derive_source_reset_identity(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
) -> Result<SourceResetIdentityOutcome, SourceResetIdentityError> {
    let endpoint = client
        .endpoint(
            api_url,
            &["v1", "source-reset", "bootstrap-identity-digest"],
        )
        .map_err(|error| {
            SourceResetIdentityError::local(SourceResetIdentityErrorKind::Endpoint(error))
        })?;
    let authorization = HeaderValue::from_str(&format!("Bearer {access_token}")).map_err(|_| {
        SourceResetIdentityError::local(SourceResetIdentityErrorKind::InvalidAuthorizationHeader)
    })?;
    match client.run(
        REQUEST_TIMEOUT,
        execute_request(client, endpoint, authorization),
    ) {
        Ok(result) => result,
        Err(_) => Ok(SourceResetIdentityOutcome::Unavailable(
            UnreachableCategory::Timeout,
        )),
    }
}

async fn execute_request(
    client: &HttpClient,
    endpoint: Url,
    authorization: HeaderValue,
) -> Result<SourceResetIdentityOutcome, SourceResetIdentityError> {
    let response = match client
        .inner()
        .post(endpoint)
        .timeout(REQUEST_TIMEOUT)
        .header(ACCEPT, JSON_MEDIA_TYPE)
        .header(AUTHORIZATION, authorization)
        .json(&serde_json::json!({"schemaVersion": 1, "boundaryId": BOUNDARY_ID}))
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) if error.is_builder() => {
            return Err(SourceResetIdentityError::protocol(
                "the authenticated request could not be constructed",
                false,
            ));
        }
        Err(error) => {
            return Ok(SourceResetIdentityOutcome::Unavailable(
                classify_reqwest_error(&error),
            ));
        }
    };
    // This private evidence endpoint has a deliberately separate closed error
    // type from public current-principal lookup; sharing it would couple two
    // independent credential-rejection contracts.
    // jscpd:ignore-start
    let status = response.status();
    let credential_rejected = status == StatusCode::UNAUTHORIZED;
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .map(http_util::media_type)
        .transpose()
        .map_err(|()| {
            SourceResetIdentityError::protocol("Content-Type is invalid", credential_rejected)
        })?;
    // jscpd:ignore-end
    let body = match http_util::read_bounded_body(response).await {
        Ok(body) => body,
        Err(BoundedBodyError::TooLarge) => {
            return Err(SourceResetIdentityError::protocol(
                "the response exceeds 1 MiB",
                credential_rejected,
            ));
        }
        Err(BoundedBodyError::Transport(error)) => {
            return Ok(SourceResetIdentityOutcome::Unavailable(
                classify_reqwest_error(&error),
            ));
        }
    };
    match status {
        StatusCode::OK => {
            if content_type.as_deref() != Some(JSON_MEDIA_TYPE) {
                return Err(SourceResetIdentityError::protocol(
                    "a successful response is not JSON",
                    false,
                ));
            }
            let evidence: SourceResetIdentityEvidence =
                serde_json::from_slice(&body).map_err(|_| {
                    SourceResetIdentityError::protocol("the evidence fields are invalid", false)
                })?;
            if !valid_evidence(&evidence) {
                return Err(SourceResetIdentityError::protocol(
                    "the evidence identity or digest is invalid",
                    false,
                ));
            }
            Ok(SourceResetIdentityOutcome::Evidence(evidence))
        }
        StatusCode::UNAUTHORIZED => Ok(SourceResetIdentityOutcome::Unauthenticated),
        StatusCode::TOO_MANY_REQUESTS => Ok(SourceResetIdentityOutcome::Unavailable(
            UnreachableCategory::RateLimited,
        )),
        status if status.is_server_error() => Ok(SourceResetIdentityOutcome::Unavailable(
            UnreachableCategory::Server,
        )),
        status if status.is_redirection() => Err(SourceResetIdentityError::protocol(
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(SourceResetIdentityError::protocol(
            "the HTTP status is invalid",
            credential_rejected,
        )),
    }
}

fn valid_evidence(evidence: &SourceResetIdentityEvidence) -> bool {
    if evidence.schema_version != 1
        || evidence.boundary_id != BOUNDARY_ID
        || evidence.identity_digest_encoding != "source_reset_human_oidc_v1"
        || !lower_hex(&evidence.human_identity_sha256, 64)
        || !lower_hex(&evidence.issuer_configuration_sha256, 64)
        || !lower_hex(&evidence.preparation_manifest_sha256, 64)
        || !lower_hex(&evidence.evidence_sha256, 64)
        || !evidence.principal_id.starts_with("prn_")
    {
        return false;
    }
    let Ok(mut unsigned) = serde_json::to_value(evidence) else {
        return false;
    };
    let Some(fields) = unsigned.as_object_mut() else {
        return false;
    };
    fields.remove("evidenceSha256");
    let Ok(canonical) = serde_json::to_vec(&unsigned) else {
        return false;
    };
    let mut input = Vec::with_capacity(EVIDENCE_DOMAIN.len() + canonical.len());
    input.extend_from_slice(EVIDENCE_DOMAIN);
    input.extend_from_slice(&canonical);
    encode_hex(digest(&SHA256, &input).as_ref()) == evidence.evidence_sha256
}

fn encode_hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
