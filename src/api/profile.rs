use std::error::Error;
use std::fmt;
use std::time::Duration;

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue, InvalidHeaderValue};
use reqwest::{Response, StatusCode, Url};

use super::generated::models;
use super::http_client::{HttpClient, HttpEndpointError};
use super::http_util::{self, BoundedBodyError};
use super::human_principal::{self, HumanPrincipal};
use super::problem::{
    self, ACCEPTED_MEDIA_TYPES, BAD_REQUEST, FORBIDDEN, JSON_MEDIA_TYPE, UNAUTHORIZED,
};
use super::{UnreachableCategory, bearer_authorization, classify_reqwest_error};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_ATTEMPTS: usize = 2;
const MERGE_PATCH_MEDIA_TYPE: &str = "application/merge-patch+json";
const INVALID_DISPLAY_NAME: &str = "https://api.scherzo.dev/problems/invalid-display-name";
const IDEMPOTENCY_CONFLICT: &str = "https://api.scherzo.dev/problems/idempotency-conflict";
const REQUEST_BODY_TOO_LARGE: &str = "https://api.scherzo.dev/problems/request-body-too-large";
const UNSUPPORTED_MEDIA_TYPE: &str = "https://api.scherzo.dev/problems/unsupported-media-type";

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum UpdateProfileOutcome {
    Updated(HumanPrincipal),
    InvalidDisplayName,
    Unauthenticated,
    Forbidden,
    IdempotencyConflict,
    RequestTooLarge,
    UnsupportedMediaType,
    Unreachable(UnreachableCategory),
}

#[derive(Debug)]
pub(crate) struct UpdateProfileError {
    kind: UpdateProfileErrorKind,
    credential_rejected: bool,
}

impl UpdateProfileError {
    pub(crate) fn credential_rejected(&self) -> bool {
        self.credential_rejected
    }

    fn local(kind: UpdateProfileErrorKind) -> Self {
        Self {
            kind,
            credential_rejected: false,
        }
    }

    fn protocol(reason: &'static str, credential_rejected: bool) -> Self {
        Self {
            kind: UpdateProfileErrorKind::Protocol { reason },
            credential_rejected,
        }
    }
}

#[derive(Debug)]
enum UpdateProfileErrorKind {
    Endpoint(HttpEndpointError),
    InvalidAuthorizationHeader(InvalidHeaderValue),
    InvalidIdempotencyHeader(InvalidHeaderValue),
    SerializeRequest(serde_json::Error),
    Protocol { reason: &'static str },
}

impl fmt::Display for UpdateProfileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            UpdateProfileErrorKind::Endpoint(HttpEndpointError::Invalid) => write!(
                formatter,
                "the deployment API URL cannot form an account update endpoint"
            ),
            UpdateProfileErrorKind::Endpoint(HttpEndpointError::InsecureHttp) => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            UpdateProfileErrorKind::InvalidAuthorizationHeader(_) => write!(
                formatter,
                "the stored access token cannot be represented as a bearer credential"
            ),
            UpdateProfileErrorKind::InvalidIdempotencyHeader(_) => write!(
                formatter,
                "the generated account update request identity is not a valid header value"
            ),
            UpdateProfileErrorKind::SerializeRequest(_) => {
                write!(
                    formatter,
                    "the account update request could not be serialized"
                )
            }
            UpdateProfileErrorKind::Protocol { reason } => write!(
                formatter,
                "account update response violates the public API contract: {reason}"
            ),
        }
    }
}

impl Error for UpdateProfileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            UpdateProfileErrorKind::InvalidAuthorizationHeader(error)
            | UpdateProfileErrorKind::InvalidIdempotencyHeader(error) => Some(error),
            UpdateProfileErrorKind::SerializeRequest(error) => Some(error),
            UpdateProfileErrorKind::Endpoint(_) | UpdateProfileErrorKind::Protocol { .. } => None,
        }
    }
}

enum AttemptError {
    Protocol(UpdateProfileError),
    Ambiguous(UnreachableCategory),
}

pub(crate) fn update_current_principal(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    idempotency_key: &str,
    display_name: Option<&str>,
) -> Result<UpdateProfileOutcome, UpdateProfileError> {
    update_current_principal_with_timeout(
        client,
        api_url,
        access_token,
        idempotency_key,
        display_name,
        REQUEST_TIMEOUT,
    )
}

fn update_current_principal_with_timeout(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    idempotency_key: &str,
    display_name: Option<&str>,
    timeout: Duration,
) -> Result<UpdateProfileOutcome, UpdateProfileError> {
    let endpoint = client
        .endpoint(api_url, &["v1", "me"])
        .map_err(|error| UpdateProfileError::local(UpdateProfileErrorKind::Endpoint(error)))?;
    let authorization = bearer_authorization(access_token).map_err(|error| {
        UpdateProfileError::local(UpdateProfileErrorKind::InvalidAuthorizationHeader(error))
    })?;
    let idempotency_key = HeaderValue::from_str(idempotency_key).map_err(|error| {
        UpdateProfileError::local(UpdateProfileErrorKind::InvalidIdempotencyHeader(error))
    })?;
    let request = models::UpdateCurrentPrincipalPatch::new(display_name.map(str::to_owned));
    let body = serde_json::to_vec(&request).map_err(|error| {
        UpdateProfileError::local(UpdateProfileErrorKind::SerializeRequest(error))
    })?;
    let mut last_transport_failure = UnreachableCategory::Connection;

    for attempt in 0..MAX_ATTEMPTS {
        let result = client.run(
            timeout,
            execute_update_request(
                client,
                endpoint.clone(),
                authorization.clone(),
                idempotency_key.clone(),
                body.clone(),
                display_name.is_some(),
                timeout,
            ),
        );
        match result {
            Ok(Ok(outcome)) => return Ok(outcome),
            Ok(Err(AttemptError::Protocol(error))) => return Err(error),
            Ok(Err(AttemptError::Ambiguous(category))) => last_transport_failure = category,
            Err(_) => last_transport_failure = UnreachableCategory::Timeout,
        }
        if attempt + 1 < MAX_ATTEMPTS {
            crate::timing::sleep(crate::timing::short_retry_delay());
        }
    }

    Ok(UpdateProfileOutcome::Unreachable(last_transport_failure))
}

async fn execute_update_request(
    client: &HttpClient,
    endpoint: Url,
    authorization: HeaderValue,
    idempotency_key: HeaderValue,
    body: Vec<u8>,
    expects_display_name: bool,
    timeout: Duration,
) -> Result<UpdateProfileOutcome, AttemptError> {
    let response = client
        .inner()
        .patch(endpoint)
        .timeout(timeout)
        .header(ACCEPT, ACCEPTED_MEDIA_TYPES)
        .header(AUTHORIZATION, authorization)
        .header("Idempotency-Key", idempotency_key.clone())
        .header(CONTENT_TYPE, MERGE_PATCH_MEDIA_TYPE)
        .body(body)
        .send()
        .await
        .map_err(|error| {
            if error.is_builder() {
                AttemptError::Protocol(UpdateProfileError::protocol(
                    "the account update request could not be constructed",
                    false,
                ))
            } else {
                AttemptError::Ambiguous(classify_reqwest_error(&error))
            }
        })?;

    decode_response(response, &idempotency_key, expects_display_name).await
}

async fn decode_response(
    response: Response,
    expected_idempotency_key: &HeaderValue,
    expects_display_name: bool,
) -> Result<UpdateProfileOutcome, AttemptError> {
    let status = response.status();
    let credential_rejected = status == StatusCode::UNAUTHORIZED;
    if status == StatusCode::OK
        && response.headers().get("Idempotency-Key") != Some(expected_idempotency_key)
    {
        return Err(AttemptError::Protocol(UpdateProfileError::protocol(
            "the successful response has a missing or mismatched Idempotency-Key header",
            false,
        )));
    }
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .map(http_util::media_type)
        .transpose()
        .map_err(|()| {
            AttemptError::Protocol(UpdateProfileError::protocol(
                "the Content-Type header is not valid text",
                credential_rejected,
            ))
        })?;
    let body = match http_util::read_bounded_body(response).await {
        Ok(body) => body,
        Err(BoundedBodyError::TooLarge) => {
            return Err(AttemptError::Protocol(UpdateProfileError::protocol(
                "the response body exceeds 1 MiB",
                credential_rejected,
            )));
        }
        Err(BoundedBodyError::Transport(_)) if credential_rejected => {
            return Err(AttemptError::Protocol(UpdateProfileError::protocol(
                "the unauthorized response body could not be read",
                true,
            )));
        }
        Err(BoundedBodyError::Transport(error)) if status == StatusCode::OK => {
            return Err(AttemptError::Ambiguous(classify_reqwest_error(&error)));
        }
        Err(BoundedBodyError::Transport(error)) => {
            return Ok(UpdateProfileOutcome::Unreachable(
                if status.is_server_error() {
                    UnreachableCategory::Server
                } else {
                    classify_reqwest_error(&error)
                },
            ));
        }
    };

    match status {
        StatusCode::OK => {
            http_util::require_media_type(content_type.as_deref(), JSON_MEDIA_TYPE).map_err(
                |reason| AttemptError::Protocol(UpdateProfileError::protocol(reason, false)),
            )?;
            decode_principal(&body, expects_display_name)
                .map(UpdateProfileOutcome::Updated)
                .map_err(AttemptError::Protocol)
        }
        StatusCode::BAD_REQUEST => {
            let problem_type = problem::decode_type_parts(&body, status, content_type.as_deref())
                .map_err(|reason| {
                AttemptError::Protocol(UpdateProfileError::protocol(reason, false))
            })?;
            if matches!(problem_type.as_str(), INVALID_DISPLAY_NAME | BAD_REQUEST) {
                Ok(UpdateProfileOutcome::InvalidDisplayName)
            } else {
                Err(AttemptError::Protocol(UpdateProfileError::protocol(
                    "a 400 response has an unrecognized problem type",
                    false,
                )))
            }
        }
        StatusCode::UNAUTHORIZED => {
            problem::require_type_parts(&body, status, content_type.as_deref(), UNAUTHORIZED)
                .map_err(|reason| {
                    AttemptError::Protocol(UpdateProfileError::protocol(reason, true))
                })?;
            Ok(UpdateProfileOutcome::Unauthenticated)
        }
        StatusCode::FORBIDDEN => {
            require_profile_problem(&body, status, content_type.as_deref(), FORBIDDEN)?;
            Ok(UpdateProfileOutcome::Forbidden)
        }
        StatusCode::CONFLICT => {
            require_profile_problem(&body, status, content_type.as_deref(), IDEMPOTENCY_CONFLICT)?;
            Ok(UpdateProfileOutcome::IdempotencyConflict)
        }
        StatusCode::PAYLOAD_TOO_LARGE => {
            require_profile_problem(
                &body,
                status,
                content_type.as_deref(),
                REQUEST_BODY_TOO_LARGE,
            )?;
            Ok(UpdateProfileOutcome::RequestTooLarge)
        }
        StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            require_profile_problem(
                &body,
                status,
                content_type.as_deref(),
                UNSUPPORTED_MEDIA_TYPE,
            )?;
            Ok(UpdateProfileOutcome::UnsupportedMediaType)
        }
        status if status.is_server_error() => Ok(UpdateProfileOutcome::Unreachable(
            UnreachableCategory::Server,
        )),
        status if status.is_redirection() => Err(AttemptError::Protocol(
            UpdateProfileError::protocol("redirect responses are not permitted", false),
        )),
        _ => Err(AttemptError::Protocol(UpdateProfileError::protocol(
            "the HTTP status is not valid for this operation",
            false,
        ))),
    }
}

fn decode_principal(
    body: &[u8],
    expects_display_name: bool,
) -> Result<HumanPrincipal, UpdateProfileError> {
    let value: serde_json::Value = serde_json::from_slice(body).map_err(|_| {
        UpdateProfileError::protocol("the principal response body is not valid JSON", false)
    })?;
    if value
        .get("displayName")
        .is_some_and(serde_json::Value::is_null)
    {
        return Err(UpdateProfileError::protocol(
            "the principal response contains an explicit null displayName",
            false,
        ));
    }
    let principal: models::Principal = serde_json::from_value(value)
        .map_err(|_| UpdateProfileError::protocol("the principal fields are invalid", false))?;
    let principal = human_principal::from_api(principal)
        .map_err(|reason| UpdateProfileError::protocol(reason, false))?;
    if principal.display_name.is_some() != expects_display_name {
        return Err(UpdateProfileError::protocol(
            "the principal display name does not match the requested operation",
            false,
        ));
    }
    Ok(principal)
}

fn require_profile_problem(
    body: &[u8],
    status: StatusCode,
    content_type: Option<&str>,
    expected_type: &'static str,
) -> Result<(), AttemptError> {
    problem::require_type_parts(body, status, content_type, expected_type)
        .map_err(|reason| AttemptError::Protocol(UpdateProfileError::protocol(reason, false)))
}
