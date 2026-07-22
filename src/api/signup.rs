use std::fmt;
use std::time::Duration;

// Signup keeps its transport imports explicit because its retrying mutation has
// different request and failure semantics from read-only principal lookup.
// jscpd:ignore-start
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use reqwest::{Response, StatusCode, Url};

use super::http_client::{HttpClient, HttpEndpointError};
use super::http_util::{self, BoundedBodyError};
use super::human_principal::{self, HumanPrincipal};
use super::problem;
// jscpd:ignore-end
use super::{UnreachableCategory, classify_reqwest_error};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_ATTEMPTS: usize = 2;
const JSON_MEDIA_TYPE: &str = "application/json";
const PROBLEM_MEDIA_TYPE: &str = "application/problem+json";
const ACCEPTED_MEDIA_TYPES: &str = "application/json, application/problem+json";
const UNAUTHORIZED: &str = "https://api.scherzo.dev/problems/unauthorized";
const SIGNUP_NOT_PERMITTED: &str = "https://api.scherzo.dev/problems/signup-not-permitted";
const PRINCIPAL_ALREADY_PROVISIONED: &str =
    "https://api.scherzo.dev/problems/principal-already-provisioned";
const IDEMPOTENCY_CONFLICT: &str = "https://api.scherzo.dev/problems/idempotency-conflict";

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum SignupOutcome {
    Authenticated(HumanPrincipal),
    Unauthenticated,
    SignupNotPermitted,
    AlreadyProvisioned,
    IdempotencyConflict,
    Unreachable(UnreachableCategory),
}

#[derive(Debug)]
pub(crate) struct SignupError {
    kind: SignupErrorKind,
    credential_rejected: bool,
}

impl SignupError {
    pub(crate) fn credential_rejected(&self) -> bool {
        self.credential_rejected
    }

    fn local(kind: SignupErrorKind) -> Self {
        Self {
            kind,
            credential_rejected: false,
        }
    }

    fn protocol(reason: &'static str, credential_rejected: bool) -> Self {
        Self {
            kind: SignupErrorKind::Protocol { reason },
            credential_rejected,
        }
    }
}

#[derive(Debug)]
enum SignupErrorKind {
    Endpoint(HttpEndpointError),
    InvalidAuthorizationHeader,
    Protocol { reason: &'static str },
}

impl fmt::Display for SignupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            SignupErrorKind::Endpoint(HttpEndpointError::Invalid) => write!(
                formatter,
                "the deployment API URL cannot form a signup endpoint"
            ),
            SignupErrorKind::Endpoint(HttpEndpointError::InsecureHttp) => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            SignupErrorKind::InvalidAuthorizationHeader => write!(
                formatter,
                "the stored access token cannot be represented as a bearer credential"
            ),
            SignupErrorKind::Protocol { reason } => write!(
                formatter,
                "signup response violates the public API contract: {reason}"
            ),
        }
    }
}

enum AttemptError {
    Protocol(SignupError),
    Transport(UnreachableCategory),
}

pub(crate) fn signup_human(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    idempotency_key: &str,
) -> Result<SignupOutcome, SignupError> {
    signup_human_with_timeout(
        client,
        api_url,
        access_token,
        idempotency_key,
        REQUEST_TIMEOUT,
    )
}

fn signup_human_with_timeout(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    idempotency_key: &str,
    timeout: Duration,
) -> Result<SignupOutcome, SignupError> {
    let endpoint = client
        .endpoint(api_url, &["v1", "signup"])
        .map_err(|error| SignupError::local(SignupErrorKind::Endpoint(error)))?;
    let authorization = HeaderValue::from_str(&format!("Bearer {access_token}"))
        .map_err(|_| SignupError::local(SignupErrorKind::InvalidAuthorizationHeader))?;
    let idempotency_key = HeaderValue::from_str(idempotency_key).map_err(|_| {
        SignupError::protocol(
            "the generated idempotency key is not a valid header value",
            false,
        )
    })?;
    let mut last_transport_failure = UnreachableCategory::Connection;

    for _ in 0..MAX_ATTEMPTS {
        let result = client.run(
            timeout,
            execute_signup_request(
                client,
                endpoint.clone(),
                authorization.clone(),
                idempotency_key.clone(),
                timeout,
            ),
        );
        match result {
            Ok(Ok(outcome)) => return Ok(outcome),
            Ok(Err(AttemptError::Protocol(error))) => return Err(error),
            Ok(Err(AttemptError::Transport(category))) => {
                last_transport_failure = category;
            }
            Err(_) => {
                last_transport_failure = UnreachableCategory::Timeout;
            }
        }
    }

    Ok(SignupOutcome::Unreachable(last_transport_failure))
}

async fn execute_signup_request(
    client: &HttpClient,
    endpoint: Url,
    authorization: HeaderValue,
    idempotency_key: HeaderValue,
    timeout: Duration,
) -> Result<SignupOutcome, AttemptError> {
    let response = client
        .inner()
        .post(endpoint)
        .timeout(timeout)
        .header(ACCEPT, ACCEPTED_MEDIA_TYPES)
        .header(AUTHORIZATION, authorization)
        .header("Idempotency-Key", idempotency_key)
        .send()
        .await
        .map_err(|error| {
            if error.is_builder() {
                AttemptError::Protocol(SignupError::protocol(
                    "the signup request could not be constructed",
                    false,
                ))
            } else {
                AttemptError::Transport(classify_reqwest_error(&error))
            }
        })?;

    decode_response(response).await
}

async fn decode_response(response: Response) -> Result<SignupOutcome, AttemptError> {
    let status = response.status();
    let credential_rejected = status == StatusCode::UNAUTHORIZED;
    // Signup must distinguish retryable transport failures from protocol
    // failures, unlike status lookup, so this small response adapter stays local.
    // jscpd:ignore-start
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .map(http_util::media_type)
        .transpose()
        .map_err(|()| {
            AttemptError::Protocol(SignupError::protocol(
                "the Content-Type header is not valid text",
                credential_rejected,
            ))
        })?;
    let body = match http_util::read_bounded_body(response).await {
        Ok(body) => body,
        Err(BoundedBodyError::TooLarge) => {
            return Err(AttemptError::Protocol(SignupError::protocol(
                "the response body exceeds 1 MiB",
                credential_rejected,
            )));
        }
        Err(BoundedBodyError::Transport(error)) => {
            return Err(AttemptError::Transport(classify_reqwest_error(&error)));
        }
    };
    // jscpd:ignore-end

    match status {
        StatusCode::CREATED => {
            require_media_type(content_type.as_deref(), JSON_MEDIA_TYPE, false)?;
            human_principal::decode(&body)
                .map(SignupOutcome::Authenticated)
                .map_err(|reason| AttemptError::Protocol(SignupError::protocol(reason, false)))
        }
        StatusCode::UNAUTHORIZED => {
            require_problem_type(
                &body,
                StatusCode::UNAUTHORIZED,
                content_type.as_deref(),
                UNAUTHORIZED,
                true,
            )?;
            Ok(SignupOutcome::Unauthenticated)
        }
        StatusCode::FORBIDDEN => {
            require_problem_type(
                &body,
                StatusCode::FORBIDDEN,
                content_type.as_deref(),
                SIGNUP_NOT_PERMITTED,
                false,
            )?;
            Ok(SignupOutcome::SignupNotPermitted)
        }
        StatusCode::CONFLICT => {
            require_media_type(content_type.as_deref(), PROBLEM_MEDIA_TYPE, false)?;
            let problem = problem::decode(&body, StatusCode::CONFLICT)
                .map_err(|reason| AttemptError::Protocol(SignupError::protocol(reason, false)))?;
            match problem.r#type.as_str() {
                PRINCIPAL_ALREADY_PROVISIONED => Ok(SignupOutcome::AlreadyProvisioned),
                IDEMPOTENCY_CONFLICT => Ok(SignupOutcome::IdempotencyConflict),
                _ => Err(AttemptError::Protocol(SignupError::protocol(
                    "a 409 response has an unrecognized problem type",
                    false,
                ))),
            }
        }
        StatusCode::TOO_MANY_REQUESTS => {
            Ok(SignupOutcome::Unreachable(UnreachableCategory::RateLimited))
        }
        status if status.is_server_error() => {
            Ok(SignupOutcome::Unreachable(UnreachableCategory::Server))
        }
        status if status.is_redirection() => Err(AttemptError::Protocol(SignupError::protocol(
            "redirect responses are not permitted",
            false,
        ))),
        _ => Err(AttemptError::Protocol(SignupError::protocol(
            "the HTTP status is not valid for this operation",
            false,
        ))),
    }
}

fn require_problem_type(
    body: &[u8],
    status: StatusCode,
    content_type: Option<&str>,
    expected_type: &'static str,
    credential_rejected: bool,
) -> Result<(), AttemptError> {
    require_media_type(content_type, PROBLEM_MEDIA_TYPE, credential_rejected)?;
    let problem = problem::decode(body, status).map_err(|reason| {
        AttemptError::Protocol(SignupError::protocol(reason, credential_rejected))
    })?;
    if problem.r#type != expected_type {
        return Err(AttemptError::Protocol(SignupError::protocol(
            "the problem type is not valid for its HTTP status",
            credential_rejected,
        )));
    }
    Ok(())
}

fn require_media_type(
    actual: Option<&str>,
    expected: &'static str,
    credential_rejected: bool,
) -> Result<(), AttemptError> {
    if actual == Some(expected) {
        Ok(())
    } else {
        Err(AttemptError::Protocol(SignupError::protocol(
            "the response Content-Type is not valid for its HTTP status",
            credential_rejected,
        )))
    }
}
