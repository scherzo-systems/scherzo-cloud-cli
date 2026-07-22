use std::fmt;
use std::io;
use std::time::Duration;

use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use reqwest::{Response, StatusCode, Url};

use super::http_client::{HttpClient, HttpEndpointError};
use super::http_util::{self, BoundedBodyError};
use super::human_principal::{self, HumanPrincipal};
use super::problem;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const PRINCIPAL_NOT_PROVISIONED: &str =
    "https://api.scherzo.dev/problems/principal-not-provisioned";
const JSON_MEDIA_TYPE: &str = "application/json";
const PROBLEM_MEDIA_TYPE: &str = "application/problem+json";
const ACCEPTED_MEDIA_TYPES: &str = "application/json, application/problem+json";

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum CurrentPrincipalOutcome {
    Authenticated(HumanPrincipal),
    SignupRequired {
        actions: Option<Vec<serde_json::Value>>,
    },
    Unauthenticated,
    Unreachable(UnreachableCategory),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnreachableCategory {
    Dns,
    Timeout,
    Connection,
    Tls,
    RateLimited,
    Server,
}

impl UnreachableCategory {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Dns => "dns",
            Self::Timeout => "timeout",
            Self::Connection => "connection",
            Self::Tls => "tls",
            Self::RateLimited => "rate_limited",
            Self::Server => "server",
        }
    }
}

#[derive(Debug)]
pub(crate) struct CurrentPrincipalError {
    kind: CurrentPrincipalErrorKind,
    credential_rejected: bool,
}

impl CurrentPrincipalError {
    pub(crate) fn credential_rejected(&self) -> bool {
        self.credential_rejected
    }

    pub(crate) fn is_local(&self) -> bool {
        !matches!(&self.kind, CurrentPrincipalErrorKind::Protocol { .. })
    }

    fn local(kind: CurrentPrincipalErrorKind) -> Self {
        Self {
            kind,
            credential_rejected: false,
        }
    }

    fn protocol(reason: &'static str, credential_rejected: bool) -> Self {
        Self {
            kind: CurrentPrincipalErrorKind::Protocol { reason },
            credential_rejected,
        }
    }
}

#[derive(Debug)]
enum CurrentPrincipalErrorKind {
    Endpoint(HttpEndpointError),
    InvalidAuthorizationHeader,
    Protocol { reason: &'static str },
}

impl fmt::Display for CurrentPrincipalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            CurrentPrincipalErrorKind::Endpoint(HttpEndpointError::Invalid) => write!(
                formatter,
                "the deployment API URL cannot form a current-principal endpoint"
            ),
            CurrentPrincipalErrorKind::Endpoint(HttpEndpointError::InsecureHttp) => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            CurrentPrincipalErrorKind::InvalidAuthorizationHeader => {
                write!(
                    formatter,
                    "the stored access token cannot be represented as a bearer credential"
                )
            }
            CurrentPrincipalErrorKind::Protocol { reason } => {
                write!(
                    formatter,
                    "current-principal response violates the public API contract: {reason}"
                )
            }
        }
    }
}

pub(crate) fn get_current_principal(
    client: &HttpClient,
    api_url: &str,
    access_token: Option<&str>,
) -> Result<CurrentPrincipalOutcome, CurrentPrincipalError> {
    get_current_principal_with_timeout(client, api_url, access_token, REQUEST_TIMEOUT)
}

fn get_current_principal_with_timeout(
    client: &HttpClient,
    api_url: &str,
    access_token: Option<&str>,
    timeout: Duration,
) -> Result<CurrentPrincipalOutcome, CurrentPrincipalError> {
    let endpoint = client.endpoint(api_url, &["v1", "me"]).map_err(|error| {
        CurrentPrincipalError::local(CurrentPrincipalErrorKind::Endpoint(error))
    })?;
    let authorization = access_token
        .map(|access_token| HeaderValue::from_str(&format!("Bearer {access_token}")))
        .transpose()
        .map_err(|_| {
            CurrentPrincipalError::local(CurrentPrincipalErrorKind::InvalidAuthorizationHeader)
        })?;
    let result = client.run(
        timeout,
        execute_current_principal_request(client, endpoint, authorization, timeout),
    );

    match result {
        Ok(result) => result,
        Err(_) => Ok(CurrentPrincipalOutcome::Unreachable(
            UnreachableCategory::Timeout,
        )),
    }
}

async fn execute_current_principal_request(
    client: &HttpClient,
    endpoint: Url,
    authorization: Option<HeaderValue>,
    timeout: Duration,
) -> Result<CurrentPrincipalOutcome, CurrentPrincipalError> {
    let mut request = client
        .inner()
        .get(endpoint)
        .timeout(timeout)
        .header(reqwest::header::ACCEPT, ACCEPTED_MEDIA_TYPES);
    if let Some(authorization) = authorization {
        request = request.header(AUTHORIZATION, authorization);
    }

    let response = match request.send().await {
        Ok(response) => response,
        Err(error) if error.is_builder() => {
            return Err(CurrentPrincipalError::protocol(
                "the current-principal request could not be constructed",
                false,
            ));
        }
        Err(error) => {
            return Ok(CurrentPrincipalOutcome::Unreachable(
                classify_reqwest_error(&error),
            ));
        }
    };

    decode_response(response).await
}

async fn decode_response(
    response: Response,
) -> Result<CurrentPrincipalOutcome, CurrentPrincipalError> {
    let status = response.status();
    let credential_rejected = status == StatusCode::UNAUTHORIZED;
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .map(http_util::media_type)
        .transpose()
        .map_err(|()| {
            CurrentPrincipalError::protocol(
                "the Content-Type header is not valid text",
                credential_rejected,
            )
        })?;
    let body = match http_util::read_bounded_body(response).await {
        Ok(body) => body,
        Err(BoundedBodyError::TooLarge) => {
            return Err(CurrentPrincipalError::protocol(
                "the response body exceeds 1 MiB",
                credential_rejected,
            ));
        }
        Err(BoundedBodyError::Transport(error)) => {
            return Ok(CurrentPrincipalOutcome::Unreachable(
                classify_reqwest_error(&error),
            ));
        }
    };

    match status {
        StatusCode::OK => {
            require_media_type(content_type.as_deref(), JSON_MEDIA_TYPE, false)?;
            decode_principal(&body).map(CurrentPrincipalOutcome::Authenticated)
        }
        StatusCode::UNAUTHORIZED => {
            require_media_type(content_type.as_deref(), PROBLEM_MEDIA_TYPE, true)?;
            let problem = decode_problem(&body, StatusCode::UNAUTHORIZED, true)?;
            let _ = problem;
            Ok(CurrentPrincipalOutcome::Unauthenticated)
        }
        StatusCode::FORBIDDEN => {
            require_media_type(content_type.as_deref(), PROBLEM_MEDIA_TYPE, false)?;
            let problem = decode_problem(&body, StatusCode::FORBIDDEN, false)?;
            if problem.r#type != PRINCIPAL_NOT_PROVISIONED {
                return Err(CurrentPrincipalError::protocol(
                    "a 403 response is not the principal-not-provisioned problem",
                    false,
                ));
            }
            Ok(CurrentPrincipalOutcome::SignupRequired {
                actions: problem.actions,
            })
        }
        StatusCode::TOO_MANY_REQUESTS => Ok(CurrentPrincipalOutcome::Unreachable(
            UnreachableCategory::RateLimited,
        )),
        status if status.is_server_error() => Ok(CurrentPrincipalOutcome::Unreachable(
            UnreachableCategory::Server,
        )),
        status if status.is_redirection() => Err(CurrentPrincipalError::protocol(
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(CurrentPrincipalError::protocol(
            "the HTTP status is not valid for this operation",
            false,
        )),
    }
}

fn require_media_type(
    actual: Option<&str>,
    expected: &'static str,
    credential_rejected: bool,
) -> Result<(), CurrentPrincipalError> {
    if actual == Some(expected) {
        Ok(())
    } else {
        Err(CurrentPrincipalError::protocol(
            "the response Content-Type is not valid for its HTTP status",
            credential_rejected,
        ))
    }
}

fn decode_principal(body: &[u8]) -> Result<HumanPrincipal, CurrentPrincipalError> {
    human_principal::decode(body).map_err(|reason| CurrentPrincipalError::protocol(reason, false))
}

fn decode_problem(
    body: &[u8],
    expected_status: StatusCode,
    credential_rejected: bool,
) -> Result<super::generated::models::Problem, CurrentPrincipalError> {
    problem::decode(body, expected_status)
        .map_err(|reason| CurrentPrincipalError::protocol(reason, credential_rejected))
}

pub(crate) fn classify_reqwest_error(error: &reqwest::Error) -> UnreachableCategory {
    if error.is_timeout() {
        UnreachableCategory::Timeout
    } else {
        classify_error_chain(error)
    }
}

fn classify_error_chain(error: &(dyn std::error::Error + 'static)) -> UnreachableCategory {
    let mut messages = String::new();
    let mut source = Some(error);
    let mut timed_out = false;
    let mut tls_failed = false;

    while let Some(current) = source {
        if current.downcast_ref::<reqwest::Error>().is_none() {
            let message = current.to_string().to_ascii_lowercase();
            messages.push_str(&message);
            messages.push('\n');
        }
        if let Some(io_error) = current.downcast_ref::<io::Error>() {
            timed_out |= io_error.kind() == io::ErrorKind::TimedOut;
        }
        tls_failed |= current.downcast_ref::<rustls::Error>().is_some();
        source = current.source();
    }

    if timed_out {
        UnreachableCategory::Timeout
    } else if contains_any(
        &messages,
        &[
            "dns",
            "failed to lookup address",
            "name or service not known",
            "nodename nor servname provided",
            "no address associated with hostname",
        ],
    ) {
        UnreachableCategory::Dns
    } else if tls_failed
        || contains_any(
            &messages,
            &[
                "tls",
                "rustls",
                "certificate",
                "invalid peer certificate",
                "handshake",
                "received corrupt message",
            ],
        )
    {
        UnreachableCategory::Tls
    } else {
        UnreachableCategory::Connection
    }
}

fn contains_any(value: &str, candidates: &[&str]) -> bool {
    candidates.iter().any(|candidate| value.contains(candidate))
}

#[cfg(test)]
mod tests;
