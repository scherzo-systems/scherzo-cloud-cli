use std::fmt;
use std::time::Duration;

use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderValue};
use reqwest::{StatusCode, Url};
use serde::Deserialize;

use crate::api::{HttpClient, UnreachableCategory, classify_reqwest_error};

use super::deployment::Deployment;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;
const MAX_ACCESS_TOKEN_BYTES: usize = 64 * 1024;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);
const JSON_MEDIA_TYPE: &str = "application/json";
const DEVICE_CODE_PATH: [&str; 3] = ["oauth", "device", "code"];
const TOKEN_PATH: [&str; 2] = ["oauth", "token"];
const DEVICE_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:device_code";
const SCOPES: &str = "openid profile email";

pub(crate) struct DeviceAuthorization {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: Option<String>,
    expires_in: Duration,
    interval: Duration,
}

impl fmt::Debug for DeviceAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeviceAuthorization")
            .field("device_code", &"[REDACTED]")
            .field("user_code", &"[ACTIVATION MATERIAL]")
            .field("verification_uri", &"[ACTIVATION MATERIAL]")
            .field("verification_uri_complete", &"[ACTIVATION MATERIAL]")
            .field("expires_in", &self.expires_in)
            .field("interval", &self.interval)
            .finish()
    }
}

impl DeviceAuthorization {
    pub(crate) fn device_code(&self) -> &str {
        &self.device_code
    }

    pub(crate) fn user_code(&self) -> &str {
        &self.user_code
    }

    pub(crate) fn verification_uri(&self) -> &str {
        &self.verification_uri
    }

    pub(crate) fn verification_uri_complete(&self) -> Option<&str> {
        self.verification_uri_complete.as_deref()
    }

    pub(crate) fn expires_in(&self) -> Duration {
        self.expires_in
    }

    pub(crate) fn interval(&self) -> Duration {
        self.interval
    }
}

#[derive(Debug)]
pub(crate) enum TokenPoll {
    Pending,
    SlowDown,
    Denied,
    Expired,
    Issued(IssuedToken),
}

pub(crate) struct IssuedToken {
    access_token: String,
    expires_in: Duration,
}

impl fmt::Debug for IssuedToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedToken")
            .field("access_token", &"[REDACTED]")
            .field("expires_in", &self.expires_in)
            .finish()
    }
}

impl IssuedToken {
    pub(crate) fn access_token(&self) -> &str {
        &self.access_token
    }

    pub(crate) fn expires_in(&self) -> Duration {
        self.expires_in
    }
}

pub(crate) fn authorize(
    client: &HttpClient,
    deployment: &Deployment,
) -> Result<DeviceAuthorization, AuthorizationError> {
    let endpoint = endpoint(deployment.fingerprint().issuer(), &DEVICE_CODE_PATH)?;
    let fields = [
        ("client_id", deployment.fingerprint().client_id()),
        ("audience", deployment.fingerprint().audience()),
        ("scope", SCOPES),
    ];
    let response = post_form(client, endpoint, &fields)?;

    match response.status {
        StatusCode::OK => {
            require_json(&response)?;
            decode_device_authorization(&response.body, deployment.allows_insecure_http())
        }
        StatusCode::TOO_MANY_REQUESTS => Err(AuthorizationError::Unreachable(
            UnreachableCategory::RateLimited,
        )),
        status if status.is_server_error() => {
            Err(AuthorizationError::Unreachable(UnreachableCategory::Server))
        }
        status if status.is_redirection() => Err(AuthorizationError::Protocol {
            reason: "redirect responses are not permitted",
        }),
        _ => Err(AuthorizationError::Protocol {
            reason: "the device-authorization HTTP status is invalid",
        }),
    }
}

pub(crate) fn poll_token(
    client: &HttpClient,
    deployment: &Deployment,
    device_code: &str,
) -> Result<TokenPoll, AuthorizationError> {
    let endpoint = endpoint(deployment.fingerprint().issuer(), &TOKEN_PATH)?;
    let fields = [
        ("grant_type", DEVICE_GRANT_TYPE),
        ("device_code", device_code),
        ("client_id", deployment.fingerprint().client_id()),
    ];
    let response = post_form(client, endpoint, &fields)?;

    if response.status == StatusCode::OK {
        require_json(&response)?;
        return decode_issued_token(&response.body).map(TokenPoll::Issued);
    }
    if response.status.is_redirection() {
        return Err(AuthorizationError::Protocol {
            reason: "redirect responses are not permitted",
        });
    }
    if response.status.is_server_error() {
        return Err(AuthorizationError::Unreachable(UnreachableCategory::Server));
    }
    if response.status.is_client_error() {
        if media_type(&response).is_some_and(|value| value.eq_ignore_ascii_case(JSON_MEDIA_TYPE)) {
            if let Ok(error) = serde_json::from_slice::<OAuthErrorResponse>(&response.body) {
                return match error.error.as_str() {
                    "authorization_pending" => Ok(TokenPoll::Pending),
                    "slow_down" => Ok(TokenPoll::SlowDown),
                    "access_denied" => Ok(TokenPoll::Denied),
                    "expired_token" => Ok(TokenPoll::Expired),
                    _ if response.status == StatusCode::TOO_MANY_REQUESTS => Err(
                        AuthorizationError::Unreachable(UnreachableCategory::RateLimited),
                    ),
                    _ => Err(AuthorizationError::Protocol {
                        reason: "the token endpoint returned an unknown OAuth error",
                    }),
                };
            }
        }
        if response.status == StatusCode::TOO_MANY_REQUESTS {
            return Err(AuthorizationError::Unreachable(
                UnreachableCategory::RateLimited,
            ));
        }
        return Err(AuthorizationError::Protocol {
            reason: "the token error response is invalid",
        });
    }

    Err(AuthorizationError::Protocol {
        reason: "the token endpoint HTTP status is invalid",
    })
}

#[derive(Debug)]
pub(crate) enum AuthorizationError {
    Local(AuthorizationLocalError),
    Unreachable(UnreachableCategory),
    Protocol { reason: &'static str },
}

impl fmt::Display for AuthorizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(error) => write!(formatter, "{error}"),
            Self::Unreachable(category) => {
                write!(
                    formatter,
                    "authorization server is unreachable ({})",
                    category.as_str()
                )
            }
            Self::Protocol { reason } => {
                write!(
                    formatter,
                    "authorization response violates the OAuth contract: {reason}"
                )
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum AuthorizationLocalError {
    InvalidEndpoint,
}

impl fmt::Display for AuthorizationLocalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                formatter,
                "authorization issuer cannot form an OAuth endpoint"
            ),
        }
    }
}

struct RawResponse {
    status: StatusCode,
    content_type: Option<HeaderValue>,
    body: Vec<u8>,
}

fn post_form(
    client: &HttpClient,
    endpoint: Url,
    fields: &[(&str, &str)],
) -> Result<RawResponse, AuthorizationError> {
    post_form_with_timeout(client, endpoint, fields, REQUEST_TIMEOUT)
}

fn post_form_with_timeout(
    client: &HttpClient,
    endpoint: Url,
    fields: &[(&str, &str)],
    timeout: Duration,
) -> Result<RawResponse, AuthorizationError> {
    let result = client.run(timeout, post_form_async(client, endpoint, fields, timeout));

    match result {
        Ok(result) => result,
        Err(_) => Err(AuthorizationError::Unreachable(
            UnreachableCategory::Timeout,
        )),
    }
}

async fn post_form_async(
    client: &HttpClient,
    endpoint: Url,
    fields: &[(&str, &str)],
    timeout: Duration,
) -> Result<RawResponse, AuthorizationError> {
    let response = client
        .inner()
        .post(endpoint)
        .timeout(timeout)
        .header(ACCEPT, JSON_MEDIA_TYPE)
        .form(fields)
        .send()
        .await
        .map_err(|error| AuthorizationError::Unreachable(classify_reqwest_error(&error)))?;
    let status = response.status();
    let content_type = response.headers().get(CONTENT_TYPE).cloned();
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BODY_BYTES as u64)
    {
        return Err(AuthorizationError::Protocol {
            reason: "the response body exceeds 1 MiB",
        });
    }
    let mut response = response;
    let mut body = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or_default()
            .min(MAX_RESPONSE_BODY_BYTES as u64) as usize,
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| AuthorizationError::Unreachable(classify_reqwest_error(&error)))?
    {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BODY_BYTES {
            return Err(AuthorizationError::Protocol {
                reason: "the response body exceeds 1 MiB",
            });
        }
        body.extend_from_slice(&chunk);
    }

    Ok(RawResponse {
        status,
        content_type,
        body,
    })
}

fn endpoint(issuer: &str, path: &[&str]) -> Result<Url, AuthorizationError> {
    let mut endpoint = Url::parse(issuer)
        .map_err(|_| AuthorizationError::Local(AuthorizationLocalError::InvalidEndpoint))?;
    let mut segments = endpoint
        .path_segments_mut()
        .map_err(|()| AuthorizationError::Local(AuthorizationLocalError::InvalidEndpoint))?;
    segments.pop_if_empty();
    for segment in path {
        segments.push(segment);
    }
    drop(segments);
    Ok(endpoint)
}

fn media_type(response: &RawResponse) -> Option<&str> {
    response
        .content_type
        .as_ref()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
}

fn require_json(response: &RawResponse) -> Result<(), AuthorizationError> {
    if media_type(response).is_some_and(|value| value.eq_ignore_ascii_case(JSON_MEDIA_TYPE)) {
        Ok(())
    } else {
        Err(AuthorizationError::Protocol {
            reason: "the response Content-Type is not application/json",
        })
    }
}

#[derive(Deserialize)]
struct DeviceAuthorizationResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: Option<String>,
    expires_in: u64,
    interval: Option<u64>,
}

fn decode_device_authorization(
    body: &[u8],
    allow_insecure_http: bool,
) -> Result<DeviceAuthorization, AuthorizationError> {
    let response: DeviceAuthorizationResponse =
        serde_json::from_slice(body).map_err(|_| AuthorizationError::Protocol {
            reason: "the device-authorization body is invalid",
        })?;
    if response.device_code.is_empty() {
        return Err(AuthorizationError::Protocol {
            reason: "the device code is empty",
        });
    }
    if response.user_code.is_empty() {
        return Err(AuthorizationError::Protocol {
            reason: "the user code is empty",
        });
    }
    validate_verification_uri(&response.verification_uri, allow_insecure_http)?;
    if let Some(uri) = &response.verification_uri_complete {
        validate_verification_uri(uri, allow_insecure_http)?;
    }
    let expires_in = positive_duration(
        response.expires_in,
        "the device-authorization lifetime is not positive",
    )?;
    let interval = match response.interval {
        Some(0) => {
            return Err(AuthorizationError::Protocol {
                reason: "the device-authorization polling interval is not positive",
            });
        }
        Some(seconds) => Duration::from_secs(seconds),
        None => DEFAULT_POLL_INTERVAL,
    };

    Ok(DeviceAuthorization {
        device_code: response.device_code,
        user_code: response.user_code,
        verification_uri: response.verification_uri,
        verification_uri_complete: response.verification_uri_complete,
        expires_in,
        interval,
    })
}

fn validate_verification_uri(
    value: &str,
    allow_insecure_http: bool,
) -> Result<(), AuthorizationError> {
    let uri = Url::parse(value).map_err(|_| AuthorizationError::Protocol {
        reason: "an activation URL is invalid",
    })?;
    let permitted_scheme =
        uri.scheme() == "https" || (allow_insecure_http && uri.scheme() == "http");
    if uri.host_str().is_none()
        || !permitted_scheme
        || !uri.username().is_empty()
        || uri.password().is_some()
    {
        return Err(AuthorizationError::Protocol {
            reason: "an activation URL is not an HTTP network URL",
        });
    }
    Ok(())
}

#[derive(Deserialize)]
struct IssuedTokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
}

fn decode_issued_token(body: &[u8]) -> Result<IssuedToken, AuthorizationError> {
    let response: IssuedTokenResponse =
        serde_json::from_slice(body).map_err(|_| AuthorizationError::Protocol {
            reason: "the successful token body is invalid",
        })?;
    if response.access_token.is_empty() {
        return Err(AuthorizationError::Protocol {
            reason: "the issued access token is empty",
        });
    }
    if response.access_token.len() > MAX_ACCESS_TOKEN_BYTES {
        return Err(AuthorizationError::Protocol {
            reason: "the issued access token exceeds 64 KiB",
        });
    }
    if !response.token_type.eq_ignore_ascii_case("Bearer") {
        return Err(AuthorizationError::Protocol {
            reason: "the issued token is not a bearer token",
        });
    }
    let expires_in = positive_duration(
        response.expires_in,
        "the issued access-token lifetime is not positive",
    )?;

    Ok(IssuedToken {
        access_token: response.access_token,
        expires_in,
    })
}

#[derive(Deserialize)]
struct OAuthErrorResponse {
    error: String,
}

fn positive_duration(seconds: u64, reason: &'static str) -> Result<Duration, AuthorizationError> {
    if seconds == 0 || i64::try_from(seconds).is_err() {
        Err(AuthorizationError::Protocol { reason })
    } else {
        Ok(Duration::from_secs(seconds))
    }
}

#[cfg(test)]
mod tests;
