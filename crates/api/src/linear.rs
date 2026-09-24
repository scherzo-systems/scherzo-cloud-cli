//! Nonsecret Linear connection projections over the generated source-connection client.
use std::fmt;
use std::time::Duration;

use reqwest::{StatusCode, Url};

use super::generated::{apis, models};
use super::http_client::{generated_configuration, zeroize_generated_bearer_access_token};
use super::http_util::{self, BoundedBodyError};
use super::{HttpTransportPolicy, UnreachableCategory, classify_reqwest_error, problem};

pub type LinearSession = models::LinearAuthorizationSession;
pub type LinearConnection = models::LinearConnection;
pub type LinearConnectionList = models::LinearConnectionList;
pub type LinearSessionStatus = models::linear_authorization_session::Status;

pub struct LinearApi {
    configuration: apis::configuration::Configuration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinearFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    NotFound,
    Conflict,
    Unreachable { category: UnreachableCategory },
    RateLimited { retry_after: Option<u64> },
    InvalidResponse { credential_rejected: bool },
}

impl LinearFailure {
    pub const fn credential_rejected(&self) -> bool {
        matches!(
            self,
            Self::Unauthenticated
                | Self::InvalidResponse {
                    credential_rejected: true
                }
        )
    }
}

impl LinearApi {
    pub fn new(
        api_url: &str,
        token: &str,
        policy: HttpTransportPolicy,
    ) -> Result<Self, LinearApiError> {
        let parsed = Url::parse(api_url).map_err(|_| LinearApiError::InvalidEndpoint)?;
        if !policy.permits(&parsed)
            || parsed.cannot_be_a_base()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(LinearApiError::InvalidEndpoint);
        }
        let configuration =
            generated_configuration(api_url, token, policy, Duration::from_secs(20))
                .map_err(LinearApiError::BuildClient)?;
        Ok(Self { configuration })
    }

    pub fn start(
        &self,
        organization: &str,
        connection_id: Option<&str>,
        key: &str,
    ) -> Result<LinearSession, LinearFailure> {
        let mut request =
            models::CreateLinearAuthorizationSessionRequest::new(match connection_id {
                Some(_) => {
                    models::create_linear_authorization_session_request::Operation::Reauthorize
                }
                None => models::create_linear_authorization_session_request::Operation::Connect,
            });
        request.connection_id = connection_id.map(str::to_owned);
        let session = apis::source_connections_api::create_linear_authorization_session(
            &self.configuration,
            organization,
            key,
            request,
        )
        .map_err(classify_error)?;
        let session = validate_session(session, organization, None)?;
        let matches_operation = match connection_id {
            Some(id) => {
                session.operation == models::linear_authorization_session::Operation::Reauthorize
                    && session.connection_id.as_deref() == Some(id)
            }
            None => {
                session.operation == models::linear_authorization_session::Operation::Connect
                    && session.connection_id.is_none()
            }
        };
        if !matches_operation {
            return Err(invalid());
        }
        Ok(session)
    }

    // The generated GET discards response headers on errors. Observe the same generated
    // model using its configured client so Retry-After survives rate-limited polling.
    pub fn session(&self, organization: &str, id: &str) -> Result<LinearSession, LinearFailure> {
        let endpoint = format!(
            "{}/v1/organizations/{}/connections/linear/authorization-sessions/{}",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(organization),
            apis::urlencode(id)
        );
        let mut request = self.configuration.client.get(endpoint);
        if let Some(token) = &self.configuration.bearer_access_token {
            request = request.bearer_auth(token);
        }
        let response = request.send().map_err(|error| LinearFailure::Unreachable {
            category: classify_reqwest_error(&error),
        })?;
        let status = response.status();
        let media_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .map(http_util::media_type)
            .transpose()
            .map_err(|_| invalid())?;
        let retry_after = response
            .headers()
            .get("Retry-After")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        let body =
            http_util::read_bounded_blocking_body(response).map_err(|error| match error {
                BoundedBodyError::TooLarge => LinearFailure::InvalidResponse {
                    credential_rejected: status == StatusCode::UNAUTHORIZED,
                },
                BoundedBodyError::Transport(error) => LinearFailure::Unreachable {
                    category: classify_reqwest_error(&error),
                },
            })?;
        if status == StatusCode::OK {
            if media_type.as_deref() != Some(problem::JSON_MEDIA_TYPE) {
                return Err(invalid());
            }
            let session =
                serde_json::from_slice(&body).map_err(|_| LinearFailure::InvalidResponse {
                    credential_rejected: false,
                })?;
            validate_session(session, organization, Some(id))
        } else {
            Err(classify_response(status, &body, retry_after))
        }
    }

    pub fn connection(
        &self,
        organization: &str,
        id: &str,
    ) -> Result<LinearConnection, LinearFailure> {
        let connection = apis::source_connections_api::get_linear_connection(
            &self.configuration,
            organization,
            id,
        )
        .map_err(classify_error)?;
        validate_connection(connection, organization, Some(id))
    }

    pub fn list(
        &self,
        organization: &str,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<LinearConnectionList, LinearFailure> {
        let page = apis::source_connections_api::list_linear_connections(
            &self.configuration,
            organization,
            limit,
            cursor,
        )
        .map_err(classify_error)?;
        for connection in &page.items {
            validate_connection(connection.clone(), organization, None)?;
        }
        Ok(page)
    }

    pub fn disconnect(
        &self,
        organization: &str,
        id: &str,
        key: &str,
    ) -> Result<LinearConnection, LinearFailure> {
        let connection = apis::source_connections_api::disconnect_linear_connection(
            &self.configuration,
            organization,
            id,
            key,
        )
        .map_err(classify_error)?;
        validate_connection(connection, organization, Some(id))
    }

    pub fn delete(&self, organization: &str, id: &str, key: &str) -> Result<(), LinearFailure> {
        apis::source_connections_api::delete_linear_connection(
            &self.configuration,
            organization,
            id,
            key,
        )
        .map_err(classify_error)
    }
}

impl Drop for LinearApi {
    fn drop(&mut self) {
        zeroize_generated_bearer_access_token(&mut self.configuration);
    }
}

fn invalid() -> LinearFailure {
    LinearFailure::InvalidResponse {
        credential_rejected: false,
    }
}

fn validate_connection(
    value: LinearConnection,
    _organization: &str,
    id: Option<&str>,
) -> Result<LinearConnection, LinearFailure> {
    if !scherzo_cloud_support::valid_typed_id(&value.id, "lcn_")
        || value.organization_id.is_empty()
        || id.is_some_and(|id| value.id != id)
        || value.lifecycle_generation < 1
        || value.created_at.is_empty()
        || value.updated_at.is_empty()
    {
        return Err(invalid());
    }
    // Organization refs may be slugs; the connection carries the canonical organization ID.
    Ok(value)
}

fn validate_session(
    value: LinearSession,
    organization: &str,
    id: Option<&str>,
) -> Result<LinearSession, LinearFailure> {
    if !scherzo_cloud_support::valid_typed_id(&value.id, "las_")
        || value.organization_id.is_empty()
        || id.is_some_and(|id| value.id != id)
        || value.created_at.is_empty()
        || value.expires_at.is_empty()
    {
        return Err(invalid());
    }
    if value.status != models::linear_authorization_session::Status::LinearSessionPending
        && value.authorization_url.is_some()
    {
        return Err(invalid());
    }
    if let Some(url) = &value.authorization_url {
        let parsed = Url::parse(url).map_err(|_| invalid())?;
        if url.chars().any(char::is_control)
            || parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(invalid());
        }
    }
    if let Some(connection) = &value.result_connection {
        validate_connection((**connection).clone(), organization, None)?;
    }
    Ok(value)
}

fn classify_error<E>(error: apis::Error<E>) -> LinearFailure {
    match error {
        apis::Error::Reqwest(error) => LinearFailure::Unreachable {
            category: classify_reqwest_error(&error),
        },
        apis::Error::ResponseError(response) => {
            classify_response(response.status, response.content.as_bytes(), None)
        }
        apis::Error::Serde(_) | apis::Error::Io(_) => invalid(),
    }
}

fn classify_response(status: StatusCode, body: &[u8], retry_after: Option<u64>) -> LinearFailure {
    if status == StatusCode::TOO_MANY_REQUESTS {
        return LinearFailure::RateLimited { retry_after };
    }
    if status.is_server_error() {
        return LinearFailure::Unreachable {
            category: UnreachableCategory::Server,
        };
    }
    let Ok(decoded) = problem::decode(body, status) else {
        return LinearFailure::InvalidResponse {
            credential_rejected: status == StatusCode::UNAUTHORIZED,
        };
    };
    match (status, decoded.r#type.as_str()) {
        (StatusCode::BAD_REQUEST, problem::BAD_REQUEST) => LinearFailure::InvalidInput,
        (StatusCode::UNAUTHORIZED, problem::UNAUTHORIZED) => LinearFailure::Unauthenticated,
        (StatusCode::FORBIDDEN, problem::FORBIDDEN) => LinearFailure::Forbidden,
        (StatusCode::NOT_FOUND, problem::NOT_FOUND) => LinearFailure::NotFound,
        (StatusCode::CONFLICT, "https://api.scherzo.dev/problems/source-connection-conflict") => {
            LinearFailure::Conflict
        }
        _ => invalid(),
    }
}

#[derive(Debug)]
pub enum LinearApiError {
    InvalidEndpoint,
    BuildClient(reqwest::Error),
}
impl fmt::Display for LinearApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                f,
                "invalid Linear connection API endpoint; check the deployment URL and HTTP policy"
            ),
            Self::BuildClient(error) => write!(f, "prepare Linear connection networking: {error}"),
        }
    }
}
impl std::error::Error for LinearApiError {}
