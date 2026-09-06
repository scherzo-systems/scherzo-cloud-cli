use std::fmt;
use std::time::Duration;

use reqwest::header::CONTENT_TYPE;
use reqwest::{StatusCode, Url};
use serde::Serialize;

use super::generated::apis::{self, source_connections_api};
use super::generated::models;
use super::http_client::{generated_configuration, zeroize_generated_bearer_access_token};
use super::http_util::{self, BoundedBodyError};
use super::{HttpTransportPolicy, UnreachableCategory, classify_reqwest_error, problem};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const SOURCE_CONNECTION_CONFLICT: &str =
    "https://api.scherzo.dev/problems/source-connection-conflict";

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GitHubInstallation {
    pub(crate) id: String,
    pub(crate) provider_installation_id: String,
    pub(crate) provider_account_id: String,
    pub(crate) provider_account_type: GitHubAccountType,
    pub(crate) state: GitHubInstallationState,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) enum GitHubAccountType {
    Organization,
    User,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GitHubInstallationState {
    Active,
    Disconnected,
    Revoked,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GitHubSetupSession {
    pub(crate) id: String,
    pub(crate) state: GitHubSetupState,
    pub(crate) expires_at: String,
    pub(crate) setup_url: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GitHubSetupState {
    Pending,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GitHubRepository {
    pub(crate) provider_repository_id: String,
    pub(crate) full_name: String,
    pub(crate) default_branch: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GitHubRepositoryList {
    pub(crate) installation: GitHubInstallation,
    pub(crate) items: Vec<GitHubRepository>,
}

pub(crate) struct GitHubApi {
    configuration: apis::configuration::Configuration,
}

impl GitHubApi {
    pub(crate) fn new(
        api_url: &str,
        access_token: &str,
        transport_policy: HttpTransportPolicy,
    ) -> Result<Self, GitHubApiError> {
        let parsed = Url::parse(api_url).map_err(|_| GitHubApiError::InvalidEndpoint)?;
        if !transport_policy.permits(&parsed) {
            return Err(if parsed.scheme() == "http" {
                GitHubApiError::InsecureHttp
            } else {
                GitHubApiError::InvalidEndpoint
            });
        }
        if parsed.cannot_be_a_base() || parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(GitHubApiError::InvalidEndpoint);
        }
        let configuration =
            generated_configuration(api_url, access_token, transport_policy, REQUEST_TIMEOUT)
                .map_err(GitHubApiError::BuildClient)?;
        Ok(Self { configuration })
    }

    pub(crate) fn list_installations(
        &self,
        organization: &str,
    ) -> Result<Vec<GitHubInstallation>, GitHubFailure> {
        let page =
            source_connections_api::list_git_hub_installations(&self.configuration, organization)
                .map_err(|error| classify_git_hub_error(error, false))?;
        page.items
            .into_iter()
            .map(GitHubInstallation::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| GitHubFailure::protocol(false))
    }

    pub(crate) fn begin_setup(
        &self,
        organization: &str,
    ) -> Result<GitHubSetupSession, GitHubFailure> {
        let session =
            source_connections_api::begin_git_hub_setup(&self.configuration, organization)
                .map_err(|error| classify_git_hub_error(error, false))?;
        GitHubSetupSession::try_from(session).map_err(|_| GitHubFailure::protocol(false))
    }

    pub(crate) fn complete_setup(
        &self,
        organization: &str,
        setup_session: &str,
        provider_installation_id: &str,
    ) -> Result<GitHubInstallation, GitHubFailure> {
        let request = models::CompleteGitHubSetupRequest::new(provider_installation_id.to_owned());
        let installation = replay_once_after_transport_error(true, || {
            source_connections_api::complete_git_hub_setup(
                &self.configuration,
                organization,
                setup_session,
                request.clone(),
            )
            .map_err(Box::new)
        })?;
        let installation = GitHubInstallation::try_from(installation)
            .map_err(|_| GitHubFailure::protocol(false))?;
        if installation.provider_installation_id != provider_installation_id {
            return Err(GitHubFailure::protocol(false));
        }
        Ok(installation)
    }

    pub(crate) fn disconnect_installation(
        &self,
        organization: &str,
        installation: &str,
    ) -> Result<GitHubInstallation, GitHubFailure> {
        let disconnected = replay_once_after_transport_error(true, || {
            source_connections_api::disconnect_git_hub_installation(
                &self.configuration,
                organization,
                installation,
            )
            .map_err(Box::new)
        })?;
        let disconnected = GitHubInstallation::try_from(disconnected)
            .map_err(|_| GitHubFailure::protocol(false))?;
        if disconnected.id != installation {
            return Err(GitHubFailure::protocol(false));
        }
        Ok(disconnected)
    }

    pub(crate) fn list_repositories(
        &self,
        organization: &str,
        installation: &str,
    ) -> Result<GitHubRepositoryList, GitHubFailure> {
        let endpoint = format!(
            "{}/v1/organizations/{}/github/installations/{}/repositories",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(organization),
            apis::urlencode(installation),
        );
        let mut request = self
            .configuration
            .client
            .get(endpoint)
            .header(reqwest::header::ACCEPT, problem::ACCEPTED_MEDIA_TYPES);
        if let Some(user_agent) = &self.configuration.user_agent {
            request = request.header(reqwest::header::USER_AGENT, user_agent);
        }
        let access_token = self
            .configuration
            .bearer_access_token
            .as_ref()
            .ok_or_else(|| GitHubFailure::protocol(false))?;
        let response = request
            .bearer_auth(access_token)
            .send()
            .map_err(|error| GitHubFailure::Unreachable(classify_reqwest_error(&error)))?;
        let status = response.status();
        let content_type = response.headers().get(CONTENT_TYPE).cloned();
        let body = match http_util::read_bounded_blocking_body(response) {
            Ok(body) => body,
            Err(BoundedBodyError::TooLarge) => {
                return Err(GitHubFailure::protocol(status == StatusCode::UNAUTHORIZED));
            }
            Err(BoundedBodyError::Transport(_)) if status == StatusCode::UNAUTHORIZED => {
                return Err(GitHubFailure::protocol(true));
            }
            Err(BoundedBodyError::Transport(error)) => {
                return Err(GitHubFailure::Unreachable(if status.is_server_error() {
                    UnreachableCategory::Server
                } else {
                    classify_reqwest_error(&error)
                }));
            }
        };
        if status != StatusCode::OK {
            return Err(classify_git_hub_response(status, &body, true));
        }
        let content_type = content_type
            .as_ref()
            .map(http_util::media_type)
            .transpose()
            .map_err(|_| GitHubFailure::protocol(false))?;
        if content_type.as_deref() != Some(problem::JSON_MEDIA_TYPE) {
            return Err(GitHubFailure::protocol(false));
        }
        let page: models::GitHubRepositoryList =
            serde_json::from_slice(&body).map_err(|_| GitHubFailure::protocol(false))?;
        let page =
            GitHubRepositoryList::try_from(page).map_err(|_| GitHubFailure::protocol(false))?;
        if page.installation.id != installation {
            return Err(GitHubFailure::protocol(false));
        }
        Ok(page)
    }
}

impl Drop for GitHubApi {
    fn drop(&mut self) {
        zeroize_generated_bearer_access_token(&mut self.configuration);
    }
}

fn replay_once_after_transport_error<T, E>(
    conflict_allowed: bool,
    mut operation: impl FnMut() -> Result<T, Box<apis::Error<E>>>,
) -> Result<T, GitHubFailure> {
    match operation() {
        Err(error) if matches!(error.as_ref(), apis::Error::Reqwest(_)) => {
            operation().map_err(|error| classify_git_hub_error(*error, conflict_allowed))
        }
        result => result.map_err(|error| classify_git_hub_error(*error, conflict_allowed)),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GitHubFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    NotFound,
    SourceConnectionConflict,
    Unreachable(UnreachableCategory),
    Protocol { credential_rejected: bool },
}

impl GitHubFailure {
    pub(crate) const fn credential_rejected(&self) -> bool {
        matches!(
            self,
            Self::Unauthenticated
                | Self::Protocol {
                    credential_rejected: true
                }
        )
    }

    const fn protocol(credential_rejected: bool) -> Self {
        Self::Protocol {
            credential_rejected,
        }
    }
}

fn classify_git_hub_error<T>(error: apis::Error<T>, conflict_allowed: bool) -> GitHubFailure {
    match error {
        apis::Error::Reqwest(error) => GitHubFailure::Unreachable(classify_reqwest_error(&error)),
        apis::Error::ResponseError(response) => classify_git_hub_response(
            response.status,
            response.content.as_bytes(),
            conflict_allowed,
        ),
        apis::Error::Serde(_) | apis::Error::Io(_) => GitHubFailure::protocol(false),
    }
}

fn classify_git_hub_response(
    status: StatusCode,
    body: &[u8],
    conflict_allowed: bool,
) -> GitHubFailure {
    if status.is_server_error() {
        return GitHubFailure::Unreachable(UnreachableCategory::Server);
    }
    let credential_rejected = status == StatusCode::UNAUTHORIZED;
    let Ok(decoded) = problem::decode(body, status) else {
        return GitHubFailure::protocol(credential_rejected);
    };
    match (status, decoded.r#type.as_str()) {
        (StatusCode::BAD_REQUEST, problem::BAD_REQUEST) => GitHubFailure::InvalidInput,
        (StatusCode::UNAUTHORIZED, problem::UNAUTHORIZED) => GitHubFailure::Unauthenticated,
        (StatusCode::FORBIDDEN, problem::FORBIDDEN) => GitHubFailure::Forbidden,
        (StatusCode::NOT_FOUND, problem::NOT_FOUND) => GitHubFailure::NotFound,
        (StatusCode::CONFLICT, SOURCE_CONNECTION_CONFLICT) if conflict_allowed => {
            GitHubFailure::SourceConnectionConflict
        }
        _ => GitHubFailure::protocol(credential_rejected),
    }
}

impl TryFrom<models::GitHubInstallation> for GitHubInstallation {
    type Error = &'static str;

    fn try_from(value: models::GitHubInstallation) -> Result<Self, Self::Error> {
        if !crate::public_id::valid_typed_id(&value.id, "ghi_") {
            return Err("the installation binding ID is invalid");
        }
        if !valid_provider_id(&value.provider_installation_id) {
            return Err("the provider installation ID is invalid");
        }
        if !valid_provider_id(&value.provider_account_id) {
            return Err("the provider account ID is invalid");
        }
        if value.created_at.is_empty() {
            return Err("the installation creation time is empty");
        }
        if value.updated_at.is_empty() {
            return Err("the installation update time is empty");
        }
        Ok(Self {
            id: value.id,
            provider_installation_id: value.provider_installation_id,
            provider_account_id: value.provider_account_id,
            provider_account_type: match value.provider_account_type {
                models::git_hub_installation::ProviderAccountType::Organization => {
                    GitHubAccountType::Organization
                }
                models::git_hub_installation::ProviderAccountType::User => GitHubAccountType::User,
            },
            state: match value.state {
                models::git_hub_installation::State::Active => GitHubInstallationState::Active,
                models::git_hub_installation::State::Disconnected => {
                    GitHubInstallationState::Disconnected
                }
                models::git_hub_installation::State::Revoked => GitHubInstallationState::Revoked,
            },
            created_at: value.created_at,
            updated_at: value.updated_at,
        })
    }
}

impl TryFrom<models::GitHubSetupSession> for GitHubSetupSession {
    type Error = &'static str;

    fn try_from(value: models::GitHubSetupSession) -> Result<Self, Self::Error> {
        let setup_url =
            Url::parse(&value.setup_url).map_err(|_| "the GitHub setup URL is invalid")?;
        if !crate::public_id::valid_typed_id(&value.id, "ghs_") {
            return Err("the GitHub setup session ID is invalid");
        }
        if value.expires_at.is_empty() {
            return Err("the GitHub setup expiration time is empty");
        }
        if setup_url.cannot_be_a_base()
            || !setup_url_transport_is_allowed(&setup_url)
            || setup_url.host_str().is_none()
            || !setup_url.username().is_empty()
            || setup_url.password().is_some()
            || setup_url.fragment().is_some()
        {
            return Err("the GitHub setup URL is invalid");
        }
        Ok(Self {
            id: value.id,
            state: match value.state {
                models::git_hub_setup_session::State::Pending => GitHubSetupState::Pending,
            },
            expires_at: value.expires_at,
            setup_url: value.setup_url,
        })
    }
}

impl TryFrom<models::GitHubRepositoryList> for GitHubRepositoryList {
    type Error = &'static str;

    fn try_from(value: models::GitHubRepositoryList) -> Result<Self, Self::Error> {
        Ok(Self {
            installation: GitHubInstallation::try_from(*value.installation)?,
            items: value
                .items
                .into_iter()
                .map(GitHubRepository::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<models::GitHubRepository> for GitHubRepository {
    type Error = &'static str;

    fn try_from(value: models::GitHubRepository) -> Result<Self, Self::Error> {
        if !valid_provider_id(&value.provider_repository_id) {
            return Err("the provider repository ID is invalid");
        }
        if value.full_name.is_empty() || value.full_name.chars().count() > 255 {
            return Err("the repository full name is invalid");
        }
        if value.default_branch.is_empty() || value.default_branch.chars().count() > 1024 {
            return Err("the repository default branch is invalid");
        }
        Ok(Self {
            provider_repository_id: value.provider_repository_id,
            full_name: value.full_name,
            default_branch: value.default_branch,
        })
    }
}

fn valid_provider_id(value: &str) -> bool {
    value
        .parse::<i64>()
        .is_ok_and(|parsed| parsed > 0 && parsed.to_string() == value)
}

fn setup_url_transport_is_allowed(url: &Url) -> bool {
    if url.scheme() == "https" {
        return true;
    }
    url.scheme() == "http"
        && url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        })
}

#[derive(Debug)]
pub(crate) enum GitHubApiError {
    InvalidEndpoint,
    InsecureHttp,
    BuildClient(reqwest::Error),
}

impl fmt::Display for GitHubApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                formatter,
                "the deployment API URL cannot form a GitHub connection endpoint"
            ),
            Self::InsecureHttp => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            Self::BuildClient(error) => {
                write!(formatter, "prepare GitHub connection networking: {error}")
            }
        }
    }
}
