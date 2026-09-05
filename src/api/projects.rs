use std::fmt;
use std::time::Duration;

use reqwest::blocking::Response;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderValue, LOCATION};
use reqwest::{Method, StatusCode, Url};
use serde::de::DeserializeOwned;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::generated::apis;
use super::generated::models;
use super::http_client::generated_configuration;
use super::http_util::{self, BoundedBodyError};
use super::problem::{self, ACCEPTED_MEDIA_TYPES, JSON_MEDIA_TYPE, PROBLEM_MEDIA_TYPE};
use super::{HttpTransportPolicy, UnreachableCategory, classify_reqwest_error};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MUTATION_ATTEMPTS: usize = 2;
const BAD_REQUEST: &str = "https://api.scherzo.dev/problems/bad-request";
const UNAUTHORIZED: &str = "https://api.scherzo.dev/problems/unauthorized";
const FORBIDDEN: &str = "https://api.scherzo.dev/problems/forbidden";
const NOT_FOUND: &str = "https://api.scherzo.dev/problems/not-found";
const REPOSITORY_NOT_BOUND: &str = "https://api.scherzo.dev/problems/repository-not-bound";
const NAME_UNAVAILABLE: &str = "https://api.scherzo.dev/problems/project-name-unavailable";
const QUANTITY_LIMIT: &str = "https://api.scherzo.dev/problems/quantity-limit-reached";
const RATE_LIMIT: &str = "https://api.scherzo.dev/problems/rate-limit-exceeded";
const IDEMPOTENCY_CONFLICT: &str = "https://api.scherzo.dev/problems/idempotency-conflict";
const SOURCE_CONFLICT: &str = "https://api.scherzo.dev/problems/source-connection-conflict";

pub(crate) type Project = models::Project;
pub(crate) type ProjectList = models::ProjectList;
pub(crate) type ProjectRepository = models::ProjectRepository;
pub(crate) type ProjectReadinessBlocker = models::ProjectReadinessBlocker;
pub(crate) type GitHubInstallation = models::GitHubInstallation;
pub(crate) type GitHubInstallationList = models::GitHubInstallationList;
pub(crate) type GitHubRepository = models::GitHubRepository;
pub(crate) type GitHubRepositoryList = models::GitHubRepositoryList;
pub(crate) type OrganizationMembershipList = models::CurrentPrincipalMembershipList;

pub(crate) struct ProjectApi {
    configuration: apis::configuration::Configuration,
}

pub(crate) struct CreateProjectInput<'a> {
    pub(crate) name: &'a str,
    pub(crate) installation_id: &'a str,
    pub(crate) repository_id: &'a str,
    pub(crate) default_branch: Option<&'a str>,
    pub(crate) runner_pool_id: Option<&'a str>,
}

struct JsonRequest<'a> {
    operation: Operation,
    method: Method,
    path: &'a [&'a str],
    query: &'a [(&'a str, String)],
    idempotency_key: Option<&'a str>,
    content_type: Option<&'static str>,
    body: Option<Vec<u8>>,
}

impl ProjectApi {
    pub(crate) fn new(
        api_url: &str,
        access_token: &str,
        transport_policy: HttpTransportPolicy,
    ) -> Result<Self, ProjectApiError> {
        let parsed = Url::parse(api_url).map_err(|_| ProjectApiError::InvalidEndpoint)?;
        if !transport_policy.permits(&parsed) {
            return Err(if parsed.scheme() == "http" {
                ProjectApiError::InsecureHttp
            } else {
                ProjectApiError::InvalidEndpoint
            });
        }
        if parsed.cannot_be_a_base() || parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(ProjectApiError::InvalidEndpoint);
        }
        let configuration =
            generated_configuration(api_url, access_token, transport_policy, REQUEST_TIMEOUT)
                .map_err(ProjectApiError::BuildClient)?;
        Ok(Self { configuration })
    }

    pub(crate) fn create(
        &self,
        organization: &str,
        idempotency_key: &str,
        input: CreateProjectInput<'_>,
    ) -> Result<Project, ProjectFailure> {
        let mut repository = models::ProjectRepositorySelection::new(
            input.installation_id.to_owned(),
            input.repository_id.to_owned(),
        );
        repository.default_branch = input.default_branch.map(str::to_owned);
        let mut request = models::CreateProjectRequest::new(input.name.to_owned(), repository);
        request.runner_pool_id = input.runner_pool_id.map(str::to_owned);
        let body = serialize_request(&request)?;
        let (project, locations) = self.request_json(JsonRequest {
            operation: Operation::Create,
            method: Method::POST,
            path: &["v1", "organizations", organization, "projects"],
            query: &[],
            idempotency_key: Some(idempotency_key),
            content_type: Some(JSON_MEDIA_TYPE),
            body: Some(body),
        })?;
        let project = validate_project(project)?;
        let expected_location = format!(
            "/v1/organizations/{}/projects/{}",
            apis::urlencode(organization),
            apis::urlencode(&project.id)
        );
        require_exact_header(locations.iter(), &expected_location)?;
        Ok(project)
    }

    pub(crate) fn list(
        &self,
        organization: &str,
        limit: Option<u16>,
        cursor: Option<&str>,
    ) -> Result<ProjectList, ProjectFailure> {
        let query = pagination_query(limit, cursor);
        let (mut page, _): (ProjectList, Vec<HeaderValue>) = self.request_json(JsonRequest {
            operation: Operation::ProjectRead,
            method: Method::GET,
            path: &["v1", "organizations", organization, "projects"],
            query: &query,
            idempotency_key: None,
            content_type: None,
            body: None,
        })?;
        page.items = page
            .items
            .into_iter()
            .map(validate_project)
            .collect::<Result<_, _>>()?;
        if page.next_cursor.as_deref() == Some("") {
            Err(ProjectFailure::protocol(false))
        } else {
            Ok(page)
        }
    }

    pub(crate) fn get(
        &self,
        organization: &str,
        project_id: &str,
    ) -> Result<Project, ProjectFailure> {
        let (project, _) = self.request_json(JsonRequest {
            operation: Operation::ProjectRead,
            method: Method::GET,
            path: &["v1", "organizations", organization, "projects", project_id],
            query: &[],
            idempotency_key: None,
            content_type: None,
            body: None,
        })?;
        validate_project(project)
    }

    pub(crate) fn rename(
        &self,
        organization: &str,
        project_id: &str,
        idempotency_key: &str,
        name: &str,
    ) -> Result<Project, ProjectFailure> {
        let body = serialize_request(&models::RenameProjectPatch::new(name.to_owned()))?;
        self.project_body_mutation(
            project_id,
            Method::PATCH,
            &["v1", "organizations", organization, "projects", project_id],
            idempotency_key,
            "application/merge-patch+json",
            body,
        )
    }

    pub(crate) fn set_runner_pool(
        &self,
        organization: &str,
        project_id: &str,
        idempotency_key: &str,
        runner_pool_id: &str,
    ) -> Result<Project, ProjectFailure> {
        let body = serialize_request(&models::SetProjectRunnerPoolRequest::new(
            runner_pool_id.to_owned(),
        ))?;
        let path = project_child_path(organization, project_id, "runner-pool");
        self.project_body_mutation(
            project_id,
            Method::PUT,
            &path,
            idempotency_key,
            JSON_MEDIA_TYPE,
            body,
        )
    }

    pub(crate) fn remove_runner_pool(
        &self,
        organization: &str,
        project_id: &str,
        idempotency_key: &str,
    ) -> Result<Project, ProjectFailure> {
        let path = project_child_path(organization, project_id, "runner-pool");
        self.project_bodyless_mutation(project_id, &path, idempotency_key)
    }

    pub(crate) fn get_repository(
        &self,
        organization: &str,
        project_id: &str,
    ) -> Result<ProjectRepository, ProjectFailure> {
        let path = project_child_path(organization, project_id, "repository");
        let (repository, _) = self.request_json(JsonRequest {
            operation: Operation::RepositoryRead,
            method: Method::GET,
            path: &path,
            query: &[],
            idempotency_key: None,
            content_type: None,
            body: None,
        })?;
        validate_repository(repository)
    }

    pub(crate) fn set_repository(
        &self,
        organization: &str,
        project_id: &str,
        idempotency_key: &str,
        installation_id: &str,
        repository_id: &str,
        default_branch: Option<&str>,
    ) -> Result<Project, ProjectFailure> {
        let mut request = models::SetProjectRepositoryRequest::new(
            installation_id.to_owned(),
            repository_id.to_owned(),
        );
        request.default_branch = default_branch.map(str::to_owned);
        let body = serialize_request(&request)?;
        let path = project_child_path(organization, project_id, "repository");
        self.project_body_mutation(
            project_id,
            Method::PUT,
            &path,
            idempotency_key,
            JSON_MEDIA_TYPE,
            body,
        )
    }

    pub(crate) fn update_repository(
        &self,
        organization: &str,
        project_id: &str,
        idempotency_key: &str,
        default_branch: &str,
    ) -> Result<Project, ProjectFailure> {
        let body = serialize_request(&models::UpdateProjectRepositoryPatch::new(
            default_branch.to_owned(),
        ))?;
        let path = project_child_path(organization, project_id, "repository");
        self.project_mutation(
            project_id,
            JsonRequest {
                operation: Operation::RepositoryUpdate,
                method: Method::PATCH,
                path: &path,
                query: &[],
                idempotency_key: Some(idempotency_key),
                content_type: Some("application/merge-patch+json"),
                body: Some(body),
            },
        )
    }

    pub(crate) fn detach_repository(
        &self,
        organization: &str,
        project_id: &str,
        idempotency_key: &str,
    ) -> Result<Project, ProjectFailure> {
        let path = project_child_path(organization, project_id, "repository");
        self.project_bodyless_mutation(project_id, &path, idempotency_key)
    }

    pub(crate) fn list_installations(
        &self,
        organization: &str,
    ) -> Result<GitHubInstallationList, ProjectFailure> {
        let (list, _) = self.request_json(JsonRequest {
            operation: Operation::InstallationList,
            method: Method::GET,
            path: &[
                "v1",
                "organizations",
                organization,
                "github",
                "installations",
            ],
            query: &[],
            idempotency_key: None,
            content_type: None,
            body: None,
        })?;
        validate_installation_list(list)
    }

    pub(crate) fn list_repositories(
        &self,
        organization: &str,
        installation_id: &str,
    ) -> Result<GitHubRepositoryList, ProjectFailure> {
        let (list, _) = self.request_json(JsonRequest {
            operation: Operation::RepositoryList,
            method: Method::GET,
            path: &[
                "v1",
                "organizations",
                organization,
                "github",
                "installations",
                installation_id,
                "repositories",
            ],
            query: &[],
            idempotency_key: None,
            content_type: None,
            body: None,
        })?;
        validate_repository_list(list)
    }

    pub(crate) fn list_organization_memberships(
        &self,
        limit: Option<u16>,
        cursor: Option<&str>,
    ) -> Result<OrganizationMembershipList, ProjectFailure> {
        let query = pagination_query(limit, cursor);
        let (page, _) = self.request_json(JsonRequest {
            operation: Operation::MembershipList,
            method: Method::GET,
            path: &["v1", "me", "memberships"],
            query: &query,
            idempotency_key: None,
            content_type: None,
            body: None,
        })?;
        validate_membership_list(page)
    }

    fn project_bodyless_mutation(
        &self,
        project_id: &str,
        path: &[&str],
        idempotency_key: &str,
    ) -> Result<Project, ProjectFailure> {
        self.project_mutation(
            project_id,
            JsonRequest {
                operation: Operation::BodylessMutation,
                method: Method::DELETE,
                path,
                query: &[],
                idempotency_key: Some(idempotency_key),
                content_type: None,
                body: None,
            },
        )
    }

    fn project_body_mutation(
        &self,
        project_id: &str,
        method: Method,
        path: &[&str],
        idempotency_key: &str,
        content_type: &'static str,
        body: Vec<u8>,
    ) -> Result<Project, ProjectFailure> {
        self.project_mutation(
            project_id,
            JsonRequest {
                operation: Operation::BodyMutation,
                method,
                path,
                query: &[],
                idempotency_key: Some(idempotency_key),
                content_type: Some(content_type),
                body: Some(body),
            },
        )
    }

    fn project_mutation(
        &self,
        project_id: &str,
        request: JsonRequest<'_>,
    ) -> Result<Project, ProjectFailure> {
        let (project, _) = self.request_json(request)?;
        let project = validate_project(project)?;
        if project.id == project_id {
            Ok(project)
        } else {
            Err(ProjectFailure::protocol(false))
        }
    }

    fn request_json<T: DeserializeOwned>(
        &self,
        spec: JsonRequest<'_>,
    ) -> Result<(T, Vec<HeaderValue>), ProjectFailure> {
        let endpoint = format!(
            "{}/{}",
            self.configuration.base_path.trim_end_matches('/'),
            spec.path
                .iter()
                .map(apis::urlencode)
                .collect::<Vec<_>>()
                .join("/")
        );
        let attempts = if spec.operation.is_mutation() {
            MUTATION_ATTEMPTS
        } else {
            1
        };
        let mut last_failure = UnreachableCategory::Connection;
        for attempt in 0..attempts {
            let mut request = self
                .configuration
                .client
                .request(spec.method.clone(), &endpoint)
                .header(ACCEPT, ACCEPTED_MEDIA_TYPES);
            if !spec.query.is_empty() {
                request = request.query(spec.query);
            }
            if let Some(user_agent) = &self.configuration.user_agent {
                request = request.header(reqwest::header::USER_AGENT, user_agent);
            }
            let Some(access_token) = &self.configuration.bearer_access_token else {
                return Err(ProjectFailure::protocol(false));
            };
            request = request.bearer_auth(access_token);
            if let Some(idempotency_key) = spec.idempotency_key {
                request = request.header("Idempotency-Key", idempotency_key);
            }
            if let Some(content_type) = spec.content_type {
                request = request.header(CONTENT_TYPE, content_type);
            }
            if let Some(body) = &spec.body {
                request = request.body(body.clone());
            }
            let response = match request.send() {
                Ok(response) => response,
                Err(error) => {
                    last_failure = retry_transport(attempt, attempts, &error)?;
                    continue;
                }
            };
            let status = response.status();
            let success = status == spec.operation.success_status();
            if success && spec.operation.is_mutation() {
                require_exact_header(
                    response.headers().get_all("Idempotency-Key").iter(),
                    spec.idempotency_key
                        .ok_or_else(|| ProjectFailure::protocol(false))?,
                )?;
            }
            let content_type = response.headers().get(CONTENT_TYPE).cloned();
            let locations = response
                .headers()
                .get_all(LOCATION)
                .iter()
                .cloned()
                .collect();
            let retry_after = response
                .headers()
                .get_all("Retry-After")
                .iter()
                .cloned()
                .collect();
            match receive_response(response, status, content_type, locations, retry_after) {
                Ok(received) if success => {
                    require_json_media_type(&received, status == StatusCode::UNAUTHORIZED)?;
                    let value = serde_json::from_slice(&received.body)
                        .map_err(|_| ProjectFailure::protocol(false))?;
                    return Ok((value, received.locations));
                }
                Ok(received) => return Err(classify_project_response(&received, spec.operation)),
                Err(ReceiveError::TooLarge) => {
                    return Err(ProjectFailure::protocol(status == StatusCode::UNAUTHORIZED));
                }
                Err(ReceiveError::Transport(error)) if success && spec.operation.is_mutation() => {
                    last_failure = retry_transport(attempt, attempts, &error)?;
                    continue;
                }
                Err(ReceiveError::Transport(_)) if status == StatusCode::UNAUTHORIZED => {
                    return Err(ProjectFailure::protocol(true));
                }
                Err(ReceiveError::Transport(error)) => {
                    return Err(ProjectFailure::Unreachable(if status.is_server_error() {
                        UnreachableCategory::Server
                    } else {
                        classify_reqwest_error(&error)
                    }));
                }
            }
        }
        Err(ProjectFailure::Unreachable(last_failure))
    }
}

impl Drop for ProjectApi {
    fn drop(&mut self) {
        super::clear_generated_access_token(&mut self.configuration);
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Operation {
    MembershipList,
    InstallationList,
    RepositoryList,
    ProjectRead,
    RepositoryRead,
    Create,
    BodyMutation,
    BodylessMutation,
    RepositoryUpdate,
}

impl Operation {
    const fn is_mutation(self) -> bool {
        matches!(
            self,
            Self::Create | Self::BodyMutation | Self::BodylessMutation | Self::RepositoryUpdate
        )
    }

    const fn success_status(self) -> StatusCode {
        if matches!(self, Self::Create) {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        }
    }

    fn permits_failure(self, status: StatusCode, problem_type: &str) -> bool {
        match status {
            StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => true,
            StatusCode::NOT_FOUND => {
                !matches!(self, Self::MembershipList)
                    && (problem_type == NOT_FOUND
                        || matches!(self, Self::RepositoryRead | Self::RepositoryUpdate)
                            && problem_type == REPOSITORY_NOT_BOUND)
            }
            StatusCode::CONFLICT => matches!(
                self,
                Self::RepositoryList
                    | Self::Create
                    | Self::BodyMutation
                    | Self::BodylessMutation
                    | Self::RepositoryUpdate
            ),
            StatusCode::PAYLOAD_TOO_LARGE | StatusCode::UNSUPPORTED_MEDIA_TYPE => {
                matches!(
                    self,
                    Self::Create | Self::BodyMutation | Self::RepositoryUpdate
                )
            }
            StatusCode::TOO_MANY_REQUESTS => matches!(self, Self::Create),
            _ => false,
        }
    }
}

struct ReceivedResponse {
    status: StatusCode,
    content_type: Option<HeaderValue>,
    locations: Vec<HeaderValue>,
    retry_after: Vec<HeaderValue>,
    body: Vec<u8>,
}

enum ReceiveError {
    TooLarge,
    Transport(reqwest::Error),
}

fn receive_response(
    response: Response,
    status: StatusCode,
    content_type: Option<HeaderValue>,
    locations: Vec<HeaderValue>,
    retry_after: Vec<HeaderValue>,
) -> Result<ReceivedResponse, ReceiveError> {
    let body = http_util::read_bounded_blocking_body(response).map_err(|error| match error {
        BoundedBodyError::TooLarge => ReceiveError::TooLarge,
        BoundedBodyError::Transport(error) => ReceiveError::Transport(error),
    })?;
    Ok(ReceivedResponse {
        status,
        content_type,
        locations,
        retry_after,
        body,
    })
}

fn serialize_request(request: &impl serde::Serialize) -> Result<Vec<u8>, ProjectFailure> {
    serde_json::to_vec(request).map_err(|_| ProjectFailure::protocol(false))
}

fn project_child_path<'a>(
    organization: &'a str,
    project_id: &'a str,
    child: &'a str,
) -> [&'a str; 6] {
    [
        "v1",
        "organizations",
        organization,
        "projects",
        project_id,
        child,
    ]
}

fn pagination_query(limit: Option<u16>, cursor: Option<&str>) -> Vec<(&'static str, String)> {
    let mut query = Vec::new();
    if let Some(limit) = limit {
        query.push(("limit", limit.to_string()));
    }
    if let Some(cursor) = cursor {
        query.push(("cursor", cursor.to_owned()));
    }
    query
}

fn retry_transport(
    attempt: usize,
    attempts: usize,
    error: &reqwest::Error,
) -> Result<UnreachableCategory, ProjectFailure> {
    let category = classify_reqwest_error(error);
    if attempt + 1 < attempts
        && matches!(
            category,
            UnreachableCategory::Connection | UnreachableCategory::Timeout
        )
    {
        crate::timing::sleep(crate::timing::short_retry_delay());
        Ok(category)
    } else {
        Err(ProjectFailure::Unreachable(category))
    }
}

fn require_exact_header<'a>(
    mut values: impl Iterator<Item = &'a HeaderValue>,
    expected: &str,
) -> Result<(), ProjectFailure> {
    if values.next().and_then(|value| value.to_str().ok()) == Some(expected)
        && values.next().is_none()
    {
        Ok(())
    } else {
        Err(ProjectFailure::protocol(false))
    }
}

fn require_json_media_type(
    response: &ReceivedResponse,
    credential_rejected: bool,
) -> Result<(), ProjectFailure> {
    require_media_type(response, JSON_MEDIA_TYPE, credential_rejected)
}

fn require_media_type(
    response: &ReceivedResponse,
    expected: &str,
    credential_rejected: bool,
) -> Result<(), ProjectFailure> {
    let actual = response
        .content_type
        .as_ref()
        .map(http_util::media_type)
        .transpose()
        .map_err(|_| ProjectFailure::protocol(credential_rejected))?;
    if actual.as_deref() == Some(expected) {
        Ok(())
    } else {
        Err(ProjectFailure::protocol(credential_rejected))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProjectFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    NotFound,
    RepositoryNotBound,
    NameUnavailable,
    QuantityLimitReached,
    RateLimited { retry_after: u64 },
    IdempotencyConflict,
    SourceConflict,
    Conflict,
    Unreachable(UnreachableCategory),
    Protocol { credential_rejected: bool },
}

// Project failures carry a closed outcome set distinct from Cloud run failures even
// though both expose the same credential-rejection predicate to human sessions.
// jscpd:ignore-start
impl ProjectFailure {
    pub(crate) fn credential_rejected(&self) -> bool {
        matches!(
            self,
            Self::Unauthenticated
                | Self::Protocol {
                    credential_rejected: true
                }
        )
    }

    fn protocol(credential_rejected: bool) -> Self {
        Self::Protocol {
            credential_rejected,
        }
    }
}
// jscpd:ignore-end

fn classify_project_response(response: &ReceivedResponse, operation: Operation) -> ProjectFailure {
    let status = response.status;
    if status.is_server_error() {
        return ProjectFailure::Unreachable(UnreachableCategory::Server);
    }
    let credential_rejected = status == StatusCode::UNAUTHORIZED;
    if require_media_type(response, PROBLEM_MEDIA_TYPE, credential_rejected).is_err() {
        return ProjectFailure::protocol(credential_rejected);
    }
    let Ok(decoded) = problem::decode(&response.body, status) else {
        return ProjectFailure::protocol(credential_rejected);
    };
    if !operation.permits_failure(status, &decoded.r#type) {
        return ProjectFailure::protocol(credential_rejected);
    }
    if status == StatusCode::TOO_MANY_REQUESTS && decoded.r#type == RATE_LIMIT {
        return parse_retry_after(response).map_or_else(
            || ProjectFailure::protocol(false),
            |retry_after| ProjectFailure::RateLimited { retry_after },
        );
    }
    match (status, decoded.r#type.as_str()) {
        (StatusCode::BAD_REQUEST, BAD_REQUEST)
        | (StatusCode::PAYLOAD_TOO_LARGE, _)
        | (StatusCode::UNSUPPORTED_MEDIA_TYPE, _) => ProjectFailure::InvalidInput,
        (StatusCode::UNAUTHORIZED, UNAUTHORIZED) => ProjectFailure::Unauthenticated,
        (StatusCode::FORBIDDEN, FORBIDDEN) => ProjectFailure::Forbidden,
        (StatusCode::NOT_FOUND, NOT_FOUND) => ProjectFailure::NotFound,
        (StatusCode::NOT_FOUND, REPOSITORY_NOT_BOUND) => ProjectFailure::RepositoryNotBound,
        (StatusCode::CONFLICT, NAME_UNAVAILABLE) => ProjectFailure::NameUnavailable,
        (StatusCode::CONFLICT, QUANTITY_LIMIT) => ProjectFailure::QuantityLimitReached,
        (StatusCode::CONFLICT, IDEMPOTENCY_CONFLICT) => ProjectFailure::IdempotencyConflict,
        (StatusCode::CONFLICT, SOURCE_CONFLICT) => ProjectFailure::SourceConflict,
        (StatusCode::CONFLICT, _) => ProjectFailure::Conflict,
        _ => ProjectFailure::protocol(credential_rejected),
    }
}

fn parse_retry_after(response: &ReceivedResponse) -> Option<u64> {
    let [value] = response.retry_after.as_slice() else {
        return None;
    };
    let value = value.to_str().ok()?;
    value
        .parse::<u64>()
        .ok()
        .filter(|seconds| *seconds > 0 && seconds.to_string() == value)
}

fn validate_project(project: Project) -> Result<Project, ProjectFailure> {
    let expected_blockers = match (project.runner_pool.is_none(), project.repository.as_deref()) {
        (true, None) => vec![
            models::ProjectReadinessBlocker::RunnerPoolUnassigned,
            models::ProjectReadinessBlocker::RepositoryDetached,
        ],
        (true, Some(repository))
            if repository.availability == models::project_repository::Availability::Unavailable =>
        {
            vec![
                models::ProjectReadinessBlocker::RunnerPoolUnassigned,
                models::ProjectReadinessBlocker::RepositoryUnavailable,
            ]
        }
        (true, Some(_)) => vec![models::ProjectReadinessBlocker::RunnerPoolUnassigned],
        (false, None) => vec![models::ProjectReadinessBlocker::RepositoryDetached],
        (false, Some(repository))
            if repository.availability == models::project_repository::Availability::Unavailable =>
        {
            vec![models::ProjectReadinessBlocker::RepositoryUnavailable]
        }
        (false, Some(_)) => Vec::new(),
    };
    let timestamps_valid = OffsetDateTime::parse(&project.created_at, &Rfc3339)
        .ok()
        .zip(OffsetDateTime::parse(&project.updated_at, &Rfc3339).ok())
        .is_some_and(|(created, updated)| updated >= created);
    let pool_valid = project.runner_pool.as_deref().is_none_or(|pool| {
        crate::public_id::valid_typed_id(&pool.id, "rpl_") && valid_bounded_text(&pool.name, 1, 63)
    });
    let repository_valid = project
        .repository
        .as_deref()
        .is_none_or(valid_repository_fields);
    let valid = crate::public_id::valid_typed_id(&project.id, "prj_")
        && crate::public_id::valid_typed_id(&project.organization_id, "org_")
        && valid_name(&project.name)
        && pool_valid
        && repository_valid
        && project.execution_readiness.blockers == expected_blockers
        && project.execution_readiness.ready == expected_blockers.is_empty()
        && timestamps_valid;
    if valid {
        Ok(project)
    } else {
        Err(ProjectFailure::protocol(false))
    }
}

fn validate_repository(repository: ProjectRepository) -> Result<ProjectRepository, ProjectFailure> {
    if valid_repository_fields(&repository) {
        Ok(repository)
    } else {
        Err(ProjectFailure::protocol(false))
    }
}

fn valid_repository_fields(repository: &ProjectRepository) -> bool {
    crate::public_id::valid_typed_id(&repository.connection_id, "rpc_")
        && crate::public_id::valid_typed_id(&repository.installation_binding_id, "ghi_")
        && valid_provider_id(&repository.provider_repository_id)
        && valid_bounded_text(&repository.full_name, 1, 255)
        && valid_bounded_text(&repository.default_branch, 1, 1024)
}

fn validate_membership_list(
    list: OrganizationMembershipList,
) -> Result<OrganizationMembershipList, ProjectFailure> {
    let valid = list.next_cursor.as_deref() != Some("")
        && list.items.iter().all(|membership| {
            crate::public_id::valid_typed_id(&membership.id, "mem_")
                && crate::public_id::valid_typed_id(&membership.organization_id, "org_")
                && membership
                    .organization_display_name
                    .as_deref()
                    .is_none_or(|name| valid_bounded_text(name, 1, 200))
                && membership
                    .organization_slug
                    .as_deref()
                    .is_none_or(valid_name)
                && OffsetDateTime::parse(&membership.created_at, &Rfc3339).is_ok()
                && OffsetDateTime::parse(&membership.updated_at, &Rfc3339).is_ok()
                && membership
                    .terminal_at
                    .as_deref()
                    .is_none_or(|time| OffsetDateTime::parse(time, &Rfc3339).is_ok())
        });
    if valid {
        Ok(list)
    } else {
        Err(ProjectFailure::protocol(false))
    }
}

fn validate_installation_list(
    list: GitHubInstallationList,
) -> Result<GitHubInstallationList, ProjectFailure> {
    if list.items.iter().all(valid_installation) {
        Ok(list)
    } else {
        Err(ProjectFailure::protocol(false))
    }
}

fn validate_repository_list(
    list: GitHubRepositoryList,
) -> Result<GitHubRepositoryList, ProjectFailure> {
    let valid = valid_installation(&list.installation)
        && list.items.iter().all(|repository| {
            valid_provider_id(&repository.provider_repository_id)
                && valid_bounded_text(&repository.full_name, 1, 255)
                && valid_bounded_text(&repository.default_branch, 1, 1024)
        });
    if valid {
        Ok(list)
    } else {
        Err(ProjectFailure::protocol(false))
    }
}

fn valid_installation(installation: &models::GitHubInstallation) -> bool {
    crate::public_id::valid_typed_id(&installation.id, "ghi_")
        && valid_provider_id(&installation.provider_installation_id)
        && valid_provider_id(&installation.provider_account_id)
        && OffsetDateTime::parse(&installation.created_at, &Rfc3339).is_ok()
        && OffsetDateTime::parse(&installation.updated_at, &Rfc3339).is_ok()
}

fn valid_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=63).contains(&bytes.len())
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

fn valid_provider_id(value: &str) -> bool {
    value
        .parse::<i64>()
        .is_ok_and(|identifier| identifier.is_positive() && identifier.to_string() == value)
}

fn valid_bounded_text(value: &str, minimum: usize, maximum: usize) -> bool {
    let length = value.chars().count();
    (minimum..=maximum).contains(&length)
}

#[derive(Debug)]
pub(crate) enum ProjectApiError {
    InvalidEndpoint,
    InsecureHttp,
    BuildClient(reqwest::Error),
}

impl fmt::Display for ProjectApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                formatter,
                "the deployment API URL cannot form a project management endpoint"
            ),
            Self::InsecureHttp => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            Self::BuildClient(error) => {
                write!(formatter, "prepare project management networking: {error}")
            }
        }
    }
}
