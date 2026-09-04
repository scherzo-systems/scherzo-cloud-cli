use std::fmt;
use std::time::Duration;

use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, TRANSFER_ENCODING};
use reqwest::{StatusCode, Url};
use zeroize::Zeroize as _;

use super::generated::apis::{self, runners_api};
use super::generated::models;
use super::http_client::generated_configuration;
use super::{HttpTransportPolicy, UnreachableCategory, classify_reqwest_error, problem};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const BAD_REQUEST: &str = "https://api.scherzo.dev/problems/bad-request";
const UNAUTHORIZED: &str = "https://api.scherzo.dev/problems/unauthorized";
const FORBIDDEN: &str = "https://api.scherzo.dev/problems/forbidden";
const NOT_FOUND: &str = "https://api.scherzo.dev/problems/not-found";
const NAME_UNAVAILABLE: &str = "https://api.scherzo.dev/problems/runner-name-unavailable";
const QUANTITY_LIMIT: &str = "https://api.scherzo.dev/problems/quantity-limit-reached";
const RATE_LIMIT: &str = "https://api.scherzo.dev/problems/rate-limit-exceeded";
const IDEMPOTENCY_CONFLICT: &str = "https://api.scherzo.dev/problems/idempotency-conflict";
const CREDENTIAL_LIMIT: &str = "https://api.scherzo.dev/problems/runner-credential-limit-reached";
const ACTIVATION_UNAVAILABLE: &str =
    "https://api.scherzo.dev/problems/runner-activation-unavailable";
const CREDENTIAL_TRANSITION_UNAVAILABLE: &str =
    "https://api.scherzo.dev/problems/runner-credential-transition-unavailable";
const POOL_MOVE_UNAVAILABLE: &str = "https://api.scherzo.dev/problems/runner-pool-move-unavailable";

pub(crate) type RunnerPool = models::RunnerPool;
pub(crate) type RunnerPoolList = models::RunnerPoolList;
pub(crate) type RunnerRegistration = models::RunnerRegistration;
pub(crate) type RunnerRegistrationList = models::RunnerRegistrationList;
pub(crate) type RunnerActivation = models::RunnerActivation;
pub(crate) type RunnerActivationList = models::RunnerActivationList;
pub(crate) type RunnerActivationState = models::runner_activation::State;
pub(crate) type RunnerActivationIssuance = models::RunnerActivationIssuance;
pub(crate) type RunnerCredential = models::RunnerCredential;
pub(crate) type RunnerCredentialList = models::RunnerCredentialList;
pub(crate) type RunnerCredentialStoredState = models::runner_credential::StoredState;
pub(crate) type RunnerCredentialEffectiveState = models::runner_credential::EffectiveState;
pub(crate) type RunnerDeletionBlocker = models::ProblemBlocker;

#[derive(Clone, Copy)]
pub(crate) enum RunnerRegistrationMode {
    Enabled,
    Draining,
    Disabled,
}

pub(crate) struct RunnerApi {
    configuration: apis::configuration::Configuration,
}

impl RunnerApi {
    pub(crate) fn new(
        api_url: &str,
        access_token: &str,
        transport_policy: HttpTransportPolicy,
    ) -> Result<Self, RunnerApiError> {
        let parsed = Url::parse(api_url).map_err(|_| RunnerApiError::InvalidEndpoint)?;
        if !transport_policy.permits(&parsed) {
            return Err(if parsed.scheme() == "http" {
                RunnerApiError::InsecureHttp
            } else {
                RunnerApiError::InvalidEndpoint
            });
        }
        if parsed.cannot_be_a_base() || parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(RunnerApiError::InvalidEndpoint);
        }
        let configuration =
            generated_configuration(api_url, access_token, transport_policy, REQUEST_TIMEOUT)
                .map_err(RunnerApiError::BuildClient)?;
        Ok(Self { configuration })
    }

    pub(crate) fn create_pool(
        &self,
        organization: &str,
        idempotency_key: &str,
        name: &str,
    ) -> Result<RunnerPool, RunnerFailure> {
        runners_api::create_runner_pool(
            &self.configuration,
            organization,
            idempotency_key,
            models::CreateRunnerPoolRequest::new(name.to_owned()),
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn list_pools(
        &self,
        organization: &str,
        limit: Option<u16>,
        cursor: Option<&str>,
    ) -> Result<RunnerPoolList, RunnerFailure> {
        runners_api::list_runner_pools(
            &self.configuration,
            organization,
            limit.map(i32::from),
            cursor,
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn get_pool(
        &self,
        organization: &str,
        pool_ref: &str,
    ) -> Result<RunnerPool, RunnerFailure> {
        find_named_resource(
            pool_ref,
            "rpl_",
            || {
                runners_api::get_runner_pool(&self.configuration, organization, pool_ref)
                    .map_err(classify_runner_error)
            },
            |cursor| {
                let page = self.list_pools(organization, Some(200), cursor)?;
                Ok((page.items, page.next_cursor))
            },
            |pool| pool.name.as_str(),
        )
    }

    pub(crate) fn rename_pool(
        &self,
        organization: &str,
        pool_ref: &str,
        idempotency_key: &str,
        name: &str,
    ) -> Result<RunnerPool, RunnerFailure> {
        let pool = self.get_pool(organization, pool_ref)?;
        runners_api::rename_runner_pool(
            &self.configuration,
            organization,
            &pool.id,
            idempotency_key,
            models::RenameRunnerPoolPatch::new(name.to_owned()),
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn delete_pool(
        &self,
        organization: &str,
        pool_id: &str,
        idempotency_key: &str,
    ) -> Result<(), RunnerFailure> {
        self.delete_resource(
            organization,
            pool_id,
            idempotency_key,
            RunnerDeletionKind::Pool,
        )
    }

    pub(crate) fn create_registration(
        &self,
        organization: &str,
        idempotency_key: &str,
        pool_id: &str,
        name: Option<&str>,
    ) -> Result<RunnerRegistration, RunnerFailure> {
        let mut request = models::CreateRunnerRegistrationRequest::new(pool_id.to_owned());
        request.name = name.map(ToOwned::to_owned);
        runners_api::create_runner_registration(
            &self.configuration,
            organization,
            idempotency_key,
            request,
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn create_activation(
        &self,
        organization: &str,
        runner_id: &str,
        idempotency_key: &str,
    ) -> Result<RunnerActivationIssuance, RunnerFailure> {
        runners_api::create_runner_activation(
            &self.configuration,
            organization,
            runner_id,
            idempotency_key,
            models::CreateRunnerActivationRequest::new(),
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn list_activations(
        &self,
        organization: &str,
        runner_id: &str,
        limit: Option<u16>,
        cursor: Option<&str>,
    ) -> Result<RunnerActivationList, RunnerFailure> {
        runners_api::list_runner_activations(
            &self.configuration,
            organization,
            runner_id,
            limit.map(i32::from),
            cursor,
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn revoke_activation(
        &self,
        organization: &str,
        runner_id: &str,
        activation_id: &str,
        idempotency_key: &str,
    ) -> Result<RunnerActivation, RunnerFailure> {
        runners_api::revoke_runner_activation(
            &self.configuration,
            organization,
            runner_id,
            activation_id,
            idempotency_key,
            models::RevokeRunnerActivationRequest::new(),
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn list_credentials(
        &self,
        organization: &str,
        runner_id: &str,
        limit: Option<u16>,
        cursor: Option<&str>,
    ) -> Result<RunnerCredentialList, RunnerFailure> {
        runners_api::list_runner_credentials(
            &self.configuration,
            organization,
            runner_id,
            limit.map(i32::from),
            cursor,
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn retire_credential(
        &self,
        organization: &str,
        runner_id: &str,
        credential_id: &str,
        idempotency_key: &str,
    ) -> Result<RunnerCredential, RunnerFailure> {
        runners_api::retire_runner_credential(
            &self.configuration,
            organization,
            runner_id,
            credential_id,
            idempotency_key,
            models::RetireRunnerCredentialRequest::new(),
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn revoke_credential(
        &self,
        organization: &str,
        runner_id: &str,
        credential_id: &str,
        idempotency_key: &str,
    ) -> Result<RunnerCredential, RunnerFailure> {
        runners_api::revoke_runner_credential(
            &self.configuration,
            organization,
            runner_id,
            credential_id,
            idempotency_key,
            models::RevokeRunnerCredentialRequest::new(),
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn list_registrations(
        &self,
        organization: &str,
        limit: Option<u16>,
        cursor: Option<&str>,
    ) -> Result<RunnerRegistrationList, RunnerFailure> {
        runners_api::list_runner_registrations(
            &self.configuration,
            organization,
            limit.map(i32::from),
            cursor,
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn get_registration(
        &self,
        organization: &str,
        runner_ref: &str,
    ) -> Result<RunnerRegistration, RunnerFailure> {
        find_named_resource(
            runner_ref,
            "rnr_",
            || {
                runners_api::get_runner_registration(&self.configuration, organization, runner_ref)
                    .map_err(classify_runner_error)
            },
            |cursor| {
                let page = self.list_registrations(organization, Some(200), cursor)?;
                Ok((page.items, page.next_cursor))
            },
            |runner| runner.name.as_str(),
        )
    }

    pub(crate) fn rename_registration(
        &self,
        organization: &str,
        runner_ref: &str,
        idempotency_key: &str,
        name: &str,
    ) -> Result<RunnerRegistration, RunnerFailure> {
        let runner = self.get_registration(organization, runner_ref)?;
        runners_api::rename_runner_registration(
            &self.configuration,
            organization,
            &runner.id,
            idempotency_key,
            models::RenameRunnerRegistrationPatch::new(name.to_owned()),
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn delete_registration(
        &self,
        organization: &str,
        runner_id: &str,
        idempotency_key: &str,
    ) -> Result<(), RunnerFailure> {
        self.delete_resource(
            organization,
            runner_id,
            idempotency_key,
            RunnerDeletionKind::Registration,
        )
    }

    pub(crate) fn update_registration_mode(
        &self,
        organization: &str,
        runner_ref: &str,
        idempotency_key: &str,
        mode: RunnerRegistrationMode,
    ) -> Result<RunnerRegistration, RunnerFailure> {
        let runner = self.get_registration(organization, runner_ref)?;
        let mode = match mode {
            RunnerRegistrationMode::Enabled => {
                models::update_runner_registration_mode_request::Mode::Enabled
            }
            RunnerRegistrationMode::Draining => {
                models::update_runner_registration_mode_request::Mode::Draining
            }
            RunnerRegistrationMode::Disabled => {
                models::update_runner_registration_mode_request::Mode::Disabled
            }
        };
        runners_api::update_runner_registration_mode(
            &self.configuration,
            organization,
            &runner.id,
            idempotency_key,
            models::UpdateRunnerRegistrationModeRequest::new(mode),
        )
        .map_err(classify_runner_error)
    }

    pub(crate) fn move_registration(
        &self,
        organization: &str,
        runner_ref: &str,
        pool_ref: &str,
        idempotency_key: &str,
    ) -> Result<RunnerRegistration, RunnerFailure> {
        let runner = self.get_registration(organization, runner_ref)?;
        let pool = self.get_pool(organization, pool_ref)?;
        runners_api::move_runner_registration(
            &self.configuration,
            organization,
            &runner.id,
            idempotency_key,
            models::MoveRunnerRegistrationRequest::new(pool.id),
        )
        .map_err(classify_runner_error)
    }

    fn delete_resource(
        &self,
        organization: &str,
        resource_id: &str,
        idempotency_key: &str,
        kind: RunnerDeletionKind,
    ) -> Result<(), RunnerFailure> {
        let resource_segment = match kind {
            RunnerDeletionKind::Pool => "runner-pools",
            RunnerDeletionKind::Registration => "runner-registrations",
        };
        let endpoint = format!(
            "{}/v1/organizations/{}/{}/{}",
            self.configuration.base_path,
            apis::urlencode(organization),
            resource_segment,
            apis::urlencode(resource_id),
        );
        let mut request = self
            .configuration
            .client
            .delete(endpoint)
            .header("Idempotency-Key", idempotency_key);
        if let Some(user_agent) = &self.configuration.user_agent {
            request = request.header(reqwest::header::USER_AGENT, user_agent);
        }
        let token = self
            .configuration
            .bearer_access_token
            .as_ref()
            .ok_or(RunnerFailure::Protocol)?;
        let response = request
            .bearer_auth(token)
            .send()
            .map_err(|error| RunnerFailure::Unreachable(classify_reqwest_error(&error)))?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .map_err(|error| RunnerFailure::Unreachable(classify_reqwest_error(&error)))?;
        if status == StatusCode::NO_CONTENT {
            return strict_deletion_success(&headers, &body, idempotency_key);
        }
        if status.is_success() {
            return Err(RunnerFailure::Protocol);
        }
        Err(classify_deletion_response(status, &body, kind))
    }
}

#[derive(Clone, Copy)]
enum RunnerDeletionKind {
    Pool,
    Registration,
}

fn strict_deletion_success(
    headers: &HeaderMap,
    body: &[u8],
    idempotency_key: &str,
) -> Result<(), RunnerFailure> {
    let echoed_keys = headers
        .get_all("Idempotency-Key")
        .iter()
        .collect::<Vec<_>>();
    let content_lengths = headers.get_all(CONTENT_LENGTH).iter().collect::<Vec<_>>();
    let content_length_valid = content_lengths.is_empty()
        || (content_lengths.len() == 1 && content_lengths[0].as_bytes() == b"0");
    if !body.is_empty()
        || echoed_keys.len() != 1
        || echoed_keys[0].as_bytes() != idempotency_key.as_bytes()
        || !content_length_valid
        || headers.contains_key(CONTENT_TYPE)
        || headers.contains_key(TRANSFER_ENCODING)
    {
        return Err(RunnerFailure::Protocol);
    }
    Ok(())
}

fn classify_deletion_response(
    status: StatusCode,
    body: &[u8],
    kind: RunnerDeletionKind,
) -> RunnerFailure {
    if status == StatusCode::CONFLICT
        && let Ok(decoded) = problem::decode(body, status)
    {
        let expected_type = match kind {
            RunnerDeletionKind::Pool => {
                "https://api.scherzo.dev/problems/runner-pool-delete-unavailable"
            }
            RunnerDeletionKind::Registration => {
                "https://api.scherzo.dev/problems/runner-registration-delete-unavailable"
            }
        };
        if decoded.r#type == expected_type {
            let blockers = decoded.blockers.unwrap_or_default();
            return if valid_deletion_blockers(kind, &blockers) {
                RunnerFailure::DeletionUnavailable(blockers)
            } else {
                RunnerFailure::Protocol
            };
        }
    }
    classify_runner_response(status, body)
}

fn valid_deletion_blockers(kind: RunnerDeletionKind, blockers: &[RunnerDeletionBlocker]) -> bool {
    let allowed: &[RunnerDeletionBlocker] = match kind {
        RunnerDeletionKind::Pool => &[
            RunnerDeletionBlocker::RunnerRegistrationsPresent,
            RunnerDeletionBlocker::ProjectAssignmentsPresent,
            RunnerDeletionBlocker::NonterminalRunsPresent,
        ],
        RunnerDeletionKind::Registration => &[
            RunnerDeletionBlocker::CapacityReserved,
            RunnerDeletionBlocker::NonterminalAssignment,
        ],
    };
    if blockers.is_empty() {
        return false;
    }
    let mut next = 0;
    for blocker in blockers {
        let Some(offset) = allowed[next..]
            .iter()
            .position(|allowed| allowed == blocker)
        else {
            return false;
        };
        next += offset + 1;
    }
    true
}

fn find_named_resource<T>(
    resource_ref: &str,
    id_prefix: &str,
    get_by_id: impl FnOnce() -> Result<T, RunnerFailure>,
    mut list_page: impl FnMut(Option<&str>) -> Result<(Vec<T>, Option<String>), RunnerFailure>,
    name: impl Fn(&T) -> &str,
) -> Result<T, RunnerFailure> {
    if resource_ref.starts_with(id_prefix) {
        return get_by_id();
    }
    let mut cursor = None;
    loop {
        let (items, next_cursor) = list_page(cursor.as_deref())?;
        if let Some(resource) = items
            .into_iter()
            .find(|resource| name(resource) == resource_ref)
        {
            return Ok(resource);
        }
        let Some(next) = next_cursor else {
            return Err(RunnerFailure::NotFound);
        };
        if cursor.as_deref() == Some(next.as_str()) {
            return Err(RunnerFailure::Protocol);
        }
        cursor = Some(next);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RunnerFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    NotFound,
    NameUnavailable,
    QuantityLimitReached,
    RateLimited,
    IdempotencyConflict,
    CredentialLimit,
    ActivationUnavailable,
    CredentialTransitionUnavailable,
    PoolMoveUnavailable,
    DeletionUnavailable(Vec<RunnerDeletionBlocker>),
    Unreachable(UnreachableCategory),
    Protocol,
}

impl RunnerFailure {
    pub(crate) fn credential_rejected(&self) -> bool {
        self == &Self::Unauthenticated
    }
}

impl Drop for RunnerApi {
    fn drop(&mut self) {
        if let Some(access_token) = &mut self.configuration.bearer_access_token {
            access_token.zeroize();
        }
    }
}

fn classify_runner_error<T>(error: apis::Error<T>) -> RunnerFailure {
    match error {
        apis::Error::Reqwest(error) => RunnerFailure::Unreachable(classify_reqwest_error(&error)),
        apis::Error::ResponseError(response) => {
            classify_runner_response(response.status, response.content.as_bytes())
        }
        apis::Error::Serde(_) | apis::Error::Io(_) => RunnerFailure::Protocol,
    }
}

fn classify_runner_response(status: StatusCode, body: &[u8]) -> RunnerFailure {
    if status.is_server_error() {
        return RunnerFailure::Unreachable(UnreachableCategory::Server);
    }
    let Ok(decoded) = problem::decode(body, status) else {
        return RunnerFailure::Protocol;
    };
    match (status, decoded.r#type.as_str()) {
        (StatusCode::BAD_REQUEST, BAD_REQUEST) => RunnerFailure::InvalidInput,
        (StatusCode::UNAUTHORIZED, UNAUTHORIZED) => RunnerFailure::Unauthenticated,
        (StatusCode::FORBIDDEN, FORBIDDEN) => RunnerFailure::Forbidden,
        (StatusCode::NOT_FOUND, NOT_FOUND) => RunnerFailure::NotFound,
        (StatusCode::CONFLICT, NAME_UNAVAILABLE) => RunnerFailure::NameUnavailable,
        (StatusCode::CONFLICT, QUANTITY_LIMIT) => RunnerFailure::QuantityLimitReached,
        (StatusCode::CONFLICT, IDEMPOTENCY_CONFLICT) => RunnerFailure::IdempotencyConflict,
        (StatusCode::CONFLICT, CREDENTIAL_LIMIT) => RunnerFailure::CredentialLimit,
        (StatusCode::CONFLICT, ACTIVATION_UNAVAILABLE) => RunnerFailure::ActivationUnavailable,
        (StatusCode::CONFLICT, CREDENTIAL_TRANSITION_UNAVAILABLE) => {
            RunnerFailure::CredentialTransitionUnavailable
        }
        (StatusCode::CONFLICT, POOL_MOVE_UNAVAILABLE) => RunnerFailure::PoolMoveUnavailable,
        (StatusCode::TOO_MANY_REQUESTS, RATE_LIMIT) => RunnerFailure::RateLimited,
        _ => RunnerFailure::Protocol,
    }
}

#[derive(Debug)]
pub(crate) enum RunnerApiError {
    InvalidEndpoint,
    InsecureHttp,
    BuildClient(reqwest::Error),
}

impl fmt::Display for RunnerApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                formatter,
                "the deployment API URL cannot form a runner administration endpoint"
            ),
            Self::InsecureHttp => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            Self::BuildClient(error) => write!(
                formatter,
                "prepare runner administration networking: {error}"
            ),
        }
    }
}

#[cfg(test)]
mod tests;
