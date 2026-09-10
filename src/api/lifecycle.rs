use std::error::Error;
use std::fmt;
use std::time::Duration;

use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderValue, InvalidHeaderValue};
use reqwest::{Method, Response, StatusCode, Url};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::http_client::{HttpClient, HttpEndpointError};
use super::http_util::{self, BufferedResponseError};
use super::problem::{
    self, ACCEPTED_MEDIA_TYPES, BAD_REQUEST, FORBIDDEN, JSON_MEDIA_TYPE, NOT_FOUND, UNAUTHORIZED,
};
use super::{UnreachableCategory, bearer_authorization, classify_reqwest_error};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MUTATION_ATTEMPTS: usize = 2;
const DELETION_LIFETIME: time::Duration = time::Duration::days(30);
const REAUTHENTICATION_REQUIRED: &str =
    "https://api.scherzo.dev/problems/reauthentication-required";
const HUMAN_OWNER_REQUIRED: &str = "https://api.scherzo.dev/problems/human-owner-required";
const TRANSITION_UNAVAILABLE: &str =
    "https://api.scherzo.dev/problems/lifecycle-transition-unavailable";
const IDEMPOTENCY_CONFLICT: &str = "https://api.scherzo.dev/problems/idempotency-conflict";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LifecycleResourceKind {
    Principal,
    Organization,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LifecycleState {
    Active,
    Suspended,
    DeletionPending,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeletionSchedule {
    pub(crate) id: String,
    pub(crate) kind: LifecycleResourceKind,
    pub(crate) state: LifecycleState,
    pub(crate) requested_at: String,
    pub(crate) deadline: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LifecycleTransition {
    pub(crate) id: String,
    pub(crate) kind: LifecycleResourceKind,
    pub(crate) state: LifecycleState,
    pub(crate) updated_at: String,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum CommonLifecycleFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    InvalidResponse,
    Unreachable(UnreachableCategory),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum RequestDeletionOutcome {
    Scheduled(DeletionSchedule),
    Common(CommonLifecycleFailure),
    NotFound,
    HumanOwnerRequired,
    TransitionUnavailable,
    IdempotencyConflict,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum CancelDeletionOutcome {
    Cancelled(LifecycleTransition),
    Common(CommonLifecycleFailure),
    ReauthenticationRequired,
    NotFound,
    TransitionUnavailable,
    IdempotencyConflict,
}

#[derive(Debug)]
pub(crate) struct LifecycleApiError {
    operation: Operation,
    kind: LifecycleApiErrorKind,
    credential_rejected: bool,
}

// Lifecycle errors retain operation-specific vocabulary and response validation rather than
// coupling deletion authorization to the broader organization API error taxonomy.
// jscpd:ignore-start
impl LifecycleApiError {
    pub(crate) fn credential_rejected(&self) -> bool {
        self.credential_rejected
    }

    pub(crate) fn invalid_response(&self) -> bool {
        matches!(&self.kind, LifecycleApiErrorKind::Protocol { .. })
    }

    fn local(operation: Operation, kind: LifecycleApiErrorKind) -> Self {
        Self {
            operation,
            kind,
            credential_rejected: false,
        }
    }

    fn protocol(operation: Operation, reason: &'static str, credential_rejected: bool) -> Self {
        Self {
            operation,
            kind: LifecycleApiErrorKind::Protocol { reason },
            credential_rejected,
        }
    }
}
// jscpd:ignore-end

#[derive(Debug)]
enum LifecycleApiErrorKind {
    Endpoint(HttpEndpointError),
    InvalidAuthorizationHeader(InvalidHeaderValue),
    InvalidIdempotencyHeader(InvalidHeaderValue),
    Protocol { reason: &'static str },
}

impl fmt::Display for LifecycleApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            LifecycleApiErrorKind::Endpoint(HttpEndpointError::Invalid) => write!(
                formatter,
                "the deployment API URL cannot form a {} endpoint",
                self.operation.name()
            ),
            LifecycleApiErrorKind::Endpoint(HttpEndpointError::InsecureHttp) => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            LifecycleApiErrorKind::InvalidAuthorizationHeader(error) => write!(
                formatter,
                "the access token cannot be represented as a bearer credential: {error}"
            ),
            LifecycleApiErrorKind::InvalidIdempotencyHeader(error) => write!(
                formatter,
                "the generated deletion request identity is not a valid header value: {error}"
            ),
            LifecycleApiErrorKind::Protocol { reason } => write!(
                formatter,
                "{} response violates the public API contract: {reason}",
                self.operation.name()
            ),
        }
    }
}

impl Error for LifecycleApiError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            LifecycleApiErrorKind::InvalidAuthorizationHeader(error)
            | LifecycleApiErrorKind::InvalidIdempotencyHeader(error) => Some(error),
            LifecycleApiErrorKind::Endpoint(_) | LifecycleApiErrorKind::Protocol { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    RequestPrincipal,
    CancelPrincipal,
    RequestOrganization,
    CancelOrganization,
}

impl Operation {
    const fn name(self) -> &'static str {
        match self {
            Self::RequestPrincipal => "account deletion request",
            Self::CancelPrincipal => "account deletion cancellation",
            Self::RequestOrganization => "organization deletion request",
            Self::CancelOrganization => "organization deletion cancellation",
        }
    }

    const fn resource_kind(self) -> LifecycleResourceKind {
        match self {
            Self::RequestPrincipal | Self::CancelPrincipal => LifecycleResourceKind::Principal,
            Self::RequestOrganization | Self::CancelOrganization => {
                LifecycleResourceKind::Organization
            }
        }
    }

    const fn success_status(self) -> StatusCode {
        match self {
            Self::RequestPrincipal | Self::RequestOrganization => StatusCode::ACCEPTED,
            Self::CancelPrincipal | Self::CancelOrganization => StatusCode::OK,
        }
    }

    const fn permits_not_found(self) -> bool {
        matches!(self, Self::RequestOrganization | Self::CancelOrganization)
    }
}

struct RequestSpec {
    operation: Operation,
    method: Method,
    endpoint: Url,
    authorization: HeaderValue,
    idempotency_key: HeaderValue,
}

type ReceivedResponse = http_util::BufferedResponse;

enum AttemptError {
    Protocol(LifecycleApiError),
    Ambiguous(UnreachableCategory),
    Unreachable(UnreachableCategory),
}

enum RequestExecution {
    Response(ReceivedResponse),
    Unreachable(UnreachableCategory),
}

pub(crate) fn request_current_principal_deletion(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    idempotency_key: &str,
) -> Result<RequestDeletionOutcome, LifecycleApiError> {
    request_deletion(
        client,
        Operation::RequestPrincipal,
        api_url,
        &["v1", "me", "deletion"],
        access_token,
        idempotency_key,
    )
}

pub(crate) fn cancel_current_principal_deletion(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    idempotency_key: &str,
) -> Result<CancelDeletionOutcome, LifecycleApiError> {
    cancel_deletion(
        client,
        Operation::CancelPrincipal,
        api_url,
        &["v1", "me", "deletion"],
        access_token,
        idempotency_key,
    )
}

pub(crate) fn request_organization_deletion(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
    idempotency_key: &str,
) -> Result<RequestDeletionOutcome, LifecycleApiError> {
    request_deletion(
        client,
        Operation::RequestOrganization,
        api_url,
        &["v1", "organizations", organization_ref, "deletion"],
        access_token,
        idempotency_key,
    )
}

pub(crate) fn cancel_organization_deletion(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
    idempotency_key: &str,
) -> Result<CancelDeletionOutcome, LifecycleApiError> {
    cancel_deletion(
        client,
        Operation::CancelOrganization,
        api_url,
        &["v1", "organizations", organization_ref, "deletion"],
        access_token,
        idempotency_key,
    )
}

fn request_deletion(
    client: &HttpClient,
    operation: Operation,
    api_url: &str,
    path: &[&str],
    access_token: &str,
    idempotency_key: &str,
) -> Result<RequestDeletionOutcome, LifecycleApiError> {
    let spec = request_spec(
        client,
        operation,
        Method::POST,
        api_url,
        path,
        access_token,
        idempotency_key,
    )?;
    match execute_request(client, &spec)? {
        RequestExecution::Response(response) => {
            decode_request_response(operation, response, idempotency_key)
        }
        RequestExecution::Unreachable(category) => Ok(RequestDeletionOutcome::Common(
            CommonLifecycleFailure::Unreachable(category),
        )),
    }
}

fn cancel_deletion(
    client: &HttpClient,
    operation: Operation,
    api_url: &str,
    path: &[&str],
    access_token: &str,
    idempotency_key: &str,
) -> Result<CancelDeletionOutcome, LifecycleApiError> {
    let spec = request_spec(
        client,
        operation,
        Method::DELETE,
        api_url,
        path,
        access_token,
        idempotency_key,
    )?;
    match execute_request(client, &spec)? {
        RequestExecution::Response(response) => {
            decode_cancel_response(operation, response, idempotency_key)
        }
        RequestExecution::Unreachable(category) => Ok(CancelDeletionOutcome::Common(
            CommonLifecycleFailure::Unreachable(category),
        )),
    }
}

fn request_spec(
    client: &HttpClient,
    operation: Operation,
    method: Method,
    api_url: &str,
    path: &[&str],
    access_token: &str,
    idempotency_key: &str,
) -> Result<RequestSpec, LifecycleApiError> {
    let endpoint = client.endpoint(api_url, path).map_err(|error| {
        LifecycleApiError::local(operation, LifecycleApiErrorKind::Endpoint(error))
    })?;
    let authorization = bearer_authorization(access_token).map_err(|error| {
        LifecycleApiError::local(
            operation,
            LifecycleApiErrorKind::InvalidAuthorizationHeader(error),
        )
    })?;
    let idempotency_key = HeaderValue::from_str(idempotency_key).map_err(|error| {
        LifecycleApiError::local(
            operation,
            LifecycleApiErrorKind::InvalidIdempotencyHeader(error),
        )
    })?;
    Ok(RequestSpec {
        operation,
        method,
        endpoint,
        authorization,
        idempotency_key,
    })
}

fn execute_request(
    client: &HttpClient,
    spec: &RequestSpec,
) -> Result<RequestExecution, LifecycleApiError> {
    let mut last_failure = UnreachableCategory::Connection;
    // Lifecycle retry distinguishes a received terminal failure from an ambiguous success-body
    // interruption, so sharing the simpler identity retry loop would weaken mutation semantics.
    // jscpd:ignore-start
    for attempt in 0..MUTATION_ATTEMPTS {
        match client.run(REQUEST_TIMEOUT, send_request(client, spec)) {
            Ok(Ok(response)) => return Ok(RequestExecution::Response(response)),
            Ok(Err(AttemptError::Protocol(error))) => return Err(error),
            Ok(Err(AttemptError::Ambiguous(category))) => last_failure = category,
            Ok(Err(AttemptError::Unreachable(category))) => {
                return Ok(RequestExecution::Unreachable(category));
            }
            Err(_) => last_failure = UnreachableCategory::Timeout,
        }
        if attempt + 1 < MUTATION_ATTEMPTS {
            crate::timing::sleep(crate::timing::short_retry_delay());
        }
    }
    // jscpd:ignore-end
    Ok(RequestExecution::Unreachable(last_failure))
}

async fn send_request(
    client: &HttpClient,
    spec: &RequestSpec,
) -> Result<ReceivedResponse, AttemptError> {
    let response = client
        .inner()
        .request(spec.method.clone(), spec.endpoint.clone())
        .timeout(REQUEST_TIMEOUT)
        .header(ACCEPT, ACCEPTED_MEDIA_TYPES)
        .header(AUTHORIZATION, spec.authorization.clone())
        .header("Idempotency-Key", spec.idempotency_key.clone())
        .send()
        .await
        .map_err(|error| {
            if error.is_builder() {
                AttemptError::Protocol(LifecycleApiError::protocol(
                    spec.operation,
                    "the request could not be constructed",
                    false,
                ))
            } else {
                AttemptError::Ambiguous(classify_reqwest_error(&error))
            }
        })?;
    receive_response(spec, response).await
}

async fn receive_response(
    spec: &RequestSpec,
    response: Response,
) -> Result<ReceivedResponse, AttemptError> {
    let status = response.status();
    if status == spec.operation.success_status()
        && response.headers().get("Idempotency-Key") != Some(&spec.idempotency_key)
    {
        return Err(AttemptError::Protocol(LifecycleApiError::protocol(
            spec.operation,
            "the successful response has a missing or mismatched Idempotency-Key header",
            false,
        )));
    }

    http_util::buffer_response(response)
        .await
        .map_err(|error| match error {
            BufferedResponseError::TooLarge { status } => {
                AttemptError::Protocol(LifecycleApiError::protocol(
                    spec.operation,
                    "the response body exceeds 1 MiB",
                    status == StatusCode::UNAUTHORIZED,
                ))
            }
            BufferedResponseError::InvalidContentType { status } => {
                AttemptError::Protocol(LifecycleApiError::protocol(
                    spec.operation,
                    "the Content-Type header is not valid text",
                    status == StatusCode::UNAUTHORIZED,
                ))
            }
            BufferedResponseError::Transport { status, .. }
                if status == StatusCode::UNAUTHORIZED =>
            {
                AttemptError::Protocol(LifecycleApiError::protocol(
                    spec.operation,
                    "the unauthorized response body could not be read",
                    true,
                ))
            }
            BufferedResponseError::Transport { status, source }
                if status == spec.operation.success_status() =>
            {
                AttemptError::Ambiguous(classify_reqwest_error(&source))
            }
            BufferedResponseError::Transport { status, source } => {
                AttemptError::Unreachable(if status.is_server_error() {
                    UnreachableCategory::Server
                } else {
                    classify_reqwest_error(&source)
                })
            }
        })
}

fn decode_request_response(
    operation: Operation,
    response: ReceivedResponse,
    expected_idempotency_key: &str,
) -> Result<RequestDeletionOutcome, LifecycleApiError> {
    match response.status {
        StatusCode::ACCEPTED => {
            require_response_idempotency_key(operation, &response, expected_idempotency_key)?;
            decode_schedule(operation, &response).map(RequestDeletionOutcome::Scheduled)
        }
        StatusCode::FORBIDDEN => {
            require_problem(operation, &response, FORBIDDEN, false)?;
            Ok(RequestDeletionOutcome::Common(
                CommonLifecycleFailure::Forbidden,
            ))
        }
        StatusCode::NOT_FOUND if operation.permits_not_found() => {
            require_problem(operation, &response, NOT_FOUND, false)?;
            Ok(RequestDeletionOutcome::NotFound)
        }
        StatusCode::CONFLICT => match decode_conflict(operation, &response)? {
            LifecycleConflict::HumanOwnerRequired
                if matches!(operation, Operation::RequestPrincipal) =>
            {
                Ok(RequestDeletionOutcome::HumanOwnerRequired)
            }
            LifecycleConflict::TransitionUnavailable => {
                Ok(RequestDeletionOutcome::TransitionUnavailable)
            }
            LifecycleConflict::IdempotencyConflict => {
                Ok(RequestDeletionOutcome::IdempotencyConflict)
            }
            LifecycleConflict::HumanOwnerRequired => Err(unrecognized_conflict(operation)),
        },
        _ => decode_common_or_invalid(operation, &response, RequestDeletionOutcome::Common),
    }
}

fn decode_cancel_response(
    operation: Operation,
    response: ReceivedResponse,
    expected_idempotency_key: &str,
) -> Result<CancelDeletionOutcome, LifecycleApiError> {
    match response.status {
        StatusCode::OK => {
            require_response_idempotency_key(operation, &response, expected_idempotency_key)?;
            decode_transition(operation, &response).map(CancelDeletionOutcome::Cancelled)
        }
        StatusCode::FORBIDDEN => {
            require_problem(operation, &response, REAUTHENTICATION_REQUIRED, false)?;
            Ok(CancelDeletionOutcome::ReauthenticationRequired)
        }
        StatusCode::NOT_FOUND if operation.permits_not_found() => {
            require_problem(operation, &response, NOT_FOUND, false)?;
            Ok(CancelDeletionOutcome::NotFound)
        }
        StatusCode::CONFLICT => match decode_conflict(operation, &response)? {
            LifecycleConflict::TransitionUnavailable => {
                Ok(CancelDeletionOutcome::TransitionUnavailable)
            }
            LifecycleConflict::IdempotencyConflict => {
                Ok(CancelDeletionOutcome::IdempotencyConflict)
            }
            LifecycleConflict::HumanOwnerRequired => Err(unrecognized_conflict(operation)),
        },
        _ => decode_common_or_invalid(operation, &response, CancelDeletionOutcome::Common),
    }
}

#[derive(Clone, Copy)]
enum LifecycleConflict {
    HumanOwnerRequired,
    TransitionUnavailable,
    IdempotencyConflict,
}

fn decode_conflict(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<LifecycleConflict, LifecycleApiError> {
    match decode_problem_type(operation, response, false)?.as_str() {
        HUMAN_OWNER_REQUIRED => Ok(LifecycleConflict::HumanOwnerRequired),
        TRANSITION_UNAVAILABLE => Ok(LifecycleConflict::TransitionUnavailable),
        IDEMPOTENCY_CONFLICT => Ok(LifecycleConflict::IdempotencyConflict),
        _ => Err(unrecognized_conflict(operation)),
    }
}

fn unrecognized_conflict(operation: Operation) -> LifecycleApiError {
    LifecycleApiError::protocol(
        operation,
        "a 409 response has an unrecognized problem type",
        false,
    )
}

fn decode_common_or_invalid<T>(
    operation: Operation,
    response: &ReceivedResponse,
    wrap: impl FnOnce(CommonLifecycleFailure) -> T,
) -> Result<T, LifecycleApiError> {
    decode_common_failure(operation, response)?.map_or_else(
        || {
            Err(LifecycleApiError::protocol(
                operation,
                "the HTTP status is not valid for this operation",
                false,
            ))
        },
        |failure| Ok(wrap(failure)),
    )
}

fn decode_common_failure(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<Option<CommonLifecycleFailure>, LifecycleApiError> {
    match response.status {
        StatusCode::BAD_REQUEST => {
            require_problem(operation, response, BAD_REQUEST, false)?;
            Ok(Some(CommonLifecycleFailure::InvalidInput))
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(operation, response, UNAUTHORIZED, true)?;
            Ok(Some(CommonLifecycleFailure::Unauthenticated))
        }
        status if status.is_server_error() => Ok(Some(CommonLifecycleFailure::Unreachable(
            UnreachableCategory::Server,
        ))),
        status if status.is_redirection() => Err(LifecycleApiError::protocol(
            operation,
            "redirect responses are not permitted",
            false,
        )),
        _ => Ok(None),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireSchedule {
    id: String,
    kind: WireKind,
    state: WireState,
    requested_at: String,
    deadline: String,
    updated_at: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireTransition {
    id: String,
    kind: WireKind,
    state: WireState,
    updated_at: String,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum WireKind {
    Principal,
    Organization,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum WireState {
    Active,
    Suspended,
    Deleted,
    DeletionPending,
}

fn decode_schedule(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<DeletionSchedule, LifecycleApiError> {
    require_media_type(operation, response, JSON_MEDIA_TYPE, false)?;
    let schedule: WireSchedule = serde_json::from_slice(&response.body).map_err(|_| {
        LifecycleApiError::protocol(operation, "the deletion schedule fields are invalid", false)
    })?;
    let kind = decode_kind(operation, schedule.kind)?;
    if schedule.state != WireState::DeletionPending || !valid_resource_id(&schedule.id, kind) {
        return Err(LifecycleApiError::protocol(
            operation,
            "the deletion schedule resource is invalid",
            false,
        ));
    }
    let requested_at = parse_timestamp(&schedule.requested_at);
    let deadline = parse_timestamp(&schedule.deadline);
    if requested_at
        .zip(deadline)
        .is_none_or(|(requested_at, deadline)| deadline - requested_at != DELETION_LIFETIME)
        || parse_timestamp(&schedule.updated_at).is_none()
    {
        return Err(LifecycleApiError::protocol(
            operation,
            "the deletion schedule timestamps are invalid",
            false,
        ));
    }
    Ok(DeletionSchedule {
        id: schedule.id,
        kind,
        state: LifecycleState::DeletionPending,
        requested_at: schedule.requested_at,
        deadline: schedule.deadline,
        updated_at: schedule.updated_at,
    })
}

fn decode_transition(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<LifecycleTransition, LifecycleApiError> {
    require_media_type(operation, response, JSON_MEDIA_TYPE, false)?;
    let transition: WireTransition = serde_json::from_slice(&response.body).map_err(|_| {
        LifecycleApiError::protocol(
            operation,
            "the lifecycle transition fields are invalid",
            false,
        )
    })?;
    let kind = decode_kind(operation, transition.kind)?;
    if !valid_resource_id(&transition.id, kind) || parse_timestamp(&transition.updated_at).is_none()
    {
        return Err(LifecycleApiError::protocol(
            operation,
            "the lifecycle transition resource is invalid",
            false,
        ));
    }
    let state = match transition.state {
        WireState::Active => LifecycleState::Active,
        WireState::Suspended if kind == LifecycleResourceKind::Organization => {
            LifecycleState::Suspended
        }
        WireState::Suspended | WireState::Deleted | WireState::DeletionPending => {
            return Err(LifecycleApiError::protocol(
                operation,
                "the lifecycle transition state is invalid for cancellation",
                false,
            ));
        }
    };
    Ok(LifecycleTransition {
        id: transition.id,
        kind,
        state,
        updated_at: transition.updated_at,
    })
}

fn decode_kind(
    operation: Operation,
    kind: WireKind,
) -> Result<LifecycleResourceKind, LifecycleApiError> {
    let kind = match kind {
        WireKind::Principal => LifecycleResourceKind::Principal,
        WireKind::Organization => LifecycleResourceKind::Organization,
    };
    if kind == operation.resource_kind() {
        Ok(kind)
    } else {
        Err(LifecycleApiError::protocol(
            operation,
            "the response resource kind does not match the request",
            false,
        ))
    }
}

fn valid_resource_id(id: &str, kind: LifecycleResourceKind) -> bool {
    let prefix = match kind {
        LifecycleResourceKind::Principal => "prn_",
        LifecycleResourceKind::Organization => "org_",
    };
    crate::public_id::valid_typed_id(id, prefix)
}

fn parse_timestamp(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).ok()
}

fn require_response_idempotency_key(
    operation: Operation,
    response: &ReceivedResponse,
    expected: &str,
) -> Result<(), LifecycleApiError> {
    if http_util::header_matches(response.idempotency_key.as_ref(), expected) {
        Ok(())
    } else {
        Err(LifecycleApiError::protocol(
            operation,
            "the successful response has a missing or mismatched Idempotency-Key header",
            false,
        ))
    }
}

fn require_media_type(
    operation: Operation,
    response: &ReceivedResponse,
    expected: &str,
    credential_rejected: bool,
) -> Result<(), LifecycleApiError> {
    http_util::require_media_type(response.content_type.as_deref(), expected)
        .map_err(|reason| LifecycleApiError::protocol(operation, reason, credential_rejected))
}

fn require_problem(
    operation: Operation,
    response: &ReceivedResponse,
    expected_type: &str,
    credential_rejected: bool,
) -> Result<(), LifecycleApiError> {
    problem::require_type(response, expected_type)
        .map_err(|reason| LifecycleApiError::protocol(operation, reason, credential_rejected))
}

fn decode_problem_type(
    operation: Operation,
    response: &ReceivedResponse,
    credential_rejected: bool,
) -> Result<String, LifecycleApiError> {
    problem::decode_type(response)
        .map_err(|reason| LifecycleApiError::protocol(operation, reason, credential_rejected))
}
