mod models;

use std::fmt;
use std::time::{Duration, Instant};

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue, LOCATION};
use reqwest::{Method, Response, StatusCode, Url};

use super::generated::models as generated_models;
use super::http_client::{HttpClient, HttpEndpointError};
use super::http_util::{self, BoundedBodyError};
use super::problem;
use super::{UnreachableCategory, classify_reqwest_error};

pub(crate) use models::{
    MembershipRole, Organization, OrganizationMembershipDirectoryEntry, OrganizationMembershipPage,
    OrganizationState, PrincipalType,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MUTATION_ATTEMPTS: usize = 2;
const READ_ATTEMPTS: usize = 1;
const JSON_MEDIA_TYPE: &str = "application/json";
const MERGE_PATCH_MEDIA_TYPE: &str = "application/merge-patch+json";
const PROBLEM_MEDIA_TYPE: &str = "application/problem+json";
const ACCEPTED_MEDIA_TYPES: &str = "application/json, application/problem+json";

const BAD_REQUEST: &str = "https://api.scherzo.dev/problems/bad-request";
const UNAUTHORIZED: &str = "https://api.scherzo.dev/problems/unauthorized";
const FORBIDDEN: &str = "https://api.scherzo.dev/problems/forbidden";
const NOT_FOUND: &str = "https://api.scherzo.dev/problems/not-found";
const CREATION_NOT_PERMITTED: &str =
    "https://api.scherzo.dev/problems/organization-creation-not-permitted";
const SLUG_UNAVAILABLE: &str = "https://api.scherzo.dev/problems/slug-unavailable";
const QUANTITY_LIMIT_REACHED: &str = "https://api.scherzo.dev/problems/quantity-limit-reached";
const RATE_LIMITED: &str = "https://api.scherzo.dev/problems/rate-limit-exceeded";
const IDEMPOTENCY_CONFLICT: &str = "https://api.scherzo.dev/problems/idempotency-conflict";

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum CommonOrganizationFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    Unreachable(UnreachableCategory),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum CreateOrganizationOutcome {
    Created(Organization),
    Common(CommonOrganizationFailure),
    CreationNotPermitted,
    SlugUnavailable,
    QuantityLimitReached,
    RateLimited { retry_after: u64 },
    IdempotencyConflict,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum GetOrganizationOutcome {
    Found(Organization),
    Common(CommonOrganizationFailure),
    NotFound,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum UpdateOrganizationOutcome {
    Updated(Organization),
    Common(CommonOrganizationFailure),
    NotFound,
    SlugUnavailable,
    IdempotencyConflict,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ListOrganizationMembershipsOutcome {
    Listed(OrganizationMembershipPage),
    Common(CommonOrganizationFailure),
    NotFound,
}

#[derive(Debug)]
pub(crate) struct OrganizationError {
    operation: Operation,
    kind: OrganizationErrorKind,
    credential_rejected: bool,
}

impl OrganizationError {
    pub(crate) fn credential_rejected(&self) -> bool {
        self.credential_rejected
    }

    fn local(operation: Operation, kind: OrganizationErrorKind) -> Self {
        Self {
            operation,
            kind,
            credential_rejected: false,
        }
    }

    fn protocol(operation: Operation, reason: &'static str, credential_rejected: bool) -> Self {
        Self {
            operation,
            kind: OrganizationErrorKind::Protocol { reason },
            credential_rejected,
        }
    }
}

#[derive(Debug)]
enum OrganizationErrorKind {
    Endpoint(HttpEndpointError),
    InvalidAuthorizationHeader,
    InvalidIdempotencyHeader,
    SerializeRequest,
    Protocol { reason: &'static str },
}

impl fmt::Display for OrganizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            OrganizationErrorKind::Endpoint(HttpEndpointError::Invalid) => write!(
                formatter,
                "the deployment API URL cannot form an organization {} endpoint",
                self.operation.name()
            ),
            OrganizationErrorKind::Endpoint(HttpEndpointError::InsecureHttp) => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            OrganizationErrorKind::InvalidAuthorizationHeader => write!(
                formatter,
                "the stored access token cannot be represented as a bearer credential"
            ),
            OrganizationErrorKind::InvalidIdempotencyHeader => write!(
                formatter,
                "the generated organization request identity is not a valid header value"
            ),
            OrganizationErrorKind::SerializeRequest => write!(
                formatter,
                "the organization {} request could not be serialized",
                self.operation.name()
            ),
            OrganizationErrorKind::Protocol { reason } => write!(
                formatter,
                "organization {} response violates the public API contract: {reason}",
                self.operation.name()
            ),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Create,
    Get,
    Update,
    ListMemberships,
}

impl Operation {
    const fn name(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Get => "show",
            Self::Update => "update",
            Self::ListMemberships => "membership-list",
        }
    }
}

struct RequestSpec {
    operation: Operation,
    method: Method,
    endpoint: Url,
    authorization: HeaderValue,
    idempotency_key: Option<HeaderValue>,
    content_type: Option<&'static str>,
    body: Option<Vec<u8>>,
    max_attempts: usize,
}

struct ReceivedResponse {
    status: StatusCode,
    content_type: Option<String>,
    idempotency_key: Option<HeaderValue>,
    location: Option<HeaderValue>,
    retry_after: Option<HeaderValue>,
    body: Vec<u8>,
}

enum RequestExecution {
    Response(ReceivedResponse),
    Unreachable(UnreachableCategory),
}

enum AttemptError {
    Protocol(OrganizationError),
    Transport(UnreachableCategory),
}

pub(crate) fn create_organization(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    idempotency_key: &str,
    display_name: &str,
    slug: Option<&str>,
) -> Result<CreateOrganizationOutcome, OrganizationError> {
    create_organization_with_timeout(
        client,
        api_url,
        access_token,
        idempotency_key,
        display_name,
        slug,
        REQUEST_TIMEOUT,
    )
}

fn create_organization_with_timeout(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    idempotency_key: &str,
    display_name: &str,
    slug: Option<&str>,
    timeout: Duration,
) -> Result<CreateOrganizationOutcome, OrganizationError> {
    let mut request = generated_models::CreateOrganizationRequest::new(display_name.to_owned());
    request.slug = slug.map(str::to_owned);
    let body = serialize_request(Operation::Create, &request)?;
    let spec = request_spec(
        client,
        Operation::Create,
        Method::POST,
        api_url,
        &["v1", "organizations"],
        access_token,
        Some(idempotency_key),
        Some(JSON_MEDIA_TYPE),
        Some(body),
        MUTATION_ATTEMPTS,
    )?;

    match execute_request(client, &spec, timeout)? {
        RequestExecution::Response(response) => decode_create_response(response, idempotency_key),
        RequestExecution::Unreachable(category) => Ok(CreateOrganizationOutcome::Common(
            CommonOrganizationFailure::Unreachable(category),
        )),
    }
}

pub(crate) fn get_organization(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
) -> Result<GetOrganizationOutcome, OrganizationError> {
    get_organization_with_timeout(
        client,
        api_url,
        access_token,
        organization_ref,
        REQUEST_TIMEOUT,
    )
}

fn get_organization_with_timeout(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
    timeout: Duration,
) -> Result<GetOrganizationOutcome, OrganizationError> {
    let spec = request_spec(
        client,
        Operation::Get,
        Method::GET,
        api_url,
        &["v1", "organizations", organization_ref],
        access_token,
        None,
        None,
        None,
        READ_ATTEMPTS,
    )?;

    match execute_request(client, &spec, timeout)? {
        RequestExecution::Response(response) => decode_get_response(response),
        RequestExecution::Unreachable(category) => Ok(GetOrganizationOutcome::Common(
            CommonOrganizationFailure::Unreachable(category),
        )),
    }
}

pub(crate) fn update_organization(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
    idempotency_key: &str,
    display_name: Option<&str>,
    slug: Option<&str>,
) -> Result<UpdateOrganizationOutcome, OrganizationError> {
    let mut request = generated_models::UpdateOrganizationPatch::new();
    request.display_name = display_name.map(str::to_owned);
    request.slug = slug.map(str::to_owned);
    let body = serialize_request(Operation::Update, &request)?;
    let spec = request_spec(
        client,
        Operation::Update,
        Method::PATCH,
        api_url,
        &["v1", "organizations", organization_ref],
        access_token,
        Some(idempotency_key),
        Some(MERGE_PATCH_MEDIA_TYPE),
        Some(body),
        MUTATION_ATTEMPTS,
    )?;

    match execute_request(client, &spec, REQUEST_TIMEOUT)? {
        RequestExecution::Response(response) => decode_update_response(response, idempotency_key),
        RequestExecution::Unreachable(category) => Ok(UpdateOrganizationOutcome::Common(
            CommonOrganizationFailure::Unreachable(category),
        )),
    }
}

pub(crate) fn list_organization_memberships(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
    limit: Option<u16>,
    cursor: Option<&str>,
) -> Result<ListOrganizationMembershipsOutcome, OrganizationError> {
    let mut endpoint = client
        .endpoint(
            api_url,
            &["v1", "organizations", organization_ref, "memberships"],
        )
        .map_err(|error| {
            OrganizationError::local(
                Operation::ListMemberships,
                OrganizationErrorKind::Endpoint(error),
            )
        })?;
    if limit.is_some() || cursor.is_some() {
        let mut query = endpoint.query_pairs_mut();
        if let Some(limit) = limit {
            query.append_pair("limit", &limit.to_string());
        }
        if let Some(cursor) = cursor {
            query.append_pair("cursor", cursor);
        }
    }
    let spec = request_spec_for_endpoint(
        Operation::ListMemberships,
        Method::GET,
        endpoint,
        access_token,
        None,
        None,
        None,
        READ_ATTEMPTS,
    )?;

    match execute_request(client, &spec, REQUEST_TIMEOUT)? {
        RequestExecution::Response(response) => decode_list_response(response),
        RequestExecution::Unreachable(category) => Ok(ListOrganizationMembershipsOutcome::Common(
            CommonOrganizationFailure::Unreachable(category),
        )),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the arguments are the fixed HTTP properties retained for byte-equivalent retry"
)]
fn request_spec(
    client: &HttpClient,
    operation: Operation,
    method: Method,
    api_url: &str,
    path: &[&str],
    access_token: &str,
    idempotency_key: Option<&str>,
    content_type: Option<&'static str>,
    body: Option<Vec<u8>>,
    max_attempts: usize,
) -> Result<RequestSpec, OrganizationError> {
    let endpoint = client.endpoint(api_url, path).map_err(|error| {
        OrganizationError::local(operation, OrganizationErrorKind::Endpoint(error))
    })?;
    request_spec_for_endpoint(
        operation,
        method,
        endpoint,
        access_token,
        idempotency_key,
        content_type,
        body,
        max_attempts,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "the arguments are the fixed HTTP properties retained for byte-equivalent retry"
)]
fn request_spec_for_endpoint(
    operation: Operation,
    method: Method,
    endpoint: Url,
    access_token: &str,
    idempotency_key: Option<&str>,
    content_type: Option<&'static str>,
    body: Option<Vec<u8>>,
    max_attempts: usize,
) -> Result<RequestSpec, OrganizationError> {
    let authorization = HeaderValue::from_str(&format!("Bearer {access_token}")).map_err(|_| {
        OrganizationError::local(operation, OrganizationErrorKind::InvalidAuthorizationHeader)
    })?;
    let idempotency_key = idempotency_key
        .map(HeaderValue::from_str)
        .transpose()
        .map_err(|_| {
            OrganizationError::local(operation, OrganizationErrorKind::InvalidIdempotencyHeader)
        })?;

    Ok(RequestSpec {
        operation,
        method,
        endpoint,
        authorization,
        idempotency_key,
        content_type,
        body,
        max_attempts,
    })
}

fn serialize_request(
    operation: Operation,
    request: &impl serde::Serialize,
) -> Result<Vec<u8>, OrganizationError> {
    serde_json::to_vec(request)
        .map_err(|_| OrganizationError::local(operation, OrganizationErrorKind::SerializeRequest))
}

fn execute_request(
    client: &HttpClient,
    spec: &RequestSpec,
    timeout: Duration,
) -> Result<RequestExecution, OrganizationError> {
    let mut last_failure = UnreachableCategory::Connection;
    for _ in 0..spec.max_attempts {
        let started = Instant::now();
        let response = match client.run(timeout, send_request(client, spec, timeout)) {
            Ok(Ok(response)) => response,
            Ok(Err(AttemptError::Protocol(error))) => return Err(error),
            Ok(Err(AttemptError::Transport(category))) => {
                last_failure = category;
                continue;
            }
            Err(_) => {
                last_failure = UnreachableCategory::Timeout;
                continue;
            }
        };
        let status = response.status();
        let remaining = timeout.saturating_sub(started.elapsed());
        match client.run(remaining, receive_response(spec.operation, response)) {
            Ok(Ok(response)) => return Ok(RequestExecution::Response(response)),
            Ok(Err(AttemptError::Protocol(error))) => return Err(error),
            Ok(Err(AttemptError::Transport(category))) => {
                return Ok(RequestExecution::Unreachable(if status.is_server_error() {
                    UnreachableCategory::Server
                } else {
                    category
                }));
            }
            Err(_) if status == StatusCode::UNAUTHORIZED => {
                return Err(OrganizationError::protocol(
                    spec.operation,
                    "the unauthorized response body exceeded the request deadline",
                    true,
                ));
            }
            Err(_) => {
                return Ok(RequestExecution::Unreachable(if status.is_server_error() {
                    UnreachableCategory::Server
                } else {
                    UnreachableCategory::Timeout
                }));
            }
        }
    }

    Ok(RequestExecution::Unreachable(last_failure))
}

async fn send_request(
    client: &HttpClient,
    spec: &RequestSpec,
    timeout: Duration,
) -> Result<Response, AttemptError> {
    let mut request = client
        .inner()
        .request(spec.method.clone(), spec.endpoint.clone())
        .timeout(timeout)
        .header(ACCEPT, ACCEPTED_MEDIA_TYPES)
        .header(AUTHORIZATION, spec.authorization.clone());
    if let Some(idempotency_key) = &spec.idempotency_key {
        request = request.header("Idempotency-Key", idempotency_key.clone());
    }
    if let Some(content_type) = spec.content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }
    if let Some(body) = &spec.body {
        request = request.body(body.clone());
    }

    request.send().await.map_err(|error| {
        if error.is_builder() {
            AttemptError::Protocol(OrganizationError::protocol(
                spec.operation,
                "the request could not be constructed",
                false,
            ))
        } else {
            AttemptError::Transport(classify_reqwest_error(&error))
        }
    })
}

async fn receive_response(
    operation: Operation,
    response: Response,
) -> Result<ReceivedResponse, AttemptError> {
    let status = response.status();
    let credential_rejected = status == StatusCode::UNAUTHORIZED;
    let content_type = response.headers().get(CONTENT_TYPE).cloned();
    let idempotency_key = response.headers().get("Idempotency-Key").cloned();
    let location = response.headers().get(LOCATION).cloned();
    let retry_after = response.headers().get("Retry-After").cloned();
    let body = match http_util::read_bounded_body(response).await {
        Ok(body) => body,
        Err(BoundedBodyError::TooLarge) => {
            return Err(AttemptError::Protocol(OrganizationError::protocol(
                operation,
                "the response body exceeds 1 MiB",
                credential_rejected,
            )));
        }
        Err(BoundedBodyError::Transport(_)) if credential_rejected => {
            return Err(AttemptError::Protocol(OrganizationError::protocol(
                operation,
                "the unauthorized response body could not be read",
                true,
            )));
        }
        Err(BoundedBodyError::Transport(error)) => {
            return Err(AttemptError::Transport(classify_reqwest_error(&error)));
        }
    };
    let content_type = content_type
        .as_ref()
        .map(http_util::media_type)
        .transpose()
        .map_err(|()| {
            AttemptError::Protocol(OrganizationError::protocol(
                operation,
                "the Content-Type header is not valid text",
                credential_rejected,
            ))
        })?;

    Ok(ReceivedResponse {
        status,
        content_type,
        idempotency_key,
        location,
        retry_after,
        body,
    })
}

fn decode_create_response(
    response: ReceivedResponse,
    expected_idempotency_key: &str,
) -> Result<CreateOrganizationOutcome, OrganizationError> {
    match response.status {
        StatusCode::CREATED => {
            require_response_idempotency_key(
                Operation::Create,
                &response,
                expected_idempotency_key,
            )?;
            let organization = decode_organization(Operation::Create, &response)?;
            require_create_location(&response, &organization.id)?;
            Ok(CreateOrganizationOutcome::Created(organization))
        }
        StatusCode::BAD_REQUEST => {
            require_problem(Operation::Create, &response, BAD_REQUEST, false)?;
            Ok(CreateOrganizationOutcome::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(Operation::Create, &response, UNAUTHORIZED, true)?;
            Ok(CreateOrganizationOutcome::Common(
                CommonOrganizationFailure::Unauthenticated,
            ))
        }
        StatusCode::FORBIDDEN => {
            let problem_type = decode_problem_type(Operation::Create, &response, false)?;
            match problem_type.as_str() {
                CREATION_NOT_PERMITTED => Ok(CreateOrganizationOutcome::CreationNotPermitted),
                FORBIDDEN => Ok(CreateOrganizationOutcome::Common(
                    CommonOrganizationFailure::Forbidden,
                )),
                _ => Err(OrganizationError::protocol(
                    Operation::Create,
                    "a 403 response has an unrecognized problem type",
                    false,
                )),
            }
        }
        StatusCode::CONFLICT => {
            let problem_type = decode_problem_type(Operation::Create, &response, false)?;
            match problem_type.as_str() {
                SLUG_UNAVAILABLE => Ok(CreateOrganizationOutcome::SlugUnavailable),
                QUANTITY_LIMIT_REACHED => Ok(CreateOrganizationOutcome::QuantityLimitReached),
                IDEMPOTENCY_CONFLICT => Ok(CreateOrganizationOutcome::IdempotencyConflict),
                _ => Err(OrganizationError::protocol(
                    Operation::Create,
                    "a 409 response has an unrecognized problem type",
                    false,
                )),
            }
        }
        StatusCode::TOO_MANY_REQUESTS => {
            require_problem(Operation::Create, &response, RATE_LIMITED, false)?;
            let retry_after = response
                .retry_after
                .as_ref()
                .and_then(|value| value.to_str().ok())
                .filter(|value| {
                    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
                })
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| {
                    OrganizationError::protocol(
                        Operation::Create,
                        "a 429 response has an invalid Retry-After header",
                        false,
                    )
                })?;
            Ok(CreateOrganizationOutcome::RateLimited { retry_after })
        }
        status if status.is_server_error() => Ok(CreateOrganizationOutcome::Common(
            CommonOrganizationFailure::Unreachable(UnreachableCategory::Server),
        )),
        status if status.is_redirection() => Err(OrganizationError::protocol(
            Operation::Create,
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(OrganizationError::protocol(
            Operation::Create,
            "the HTTP status is not valid for this operation",
            false,
        )),
    }
}

fn decode_get_response(
    response: ReceivedResponse,
) -> Result<GetOrganizationOutcome, OrganizationError> {
    match response.status {
        StatusCode::OK => {
            decode_organization(Operation::Get, &response).map(GetOrganizationOutcome::Found)
        }
        StatusCode::BAD_REQUEST => {
            require_problem(Operation::Get, &response, BAD_REQUEST, false)?;
            Ok(GetOrganizationOutcome::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(Operation::Get, &response, UNAUTHORIZED, true)?;
            Ok(GetOrganizationOutcome::Common(
                CommonOrganizationFailure::Unauthenticated,
            ))
        }
        StatusCode::FORBIDDEN => {
            require_problem(Operation::Get, &response, FORBIDDEN, false)?;
            Ok(GetOrganizationOutcome::Common(
                CommonOrganizationFailure::Forbidden,
            ))
        }
        StatusCode::NOT_FOUND => {
            require_problem(Operation::Get, &response, NOT_FOUND, false)?;
            Ok(GetOrganizationOutcome::NotFound)
        }
        status if status.is_server_error() => Ok(GetOrganizationOutcome::Common(
            CommonOrganizationFailure::Unreachable(UnreachableCategory::Server),
        )),
        status if status.is_redirection() => Err(OrganizationError::protocol(
            Operation::Get,
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(OrganizationError::protocol(
            Operation::Get,
            "the HTTP status is not valid for this operation",
            false,
        )),
    }
}

fn decode_update_response(
    response: ReceivedResponse,
    expected_idempotency_key: &str,
) -> Result<UpdateOrganizationOutcome, OrganizationError> {
    match response.status {
        StatusCode::OK => {
            require_response_idempotency_key(
                Operation::Update,
                &response,
                expected_idempotency_key,
            )?;
            decode_organization(Operation::Update, &response)
                .map(UpdateOrganizationOutcome::Updated)
        }
        StatusCode::BAD_REQUEST => {
            require_problem(Operation::Update, &response, BAD_REQUEST, false)?;
            Ok(UpdateOrganizationOutcome::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(Operation::Update, &response, UNAUTHORIZED, true)?;
            Ok(UpdateOrganizationOutcome::Common(
                CommonOrganizationFailure::Unauthenticated,
            ))
        }
        StatusCode::FORBIDDEN => {
            require_problem(Operation::Update, &response, FORBIDDEN, false)?;
            Ok(UpdateOrganizationOutcome::Common(
                CommonOrganizationFailure::Forbidden,
            ))
        }
        StatusCode::NOT_FOUND => {
            require_problem(Operation::Update, &response, NOT_FOUND, false)?;
            Ok(UpdateOrganizationOutcome::NotFound)
        }
        StatusCode::CONFLICT => {
            let problem_type = decode_problem_type(Operation::Update, &response, false)?;
            match problem_type.as_str() {
                SLUG_UNAVAILABLE => Ok(UpdateOrganizationOutcome::SlugUnavailable),
                IDEMPOTENCY_CONFLICT => Ok(UpdateOrganizationOutcome::IdempotencyConflict),
                _ => Err(OrganizationError::protocol(
                    Operation::Update,
                    "a 409 response has an unrecognized problem type",
                    false,
                )),
            }
        }
        status if status.is_server_error() => Ok(UpdateOrganizationOutcome::Common(
            CommonOrganizationFailure::Unreachable(UnreachableCategory::Server),
        )),
        status if status.is_redirection() => Err(OrganizationError::protocol(
            Operation::Update,
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(OrganizationError::protocol(
            Operation::Update,
            "the HTTP status is not valid for this operation",
            false,
        )),
    }
}

fn decode_list_response(
    response: ReceivedResponse,
) -> Result<ListOrganizationMembershipsOutcome, OrganizationError> {
    match response.status {
        StatusCode::OK => {
            require_media_type(
                Operation::ListMemberships,
                &response,
                JSON_MEDIA_TYPE,
                false,
            )?;
            let value: serde_json::Value =
                serde_json::from_slice(&response.body).map_err(|_| {
                    OrganizationError::protocol(
                        Operation::ListMemberships,
                        "the membership-list response body is invalid",
                        false,
                    )
                })?;
            let has_null_display_name = value
                .get("items")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|items| {
                    items.iter().any(|item| {
                        item.get("displayName")
                            .is_some_and(serde_json::Value::is_null)
                    })
                });
            if value
                .get("nextCursor")
                .is_some_and(serde_json::Value::is_null)
                || has_null_display_name
            {
                return Err(OrganizationError::protocol(
                    Operation::ListMemberships,
                    "the membership-list response contains an explicit null optional field",
                    false,
                ));
            }
            let generated: generated_models::OrganizationMembershipList =
                serde_json::from_value(value).map_err(|_| {
                    OrganizationError::protocol(
                        Operation::ListMemberships,
                        "the membership-list response body is invalid",
                        false,
                    )
                })?;
            OrganizationMembershipPage::try_from(generated)
                .map(ListOrganizationMembershipsOutcome::Listed)
                .map_err(|reason| {
                    OrganizationError::protocol(Operation::ListMemberships, reason, false)
                })
        }
        StatusCode::BAD_REQUEST => {
            require_problem(Operation::ListMemberships, &response, BAD_REQUEST, false)?;
            Ok(ListOrganizationMembershipsOutcome::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(Operation::ListMemberships, &response, UNAUTHORIZED, true)?;
            Ok(ListOrganizationMembershipsOutcome::Common(
                CommonOrganizationFailure::Unauthenticated,
            ))
        }
        StatusCode::FORBIDDEN => {
            require_problem(Operation::ListMemberships, &response, FORBIDDEN, false)?;
            Ok(ListOrganizationMembershipsOutcome::Common(
                CommonOrganizationFailure::Forbidden,
            ))
        }
        StatusCode::NOT_FOUND => {
            require_problem(Operation::ListMemberships, &response, NOT_FOUND, false)?;
            Ok(ListOrganizationMembershipsOutcome::NotFound)
        }
        status if status.is_server_error() => Ok(ListOrganizationMembershipsOutcome::Common(
            CommonOrganizationFailure::Unreachable(UnreachableCategory::Server),
        )),
        status if status.is_redirection() => Err(OrganizationError::protocol(
            Operation::ListMemberships,
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(OrganizationError::protocol(
            Operation::ListMemberships,
            "the HTTP status is not valid for this operation",
            false,
        )),
    }
}

fn require_response_idempotency_key(
    operation: Operation,
    response: &ReceivedResponse,
    expected: &str,
) -> Result<(), OrganizationError> {
    if response
        .idempotency_key
        .as_ref()
        .and_then(|value| value.to_str().ok())
        == Some(expected)
    {
        Ok(())
    } else {
        Err(OrganizationError::protocol(
            operation,
            "the successful response has a missing or mismatched Idempotency-Key header",
            false,
        ))
    }
}

fn require_create_location(
    response: &ReceivedResponse,
    organization_id: &str,
) -> Result<(), OrganizationError> {
    let expected = format!("/v1/organizations/{organization_id}");
    if response
        .location
        .as_ref()
        .and_then(|value| value.to_str().ok())
        == Some(expected.as_str())
    {
        Ok(())
    } else {
        Err(OrganizationError::protocol(
            Operation::Create,
            "the successful response has a missing or mismatched Location header",
            false,
        ))
    }
}

fn decode_organization(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<Organization, OrganizationError> {
    require_media_type(operation, response, JSON_MEDIA_TYPE, false)?;
    let generated: generated_models::Organization = serde_json::from_slice(&response.body)
        .map_err(|_| {
            OrganizationError::protocol(
                operation,
                "the organization response body is invalid",
                false,
            )
        })?;
    Organization::try_from(generated)
        .map_err(|reason| OrganizationError::protocol(operation, reason, false))
}

fn require_problem(
    operation: Operation,
    response: &ReceivedResponse,
    expected_type: &'static str,
    credential_rejected: bool,
) -> Result<(), OrganizationError> {
    let actual = decode_problem_type(operation, response, credential_rejected)?;
    if actual == expected_type {
        Ok(())
    } else {
        Err(OrganizationError::protocol(
            operation,
            "the problem type is not valid for its HTTP status",
            credential_rejected,
        ))
    }
}

fn decode_problem_type(
    operation: Operation,
    response: &ReceivedResponse,
    credential_rejected: bool,
) -> Result<String, OrganizationError> {
    require_media_type(operation, response, PROBLEM_MEDIA_TYPE, credential_rejected)?;
    let decoded = problem::decode(&response.body, response.status)
        .map_err(|reason| OrganizationError::protocol(operation, reason, credential_rejected))?;
    Ok(decoded.r#type)
}

fn require_media_type(
    operation: Operation,
    response: &ReceivedResponse,
    expected: &'static str,
    credential_rejected: bool,
) -> Result<(), OrganizationError> {
    if response.content_type.as_deref() == Some(expected) {
        Ok(())
    } else {
        Err(OrganizationError::protocol(
            operation,
            "the response Content-Type is not valid for its HTTP status",
            credential_rejected,
        ))
    }
}

#[cfg(test)]
mod tests;
