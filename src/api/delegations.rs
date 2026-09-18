use std::error::Error;
use std::fmt;
use std::time::Duration;

// Delegation transport retains a domain-local protocol vocabulary; sharing imports would not
// create a concept that can evolve independently of the operation-specific response decoder.
// jscpd:ignore-start
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use reqwest::{Method, StatusCode, Url};
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use zeroize::Zeroizing;

use super::generated::models as generated_models;
use super::http_client::{HttpClient, HttpEndpointError};
use super::http_util::{self, ApiAttemptError, BufferedResponse};
use super::problem::{
    self, ACCEPTED_MEDIA_TYPES, BAD_REQUEST, FORBIDDEN, JSON_MEDIA_TYPE, NOT_FOUND, UNAUTHORIZED,
};
use super::{UnreachableCategory, bearer_authorization, classify_reqwest_error};
// jscpd:ignore-end

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const READ_ATTEMPTS: usize = 1;
const MUTATION_ATTEMPTS: usize = 2;
const DELEGATION_TRANSITION_UNAVAILABLE: &str =
    "https://api.scherzo.dev/problems/delegation-transition-unavailable";
const IDEMPOTENCY_CONFLICT: &str = "https://api.scherzo.dev/problems/idempotency-conflict";
const REQUEST_BODY_TOO_LARGE: &str = "https://api.scherzo.dev/problems/request-body-too-large";
const UNSUPPORTED_MEDIA_TYPE: &str = "https://api.scherzo.dev/problems/unsupported-media-type";
const RETRYABLE_CONFLICT: &str = "https://api.scherzo.dev/problems/retryable-conflict";

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Delegation {
    pub(crate) id: String,
    pub(crate) human_principal_id: String,
    pub(crate) service_principal_id: String,
    pub(crate) state: DelegationState,
    pub(crate) proposed_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) accepted_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) ended_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) terminal_reason: Option<DelegationTerminalReason>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DelegationState {
    Pending,
    Active,
    Ended,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DelegationTerminalReason {
    ParticipantEnded,
    ParticipantDeleted,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DelegationPage {
    pub(crate) items: Vec<Delegation>,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum CommonDelegationFailure {
    InvalidInput,
    Unauthenticated,
    Forbidden,
    Unreachable(UnreachableCategory),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ListDelegationsOutcome {
    Listed(DelegationPage),
    Common(CommonDelegationFailure),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum GetDelegationOutcome {
    Found(Delegation),
    Common(CommonDelegationFailure),
    NotFound,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ProposeDelegationOutcome {
    Proposed(Delegation),
    Common(CommonDelegationFailure),
    TransitionUnavailable,
    IdempotencyConflict,
    RetryableConflict { retry_after: u64 },
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum AcceptDelegationOutcome {
    Accepted(Delegation),
    Common(CommonDelegationFailure),
    NotFound,
    TransitionUnavailable,
    IdempotencyConflict,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum EndDelegationOutcome {
    Ended,
    Common(CommonDelegationFailure),
    NotFound,
    TransitionUnavailable,
    IdempotencyConflict,
}

#[derive(Debug)]
pub(crate) struct DelegationApiError {
    operation: Operation,
    kind: DelegationApiErrorKind,
    credential_rejected: bool,
}

// Delegation errors keep their own operation names and credential-rejection signal rather than
// coupling this authorization surface to another API domain's error type.
// jscpd:ignore-start
impl DelegationApiError {
    pub(crate) fn credential_rejected(&self) -> bool {
        self.credential_rejected
    }

    fn local(operation: Operation, kind: DelegationApiErrorKind) -> Self {
        Self {
            operation,
            kind,
            credential_rejected: false,
        }
    }

    fn protocol(operation: Operation, reason: &'static str, credential_rejected: bool) -> Self {
        Self {
            operation,
            kind: DelegationApiErrorKind::Protocol { reason },
            credential_rejected,
        }
    }
}
// jscpd:ignore-end

// Protocol diagnostics name delegation-specific actions, so a separate error kind and renderer
// are clearer than a generic API error that would erase the failed operation.
// jscpd:ignore-start
#[derive(Debug)]
enum DelegationApiErrorKind {
    Endpoint(HttpEndpointError),
    InvalidAuthorizationHeader(reqwest::header::InvalidHeaderValue),
    InvalidIdempotencyHeader(reqwest::header::InvalidHeaderValue),
    SerializeRequest(serde_json::Error),
    BuildRequest(reqwest::Error),
    DecodeResponse {
        stage: DelegationDecodeStage,
        source: serde_json::Error,
    },
    InvalidTimestamp {
        field: DelegationTimestampField,
        source: time::error::Parse,
    },
    Protocol {
        reason: &'static str,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DelegationDecodeStage {
    ListDocument,
    ListFields,
    DelegationDocument,
    DelegationFields,
}

impl DelegationDecodeStage {
    const fn failure_reason(self) -> &'static str {
        match self {
            Self::ListDocument => "the delegation-list response body is not valid JSON",
            Self::ListFields => "the delegation-list response fields are invalid",
            Self::DelegationDocument => "the delegation response body is not valid JSON",
            Self::DelegationFields => "the delegation response fields are invalid",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DelegationTimestampField {
    Proposed,
    Accepted,
    Ended,
}

impl DelegationTimestampField {
    const fn failure_reason(self) -> &'static str {
        match self {
            Self::Proposed => "the proposal time is invalid",
            Self::Accepted => "the acceptance time is invalid",
            Self::Ended => "the ending time is invalid",
        }
    }
}

impl fmt::Display for DelegationApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            DelegationApiErrorKind::Endpoint(HttpEndpointError::Invalid) => write!(
                formatter,
                "the deployment API URL cannot form a delegation {} endpoint",
                self.operation.name()
            ),
            DelegationApiErrorKind::Endpoint(HttpEndpointError::InsecureHttp) => formatter
                .write_str(
                    "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it",
                ),
            DelegationApiErrorKind::InvalidAuthorizationHeader(error) => write!(
                formatter,
                "the selected credential cannot be represented as a bearer credential: {error}"
            ),
            DelegationApiErrorKind::InvalidIdempotencyHeader(error) => write!(
                formatter,
                "the generated delegation request identity is not a valid header value: {error}"
            ),
            DelegationApiErrorKind::SerializeRequest(error) => {
                write!(formatter, "the delegation proposal cannot be serialized: {error}")
            }
            DelegationApiErrorKind::BuildRequest(_) => write!(
                formatter,
                "construct the delegation {} request",
                self.operation.name()
            ),
            DelegationApiErrorKind::DecodeResponse { stage, .. } => write!(
                formatter,
                "delegation {} response violates the public API contract: {}",
                self.operation.name(),
                stage.failure_reason()
            ),
            DelegationApiErrorKind::InvalidTimestamp { field, .. } => write!(
                formatter,
                "delegation {} response violates the public API contract: {}",
                self.operation.name(),
                field.failure_reason()
            ),
            DelegationApiErrorKind::Protocol { reason } => write!(
                formatter,
                "delegation {} response violates the public API contract: {reason}",
                self.operation.name()
            ),
        }
    }
}
// jscpd:ignore-end

impl Error for DelegationApiError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            DelegationApiErrorKind::InvalidAuthorizationHeader(error)
            | DelegationApiErrorKind::InvalidIdempotencyHeader(error) => Some(error),
            DelegationApiErrorKind::SerializeRequest(error) => Some(error),
            DelegationApiErrorKind::BuildRequest(source) => Some(source),
            DelegationApiErrorKind::DecodeResponse { source, .. } => Some(source),
            DelegationApiErrorKind::InvalidTimestamp { source, .. } => Some(source),
            DelegationApiErrorKind::Endpoint(_) | DelegationApiErrorKind::Protocol { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    List,
    Propose,
    Show,
    Accept,
    End,
}

impl Operation {
    const fn name(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Propose => "proposal",
            Self::Show => "show",
            Self::Accept => "acceptance",
            Self::End => "ending",
        }
    }
}

// The delegation request plan owns this domain's bodyless participant mutations and proposal
// body; keeping it typed here prevents another domain from selecting delegation retry semantics.
// jscpd:ignore-start
struct RequestSpec {
    operation: Operation,
    method: Method,
    endpoint: Url,
    authorization: HeaderValue,
    idempotency_key: Option<HeaderValue>,
    body: Option<Zeroizing<Vec<u8>>>,
    maximum_attempts: usize,
}
// jscpd:ignore-end

type AttemptError = ApiAttemptError<DelegationApiError>;

enum RequestExecution {
    Response(BufferedResponse),
    Unreachable(UnreachableCategory),
}

pub(crate) fn list_current_principal_delegations(
    client: &HttpClient,
    api_url: &str,
    credential: &str,
    limit: Option<u16>,
    cursor: Option<&str>,
) -> Result<ListDelegationsOutcome, DelegationApiError> {
    let mut endpoint = endpoint(
        client,
        Operation::List,
        api_url,
        &["v1", "me", "delegations"],
    )?;
    http_util::append_pagination(&mut endpoint, limit, cursor);
    let spec = request_spec(
        Operation::List,
        Method::GET,
        endpoint,
        credential,
        None,
        None,
        READ_ATTEMPTS,
    )?;
    match execute_request(client, &spec)? {
        RequestExecution::Response(response) => decode_list(response),
        RequestExecution::Unreachable(category) => Ok(ListDelegationsOutcome::Common(
            CommonDelegationFailure::Unreachable(category),
        )),
    }
}

pub(crate) fn propose_delegation(
    client: &HttpClient,
    api_url: &str,
    credential: &str,
    service_principal_id: &str,
    idempotency_key: &str,
) -> Result<ProposeDelegationOutcome, DelegationApiError> {
    let request = generated_models::ProposeDelegationRequest::new(service_principal_id.to_owned());
    let body = serde_json::to_vec(&request)
        .map(Zeroizing::new)
        .map_err(|error| {
            DelegationApiError::local(
                Operation::Propose,
                DelegationApiErrorKind::SerializeRequest(error),
            )
        })?;
    let spec = request_spec(
        Operation::Propose,
        Method::POST,
        endpoint(client, Operation::Propose, api_url, &["v1", "delegations"])?,
        credential,
        Some(idempotency_key),
        Some(body),
        MUTATION_ATTEMPTS,
    )?;
    match execute_request(client, &spec)? {
        RequestExecution::Response(response) => decode_propose(response, idempotency_key),
        RequestExecution::Unreachable(category) => Ok(ProposeDelegationOutcome::Common(
            CommonDelegationFailure::Unreachable(category),
        )),
    }
}

pub(crate) fn get_delegation(
    client: &HttpClient,
    api_url: &str,
    credential: &str,
    delegation_id: &str,
) -> Result<GetDelegationOutcome, DelegationApiError> {
    let spec = request_spec(
        Operation::Show,
        Method::GET,
        endpoint(
            client,
            Operation::Show,
            api_url,
            &["v1", "delegations", delegation_id],
        )?,
        credential,
        None,
        None,
        READ_ATTEMPTS,
    )?;
    match execute_request(client, &spec)? {
        RequestExecution::Response(response) => decode_get(response),
        RequestExecution::Unreachable(category) => Ok(GetDelegationOutcome::Common(
            CommonDelegationFailure::Unreachable(category),
        )),
    }
}

pub(crate) fn accept_delegation(
    client: &HttpClient,
    api_url: &str,
    credential: &str,
    delegation_id: &str,
    idempotency_key: &str,
) -> Result<AcceptDelegationOutcome, DelegationApiError> {
    execute_delegation_mutation(
        client,
        MutationInput {
            operation: Operation::Accept,
            method: Method::POST,
            api_url,
            path: &["v1", "delegations", delegation_id, "accept"],
            credential,
            idempotency_key,
        },
        |response| decode_accept(response, idempotency_key),
        |category| AcceptDelegationOutcome::Common(CommonDelegationFailure::Unreachable(category)),
    )
}

pub(crate) fn end_delegation(
    client: &HttpClient,
    api_url: &str,
    credential: &str,
    delegation_id: &str,
    idempotency_key: &str,
) -> Result<EndDelegationOutcome, DelegationApiError> {
    execute_delegation_mutation(
        client,
        MutationInput {
            operation: Operation::End,
            method: Method::DELETE,
            api_url,
            path: &["v1", "delegations", delegation_id],
            credential,
            idempotency_key,
        },
        |response| decode_end(response, idempotency_key),
        |category| EndDelegationOutcome::Common(CommonDelegationFailure::Unreachable(category)),
    )
}

struct MutationInput<'a> {
    operation: Operation,
    method: Method,
    api_url: &'a str,
    path: &'a [&'a str],
    credential: &'a str,
    idempotency_key: &'a str,
}

fn execute_delegation_mutation<T>(
    client: &HttpClient,
    input: MutationInput<'_>,
    decode: impl FnOnce(BufferedResponse) -> Result<T, DelegationApiError>,
    unreachable: impl FnOnce(UnreachableCategory) -> T,
) -> Result<T, DelegationApiError> {
    let spec = request_spec(
        input.operation,
        input.method,
        endpoint(client, input.operation, input.api_url, input.path)?,
        input.credential,
        Some(input.idempotency_key),
        None,
        MUTATION_ATTEMPTS,
    )?;
    match execute_request(client, &spec)? {
        RequestExecution::Response(response) => decode(response),
        RequestExecution::Unreachable(category) => Ok(unreachable(category)),
    }
}

fn endpoint(
    client: &HttpClient,
    operation: Operation,
    api_url: &str,
    path: &[&str],
) -> Result<Url, DelegationApiError> {
    client.endpoint(api_url, path).map_err(|error| {
        DelegationApiError::local(operation, DelegationApiErrorKind::Endpoint(error))
    })
}

fn request_spec(
    operation: Operation,
    method: Method,
    endpoint: Url,
    credential: &str,
    idempotency_key: Option<&str>,
    body: Option<Zeroizing<Vec<u8>>>,
    maximum_attempts: usize,
) -> Result<RequestSpec, DelegationApiError> {
    let authorization = bearer_authorization(credential).map_err(|error| {
        DelegationApiError::local(
            operation,
            DelegationApiErrorKind::InvalidAuthorizationHeader(error),
        )
    })?;
    let idempotency_key = idempotency_key
        .map(HeaderValue::from_str)
        .transpose()
        .map_err(|error| {
            DelegationApiError::local(
                operation,
                DelegationApiErrorKind::InvalidIdempotencyHeader(error),
            )
        })?;
    Ok(RequestSpec {
        operation,
        method,
        endpoint,
        authorization,
        idempotency_key,
        body,
        maximum_attempts,
    })
}

// Delegation transport owns its exact mutation retry count and typed error construction; sharing
// another domain's loop would let unrelated operations alter this contract.
// jscpd:ignore-start
fn execute_request(
    client: &HttpClient,
    spec: &RequestSpec,
) -> Result<RequestExecution, DelegationApiError> {
    let mut last_failure = UnreachableCategory::Connection;
    for attempt in 0..spec.maximum_attempts {
        match client.run(REQUEST_TIMEOUT, send_request(client, spec)) {
            Ok(Ok(response)) => return Ok(RequestExecution::Response(response)),
            Ok(Err(AttemptError::Protocol(error))) => return Err(error),
            Ok(Err(AttemptError::Transport(category))) => last_failure = category,
            Err(_) => last_failure = UnreachableCategory::Timeout,
        }
        if attempt + 1 < spec.maximum_attempts {
            crate::timing::sleep(crate::timing::short_retry_delay());
        }
    }
    Ok(RequestExecution::Unreachable(last_failure))
}

async fn send_request(
    client: &HttpClient,
    spec: &RequestSpec,
) -> Result<BufferedResponse, AttemptError> {
    let mut request = client
        .inner()
        .request(spec.method.clone(), spec.endpoint.clone())
        .timeout(REQUEST_TIMEOUT)
        .header(ACCEPT, ACCEPTED_MEDIA_TYPES)
        .header(AUTHORIZATION, spec.authorization.clone());
    if let Some(idempotency_key) = &spec.idempotency_key {
        request = request.header("Idempotency-Key", idempotency_key.clone());
    }
    if let Some(body) = &spec.body {
        request = request
            .header(CONTENT_TYPE, JSON_MEDIA_TYPE)
            .body(body.as_slice().to_vec());
    }
    let response = request.send().await.map_err(|error| {
        if error.is_builder() {
            AttemptError::Protocol(DelegationApiError::local(
                spec.operation,
                DelegationApiErrorKind::BuildRequest(error),
            ))
        } else {
            AttemptError::Transport(classify_reqwest_error(&error))
        }
    })?;
    http_util::buffer_api_response(response, |reason, rejected| {
        DelegationApiError::protocol(spec.operation, reason, rejected)
    })
    .await
}
// jscpd:ignore-end

fn decode_list(response: BufferedResponse) -> Result<ListDelegationsOutcome, DelegationApiError> {
    match response.status {
        StatusCode::OK => {
            decode_delegation_page(Operation::List, &response).map(ListDelegationsOutcome::Listed)
        }
        _ => decode_common_failure(Operation::List, &response).map(ListDelegationsOutcome::Common),
    }
}

fn decode_get(response: BufferedResponse) -> Result<GetDelegationOutcome, DelegationApiError> {
    match response.status {
        StatusCode::OK => {
            decode_delegation(Operation::Show, &response).map(GetDelegationOutcome::Found)
        }
        StatusCode::NOT_FOUND => {
            require_problem(Operation::Show, &response, NOT_FOUND, false)?;
            Ok(GetDelegationOutcome::NotFound)
        }
        _ => decode_common_failure(Operation::Show, &response).map(GetDelegationOutcome::Common),
    }
}

fn decode_propose(
    response: BufferedResponse,
    idempotency_key: &str,
) -> Result<ProposeDelegationOutcome, DelegationApiError> {
    match response.status {
        StatusCode::CREATED => {
            require_response_idempotency(Operation::Propose, &response, idempotency_key)?;
            let delegation = decode_delegation(Operation::Propose, &response)?;
            if delegation.state != DelegationState::Pending {
                return Err(DelegationApiError::protocol(
                    Operation::Propose,
                    "the proposed delegation is not pending",
                    false,
                ));
            }
            require_location(Operation::Propose, &response, &delegation.id)?;
            Ok(ProposeDelegationOutcome::Proposed(delegation))
        }
        StatusCode::CONFLICT => {
            match decode_problem_type(Operation::Propose, &response, false)?.as_str() {
                DELEGATION_TRANSITION_UNAVAILABLE => {
                    Ok(ProposeDelegationOutcome::TransitionUnavailable)
                }
                IDEMPOTENCY_CONFLICT => Ok(ProposeDelegationOutcome::IdempotencyConflict),
                _ => Err(unrecognized_conflict(Operation::Propose)),
            }
        }
        StatusCode::PAYLOAD_TOO_LARGE => {
            require_problem(Operation::Propose, &response, REQUEST_BODY_TOO_LARGE, false)?;
            Ok(ProposeDelegationOutcome::Common(
                CommonDelegationFailure::InvalidInput,
            ))
        }
        StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            require_problem(Operation::Propose, &response, UNSUPPORTED_MEDIA_TYPE, false)?;
            Ok(ProposeDelegationOutcome::Common(
                CommonDelegationFailure::InvalidInput,
            ))
        }
        StatusCode::SERVICE_UNAVAILABLE => {
            require_problem(Operation::Propose, &response, RETRYABLE_CONFLICT, false)?;
            parse_retry_after(Operation::Propose, &response)
                .map(|retry_after| ProposeDelegationOutcome::RetryableConflict { retry_after })
        }
        _ => decode_common_failure(Operation::Propose, &response)
            .map(ProposeDelegationOutcome::Common),
    }
}

fn decode_accept(
    response: BufferedResponse,
    idempotency_key: &str,
) -> Result<AcceptDelegationOutcome, DelegationApiError> {
    match response.status {
        StatusCode::OK => {
            require_response_idempotency(Operation::Accept, &response, idempotency_key)?;
            let delegation = decode_delegation(Operation::Accept, &response)?;
            if delegation.state != DelegationState::Active {
                return Err(DelegationApiError::protocol(
                    Operation::Accept,
                    "the accepted delegation is not active",
                    false,
                ));
            }
            Ok(AcceptDelegationOutcome::Accepted(delegation))
        }
        StatusCode::NOT_FOUND => {
            require_problem(Operation::Accept, &response, NOT_FOUND, false)?;
            Ok(AcceptDelegationOutcome::NotFound)
        }
        StatusCode::CONFLICT => {
            match decode_problem_type(Operation::Accept, &response, false)?.as_str() {
                DELEGATION_TRANSITION_UNAVAILABLE => {
                    Ok(AcceptDelegationOutcome::TransitionUnavailable)
                }
                IDEMPOTENCY_CONFLICT => Ok(AcceptDelegationOutcome::IdempotencyConflict),
                _ => Err(unrecognized_conflict(Operation::Accept)),
            }
        }
        _ => {
            decode_common_failure(Operation::Accept, &response).map(AcceptDelegationOutcome::Common)
        }
    }
}

fn decode_end(
    response: BufferedResponse,
    idempotency_key: &str,
) -> Result<EndDelegationOutcome, DelegationApiError> {
    match response.status {
        StatusCode::NO_CONTENT => {
            require_response_idempotency(Operation::End, &response, idempotency_key)?;
            if response.content_type.is_some() || !response.body.is_empty() {
                return Err(DelegationApiError::protocol(
                    Operation::End,
                    "the successful response contains an unexpected representation",
                    false,
                ));
            }
            Ok(EndDelegationOutcome::Ended)
        }
        StatusCode::NOT_FOUND => {
            require_problem(Operation::End, &response, NOT_FOUND, false)?;
            Ok(EndDelegationOutcome::NotFound)
        }
        StatusCode::CONFLICT => {
            match decode_problem_type(Operation::End, &response, false)?.as_str() {
                DELEGATION_TRANSITION_UNAVAILABLE => {
                    Ok(EndDelegationOutcome::TransitionUnavailable)
                }
                IDEMPOTENCY_CONFLICT => Ok(EndDelegationOutcome::IdempotencyConflict),
                _ => Err(unrecognized_conflict(Operation::End)),
            }
        }
        _ => decode_common_failure(Operation::End, &response).map(EndDelegationOutcome::Common),
    }
}

// Common statuses retain delegation-specific typed outcomes and protocol diagnostics; folding
// them into another domain would couple independent authorization and error contracts.
// jscpd:ignore-start
fn decode_common_failure(
    operation: Operation,
    response: &BufferedResponse,
) -> Result<CommonDelegationFailure, DelegationApiError> {
    match response.status {
        StatusCode::BAD_REQUEST => {
            require_problem(operation, response, BAD_REQUEST, false)?;
            Ok(CommonDelegationFailure::InvalidInput)
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(operation, response, UNAUTHORIZED, true)?;
            Ok(CommonDelegationFailure::Unauthenticated)
        }
        StatusCode::FORBIDDEN => {
            require_problem(operation, response, FORBIDDEN, false)?;
            Ok(CommonDelegationFailure::Forbidden)
        }
        status if status.is_server_error() => Ok(CommonDelegationFailure::Unreachable(
            UnreachableCategory::Server,
        )),
        status if status.is_redirection() => Err(DelegationApiError::protocol(
            operation,
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(DelegationApiError::protocol(
            operation,
            "the HTTP status is not valid for this operation",
            false,
        )),
    }
}
// jscpd:ignore-end

fn decode_delegation_page(
    operation: Operation,
    response: &BufferedResponse,
) -> Result<DelegationPage, DelegationApiError> {
    require_json(operation, response)?;
    let value: serde_json::Value = serde_json::from_slice(&response.body).map_err(|source| {
        DelegationApiError::local(
            operation,
            DelegationApiErrorKind::DecodeResponse {
                stage: DelegationDecodeStage::ListDocument,
                source,
            },
        )
    })?;
    if value
        .get("nextCursor")
        .is_some_and(serde_json::Value::is_null)
    {
        return Err(DelegationApiError::protocol(
            operation,
            "the delegation list contains an explicit null optional field",
            false,
        ));
    }
    if let Some(items) = value.get("items").and_then(serde_json::Value::as_array) {
        for item in items {
            reject_null_optionals(operation, item)?;
        }
    }
    let page: generated_models::DelegationList =
        serde_json::from_value(value).map_err(|source| {
            DelegationApiError::local(
                operation,
                DelegationApiErrorKind::DecodeResponse {
                    stage: DelegationDecodeStage::ListFields,
                    source,
                },
            )
        })?;
    if page.next_cursor.as_deref() == Some("") {
        return Err(DelegationApiError::protocol(
            operation,
            "the delegation-list cursor is empty",
            false,
        ));
    }
    let items = page
        .items
        .into_iter()
        .map(|item| convert_delegation(operation, item))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DelegationPage {
        items,
        next_cursor: page.next_cursor,
    })
}

fn decode_delegation(
    operation: Operation,
    response: &BufferedResponse,
) -> Result<Delegation, DelegationApiError> {
    require_json(operation, response)?;
    let value: serde_json::Value = serde_json::from_slice(&response.body).map_err(|source| {
        DelegationApiError::local(
            operation,
            DelegationApiErrorKind::DecodeResponse {
                stage: DelegationDecodeStage::DelegationDocument,
                source,
            },
        )
    })?;
    reject_null_optionals(operation, &value)?;
    let delegation: generated_models::Delegation =
        serde_json::from_value(value).map_err(|source| {
            DelegationApiError::local(
                operation,
                DelegationApiErrorKind::DecodeResponse {
                    stage: DelegationDecodeStage::DelegationFields,
                    source,
                },
            )
        })?;
    convert_delegation(operation, delegation)
}

fn reject_null_optionals(
    operation: Operation,
    value: &serde_json::Value,
) -> Result<(), DelegationApiError> {
    if ["acceptedAt", "endedAt", "terminalReason"]
        .into_iter()
        .any(|field| value.get(field).is_some_and(serde_json::Value::is_null))
    {
        Err(DelegationApiError::protocol(
            operation,
            "the delegation response contains an explicit null optional field",
            false,
        ))
    } else {
        Ok(())
    }
}

fn convert_delegation(
    operation: Operation,
    value: generated_models::Delegation,
) -> Result<Delegation, DelegationApiError> {
    if !crate::public_id::valid_typed_id(&value.id, "dlg_") {
        return Err(DelegationApiError::protocol(
            operation,
            "the delegation ID is invalid",
            false,
        ));
    }
    if !crate::public_id::valid_typed_id(&value.human_principal_id, "prn_") {
        return Err(DelegationApiError::protocol(
            operation,
            "the human principal ID is invalid",
            false,
        ));
    }
    if !crate::public_id::valid_typed_id(&value.service_principal_id, "prn_") {
        return Err(DelegationApiError::protocol(
            operation,
            "the service principal ID is invalid",
            false,
        ));
    }
    parse_timestamp(
        operation,
        &value.proposed_at,
        DelegationTimestampField::Proposed,
    )?;
    if let Some(accepted_at) = value.accepted_at.as_deref() {
        parse_timestamp(operation, accepted_at, DelegationTimestampField::Accepted)?;
    }
    if let Some(ended_at) = value.ended_at.as_deref() {
        parse_timestamp(operation, ended_at, DelegationTimestampField::Ended)?;
    }

    let state = match value.state {
        generated_models::delegation::State::Pending => DelegationState::Pending,
        generated_models::delegation::State::Active => DelegationState::Active,
        generated_models::delegation::State::Ended => DelegationState::Ended,
    };
    let terminal_reason = value.terminal_reason.map(|reason| match reason {
        generated_models::delegation::TerminalReason::ParticipantEnded => {
            DelegationTerminalReason::ParticipantEnded
        }
        generated_models::delegation::TerminalReason::ParticipantDeleted => {
            DelegationTerminalReason::ParticipantDeleted
        }
    });
    let lifecycle_valid = match state {
        DelegationState::Pending => {
            value.accepted_at.is_none() && value.ended_at.is_none() && terminal_reason.is_none()
        }
        DelegationState::Active => {
            value.accepted_at.is_some() && value.ended_at.is_none() && terminal_reason.is_none()
        }
        DelegationState::Ended => value.ended_at.is_some() && terminal_reason.is_some(),
    };
    if !lifecycle_valid {
        return Err(DelegationApiError::protocol(
            operation,
            "the delegation lifecycle fields are inconsistent",
            false,
        ));
    }

    Ok(Delegation {
        id: value.id,
        human_principal_id: value.human_principal_id,
        service_principal_id: value.service_principal_id,
        state,
        proposed_at: value.proposed_at,
        accepted_at: value.accepted_at,
        ended_at: value.ended_at,
        terminal_reason,
    })
}

fn parse_timestamp(
    operation: Operation,
    value: &str,
    field: DelegationTimestampField,
) -> Result<(), DelegationApiError> {
    OffsetDateTime::parse(value, &Rfc3339)
        .map(|_| ())
        .map_err(|source| {
            DelegationApiError::local(
                operation,
                DelegationApiErrorKind::InvalidTimestamp { field, source },
            )
        })
}

fn require_json(
    operation: Operation,
    response: &BufferedResponse,
) -> Result<(), DelegationApiError> {
    http_util::require_media_type(response.content_type.as_deref(), JSON_MEDIA_TYPE)
        .map_err(|reason| DelegationApiError::protocol(operation, reason, false))
}

fn require_response_idempotency(
    operation: Operation,
    response: &BufferedResponse,
    expected: &str,
) -> Result<(), DelegationApiError> {
    if http_util::header_matches(response.idempotency_key.as_ref(), expected) {
        Ok(())
    } else {
        Err(DelegationApiError::protocol(
            operation,
            "the successful response has a missing or mismatched Idempotency-Key header",
            false,
        ))
    }
}

fn require_location(
    operation: Operation,
    response: &BufferedResponse,
    delegation_id: &str,
) -> Result<(), DelegationApiError> {
    let expected = format!("/v1/delegations/{delegation_id}");
    if http_util::header_matches(response.location.as_ref(), &expected) {
        Ok(())
    } else {
        Err(DelegationApiError::protocol(
            operation,
            "the successful response has a missing or mismatched Location header",
            false,
        ))
    }
}

fn require_problem(
    operation: Operation,
    response: &BufferedResponse,
    expected: &str,
    credential_rejected: bool,
) -> Result<(), DelegationApiError> {
    problem::require_type(response, expected)
        .map_err(|reason| DelegationApiError::protocol(operation, reason, credential_rejected))
}

fn decode_problem_type(
    operation: Operation,
    response: &BufferedResponse,
    credential_rejected: bool,
) -> Result<String, DelegationApiError> {
    problem::decode_type(response)
        .map_err(|reason| DelegationApiError::protocol(operation, reason, credential_rejected))
}

// Proposal concurrency is the only delegation response carrying Retry-After; keeping validation
// here preserves that narrow contract instead of exposing a generic retry policy.
// jscpd:ignore-start
fn parse_retry_after(
    operation: Operation,
    response: &BufferedResponse,
) -> Result<u64, DelegationApiError> {
    response
        .retry_after
        .as_ref()
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            DelegationApiError::protocol(
                operation,
                "the Retry-After header is missing or invalid",
                false,
            )
        })
}
// jscpd:ignore-end

fn unrecognized_conflict(operation: Operation) -> DelegationApiError {
    DelegationApiError::protocol(
        operation,
        "a 409 response has an unrecognized problem type",
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(body: &[u8]) -> BufferedResponse {
        BufferedResponse {
            status: StatusCode::OK,
            content_type: Some(JSON_MEDIA_TYPE.to_owned()),
            idempotency_key: None,
            location: None,
            retry_after: None,
            body: Zeroizing::new(body.to_vec()),
        }
    }

    fn assert_decode_source(error: &DelegationApiError, expected: DelegationDecodeStage) {
        assert!(matches!(
            &error.kind,
            DelegationApiErrorKind::DecodeResponse { stage, .. } if *stage == expected
        ));
        assert!(
            error
                .source()
                .is_some_and(|source| source.downcast_ref::<serde_json::Error>().is_some())
        );
    }

    #[test]
    fn response_decoding_failures_retain_their_json_source_and_stage() {
        let invalid_list_document =
            decode_delegation_page(Operation::List, &response(b"{")).unwrap_err();
        assert_decode_source(&invalid_list_document, DelegationDecodeStage::ListDocument);

        let invalid_list_fields =
            decode_delegation_page(Operation::List, &response(br#"{"items":"not-an-array"}"#))
                .unwrap_err();
        assert_decode_source(&invalid_list_fields, DelegationDecodeStage::ListFields);

        let invalid_delegation_document =
            decode_delegation(Operation::Show, &response(b"{")).unwrap_err();
        assert_decode_source(
            &invalid_delegation_document,
            DelegationDecodeStage::DelegationDocument,
        );

        let invalid_delegation_fields =
            decode_delegation(Operation::Show, &response(br#"{"id":1}"#)).unwrap_err();
        assert_decode_source(
            &invalid_delegation_fields,
            DelegationDecodeStage::DelegationFields,
        );
    }

    #[test]
    fn timestamp_failure_retains_its_parse_source_and_field() {
        let error = decode_delegation(
            Operation::Show,
            &response(
                serde_json::json!({
                    "id": "dlg_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "humanPrincipalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "servicePrincipalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abd",
                    "state": "pending",
                    "proposedAt": "not-a-timestamp"
                })
                .to_string()
                .as_bytes(),
            ),
        )
        .unwrap_err();

        assert!(matches!(
            &error.kind,
            DelegationApiErrorKind::InvalidTimestamp {
                field: DelegationTimestampField::Proposed,
                ..
            }
        ));
        assert!(
            error
                .source()
                .is_some_and(|source| source.downcast_ref::<time::error::Parse>().is_some())
        );
    }
}
