use std::fmt;
use std::time::Duration;

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use reqwest::{Method, StatusCode, Url};
use serde::Serialize;
use zeroize::Zeroizing;

use super::generated::models as generated_models;
use super::http_client::{HttpClient, HttpEndpointError};
use super::http_util;
use super::problem::{
    self, ACCEPTED_MEDIA_TYPES, BAD_REQUEST, FORBIDDEN, JSON_MEDIA_TYPE, UNAUTHORIZED,
};
use super::{UnreachableCategory, bearer_authorization, classify_reqwest_error};
use crate::public_id::valid_typed_id;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const READ_ATTEMPTS: usize = 1;
const MUTATION_ATTEMPTS: usize = 2;
const INVALID_IDENTITY_PROOF: &str = "https://api.scherzo.dev/problems/invalid-identity-proof";
const IDENTITY_UNAVAILABLE: &str = "https://api.scherzo.dev/problems/identity-unavailable";
const IDENTITY_NOT_FOUND: &str = "https://api.scherzo.dev/problems/identity-not-found";
const IDENTITY_REMOVAL_UNAVAILABLE: &str =
    "https://api.scherzo.dev/problems/identity-removal-unavailable";
const REAUTHENTICATION_REQUIRED: &str =
    "https://api.scherzo.dev/problems/reauthentication-required";
const IDEMPOTENCY_CONFLICT: &str = "https://api.scherzo.dev/problems/idempotency-conflict";
const REQUEST_BODY_TOO_LARGE: &str = "https://api.scherzo.dev/problems/request-body-too-large";
const UNSUPPORTED_MEDIA_TYPE: &str = "https://api.scherzo.dev/problems/unsupported-media-type";

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OidcIdentity {
    pub(crate) id: String,
    pub(crate) kind: IdentityKind,
    pub(crate) issuer: String,
    pub(crate) subject: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) asserted_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) email_verified: Option<bool>,
    pub(crate) created_at: String,
    pub(crate) current: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IdentityKind {
    Oidc,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OidcIdentityPage {
    pub(crate) items: Vec<OidcIdentity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) next_cursor: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum CommonIdentityFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    Unreachable(UnreachableCategory),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ListIdentitiesOutcome {
    Listed(OidcIdentityPage),
    Common(CommonIdentityFailure),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum LinkIdentityOutcome {
    Linked(OidcIdentity),
    Common(CommonIdentityFailure),
    InvalidProof,
    IdentityUnavailable,
    IdempotencyConflict,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum RemoveIdentityOutcome {
    Removed,
    Common(CommonIdentityFailure),
    ReauthenticationRequired,
    NotFound,
    RemovalUnavailable,
    IdempotencyConflict,
}

#[derive(Debug)]
pub(crate) struct IdentityApiError {
    operation: Operation,
    kind: IdentityApiErrorKind,
    credential_rejected: bool,
}

// Identity and organization API errors retain separate operation vocabularies and
// protocol reasons even though both expose the shared credential-rejection signal.
// jscpd:ignore-start
impl IdentityApiError {
    pub(crate) fn credential_rejected(&self) -> bool {
        self.credential_rejected
    }

    fn local(operation: Operation, kind: IdentityApiErrorKind) -> Self {
        Self {
            operation,
            kind,
            credential_rejected: false,
        }
    }

    fn protocol(operation: Operation, reason: &'static str, credential_rejected: bool) -> Self {
        Self {
            operation,
            kind: IdentityApiErrorKind::Protocol { reason },
            credential_rejected,
        }
    }
}
// jscpd:ignore-end

#[derive(Debug)]
enum IdentityApiErrorKind {
    Endpoint(HttpEndpointError),
    InvalidAuthorizationHeader(reqwest::header::InvalidHeaderValue),
    InvalidIdempotencyHeader(reqwest::header::InvalidHeaderValue),
    SerializeRequest(serde_json::Error),
    Protocol { reason: &'static str },
}

impl std::error::Error for IdentityApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            IdentityApiErrorKind::InvalidAuthorizationHeader(error)
            | IdentityApiErrorKind::InvalidIdempotencyHeader(error) => Some(error),
            IdentityApiErrorKind::SerializeRequest(error) => Some(error),
            IdentityApiErrorKind::Endpoint(_) | IdentityApiErrorKind::Protocol { .. } => None,
        }
    }
}

impl fmt::Display for IdentityApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            IdentityApiErrorKind::Endpoint(HttpEndpointError::Invalid) => write!(
                formatter,
                "the deployment API URL cannot form an identity {} endpoint",
                self.operation.name()
            ),
            IdentityApiErrorKind::Endpoint(HttpEndpointError::InsecureHttp) => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            IdentityApiErrorKind::InvalidAuthorizationHeader(error) => write!(
                formatter,
                "the stored access token cannot be represented as a bearer credential: {error}"
            ),
            IdentityApiErrorKind::InvalidIdempotencyHeader(error) => write!(
                formatter,
                "the generated identity request identity is not a valid header value: {error}"
            ),
            IdentityApiErrorKind::SerializeRequest(error) => write!(
                formatter,
                "the identity link request cannot be serialized: {error}"
            ),
            IdentityApiErrorKind::Protocol { reason } => write!(
                formatter,
                "identity {} response violates the public API contract: {reason}",
                self.operation.name()
            ),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    List,
    Link,
    Remove,
}

impl Operation {
    const fn name(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Link => "link",
            Self::Remove => "removal",
        }
    }
}

struct RequestSpec {
    operation: Operation,
    method: Method,
    endpoint: Url,
    authorization: HeaderValue,
    idempotency_key: Option<HeaderValue>,
    body: Option<Zeroizing<Vec<u8>>>,
    max_attempts: usize,
}

type ReceivedResponse = http_util::BufferedResponse;
type AttemptError = http_util::ApiAttemptError<IdentityApiError>;

enum RequestExecution {
    Response(ReceivedResponse),
    Unreachable(UnreachableCategory),
}

pub(crate) fn list_identities(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    limit: Option<u16>,
    cursor: Option<&str>,
) -> Result<ListIdentitiesOutcome, IdentityApiError> {
    let mut endpoint = endpoint(client, Operation::List, api_url, &[])?;
    http_util::append_pagination(&mut endpoint, limit, cursor);
    let spec = request_spec(
        Operation::List,
        Method::GET,
        endpoint,
        access_token,
        None,
        None,
        READ_ATTEMPTS,
    )?;
    match execute_request(client, &spec)? {
        RequestExecution::Response(response) => decode_list_response(response),
        RequestExecution::Unreachable(category) => Ok(ListIdentitiesOutcome::Common(
            CommonIdentityFailure::Unreachable(category),
        )),
    }
}

pub(crate) fn link_identity(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    idempotency_key: &str,
    proposed_identity_access_token: &str,
) -> Result<LinkIdentityOutcome, IdentityApiError> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct LinkRequest<'a> {
        proposed_identity_access_token: &'a str,
    }

    let body = serde_json::to_vec(&LinkRequest {
        proposed_identity_access_token,
    })
    .map(Zeroizing::new)
    .map_err(|error| {
        IdentityApiError::local(
            Operation::Link,
            IdentityApiErrorKind::SerializeRequest(error),
        )
    })?;
    let spec = request_spec(
        Operation::Link,
        Method::POST,
        endpoint(client, Operation::Link, api_url, &[])?,
        access_token,
        Some(idempotency_key),
        Some(body),
        MUTATION_ATTEMPTS,
    )?;
    match execute_request(client, &spec)? {
        RequestExecution::Response(response) => decode_link_response(response, idempotency_key),
        RequestExecution::Unreachable(category) => Ok(LinkIdentityOutcome::Common(
            CommonIdentityFailure::Unreachable(category),
        )),
    }
}

pub(crate) fn remove_identity(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    identity_id: &str,
    idempotency_key: &str,
) -> Result<RemoveIdentityOutcome, IdentityApiError> {
    let spec = request_spec(
        Operation::Remove,
        Method::DELETE,
        endpoint(client, Operation::Remove, api_url, &[identity_id])?,
        access_token,
        Some(idempotency_key),
        None,
        MUTATION_ATTEMPTS,
    )?;
    match execute_request(client, &spec)? {
        RequestExecution::Response(response) => decode_remove_response(response, idempotency_key),
        RequestExecution::Unreachable(category) => Ok(RemoveIdentityOutcome::Common(
            CommonIdentityFailure::Unreachable(category),
        )),
    }
}

fn endpoint(
    client: &HttpClient,
    operation: Operation,
    api_url: &str,
    suffix: &[&str],
) -> Result<Url, IdentityApiError> {
    let mut path = vec!["v1", "me", "identities"];
    path.extend_from_slice(suffix);
    client
        .endpoint(api_url, &path)
        .map_err(|error| IdentityApiError::local(operation, IdentityApiErrorKind::Endpoint(error)))
}

fn request_spec(
    operation: Operation,
    method: Method,
    endpoint: Url,
    access_token: &str,
    idempotency_key: Option<&str>,
    body: Option<Zeroizing<Vec<u8>>>,
    max_attempts: usize,
) -> Result<RequestSpec, IdentityApiError> {
    let authorization = bearer_authorization(access_token).map_err(|error| {
        IdentityApiError::local(
            operation,
            IdentityApiErrorKind::InvalidAuthorizationHeader(error),
        )
    })?;
    let idempotency_key = idempotency_key
        .map(HeaderValue::from_str)
        .transpose()
        .map_err(|error| {
            IdentityApiError::local(
                operation,
                IdentityApiErrorKind::InvalidIdempotencyHeader(error),
            )
        })?;
    Ok(RequestSpec {
        operation,
        method,
        endpoint,
        authorization,
        idempotency_key,
        body,
        max_attempts,
    })
}

fn execute_request(
    client: &HttpClient,
    spec: &RequestSpec,
) -> Result<RequestExecution, IdentityApiError> {
    let mut last_failure = UnreachableCategory::Connection;
    for attempt in 0..spec.max_attempts {
        match client.run(REQUEST_TIMEOUT, send_request(client, spec)) {
            Ok(Ok(response)) => return Ok(RequestExecution::Response(response)),
            Ok(Err(AttemptError::Protocol(error))) => return Err(error),
            Ok(Err(AttemptError::Transport(category))) => last_failure = category,
            Err(_) => last_failure = UnreachableCategory::Timeout,
        }
        if attempt + 1 < spec.max_attempts {
            crate::timing::sleep(crate::timing::short_retry_delay());
        }
    }
    Ok(RequestExecution::Unreachable(last_failure))
}

async fn send_request(
    client: &HttpClient,
    spec: &RequestSpec,
) -> Result<ReceivedResponse, AttemptError> {
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
            AttemptError::Protocol(IdentityApiError::protocol(
                spec.operation,
                "the request could not be constructed",
                false,
            ))
        } else {
            AttemptError::Transport(classify_reqwest_error(&error))
        }
    })?;
    receive_response(spec.operation, response).await
}

async fn receive_response(
    operation: Operation,
    response: reqwest::Response,
) -> Result<ReceivedResponse, AttemptError> {
    http_util::buffer_api_response(response, |reason, rejected| {
        IdentityApiError::protocol(operation, reason, rejected)
    })
    .await
}

fn decode_list_response(
    response: ReceivedResponse,
) -> Result<ListIdentitiesOutcome, IdentityApiError> {
    match response.status {
        StatusCode::OK => {
            decode_identity_page(Operation::List, &response).map(ListIdentitiesOutcome::Listed)
        }
        StatusCode::BAD_REQUEST => {
            require_problem(Operation::List, &response, BAD_REQUEST, false)?;
            Ok(ListIdentitiesOutcome::Common(
                CommonIdentityFailure::InvalidInput,
            ))
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(Operation::List, &response, UNAUTHORIZED, true)?;
            Ok(ListIdentitiesOutcome::Common(
                CommonIdentityFailure::Unauthenticated,
            ))
        }
        StatusCode::FORBIDDEN => {
            require_problem(Operation::List, &response, FORBIDDEN, false)?;
            Ok(ListIdentitiesOutcome::Common(
                CommonIdentityFailure::Forbidden,
            ))
        }
        status if status.is_server_error() => Ok(ListIdentitiesOutcome::Common(
            CommonIdentityFailure::Unreachable(UnreachableCategory::Server),
        )),
        status if status.is_redirection() => Err(IdentityApiError::protocol(
            Operation::List,
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(IdentityApiError::protocol(
            Operation::List,
            "the HTTP status is not valid for this operation",
            false,
        )),
    }
}

fn decode_link_response(
    response: ReceivedResponse,
    idempotency_key: &str,
) -> Result<LinkIdentityOutcome, IdentityApiError> {
    match response.status {
        StatusCode::CREATED => {
            require_response_idempotency_key(Operation::Link, &response, idempotency_key)?;
            let identity = decode_identity(Operation::Link, &response)?;
            if identity.current {
                return Err(IdentityApiError::protocol(
                    Operation::Link,
                    "the linked identity is marked current",
                    false,
                ));
            }
            require_link_location(&response, &identity.id)?;
            Ok(LinkIdentityOutcome::Linked(identity))
        }
        StatusCode::BAD_REQUEST => {
            let problem_type = problem::decode_type(&response)
                .map_err(|reason| IdentityApiError::protocol(Operation::Link, reason, false))?;
            match problem_type.as_str() {
                BAD_REQUEST => Ok(LinkIdentityOutcome::Common(
                    CommonIdentityFailure::InvalidInput,
                )),
                INVALID_IDENTITY_PROOF => Ok(LinkIdentityOutcome::InvalidProof),
                _ => Err(IdentityApiError::protocol(
                    Operation::Link,
                    "a 400 response has an unrecognized problem type",
                    false,
                )),
            }
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(Operation::Link, &response, UNAUTHORIZED, true)?;
            Ok(LinkIdentityOutcome::Common(
                CommonIdentityFailure::Unauthenticated,
            ))
        }
        StatusCode::FORBIDDEN => {
            require_problem(Operation::Link, &response, FORBIDDEN, false)?;
            Ok(LinkIdentityOutcome::Common(
                CommonIdentityFailure::Forbidden,
            ))
        }
        StatusCode::CONFLICT => {
            let problem_type = problem::decode_type(&response)
                .map_err(|reason| IdentityApiError::protocol(Operation::Link, reason, false))?;
            match problem_type.as_str() {
                IDENTITY_UNAVAILABLE => Ok(LinkIdentityOutcome::IdentityUnavailable),
                IDEMPOTENCY_CONFLICT => Ok(LinkIdentityOutcome::IdempotencyConflict),
                _ => Err(IdentityApiError::protocol(
                    Operation::Link,
                    "a 409 response has an unrecognized problem type",
                    false,
                )),
            }
        }
        StatusCode::PAYLOAD_TOO_LARGE => {
            require_problem(Operation::Link, &response, REQUEST_BODY_TOO_LARGE, false)?;
            Ok(LinkIdentityOutcome::Common(
                CommonIdentityFailure::InvalidInput,
            ))
        }
        StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            require_problem(Operation::Link, &response, UNSUPPORTED_MEDIA_TYPE, false)?;
            Ok(LinkIdentityOutcome::Common(
                CommonIdentityFailure::InvalidInput,
            ))
        }
        status if status.is_server_error() => Ok(LinkIdentityOutcome::Common(
            CommonIdentityFailure::Unreachable(UnreachableCategory::Server),
        )),
        status if status.is_redirection() => Err(IdentityApiError::protocol(
            Operation::Link,
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(IdentityApiError::protocol(
            Operation::Link,
            "the HTTP status is not valid for this operation",
            false,
        )),
    }
}

fn decode_remove_response(
    response: ReceivedResponse,
    idempotency_key: &str,
) -> Result<RemoveIdentityOutcome, IdentityApiError> {
    match response.status {
        StatusCode::NO_CONTENT => {
            require_response_idempotency_key(Operation::Remove, &response, idempotency_key)?;
            if !response.body.is_empty() {
                return Err(IdentityApiError::protocol(
                    Operation::Remove,
                    "the successful response contains a body",
                    false,
                ));
            }
            Ok(RemoveIdentityOutcome::Removed)
        }
        StatusCode::BAD_REQUEST => {
            require_problem(Operation::Remove, &response, BAD_REQUEST, false)?;
            Ok(RemoveIdentityOutcome::Common(
                CommonIdentityFailure::InvalidInput,
            ))
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(Operation::Remove, &response, UNAUTHORIZED, true)?;
            Ok(RemoveIdentityOutcome::Common(
                CommonIdentityFailure::Unauthenticated,
            ))
        }
        StatusCode::FORBIDDEN => {
            let problem_type = problem::decode_type(&response)
                .map_err(|reason| IdentityApiError::protocol(Operation::Remove, reason, false))?;
            match problem_type.as_str() {
                FORBIDDEN => Ok(RemoveIdentityOutcome::Common(
                    CommonIdentityFailure::Forbidden,
                )),
                REAUTHENTICATION_REQUIRED => Ok(RemoveIdentityOutcome::ReauthenticationRequired),
                _ => Err(IdentityApiError::protocol(
                    Operation::Remove,
                    "a 403 response has an unrecognized problem type",
                    false,
                )),
            }
        }
        StatusCode::NOT_FOUND => {
            require_problem(Operation::Remove, &response, IDENTITY_NOT_FOUND, false)?;
            Ok(RemoveIdentityOutcome::NotFound)
        }
        StatusCode::CONFLICT => {
            let problem_type = problem::decode_type(&response)
                .map_err(|reason| IdentityApiError::protocol(Operation::Remove, reason, false))?;
            match problem_type.as_str() {
                IDENTITY_REMOVAL_UNAVAILABLE => Ok(RemoveIdentityOutcome::RemovalUnavailable),
                IDEMPOTENCY_CONFLICT => Ok(RemoveIdentityOutcome::IdempotencyConflict),
                _ => Err(IdentityApiError::protocol(
                    Operation::Remove,
                    "a 409 response has an unrecognized problem type",
                    false,
                )),
            }
        }
        status if status.is_server_error() => Ok(RemoveIdentityOutcome::Common(
            CommonIdentityFailure::Unreachable(UnreachableCategory::Server),
        )),
        status if status.is_redirection() => Err(IdentityApiError::protocol(
            Operation::Remove,
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(IdentityApiError::protocol(
            Operation::Remove,
            "the HTTP status is not valid for this operation",
            false,
        )),
    }
}

fn decode_identity_page(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<OidcIdentityPage, IdentityApiError> {
    require_media_type(operation, response, JSON_MEDIA_TYPE, false)?;
    let value: serde_json::Value = serde_json::from_slice(&response.body).map_err(|_| {
        IdentityApiError::protocol(
            operation,
            "the identity-list response body is invalid",
            false,
        )
    })?;
    if value
        .get("nextCursor")
        .is_some_and(serde_json::Value::is_null)
        || optional_identity_field_is_null(&value)
    {
        return Err(IdentityApiError::protocol(
            operation,
            "the identity-list response contains an explicit null optional field",
            false,
        ));
    }
    let page: generated_models::OidcIdentityList = serde_json::from_value(value).map_err(|_| {
        IdentityApiError::protocol(
            operation,
            "the identity-list response body is invalid",
            false,
        )
    })?;
    if page.next_cursor.as_deref() == Some("") {
        return Err(IdentityApiError::protocol(
            operation,
            "the identity-list cursor is empty",
            false,
        ));
    }
    let items = page
        .items
        .into_iter()
        .map(OidcIdentity::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|reason| IdentityApiError::protocol(operation, reason, false))?;
    Ok(OidcIdentityPage {
        items,
        next_cursor: page.next_cursor,
    })
}

fn optional_identity_field_is_null(value: &serde_json::Value) -> bool {
    value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                ["assertedEmail", "emailVerified"]
                    .into_iter()
                    .any(|field| item.get(field).is_some_and(serde_json::Value::is_null))
            })
        })
}

fn decode_identity(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<OidcIdentity, IdentityApiError> {
    require_media_type(operation, response, JSON_MEDIA_TYPE, false)?;
    let value: serde_json::Value = serde_json::from_slice(&response.body).map_err(|_| {
        IdentityApiError::protocol(operation, "the identity response body is invalid", false)
    })?;
    if ["assertedEmail", "emailVerified"]
        .into_iter()
        .any(|field| value.get(field).is_some_and(serde_json::Value::is_null))
    {
        return Err(IdentityApiError::protocol(
            operation,
            "the identity response contains an explicit null optional field",
            false,
        ));
    }
    let identity: generated_models::OidcIdentityLink =
        serde_json::from_value(value).map_err(|_| {
            IdentityApiError::protocol(operation, "the identity response body is invalid", false)
        })?;
    OidcIdentity::try_from(identity)
        .map_err(|reason| IdentityApiError::protocol(operation, reason, false))
}

impl TryFrom<generated_models::OidcIdentityLink> for OidcIdentity {
    type Error = &'static str;

    fn try_from(value: generated_models::OidcIdentityLink) -> Result<Self, Self::Error> {
        if !valid_typed_id(&value.id, "idn_") {
            return Err("the identity ID is invalid");
        }
        http_util::require_nonempty(&value.issuer, "the identity issuer is empty")?;
        http_util::require_nonempty(&value.subject, "the identity subject is empty")?;
        http_util::require_nonempty(&value.created_at, "the identity creation time is empty")?;
        if value.asserted_email.as_deref() == Some("") {
            return Err("the asserted identity email is empty");
        }
        if value.email_verified.is_some() && value.asserted_email.is_none() {
            return Err("the identity email verification has no asserted email");
        }
        let kind = match value.kind {
            generated_models::oidc_identity_link::Kind::Oidc => IdentityKind::Oidc,
            generated_models::oidc_identity_link::Kind::WorkloadOidc => {
                return Err("the human identity kind is not OIDC");
            }
        };
        Ok(Self {
            id: value.id,
            kind,
            issuer: value.issuer,
            subject: value.subject,
            asserted_email: value.asserted_email,
            email_verified: value.email_verified,
            created_at: value.created_at,
            current: value.current,
        })
    }
}

fn require_response_idempotency_key(
    operation: Operation,
    response: &ReceivedResponse,
    expected: &str,
) -> Result<(), IdentityApiError> {
    if http_util::header_matches(response.idempotency_key.as_ref(), expected) {
        Ok(())
    } else {
        Err(IdentityApiError::protocol(
            operation,
            "the successful response has a missing or mismatched Idempotency-Key header",
            false,
        ))
    }
}

fn require_link_location(
    response: &ReceivedResponse,
    identity_id: &str,
) -> Result<(), IdentityApiError> {
    let expected = format!("/v1/me/identities/{identity_id}");
    if http_util::header_matches(response.location.as_ref(), &expected) {
        Ok(())
    } else {
        Err(IdentityApiError::protocol(
            Operation::Link,
            "the successful response has a missing or mismatched Location header",
            false,
        ))
    }
}

fn require_problem(
    operation: Operation,
    response: &ReceivedResponse,
    expected_type: &'static str,
    credential_rejected: bool,
) -> Result<(), IdentityApiError> {
    problem::require_type(response, expected_type)
        .map_err(|reason| IdentityApiError::protocol(operation, reason, credential_rejected))
}

// Each API domain maps the shared media-type validator into its own typed operation error.
// Keeping that one-line adapter local preserves useful identity operation context.
// jscpd:ignore-start
fn require_media_type(
    operation: Operation,
    response: &ReceivedResponse,
    expected: &'static str,
    credential_rejected: bool,
) -> Result<(), IdentityApiError> {
    http_util::require_media_type(response.content_type.as_deref(), expected)
        .map_err(|reason| IdentityApiError::protocol(operation, reason, credential_rejected))
}
// jscpd:ignore-end
