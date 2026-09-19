use std::error::Error;
use std::fmt;
use std::time::Duration;

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue, InvalidHeaderValue};
use reqwest::{Method, Response, StatusCode, Url};
use serde::{Deserialize, Deserializer, Serialize};
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

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const READ_ATTEMPTS: usize = 1;
const MUTATION_ATTEMPTS: usize = 2;
const IDEMPOTENCY_CONFLICT: &str = "https://api.scherzo.dev/problems/idempotency-conflict";
const QUANTITY_LIMIT_REACHED: &str = "https://api.scherzo.dev/problems/quantity-limit-reached";
const RATE_LIMIT_EXCEEDED: &str = "https://api.scherzo.dev/problems/rate-limit-exceeded";
const REQUEST_BODY_TOO_LARGE: &str = "https://api.scherzo.dev/problems/request-body-too-large";
const CREDENTIAL_REMOVAL_UNAVAILABLE: &str =
    "https://api.scherzo.dev/problems/credential-removal-unavailable";
const PLATFORM_CREDENTIAL_REQUIRED: &str =
    "https://api.scherzo.dev/problems/platform-credential-required";

struct SecretResponseString(Zeroizing<String>);

impl<'de> Deserialize<'de> for SecretResponseString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)
            .map(Zeroizing::new)
            .map(Self)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceCredentialSecretResponse {
    id: String,
    api_key: Option<SecretResponseString>,
    created_at: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateServicePrincipalResponse {
    principal: Box<generated_models::Principal>,
    initial_credential: ServiceCredentialSecretResponse,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServicePrincipal {
    pub id: String,
    pub r#type: &'static str,
    pub state: &'static str,
    pub display_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceCredential {
    pub id: String,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current: Option<bool>,
}

pub struct IssuedServiceApiKey(Zeroizing<String>);

impl IssuedServiceApiKey {
    pub fn new(value: Zeroizing<String>) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for IssuedServiceApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("IssuedServiceApiKey([REDACTED])")
    }
}

pub struct IssuedServiceCredential {
    pub credential: ServiceCredential,
    pub api_key: Option<IssuedServiceApiKey>,
}

impl fmt::Debug for IssuedServiceCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedServiceCredential")
            .field("credential", &self.credential)
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

#[derive(Debug)]
pub struct CreatedServicePrincipal {
    pub principal: ServicePrincipal,
    pub initial_credential: IssuedServiceCredential,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ServiceCredentialPage {
    pub items: Vec<ServiceCredential>,
    pub next_cursor: Option<String>,
}

#[derive(Debug)]
pub enum CreateServicePrincipalOutcome {
    Created(CreatedServicePrincipal),
    InvalidDisplayName,
    Unauthenticated,
    Forbidden,
    QuantityLimitReached,
    RateLimited { retry_after: u64 },
    IdempotencyConflict,
    RequestTooLarge,
    Unreachable(UnreachableCategory),
}

#[derive(Debug, Eq, PartialEq)]
pub enum ListServiceCredentialsOutcome {
    Listed(ServiceCredentialPage),
    InvalidInput,
    Unauthenticated,
    Forbidden,
    Unreachable(UnreachableCategory),
}

#[derive(Debug)]
pub enum IssueServiceCredentialOutcome {
    Issued(IssuedServiceCredential),
    InvalidInput,
    Unauthenticated,
    Forbidden,
    QuantityLimitReached,
    IdempotencyConflict,
    Unreachable(UnreachableCategory),
}

#[derive(Debug, Eq, PartialEq)]
pub enum RevokeServiceCredentialOutcome {
    Revoked,
    InvalidInput,
    Unauthenticated,
    Forbidden,
    NotFound,
    RemovalUnavailable,
    IdempotencyConflict,
    Unreachable(UnreachableCategory),
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    CreatePrincipal,
    ListCredentials,
    IssueCredential,
    RevokeCredential,
}

impl Operation {
    const fn name(self) -> &'static str {
        match self {
            Self::CreatePrincipal => "service-principal creation",
            Self::ListCredentials => "service-credential listing",
            Self::IssueCredential => "service-credential issuance",
            Self::RevokeCredential => "service-credential revocation",
        }
    }
}

struct SafeJsonError {
    source: serde_json::Error,
}

impl SafeJsonError {
    fn new(source: serde_json::Error) -> Self {
        Self { source }
    }

    fn category(&self) -> &'static str {
        match self.source.classify() {
            serde_json::error::Category::Io => "I/O",
            serde_json::error::Category::Syntax => "syntax",
            serde_json::error::Category::Data => "data",
            serde_json::error::Category::Eof => "end-of-input",
        }
    }
}

impl fmt::Debug for SafeJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeJsonError")
            .field("category", &self.category())
            .field("line", &self.source.line())
            .field("column", &self.source.column())
            .finish()
    }
}

impl fmt::Display for SafeJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} JSON error at line {}, column {}",
            self.category(),
            self.source.line(),
            self.source.column()
        )
    }
}

// serde_json may retain server-controlled values in its diagnostic text. The original
// error stays owned here, but deliberately is not exposed as this wrapper's source.
impl Error for SafeJsonError {}

#[derive(Debug)]
pub struct ServicePrincipalApiError {
    operation: Operation,
    kind: ServicePrincipalApiErrorKind,
    credential_rejected: bool,
}

// Service-principal failures retain their own operation vocabulary and secret-safe
// protocol diagnostics rather than sharing organization error construction.
// jscpd:ignore-start
impl ServicePrincipalApiError {
    pub fn credential_rejected(&self) -> bool {
        self.credential_rejected
    }

    fn local(operation: Operation, kind: ServicePrincipalApiErrorKind) -> Self {
        Self {
            operation,
            kind,
            credential_rejected: false,
        }
    }

    fn protocol(operation: Operation, reason: &'static str, credential_rejected: bool) -> Self {
        Self {
            operation,
            kind: ServicePrincipalApiErrorKind::Protocol { reason },
            credential_rejected,
        }
    }
}
// jscpd:ignore-end

#[derive(Debug)]
enum ServicePrincipalApiErrorKind {
    Endpoint(HttpEndpointError),
    InvalidAuthorizationHeader(InvalidHeaderValue),
    InvalidIdempotencyHeader(InvalidHeaderValue),
    SerializeRequest(SafeJsonError),
    DecodeCreatedPrincipal(SafeJsonError),
    DecodeCredentialPage(SafeJsonError),
    DecodeIssuedCredential(SafeJsonError),
    InvalidCredentialTimestamp(time::error::Parse),
    Protocol { reason: &'static str },
}

impl fmt::Display for ServicePrincipalApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ServicePrincipalApiErrorKind::Endpoint(HttpEndpointError::Invalid) => write!(
                formatter,
                "the deployment API URL cannot form the {} endpoint",
                self.operation.name()
            ),
            ServicePrincipalApiErrorKind::Endpoint(HttpEndpointError::InsecureHttp) => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            ServicePrincipalApiErrorKind::InvalidAuthorizationHeader(_) => write!(
                formatter,
                "the selected credential cannot be represented as a bearer credential"
            ),
            ServicePrincipalApiErrorKind::InvalidIdempotencyHeader(_) => write!(
                formatter,
                "the generated {} request identity is not a valid header value",
                self.operation.name()
            ),
            ServicePrincipalApiErrorKind::SerializeRequest(_) => write!(
                formatter,
                "the {} request could not be serialized",
                self.operation.name()
            ),
            ServicePrincipalApiErrorKind::DecodeCreatedPrincipal(_) => write!(
                formatter,
                "the {} response could not be decoded as a created service principal",
                self.operation.name()
            ),
            ServicePrincipalApiErrorKind::DecodeCredentialPage(_) => write!(
                formatter,
                "the {} response could not be decoded as a credential page",
                self.operation.name()
            ),
            ServicePrincipalApiErrorKind::DecodeIssuedCredential(_) => write!(
                formatter,
                "the {} response could not be decoded as an issued credential",
                self.operation.name()
            ),
            ServicePrincipalApiErrorKind::InvalidCredentialTimestamp(_) => write!(
                formatter,
                "the credential timestamp in the {} response could not be parsed",
                self.operation.name()
            ),
            ServicePrincipalApiErrorKind::Protocol { reason } => write!(
                formatter,
                "{} response violates the public API contract: {reason}",
                self.operation.name()
            ),
        }
    }
}

impl Error for ServicePrincipalApiError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            ServicePrincipalApiErrorKind::InvalidAuthorizationHeader(source)
            | ServicePrincipalApiErrorKind::InvalidIdempotencyHeader(source) => Some(source),
            ServicePrincipalApiErrorKind::SerializeRequest(source)
            | ServicePrincipalApiErrorKind::DecodeCreatedPrincipal(source)
            | ServicePrincipalApiErrorKind::DecodeCredentialPage(source)
            | ServicePrincipalApiErrorKind::DecodeIssuedCredential(source) => Some(source),
            ServicePrincipalApiErrorKind::InvalidCredentialTimestamp(source) => Some(source),
            ServicePrincipalApiErrorKind::Endpoint(_)
            | ServicePrincipalApiErrorKind::Protocol { .. } => None,
        }
    }
}

// This request plan owns secret-bearing service responses and retry counts, so it stays
// distinct from the organization transport plan despite their common HTTP fields.
// jscpd:ignore-start
struct RequestSpec {
    operation: Operation,
    method: Method,
    endpoint: Url,
    authorization: HeaderValue,
    idempotency_key: Option<HeaderValue>,
    content_type: Option<&'static str>,
    body: Option<Vec<u8>>,
    attempts: usize,
}
// jscpd:ignore-end

enum RequestExecution {
    Response(BufferedResponse),
    Unreachable(UnreachableCategory),
}

struct RequestInput<'a> {
    operation: Operation,
    method: Method,
    path: &'a [&'a str],
    credential: &'a str,
    idempotency_key: Option<&'a str>,
    content_type: Option<&'static str>,
    body: Option<Vec<u8>>,
    attempts: usize,
}

type AttemptError = ApiAttemptError<ServicePrincipalApiError>;

pub fn create_service_principal(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    idempotency_key: &str,
    display_name: &str,
) -> Result<CreateServicePrincipalOutcome, ServicePrincipalApiError> {
    let request = generated_models::CreateServicePrincipalRequest::new(display_name.to_owned());
    let body = serde_json::to_vec(&request).map_err(|source| {
        ServicePrincipalApiError::local(
            Operation::CreatePrincipal,
            ServicePrincipalApiErrorKind::SerializeRequest(SafeJsonError::new(source)),
        )
    })?;
    let spec = request_spec(
        client,
        api_url,
        RequestInput {
            operation: Operation::CreatePrincipal,
            method: Method::POST,
            path: &["v1", "service-principals"],
            credential: access_token,
            idempotency_key: Some(idempotency_key),
            content_type: Some(JSON_MEDIA_TYPE),
            body: Some(body),
            attempts: MUTATION_ATTEMPTS,
        },
    )?;
    match execute_request(client, &spec)? {
        RequestExecution::Response(response) => decode_create(response, idempotency_key),
        RequestExecution::Unreachable(category) => {
            Ok(CreateServicePrincipalOutcome::Unreachable(category))
        }
    }
}

pub fn list_service_credentials(
    client: &HttpClient,
    api_url: &str,
    api_key: &str,
    limit: Option<u16>,
    cursor: Option<&str>,
) -> Result<ListServiceCredentialsOutcome, ServicePrincipalApiError> {
    let mut spec = request_spec(
        client,
        api_url,
        RequestInput {
            operation: Operation::ListCredentials,
            method: Method::GET,
            path: &["v1", "me", "credentials"],
            credential: api_key,
            idempotency_key: None,
            content_type: None,
            body: None,
            attempts: READ_ATTEMPTS,
        },
    )?;
    http_util::append_pagination(&mut spec.endpoint, limit, cursor);
    match execute_request(client, &spec)? {
        RequestExecution::Response(response) => decode_list(response),
        RequestExecution::Unreachable(category) => {
            Ok(ListServiceCredentialsOutcome::Unreachable(category))
        }
    }
}

pub fn issue_service_credential(
    client: &HttpClient,
    api_url: &str,
    api_key: &str,
    idempotency_key: &str,
) -> Result<IssueServiceCredentialOutcome, ServicePrincipalApiError> {
    execute_credential_mutation(
        client,
        api_url,
        RequestInput {
            operation: Operation::IssueCredential,
            method: Method::POST,
            path: &["v1", "me", "credentials"],
            credential: api_key,
            idempotency_key: Some(idempotency_key),
            content_type: None,
            body: None,
            attempts: MUTATION_ATTEMPTS,
        },
        decode_issue,
        IssueServiceCredentialOutcome::Unreachable,
    )
}

pub fn revoke_service_credential(
    client: &HttpClient,
    api_url: &str,
    api_key: &str,
    credential_id: &str,
    idempotency_key: &str,
) -> Result<RevokeServiceCredentialOutcome, ServicePrincipalApiError> {
    execute_credential_mutation(
        client,
        api_url,
        RequestInput {
            operation: Operation::RevokeCredential,
            method: Method::DELETE,
            path: &["v1", "me", "credentials", credential_id],
            credential: api_key,
            idempotency_key: Some(idempotency_key),
            content_type: None,
            body: None,
            attempts: MUTATION_ATTEMPTS,
        },
        decode_revoke,
        RevokeServiceCredentialOutcome::Unreachable,
    )
}

fn execute_credential_mutation<T>(
    client: &HttpClient,
    api_url: &str,
    input: RequestInput<'_>,
    decode: impl FnOnce(BufferedResponse, &str) -> Result<T, ServicePrincipalApiError>,
    unreachable: impl FnOnce(UnreachableCategory) -> T,
) -> Result<T, ServicePrincipalApiError> {
    let idempotency_key = input.idempotency_key.ok_or_else(|| {
        ServicePrincipalApiError::protocol(
            input.operation,
            "credential mutation request omitted its idempotency key",
            false,
        )
    })?;
    let spec = request_spec(client, api_url, input)?;
    match execute_request(client, &spec)? {
        RequestExecution::Response(response) => decode(response, idempotency_key),
        RequestExecution::Unreachable(category) => Ok(unreachable(category)),
    }
}

// Service credential requests keep operation-specific secret and credential-rejection
// diagnostics; sharing another domain's request builder would erase that distinction.
// jscpd:ignore-start
fn request_spec(
    client: &HttpClient,
    api_url: &str,
    input: RequestInput<'_>,
) -> Result<RequestSpec, ServicePrincipalApiError> {
    let endpoint = client.endpoint(api_url, input.path).map_err(|error| {
        ServicePrincipalApiError::local(
            input.operation,
            ServicePrincipalApiErrorKind::Endpoint(error),
        )
    })?;
    let authorization = bearer_authorization(input.credential).map_err(|source| {
        ServicePrincipalApiError::local(
            input.operation,
            ServicePrincipalApiErrorKind::InvalidAuthorizationHeader(source),
        )
    })?;
    let idempotency_key = input
        .idempotency_key
        .map(HeaderValue::from_str)
        .transpose()
        .map_err(|source| {
            ServicePrincipalApiError::local(
                input.operation,
                ServicePrincipalApiErrorKind::InvalidIdempotencyHeader(source),
            )
        })?;
    Ok(RequestSpec {
        operation: input.operation,
        method: input.method,
        endpoint,
        authorization,
        idempotency_key,
        content_type: input.content_type,
        body: input.body,
        attempts: input.attempts,
    })
}
// jscpd:ignore-end

fn execute_request(
    client: &HttpClient,
    spec: &RequestSpec,
) -> Result<RequestExecution, ServicePrincipalApiError> {
    let mut last_failure = UnreachableCategory::Connection;
    for attempt in 0..spec.attempts {
        match client.run(REQUEST_TIMEOUT, send_and_receive(client, spec)) {
            Ok(Ok(response)) => return Ok(RequestExecution::Response(response)),
            Ok(Err(ApiAttemptError::Protocol(error))) => return Err(error),
            Ok(Err(ApiAttemptError::Transport(category))) => last_failure = category,
            Err(_) => last_failure = UnreachableCategory::Timeout,
        }
        if attempt + 1 < spec.attempts {
            scherzo_cloud_support::sleep(scherzo_cloud_support::short_retry_delay());
        }
    }
    Ok(RequestExecution::Unreachable(last_failure))
}

// The service transport deliberately mirrors the repository's strict HTTP boundary while
// retaining service-specific retries and redacted protocol errors.
// jscpd:ignore-start
async fn send_and_receive(
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
    if let Some(content_type) = spec.content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }
    if let Some(body) = &spec.body {
        request = request.body(body.clone());
    }
    let response = request.send().await.map_err(|error| {
        if error.is_builder() {
            ApiAttemptError::Protocol(ServicePrincipalApiError::protocol(
                spec.operation,
                "the request could not be constructed",
                false,
            ))
        } else {
            ApiAttemptError::Transport(classify_reqwest_error(&error))
        }
    })?;
    receive_response(spec.operation, response).await
}
// jscpd:ignore-end

async fn receive_response(
    operation: Operation,
    response: Response,
) -> Result<BufferedResponse, AttemptError> {
    http_util::buffer_api_response(response, |reason, rejected| {
        ServicePrincipalApiError::protocol(operation, reason, rejected)
    })
    .await
}

fn decode_create(
    response: BufferedResponse,
    idempotency_key: &str,
) -> Result<CreateServicePrincipalOutcome, ServicePrincipalApiError> {
    match response.status {
        StatusCode::CREATED => {
            require_success_json(Operation::CreatePrincipal, &response, idempotency_key)?;
            decode_created_principal(&response).map(CreateServicePrincipalOutcome::Created)
        }
        StatusCode::BAD_REQUEST => {
            require_problem(Operation::CreatePrincipal, &response, BAD_REQUEST, false)?;
            Ok(CreateServicePrincipalOutcome::InvalidDisplayName)
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(Operation::CreatePrincipal, &response, UNAUTHORIZED, true)?;
            Ok(CreateServicePrincipalOutcome::Unauthenticated)
        }
        StatusCode::FORBIDDEN => {
            require_problem(Operation::CreatePrincipal, &response, FORBIDDEN, false)?;
            Ok(CreateServicePrincipalOutcome::Forbidden)
        }
        StatusCode::CONFLICT => {
            match problem_type(Operation::CreatePrincipal, &response, false)?.as_str() {
                QUANTITY_LIMIT_REACHED => Ok(CreateServicePrincipalOutcome::QuantityLimitReached),
                IDEMPOTENCY_CONFLICT => Ok(CreateServicePrincipalOutcome::IdempotencyConflict),
                _ => Err(unrecognized_problem(Operation::CreatePrincipal, 409)),
            }
        }
        StatusCode::PAYLOAD_TOO_LARGE => {
            require_problem(
                Operation::CreatePrincipal,
                &response,
                REQUEST_BODY_TOO_LARGE,
                false,
            )?;
            Ok(CreateServicePrincipalOutcome::RequestTooLarge)
        }
        StatusCode::TOO_MANY_REQUESTS => {
            require_problem(
                Operation::CreatePrincipal,
                &response,
                RATE_LIMIT_EXCEEDED,
                false,
            )?;
            parse_retry_after(Operation::CreatePrincipal, &response)
                .map(|retry_after| CreateServicePrincipalOutcome::RateLimited { retry_after })
        }
        status if status.is_server_error() => Ok(CreateServicePrincipalOutcome::Unreachable(
            UnreachableCategory::Server,
        )),
        status if status.is_redirection() => Err(ServicePrincipalApiError::protocol(
            Operation::CreatePrincipal,
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(invalid_status(Operation::CreatePrincipal)),
    }
}

fn decode_list(
    response: BufferedResponse,
) -> Result<ListServiceCredentialsOutcome, ServicePrincipalApiError> {
    match response.status {
        StatusCode::OK => {
            require_json(Operation::ListCredentials, &response)?;
            decode_credential_page(&response).map(ListServiceCredentialsOutcome::Listed)
        }
        StatusCode::BAD_REQUEST => {
            require_problem(Operation::ListCredentials, &response, BAD_REQUEST, false)?;
            Ok(ListServiceCredentialsOutcome::InvalidInput)
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(Operation::ListCredentials, &response, UNAUTHORIZED, true)?;
            Ok(ListServiceCredentialsOutcome::Unauthenticated)
        }
        StatusCode::FORBIDDEN => {
            require_problem(Operation::ListCredentials, &response, FORBIDDEN, false)?;
            Ok(ListServiceCredentialsOutcome::Forbidden)
        }
        status if status.is_server_error() => Ok(ListServiceCredentialsOutcome::Unreachable(
            UnreachableCategory::Server,
        )),
        status if status.is_redirection() => Err(ServicePrincipalApiError::protocol(
            Operation::ListCredentials,
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(invalid_status(Operation::ListCredentials)),
    }
}

fn decode_issue(
    response: BufferedResponse,
    idempotency_key: &str,
) -> Result<IssueServiceCredentialOutcome, ServicePrincipalApiError> {
    match response.status {
        StatusCode::CREATED => {
            require_success_json(Operation::IssueCredential, &response, idempotency_key)?;
            decode_issued_credential(Operation::IssueCredential, &response)
                .map(IssueServiceCredentialOutcome::Issued)
        }
        StatusCode::BAD_REQUEST => {
            require_problem(Operation::IssueCredential, &response, BAD_REQUEST, false)?;
            Ok(IssueServiceCredentialOutcome::InvalidInput)
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(Operation::IssueCredential, &response, UNAUTHORIZED, true)?;
            Ok(IssueServiceCredentialOutcome::Unauthenticated)
        }
        StatusCode::FORBIDDEN => {
            require_one_of_problems(
                Operation::IssueCredential,
                &response,
                &[FORBIDDEN, PLATFORM_CREDENTIAL_REQUIRED],
            )?;
            Ok(IssueServiceCredentialOutcome::Forbidden)
        }
        StatusCode::CONFLICT => {
            match problem_type(Operation::IssueCredential, &response, false)?.as_str() {
                QUANTITY_LIMIT_REACHED => Ok(IssueServiceCredentialOutcome::QuantityLimitReached),
                IDEMPOTENCY_CONFLICT => Ok(IssueServiceCredentialOutcome::IdempotencyConflict),
                _ => Err(unrecognized_problem(Operation::IssueCredential, 409)),
            }
        }
        status if status.is_server_error() => Ok(IssueServiceCredentialOutcome::Unreachable(
            UnreachableCategory::Server,
        )),
        status if status.is_redirection() => Err(ServicePrincipalApiError::protocol(
            Operation::IssueCredential,
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(invalid_status(Operation::IssueCredential)),
    }
}

fn decode_revoke(
    response: BufferedResponse,
    idempotency_key: &str,
) -> Result<RevokeServiceCredentialOutcome, ServicePrincipalApiError> {
    match response.status {
        StatusCode::NO_CONTENT => {
            require_success_empty(Operation::RevokeCredential, &response, idempotency_key)?;
            Ok(RevokeServiceCredentialOutcome::Revoked)
        }
        StatusCode::BAD_REQUEST => {
            require_problem(Operation::RevokeCredential, &response, BAD_REQUEST, false)?;
            Ok(RevokeServiceCredentialOutcome::InvalidInput)
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(Operation::RevokeCredential, &response, UNAUTHORIZED, true)?;
            Ok(RevokeServiceCredentialOutcome::Unauthenticated)
        }
        StatusCode::FORBIDDEN => {
            require_one_of_problems(
                Operation::RevokeCredential,
                &response,
                &[FORBIDDEN, PLATFORM_CREDENTIAL_REQUIRED],
            )?;
            Ok(RevokeServiceCredentialOutcome::Forbidden)
        }
        StatusCode::NOT_FOUND => {
            require_problem(Operation::RevokeCredential, &response, NOT_FOUND, false)?;
            Ok(RevokeServiceCredentialOutcome::NotFound)
        }
        StatusCode::CONFLICT => {
            match problem_type(Operation::RevokeCredential, &response, false)?.as_str() {
                CREDENTIAL_REMOVAL_UNAVAILABLE => {
                    Ok(RevokeServiceCredentialOutcome::RemovalUnavailable)
                }
                IDEMPOTENCY_CONFLICT => Ok(RevokeServiceCredentialOutcome::IdempotencyConflict),
                _ => Err(unrecognized_problem(Operation::RevokeCredential, 409)),
            }
        }
        status if status.is_server_error() => Ok(RevokeServiceCredentialOutcome::Unreachable(
            UnreachableCategory::Server,
        )),
        status if status.is_redirection() => Err(ServicePrincipalApiError::protocol(
            Operation::RevokeCredential,
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(invalid_status(Operation::RevokeCredential)),
    }
}

fn decode_created_principal(
    response: &BufferedResponse,
) -> Result<CreatedServicePrincipal, ServicePrincipalApiError> {
    let value: CreateServicePrincipalResponse =
        serde_json::from_slice(&response.body).map_err(|source| {
            ServicePrincipalApiError::local(
                Operation::CreatePrincipal,
                ServicePrincipalApiErrorKind::DecodeCreatedPrincipal(SafeJsonError::new(source)),
            )
        })?;
    let principal = value.principal;
    if principal.r#type != generated_models::principal::Type::PrincipalTypeService
        || principal.id.is_empty()
        || principal.display_name.as_ref().is_none_or(String::is_empty)
    {
        return Err(ServicePrincipalApiError::protocol(
            Operation::CreatePrincipal,
            "the service principal fields are invalid",
            false,
        ));
    }
    let principal = ServicePrincipal {
        id: principal.id,
        r#type: "service",
        state: "active",
        display_name: principal.display_name.unwrap_or_default(),
    };
    let initial_credential =
        decode_credential_parts(Operation::CreatePrincipal, value.initial_credential, false)?;
    Ok(CreatedServicePrincipal {
        principal,
        initial_credential,
    })
}

fn decode_credential_page(
    response: &BufferedResponse,
) -> Result<ServiceCredentialPage, ServicePrincipalApiError> {
    let page: generated_models::ServiceCredentialList = serde_json::from_slice(&response.body)
        .map_err(|source| {
            ServicePrincipalApiError::local(
                Operation::ListCredentials,
                ServicePrincipalApiErrorKind::DecodeCredentialPage(SafeJsonError::new(source)),
            )
        })?;
    let items = page
        .items
        .into_iter()
        .map(|item| {
            validate_credential_metadata(
                Operation::ListCredentials,
                item.id,
                item.created_at,
                Some(item.current),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    if page.next_cursor.as_ref().is_some_and(String::is_empty) {
        return Err(ServicePrincipalApiError::protocol(
            Operation::ListCredentials,
            "the next cursor is empty",
            false,
        ));
    }
    Ok(ServiceCredentialPage {
        items,
        next_cursor: page.next_cursor,
    })
}

fn decode_issued_credential(
    operation: Operation,
    response: &BufferedResponse,
) -> Result<IssuedServiceCredential, ServicePrincipalApiError> {
    let credential: ServiceCredentialSecretResponse = serde_json::from_slice(&response.body)
        .map_err(|source| {
            ServicePrincipalApiError::local(
                operation,
                ServicePrincipalApiErrorKind::DecodeIssuedCredential(SafeJsonError::new(source)),
            )
        })?;
    decode_credential_parts(operation, credential, false)
}

fn decode_credential_parts(
    operation: Operation,
    credential: ServiceCredentialSecretResponse,
    current: bool,
) -> Result<IssuedServiceCredential, ServicePrincipalApiError> {
    let ServiceCredentialSecretResponse {
        id,
        api_key,
        created_at,
    } = credential;
    let api_key = api_key.map(|value| IssuedServiceApiKey::new(value.0));
    let metadata =
        validate_credential_metadata(operation, id, created_at, current.then_some(true))?;
    if api_key.as_ref().is_some_and(|key| {
        key.expose()
            .split_once('.')
            .is_none_or(|(id, _)| id != metadata.id)
    }) {
        return Err(ServicePrincipalApiError::protocol(
            operation,
            "the returned API key does not match the credential ID",
            false,
        ));
    }
    Ok(IssuedServiceCredential {
        credential: metadata,
        api_key,
    })
}

fn validate_credential_metadata(
    operation: Operation,
    id: String,
    created_at: String,
    current: Option<bool>,
) -> Result<ServiceCredential, ServicePrincipalApiError> {
    if !scherzo_cloud_support::valid_typed_id(&id, "crd_") {
        return Err(ServicePrincipalApiError::protocol(
            operation,
            "the credential metadata is invalid",
            false,
        ));
    }
    OffsetDateTime::parse(&created_at, &Rfc3339).map_err(|source| {
        ServicePrincipalApiError::local(
            operation,
            ServicePrincipalApiErrorKind::InvalidCredentialTimestamp(source),
        )
    })?;
    Ok(ServiceCredential {
        id,
        created_at,
        current,
    })
}

fn require_success_json(
    operation: Operation,
    response: &BufferedResponse,
    idempotency_key: &str,
) -> Result<(), ServicePrincipalApiError> {
    require_response_idempotency(operation, response, idempotency_key)?;
    require_json(operation, response)
}

fn require_success_empty(
    operation: Operation,
    response: &BufferedResponse,
    idempotency_key: &str,
) -> Result<(), ServicePrincipalApiError> {
    require_response_idempotency(operation, response, idempotency_key)?;
    if response.content_type.is_some() || !response.body.is_empty() {
        return Err(ServicePrincipalApiError::protocol(
            operation,
            "the successful response must have no content type or body",
            false,
        ));
    }
    Ok(())
}

fn require_response_idempotency(
    operation: Operation,
    response: &BufferedResponse,
    idempotency_key: &str,
) -> Result<(), ServicePrincipalApiError> {
    if !http_util::header_matches(response.idempotency_key.as_ref(), idempotency_key) {
        return Err(ServicePrincipalApiError::protocol(
            operation,
            "the successful response has a missing or mismatched Idempotency-Key header",
            false,
        ));
    }
    Ok(())
}

fn require_json(
    operation: Operation,
    response: &BufferedResponse,
) -> Result<(), ServicePrincipalApiError> {
    http_util::require_media_type(response.content_type.as_deref(), JSON_MEDIA_TYPE)
        .map_err(|reason| ServicePrincipalApiError::protocol(operation, reason, false))
}

fn problem_type(
    operation: Operation,
    response: &BufferedResponse,
    credential_rejected: bool,
) -> Result<String, ServicePrincipalApiError> {
    problem::decode_type(response).map_err(|reason| {
        ServicePrincipalApiError::protocol(operation, reason, credential_rejected)
    })
}

fn require_problem(
    operation: Operation,
    response: &BufferedResponse,
    expected: &str,
    credential_rejected: bool,
) -> Result<(), ServicePrincipalApiError> {
    if problem_type(operation, response, credential_rejected)? == expected {
        Ok(())
    } else {
        Err(ServicePrincipalApiError::protocol(
            operation,
            "the problem type is not valid for its HTTP status",
            credential_rejected,
        ))
    }
}

fn require_one_of_problems(
    operation: Operation,
    response: &BufferedResponse,
    expected: &[&str],
) -> Result<(), ServicePrincipalApiError> {
    let actual = problem_type(operation, response, false)?;
    if expected.contains(&actual.as_str()) {
        Ok(())
    } else {
        Err(ServicePrincipalApiError::protocol(
            operation,
            "the problem type is not valid for its HTTP status",
            false,
        ))
    }
}

fn parse_retry_after(
    operation: Operation,
    response: &BufferedResponse,
) -> Result<u64, ServicePrincipalApiError> {
    response
        .retry_after
        .as_ref()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            ServicePrincipalApiError::protocol(
                operation,
                "the Retry-After header is missing or invalid",
                false,
            )
        })
}

fn invalid_status(operation: Operation) -> ServicePrincipalApiError {
    ServicePrincipalApiError::protocol(
        operation,
        "the HTTP status is not valid for this operation",
        false,
    )
}

fn unrecognized_problem(operation: Operation, status: u16) -> ServicePrincipalApiError {
    let reason = match status {
        409 => "a 409 response has an unrecognized problem type",
        _ => "the response has an unrecognized problem type",
    };
    ServicePrincipalApiError::protocol(operation, reason, false)
}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "service-principal API unit tests use Rust test assertions and fixture extraction"
)]
mod tests {
    use super::*;
    use crate::HttpTransportPolicy;
    use scherzo_cloud_test_support::{REQUEST_IDEMPOTENCY_KEY_ECHO, ScriptedHttpServer};

    const CREDENTIAL_ID: &str = "crd_01k0z6r1w8f4jy2m7q9v3x5abc";
    const API_KEY: &str =
        "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const CREATED_AT: &str = "2026-01-02T03:04:05Z";

    fn client() -> HttpClient {
        HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).unwrap()
    }

    fn response(status: &str, headers: &str, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status}\r\nConnection: close\r\n{headers}Content-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    #[test]
    fn creation_sends_human_bearer_and_returns_a_redacted_one_time_key() {
        let body = format!(
            r#"{{"principal":{{"id":"prn_service","type":"service","state":"active","displayName":"Build agent"}},"initialCredential":{{"id":"{CREDENTIAL_ID}","apiKey":"{API_KEY}","createdAt":"{CREATED_AT}"}}}}"#
        );
        let server = ScriptedHttpServer::respond(response(
            "201 Created",
            &format!(
                "Content-Type: application/json\r\nIdempotency-Key: {REQUEST_IDEMPOTENCY_KEY_ECHO}\r\n"
            ),
            &body,
        ));

        let outcome = create_service_principal(
            &client(),
            &server.api_url,
            "human-access-token",
            "request-key",
            "Build agent",
        )
        .unwrap();
        let CreateServicePrincipalOutcome::Created(created) = outcome else {
            panic!("creation should succeed");
        };
        assert_eq!(created.principal.r#type, "service");
        assert_eq!(created.initial_credential.credential.id, CREDENTIAL_ID);
        let key = created
            .initial_credential
            .api_key
            .as_ref()
            .expect("first response should contain the key");
        assert_eq!(key.expose(), API_KEY);
        assert!(!format!("{created:?}").contains(API_KEY));

        let request = server.finish_one();
        assert!(request.starts_with("POST /api/v1/service-principals HTTP/1.1\r\n"));
        assert!(request.contains("authorization: Bearer human-access-token\r\n"));
        assert!(request.contains("idempotency-key: request-key\r\n"));
        assert!(request.ends_with(r#"{"displayName":"Build agent"}"#));
    }

    #[test]
    fn issuance_replay_preserves_metadata_without_inventing_a_key() {
        let body = format!(r#"{{"id":"{CREDENTIAL_ID}","createdAt":"{CREATED_AT}"}}"#);
        let server = ScriptedHttpServer::respond(response(
            "201 Created",
            &format!(
                "Content-Type: application/json\r\nIdempotency-Key: {REQUEST_IDEMPOTENCY_KEY_ECHO}\r\n"
            ),
            &body,
        ));

        let outcome =
            issue_service_credential(&client(), &server.api_url, API_KEY, "issue-key").unwrap();
        let IssueServiceCredentialOutcome::Issued(issued) = outcome else {
            panic!("issuance replay should succeed");
        };
        assert_eq!(issued.credential.id, CREDENTIAL_ID);
        assert!(issued.api_key.is_none());
        let request = server.finish_one();
        assert!(request.contains(&format!("authorization: Bearer {API_KEY}\r\n")));
        assert!(request.ends_with("\r\n\r\n"));
        assert!(!request.contains("content-type:"));
    }

    #[test]
    fn list_and_revoke_validate_non_secret_lifecycle_contracts() {
        let list_body = format!(
            r#"{{"items":[{{"id":"{CREDENTIAL_ID}","createdAt":"{CREATED_AT}","current":true}}],"nextCursor":"next"}}"#
        );
        let list_server = ScriptedHttpServer::respond(response(
            "200 OK",
            "Content-Type: application/json\r\n",
            &list_body,
        ));
        let listed = list_service_credentials(
            &client(),
            &list_server.api_url,
            API_KEY,
            Some(25),
            Some("cursor"),
        )
        .unwrap();
        assert_eq!(
            listed,
            ListServiceCredentialsOutcome::Listed(ServiceCredentialPage {
                items: vec![ServiceCredential {
                    id: CREDENTIAL_ID.to_owned(),
                    created_at: CREATED_AT.to_owned(),
                    current: Some(true),
                }],
                next_cursor: Some("next".to_owned()),
            })
        );
        let list_request = list_server.finish_one();
        assert!(
            list_request
                .starts_with("GET /api/v1/me/credentials?limit=25&cursor=cursor HTTP/1.1\r\n")
        );

        let revoke_server = ScriptedHttpServer::respond(response(
            "204 No Content",
            &format!("Idempotency-Key: {REQUEST_IDEMPOTENCY_KEY_ECHO}\r\n"),
            "",
        ));
        let revoked = revoke_service_credential(
            &client(),
            &revoke_server.api_url,
            API_KEY,
            CREDENTIAL_ID,
            "revoke-key",
        )
        .unwrap();
        assert_eq!(revoked, RevokeServiceCredentialOutcome::Revoked);
        let revoke_request = revoke_server.finish_one();
        assert!(revoke_request.starts_with(&format!(
            "DELETE /api/v1/me/credentials/{CREDENTIAL_ID} HTTP/1.1\r\n"
        )));
    }

    #[test]
    fn malformed_success_response_retains_a_safe_source_without_exposing_server_data() {
        const RESPONSE_SECRET: &str = "server-secret-sentinel-must-not-escape";
        let body = format!(r#"{{"items":"{RESPONSE_SECRET}"}}"#);
        let server = ScriptedHttpServer::respond(response(
            "200 OK",
            "Content-Type: application/json\r\n",
            &body,
        ));

        let error = list_service_credentials(&client(), &server.api_url, API_KEY, None, None)
            .expect_err("malformed credential list should fail");
        assert!(matches!(
            &error.kind,
            ServicePrincipalApiErrorKind::DecodeCredentialPage(_)
        ));
        let source = error
            .source()
            .expect("response decoding failure should retain a safe source");
        assert!(source.source().is_none());
        let boundary_rendering = format!("{error}\n{error:?}\n{source}\n{source:?}");
        assert!(!boundary_rendering.contains(RESPONSE_SECRET));

        let report = anyhow::Error::new(error);
        let report_rendering = format!("{report:#}\n{report:?}");
        assert!(!report_rendering.contains(RESPONSE_SECRET));
        server.finish_one();
    }

    #[test]
    fn malformed_unauthorized_response_is_retryable_for_humans_without_exposing_bearers() {
        let server = ScriptedHttpServer::respond(response(
            "401 Unauthorized",
            "Content-Type: text/plain\r\n",
            "not a problem",
        ));
        let error = create_service_principal(
            &client(),
            &server.api_url,
            "sensitive-human-token",
            "request-key",
            "Build agent",
        )
        .unwrap_err();

        assert!(error.credential_rejected());
        assert!(!error.to_string().contains("sensitive-human-token"));
        server.finish_one();
    }
}
