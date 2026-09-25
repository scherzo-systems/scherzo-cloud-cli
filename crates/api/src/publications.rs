use std::fmt;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Method, StatusCode, Url};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

// Resource adapters keep their generated model and problem vocabularies local so Publication
// validation does not depend on the Run adapter's private contract surface.
// jscpd:ignore-start
use super::generated::{apis, models};
use super::http_client::generated_configuration;
use super::http_util::{self, BoundedBodyError, BufferedBlockingResponse};
use super::problem::{
    self, BAD_REQUEST, FORBIDDEN, JSON_MEDIA_TYPE, NOT_FOUND, PROBLEM_MEDIA_TYPE, UNAUTHORIZED,
};
use super::{HttpTransportPolicy, UnreachableCategory, classify_reqwest_error};
// jscpd:ignore-end

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const CREATE_ATTEMPTS: usize = 2;
const DEFAULT_LIST_LIMIT: usize = 50;
const PRIVATE_CACHE_CONTROL: &str = "private, no-store";
const GONE: &str = "https://api.scherzo.dev/problems/gone";
const IDEMPOTENCY_CONFLICT: &str = "https://api.scherzo.dev/problems/idempotency-conflict";
const RUN_NOT_PUBLISHABLE: &str = "https://api.scherzo.dev/problems/run-not-publishable";
const EXPORT_NOT_PUBLISHABLE: &str = "https://api.scherzo.dev/problems/export-not-publishable";
const TARGET_MISMATCH: &str = "https://api.scherzo.dev/problems/publication-target-mismatch";
const REQUEST_BODY_TOO_LARGE: &str = "https://api.scherzo.dev/problems/request-body-too-large";
const UNSUPPORTED_MEDIA_TYPE: &str = "https://api.scherzo.dev/problems/unsupported-media-type";
const INTERNAL_SERVER_ERROR: &str = "https://api.scherzo.dev/problems/internal-server-error";
const RETRYABLE_CONFLICT: &str = "https://api.scherzo.dev/problems/retryable-conflict";

pub type Publication = models::Publication;
pub type PublicationList = models::PublicationList;
pub type PublicationState = models::publication::State;

pub struct PublicationApi {
    configuration: apis::configuration::Configuration,
}

impl PublicationApi {
    pub fn new(
        api_url: &str,
        access_token: &str,
        transport_policy: HttpTransportPolicy,
    ) -> Result<Self, PublicationApiError> {
        let parsed = Url::parse(api_url).map_err(|_| PublicationApiError::InvalidEndpoint)?;
        if !transport_policy.permits(&parsed) {
            return Err(if parsed.scheme() == "http" {
                PublicationApiError::InsecureHttp
            } else {
                PublicationApiError::InvalidEndpoint
            });
        }
        if parsed.cannot_be_a_base() || parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(PublicationApiError::InvalidEndpoint);
        }
        let configuration =
            generated_configuration(api_url, access_token, transport_policy, REQUEST_TIMEOUT)
                .map_err(PublicationApiError::BuildClient)?;
        Ok(Self { configuration })
    }

    pub fn create(
        &self,
        organization: &str,
        run_id: &str,
        export_name: &str,
        idempotency_key: &str,
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<Publication, PublicationFailure> {
        let endpoint = format!(
            "{}/v1/organizations/{}/runs/{}/publications",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(organization),
            apis::urlencode(run_id)
        );
        let request = models::CreatePublicationRequest::new(export_name.to_owned());
        let mut last_transport_failure = UnreachableCategory::Connection;
        for attempt in 0..CREATE_ATTEMPTS {
            if !begin_dispatch() {
                return Err(PublicationFailure::Interrupted);
            }
            let response =
                match super::generated_api_request(&self.configuration, Method::POST, &endpoint)
                    .header("Idempotency-Key", idempotency_key)
                    .json(&request)
                    .send()
                {
                    Ok(response) => response,
                    Err(error) => {
                        let category = classify_reqwest_error(&error);
                        if retry_transport_failure(attempt, category, &mut last_transport_failure) {
                            continue;
                        }
                        return Err(PublicationFailure::Unreachable(category));
                    }
                };
            let status = response.status();
            if status == StatusCode::ACCEPTED {
                require_retryable_success_headers(response.headers(), idempotency_key)?;
            }
            match http_util::buffer_blocking_response(response) {
                Ok(response) => {
                    return decode_create_response(response, run_id, export_name, idempotency_key);
                }
                Err(BoundedBodyError::TooLarge) => {
                    return Err(PublicationFailure::protocol(
                        status == StatusCode::UNAUTHORIZED,
                    ));
                }
                Err(BoundedBodyError::Transport(error)) if status == StatusCode::ACCEPTED => {
                    let category = classify_reqwest_error(&error);
                    if retry_transport_failure(attempt, category, &mut last_transport_failure) {
                        continue;
                    }
                    return Err(PublicationFailure::Unreachable(category));
                }
                Err(BoundedBodyError::Transport(_)) if status == StatusCode::UNAUTHORIZED => {
                    return Err(PublicationFailure::protocol(true));
                }
                Err(BoundedBodyError::Transport(error)) => {
                    return Err(PublicationFailure::Unreachable(
                        if status.is_server_error() {
                            UnreachableCategory::Server
                        } else {
                            classify_reqwest_error(&error)
                        },
                    ));
                }
            }
        }
        Err(PublicationFailure::Unreachable(last_transport_failure))
    }

    pub fn get(
        &self,
        organization: &str,
        run_id: &str,
        publication_id: &str,
    ) -> Result<Publication, PublicationFailure> {
        self.get_with_timeout(organization, run_id, publication_id, None)
    }

    pub fn get_with_timeout(
        &self,
        organization: &str,
        run_id: &str,
        publication_id: &str,
        timeout: Option<Duration>,
    ) -> Result<Publication, PublicationFailure> {
        let endpoint = format!(
            "{}/{}",
            self.collection_endpoint(organization, run_id),
            apis::urlencode(publication_id)
        );
        let mut request = super::generated_api_request(&self.configuration, Method::GET, &endpoint);
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }
        let response = self.read_response(request)?;
        decode_get_response(response, run_id, publication_id)
    }

    pub fn list(
        &self,
        organization: &str,
        run_id: &str,
        limit: Option<u16>,
        cursor: Option<&str>,
    ) -> Result<PublicationList, PublicationFailure> {
        let endpoint = self.collection_endpoint(organization, run_id);
        let mut request = super::generated_api_request(&self.configuration, Method::GET, &endpoint);
        if let Some(limit) = limit {
            request = request.query(&[("limit", limit)]);
        }
        if let Some(cursor) = cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        let response = self.read_response(request)?;
        decode_list_response(
            response,
            run_id,
            limit.map_or(DEFAULT_LIST_LIMIT, usize::from),
        )
    }

    fn collection_endpoint(&self, organization: &str, run_id: &str) -> String {
        format!(
            "{}/v1/organizations/{}/runs/{}/publications",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(organization),
            apis::urlencode(run_id)
        )
    }

    fn read_response(
        &self,
        request: reqwest::blocking::RequestBuilder,
    ) -> Result<BufferedBlockingResponse, PublicationFailure> {
        let response = request
            .send()
            .map_err(|error| PublicationFailure::Unreachable(classify_reqwest_error(&error)))?;
        let status = response.status();
        http_util::buffer_blocking_response(response).map_err(|error| match error {
            BoundedBodyError::TooLarge => {
                PublicationFailure::protocol(status == StatusCode::UNAUTHORIZED)
            }
            BoundedBodyError::Transport(_) if status == StatusCode::UNAUTHORIZED => {
                PublicationFailure::protocol(true)
            }
            BoundedBodyError::Transport(error) => {
                PublicationFailure::Unreachable(if status.is_server_error() {
                    UnreachableCategory::Server
                } else {
                    classify_reqwest_error(&error)
                })
            }
        })
    }
}

impl Drop for PublicationApi {
    fn drop(&mut self) {
        super::clear_generated_access_token(&mut self.configuration);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    NotFound,
    Conflict,
    Gone,
    Unreachable(UnreachableCategory),
    Interrupted,
    Protocol { credential_rejected: bool },
}

impl PublicationFailure {
    pub fn credential_rejected(&self) -> bool {
        match self {
            Self::Unauthenticated => true,
            Self::Protocol {
                credential_rejected,
            } => *credential_rejected,
            _ => false,
        }
    }

    pub fn retryable_observation(&self) -> bool {
        matches!(self, Self::Unreachable(category) if category.retryable_observation())
    }

    fn protocol(credential_rejected: bool) -> Self {
        Self::Protocol {
            credential_rejected,
        }
    }
}

fn retry_transport_failure(
    attempt: usize,
    category: UnreachableCategory,
    last_failure: &mut UnreachableCategory,
) -> bool {
    *last_failure = category;
    let retry = http_util::can_retry_ambiguous_mutation(attempt, CREATE_ATTEMPTS, category);
    if retry {
        scherzo_cloud_support::sleep(scherzo_cloud_support::short_retry_delay());
    }
    retry
}

fn require_retryable_success_headers(
    headers: &HeaderMap,
    idempotency_key: &str,
) -> Result<(), PublicationFailure> {
    require_exact_header(headers.get_all("Idempotency-Key").iter(), idempotency_key)?;
    require_exact_header(
        headers.get_all(reqwest::header::CACHE_CONTROL).iter(),
        PRIVATE_CACHE_CONTROL,
    )?;
    let content_type = headers
        .get(reqwest::header::CONTENT_TYPE)
        .map(http_util::media_type)
        .transpose()
        .map_err(|_| PublicationFailure::protocol(false))?;
    if content_type.as_deref() != Some(JSON_MEDIA_TYPE) {
        return Err(PublicationFailure::protocol(false));
    }
    let mut locations = headers.get_all(reqwest::header::LOCATION).iter();
    if locations
        .next()
        .and_then(|location| location.to_str().ok())
        .is_none_or(str::is_empty)
        || locations.next().is_some()
    {
        return Err(PublicationFailure::protocol(false));
    }
    Ok(())
}

fn decode_create_response(
    response: BufferedBlockingResponse,
    requested_run_id: &str,
    requested_export_name: &str,
    expected_idempotency_key: &str,
) -> Result<Publication, PublicationFailure> {
    if response.status != StatusCode::ACCEPTED {
        return Err(classify_failure(&response, Operation::Create));
    }
    require_media_type(&response, JSON_MEDIA_TYPE, false)?;
    require_exact_header(response.idempotency_keys.iter(), expected_idempotency_key)?;
    require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
    let publication = decode_closed_publication(&response.body)?;
    validate_publication(
        &publication,
        requested_run_id,
        Some(requested_export_name),
        None,
    )?;
    let expected_location = format!(
        "/v1/organizations/{}/runs/{}/publications/{}",
        apis::urlencode(&publication.organization_id),
        apis::urlencode(requested_run_id),
        apis::urlencode(&publication.id)
    );
    require_exact_header(response.locations.iter(), &expected_location)?;
    Ok(publication)
}

fn decode_get_response(
    response: BufferedBlockingResponse,
    requested_run_id: &str,
    requested_publication_id: &str,
) -> Result<Publication, PublicationFailure> {
    if response.status != StatusCode::OK {
        return Err(classify_failure(&response, Operation::Get));
    }
    require_media_type(&response, JSON_MEDIA_TYPE, false)?;
    require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
    let publication = decode_closed_publication(&response.body)?;
    validate_publication(
        &publication,
        requested_run_id,
        None,
        Some(requested_publication_id),
    )?;
    Ok(publication)
}

fn decode_list_response(
    response: BufferedBlockingResponse,
    requested_run_id: &str,
    maximum_items: usize,
) -> Result<PublicationList, PublicationFailure> {
    if response.status != StatusCode::OK {
        return Err(classify_failure(&response, Operation::List));
    }
    require_media_type(&response, JSON_MEDIA_TYPE, false)?;
    require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
    decode_closed_publication_list(&response.body, requested_run_id, maximum_items)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Operation {
    Create,
    Get,
    List,
}

fn classify_failure(
    response: &BufferedBlockingResponse,
    operation: Operation,
) -> PublicationFailure {
    match response.status {
        StatusCode::BAD_REQUEST => validated_problem_failure(
            response,
            &[BAD_REQUEST],
            PublicationFailure::InvalidInput,
            false,
        ),
        StatusCode::UNAUTHORIZED => validated_problem_failure(
            response,
            &[UNAUTHORIZED],
            PublicationFailure::Unauthenticated,
            true,
        ),
        StatusCode::FORBIDDEN => {
            validated_problem_failure(response, &[FORBIDDEN], PublicationFailure::Forbidden, false)
        }
        StatusCode::NOT_FOUND => {
            validated_problem_failure(response, &[NOT_FOUND], PublicationFailure::NotFound, false)
        }
        StatusCode::CONFLICT if operation == Operation::Create => validated_problem_failure(
            response,
            &[
                IDEMPOTENCY_CONFLICT,
                RUN_NOT_PUBLISHABLE,
                EXPORT_NOT_PUBLISHABLE,
                TARGET_MISMATCH,
            ],
            PublicationFailure::Conflict,
            false,
        ),
        StatusCode::GONE if operation == Operation::Create => {
            validated_problem_failure(response, &[GONE], PublicationFailure::Gone, false)
        }
        StatusCode::PAYLOAD_TOO_LARGE if operation == Operation::Create => {
            validated_problem_failure(
                response,
                &[REQUEST_BODY_TOO_LARGE],
                PublicationFailure::InvalidInput,
                false,
            )
        }
        StatusCode::UNSUPPORTED_MEDIA_TYPE if operation == Operation::Create => {
            validated_problem_failure(
                response,
                &[UNSUPPORTED_MEDIA_TYPE],
                PublicationFailure::InvalidInput,
                false,
            )
        }
        StatusCode::INTERNAL_SERVER_ERROR => validated_problem_failure(
            response,
            &[INTERNAL_SERVER_ERROR],
            PublicationFailure::Unreachable(UnreachableCategory::Server),
            false,
        ),
        StatusCode::SERVICE_UNAVAILABLE if operation == Operation::Create => {
            let failure = validated_problem_failure(
                response,
                &[RETRYABLE_CONFLICT],
                PublicationFailure::Unreachable(UnreachableCategory::Server),
                false,
            );
            if matches!(failure, PublicationFailure::Protocol { .. })
                || !valid_retry_after(&response.retry_afters)
            {
                PublicationFailure::protocol(false)
            } else {
                failure
            }
        }
        _ => PublicationFailure::protocol(false),
    }
}

fn validated_problem_failure(
    response: &BufferedBlockingResponse,
    expected_types: &[&str],
    failure: PublicationFailure,
    credential_rejected: bool,
) -> PublicationFailure {
    if require_media_type(response, PROBLEM_MEDIA_TYPE, credential_rejected).is_err() {
        return PublicationFailure::protocol(credential_rejected);
    }
    let decoded = match problem::decode(&response.body, response.status) {
        Ok(decoded) => decoded,
        Err(_) => return PublicationFailure::protocol(credential_rejected),
    };
    if expected_types.contains(&decoded.r#type.as_str()) {
        failure
    } else {
        PublicationFailure::protocol(credential_rejected)
    }
}

fn valid_retry_after(values: &[HeaderValue]) -> bool {
    let mut values = values.iter();
    values
        .next()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|seconds| seconds > 0)
        && values.next().is_none()
}

fn require_exact_header<'a>(
    mut values: impl Iterator<Item = &'a HeaderValue>,
    expected: &str,
) -> Result<(), PublicationFailure> {
    if values.next().and_then(|value| value.to_str().ok()) == Some(expected)
        && values.next().is_none()
    {
        Ok(())
    } else {
        Err(PublicationFailure::protocol(false))
    }
}

fn require_media_type(
    response: &BufferedBlockingResponse,
    expected: &str,
    credential_rejected: bool,
) -> Result<(), PublicationFailure> {
    let actual = response
        .content_type
        .as_ref()
        .map(http_util::media_type)
        .transpose()
        .map_err(|_| PublicationFailure::protocol(credential_rejected))?;
    if actual.as_deref() == Some(expected) {
        Ok(())
    } else {
        Err(PublicationFailure::protocol(credential_rejected))
    }
}

fn decode_closed_publication(body: &[u8]) -> Result<Publication, PublicationFailure> {
    let value = scherzo_cloud_support::strict_json_from_slice(body)
        .map_err(|_| PublicationFailure::protocol(false))?;
    decode_closed_publication_value(value)
}

fn decode_closed_publication_value(
    value: serde_json::Value,
) -> Result<Publication, PublicationFailure> {
    require_closed_object(
        &value,
        &[
            "id",
            "organizationId",
            "projectId",
            "runId",
            "artifactSetId",
            "exportName",
            "state",
            "version",
            "artifact",
            "target",
            "pullRequestMetadata",
            "branch",
            "pullRequest",
            "outcome",
            "failure",
            "actorPrincipalId",
            "createdAt",
            "updatedAt",
            "startedAt",
            "terminalAt",
        ],
    )?;
    require_closed_field(&value, "artifact", ARTIFACT_FIELDS, false)?;
    require_closed_field(&value, "target", TARGET_FIELDS, false)?;
    require_closed_field(
        &value,
        "pullRequestMetadata",
        PULL_REQUEST_METADATA_FIELDS,
        false,
    )?;
    require_closed_field(&value, "branch", BRANCH_FIELDS, true)?;
    require_closed_field(&value, "pullRequest", PULL_REQUEST_FIELDS, true)?;
    require_closed_field(&value, "failure", FAILURE_FIELDS, true)?;
    serde_json::from_value(value).map_err(|_| PublicationFailure::protocol(false))
}

fn decode_closed_publication_list(
    body: &[u8],
    requested_run_id: &str,
    maximum_items: usize,
) -> Result<PublicationList, PublicationFailure> {
    let value = scherzo_cloud_support::strict_json_from_slice(body)
        .map_err(|_| PublicationFailure::protocol(false))?;
    let object = value
        .as_object()
        .ok_or_else(|| PublicationFailure::protocol(false))?;
    if !object.contains_key("items")
        || object
            .keys()
            .any(|field| !matches!(field.as_str(), "items" | "nextCursor"))
    {
        return Err(PublicationFailure::protocol(false));
    }
    let raw_items = object
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| PublicationFailure::protocol(false))?;
    if raw_items.len() > maximum_items {
        return Err(PublicationFailure::protocol(false));
    }
    let items = raw_items
        .iter()
        .cloned()
        .map(decode_closed_publication_value)
        .map(|result| {
            result.and_then(|publication| {
                validate_publication(&publication, requested_run_id, None, None)?;
                Ok(publication)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if items.windows(2).any(|pair| {
        let first_created = timestamp(&pair[0].created_at);
        let second_created = timestamp(&pair[1].created_at);
        match (first_created, second_created) {
            (Ok(first), Ok(second)) => {
                first > second || (first == second && pair[0].id >= pair[1].id)
            }
            _ => true,
        }
    }) {
        return Err(PublicationFailure::protocol(false));
    }
    let next_cursor = match object.get("nextCursor") {
        None => None,
        Some(serde_json::Value::String(cursor)) if (1..=2048).contains(&cursor.chars().count()) => {
            Some(cursor.clone())
        }
        Some(_) => return Err(PublicationFailure::protocol(false)),
    };
    Ok(PublicationList { items, next_cursor })
}

const ARTIFACT_FIELDS: &[&str] = &[
    "artifactVersion",
    "objectFormat",
    "baseOid",
    "headOid",
    "treeOid",
    "expiresAt",
];
const TARGET_FIELDS: &[&str] = &[
    "repositoryConnectionId",
    "providerRepositoryId",
    "fullName",
    "baseBranch",
    "destinationBranch",
];
const PULL_REQUEST_METADATA_FIELDS: &[&str] = &["title", "body"];
const BRANCH_FIELDS: &[&str] = &["headOid", "disposition", "url"];
const PULL_REQUEST_FIELDS: &[&str] = &["providerId", "number", "url", "disposition", "state"];
const FAILURE_FIELDS: &[&str] = &["phase", "code", "retryable"];

fn require_closed_field(
    root: &serde_json::Value,
    field: &str,
    expected: &[&str],
    nullable: bool,
) -> Result<(), PublicationFailure> {
    let value = root
        .get(field)
        .ok_or_else(|| PublicationFailure::protocol(false))?;
    if nullable && value.is_null() {
        Ok(())
    } else {
        require_closed_object(value, expected)
    }
}

fn require_closed_object(
    value: &serde_json::Value,
    expected: &[&str],
) -> Result<(), PublicationFailure> {
    let object = value
        .as_object()
        .ok_or_else(|| PublicationFailure::protocol(false))?;
    if object.len() == expected.len() && expected.iter().all(|field| object.contains_key(*field)) {
        Ok(())
    } else {
        Err(PublicationFailure::protocol(false))
    }
}

fn validate_publication(
    publication: &Publication,
    requested_run_id: &str,
    requested_export_name: Option<&str>,
    requested_publication_id: Option<&str>,
) -> Result<(), PublicationFailure> {
    use models::publication::{Outcome, State};

    let created_at = timestamp(&publication.created_at)?;
    let updated_at = timestamp(&publication.updated_at)?;
    let expires_at = timestamp(&publication.artifact.expires_at)?;
    let started_at = publication
        .started_at
        .as_deref()
        .map(timestamp)
        .transpose()?;
    let terminal_at = publication
        .terminal_at
        .as_deref()
        .map(timestamp)
        .transpose()?;
    let identities_valid = scherzo_cloud_support::valid_typed_id(&publication.id, "pub_")
        && scherzo_cloud_support::valid_typed_id(&publication.organization_id, "org_")
        && scherzo_cloud_support::valid_typed_id(&publication.project_id, "prj_")
        && scherzo_cloud_support::valid_typed_id(&publication.run_id, "run_")
        && scherzo_cloud_support::valid_typed_id(&publication.artifact_set_id, "ats_")
        && scherzo_cloud_support::valid_typed_id(&publication.actor_principal_id, "prn_")
        && scherzo_cloud_support::valid_typed_id(
            &publication.target.repository_connection_id,
            "rpc_",
        );
    let snapshot_valid = publication.run_id == requested_run_id
        && requested_publication_id.is_none_or(|expected| publication.id == expected)
        && requested_export_name.is_none_or(|expected| publication.export_name == expected)
        && scherzo_cloud_support::is_identifier(&publication.export_name)
        && publication.version >= 1
        && publication.artifact.artifact_version == 1
        && scherzo_cloud_support::is_lowercase_hex(&publication.artifact.base_oid, 40)
        && scherzo_cloud_support::is_lowercase_hex(&publication.artifact.head_oid, 40)
        && scherzo_cloud_support::is_lowercase_hex(&publication.artifact.tree_oid, 40)
        && valid_provider_id(&publication.target.provider_repository_id)
        && valid_repository_full_name(&publication.target.full_name)
        && valid_bounded_string(&publication.target.base_branch, 1, 1024)
        && publication.target.destination_branch
            == format!("scherzo/{requested_run_id}/{}", publication.export_name)
        && valid_bounded_string(&publication.target.destination_branch, 1, 1024)
        && valid_bounded_string(&publication.pull_request_metadata.title, 1, 256)
        && valid_bounded_string(&publication.pull_request_metadata.body, 1, 65_536)
        && updated_at >= created_at
        && expires_at > created_at
        && started_at.is_none_or(|started| started >= created_at && started <= updated_at)
        && terminal_at.is_none_or(|terminal| {
            started_at.is_some_and(|started| terminal >= started && terminal <= updated_at)
        });
    let receipts_valid = publication
        .branch
        .as_deref()
        .is_none_or(|branch| valid_branch(branch, &publication.artifact.head_oid))
        && publication
            .pull_request
            .as_deref()
            .is_none_or(valid_pull_request)
        && (publication.pull_request.is_none() || publication.branch.is_some());
    let lifecycle_valid = match publication.state {
        State::Queued => {
            started_at.is_none()
                && terminal_at.is_none()
                && publication.branch.is_none()
                && publication.pull_request.is_none()
                && publication.outcome.is_none()
                && publication.failure.is_none()
        }
        State::Running => {
            started_at.is_some()
                && terminal_at.is_none()
                && publication.outcome.is_none()
                && publication.failure.is_none()
        }
        State::Succeeded => {
            terminal_at.is_some()
                && publication.failure.is_none()
                && match publication.outcome {
                    Some(Outcome::NoChanges) => {
                        publication.branch.is_none() && publication.pull_request.is_none()
                    }
                    Some(Outcome::PullRequestPublished) => publication
                        .pull_request
                        .as_deref()
                        .is_some_and(|pull_request| {
                            publication.branch.is_some()
                                && pull_request.state
                                    == models::publication_pull_request::State::Open
                        }),
                    Some(Outcome::PullRequestAlreadyMerged) => publication
                        .pull_request
                        .as_deref()
                        .is_some_and(|pull_request| {
                            publication.branch.is_some()
                                && pull_request.state
                                    == models::publication_pull_request::State::Merged
                        }),
                    None => false,
                }
        }
        State::Failed => {
            terminal_at.is_some()
                && publication.outcome.is_none()
                && publication.pull_request.is_none()
                && publication
                    .failure
                    .as_deref()
                    .is_some_and(valid_publication_failure)
                && publication.branch.as_deref().is_none_or(|_| {
                    publication.failure.as_deref().is_some_and(|failure| {
                        matches!(
                            failure.phase,
                            models::publication_failure::Phase::Branch
                                | models::publication_failure::Phase::PullRequest
                        ) && failure.code != models::publication_failure::Code::TargetBranchConflict
                    })
                })
        }
    };
    if identities_valid && snapshot_valid && receipts_valid && lifecycle_valid {
        Ok(())
    } else {
        Err(PublicationFailure::protocol(false))
    }
}

fn valid_branch(branch: &models::PublicationBranch, expected_head: &str) -> bool {
    branch.head_oid == expected_head
        && scherzo_cloud_support::is_lowercase_hex(&branch.head_oid, 40)
        && valid_https_url(&branch.url)
}

fn valid_pull_request(pull_request: &models::PublicationPullRequest) -> bool {
    valid_provider_id(&pull_request.provider_id)
        && pull_request.number >= 1
        && valid_https_url(&pull_request.url)
}

fn valid_publication_failure(failure: &models::PublicationFailure) -> bool {
    use models::publication_failure::{Code, Phase};

    let expected_retryable = match failure.code {
        Code::ArtifactExpired
        | Code::ArtifactInvalid
        | Code::ArtifactNotApplicable
        | Code::WorkflowFileChangeUnsupported
        | Code::ProviderRejected => Some(false),
        Code::BaseBranchUnavailable
        | Code::BaseBranchIncompatible
        | Code::ActorAuthorityLost
        | Code::ProjectRepositoryChanged
        | Code::ProviderPermissionUnavailable
        | Code::TargetBranchConflict
        | Code::PullRequestConflict
        | Code::ProviderUnavailable => Some(true),
        Code::InternalPublicationFailure => None,
    };
    let phase_valid = match failure.code {
        Code::ArtifactExpired => matches!(failure.phase, Phase::Artifact | Phase::Preflight),
        Code::ArtifactInvalid => failure.phase == Phase::Artifact,
        Code::ArtifactNotApplicable
        | Code::WorkflowFileChangeUnsupported
        | Code::BaseBranchUnavailable
        | Code::BaseBranchIncompatible => failure.phase == Phase::Preflight,
        Code::ActorAuthorityLost | Code::ProjectRepositoryChanged => {
            matches!(failure.phase, Phase::Branch | Phase::PullRequest)
        }
        Code::ProviderPermissionUnavailable | Code::ProviderUnavailable => matches!(
            failure.phase,
            Phase::Preflight | Phase::Branch | Phase::PullRequest
        ),
        Code::TargetBranchConflict => failure.phase == Phase::Branch,
        Code::PullRequestConflict => failure.phase == Phase::PullRequest,
        Code::ProviderRejected => {
            matches!(failure.phase, Phase::Branch | Phase::PullRequest)
        }
        Code::InternalPublicationFailure => true,
    };
    phase_valid && expected_retryable.is_none_or(|expected| failure.retryable == expected)
}

fn valid_provider_id(value: &str) -> bool {
    value
        .parse::<i64>()
        .ok()
        .filter(|identifier| *identifier > 0)
        .is_some_and(|identifier| identifier.to_string() == value)
}

fn valid_repository_full_name(value: &str) -> bool {
    let mut parts = value.split('/');
    let valid_part = |part: &str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    };
    (3..=255).contains(&value.len())
        && parts.next().is_some_and(valid_part)
        && parts.next().is_some_and(valid_part)
        && parts.next().is_none()
}

fn valid_bounded_string(value: &str, minimum: usize, maximum: usize) -> bool {
    (minimum..=maximum).contains(&value.chars().count())
}

fn valid_https_url(value: &str) -> bool {
    value.starts_with("https://")
        && Url::parse(value)
            .ok()
            .is_some_and(|url| url.scheme() == "https" && url.has_host())
}

fn timestamp(value: &str) -> Result<OffsetDateTime, PublicationFailure> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|_| PublicationFailure::protocol(false))
}

#[derive(Debug)]
pub enum PublicationApiError {
    InvalidEndpoint,
    InsecureHttp,
    BuildClient(reqwest::Error),
}

impl fmt::Display for PublicationApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                formatter,
                "the deployment API URL cannot form a Cloud publication endpoint"
            ),
            Self::InsecureHttp => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            Self::BuildClient(error) => {
                write!(formatter, "prepare Cloud publication networking: {error}")
            }
        }
    }
}
