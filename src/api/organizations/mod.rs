mod models;

use std::fmt;
use std::time::Duration;

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue, LOCATION};
use reqwest::{Method, Response, StatusCode, Url};
use serde::de::DeserializeOwned;
use zeroize::Zeroizing;

use super::bearer_authorization;
use super::generated::models as generated_models;
use super::http_client::{HttpClient, HttpEndpointError};
use super::http_util;
#[cfg(test)]
use super::problem::PROBLEM_MEDIA_TYPE;
use super::problem::{
    self, ACCEPTED_MEDIA_TYPES, BAD_REQUEST, FORBIDDEN, JSON_MEDIA_TYPE, NOT_FOUND, UNAUTHORIZED,
};
use super::{UnreachableCategory, classify_reqwest_error};

pub(crate) use models::{
    AcceptedInvitationMembership, CurrentPrincipalMembership, CurrentPrincipalMembershipPage,
    Invitation, InvitationDeliveryState, InvitationInboxEntry, InvitationInboxPage, InvitationPage,
    InvitationPreview, InvitationState, InvitationTargetKind, MembershipRole, MembershipState,
    Organization, OrganizationMembershipDirectoryEntry, OrganizationMembershipHistoryEntry,
    OrganizationMembershipHistoryPage, OrganizationMembershipPage, OrganizationState,
    PrincipalType,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MUTATION_ATTEMPTS: usize = 2;
const READ_ATTEMPTS: usize = 1;
const MERGE_PATCH_MEDIA_TYPE: &str = "application/merge-patch+json";
const CREATION_NOT_PERMITTED: &str =
    "https://api.scherzo.dev/problems/organization-creation-not-permitted";
const SLUG_UNAVAILABLE: &str = "https://api.scherzo.dev/problems/slug-unavailable";
const QUANTITY_LIMIT_REACHED: &str = "https://api.scherzo.dev/problems/quantity-limit-reached";
const RATE_LIMITED: &str = "https://api.scherzo.dev/problems/rate-limit-exceeded";
const IDEMPOTENCY_CONFLICT: &str = "https://api.scherzo.dev/problems/idempotency-conflict";
const MEMBERSHIP_TRANSITION_UNAVAILABLE: &str =
    "https://api.scherzo.dev/problems/membership-transition-unavailable";
const HUMAN_OWNER_REQUIRED: &str = "https://api.scherzo.dev/problems/human-owner-required";
const RECIPIENT_UNAVAILABLE: &str = "https://api.scherzo.dev/problems/recipient-unavailable";
const INVITATION_UNAVAILABLE: &str = "https://api.scherzo.dev/problems/invitation-unavailable";
const OUTSTANDING_INVITATION_LIMIT: &str =
    "https://api.scherzo.dev/problems/outstanding-invitation-limit-reached";
const MEMBERSHIP_LIMIT: &str = "https://api.scherzo.dev/problems/membership-limit-reached";
const REQUEST_BODY_TOO_LARGE: &str = "https://api.scherzo.dev/problems/request-body-too-large";
const UNSUPPORTED_MEDIA_TYPE: &str = "https://api.scherzo.dev/problems/unsupported-media-type";

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
pub(crate) enum ListCurrentPrincipalMembershipsOutcome {
    Listed(CurrentPrincipalMembershipPage),
    Common(CommonOrganizationFailure),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ListOrganizationMembershipsOutcome {
    Listed(OrganizationMembershipPage),
    Common(CommonOrganizationFailure),
    NotFound,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ListOrganizationMembershipHistoryOutcome {
    Listed(OrganizationMembershipHistoryPage),
    Common(CommonOrganizationFailure),
    NotFound,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum UpdateOrganizationMembershipOutcome {
    Updated(OrganizationMembershipHistoryEntry),
    Common(CommonOrganizationFailure),
    NotFound,
    TransitionUnavailable,
    HumanOwnerRequired,
    IdempotencyConflict,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum MembershipTerminationOutcome {
    Ended,
    Common(CommonOrganizationFailure),
    NotFound,
    TransitionUnavailable,
    HumanOwnerRequired,
    IdempotencyConflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InvitationTarget<'a> {
    Principal(&'a str),
    Email(&'a str),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum IssueInvitationOutcome {
    Issued(Box<Invitation>),
    Common(CommonOrganizationFailure),
    NotFound,
    RecipientUnavailable,
    OutstandingLimitReached,
    RateLimited { retry_after: u64 },
    IdempotencyConflict,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ListOrganizationInvitationsOutcome {
    Listed(InvitationPage),
    Common(CommonOrganizationFailure),
    NotFound,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ListInvitationInboxOutcome {
    Listed(InvitationInboxPage),
    Common(CommonOrganizationFailure),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum PreviewInvitationOutcome {
    Previewed(InvitationPreview),
    Common(CommonOrganizationFailure),
    Unavailable,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum AcceptInvitationOutcome {
    Accepted(AcceptedInvitationMembership),
    Common(CommonOrganizationFailure),
    Unavailable,
    MembershipLimitReached,
    IdempotencyConflict,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum InvitationTerminationOutcome {
    Completed,
    Common(CommonOrganizationFailure),
    NotFound,
    Unavailable,
    IdempotencyConflict,
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
    ListCurrentMemberships,
    ListMemberships,
    ListMembershipHistory,
    UpdateMembership,
    EndMembership,
    Leave,
    IssueInvitation,
    ListOrganizationInvitations,
    RevokeInvitation,
    ListInvitationInbox,
    PreviewInvitation,
    AcceptInvitation,
    DeclineInvitation,
}

impl Operation {
    const fn name(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Get => "show",
            Self::Update => "update",
            Self::ListCurrentMemberships => "current-membership-list",
            Self::ListMemberships => "membership-list",
            Self::ListMembershipHistory => "membership-history-list",
            Self::UpdateMembership => "membership-update",
            Self::EndMembership => "membership-removal",
            Self::Leave => "leave",
            Self::IssueInvitation => "invitation issue",
            Self::ListOrganizationInvitations => "organization-invitation list",
            Self::RevokeInvitation => "invitation revocation",
            Self::ListInvitationInbox => "invitation inbox list",
            Self::PreviewInvitation => "invitation preview",
            Self::AcceptInvitation => "invitation acceptance",
            Self::DeclineInvitation => "invitation decline",
        }
    }

    fn can_retry_interrupted_response(self, status: StatusCode) -> bool {
        matches!(
            (self, status),
            (Self::Create | Self::IssueInvitation, StatusCode::CREATED)
                | (
                    Self::Update | Self::UpdateMembership | Self::AcceptInvitation,
                    StatusCode::OK
                )
                | (
                    Self::EndMembership
                        | Self::Leave
                        | Self::RevokeInvitation
                        | Self::DeclineInvitation,
                    StatusCode::NO_CONTENT
                )
        )
    }

    fn success_has_json_body(self) -> bool {
        matches!(
            self,
            Self::Create
                | Self::Update
                | Self::UpdateMembership
                | Self::IssueInvitation
                | Self::AcceptInvitation
        )
    }
}

struct RequestSpec {
    operation: Operation,
    method: Method,
    endpoint: Url,
    authorization: HeaderValue,
    idempotency_key: Option<HeaderValue>,
    content_type: Option<&'static str>,
    body: Option<Zeroizing<Vec<u8>>>,
    max_attempts: usize,
}

type ReceivedResponse = http_util::BufferedResponse;
type AttemptError = http_util::ApiAttemptError<OrganizationError>;

enum RequestExecution {
    Response(ReceivedResponse),
    Unreachable(UnreachableCategory),
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
    let spec = merge_patch_request_spec(
        client,
        Operation::Update,
        api_url,
        &["v1", "organizations", organization_ref],
        access_token,
        idempotency_key,
        body,
    )?;

    match execute_request(client, &spec, REQUEST_TIMEOUT)? {
        RequestExecution::Response(response) => decode_update_response(response, idempotency_key),
        RequestExecution::Unreachable(category) => Ok(UpdateOrganizationOutcome::Common(
            CommonOrganizationFailure::Unreachable(category),
        )),
    }
}

pub(crate) fn list_current_principal_memberships(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    limit: Option<u16>,
    cursor: Option<&str>,
) -> Result<ListCurrentPrincipalMembershipsOutcome, OrganizationError> {
    let spec = list_request_spec(
        client,
        Operation::ListCurrentMemberships,
        api_url,
        &["v1", "me", "memberships"],
        access_token,
        limit,
        cursor,
    )?;

    match execute_request(client, &spec, REQUEST_TIMEOUT)? {
        RequestExecution::Response(response) => decode_current_membership_list_response(response),
        RequestExecution::Unreachable(category) => {
            Ok(ListCurrentPrincipalMembershipsOutcome::Common(
                CommonOrganizationFailure::Unreachable(category),
            ))
        }
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
    let spec = list_request_spec(
        client,
        Operation::ListMemberships,
        api_url,
        &["v1", "organizations", organization_ref, "memberships"],
        access_token,
        limit,
        cursor,
    )?;

    match execute_request(client, &spec, REQUEST_TIMEOUT)? {
        RequestExecution::Response(response) => decode_list_response(response),
        RequestExecution::Unreachable(category) => Ok(ListOrganizationMembershipsOutcome::Common(
            CommonOrganizationFailure::Unreachable(category),
        )),
    }
}

pub(crate) fn list_organization_membership_history(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
    limit: Option<u16>,
    cursor: Option<&str>,
) -> Result<ListOrganizationMembershipHistoryOutcome, OrganizationError> {
    let spec = list_request_spec(
        client,
        Operation::ListMembershipHistory,
        api_url,
        &[
            "v1",
            "organizations",
            organization_ref,
            "memberships",
            "history",
        ],
        access_token,
        limit,
        cursor,
    )?;

    match execute_request(client, &spec, REQUEST_TIMEOUT)? {
        RequestExecution::Response(response) => decode_membership_history_response(response),
        RequestExecution::Unreachable(category) => {
            Ok(ListOrganizationMembershipHistoryOutcome::Common(
                CommonOrganizationFailure::Unreachable(category),
            ))
        }
    }
}

pub(crate) fn update_organization_membership_role(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
    membership_id: &str,
    idempotency_key: &str,
    role: MembershipRole,
) -> Result<UpdateOrganizationMembershipOutcome, OrganizationError> {
    let mut request = generated_models::UpdateOrganizationMembershipPatch::new();
    request.role = Some(match role {
        MembershipRole::Owner => {
            generated_models::update_organization_membership_patch::Role::MembershipPatchRoleOwner
        }
        MembershipRole::Member => {
            generated_models::update_organization_membership_patch::Role::MembershipPatchRoleMember
        }
    });
    let body = serialize_request(Operation::UpdateMembership, &request)?;
    let spec = merge_patch_request_spec(
        client,
        Operation::UpdateMembership,
        api_url,
        &[
            "v1",
            "organizations",
            organization_ref,
            "memberships",
            membership_id,
        ],
        access_token,
        idempotency_key,
        body,
    )?;

    match execute_request(client, &spec, REQUEST_TIMEOUT)? {
        RequestExecution::Response(response) => {
            decode_update_membership_response(response, idempotency_key)
        }
        RequestExecution::Unreachable(category) => Ok(UpdateOrganizationMembershipOutcome::Common(
            CommonOrganizationFailure::Unreachable(category),
        )),
    }
}

pub(crate) fn end_organization_membership(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
    membership_id: &str,
    idempotency_key: &str,
) -> Result<MembershipTerminationOutcome, OrganizationError> {
    execute_membership_termination(
        client,
        api_url,
        access_token,
        &[
            "v1",
            "organizations",
            organization_ref,
            "memberships",
            membership_id,
        ],
        idempotency_key,
        Operation::EndMembership,
    )
}

pub(crate) fn leave_organization(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
    idempotency_key: &str,
) -> Result<MembershipTerminationOutcome, OrganizationError> {
    execute_membership_termination(
        client,
        api_url,
        access_token,
        &["v1", "organizations", organization_ref, "memberships", "me"],
        idempotency_key,
        Operation::Leave,
    )
}

pub(crate) fn issue_invitation(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
    idempotency_key: &str,
    target: InvitationTarget<'_>,
) -> Result<IssueInvitationOutcome, OrganizationError> {
    #[derive(serde::Serialize)]
    #[serde(tag = "kind", rename_all = "lowercase")]
    enum TargetRequest<'a> {
        Principal {
            #[serde(rename = "principalId")]
            principal_id: &'a str,
        },
        Email {
            email: &'a str,
        },
    }

    let request = match target {
        InvitationTarget::Principal(principal_id) => TargetRequest::Principal { principal_id },
        InvitationTarget::Email(email) => TargetRequest::Email { email },
    };
    let body = serialize_request(Operation::IssueInvitation, &request)?;
    let spec = request_spec(
        client,
        Operation::IssueInvitation,
        Method::POST,
        api_url,
        &["v1", "organizations", organization_ref, "invitations"],
        access_token,
        Some(idempotency_key),
        Some(JSON_MEDIA_TYPE),
        Some(body),
        MUTATION_ATTEMPTS,
    )?;
    execute_invitation_spec(
        client,
        &spec,
        |response| decode_issue_invitation_response(response, idempotency_key),
        |category| IssueInvitationOutcome::Common(CommonOrganizationFailure::Unreachable(category)),
    )
}

pub(crate) fn list_organization_invitations(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
    limit: Option<u16>,
    cursor: Option<&str>,
) -> Result<ListOrganizationInvitationsOutcome, OrganizationError> {
    let spec = list_request_spec(
        client,
        Operation::ListOrganizationInvitations,
        api_url,
        &["v1", "organizations", organization_ref, "invitations"],
        access_token,
        limit,
        cursor,
    )?;
    execute_invitation_spec(
        client,
        &spec,
        decode_organization_invitation_list_response,
        |category| {
            ListOrganizationInvitationsOutcome::Common(CommonOrganizationFailure::Unreachable(
                category,
            ))
        },
    )
}

pub(crate) fn revoke_invitation(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    organization_ref: &str,
    invitation_id: &str,
    idempotency_key: &str,
) -> Result<InvitationTerminationOutcome, OrganizationError> {
    let spec = request_spec(
        client,
        Operation::RevokeInvitation,
        Method::DELETE,
        api_url,
        &[
            "v1",
            "organizations",
            organization_ref,
            "invitations",
            invitation_id,
        ],
        access_token,
        Some(idempotency_key),
        None,
        None,
        MUTATION_ATTEMPTS,
    )?;
    execute_invitation_spec(
        client,
        &spec,
        |response| {
            decode_invitation_termination_response(
                Operation::RevokeInvitation,
                response,
                idempotency_key,
            )
        },
        |category| {
            InvitationTerminationOutcome::Common(CommonOrganizationFailure::Unreachable(category))
        },
    )
}

pub(crate) fn list_invitation_inbox(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    limit: Option<u16>,
    cursor: Option<&str>,
) -> Result<ListInvitationInboxOutcome, OrganizationError> {
    let spec = list_request_spec(
        client,
        Operation::ListInvitationInbox,
        api_url,
        &["v1", "me", "invitations"],
        access_token,
        limit,
        cursor,
    )?;
    execute_invitation_spec(
        client,
        &spec,
        decode_invitation_inbox_response,
        |category| {
            ListInvitationInboxOutcome::Common(CommonOrganizationFailure::Unreachable(category))
        },
    )
}

pub(crate) fn preview_invitation(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    invitation_id: &str,
    capability: Option<&str>,
) -> Result<PreviewInvitationOutcome, OrganizationError> {
    let body = invitation_capability_body(Operation::PreviewInvitation, capability)?;
    let spec = invitation_action_spec(
        client,
        api_url,
        invitation_id,
        access_token,
        InvitationAction::Preview,
        body,
    )?;
    execute_invitation_spec(
        client,
        &spec,
        decode_invitation_preview_response,
        |category| {
            PreviewInvitationOutcome::Common(CommonOrganizationFailure::Unreachable(category))
        },
    )
}

pub(crate) fn accept_invitation(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    invitation_id: &str,
    capability: Option<&str>,
    idempotency_key: &str,
) -> Result<AcceptInvitationOutcome, OrganizationError> {
    let body = invitation_capability_body(Operation::AcceptInvitation, capability)?;
    let spec = invitation_action_spec(
        client,
        api_url,
        invitation_id,
        access_token,
        InvitationAction::Accept { idempotency_key },
        body,
    )?;
    execute_invitation_spec(
        client,
        &spec,
        |response| decode_accept_invitation_response(response, idempotency_key),
        |category| {
            AcceptInvitationOutcome::Common(CommonOrganizationFailure::Unreachable(category))
        },
    )
}

pub(crate) fn decline_invitation(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    invitation_id: &str,
    capability: Option<&str>,
    idempotency_key: &str,
) -> Result<InvitationTerminationOutcome, OrganizationError> {
    let body = invitation_capability_body(Operation::DeclineInvitation, capability)?;
    let spec = invitation_action_spec(
        client,
        api_url,
        invitation_id,
        access_token,
        InvitationAction::Decline { idempotency_key },
        body,
    )?;
    execute_invitation_spec(
        client,
        &spec,
        |response| {
            decode_invitation_termination_response(
                Operation::DeclineInvitation,
                response,
                idempotency_key,
            )
        },
        |category| {
            InvitationTerminationOutcome::Common(CommonOrganizationFailure::Unreachable(category))
        },
    )
}

enum InvitationAction<'a> {
    Preview,
    Accept { idempotency_key: &'a str },
    Decline { idempotency_key: &'a str },
}

fn invitation_action_spec(
    client: &HttpClient,
    api_url: &str,
    invitation_id: &str,
    access_token: &str,
    action: InvitationAction<'_>,
    body: Zeroizing<Vec<u8>>,
) -> Result<RequestSpec, OrganizationError> {
    let (operation, action_path, idempotency_key, max_attempts) = match action {
        InvitationAction::Preview => (Operation::PreviewInvitation, "preview", None, READ_ATTEMPTS),
        InvitationAction::Accept { idempotency_key } => (
            Operation::AcceptInvitation,
            "accept",
            Some(idempotency_key),
            MUTATION_ATTEMPTS,
        ),
        InvitationAction::Decline { idempotency_key } => (
            Operation::DeclineInvitation,
            "decline",
            Some(idempotency_key),
            MUTATION_ATTEMPTS,
        ),
    };
    request_spec(
        client,
        operation,
        Method::POST,
        api_url,
        &["v1", "invitations", invitation_id, action_path],
        access_token,
        idempotency_key,
        Some(JSON_MEDIA_TYPE),
        Some(body),
        max_attempts,
    )
}

fn execute_invitation_spec<O>(
    client: &HttpClient,
    spec: &RequestSpec,
    decode: impl FnOnce(ReceivedResponse) -> Result<O, OrganizationError>,
    unreachable: impl FnOnce(UnreachableCategory) -> O,
) -> Result<O, OrganizationError> {
    match execute_request(client, spec, REQUEST_TIMEOUT)? {
        RequestExecution::Response(response) => decode(response),
        RequestExecution::Unreachable(category) => Ok(unreachable(category)),
    }
}

fn invitation_capability_body(
    operation: Operation,
    capability: Option<&str>,
) -> Result<Zeroizing<Vec<u8>>, OrganizationError> {
    #[derive(serde::Serialize)]
    struct CapabilityRequest<'a> {
        #[serde(skip_serializing_if = "Option::is_none")]
        capability: Option<&'a str>,
    }

    serialize_request(operation, &CapabilityRequest { capability })
}

fn execute_membership_termination(
    client: &HttpClient,
    api_url: &str,
    access_token: &str,
    path: &[&str],
    idempotency_key: &str,
    operation: Operation,
) -> Result<MembershipTerminationOutcome, OrganizationError> {
    let spec = request_spec(
        client,
        operation,
        Method::DELETE,
        api_url,
        path,
        access_token,
        Some(idempotency_key),
        None,
        None,
        MUTATION_ATTEMPTS,
    )?;

    match execute_request(client, &spec, REQUEST_TIMEOUT)? {
        RequestExecution::Response(response) => {
            decode_membership_termination_response(operation, response, idempotency_key)
        }
        RequestExecution::Unreachable(category) => Ok(MembershipTerminationOutcome::Common(
            CommonOrganizationFailure::Unreachable(category),
        )),
    }
}

fn merge_patch_request_spec(
    client: &HttpClient,
    operation: Operation,
    api_url: &str,
    path: &[&str],
    access_token: &str,
    idempotency_key: &str,
    body: Zeroizing<Vec<u8>>,
) -> Result<RequestSpec, OrganizationError> {
    request_spec(
        client,
        operation,
        Method::PATCH,
        api_url,
        path,
        access_token,
        Some(idempotency_key),
        Some(MERGE_PATCH_MEDIA_TYPE),
        Some(body),
        MUTATION_ATTEMPTS,
    )
}

fn list_request_spec(
    client: &HttpClient,
    operation: Operation,
    api_url: &str,
    path: &[&str],
    access_token: &str,
    limit: Option<u16>,
    cursor: Option<&str>,
) -> Result<RequestSpec, OrganizationError> {
    let mut endpoint = client.endpoint(api_url, path).map_err(|error| {
        OrganizationError::local(operation, OrganizationErrorKind::Endpoint(error))
    })?;
    http_util::append_pagination(&mut endpoint, limit, cursor);
    request_spec_for_endpoint(
        operation,
        Method::GET,
        endpoint,
        access_token,
        None,
        None,
        None,
        READ_ATTEMPTS,
    )
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
    body: Option<Zeroizing<Vec<u8>>>,
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
    body: Option<Zeroizing<Vec<u8>>>,
    max_attempts: usize,
) -> Result<RequestSpec, OrganizationError> {
    let authorization = bearer_authorization(access_token).map_err(|_| {
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
) -> Result<Zeroizing<Vec<u8>>, OrganizationError> {
    serde_json::to_vec(request)
        .map(Zeroizing::new)
        .map_err(|_| OrganizationError::local(operation, OrganizationErrorKind::SerializeRequest))
}

fn execute_request(
    client: &HttpClient,
    spec: &RequestSpec,
    timeout: Duration,
) -> Result<RequestExecution, OrganizationError> {
    let mut last_failure = UnreachableCategory::Connection;
    for attempt in 0..spec.max_attempts {
        let started = crate::timing::monotonic_now();
        let response = match client.run(timeout, send_request(client, spec, timeout)) {
            Ok(Ok(response)) => response,
            Ok(Err(AttemptError::Protocol(error))) => return Err(error),
            Ok(Err(AttemptError::Transport(category))) => {
                last_failure = category;
                delay_before_retry(attempt, spec.max_attempts);
                continue;
            }
            Err(_) => {
                last_failure = UnreachableCategory::Timeout;
                delay_before_retry(attempt, spec.max_attempts);
                continue;
            }
        };
        let status = response.status();
        if spec.operation.can_retry_interrupted_response(status) {
            require_replayable_success_headers(spec, &response)?;
        }
        let remaining = timeout.saturating_sub(crate::timing::elapsed(started));
        match client.run(remaining, receive_response(spec.operation, response)) {
            Ok(Ok(response)) => return Ok(RequestExecution::Response(response)),
            Ok(Err(AttemptError::Protocol(error))) => return Err(error),
            Ok(Err(AttemptError::Transport(category)))
                if spec.operation.can_retry_interrupted_response(status) =>
            {
                last_failure = category;
                delay_before_retry(attempt, spec.max_attempts);
                continue;
            }
            Ok(Err(AttemptError::Transport(category))) => {
                return Ok(RequestExecution::Unreachable(if status.is_server_error() {
                    UnreachableCategory::Server
                } else {
                    category
                }));
            }
            Err(_) => match classify_response_deadline(spec.operation, status)? {
                Some(execution) => return Ok(execution),
                None => {
                    last_failure = UnreachableCategory::Timeout;
                    delay_before_retry(attempt, spec.max_attempts);
                    continue;
                }
            },
        }
    }

    Ok(RequestExecution::Unreachable(last_failure))
}

fn delay_before_retry(attempt: usize, max_attempts: usize) {
    if attempt + 1 < max_attempts {
        crate::timing::sleep(crate::timing::short_retry_delay());
    }
}

fn classify_response_deadline(
    operation: Operation,
    status: StatusCode,
) -> Result<Option<RequestExecution>, OrganizationError> {
    if status == StatusCode::UNAUTHORIZED {
        return Err(OrganizationError::protocol(
            operation,
            "the unauthorized response body exceeded the request deadline",
            true,
        ));
    }
    if operation.can_retry_interrupted_response(status) {
        return Ok(None);
    }

    Ok(Some(RequestExecution::Unreachable(
        if status.is_server_error() {
            UnreachableCategory::Server
        } else {
            UnreachableCategory::Timeout
        },
    )))
}

fn require_replayable_success_headers(
    spec: &RequestSpec,
    response: &Response,
) -> Result<(), OrganizationError> {
    if response.headers().get("Idempotency-Key") != spec.idempotency_key.as_ref() {
        return Err(OrganizationError::protocol(
            spec.operation,
            "the successful response has a missing or mismatched Idempotency-Key header",
            false,
        ));
    }

    if spec.operation.success_has_json_body() {
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .map(http_util::media_type)
            .transpose()
            .map_err(|()| {
                OrganizationError::protocol(
                    spec.operation,
                    "the Content-Type header is not valid text",
                    false,
                )
            })?;
        if content_type.as_deref() != Some(JSON_MEDIA_TYPE) {
            return Err(OrganizationError::protocol(
                spec.operation,
                "the response Content-Type is not valid for its HTTP status",
                false,
            ));
        }
    }

    if matches!(
        spec.operation,
        Operation::Create | Operation::IssueInvitation
    ) && response
        .headers()
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .is_none()
    {
        return Err(OrganizationError::protocol(
            spec.operation,
            "the successful response has a missing or mismatched Location header",
            false,
        ));
    }

    Ok(())
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
        request = request.body(body.as_slice().to_vec());
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
    http_util::buffer_api_response(response, |reason, rejected| {
        OrganizationError::protocol(operation, reason, rejected)
    })
    .await
}

fn decode_issue_invitation_response(
    response: ReceivedResponse,
    expected_idempotency_key: &str,
) -> Result<IssueInvitationOutcome, OrganizationError> {
    match response.status {
        StatusCode::CREATED => {
            require_response_idempotency_key(
                Operation::IssueInvitation,
                &response,
                expected_idempotency_key,
            )?;
            let invitation = decode_invitation(Operation::IssueInvitation, &response)?;
            require_invitation_location(&response, &invitation.id)?;
            Ok(IssueInvitationOutcome::Issued(Box::new(invitation)))
        }
        StatusCode::NOT_FOUND => {
            require_problem(Operation::IssueInvitation, &response, NOT_FOUND, false)?;
            Ok(IssueInvitationOutcome::NotFound)
        }
        StatusCode::CONFLICT => {
            let problem_type = decode_problem_type(Operation::IssueInvitation, &response, false)?;
            match problem_type.as_str() {
                RECIPIENT_UNAVAILABLE => Ok(IssueInvitationOutcome::RecipientUnavailable),
                OUTSTANDING_INVITATION_LIMIT => Ok(IssueInvitationOutcome::OutstandingLimitReached),
                IDEMPOTENCY_CONFLICT => Ok(IssueInvitationOutcome::IdempotencyConflict),
                _ => Err(OrganizationError::protocol(
                    Operation::IssueInvitation,
                    "a 409 response has an unrecognized problem type",
                    false,
                )),
            }
        }
        StatusCode::TOO_MANY_REQUESTS => {
            require_problem(Operation::IssueInvitation, &response, RATE_LIMITED, false)?;
            parse_retry_after(Operation::IssueInvitation, &response)
                .map(|retry_after| IssueInvitationOutcome::RateLimited { retry_after })
        }
        StatusCode::PAYLOAD_TOO_LARGE => {
            require_problem(
                Operation::IssueInvitation,
                &response,
                REQUEST_BODY_TOO_LARGE,
                false,
            )?;
            Ok(IssueInvitationOutcome::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            require_problem(
                Operation::IssueInvitation,
                &response,
                UNSUPPORTED_MEDIA_TYPE,
                false,
            )?;
            Ok(IssueInvitationOutcome::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        _ => decode_common_list_failure(Operation::IssueInvitation, &response)
            .map(IssueInvitationOutcome::Common),
    }
}

fn decode_organization_invitation_list_response(
    response: ReceivedResponse,
) -> Result<ListOrganizationInvitationsOutcome, OrganizationError> {
    match response.status {
        StatusCode::OK => decode_invitation_page(Operation::ListOrganizationInvitations, &response)
            .map(ListOrganizationInvitationsOutcome::Listed),
        StatusCode::NOT_FOUND => {
            require_problem(
                Operation::ListOrganizationInvitations,
                &response,
                NOT_FOUND,
                false,
            )?;
            Ok(ListOrganizationInvitationsOutcome::NotFound)
        }
        _ => decode_common_list_failure(Operation::ListOrganizationInvitations, &response)
            .map(ListOrganizationInvitationsOutcome::Common),
    }
}

fn decode_invitation_inbox_response(
    response: ReceivedResponse,
) -> Result<ListInvitationInboxOutcome, OrganizationError> {
    match response.status {
        StatusCode::OK => decode_invitation_inbox_page(Operation::ListInvitationInbox, &response)
            .map(ListInvitationInboxOutcome::Listed),
        _ => decode_common_list_failure(Operation::ListInvitationInbox, &response)
            .map(ListInvitationInboxOutcome::Common),
    }
}

fn decode_invitation_preview_response(
    response: ReceivedResponse,
) -> Result<PreviewInvitationOutcome, OrganizationError> {
    match response.status {
        StatusCode::OK => decode_invitation_preview(Operation::PreviewInvitation, &response)
            .map(PreviewInvitationOutcome::Previewed),
        StatusCode::CONFLICT => {
            require_problem(
                Operation::PreviewInvitation,
                &response,
                INVITATION_UNAVAILABLE,
                false,
            )?;
            Ok(PreviewInvitationOutcome::Unavailable)
        }
        StatusCode::PAYLOAD_TOO_LARGE => {
            require_problem(
                Operation::PreviewInvitation,
                &response,
                REQUEST_BODY_TOO_LARGE,
                false,
            )?;
            Ok(PreviewInvitationOutcome::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            require_problem(
                Operation::PreviewInvitation,
                &response,
                UNSUPPORTED_MEDIA_TYPE,
                false,
            )?;
            Ok(PreviewInvitationOutcome::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        _ => decode_common_list_failure(Operation::PreviewInvitation, &response)
            .map(PreviewInvitationOutcome::Common),
    }
}

fn decode_accept_invitation_response(
    response: ReceivedResponse,
    expected_idempotency_key: &str,
) -> Result<AcceptInvitationOutcome, OrganizationError> {
    match response.status {
        StatusCode::OK => {
            require_response_idempotency_key(
                Operation::AcceptInvitation,
                &response,
                expected_idempotency_key,
            )?;
            decode_accepted_invitation_membership(Operation::AcceptInvitation, &response)
                .map(AcceptInvitationOutcome::Accepted)
        }
        StatusCode::CONFLICT => {
            let problem_type = decode_problem_type(Operation::AcceptInvitation, &response, false)?;
            match problem_type.as_str() {
                INVITATION_UNAVAILABLE => Ok(AcceptInvitationOutcome::Unavailable),
                MEMBERSHIP_LIMIT => Ok(AcceptInvitationOutcome::MembershipLimitReached),
                IDEMPOTENCY_CONFLICT => Ok(AcceptInvitationOutcome::IdempotencyConflict),
                _ => Err(OrganizationError::protocol(
                    Operation::AcceptInvitation,
                    "a 409 response has an unrecognized problem type",
                    false,
                )),
            }
        }
        StatusCode::PAYLOAD_TOO_LARGE => {
            require_problem(
                Operation::AcceptInvitation,
                &response,
                REQUEST_BODY_TOO_LARGE,
                false,
            )?;
            Ok(AcceptInvitationOutcome::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            require_problem(
                Operation::AcceptInvitation,
                &response,
                UNSUPPORTED_MEDIA_TYPE,
                false,
            )?;
            Ok(AcceptInvitationOutcome::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        _ => decode_common_list_failure(Operation::AcceptInvitation, &response)
            .map(AcceptInvitationOutcome::Common),
    }
}

fn decode_invitation_termination_response(
    operation: Operation,
    response: ReceivedResponse,
    expected_idempotency_key: &str,
) -> Result<InvitationTerminationOutcome, OrganizationError> {
    match response.status {
        StatusCode::NO_CONTENT => {
            require_response_idempotency_key(operation, &response, expected_idempotency_key)?;
            if response.content_type.is_some() || !response.body.is_empty() {
                return Err(OrganizationError::protocol(
                    operation,
                    "the successful response contains an unexpected representation",
                    false,
                ));
            }
            Ok(InvitationTerminationOutcome::Completed)
        }
        StatusCode::NOT_FOUND if matches!(operation, Operation::RevokeInvitation) => {
            require_problem(operation, &response, NOT_FOUND, false)?;
            Ok(InvitationTerminationOutcome::NotFound)
        }
        StatusCode::CONFLICT => {
            let problem_type = decode_problem_type(operation, &response, false)?;
            match problem_type.as_str() {
                INVITATION_UNAVAILABLE => Ok(InvitationTerminationOutcome::Unavailable),
                IDEMPOTENCY_CONFLICT => Ok(InvitationTerminationOutcome::IdempotencyConflict),
                _ => Err(OrganizationError::protocol(
                    operation,
                    "a 409 response has an unrecognized problem type",
                    false,
                )),
            }
        }
        StatusCode::PAYLOAD_TOO_LARGE if matches!(operation, Operation::DeclineInvitation) => {
            require_problem(operation, &response, REQUEST_BODY_TOO_LARGE, false)?;
            Ok(InvitationTerminationOutcome::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        StatusCode::UNSUPPORTED_MEDIA_TYPE if matches!(operation, Operation::DeclineInvitation) => {
            require_problem(operation, &response, UNSUPPORTED_MEDIA_TYPE, false)?;
            Ok(InvitationTerminationOutcome::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        _ => decode_common_list_failure(operation, &response)
            .map(InvitationTerminationOutcome::Common),
    }
}

fn decode_invitation_page(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<InvitationPage, OrganizationError> {
    let value = decode_json_value(
        operation,
        response,
        "the organization-invitation-list response body is invalid",
    )?;
    reject_null_invitation_page_optionals(operation, &value)?;
    let generated: generated_models::InvitationList = decode_json_model(
        operation,
        value,
        "the organization-invitation-list response body is invalid",
    )?;
    InvitationPage::try_from(generated)
        .map_err(|reason| OrganizationError::protocol(operation, reason, false))
}

fn decode_invitation_inbox_page(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<InvitationInboxPage, OrganizationError> {
    let value = decode_json_value(
        operation,
        response,
        "the invitation-inbox-list response body is invalid",
    )?;
    if value
        .get("nextCursor")
        .is_some_and(serde_json::Value::is_null)
    {
        return Err(OrganizationError::protocol(
            operation,
            "the invitation inbox response contains an explicit null optional field",
            false,
        ));
    }
    let generated: generated_models::InvitationInboxList = decode_json_model(
        operation,
        value,
        "the invitation-inbox-list response body is invalid",
    )?;
    InvitationInboxPage::try_from(generated)
        .map_err(|reason| OrganizationError::protocol(operation, reason, false))
}

fn decode_invitation(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<Invitation, OrganizationError> {
    let value = decode_json_value(
        operation,
        response,
        "the invitation response body is invalid",
    )?;
    reject_null_invitation_optionals(operation, &value)?;
    let generated: generated_models::Invitation =
        decode_json_model(operation, value, "the invitation response body is invalid")?;
    Invitation::try_from(generated)
        .map_err(|reason| OrganizationError::protocol(operation, reason, false))
}

fn decode_invitation_preview(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<InvitationPreview, OrganizationError> {
    let value = decode_json_value(
        operation,
        response,
        "the invitation preview response body is invalid",
    )?;
    let generated: generated_models::InvitationPreview = decode_json_model(
        operation,
        value,
        "the invitation preview response body is invalid",
    )?;
    InvitationPreview::try_from(generated)
        .map_err(|reason| OrganizationError::protocol(operation, reason, false))
}

fn decode_accepted_invitation_membership(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<AcceptedInvitationMembership, OrganizationError> {
    let value = decode_json_value(
        operation,
        response,
        "the invitation-acceptance response body is invalid",
    )?;
    let generated: generated_models::AcceptedInvitationMembership = decode_json_model(
        operation,
        value,
        "the invitation-acceptance response body is invalid",
    )?;
    AcceptedInvitationMembership::try_from(generated)
        .map_err(|reason| OrganizationError::protocol(operation, reason, false))
}

fn reject_null_invitation_page_optionals(
    operation: Operation,
    value: &serde_json::Value,
) -> Result<(), OrganizationError> {
    if value
        .get("nextCursor")
        .is_some_and(serde_json::Value::is_null)
    {
        return Err(OrganizationError::protocol(
            operation,
            "the invitation list response contains an explicit null optional field",
            false,
        ));
    }
    if let Some(items) = value.get("items").and_then(serde_json::Value::as_array) {
        for item in items {
            reject_null_invitation_optionals(operation, item)?;
        }
    }
    Ok(())
}

fn reject_null_invitation_optionals(
    operation: Operation,
    value: &serde_json::Value,
) -> Result<(), OrganizationError> {
    let has_null_optional = [
        "targetPrincipalId",
        "targetEmail",
        "terminalAt",
        "replacedInvitationId",
        "replacementInvitationId",
        "deliveryState",
    ]
    .into_iter()
    .any(|field| value.get(field).is_some_and(serde_json::Value::is_null));
    if has_null_optional {
        Err(OrganizationError::protocol(
            operation,
            "the invitation response contains an explicit null optional field",
            false,
        ))
    } else {
        Ok(())
    }
}

fn require_invitation_location(
    response: &ReceivedResponse,
    invitation_id: &str,
) -> Result<(), OrganizationError> {
    let expected = format!("/v1/invitations/{invitation_id}");
    if http_util::header_matches(response.location.as_ref(), &expected) {
        Ok(())
    } else {
        Err(OrganizationError::protocol(
            Operation::IssueInvitation,
            "the successful response has a missing or mismatched Location header",
            false,
        ))
    }
}

fn parse_retry_after(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<u64, OrganizationError> {
    response
        .retry_after
        .as_ref()
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            OrganizationError::protocol(
                operation,
                "a 429 response has an invalid Retry-After header",
                false,
            )
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

fn decode_current_membership_list_response(
    response: ReceivedResponse,
) -> Result<ListCurrentPrincipalMembershipsOutcome, OrganizationError> {
    match response.status {
        StatusCode::OK => {
            let value = decode_json_value(
                Operation::ListCurrentMemberships,
                &response,
                "the current-membership-list response body is invalid",
            )?;
            let has_null_optional_field = value
                .get("items")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|items| {
                    items.iter().any(|item| {
                        ["organizationDisplayName", "organizationSlug", "terminalAt"]
                            .into_iter()
                            .any(|field| item.get(field).is_some_and(serde_json::Value::is_null))
                    })
                });
            if value
                .get("nextCursor")
                .is_some_and(serde_json::Value::is_null)
                || has_null_optional_field
            {
                return Err(OrganizationError::protocol(
                    Operation::ListCurrentMemberships,
                    "the current-membership-list response contains an explicit null optional field",
                    false,
                ));
            }
            let generated: generated_models::CurrentPrincipalMembershipList = decode_json_model(
                Operation::ListCurrentMemberships,
                value,
                "the current-membership-list response body is invalid",
            )?;
            CurrentPrincipalMembershipPage::try_from(generated)
                .map(ListCurrentPrincipalMembershipsOutcome::Listed)
                .map_err(|reason| {
                    OrganizationError::protocol(Operation::ListCurrentMemberships, reason, false)
                })
        }
        _ => decode_common_list_failure(Operation::ListCurrentMemberships, &response)
            .map(ListCurrentPrincipalMembershipsOutcome::Common),
    }
}

fn decode_list_response(
    response: ReceivedResponse,
) -> Result<ListOrganizationMembershipsOutcome, OrganizationError> {
    match response.status {
        StatusCode::OK => {
            let value = decode_json_value(
                Operation::ListMemberships,
                &response,
                "the membership-list response body is invalid",
            )?;
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
            let generated: generated_models::OrganizationMembershipList = decode_json_model(
                Operation::ListMemberships,
                value,
                "the membership-list response body is invalid",
            )?;
            OrganizationMembershipPage::try_from(generated)
                .map(ListOrganizationMembershipsOutcome::Listed)
                .map_err(|reason| {
                    OrganizationError::protocol(Operation::ListMemberships, reason, false)
                })
        }
        StatusCode::NOT_FOUND => {
            require_problem(Operation::ListMemberships, &response, NOT_FOUND, false)?;
            Ok(ListOrganizationMembershipsOutcome::NotFound)
        }
        _ => decode_common_list_failure(Operation::ListMemberships, &response)
            .map(ListOrganizationMembershipsOutcome::Common),
    }
}

fn decode_membership_history_response(
    response: ReceivedResponse,
) -> Result<ListOrganizationMembershipHistoryOutcome, OrganizationError> {
    match response.status {
        StatusCode::OK => {
            let value = decode_json_value(
                Operation::ListMembershipHistory,
                &response,
                "the membership-history-list response body is invalid",
            )?;
            reject_null_membership_history_optionals(Operation::ListMembershipHistory, &value)?;
            let generated: generated_models::OrganizationMembershipHistoryList = decode_json_model(
                Operation::ListMembershipHistory,
                value,
                "the membership-history-list response body is invalid",
            )?;
            OrganizationMembershipHistoryPage::try_from(generated)
                .map(ListOrganizationMembershipHistoryOutcome::Listed)
                .map_err(|reason| {
                    OrganizationError::protocol(Operation::ListMembershipHistory, reason, false)
                })
        }
        StatusCode::NOT_FOUND => {
            require_problem(
                Operation::ListMembershipHistory,
                &response,
                NOT_FOUND,
                false,
            )?;
            Ok(ListOrganizationMembershipHistoryOutcome::NotFound)
        }
        _ => decode_common_list_failure(Operation::ListMembershipHistory, &response)
            .map(ListOrganizationMembershipHistoryOutcome::Common),
    }
}

fn decode_update_membership_response(
    response: ReceivedResponse,
    expected_idempotency_key: &str,
) -> Result<UpdateOrganizationMembershipOutcome, OrganizationError> {
    if response.status == StatusCode::OK {
        require_response_idempotency_key(
            Operation::UpdateMembership,
            &response,
            expected_idempotency_key,
        )?;
        let value = decode_json_value(
            Operation::UpdateMembership,
            &response,
            "the membership-update response body is invalid",
        )?;
        reject_null_membership_history_optionals(Operation::UpdateMembership, &value)?;
        let generated: generated_models::OrganizationMembershipHistoryEntry = decode_json_model(
            Operation::UpdateMembership,
            value,
            "the membership-update response body is invalid",
        )?;
        return OrganizationMembershipHistoryEntry::try_from(generated)
            .map(UpdateOrganizationMembershipOutcome::Updated)
            .map_err(|reason| {
                OrganizationError::protocol(Operation::UpdateMembership, reason, false)
            });
    }

    decode_membership_mutation_failure(Operation::UpdateMembership, &response).map(|failure| {
        match failure {
            MembershipMutationFailure::Common(common) => {
                UpdateOrganizationMembershipOutcome::Common(common)
            }
            MembershipMutationFailure::NotFound => UpdateOrganizationMembershipOutcome::NotFound,
            MembershipMutationFailure::TransitionUnavailable => {
                UpdateOrganizationMembershipOutcome::TransitionUnavailable
            }
            MembershipMutationFailure::HumanOwnerRequired => {
                UpdateOrganizationMembershipOutcome::HumanOwnerRequired
            }
            MembershipMutationFailure::IdempotencyConflict => {
                UpdateOrganizationMembershipOutcome::IdempotencyConflict
            }
        }
    })
}

fn decode_membership_termination_response(
    operation: Operation,
    response: ReceivedResponse,
    expected_idempotency_key: &str,
) -> Result<MembershipTerminationOutcome, OrganizationError> {
    if response.status == StatusCode::NO_CONTENT {
        require_response_idempotency_key(operation, &response, expected_idempotency_key)?;
        if response.content_type.is_some() || !response.body.is_empty() {
            return Err(OrganizationError::protocol(
                operation,
                "the successful response contains an unexpected representation",
                false,
            ));
        }
        return Ok(MembershipTerminationOutcome::Ended);
    }

    decode_membership_mutation_failure(operation, &response).map(|failure| match failure {
        MembershipMutationFailure::Common(common) => MembershipTerminationOutcome::Common(common),
        MembershipMutationFailure::NotFound => MembershipTerminationOutcome::NotFound,
        MembershipMutationFailure::TransitionUnavailable => {
            MembershipTerminationOutcome::TransitionUnavailable
        }
        MembershipMutationFailure::HumanOwnerRequired => {
            MembershipTerminationOutcome::HumanOwnerRequired
        }
        MembershipMutationFailure::IdempotencyConflict => {
            MembershipTerminationOutcome::IdempotencyConflict
        }
    })
}

fn reject_null_membership_history_optionals(
    operation: Operation,
    value: &serde_json::Value,
) -> Result<(), OrganizationError> {
    let has_null_optional = |entry: &serde_json::Value| {
        ["displayName", "terminalAt"]
            .into_iter()
            .any(|field| entry.get(field).is_some_and(serde_json::Value::is_null))
    };
    let null_entry_optional = has_null_optional(value)
        || value
            .get("items")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| items.iter().any(has_null_optional));
    let null_page_optional = value
        .get("nextCursor")
        .is_some_and(serde_json::Value::is_null);
    if null_entry_optional || null_page_optional {
        return Err(OrganizationError::protocol(
            operation,
            "the membership history response contains an explicit null optional field",
            false,
        ));
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
enum MembershipMutationFailure {
    Common(CommonOrganizationFailure),
    NotFound,
    TransitionUnavailable,
    HumanOwnerRequired,
    IdempotencyConflict,
}

fn decode_membership_mutation_failure(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<MembershipMutationFailure, OrganizationError> {
    match response.status {
        StatusCode::BAD_REQUEST => {
            require_problem(operation, response, BAD_REQUEST, false)?;
            Ok(MembershipMutationFailure::Common(
                CommonOrganizationFailure::InvalidInput,
            ))
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(operation, response, UNAUTHORIZED, true)?;
            Ok(MembershipMutationFailure::Common(
                CommonOrganizationFailure::Unauthenticated,
            ))
        }
        StatusCode::FORBIDDEN => {
            require_problem(operation, response, FORBIDDEN, false)?;
            Ok(MembershipMutationFailure::Common(
                CommonOrganizationFailure::Forbidden,
            ))
        }
        StatusCode::NOT_FOUND => {
            require_problem(operation, response, NOT_FOUND, false)?;
            Ok(MembershipMutationFailure::NotFound)
        }
        StatusCode::CONFLICT => {
            let problem_type = decode_problem_type(operation, response, false)?;
            match problem_type.as_str() {
                MEMBERSHIP_TRANSITION_UNAVAILABLE => {
                    Ok(MembershipMutationFailure::TransitionUnavailable)
                }
                HUMAN_OWNER_REQUIRED => Ok(MembershipMutationFailure::HumanOwnerRequired),
                IDEMPOTENCY_CONFLICT => Ok(MembershipMutationFailure::IdempotencyConflict),
                _ => Err(OrganizationError::protocol(
                    operation,
                    "a 409 response has an unrecognized problem type",
                    false,
                )),
            }
        }
        _ => decode_server_or_invalid_response(operation, response)
            .map(MembershipMutationFailure::Common),
    }
}

fn decode_json_value(
    operation: Operation,
    response: &ReceivedResponse,
    invalid_body_reason: &'static str,
) -> Result<serde_json::Value, OrganizationError> {
    require_media_type(operation, response, JSON_MEDIA_TYPE, false)?;
    serde_json::from_slice(&response.body)
        .map_err(|_| OrganizationError::protocol(operation, invalid_body_reason, false))
}

fn decode_json_model<T: DeserializeOwned>(
    operation: Operation,
    value: serde_json::Value,
    invalid_body_reason: &'static str,
) -> Result<T, OrganizationError> {
    serde_json::from_value(value)
        .map_err(|_| OrganizationError::protocol(operation, invalid_body_reason, false))
}

fn decode_common_list_failure(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<CommonOrganizationFailure, OrganizationError> {
    match response.status {
        StatusCode::BAD_REQUEST => {
            require_problem(operation, response, BAD_REQUEST, false)?;
            Ok(CommonOrganizationFailure::InvalidInput)
        }
        StatusCode::UNAUTHORIZED => {
            require_problem(operation, response, UNAUTHORIZED, true)?;
            Ok(CommonOrganizationFailure::Unauthenticated)
        }
        StatusCode::FORBIDDEN => {
            require_problem(operation, response, FORBIDDEN, false)?;
            Ok(CommonOrganizationFailure::Forbidden)
        }
        _ => decode_server_or_invalid_response(operation, response),
    }
}

fn decode_server_or_invalid_response(
    operation: Operation,
    response: &ReceivedResponse,
) -> Result<CommonOrganizationFailure, OrganizationError> {
    if response.status.is_server_error() {
        return Ok(CommonOrganizationFailure::Unreachable(
            UnreachableCategory::Server,
        ));
    }
    let reason = if response.status.is_redirection() {
        "redirect responses are not permitted"
    } else {
        "the HTTP status is not valid for this operation"
    };
    Err(OrganizationError::protocol(operation, reason, false))
}

fn require_response_idempotency_key(
    operation: Operation,
    response: &ReceivedResponse,
    expected: &str,
) -> Result<(), OrganizationError> {
    if http_util::header_matches(response.idempotency_key.as_ref(), expected) {
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
    if http_util::header_matches(response.location.as_ref(), &expected) {
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
    problem::require_type(response, expected_type)
        .map_err(|reason| OrganizationError::protocol(operation, reason, credential_rejected))
}

fn decode_problem_type(
    operation: Operation,
    response: &ReceivedResponse,
    credential_rejected: bool,
) -> Result<String, OrganizationError> {
    problem::decode_type(response)
        .map_err(|reason| OrganizationError::protocol(operation, reason, credential_rejected))
}

fn require_media_type(
    operation: Operation,
    response: &ReceivedResponse,
    expected: &'static str,
    credential_rejected: bool,
) -> Result<(), OrganizationError> {
    http_util::require_media_type(response.content_type.as_deref(), expected)
        .map_err(|reason| OrganizationError::protocol(operation, reason, credential_rejected))
}

#[cfg(test)]
mod tests;
