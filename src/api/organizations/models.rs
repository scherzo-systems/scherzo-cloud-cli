use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::api::generated::models;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Organization {
    pub(crate) id: String,
    pub(crate) state: OrganizationState,
    pub(crate) display_name: String,
    pub(crate) slug: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OrganizationState {
    Active,
    Suspended,
    DeletionPending,
    Deleted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CurrentPrincipalMembership {
    pub(crate) id: String,
    pub(crate) organization_id: String,
    pub(crate) organization_state: OrganizationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) organization_display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) organization_slug: Option<String>,
    pub(crate) role: MembershipRole,
    pub(crate) state: MembershipState,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) terminal_at: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CurrentPrincipalMembershipPage {
    pub(crate) items: Vec<CurrentPrincipalMembership>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrganizationMembershipDirectoryEntry {
    pub(crate) id: String,
    pub(crate) principal_id: String,
    pub(crate) principal_type: PrincipalType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) display_name: Option<String>,
    pub(crate) role: MembershipRole,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrganizationMembershipPage {
    pub(crate) items: Vec<OrganizationMembershipDirectoryEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrganizationMembershipHistoryEntry {
    // Owner history and current-principal history intentionally remain separate models: their
    // optional profiles belong to different principals and obey different privacy rules.
    // jscpd:ignore-start
    pub(crate) id: String,
    pub(crate) organization_id: String,
    pub(crate) principal_id: String,
    pub(crate) principal_type: PrincipalType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) display_name: Option<String>,
    pub(crate) role: MembershipRole,
    pub(crate) state: MembershipState,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) terminal_at: Option<String>,
    // jscpd:ignore-end
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrganizationMembershipHistoryPage {
    pub(crate) items: Vec<OrganizationMembershipHistoryEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Invitation {
    pub(crate) id: String,
    pub(crate) organization_id: String,
    pub(crate) issuer_principal_id: String,
    pub(crate) target_kind: InvitationTargetKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) target_principal_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) target_email: Option<String>,
    pub(crate) state: InvitationState,
    pub(crate) issued_at: String,
    pub(crate) expires_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) terminal_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) replaced_invitation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) replacement_invitation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) delivery_state: Option<InvitationDeliveryState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InvitationTargetKind {
    Principal,
    Email,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InvitationState {
    Outstanding,
    Accepted,
    Declined,
    Revoked,
    Expired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InvitationDeliveryState {
    Pending,
    Leased,
    Sent,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InvitationPage {
    pub(crate) items: Vec<Invitation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InvitationInboxEntry {
    pub(crate) id: String,
    pub(crate) organization_id: String,
    pub(crate) organization_display_name: String,
    pub(crate) organization_slug: String,
    pub(crate) issuer_principal_id: String,
    pub(crate) expires_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InvitationInboxPage {
    pub(crate) items: Vec<InvitationInboxEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InvitationPreview {
    pub(crate) id: String,
    pub(crate) organization_id: String,
    pub(crate) organization_display_name: String,
    pub(crate) organization_slug: String,
    pub(crate) target_kind: InvitationTargetKind,
    pub(crate) expires_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AcceptedInvitationMembership {
    pub(crate) id: String,
    pub(crate) organization_id: String,
    pub(crate) principal_id: String,
    pub(crate) role: MembershipRole,
    pub(crate) state: MembershipState,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PrincipalType {
    Human,
    Service,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MembershipRole {
    Owner,
    Member,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MembershipState {
    Active,
    Suspended,
    Ended,
}

impl TryFrom<models::Organization> for Organization {
    type Error = &'static str;

    fn try_from(value: models::Organization) -> Result<Self, Self::Error> {
        super::super::http_util::require_nonempty(&value.id, "the organization ID is empty")?;
        super::super::http_util::require_nonempty(
            &value.display_name,
            "the organization display name is empty",
        )?;
        super::super::http_util::require_nonempty(&value.slug, "the organization slug is empty")?;
        super::super::http_util::require_nonempty(
            &value.created_at,
            "the organization creation time is empty",
        )?;
        super::super::http_util::require_nonempty(
            &value.updated_at,
            "the organization update time is empty",
        )?;

        Ok(Self {
            id: value.id,
            state: match value.state {
                models::organization::State::Active => OrganizationState::Active,
            },
            display_name: value.display_name,
            slug: value.slug,
            created_at: value.created_at,
            updated_at: value.updated_at,
        })
    }
}

impl TryFrom<models::InvitationList> for InvitationPage {
    type Error = &'static str;

    fn try_from(value: models::InvitationList) -> Result<Self, Self::Error> {
        let (items, next_cursor) = convert_page(
            value.items,
            value.next_cursor,
            "the organization invitation cursor is empty",
        )?;
        Ok(Self { items, next_cursor })
    }
}

impl TryFrom<models::Invitation> for Invitation {
    type Error = &'static str;

    fn try_from(value: models::Invitation) -> Result<Self, Self::Error> {
        if !crate::public_id::valid_typed_id(&value.id, "inv_") {
            return Err("the invitation ID is invalid");
        }
        if !crate::public_id::valid_typed_id(&value.organization_id, "org_") {
            return Err("the invitation organization ID is invalid");
        }
        if !crate::public_id::valid_typed_id(&value.issuer_principal_id, "prn_") {
            return Err("the invitation issuer principal ID is invalid");
        }
        parse_timestamp(&value.issued_at, "the invitation issue time is invalid")?;
        parse_timestamp(
            &value.expires_at,
            "the invitation expiration time is invalid",
        )?;
        if let Some(terminal_at) = value.terminal_at.as_deref() {
            parse_timestamp(terminal_at, "the invitation terminal time is invalid")?;
        }
        for related_id in [
            value.replaced_invitation_id.as_deref(),
            value.replacement_invitation_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if !crate::public_id::valid_typed_id(related_id, "inv_") {
                return Err("an invitation replacement ID is invalid");
            }
        }

        let target_kind = match value.target_kind {
            models::invitation::TargetKind::Principal => InvitationTargetKind::Principal,
            models::invitation::TargetKind::Email => InvitationTargetKind::Email,
        };
        let state = match value.state {
            models::invitation::State::Outstanding => InvitationState::Outstanding,
            models::invitation::State::Accepted => InvitationState::Accepted,
            models::invitation::State::Declined => InvitationState::Declined,
            models::invitation::State::Revoked => InvitationState::Revoked,
            models::invitation::State::Expired => InvitationState::Expired,
        };
        let target_is_valid = match (state, target_kind) {
            (InvitationState::Outstanding, InvitationTargetKind::Principal) => {
                value
                    .target_principal_id
                    .as_deref()
                    .is_some_and(|id| crate::public_id::valid_typed_id(id, "prn_"))
                    && value.target_email.is_none()
            }
            (InvitationState::Outstanding, InvitationTargetKind::Email) => {
                value
                    .target_email
                    .as_deref()
                    .is_some_and(|email| valid_bounded_text(email, 3, 320))
                    && value.target_principal_id.is_none()
            }
            (_, _) => value.target_principal_id.is_none() && value.target_email.is_none(),
        };
        if !target_is_valid
            || target_kind == InvitationTargetKind::Principal && value.delivery_state.is_some()
        {
            return Err("the invitation target projection is invalid");
        }

        Ok(Self {
            id: value.id,
            organization_id: value.organization_id,
            issuer_principal_id: value.issuer_principal_id,
            target_kind,
            target_principal_id: value.target_principal_id,
            target_email: value.target_email,
            state,
            issued_at: value.issued_at,
            expires_at: value.expires_at,
            terminal_at: value.terminal_at,
            replaced_invitation_id: value.replaced_invitation_id,
            replacement_invitation_id: value.replacement_invitation_id,
            delivery_state: value.delivery_state.map(|state| match state {
                models::invitation::DeliveryState::Pending => InvitationDeliveryState::Pending,
                models::invitation::DeliveryState::Leased => InvitationDeliveryState::Leased,
                models::invitation::DeliveryState::Sent => InvitationDeliveryState::Sent,
                models::invitation::DeliveryState::Failed => InvitationDeliveryState::Failed,
            }),
        })
    }
}

impl TryFrom<models::InvitationInboxList> for InvitationInboxPage {
    type Error = &'static str;

    fn try_from(value: models::InvitationInboxList) -> Result<Self, Self::Error> {
        let (items, next_cursor) = convert_page(
            value.items,
            value.next_cursor,
            "the invitation inbox cursor is empty",
        )?;
        Ok(Self { items, next_cursor })
    }
}

impl TryFrom<models::InvitationInboxEntry> for InvitationInboxEntry {
    type Error = &'static str;

    fn try_from(value: models::InvitationInboxEntry) -> Result<Self, Self::Error> {
        if !crate::public_id::valid_typed_id(&value.id, "inv_")
            || !crate::public_id::valid_typed_id(&value.organization_id, "org_")
            || !crate::public_id::valid_typed_id(&value.issuer_principal_id, "prn_")
            || !valid_bounded_text(&value.organization_display_name, 1, 200)
            || !crate::public_id::valid_url_safe_name(&value.organization_slug)
        {
            return Err("the invitation inbox entry is invalid");
        }
        parse_timestamp(
            &value.expires_at,
            "the invitation inbox expiration time is invalid",
        )?;
        Ok(Self {
            id: value.id,
            organization_id: value.organization_id,
            organization_display_name: value.organization_display_name,
            organization_slug: value.organization_slug,
            issuer_principal_id: value.issuer_principal_id,
            expires_at: value.expires_at,
        })
    }
}

impl TryFrom<models::InvitationPreview> for InvitationPreview {
    type Error = &'static str;

    fn try_from(value: models::InvitationPreview) -> Result<Self, Self::Error> {
        if !crate::public_id::valid_typed_id(&value.id, "inv_")
            || !crate::public_id::valid_typed_id(&value.organization_id, "org_")
            || !valid_bounded_text(&value.organization_display_name, 1, 200)
            || !crate::public_id::valid_url_safe_name(&value.organization_slug)
        {
            return Err("the invitation preview is invalid");
        }
        parse_timestamp(
            &value.expires_at,
            "the invitation preview expiration time is invalid",
        )?;
        Ok(Self {
            id: value.id,
            organization_id: value.organization_id,
            organization_display_name: value.organization_display_name,
            organization_slug: value.organization_slug,
            target_kind: match value.target_kind {
                models::invitation_preview::TargetKind::Principal => {
                    InvitationTargetKind::Principal
                }
                models::invitation_preview::TargetKind::Email => InvitationTargetKind::Email,
            },
            expires_at: value.expires_at,
        })
    }
}

impl TryFrom<models::AcceptedInvitationMembership> for AcceptedInvitationMembership {
    type Error = &'static str;

    fn try_from(value: models::AcceptedInvitationMembership) -> Result<Self, Self::Error> {
        if !crate::public_id::valid_typed_id(&value.id, "mem_")
            || !crate::public_id::valid_typed_id(&value.organization_id, "org_")
            || !crate::public_id::valid_typed_id(&value.principal_id, "prn_")
        {
            return Err("the accepted invitation membership is invalid");
        }
        parse_timestamp(
            &value.created_at,
            "the accepted membership creation time is invalid",
        )?;
        parse_timestamp(
            &value.updated_at,
            "the accepted membership update time is invalid",
        )?;
        Ok(Self {
            id: value.id,
            organization_id: value.organization_id,
            principal_id: value.principal_id,
            role: match value.role {
                models::accepted_invitation_membership::Role::Owner => MembershipRole::Owner,
                models::accepted_invitation_membership::Role::Member => MembershipRole::Member,
            },
            state: match value.state {
                models::accepted_invitation_membership::State::Active => MembershipState::Active,
            },
            created_at: value.created_at,
            updated_at: value.updated_at,
        })
    }
}

impl TryFrom<models::CurrentPrincipalMembershipList> for CurrentPrincipalMembershipPage {
    type Error = &'static str;

    fn try_from(value: models::CurrentPrincipalMembershipList) -> Result<Self, Self::Error> {
        let (items, next_cursor) = convert_page(
            value.items,
            value.next_cursor,
            "the current-principal membership cursor is empty",
        )?;
        Ok(Self { items, next_cursor })
    }
}

impl TryFrom<models::CurrentPrincipalMembershipEntry> for CurrentPrincipalMembership {
    type Error = &'static str;

    fn try_from(value: models::CurrentPrincipalMembershipEntry) -> Result<Self, Self::Error> {
        if !crate::public_id::valid_typed_id(&value.id, "mem_") {
            return Err("the current-principal membership ID is invalid");
        }
        if !crate::public_id::valid_typed_id(&value.organization_id, "org_") {
            return Err("the current-principal membership organization ID is invalid");
        }

        let organization_state = match value.organization_state {
            models::current_principal_membership_entry::OrganizationState::Active => {
                OrganizationState::Active
            }
            models::current_principal_membership_entry::OrganizationState::Suspended => {
                OrganizationState::Suspended
            }
            models::current_principal_membership_entry::OrganizationState::DeletionPending => {
                OrganizationState::DeletionPending
            }
            models::current_principal_membership_entry::OrganizationState::Deleted => {
                OrganizationState::Deleted
            }
        };
        let state = match value.state {
            models::current_principal_membership_entry::State::Active => MembershipState::Active,
            models::current_principal_membership_entry::State::Suspended => {
                MembershipState::Suspended
            }
            models::current_principal_membership_entry::State::Ended => MembershipState::Ended,
        };
        let profile_is_visible =
            organization_state == OrganizationState::Active && state == MembershipState::Active;
        match (
            profile_is_visible,
            value.organization_display_name.as_deref(),
            value.organization_slug.as_deref(),
        ) {
            (true, Some(name), Some(slug))
                if valid_bounded_text(name, 1, 200)
                    && crate::public_id::valid_url_safe_name(slug) => {}
            (true, _, _) => {
                return Err("an active membership is missing its organization profile");
            }
            (false, None, None) => {}
            (false, _, _) => {
                return Err("an inactive membership exposes its organization profile");
            }
        }

        parse_timestamp(
            &value.created_at,
            "the current-principal membership creation time is invalid",
        )?;
        parse_timestamp(
            &value.updated_at,
            "the current-principal membership update time is invalid",
        )?;
        if let Some(terminal_at) = value.terminal_at.as_deref() {
            parse_timestamp(
                terminal_at,
                "the current-principal membership terminal time is invalid",
            )?;
        }

        Ok(Self {
            id: value.id,
            organization_id: value.organization_id,
            organization_state,
            organization_display_name: value.organization_display_name,
            organization_slug: value.organization_slug,
            role: match value.role {
                models::current_principal_membership_entry::Role::Owner => MembershipRole::Owner,
                models::current_principal_membership_entry::Role::Member => MembershipRole::Member,
            },
            state,
            created_at: value.created_at,
            updated_at: value.updated_at,
            terminal_at: value.terminal_at,
        })
    }
}

impl TryFrom<models::OrganizationMembershipList> for OrganizationMembershipPage {
    type Error = &'static str;

    fn try_from(value: models::OrganizationMembershipList) -> Result<Self, Self::Error> {
        let (items, next_cursor) = convert_page(
            value.items,
            value.next_cursor,
            "the organization membership cursor is empty",
        )?;
        Ok(Self { items, next_cursor })
    }
}

impl TryFrom<models::OrganizationMembershipDirectoryEntry>
    for OrganizationMembershipDirectoryEntry
{
    type Error = &'static str;

    fn try_from(value: models::OrganizationMembershipDirectoryEntry) -> Result<Self, Self::Error> {
        super::super::http_util::require_nonempty(
            &value.id,
            "the organization membership ID is empty",
        )?;
        super::super::http_util::require_nonempty(
            &value.principal_id,
            "the organization membership principal ID is empty",
        )?;
        if value.display_name.as_deref() == Some("") {
            return Err("the organization membership display name is empty");
        }

        Ok(Self {
            id: value.id,
            principal_id: value.principal_id,
            principal_type: match value.principal_type {
                models::organization_membership_directory_entry::PrincipalType::Human => {
                    PrincipalType::Human
                }
                models::organization_membership_directory_entry::PrincipalType::Service => {
                    PrincipalType::Service
                }
            },
            display_name: value.display_name,
            role: match value.role {
                models::organization_membership_directory_entry::Role::Owner => {
                    MembershipRole::Owner
                }
                models::organization_membership_directory_entry::Role::Member => {
                    MembershipRole::Member
                }
            },
        })
    }
}

impl TryFrom<models::OrganizationMembershipHistoryList> for OrganizationMembershipHistoryPage {
    type Error = &'static str;

    fn try_from(value: models::OrganizationMembershipHistoryList) -> Result<Self, Self::Error> {
        let (items, next_cursor) = convert_page(
            value.items,
            value.next_cursor,
            "the organization membership history cursor is empty",
        )?;
        Ok(Self { items, next_cursor })
    }
}

impl TryFrom<models::OrganizationMembershipHistoryEntry> for OrganizationMembershipHistoryEntry {
    type Error = &'static str;

    fn try_from(value: models::OrganizationMembershipHistoryEntry) -> Result<Self, Self::Error> {
        if !crate::public_id::valid_typed_id(&value.id, "mem_") {
            return Err("the organization membership history ID is invalid");
        }
        if !crate::public_id::valid_typed_id(&value.organization_id, "org_") {
            return Err("the organization membership history organization ID is invalid");
        }
        if !crate::public_id::valid_typed_id(&value.principal_id, "prn_") {
            return Err("the organization membership history principal ID is invalid");
        }
        if value
            .display_name
            .as_deref()
            .is_some_and(|name| !valid_bounded_text(name, 1, 200))
        {
            return Err("the organization membership history display name is invalid");
        }
        parse_timestamp(
            &value.created_at,
            "the organization membership history creation time is invalid",
        )?;
        parse_timestamp(
            &value.updated_at,
            "the organization membership history update time is invalid",
        )?;
        if let Some(terminal_at) = value.terminal_at.as_deref() {
            parse_timestamp(
                terminal_at,
                "the organization membership history terminal time is invalid",
            )?;
        }

        Ok(Self {
            id: value.id,
            organization_id: value.organization_id,
            principal_id: value.principal_id,
            principal_type: match value.principal_type {
                models::organization_membership_history_entry::PrincipalType::Human => {
                    PrincipalType::Human
                }
                models::organization_membership_history_entry::PrincipalType::Service => {
                    PrincipalType::Service
                }
            },
            display_name: value.display_name,
            role: match value.role {
                models::organization_membership_history_entry::Role::Owner => MembershipRole::Owner,
                models::organization_membership_history_entry::Role::Member => {
                    MembershipRole::Member
                }
            },
            state: match value.state {
                models::organization_membership_history_entry::State::Active => {
                    MembershipState::Active
                }
                models::organization_membership_history_entry::State::Suspended => {
                    MembershipState::Suspended
                }
                models::organization_membership_history_entry::State::Ended => {
                    MembershipState::Ended
                }
            },
            created_at: value.created_at,
            updated_at: value.updated_at,
            terminal_at: value.terminal_at,
        })
    }
}

fn convert_page<T, U>(
    items: Vec<T>,
    next_cursor: Option<String>,
    empty_cursor_reason: &'static str,
) -> Result<(Vec<U>, Option<String>), &'static str>
where
    U: TryFrom<T, Error = &'static str>,
{
    if next_cursor.as_deref() == Some("") {
        return Err(empty_cursor_reason);
    }
    let items = items
        .into_iter()
        .map(U::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((items, next_cursor))
}

fn parse_timestamp(value: &str, reason: &'static str) -> Result<OffsetDateTime, &'static str> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|_| reason)
}

fn valid_bounded_text(value: &str, minimum: usize, maximum: usize) -> bool {
    (minimum..=maximum).contains(&value.chars().count())
}
