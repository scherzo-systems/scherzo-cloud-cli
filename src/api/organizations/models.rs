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
