use serde::Serialize;

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

impl TryFrom<models::Organization> for Organization {
    type Error = &'static str;

    fn try_from(value: models::Organization) -> Result<Self, Self::Error> {
        require_nonempty(&value.id, "the organization ID is empty")?;
        require_nonempty(
            &value.display_name,
            "the organization display name is empty",
        )?;
        require_nonempty(&value.slug, "the organization slug is empty")?;
        require_nonempty(&value.created_at, "the organization creation time is empty")?;
        require_nonempty(&value.updated_at, "the organization update time is empty")?;

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

impl TryFrom<models::OrganizationMembershipList> for OrganizationMembershipPage {
    type Error = &'static str;

    fn try_from(value: models::OrganizationMembershipList) -> Result<Self, Self::Error> {
        if value.next_cursor.as_deref() == Some("") {
            return Err("the organization membership cursor is empty");
        }
        let items = value
            .items
            .into_iter()
            .map(OrganizationMembershipDirectoryEntry::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            items,
            next_cursor: value.next_cursor,
        })
    }
}

impl TryFrom<models::OrganizationMembershipDirectoryEntry>
    for OrganizationMembershipDirectoryEntry
{
    type Error = &'static str;

    fn try_from(value: models::OrganizationMembershipDirectoryEntry) -> Result<Self, Self::Error> {
        require_nonempty(&value.id, "the organization membership ID is empty")?;
        require_nonempty(
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

fn require_nonempty(value: &str, reason: &'static str) -> Result<(), &'static str> {
    if value.is_empty() {
        Err(reason)
    } else {
        Ok(())
    }
}
