use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use crate::api::{
    CommonOrganizationFailure, CreateOrganizationOutcome, CurrentPrincipalMembership,
    GetOrganizationOutcome, ListCurrentPrincipalMembershipsOutcome,
    ListOrganizationMembershipsOutcome, MembershipRole, MembershipState, Organization,
    OrganizationMembershipDirectoryEntry, OrganizationState, PrincipalType,
    UpdateOrganizationOutcome,
};
use crate::exit_code::{ExitCode, OutcomeClass};

pub(super) fn write_create(
    deployment: &str,
    outcome: &CreateOrganizationOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        CreateOrganizationOutcome::Created(organization) => {
            write_organization_success(deployment, "created", organization, json)?;
            Ok(ExitCode::Success)
        }
        CreateOrganizationOutcome::Common(common) => write_common(
            deployment,
            common,
            json,
            "Organization creation could not be confirmed",
        ),
        CreateOrganizationOutcome::CreationNotPermitted => write_failure(
            deployment,
            "creation_not_permitted",
            None,
            None,
            "! Organization creation is not permitted for this account.",
            OutcomeClass::Forbidden,
            json,
        ),
        CreateOrganizationOutcome::SlugUnavailable => write_failure(
            deployment,
            "slug_unavailable",
            None,
            None,
            "! The requested organization slug is unavailable.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        CreateOrganizationOutcome::QuantityLimitReached => write_failure(
            deployment,
            "quantity_limit_reached",
            None,
            None,
            "! The organization quantity limit has been reached.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        CreateOrganizationOutcome::RateLimited { retry_after } => write_failure(
            deployment,
            "rate_limited",
            None,
            Some(*retry_after),
            &format!(
                "! Organization creation is rate limited. Try again in {retry_after} seconds."
            ),
            OutcomeClass::RateLimited,
            json,
        ),
        CreateOrganizationOutcome::IdempotencyConflict => write_failure(
            deployment,
            "idempotency_conflict",
            None,
            None,
            "! The organization request identity conflicted with another request.",
            OutcomeClass::GeneralFailure,
            json,
        ),
    }
}

pub(super) fn write_show(
    deployment: &str,
    outcome: &GetOrganizationOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        GetOrganizationOutcome::Found(organization) => {
            write_organization_success(deployment, "found", organization, json)?;
            Ok(ExitCode::Success)
        }
        GetOrganizationOutcome::Common(common) => write_common(
            deployment,
            common,
            json,
            "The Scherzo Cloud deployment could not be reached",
        ),
        GetOrganizationOutcome::NotFound => write_not_found(deployment, json),
    }
}

pub(super) fn write_update(
    deployment: &str,
    outcome: &UpdateOrganizationOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        UpdateOrganizationOutcome::Updated(organization) => {
            write_organization_success(deployment, "updated", organization, json)?;
            Ok(ExitCode::Success)
        }
        UpdateOrganizationOutcome::Common(common) => write_common(
            deployment,
            common,
            json,
            "Organization update could not be confirmed",
        ),
        UpdateOrganizationOutcome::NotFound => write_not_found(deployment, json),
        UpdateOrganizationOutcome::SlugUnavailable => write_failure(
            deployment,
            "slug_unavailable",
            None,
            None,
            "! The requested organization slug is unavailable.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        UpdateOrganizationOutcome::IdempotencyConflict => write_failure(
            deployment,
            "idempotency_conflict",
            None,
            None,
            "! The organization request identity conflicted with another request.",
            OutcomeClass::GeneralFailure,
            json,
        ),
    }
}

pub(super) fn write_list(
    deployment: &str,
    outcome: &ListCurrentPrincipalMembershipsOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        ListCurrentPrincipalMembershipsOutcome::Listed(page) => {
            if json {
                write_list_json(deployment, &page.items, page.next_cursor.as_deref())?;
            } else {
                write_current_memberships_human(
                    deployment,
                    &page.items,
                    page.next_cursor.as_deref(),
                )?;
            }
            Ok(ExitCode::Success)
        }
        ListCurrentPrincipalMembershipsOutcome::Common(common) => {
            write_current_membership_failure(deployment, common, json)
        }
    }
}

pub(super) fn write_members_list(
    deployment: &str,
    outcome: &ListOrganizationMembershipsOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        ListOrganizationMembershipsOutcome::Listed(page) => {
            if json {
                write_list_json(deployment, &page.items, page.next_cursor.as_deref())?;
            } else {
                write_members_human(deployment, &page.items, page.next_cursor.as_deref())?;
            }
            Ok(ExitCode::Success)
        }
        ListOrganizationMembershipsOutcome::Common(common) => write_common(
            deployment,
            common,
            json,
            "The Scherzo Cloud deployment could not be reached",
        ),
        ListOrganizationMembershipsOutcome::NotFound => write_not_found(deployment, json),
    }
}

fn write_not_found(deployment: &str, json: bool) -> anyhow::Result<ExitCode> {
    write_failure(
        deployment,
        "not_found",
        None,
        None,
        "! Organization not found or unavailable.",
        OutcomeClass::GeneralFailure,
        json,
    )
}

fn write_common(
    deployment: &str,
    outcome: &CommonOrganizationFailure,
    json: bool,
    unreachable_message: &'static str,
) -> anyhow::Result<ExitCode> {
    match outcome {
        CommonOrganizationFailure::Unauthenticated => write_failure(
            deployment,
            "unauthenticated",
            None,
            None,
            "! You must sign in before managing Scherzo Cloud organizations.\n\nRun:\n  scherzo-cloud auth login",
            OutcomeClass::Unauthenticated,
            json,
        ),
        CommonOrganizationFailure::Forbidden => write_failure(
            deployment,
            "forbidden",
            None,
            None,
            "! This account is not permitted to perform that organization operation.",
            OutcomeClass::Forbidden,
            json,
        ),
        CommonOrganizationFailure::InvalidInput => write_failure(
            deployment,
            "invalid_input",
            None,
            None,
            "! The organization input was rejected by the deployment.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        CommonOrganizationFailure::Unreachable(category) => write_failure(
            deployment,
            "unreachable",
            Some(category.as_str()),
            None,
            &format!("! {unreachable_message} ({}).", category.as_str()),
            super::super::unreachable_outcome_class(*category),
            json,
        ),
    }
}

fn write_organization_success(
    deployment: &str,
    outcome: &'static str,
    organization: &Organization,
    json: bool,
) -> anyhow::Result<()> {
    if json {
        write_json(&OrganizationResult {
            schema_version: 1,
            deployment,
            outcome,
            organization,
        })
    } else {
        let heading = match outcome {
            "created" => "✓ Organization created.",
            "found" => "✓ Organization found.",
            "updated" => "✓ Organization updated.",
            _ => "✓ Organization available.",
        };
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        writeln!(stdout, "{heading}\n")?;
        writeln!(stdout, "  Organization: {}", organization.id)?;
        writeln!(stdout, "  Name:         {}", organization.display_name)?;
        writeln!(stdout, "  Slug:         {}", organization.slug)?;
        writeln!(
            stdout,
            "  State:        {}",
            organization_state(organization.state)
        )?;
        writeln!(stdout, "  Deployment:   {deployment}")?;
        Ok(())
    }
}

fn write_list_json(
    deployment: &str,
    items: &[impl Serialize],
    next_cursor: Option<&str>,
) -> anyhow::Result<()> {
    write_json(&ListResult {
        schema_version: 1,
        deployment,
        outcome: "listed",
        items,
        next_cursor,
    })
}

fn write_current_memberships_human(
    deployment: &str,
    items: &[CurrentPrincipalMembership],
    next_cursor: Option<&str>,
) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✓ Organization memberships listed.\n")?;
    for item in items {
        writeln!(
            stdout,
            "membership: {} · role: {} · membership state: {}",
            item.id,
            membership_role(item.role),
            membership_state(item.state)
        )?;
        writeln!(
            stdout,
            "organization: {} · organization state: {}",
            item.organization_id,
            organization_state(item.organization_state)
        )?;
        if let Some(display_name) = &item.organization_display_name {
            writeln!(stdout, "organization name: {display_name}")?;
        }
        if let Some(slug) = &item.organization_slug {
            writeln!(stdout, "organization slug: {slug}")?;
        }
        writeln!(stdout, "created: {}", item.created_at)?;
        writeln!(stdout, "updated: {}", item.updated_at)?;
        if let Some(terminal_at) = &item.terminal_at {
            writeln!(stdout, "terminal: {terminal_at}")?;
        }
        writeln!(stdout)?;
    }
    if let Some(next_cursor) = next_cursor {
        writeln!(stdout, "next cursor: {next_cursor}")?;
    }
    writeln!(stdout, "deployment: {deployment}")?;
    Ok(())
}

fn write_members_human(
    deployment: &str,
    items: &[OrganizationMembershipDirectoryEntry],
    next_cursor: Option<&str>,
) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✓ Organization members listed.\n")?;
    for item in items {
        write!(
            stdout,
            "  Membership: {}  Principal: {}  Type: {}  Role: {}",
            item.id,
            item.principal_id,
            principal_type(item.principal_type),
            membership_role(item.role)
        )?;
        if let Some(display_name) = &item.display_name {
            write!(stdout, "  Name: {display_name}")?;
        }
        writeln!(stdout)?;
    }
    if !items.is_empty() {
        writeln!(stdout)?;
    }
    if let Some(next_cursor) = next_cursor {
        writeln!(stdout, "  Next cursor: {next_cursor}")?;
    }
    writeln!(stdout, "  Deployment: {deployment}")?;
    Ok(())
}

fn write_current_membership_failure(
    deployment: &str,
    failure: &CommonOrganizationFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (outcome, category, human, outcome_class) = match failure {
        CommonOrganizationFailure::Unauthenticated => (
            "unauthenticated",
            None,
            "error: organization membership history requires sign-in\n\nSign in first:\n  scherzo-cloud auth login".to_owned(),
            OutcomeClass::Unauthenticated,
        ),
        CommonOrganizationFailure::Forbidden => (
            "forbidden",
            None,
            "error: organization membership history unavailable for this account\n\nAsk the deployment operator to restore account access.".to_owned(),
            OutcomeClass::Forbidden,
        ),
        CommonOrganizationFailure::InvalidInput => (
            "invalid_input",
            None,
            format!(
                "error: organization membership cursor rejected by {deployment}\n\nRestart the listing without --cursor."
            ),
            OutcomeClass::GeneralFailure,
        ),
        CommonOrganizationFailure::Unreachable(category) => (
            "unreachable",
            Some(category.as_str()),
            format!(
                "error: contact Scherzo Cloud API at {deployment}: {}\n\nCheck network access to the deployment and try again.",
                category.as_str()
            ),
            super::super::unreachable_outcome_class(*category),
        ),
    };
    if json {
        write_json(&super::super::ApiFailureResult::new(
            deployment, outcome, category,
        ))?;
    } else {
        writeln!(io::stderr().lock(), "{human}")?;
    }
    Ok(outcome_class.exit_code())
}

fn write_failure(
    deployment: &str,
    outcome: &'static str,
    category: Option<&'static str>,
    retry_after: Option<u64>,
    human: &str,
    outcome_class: OutcomeClass,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&super::super::ApiFailureResult::with_retry_after(
            deployment,
            outcome,
            category,
            retry_after,
        ))?;
    } else {
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        writeln!(stdout, "{human}")?;
    }
    Ok(outcome_class.exit_code())
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::write_pretty_json(value).context("write JSON organization result")
}

const fn organization_state(state: OrganizationState) -> &'static str {
    match state {
        OrganizationState::Active => "active",
        OrganizationState::Suspended => "suspended",
        OrganizationState::DeletionPending => "deletion_pending",
        OrganizationState::Deleted => "deleted",
    }
}

const fn principal_type(principal_type: PrincipalType) -> &'static str {
    match principal_type {
        PrincipalType::Human => "human",
        PrincipalType::Service => "service",
    }
}

const fn membership_role(role: MembershipRole) -> &'static str {
    match role {
        MembershipRole::Owner => "owner",
        MembershipRole::Member => "member",
    }
}

const fn membership_state(state: MembershipState) -> &'static str {
    match state {
        MembershipState::Active => "active",
        MembershipState::Suspended => "suspended",
        MembershipState::Ended => "ended",
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OrganizationResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization: &'a Organization,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListResult<'a, T> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    items: &'a [T],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
}
