use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use crate::api::{
    CommonOrganizationFailure, CreateOrganizationOutcome, GetOrganizationOutcome,
    ListOrganizationMembershipsOutcome, MembershipRole, Organization,
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

pub(super) fn write_members_list(
    deployment: &str,
    outcome: &ListOrganizationMembershipsOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        ListOrganizationMembershipsOutcome::Listed(page) => {
            if json {
                write_json(&MembershipListResult {
                    schema_version: 1,
                    deployment,
                    outcome: "listed",
                    items: &page.items,
                    next_cursor: page.next_cursor.as_deref(),
                })?;
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
        write_json(&FailureResult {
            schema_version: 1,
            deployment,
            outcome,
            category,
            retry_after,
        })?;
    } else {
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        writeln!(stdout, "{human}")?;
    }
    Ok(outcome_class.exit_code())
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, value)
        .context("serialize JSON organization result")?;
    writeln!(stdout).context("write organization result")
}

const fn organization_state(state: OrganizationState) -> &'static str {
    match state {
        OrganizationState::Active => "active",
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
struct MembershipListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    items: &'a [OrganizationMembershipDirectoryEntry],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FailureResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after: Option<u64>,
}
