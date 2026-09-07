use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use crate::api::{
    AuditActor, AuditProjectionWarning, AuditProjectionWarningReason, CommonOrganizationFailure,
    CreateOrganizationOutcome, CurrentPrincipalMembership, GetOrganizationOutcome,
    ListCurrentPrincipalMembershipsOutcome, ListOrganizationAuditRecordsOutcome,
    ListOrganizationMembershipHistoryOutcome, ListOrganizationMembershipsOutcome,
    MembershipTerminationOutcome, Organization, OrganizationAuditRecord,
    OrganizationAuditSubjectKind, OrganizationMembershipDirectoryEntry,
    OrganizationMembershipHistoryEntry, OrganizationState, PrincipalType,
    UpdateOrganizationMembershipOutcome, UpdateOrganizationOutcome,
};
use crate::exit_code::{ExitCode, OutcomeClass};

use super::super::{membership_role, membership_state, write_page_footer};

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

pub(super) fn write_members_history(
    deployment: &str,
    outcome: &ListOrganizationMembershipHistoryOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        ListOrganizationMembershipHistoryOutcome::Listed(page) => {
            if json {
                write_list_json(deployment, &page.items, page.next_cursor.as_deref())?;
            } else {
                write_membership_history_human(
                    "✓ Organization membership history listed.",
                    deployment,
                    &page.items,
                    page.next_cursor.as_deref(),
                )?;
            }
            Ok(ExitCode::Success)
        }
        ListOrganizationMembershipHistoryOutcome::Common(common) => {
            write_membership_common_failure(deployment, common, json)
        }
        ListOrganizationMembershipHistoryOutcome::NotFound => {
            write_membership_not_found(deployment, json)
        }
    }
}

pub(super) fn write_audit_list(
    deployment: &str,
    outcome: &ListOrganizationAuditRecordsOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        ListOrganizationAuditRecordsOutcome::Listed(page) => {
            if json {
                write_json(&AuditListResult {
                    schema_version: 1,
                    deployment,
                    outcome: "listed",
                    items: &page.items,
                    next_cursor: page.next_cursor.as_deref(),
                    warnings: page.warnings.as_deref(),
                })?;
            } else {
                write_audit_records_human(
                    deployment,
                    &page.items,
                    page.next_cursor.as_deref(),
                    page.warnings.as_deref(),
                )?;
            }
            Ok(ExitCode::Success)
        }
        ListOrganizationAuditRecordsOutcome::Common(common) => {
            write_audit_common_failure(deployment, common, json)
        }
        ListOrganizationAuditRecordsOutcome::NotFound => write_organization_operation_failure(
            deployment,
            "not_found",
            None,
            "error: organization audit records not found or unavailable\n\nCheck the organization reference and your access, then try again.",
            OutcomeClass::GeneralFailure,
            json,
        ),
    }
}

pub(super) fn write_members_update(
    deployment: &str,
    outcome: &UpdateOrganizationMembershipOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        UpdateOrganizationMembershipOutcome::Updated(membership) => {
            if json {
                write_json(&MembershipResult {
                    schema_version: 1,
                    deployment,
                    outcome: "updated",
                    membership,
                })?;
            } else {
                write_membership_history_human(
                    "✓ Organization member updated.",
                    deployment,
                    std::slice::from_ref(membership),
                    None,
                )?;
            }
            Ok(ExitCode::Success)
        }
        UpdateOrganizationMembershipOutcome::Common(common) => {
            write_membership_common_failure(deployment, common, json)
        }
        UpdateOrganizationMembershipOutcome::NotFound => {
            write_membership_not_found(deployment, json)
        }
        UpdateOrganizationMembershipOutcome::TransitionUnavailable => {
            write_membership_conflict(deployment, MembershipConflict::TransitionUnavailable, json)
        }
        UpdateOrganizationMembershipOutcome::HumanOwnerRequired => {
            write_membership_conflict(deployment, MembershipConflict::HumanOwnerRequired, json)
        }
        UpdateOrganizationMembershipOutcome::IdempotencyConflict => {
            write_membership_conflict(deployment, MembershipConflict::IdempotencyConflict, json)
        }
    }
}

pub(super) fn write_member_removal(
    deployment: &str,
    organization: &str,
    membership_id: &str,
    outcome: &MembershipTerminationOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_membership_termination(
        deployment,
        organization,
        Some(membership_id),
        "removed",
        "✓ Organization member removed.",
        outcome,
        json,
    )
}

pub(super) fn write_leave(
    deployment: &str,
    organization: &str,
    outcome: &MembershipTerminationOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_membership_termination(
        deployment,
        organization,
        None,
        "left",
        "✓ Organization membership ended.",
        outcome,
        json,
    )
}

fn write_audit_records_human(
    deployment: &str,
    items: &[OrganizationAuditRecord],
    next_cursor: Option<&str>,
    warnings: Option<&[AuditProjectionWarning]>,
) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Organization audit records listed.\n")?;
    for item in items {
        match item {
            OrganizationAuditRecord::DetailsAvailable {
                id,
                occurred_at,
                retention,
                actor,
                delegating_principal_id,
                action,
                subject,
                changes,
            } => {
                writeln!(stdout, "record: {id}")?;
                writeln!(stdout, "time: {occurred_at}")?;
                writeln!(stdout, "details: available")?;
                write_audit_actor(&mut stdout, actor)?;
                if let Some(principal_id) = delegating_principal_id {
                    writeln!(stdout, "delegating principal: {principal_id}")?;
                }
                writeln!(stdout, "action: {action}")?;
                writeln!(
                    stdout,
                    "target: {} {}",
                    audit_subject_kind(subject.kind),
                    subject.id
                )?;
                writeln!(stdout, "changes: {}", changes.len())?;
                writeln!(
                    stdout,
                    "retention: {} · retain until: {}\n",
                    retention.identifier, retention.retain_until
                )?;
            }
            OrganizationAuditRecord::DetailsUnavailable {
                id,
                occurred_at,
                retention,
            } => {
                writeln!(stdout, "record: {id}")?;
                writeln!(stdout, "time: {occurred_at}")?;
                writeln!(stdout, "details: unavailable")?;
                writeln!(
                    stdout,
                    "retention: {} · retain until: {}\n",
                    retention.identifier, retention.retain_until
                )?;
            }
        }
    }
    if let Some(warnings) = warnings {
        for warning in warnings {
            writeln!(
                stdout,
                "warning: {} · reason: {}",
                warning.record_id,
                audit_warning_reason(warning.reason)
            )?;
        }
        if !warnings.is_empty() {
            writeln!(stdout)?;
        }
    }
    write_page_footer(&mut stdout, deployment, next_cursor)?;
    Ok(())
}

fn write_audit_actor(output: &mut impl Write, actor: &AuditActor) -> io::Result<()> {
    match actor {
        AuditActor::Principal { principal_id } => {
            writeln!(output, "actor: principal {principal_id}")
        }
        AuditActor::Runner { runner_id } => writeln!(output, "actor: runner {runner_id}"),
        AuditActor::System => writeln!(output, "actor: system"),
    }
}

const fn audit_subject_kind(kind: OrganizationAuditSubjectKind) -> &'static str {
    match kind {
        OrganizationAuditSubjectKind::Organization => "organization",
        OrganizationAuditSubjectKind::ArtifactSet => "artifact_set",
        OrganizationAuditSubjectKind::RunInputSet => "run_input_set",
        OrganizationAuditSubjectKind::Membership => "membership",
        OrganizationAuditSubjectKind::Invitation => "invitation",
        OrganizationAuditSubjectKind::RunnerPool => "runner_pool",
        OrganizationAuditSubjectKind::RunnerRegistration => "runner_registration",
        OrganizationAuditSubjectKind::RunnerActivation => "runner_activation",
        OrganizationAuditSubjectKind::RunnerCredential => "runner_credential",
        OrganizationAuditSubjectKind::GithubInstallation => "github_installation",
        OrganizationAuditSubjectKind::Project => "project",
        OrganizationAuditSubjectKind::RepositoryConnection => "repository_connection",
        OrganizationAuditSubjectKind::Assignment => "assignment",
    }
}

const fn audit_warning_reason(reason: AuditProjectionWarningReason) -> &'static str {
    match reason {
        AuditProjectionWarningReason::UnknownAction => "unknown_action",
        AuditProjectionWarningReason::MalformedRecord => "malformed_record",
    }
}

struct CommonFailurePresentation {
    unauthenticated: &'static str,
    forbidden: &'static str,
    invalid_input_subject: &'static str,
    invalid_input_remedy: &'static str,
}

const AUDIT_FAILURE_PRESENTATION: CommonFailurePresentation = CommonFailurePresentation {
    unauthenticated: "error: organization audit records require sign-in\n\nSign in first:\n  scherzo-cloud auth login",
    forbidden: "error: organization audit records unavailable for this account\n\nUse an active organization owner account.",
    invalid_input_subject: "organization audit request",
    invalid_input_remedy: "Check the organization reference, limit, and cursor, then try again.",
};

const MEMBERSHIP_FAILURE_PRESENTATION: CommonFailurePresentation = CommonFailurePresentation {
    unauthenticated: "error: organization membership management requires sign-in\n\nSign in first:\n  scherzo-cloud auth login",
    forbidden: "error: organization membership operation not permitted\n\nUse an active organization owner account.",
    invalid_input_subject: "organization membership input",
    invalid_input_remedy: "Check the organization reference, membership ID, and cursor, then try again.",
};

fn write_audit_common_failure(
    deployment: &str,
    failure: &CommonOrganizationFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_common_organization_operation_failure(
        deployment,
        failure,
        &AUDIT_FAILURE_PRESENTATION,
        json,
    )
}

fn write_membership_termination(
    deployment: &str,
    organization: &str,
    membership_id: Option<&str>,
    success_outcome: &'static str,
    heading: &'static str,
    outcome: &MembershipTerminationOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        MembershipTerminationOutcome::Ended => {
            if json {
                write_json(&MembershipTerminationResult {
                    schema_version: 1,
                    deployment,
                    outcome: success_outcome,
                    organization,
                    membership_id,
                })?;
            } else {
                let mut stdout = io::stdout().lock();
                writeln!(stdout, "{heading}\n")?;
                writeln!(stdout, "organization: {organization}")?;
                if let Some(membership_id) = membership_id {
                    writeln!(stdout, "membership: {membership_id}")?;
                }
                writeln!(stdout, "deployment: {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        MembershipTerminationOutcome::Common(common) => {
            write_membership_common_failure(deployment, common, json)
        }
        MembershipTerminationOutcome::NotFound => write_membership_not_found(deployment, json),
        MembershipTerminationOutcome::TransitionUnavailable => {
            write_membership_conflict(deployment, MembershipConflict::TransitionUnavailable, json)
        }
        MembershipTerminationOutcome::HumanOwnerRequired => {
            write_membership_conflict(deployment, MembershipConflict::HumanOwnerRequired, json)
        }
        MembershipTerminationOutcome::IdempotencyConflict => {
            write_membership_conflict(deployment, MembershipConflict::IdempotencyConflict, json)
        }
    }
}

fn write_membership_common_failure(
    deployment: &str,
    failure: &CommonOrganizationFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_common_organization_operation_failure(
        deployment,
        failure,
        &MEMBERSHIP_FAILURE_PRESENTATION,
        json,
    )
}

fn write_common_organization_operation_failure(
    deployment: &str,
    failure: &CommonOrganizationFailure,
    presentation: &CommonFailurePresentation,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (outcome, category, message, class) = match failure {
        CommonOrganizationFailure::Unauthenticated => (
            "unauthenticated",
            None,
            presentation.unauthenticated.to_owned(),
            OutcomeClass::Unauthenticated,
        ),
        CommonOrganizationFailure::Forbidden => (
            "forbidden",
            None,
            presentation.forbidden.to_owned(),
            OutcomeClass::Forbidden,
        ),
        CommonOrganizationFailure::InvalidInput => (
            "invalid_input",
            None,
            format!(
                "error: {} rejected by {deployment}\n\n{}",
                presentation.invalid_input_subject, presentation.invalid_input_remedy
            ),
            OutcomeClass::GeneralFailure,
        ),
        CommonOrganizationFailure::Unreachable(category) => {
            organization_operation_unreachable(deployment, *category)
        }
    };
    write_organization_operation_failure(deployment, outcome, category, &message, class, json)
}

fn organization_operation_unreachable(
    deployment: &str,
    category: crate::api::UnreachableCategory,
) -> (&'static str, Option<&'static str>, String, OutcomeClass) {
    (
        "unreachable",
        Some(category.as_str()),
        format!(
            "error: contact Scherzo Cloud API at {deployment}: {}\n\nCheck network access to the deployment and try again.",
            category.as_str()
        ),
        super::super::unreachable_outcome_class(category),
    )
}

fn write_membership_not_found(deployment: &str, json: bool) -> anyhow::Result<ExitCode> {
    write_organization_operation_failure(
        deployment,
        "not_found",
        None,
        "error: organization or membership not found or unavailable\n\nCheck the organization reference and your access, then try again.",
        OutcomeClass::GeneralFailure,
        json,
    )
}

#[derive(Clone, Copy)]
enum MembershipConflict {
    TransitionUnavailable,
    HumanOwnerRequired,
    IdempotencyConflict,
}

fn write_membership_conflict(
    deployment: &str,
    conflict: MembershipConflict,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (outcome, message) = match conflict {
        MembershipConflict::TransitionUnavailable => (
            "transition_unavailable",
            "error: membership transition unavailable\n\nList membership history and choose an active or suspended membership.",
        ),
        MembershipConflict::HumanOwnerRequired => (
            "human_owner_required",
            "error: organization must retain an active human owner\n\nMake another active human member an owner first.",
        ),
        MembershipConflict::IdempotencyConflict => (
            "idempotency_conflict",
            "error: organization membership request identity conflicted with another request\n\nRun the command again to use a new request identity.",
        ),
    };
    write_organization_operation_failure(
        deployment,
        outcome,
        None,
        message,
        OutcomeClass::GeneralFailure,
        json,
    )
}

fn write_organization_operation_failure(
    deployment: &str,
    outcome: &'static str,
    category: Option<&'static str>,
    message: &str,
    class: OutcomeClass,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_cloud_failure_json(deployment, outcome, category, None)?;
    } else {
        writeln!(io::stderr().lock(), "{message}")?;
    }
    Ok(class.exit_code())
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
    super::super::write_cloud_list_json(deployment, items, next_cursor)
        .context("write JSON organization list result")
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
        write_membership_times(&mut stdout, item)?;
    }
    write_page_footer(&mut stdout, deployment, next_cursor)?;
    Ok(())
}

trait MembershipTimes {
    fn created_at(&self) -> &str;
    fn updated_at(&self) -> &str;
    fn terminal_at(&self) -> Option<&str>;
}

macro_rules! impl_membership_times {
    ($membership:ty) => {
        impl MembershipTimes for $membership {
            fn created_at(&self) -> &str {
                &self.created_at
            }

            fn updated_at(&self) -> &str {
                &self.updated_at
            }

            fn terminal_at(&self) -> Option<&str> {
                self.terminal_at.as_deref()
            }
        }
    };
}

impl_membership_times!(CurrentPrincipalMembership);
impl_membership_times!(OrganizationMembershipHistoryEntry);

fn write_membership_times(
    output: &mut impl Write,
    membership: &impl MembershipTimes,
) -> io::Result<()> {
    writeln!(output, "created: {}", membership.created_at())?;
    writeln!(output, "updated: {}", membership.updated_at())?;
    if let Some(terminal_at) = membership.terminal_at() {
        writeln!(output, "terminal: {terminal_at}")?;
    }
    writeln!(output)
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

fn write_membership_history_human(
    heading: &str,
    deployment: &str,
    items: &[OrganizationMembershipHistoryEntry],
    next_cursor: Option<&str>,
) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{heading}\n")?;
    for item in items {
        writeln!(
            stdout,
            "membership: {} · role: {} · state: {}",
            item.id,
            membership_role(item.role),
            membership_state(item.state)
        )?;
        writeln!(
            stdout,
            "principal: {} · type: {}",
            item.principal_id,
            principal_type(item.principal_type)
        )?;
        if let Some(display_name) = &item.display_name {
            writeln!(stdout, "name: {display_name}")?;
        }
        writeln!(stdout, "organization: {}", item.organization_id)?;
        write_membership_times(&mut stdout, item)?;
    }
    write_page_footer(&mut stdout, deployment, next_cursor)?;
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
        CommonOrganizationFailure::Unreachable(category) => {
            organization_operation_unreachable(deployment, *category)
        }
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

fn write_cloud_failure_json(
    deployment: &str,
    outcome: &'static str,
    category: Option<&'static str>,
    retry_after: Option<u64>,
) -> anyhow::Result<()> {
    write_json(&super::super::ApiFailureResult::with_retry_after(
        deployment,
        outcome,
        category,
        retry_after,
    ))
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuditListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    items: &'a [OrganizationAuditRecord],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    warnings: Option<&'a [AuditProjectionWarning]>,
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
struct MembershipResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    membership: &'a OrganizationMembershipHistoryEntry,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MembershipTerminationResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    membership_id: Option<&'a str>,
}
