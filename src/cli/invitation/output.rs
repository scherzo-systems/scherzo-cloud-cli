use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use crate::api::{
    AcceptInvitationOutcome, AcceptedInvitationMembership, CommonOrganizationFailure, Invitation,
    InvitationDeliveryState, InvitationInboxEntry, InvitationState, InvitationTargetKind,
    InvitationTerminationOutcome, IssueInvitationOutcome, ListInvitationInboxOutcome,
    ListOrganizationInvitationsOutcome, PreviewInvitationOutcome,
};
use crate::exit_code::{ExitCode, OutcomeClass};

use super::CapabilityError;

pub(super) fn write_issue(
    deployment: &str,
    outcome: &IssueInvitationOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        IssueInvitationOutcome::Issued(invitation) => {
            write_invitation_result(deployment, "issued", invitation, json)?;
            Ok(ExitCode::Success)
        }
        IssueInvitationOutcome::Common(failure) => write_common(deployment, failure, json),
        IssueInvitationOutcome::NotFound => write_failure(
            deployment,
            "not_found",
            None,
            None,
            "error: organization not found or unavailable\n\nCheck the organization reference and your access, then try again.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        IssueInvitationOutcome::RecipientUnavailable => write_failure(
            deployment,
            "recipient_unavailable",
            None,
            None,
            "error: invitation recipient unavailable\n\nCheck the exact active principal or email address, then try again.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        IssueInvitationOutcome::OutstandingLimitReached => write_failure(
            deployment,
            "outstanding_invitation_limit_reached",
            None,
            None,
            "error: organization outstanding invitation limit reached\n\nRevoke an outstanding invitation before issuing another.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        IssueInvitationOutcome::RateLimited { retry_after } => write_failure(
            deployment,
            "rate_limited",
            None,
            Some(*retry_after),
            &format!(
                "error: invitation issuance rate limited\n\nTry again in {retry_after} seconds."
            ),
            OutcomeClass::RateLimited,
            json,
        ),
        IssueInvitationOutcome::IdempotencyConflict => write_idempotency_conflict(deployment, json),
    }
}

pub(super) fn write_organization_list(
    deployment: &str,
    outcome: &ListOrganizationInvitationsOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        ListOrganizationInvitationsOutcome::Listed(page) => {
            if json {
                write_invitation_page_json(deployment, &page.items, page.next_cursor.as_deref())?;
            } else {
                write_organization_list_human(
                    deployment,
                    &page.items,
                    page.next_cursor.as_deref(),
                )?;
            }
            Ok(ExitCode::Success)
        }
        ListOrganizationInvitationsOutcome::Common(failure) => {
            write_common(deployment, failure, json)
        }
        ListOrganizationInvitationsOutcome::NotFound => write_failure(
            deployment,
            "not_found",
            None,
            None,
            "error: organization not found or unavailable\n\nCheck the organization reference and your access, then try again.",
            OutcomeClass::GeneralFailure,
            json,
        ),
    }
}

pub(super) fn write_inbox(
    deployment: &str,
    outcome: &ListInvitationInboxOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        ListInvitationInboxOutcome::Listed(page) => {
            if json {
                write_invitation_page_json(deployment, &page.items, page.next_cursor.as_deref())?;
            } else {
                write_inbox_human(deployment, &page.items, page.next_cursor.as_deref())?;
            }
            Ok(ExitCode::Success)
        }
        ListInvitationInboxOutcome::Common(failure) => write_common(deployment, failure, json),
    }
}

pub(super) fn write_preview(
    deployment: &str,
    outcome: &PreviewInvitationOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        PreviewInvitationOutcome::Previewed(preview) => {
            if json {
                super::super::write_pretty_json(&PreviewResult {
                    schema_version: 1,
                    deployment,
                    outcome: "previewed",
                    invitation: preview,
                })
                .context("write JSON invitation result")?;
            } else {
                let mut stdout = io::stdout().lock();
                writeln!(stdout, "✓ Invitation previewed.\n")?;
                writeln!(stdout, "invitation: {}", preview.id)?;
                writeln!(stdout, "organization: {}", preview.organization_id)?;
                writeln!(
                    stdout,
                    "organization name: {}",
                    preview.organization_display_name
                )?;
                writeln!(stdout, "organization slug: {}", preview.organization_slug)?;
                writeln!(stdout, "target: {}", target_kind(preview.target_kind))?;
                writeln!(stdout, "expires: {}", preview.expires_at)?;
                writeln!(stdout, "deployment: {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        PreviewInvitationOutcome::Common(failure) => write_common(deployment, failure, json),
        PreviewInvitationOutcome::Unavailable => write_unavailable(deployment, json),
    }
}

pub(super) fn write_accept(
    deployment: &str,
    outcome: &AcceptInvitationOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        AcceptInvitationOutcome::Accepted(membership) => {
            if json {
                super::super::write_pretty_json(&AcceptanceResult {
                    schema_version: 1,
                    deployment,
                    outcome: "accepted",
                    membership,
                })
                .context("write JSON invitation result")?;
            } else {
                write_membership_human(deployment, membership)?;
            }
            Ok(ExitCode::Success)
        }
        AcceptInvitationOutcome::Common(failure) => write_common(deployment, failure, json),
        AcceptInvitationOutcome::Unavailable => write_unavailable(deployment, json),
        AcceptInvitationOutcome::MembershipLimitReached => write_failure(
            deployment,
            "membership_limit_reached",
            None,
            None,
            "error: organization membership limit reached\n\nAsk an organization owner to make capacity before accepting again.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        AcceptInvitationOutcome::IdempotencyConflict => {
            write_idempotency_conflict(deployment, json)
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum TerminationAction<'a> {
    Revoke { organization: &'a str },
    Decline,
}

pub(super) fn write_termination(
    deployment: &str,
    invitation_id: &str,
    outcome: &InvitationTerminationOutcome,
    action: TerminationAction<'_>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        InvitationTerminationOutcome::Completed => {
            let (outcome, heading, organization) = match action {
                TerminationAction::Revoke { organization } => {
                    ("revoked", "✓ Invitation revoked.", Some(organization))
                }
                TerminationAction::Decline => ("declined", "✓ Invitation declined.", None),
            };
            if json {
                super::super::write_pretty_json(&TerminationResult {
                    schema_version: 1,
                    deployment,
                    outcome,
                    organization_ref: organization,
                    invitation_id,
                })
                .context("write JSON invitation result")?;
            } else {
                let mut stdout = io::stdout().lock();
                writeln!(stdout, "{heading}\n")?;
                writeln!(stdout, "invitation: {invitation_id}")?;
                if let Some(organization) = organization {
                    writeln!(stdout, "organization: {organization}")?;
                }
                writeln!(stdout, "deployment: {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        InvitationTerminationOutcome::Common(failure) => write_common(deployment, failure, json),
        InvitationTerminationOutcome::NotFound => write_failure(
            deployment,
            "not_found",
            None,
            None,
            "error: organization or invitation not found or unavailable\n\nList organization invitation history and check the identifiers.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        InvitationTerminationOutcome::Unavailable => write_unavailable(deployment, json),
        InvitationTerminationOutcome::IdempotencyConflict => {
            write_idempotency_conflict(deployment, json)
        }
    }
}

pub(super) fn write_capability_error(
    deployment: &str,
    error: &CapabilityError,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        super::super::write_cloud_failure_json(deployment, error.outcome(), None, None)
            .context("write JSON invitation failure")?;
    } else {
        writeln!(
            io::stderr().lock(),
            "error: {error}\n\nUse a current capability for this invitation from a mode-0600 file, or pass it through --capability-file -."
        )?;
    }
    Ok(ExitCode::GeneralFailure)
}

// Invitation authorization remedies distinguish recipients from organization owners, so this
// mapping stays beside invitation output instead of reusing membership-specific prose.
// jscpd:ignore-start
fn write_common(
    deployment: &str,
    failure: &CommonOrganizationFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (outcome, category, message, class) = match failure {
        CommonOrganizationFailure::Unauthenticated => (
            "unauthenticated",
            None,
            "error: invitation management requires sign-in\n\nSign in first:\n  scherzo-cloud auth login".to_owned(),
            OutcomeClass::Unauthenticated,
        ),
        CommonOrganizationFailure::Forbidden => (
            "forbidden",
            None,
            "error: invitation operation not permitted\n\nUse the exact invited account, or an active organization owner where required.".to_owned(),
            OutcomeClass::Forbidden,
        ),
        CommonOrganizationFailure::InvalidInput => (
            "invalid_input",
            None,
            format!(
                "error: invitation input rejected by {deployment}\n\nCheck the organization, invitation, recipient, capability source, and cursor, then try again."
            ),
            OutcomeClass::GeneralFailure,
        ),
        CommonOrganizationFailure::Unreachable(category) => (
            "unreachable",
            Some(category.as_str()),
            format!(
                "error: contact invitation API at {deployment}: {}\n\nCheck network access to the deployment and try again.",
                category.as_str()
            ),
            super::super::unreachable_outcome_class(*category),
        ),
    };
    write_failure(deployment, outcome, category, None, &message, class, json)
}
// jscpd:ignore-end

fn write_unavailable(deployment: &str, json: bool) -> anyhow::Result<ExitCode> {
    write_failure(
        deployment,
        "invitation_unavailable",
        None,
        None,
        "error: invitation unavailable\n\nThe invitation may be expired, revoked, already used, inaccessible, or paired with the wrong capability. Ask an organization owner for a current invitation.",
        OutcomeClass::GeneralFailure,
        json,
    )
}

fn write_idempotency_conflict(deployment: &str, json: bool) -> anyhow::Result<ExitCode> {
    write_failure(
        deployment,
        "idempotency_conflict",
        None,
        None,
        "error: invitation request identity conflicted with another request\n\nRun the command again to use a new request identity.",
        OutcomeClass::GeneralFailure,
        json,
    )
}

fn write_failure(
    deployment: &str,
    outcome: &'static str,
    category: Option<&'static str>,
    retry_after: Option<u64>,
    human: &str,
    class: OutcomeClass,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        super::super::write_cloud_failure_json(deployment, outcome, category, retry_after)
            .context("write JSON invitation failure")?;
    } else {
        writeln!(io::stderr().lock(), "{human}")?;
    }
    Ok(class.exit_code())
}

fn write_invitation_result(
    deployment: &str,
    outcome: &'static str,
    invitation: &Invitation,
    json: bool,
) -> anyhow::Result<()> {
    if json {
        super::super::write_pretty_json(&InvitationResult {
            schema_version: 1,
            deployment,
            outcome,
            invitation,
        })
        .context("write JSON invitation result")
    } else {
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "✓ Invitation issued.\n")?;
        write_invitation_fields(&mut stdout, invitation)?;
        writeln!(stdout, "deployment: {deployment}").context("write invitation deployment")
    }
}

fn write_organization_list_human(
    deployment: &str,
    items: &[Invitation],
    next_cursor: Option<&str>,
) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Organization invitation history listed.\n")?;
    for invitation in items {
        write_invitation_fields(&mut stdout, invitation)?;
        writeln!(stdout)?;
    }
    super::super::write_page_footer(&mut stdout, deployment, next_cursor)?;
    Ok(())
}

fn write_invitation_fields(output: &mut impl Write, invitation: &Invitation) -> io::Result<()> {
    writeln!(
        output,
        "invitation: {} · state: {} · target: {}",
        invitation.id,
        invitation_state(invitation.state),
        target_kind(invitation.target_kind)
    )?;
    writeln!(output, "organization: {}", invitation.organization_id)?;
    writeln!(output, "issuer: {}", invitation.issuer_principal_id)?;
    if let Some(principal_id) = &invitation.target_principal_id {
        writeln!(output, "target principal: {principal_id}")?;
    }
    if let Some(email) = &invitation.target_email {
        writeln!(output, "target email: {email}")?;
    }
    if let Some(delivery_state) = invitation.delivery_state {
        writeln!(output, "delivery: {}", delivery_state_text(delivery_state))?;
    }
    writeln!(output, "issued: {}", invitation.issued_at)?;
    writeln!(output, "expires: {}", invitation.expires_at)?;
    if let Some(terminal_at) = &invitation.terminal_at {
        writeln!(output, "terminal: {terminal_at}")?;
    }
    if let Some(replaced) = &invitation.replaced_invitation_id {
        writeln!(output, "replaced invitation: {replaced}")?;
    }
    if let Some(replacement) = &invitation.replacement_invitation_id {
        writeln!(output, "replacement invitation: {replacement}")?;
    }
    Ok(())
}

fn write_inbox_human(
    deployment: &str,
    items: &[InvitationInboxEntry],
    next_cursor: Option<&str>,
) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Invitation inbox listed.\n")?;
    for invitation in items {
        writeln!(stdout, "invitation: {}", invitation.id)?;
        writeln!(
            stdout,
            "organization: {} · slug: {}",
            invitation.organization_id, invitation.organization_slug
        )?;
        writeln!(
            stdout,
            "organization name: {}",
            invitation.organization_display_name
        )?;
        writeln!(stdout, "issuer: {}", invitation.issuer_principal_id)?;
        writeln!(stdout, "expires: {}\n", invitation.expires_at)?;
    }
    super::super::write_page_footer(&mut stdout, deployment, next_cursor)?;
    Ok(())
}

fn write_membership_human(
    deployment: &str,
    membership: &AcceptedInvitationMembership,
) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Invitation accepted.\n")?;
    writeln!(stdout, "membership: {}", membership.id)?;
    writeln!(stdout, "organization: {}", membership.organization_id)?;
    writeln!(stdout, "principal: {}", membership.principal_id)?;
    writeln!(
        stdout,
        "role: {}",
        super::super::membership_role(membership.role)
    )?;
    writeln!(
        stdout,
        "state: {}",
        super::super::membership_state(membership.state)
    )?;
    writeln!(stdout, "created: {}", membership.created_at)?;
    writeln!(stdout, "updated: {}", membership.updated_at)?;
    writeln!(stdout, "deployment: {deployment}")?;
    Ok(())
}

// Both invitation page projections intentionally share the root JSON list envelope while
// retaining invitation-specific output context at this boundary.
// jscpd:ignore-start
fn write_invitation_page_json(
    deployment: &str,
    items: &[impl Serialize],
    next_cursor: Option<&str>,
) -> anyhow::Result<()> {
    super::super::write_cloud_list_json(deployment, items, next_cursor)
        .context("write JSON invitation list result")
}
// jscpd:ignore-end

const fn target_kind(kind: InvitationTargetKind) -> &'static str {
    match kind {
        InvitationTargetKind::Principal => "principal",
        InvitationTargetKind::Email => "email",
    }
}

const fn invitation_state(state: InvitationState) -> &'static str {
    match state {
        InvitationState::Outstanding => "outstanding",
        InvitationState::Accepted => "accepted",
        InvitationState::Declined => "declined",
        InvitationState::Revoked => "revoked",
        InvitationState::Expired => "expired",
    }
}

const fn delivery_state_text(state: InvitationDeliveryState) -> &'static str {
    match state {
        InvitationDeliveryState::Pending => "pending",
        InvitationDeliveryState::Leased => "leased",
        InvitationDeliveryState::Sent => "sent",
        InvitationDeliveryState::Failed => "failed",
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InvitationResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    invitation: &'a Invitation,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PreviewResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    invitation: &'a crate::api::InvitationPreview,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AcceptanceResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    membership: &'a AcceptedInvitationMembership,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TerminationResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization_ref: Option<&'a str>,
    invitation_id: &'a str,
}
