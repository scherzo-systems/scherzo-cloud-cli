use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;
use time::OffsetDateTime;

use crate::api::{
    CommonIdentityFailure, LinkIdentityOutcome, ListIdentitiesOutcome, OidcIdentity,
    RemoveIdentityOutcome, UnreachableCategory,
};
use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::deployment::Deployment;
use crate::human_auth::device_authorization::DeviceAuthorization;
use crate::human_auth::session::LocalCredentialState;

pub(super) fn write_list(
    deployment: &str,
    outcome: &ListIdentitiesOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        // Identity and membership collections intentionally keep separate JSON contracts and
        // human renderers because their item vocabularies evolve independently.
        // jscpd:ignore-start
        ListIdentitiesOutcome::Listed(page) => {
            if json {
                write_json(&ListResult {
                    schema_version: 1,
                    deployment,
                    outcome: "listed",
                    items: &page.items,
                    next_cursor: page.next_cursor.as_deref(),
                })?;
            } else {
                write_identity_list_human(deployment, &page.items, page.next_cursor.as_deref())?;
            }
            Ok(ExitCode::Success)
        }
        // jscpd:ignore-end
        ListIdentitiesOutcome::Common(common) => write_common(deployment, common, json),
    }
}

pub(super) fn write_remove(
    deployment: &str,
    identity_id: &str,
    outcome: &RemoveIdentityOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        RemoveIdentityOutcome::Removed => {
            if json {
                write_json(&RemoveResult {
                    schema_version: 1,
                    deployment,
                    outcome: "removed",
                    identity_id,
                    local_session_identity: "unchanged",
                })?;
            } else {
                let stdout = io::stdout();
                let mut stdout = stdout.lock();
                writeln!(stdout, "✓ Linked identity removed.\n")?;
                writeln!(stdout, "identity: {identity_id}")?;
                writeln!(stdout, "local session identity: unchanged")?;
                writeln!(stdout, "deployment: {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        RemoveIdentityOutcome::Common(common) => write_common(deployment, common, json),
        RemoveIdentityOutcome::ReauthenticationRequired => write_failure(
            deployment,
            "reauthentication_required",
            None,
            "! Recent sign-in is required before removing a linked identity.\n\nSign in again with a linked identity that will remain:\n  scherzo-cloud auth login --force",
            OutcomeClass::Forbidden,
            json,
        ),
        RemoveIdentityOutcome::NotFound => write_failure(
            deployment,
            "not_found",
            None,
            "! Linked identity not found or unavailable.\n\nList the identities attached to your account:\n  scherzo-cloud auth identities list",
            OutcomeClass::GeneralFailure,
            json,
        ),
        RemoveIdentityOutcome::RemovalUnavailable => write_failure(
            deployment,
            "removal_unavailable",
            None,
            "! The current or last linked identity cannot be removed.\n\nLink another identity, or sign in with a different linked identity before trying again.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        RemoveIdentityOutcome::IdempotencyConflict => write_failure(
            deployment,
            "idempotency_conflict",
            None,
            "! The identity-removal request conflicted with another request.",
            OutcomeClass::GeneralFailure,
            json,
        ),
    }
}

pub(super) struct LinkOutput {
    json: bool,
}

struct LinkTerminal<'a> {
    outcome: &'static str,
    phase: Option<&'static str>,
    category: Option<&'static str>,
    identity: Option<&'a OidcIdentity>,
    local_session_identity: &'static str,
}

impl LinkTerminal<'_> {
    const fn new(outcome: &'static str) -> Self {
        Self {
            outcome,
            phase: None,
            category: None,
            identity: None,
            local_session_identity: "unchanged",
        }
    }

    const fn with_credential_state(mut self, state: LocalCredentialState) -> Self {
        self.local_session_identity = match state {
            LocalCredentialState::Retained => "unchanged",
            LocalCredentialState::Removed => "removed",
        };
        self
    }
}

impl LinkOutput {
    pub(super) const fn new(json: bool) -> Self {
        Self { json }
    }

    pub(super) const fn is_json(&self) -> bool {
        self.json
    }

    pub(super) fn activation(
        &mut self,
        deployment: &Deployment,
        authorization: &DeviceAuthorization,
        expires_at: OffsetDateTime,
    ) -> anyhow::Result<()> {
        if self.json {
            let event = crate::human_auth::device_flow::activation_event(
                deployment,
                authorization,
                expires_at,
                Some("identity_link"),
            )
            .context("format identity-link activation expiration")?;
            self.json_line(&event)
        } else {
            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            writeln!(stdout, "Link a sign-in identity to Scherzo Cloud\n")?;
            writeln!(stdout, "open: {}", authorization.activation_uri())?;
            writeln!(stdout, "code: {}", authorization.user_code())?;
            writeln!(stdout, "\nSign in with the identity you want to link.")?;
            stdout.flush().context("write identity-link activation")?;
            let stderr = io::stderr();
            let mut stderr = stderr.lock();
            writeln!(stderr, "Waiting for authorization...")
                .context("write identity-link progress")?;
            stderr.flush().context("write identity-link progress")
        }
    }

    pub(super) fn cancelled(&mut self, deployment: &str) -> anyhow::Result<ExitCode> {
        self.write_terminal(
            deployment,
            LinkTerminal::new("cancelled"),
            "! Identity linking cancelled.",
            OutcomeClass::Interrupted,
        )
    }

    pub(super) fn browser_failure(
        &mut self,
        deployment: &str,
        outcome: &'static str,
        phase: &'static str,
        category: Option<UnreachableCategory>,
        human: &str,
        class: OutcomeClass,
    ) -> anyhow::Result<ExitCode> {
        self.write_terminal(
            deployment,
            LinkTerminal {
                outcome,
                phase: Some(phase),
                category: category.map(UnreachableCategory::as_str),
                identity: None,
                local_session_identity: "unchanged",
            },
            human,
            class,
        )
    }

    pub(super) fn api_outcome(
        &mut self,
        deployment: &str,
        outcome: &LinkIdentityOutcome,
        credential_state: LocalCredentialState,
    ) -> anyhow::Result<ExitCode> {
        match outcome {
            LinkIdentityOutcome::Linked(identity) => self.write_terminal(
                deployment,
                LinkTerminal {
                    outcome: "linked",
                    phase: None,
                    category: None,
                    identity: Some(identity),
                    local_session_identity: "unchanged",
                }
                .with_credential_state(credential_state),
                "✓ Sign-in identity linked.",
                OutcomeClass::Success,
            ),
            LinkIdentityOutcome::Common(common) => match common {
                CommonIdentityFailure::Unauthenticated => self.write_terminal(
                    deployment,
                    LinkTerminal::new("unauthenticated")
                        .with_credential_state(credential_state),
                    "! You must sign in before linking another identity.\n\nRun:\n  scherzo-cloud auth login",
                    OutcomeClass::Unauthenticated,
                ),
                CommonIdentityFailure::Forbidden => self.write_terminal(
                    deployment,
                    LinkTerminal::new("forbidden").with_credential_state(credential_state),
                    "! Your account is not permitted to link that identity.",
                    OutcomeClass::Forbidden,
                ),
                CommonIdentityFailure::InvalidInput => self.write_terminal(
                    deployment,
                    LinkTerminal::new("invalid_input").with_credential_state(credential_state),
                    "! The identity-link request was rejected by the deployment.",
                    OutcomeClass::GeneralFailure,
                ),
                CommonIdentityFailure::Unreachable(category) => self.write_terminal(
                    deployment,
                    LinkTerminal {
                        outcome: "unreachable",
                        phase: None,
                        category: Some(category.as_str()),
                        identity: None,
                        local_session_identity: "unchanged",
                    }
                    .with_credential_state(credential_state),
                    "! The identity-link result is unknown.\n\nList linked identities before trying again:\n  scherzo-cloud auth identities list",
                    super::super::super::unreachable_outcome_class(*category),
                ),
            },
            LinkIdentityOutcome::InvalidProof => self.write_terminal(
                deployment,
                LinkTerminal::new("invalid_identity_proof")
                    .with_credential_state(credential_state),
                "! The newly authorized identity proof was rejected.\n\nStart the linking flow again.",
                OutcomeClass::GeneralFailure,
            ),
            LinkIdentityOutcome::IdentityUnavailable => self.write_terminal(
                deployment,
                LinkTerminal::new("identity_unavailable")
                    .with_credential_state(credential_state),
                "! That identity cannot be linked to this account.\n\nChoose a different identity or list the identities already linked.",
                OutcomeClass::GeneralFailure,
            ),
            LinkIdentityOutcome::IdempotencyConflict => self.write_terminal(
                deployment,
                LinkTerminal::new("idempotency_conflict")
                    .with_credential_state(credential_state),
                "! The identity-link request conflicted with another request.",
                OutcomeClass::GeneralFailure,
            ),
        }
    }

    pub(super) fn acting_session_changed(&mut self, deployment: &str) -> anyhow::Result<ExitCode> {
        self.write_terminal(
            deployment,
            LinkTerminal {
                outcome: "acting_session_changed",
                phase: None,
                category: None,
                identity: None,
                local_session_identity: "changed",
            },
            "! The local sign-in changed while identity linking was in progress.\n\nStart the linking flow again.",
            OutcomeClass::GeneralFailure,
        )
    }

    fn write_terminal(
        &mut self,
        deployment: &str,
        terminal: LinkTerminal<'_>,
        human: &str,
        class: OutcomeClass,
    ) -> anyhow::Result<ExitCode> {
        if self.json {
            self.json_line(&LinkResultEvent {
                schema_version: 1,
                event: "result",
                deployment,
                outcome: terminal.outcome,
                phase: terminal.phase,
                category: terminal.category,
                identity: terminal.identity,
                local_session_identity: terminal.local_session_identity,
            })?;
        } else if let Some(identity) = terminal.identity {
            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            writeln!(stdout, "{human}\n")?;
            write_identity_fields(&mut stdout, identity)?;
            writeln!(
                stdout,
                "local session identity: {}",
                terminal.local_session_identity
            )?;
            writeln!(stdout, "deployment: {deployment}")?;
        } else {
            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            writeln!(stdout, "\n{human}")?;
        }
        Ok(class.exit_code())
    }

    fn json_line(&mut self, value: &impl Serialize) -> anyhow::Result<()> {
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        serde_json::to_writer(&mut stdout, value).context("serialize JSON identity-link event")?;
        writeln!(stdout).context("write identity-link event")?;
        stdout.flush().context("write identity-link event")
    }
}

pub(super) fn write_common(
    deployment: &str,
    common: &CommonIdentityFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match common {
        CommonIdentityFailure::Unauthenticated => write_failure(
            deployment,
            "unauthenticated",
            None,
            "! You must sign in before managing linked identities.\n\nRun:\n  scherzo-cloud auth login",
            OutcomeClass::Unauthenticated,
            json,
        ),
        CommonIdentityFailure::Forbidden => write_failure(
            deployment,
            "forbidden",
            None,
            "! Your account is not permitted to perform that identity operation.",
            OutcomeClass::Forbidden,
            json,
        ),
        CommonIdentityFailure::InvalidInput => write_failure(
            deployment,
            "invalid_input",
            None,
            "! The identity input was rejected by the deployment.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        CommonIdentityFailure::Unreachable(category) => write_failure(
            deployment,
            "unreachable",
            Some(category.as_str()),
            &format!(
                "! The Scherzo Cloud deployment is unreachable ({}).",
                category.as_str()
            ),
            super::super::super::unreachable_outcome_class(*category),
            json,
        ),
    }
}

fn write_failure(
    deployment: &str,
    outcome: &'static str,
    category: Option<&'static str>,
    human: &str,
    class: OutcomeClass,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&super::super::super::ApiFailureResult::new(
            deployment, outcome, category,
        ))?;
    } else {
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        writeln!(stdout, "{human}")?;
    }
    Ok(class.exit_code())
}

fn write_identity_list_human(
    deployment: &str,
    identities: &[OidcIdentity],
    next_cursor: Option<&str>,
) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✓ Linked sign-in identities listed.")?;
    writeln!(stdout, "deployment: {deployment}")?;
    writeln!(stdout, "count: {}", identities.len())?;
    for identity in identities {
        writeln!(stdout, "\n── identity ──")?;
        write_identity_fields(&mut stdout, identity)?;
    }
    if let Some(next_cursor) = next_cursor {
        writeln!(stdout, "\nnext cursor: {next_cursor}")?;
    }
    Ok(())
}

fn write_identity_fields(output: &mut impl Write, identity: &OidcIdentity) -> io::Result<()> {
    writeln!(output, "identity: {}", identity.id)?;
    writeln!(
        output,
        "current: {}",
        if identity.current { "yes" } else { "no" }
    )?;
    writeln!(output, "issuer: {}", identity.issuer)?;
    writeln!(output, "subject: {}", identity.subject)?;
    if let Some(email) = &identity.asserted_email {
        writeln!(output, "asserted email: {email}")?;
    }
    if let Some(verified) = identity.email_verified {
        writeln!(
            output,
            "email verified: {}",
            if verified { "yes" } else { "no" }
        )?;
    }
    writeln!(output, "linked: {}", identity.created_at)
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::super::write_pretty_json(value).context("write JSON identity result")
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    items: &'a [OidcIdentity],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemoveResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    identity_id: &'a str,
    local_session_identity: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LinkResultEvent<'a> {
    schema_version: u8,
    event: &'static str,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    identity: Option<&'a OidcIdentity>,
    local_session_identity: &'static str,
}
