use std::io::{self, Write};

use anyhow::Context;
use clap::{Args, Subcommand};

use super::{CloudOptions, PaginationArgs, cloud};
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;
use crate::idempotency::generate_idempotency_key;

pub(super) const ABOUT: &str = "Manage runner credential lifecycle";
const COMMAND_PATH: &[&str] = &["runner", "credential"];

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<CredentialCommand>,
}

#[derive(Debug, Subcommand)]
enum CredentialCommand {
    #[command(about = "List non-secret runner credential metadata")]
    List(ListCommand),
    #[command(about = "Schedule fixed-grace credential retirement")]
    Retire(MutationCommand),
    #[command(about = "Revoke a runner credential immediately")]
    Revoke(MutationCommand),
}

// Credential commands keep their own Clap metadata so help and positional
// resource contracts remain explicit instead of coupling lifecycle commands to
// activation or registration administration.
// jscpd:ignore-start
#[derive(Debug, Args)]
struct ListCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,
    #[arg(value_name = "RUNNER", help = "Runner ID or exact name")]
    runner: String,
    #[command(flatten)]
    pagination: PaginationArgs,
    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct MutationCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,
    #[arg(value_name = "RUNNER", help = "Runner ID or exact name")]
    runner: String,
    #[arg(value_name = "CREDENTIAL", help = "Runner credential ID")]
    credential: String,
    #[command(flatten)]
    options: CloudOptions,
}
// jscpd:ignore-end

// This namespace deliberately mirrors sibling runner namespaces while retaining
// its credential-specific command path and deployment-loading boundary.
// jscpd:ignore-start
impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let Some(command) = self.command else {
            return super::super::print_help(COMMAND_PATH);
        };
        super::execute_cloud(command, |command, deployment| command.execute(deployment))
    }
}
// jscpd:ignore-end

impl CredentialCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        match self {
            Self::List(command) => command.execute(deployment),
            Self::Retire(command) => command.execute(deployment, CredentialMutation::Retire),
            Self::Revoke(command) => command.execute(deployment, CredentialMutation::Revoke),
        }
    }
}

impl ListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = cloud::with_api(deployment, self.options.http.transport_policy(), |api| {
            let runner = api.get_registration(&self.organization, &self.runner)?;
            api.list_credentials(
                &self.organization,
                &runner.id,
                self.pagination.limit,
                self.pagination.cursor.as_deref(),
            )
        })?;
        match result {
            Ok(page) => {
                if self.options.json {
                    serde_json::to_writer_pretty(
                        &mut io::stdout().lock(),
                        &serde_json::json!({
                            "schemaVersion": 1,
                            "outcome": "listed",
                            "items": page.items,
                            "nextCursor": page.next_cursor,
                        }),
                    )?;
                    writeln!(io::stdout().lock())?;
                } else {
                    let mut output = io::stdout().lock();
                    writeln!(output, "✓ Runner credentials listed.\n")?;
                    for credential in page.items {
                        writeln!(
                            output,
                            "  Credential: {}  Stored: {}  Effective: {}  Created: {}",
                            credential.id,
                            stored_state_label(credential.stored_state),
                            effective_state_label(credential.effective_state),
                            credential.created_at,
                        )?;
                        if let Some(last_authenticated_at) = credential.last_authenticated_at {
                            writeln!(output, "    Last authenticated: {last_authenticated_at}")?;
                        }
                        if let Some(retire_at) = credential.retire_at {
                            writeln!(output, "    Retires:            {retire_at}")?;
                        }
                        if let Some(revoked_at) = credential.revoked_at {
                            writeln!(output, "    Revoked:            {revoked_at}")?;
                        }
                    }
                    if let Some(cursor) = page.next_cursor {
                        writeln!(output, "\n  Next cursor: {cursor}")?;
                    }
                }
                Ok(ExitCode::Success)
            }
            Err(failure) => write_failure(deployment, &failure, self.options.json),
        }
    }
}

#[derive(Clone, Copy)]
enum CredentialMutation {
    Retire,
    Revoke,
}

impl MutationCommand {
    fn execute(
        self,
        deployment: &Deployment,
        mutation: CredentialMutation,
    ) -> anyhow::Result<ExitCode> {
        let key = generate_idempotency_key().context(match mutation {
            CredentialMutation::Retire => "generate credential retirement identity",
            CredentialMutation::Revoke => "generate credential revocation identity",
        })?;
        let result = cloud::with_api(deployment, self.options.http.transport_policy(), |api| {
            let runner = api.get_registration(&self.organization, &self.runner)?;
            match mutation {
                CredentialMutation::Retire => {
                    api.retire_credential(&self.organization, &runner.id, &self.credential, &key)
                }
                CredentialMutation::Revoke => {
                    api.revoke_credential(&self.organization, &runner.id, &self.credential, &key)
                }
            }
        })?;
        match result {
            Ok(credential) => {
                let outcome = match mutation {
                    CredentialMutation::Retire => "retiring",
                    CredentialMutation::Revoke => "revoked",
                };
                if self.options.json {
                    serde_json::to_writer_pretty(
                        &mut io::stdout().lock(),
                        &serde_json::json!({
                            "schemaVersion": 1,
                            "outcome": outcome,
                            "credential": credential,
                        }),
                    )?;
                    writeln!(io::stdout().lock())?;
                } else {
                    let heading = match mutation {
                        CredentialMutation::Retire => "✓ Runner credential retirement scheduled.",
                        CredentialMutation::Revoke => "✓ Runner credential revoked.",
                    };
                    let mut output = io::stdout().lock();
                    writeln!(output, "{heading}\n")?;
                    writeln!(output, "  Credential: {}", credential.id)?;
                    writeln!(
                        output,
                        "  Effective state: {}",
                        effective_state_label(credential.effective_state),
                    )?;
                    if let Some(retire_at) = credential.retire_at {
                        writeln!(output, "  Retires: {retire_at}")?;
                    }
                }
                Ok(ExitCode::Success)
            }
            Err(failure) => write_failure(deployment, &failure, self.options.json),
        }
    }
}

fn write_failure(
    deployment: &Deployment,
    failure: &crate::api::RunnerFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    cloud::write_failure(deployment.fingerprint().api_url(), failure, json)
}

fn stored_state_label(state: crate::api::RunnerCredentialStoredState) -> &'static str {
    use crate::api::RunnerCredentialStoredState;
    match state {
        RunnerCredentialStoredState::Active => "active",
        RunnerCredentialStoredState::Retiring => "retiring",
        RunnerCredentialStoredState::Revoked => "revoked",
    }
}

fn effective_state_label(state: crate::api::RunnerCredentialEffectiveState) -> &'static str {
    use crate::api::RunnerCredentialEffectiveState;
    match state {
        RunnerCredentialEffectiveState::Active => "active",
        RunnerCredentialEffectiveState::Retiring => "retiring",
        RunnerCredentialEffectiveState::Revoked => "revoked",
    }
}
