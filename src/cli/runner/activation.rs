use std::io::{self, Write};

use anyhow::Context;
use clap::{Args, Subcommand};

use super::{
    CloudOptions, RegistrationTarget, cloud, completed_cloud_result,
    validate_activation_destination, write_activation_issuance, write_activation_summary,
};
use crate::api::generate_idempotency_key;
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

pub(super) const ABOUT: &str = "Manage runner enrollment activations";
const COMMAND_PATH: &[&str] = &["runner", "activation"];

// Pool and activation namespaces keep concrete subcommand enums so Clap owns
// each exact operator vocabulary without a metadata-driven command abstraction.
// jscpd:ignore-start
#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<ActivationCommand>,
}

#[derive(Debug, Subcommand)]
enum ActivationCommand {
    #[command(about = "Create a single-use runner activation")]
    Create(CreateCommand),
    #[command(about = "List runner activations")]
    List(ListCommand),
    #[command(about = "Revoke a runner activation")]
    Revoke(RevokeCommand),
}
// jscpd:ignore-end

#[derive(Debug, Args)]
struct CreateCommand {
    #[command(flatten)]
    target: RegistrationTarget,
    #[arg(
        long,
        value_name = "PATH|-",
        help = "Create the protected artifact file, or write only the artifact to stdout"
    )]
    activation_file: String,
    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct ListCommand {
    #[command(flatten)]
    target: RegistrationTarget,
    #[arg(long, value_name = "LIMIT")]
    limit: Option<u16>,
    #[arg(long, value_name = "CURSOR")]
    cursor: Option<String>,
    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct RevokeCommand {
    #[command(flatten)]
    target: RegistrationTarget,
    #[arg(value_name = "ACTIVATION", help = "Runner activation ID")]
    activation: String,
    #[command(flatten)]
    options: CloudOptions,
}

// Nested command dispatch deliberately mirrors the pool namespace while
// preserving activation-specific help and subcommand types.
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

impl ActivationCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        match self {
            Self::Create(command) => command.execute(deployment),
            Self::List(command) => command.execute(deployment),
            Self::Revoke(command) => command.execute(deployment),
        }
    }
}

impl CreateCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        validate_activation_destination(&self.activation_file, self.options.json)?;
        let key = generate_idempotency_key().context("generate activation request identity")?;
        let result = cloud::with_api(deployment, self.options.http.transport_policy(), |api| {
            let runner = api.get_registration(&self.target.organization, &self.target.runner)?;
            api.create_activation(&self.target.organization, &runner.id, &key)
        })?;
        let issuance = match completed_cloud_result(deployment, result, self.options.json)? {
            Ok(issuance) => issuance,
            Err(exit_code) => return Ok(exit_code),
        };
        let artifact = write_activation_issuance(&self.activation_file, &issuance)?;
        if self.activation_file == "-" {
            writeln!(
                io::stderr().lock(),
                "✓ Runner activation created for {}.",
                artifact.runner_id()
            )?;
        } else if self.options.json {
            // Registration creation and standalone activation issuance expose
            // deliberately different non-secret JSON result documents.
            // jscpd:ignore-start
            serde_json::to_writer_pretty(
                &mut io::stdout().lock(),
                &serde_json::json!({
                    "schemaVersion": 1,
                    "outcome": "created",
                    "activation": issuance.activation,
                    "activationFile": self.activation_file,
                }),
            )?;
            writeln!(io::stdout().lock())?;
            // jscpd:ignore-end
        } else {
            write_activation_summary(
                &mut io::stdout().lock(),
                "✓ Runner activation created.",
                artifact.runner_id(),
                None,
                &self.activation_file,
            )?;
        }
        Ok(ExitCode::Success)
    }
}

impl ListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = cloud::with_api(deployment, self.options.http.transport_policy(), |api| {
            let runner = api.get_registration(&self.target.organization, &self.target.runner)?;
            api.list_activations(
                &self.target.organization,
                &runner.id,
                self.limit,
                self.cursor.as_deref(),
            )
        })?;
        // List and revoke retain concrete success documents and human reports;
        // only their common Cloud failure renderer is intentionally parallel.
        // jscpd:ignore-start
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
                    writeln!(io::stdout().lock(), "✓ Runner activations listed.\n")?;
                    for activation in page.items {
                        writeln!(
                            io::stdout().lock(),
                            "  Activation: {}  State: {}  Expires: {}",
                            activation.id,
                            activation_state_label(activation.state),
                            activation.expires_at
                        )?;
                    }
                }
                Ok(ExitCode::Success)
            }
            Err(failure) => cloud::write_failure(
                deployment.fingerprint().api_url(),
                &failure,
                self.options.json,
            ),
        }
        // jscpd:ignore-end
    }
}

impl RevokeCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key = generate_idempotency_key().context("generate activation revocation identity")?;
        let result = cloud::with_api(deployment, self.options.http.transport_policy(), |api| {
            let runner = api.get_registration(&self.target.organization, &self.target.runner)?;
            api.revoke_activation(
                &self.target.organization,
                &runner.id,
                &self.activation,
                &key,
            )
        })?;
        match result {
            Ok(activation) => {
                if self.options.json {
                    serde_json::to_writer_pretty(
                        &mut io::stdout().lock(),
                        &serde_json::json!({
                            "schemaVersion": 1,
                            "outcome": "revoked",
                            "activation": activation,
                        }),
                    )?;
                    writeln!(io::stdout().lock())?;
                } else {
                    writeln!(
                        io::stdout().lock(),
                        "✓ Runner activation revoked.\n\n  Activation: {}",
                        activation.id
                    )?;
                }
                Ok(ExitCode::Success)
            }
            Err(failure) => cloud::write_failure(
                deployment.fingerprint().api_url(),
                &failure,
                self.options.json,
            ),
        }
    }
}

fn activation_state_label(state: crate::api::RunnerActivationState) -> &'static str {
    use crate::api::RunnerActivationState;
    match state {
        RunnerActivationState::Issued => "issued",
        RunnerActivationState::Consumed => "consumed",
        RunnerActivationState::Revoked => "revoked",
        RunnerActivationState::Expired => "expired",
    }
}
