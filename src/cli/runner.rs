mod activation;
mod cloud;
mod credential;
mod doctor;
mod enroll;
mod pool;
mod serve;
mod status;

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use clap::{Args, Subcommand};

use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;
use crate::idempotency::generate_idempotency_key;

pub(super) const ABOUT: &str = "Work with the Scherzo Cloud runner";
const NAME: &str = "runner";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<RunnerCommand>,
}

#[derive(Debug, Subcommand)]
enum RunnerCommand {
    #[command(about = pool::ABOUT)]
    Pool(pool::Command),
    #[command(about = "Create a runner registration and enrollment activation")]
    Create(CreateCommand),
    #[command(about = activation::ABOUT)]
    Activation(activation::Command),
    #[command(about = credential::ABOUT)]
    Credential(credential::Command),
    #[command(about = enroll::ABOUT)]
    Enroll(enroll::Command),
    #[command(about = "List Scherzo Cloud runner registrations")]
    List(ListCommand),
    #[command(about = "Show a Scherzo Cloud runner registration")]
    Show(ShowCommand),
    #[command(about = "Enable a Scherzo Cloud runner registration")]
    Enable(ModeCommand),
    #[command(about = "Drain a Scherzo Cloud runner registration")]
    Drain(ModeCommand),
    #[command(about = "Disable a Scherzo Cloud runner registration")]
    Disable(ModeCommand),
    #[command(about = "Move a quiescent Scherzo Cloud runner registration")]
    Move(MoveCommand),
    #[command(about = "Rename a Scherzo Cloud runner registration")]
    Rename(RenameCommand),
    #[command(about = doctor::ABOUT)]
    Doctor(doctor::Command),
    #[command(about = serve::ABOUT)]
    Serve(serve::Command),
    #[command(about = status::ABOUT)]
    Status(status::Command),
}

#[derive(Debug, Args)]
struct CloudOptions {
    #[arg(long, help = "Print the runner result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::HttpOptions,
}

// Registration creation and standalone activation issuance intentionally keep
// distinct Clap types because their positional resources and required options
// are different operator contracts.
// jscpd:ignore-start
#[derive(Debug, Args)]
struct CreateCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(long, value_name = "POOL", help = "Runner pool ID or exact name")]
    pool: String,

    #[arg(long, help = "Set the exact runner name")]
    name: Option<String>,

    #[arg(
        long,
        value_name = "PATH|-",
        help = "Create the protected artifact file, or write only the artifact to stdout"
    )]
    activation_file: String,

    #[command(flatten)]
    options: CloudOptions,
}
// jscpd:ignore-end

#[derive(Debug, Args)]
struct ListCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(
        long,
        value_name = "LIMIT",
        help = "Limit the number of runners returned"
    )]
    limit: Option<u16>,

    #[arg(
        long,
        value_name = "CURSOR",
        help = "Continue from an opaque page cursor"
    )]
    cursor: Option<String>,

    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct RegistrationTarget {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(value_name = "RUNNER", help = "Runner ID or exact name")]
    runner: String,
}

#[derive(Debug, Args)]
struct ShowCommand {
    #[command(flatten)]
    target: RegistrationTarget,

    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct ModeCommand {
    #[command(flatten)]
    target: RegistrationTarget,

    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct MoveCommand {
    #[command(flatten)]
    target: RegistrationTarget,

    #[arg(long, value_name = "POOL", help = "Destination pool ID or exact name")]
    pool: String,

    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct RenameCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(value_name = "RUNNER", help = "Runner ID or exact name")]
    runner: String,

    #[arg(long, help = "Set the exact runner name")]
    name: String,

    #[command(flatten)]
    options: CloudOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(RunnerCommand::Pool(command)) => command.execute(),
            Some(RunnerCommand::Create(command)) => execute_cloud(command, CreateCommand::execute),
            Some(RunnerCommand::Activation(command)) => command.execute(),
            Some(RunnerCommand::Credential(command)) => command.execute(),
            Some(RunnerCommand::Enroll(command)) => command.execute(),
            Some(RunnerCommand::List(command)) => execute_cloud(command, ListCommand::execute),
            Some(RunnerCommand::Show(command)) => execute_cloud(command, ShowCommand::execute),
            Some(RunnerCommand::Enable(command)) => {
                execute_cloud(command, |command, deployment| {
                    command.execute(
                        deployment,
                        crate::api::RunnerRegistrationMode::Enabled,
                        "enabled",
                        "✓ Runner enabled.",
                    )
                })
            }
            Some(RunnerCommand::Drain(command)) => execute_cloud(command, |command, deployment| {
                command.execute(
                    deployment,
                    crate::api::RunnerRegistrationMode::Draining,
                    "draining",
                    "✓ Runner draining.",
                )
            }),
            Some(RunnerCommand::Disable(command)) => {
                execute_cloud(command, |command, deployment| {
                    command.execute(
                        deployment,
                        crate::api::RunnerRegistrationMode::Disabled,
                        "disabled",
                        "✓ Runner disabled.",
                    )
                })
            }
            Some(RunnerCommand::Move(command)) => execute_cloud(command, MoveCommand::execute),
            Some(RunnerCommand::Rename(command)) => execute_cloud(command, RenameCommand::execute),
            Some(RunnerCommand::Doctor(command)) => command.execute(),
            Some(RunnerCommand::Serve(command)) => command.execute(),
            Some(RunnerCommand::Status(command)) => command.execute(),
        }
    }
}

fn operator_config_path(path: &Path) -> anyhow::Result<PathBuf> {
    std::path::absolute(path)
        .with_context(|| format!("resolve runner operator configuration {}", path.display()))
}

enum CreateOutcome {
    Complete {
        registration: crate::api::RunnerRegistration,
        issuance: crate::api::RunnerActivationIssuance,
    },
    ActivationFailed {
        registration: crate::api::RunnerRegistration,
        failure: crate::api::RunnerFailure,
    },
}

impl CreateOutcome {
    fn credential_rejected(&self) -> bool {
        matches!(
            self,
            Self::ActivationFailed { failure, .. } if failure.credential_rejected()
        )
    }
}

impl CreateCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        validate_activation_destination(&self.activation_file, self.options.json)?;
        let registration_key =
            generate_idempotency_key().context("generate runner registration request identity")?;
        let activation_key =
            generate_idempotency_key().context("generate activation request identity")?;
        let mut committed_registration = None;
        let result = cloud::with_api_retrying_rejected_result(
            deployment,
            self.options.http.transport_policy(),
            |api| {
                let pool = api.get_pool(&self.organization, &self.pool)?;
                let registration = api.create_registration(
                    &self.organization,
                    &registration_key,
                    &pool.id,
                    self.name.as_deref(),
                )?;
                committed_registration = Some(registration.clone());
                Ok(
                    match api.create_activation(
                        &self.organization,
                        &registration.id,
                        &activation_key,
                    ) {
                        Ok(issuance) => CreateOutcome::Complete {
                            registration,
                            issuance,
                        },
                        Err(failure) => CreateOutcome::ActivationFailed {
                            registration,
                            failure,
                        },
                    },
                )
            },
            CreateOutcome::credential_rejected,
        )?;
        let result = match (result, committed_registration) {
            (Err(failure), Some(registration)) => Ok(CreateOutcome::ActivationFailed {
                registration,
                failure,
            }),
            (result, _) => result,
        };
        let outcome = match completed_cloud_result(deployment, result, self.options.json)? {
            Ok(outcome) => outcome,
            Err(exit_code) => return Ok(exit_code),
        };
        let (registration, issuance) = match outcome {
            CreateOutcome::Complete {
                registration,
                issuance,
            } => (registration, issuance),
            CreateOutcome::ActivationFailed {
                registration,
                failure,
            } => {
                return cloud::write_activation_failure(
                    deployment.fingerprint().api_url(),
                    &failure,
                    &self.organization,
                    &registration.id,
                    self.options.json,
                );
            }
        };
        write_activation_issuance(&self.activation_file, &issuance)?;
        if self.activation_file == "-" {
            writeln!(
                io::stderr().lock(),
                "✓ Runner {} created with an activation.",
                registration.id
            )?;
        } else if self.options.json {
            serde_json::to_writer_pretty(
                &mut io::stdout().lock(),
                &serde_json::json!({
                    "schemaVersion": 1,
                    "outcome": "created",
                    "runner": registration,
                    "activation": issuance.activation,
                    "activationFile": self.activation_file,
                }),
            )?;
            writeln!(io::stdout().lock())?;
        } else {
            write_activation_summary(
                &mut io::stdout().lock(),
                "✓ Runner created.",
                &registration.id,
                Some(&registration.name),
                &self.activation_file,
            )?;
        }
        Ok(ExitCode::Success)
    }
}

fn completed_cloud_result<T>(
    deployment: &Deployment,
    result: Result<T, crate::api::RunnerFailure>,
    json: bool,
) -> anyhow::Result<Result<T, ExitCode>> {
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(failure) => Ok(Err(cloud::write_failure(
            deployment.fingerprint().api_url(),
            &failure,
            json,
        )?)),
    }
}

fn validate_activation_destination(destination: &str, json: bool) -> anyhow::Result<()> {
    if destination == "-" && json {
        return Err(anyhow::anyhow!(
            "--json cannot be combined with --activation-file -"
        ));
    }
    Ok(())
}

fn write_activation_issuance(
    destination: &str,
    issuance: &crate::api::RunnerActivationIssuance,
) -> anyhow::Result<crate::runner::enrollment::ActivationArtifact> {
    let api_artifact = issuance.artifact.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "activation issuance replay omitted its secret; issue a replacement activation with a new command"
        )
    })?;
    let artifact = crate::runner::enrollment::ActivationArtifact::from_parts(
        crate::runner::enrollment::ActivationArtifactParts {
            activation_url: api_artifact.activation_url.clone(),
            activation_token: api_artifact.activation_token.clone(),
            runner_id: api_artifact.runner_id.clone(),
            expires_at: api_artifact.expires_at.clone(),
        },
    );
    crate::runner::enrollment::write_activation_file(destination, &artifact)
        .map_err(|error| anyhow::anyhow!(error))?;
    Ok(artifact)
}

fn write_activation_summary(
    output: &mut impl Write,
    heading: &str,
    runner_id: &str,
    runner_name: Option<&str>,
    destination: &str,
) -> io::Result<()> {
    writeln!(output, "{heading}\n")?;
    writeln!(output, "  Runner:          {runner_id}")?;
    if let Some(name) = runner_name {
        writeln!(output, "  Name:            {name}")?;
    }
    writeln!(output, "  Activation file: {destination}")
}

impl ListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let Self {
            organization,
            limit,
            cursor,
            options,
        } = self;
        let result = cloud::with_api(deployment, options.http.transport_policy(), |api| {
            api.list_registrations(&organization, limit, cursor.as_deref())
        })?;
        cloud::write_runner_list(deployment.fingerprint().api_url(), &result, options.json)
    }
}

impl ShowCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let Self { target, options } = self;
        let result = cloud::with_api(deployment, options.http.transport_policy(), |api| {
            api.get_registration(&target.organization, &target.runner)
        })?;
        cloud::write_runner_show(deployment.fingerprint().api_url(), &result, options.json)
    }
}

impl ModeCommand {
    fn execute(
        self,
        deployment: &Deployment,
        mode: crate::api::RunnerRegistrationMode,
        outcome: &'static str,
        heading: &'static str,
    ) -> anyhow::Result<ExitCode> {
        let Self { target, options } = self;
        let key = generate_idempotency_key().context("generate runner mode request identity")?;
        let result = cloud::with_api(deployment, options.http.transport_policy(), |api| {
            api.update_registration_mode(&target.organization, &target.runner, &key, mode)
        })?;
        cloud::write_runner_transition(
            deployment.fingerprint().api_url(),
            &result,
            outcome,
            heading,
            options.json,
        )
    }
}

impl MoveCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let Self {
            target,
            pool,
            options,
        } = self;
        let key = generate_idempotency_key().context("generate runner move request identity")?;
        let result = cloud::with_api(deployment, options.http.transport_policy(), |api| {
            api.move_registration(&target.organization, &target.runner, &pool, &key)
        })?;
        cloud::write_runner_transition(
            deployment.fingerprint().api_url(),
            &result,
            "moved",
            "✓ Runner moved.",
            options.json,
        )
    }
}

impl RenameCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let Self {
            organization,
            runner,
            name,
            options,
        } = self;
        let key = generate_idempotency_key().context("generate runner rename request identity")?;
        let result = cloud::with_api(deployment, options.http.transport_policy(), |api| {
            api.rename_registration(&organization, &runner, &key, &name)
        })?;
        cloud::write_runner_rename(deployment.fingerprint().api_url(), &result, options.json)
    }
}

fn execute_cloud<T>(
    command: T,
    execute: impl FnOnce(T, &Deployment) -> anyhow::Result<ExitCode>,
) -> super::CommandResult {
    super::execute_deployment_command(
        Some(command),
        &[NAME],
        "configure Scherzo Cloud runner administration",
        |command, deployment| execute(command, deployment).map_err(Into::into),
    )
}

use anyhow::Context as _;
