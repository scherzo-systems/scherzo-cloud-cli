mod cloud;
mod doctor;
mod pool;
mod serve;

use clap::{Args, Subcommand};

use crate::api::generate_idempotency_key;
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

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
    #[command(about = "List Scherzo Cloud runner registrations")]
    List(ListCommand),
    #[command(about = "Show a Scherzo Cloud runner registration")]
    Show(ShowCommand),
    #[command(about = "Rename a Scherzo Cloud runner registration")]
    Rename(RenameCommand),
    #[command(about = doctor::ABOUT)]
    Doctor(doctor::Command),
    #[command(about = serve::ABOUT)]
    Serve(serve::Command),
}

#[derive(Debug, Args)]
struct CloudOptions {
    #[arg(long, help = "Print the runner result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::HttpOptions,
}

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
struct ShowCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(value_name = "RUNNER", help = "Runner ID or exact name")]
    runner: String,

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
            Some(RunnerCommand::List(command)) => execute_cloud(command, ListCommand::execute),
            Some(RunnerCommand::Show(command)) => execute_cloud(command, ShowCommand::execute),
            Some(RunnerCommand::Rename(command)) => execute_cloud(command, RenameCommand::execute),
            Some(RunnerCommand::Doctor(command)) => command.execute(),
            Some(RunnerCommand::Serve(command)) => command.execute(),
        }
    }
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
        let Self {
            organization,
            runner,
            options,
        } = self;
        let result = cloud::with_api(deployment, options.http.transport_policy(), |api| {
            api.get_registration(&organization, &runner)
        })?;
        cloud::write_runner_show(deployment.fingerprint().api_url(), &result, options.json)
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
