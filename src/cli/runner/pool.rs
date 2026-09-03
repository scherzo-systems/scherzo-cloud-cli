use anyhow::Context;
use clap::{Args, Subcommand};

use super::{CloudOptions, Deployment, ExitCode, PaginationArgs, cloud, generate_idempotency_key};

pub(super) const ABOUT: &str = "Manage Scherzo Cloud runner pools";
const COMMAND_PATH: &[&str] = &["runner", "pool"];

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<PoolCommand>,
}

#[derive(Debug, Subcommand)]
enum PoolCommand {
    #[command(about = "Create a Scherzo Cloud runner pool")]
    Create(CreateCommand),
    #[command(about = "List Scherzo Cloud runner pools")]
    List(ListCommand),
    #[command(about = "Show a Scherzo Cloud runner pool")]
    Show(ShowCommand),
    #[command(about = "Rename a Scherzo Cloud runner pool")]
    Rename(RenameCommand),
}

#[derive(Debug, Args)]
struct CreateCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(long, help = "Set the exact runner pool name")]
    name: String,

    #[command(flatten)]
    options: CloudOptions,
}

// Pool and registration commands intentionally keep distinct Clap types so their
// nouns, value names, and help remain exact without a metadata abstraction.
// jscpd:ignore-start
#[derive(Debug, Args)]
struct ListCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[command(flatten)]
    pagination: PaginationArgs,

    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct ShowCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(value_name = "POOL", help = "Runner pool ID or exact name")]
    pool: String,

    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct RenameCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(value_name = "POOL", help = "Runner pool ID or exact name")]
    pool: String,

    #[arg(long, help = "Set the exact runner pool name")]
    name: String,

    #[command(flatten)]
    options: CloudOptions,
}
// jscpd:ignore-end

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let Some(command) = self.command else {
            return super::super::print_help(COMMAND_PATH);
        };
        super::execute_cloud(command, |command, deployment| command.execute(deployment))
    }
}

impl PoolCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        match self {
            Self::Create(command) => command.execute(deployment),
            Self::List(command) => command.execute(deployment),
            Self::Show(command) => command.execute(deployment),
            Self::Rename(command) => command.execute(deployment),
        }
    }
}

impl CreateCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key =
            generate_idempotency_key().context("generate runner pool creation request identity")?;
        let result = cloud::with_api(deployment, self.options.http.transport_policy(), |api| {
            api.create_pool(&self.organization, &key, &self.name)
        })?;
        cloud::write_pool_create(
            deployment.fingerprint().api_url(),
            &result,
            self.options.json,
        )
    }
}

impl ListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = cloud::with_api(deployment, self.options.http.transport_policy(), |api| {
            api.list_pools(
                &self.organization,
                self.pagination.limit,
                self.pagination.cursor.as_deref(),
            )
        })?;
        cloud::write_pool_list(
            deployment.fingerprint().api_url(),
            &result,
            self.options.json,
        )
    }
}

impl ShowCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = cloud::with_api(deployment, self.options.http.transport_policy(), |api| {
            api.get_pool(&self.organization, &self.pool)
        })?;
        cloud::write_pool_show(
            deployment.fingerprint().api_url(),
            &result,
            self.options.json,
        )
    }
}

impl RenameCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key =
            generate_idempotency_key().context("generate runner pool rename request identity")?;
        let result = cloud::with_api(deployment, self.options.http.transport_policy(), |api| {
            api.rename_pool(&self.organization, &self.pool, &key, &self.name)
        })?;
        cloud::write_pool_rename(
            deployment.fingerprint().api_url(),
            &result,
            self.options.json,
        )
    }
}
