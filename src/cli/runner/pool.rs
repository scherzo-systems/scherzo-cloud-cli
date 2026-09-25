use anyhow::Context;
use clap::{Args, Subcommand};

use super::{
    CloudOptions, Deployment, ExitCode, OrganizationArg, PaginationArgs, PoolArg, cloud,
    generate_idempotency_key,
};

pub(super) const ABOUT: &str = "Manage Scherzo Cloud runner pools";
const COMMAND_PATH: &[&str] = &["runner", "pool"];

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<PoolCommand>,
}

#[derive(Debug, Subcommand)]
enum PoolCommand {
    #[command(about = "Create a runner pool")]
    Create(CreateCommand),
    #[command(
        about = "Delete a runner pool",
        after_help = "Eligibility:\n  The runner pool must be unused before deletion."
    )]
    Delete(DeleteCommand),
    #[command(about = "List runner pools")]
    List(ListCommand),
    #[command(about = "Rename a runner pool")]
    Rename(RenameCommand),
    #[command(about = "Show a runner pool")]
    Show(ShowCommand),
}

#[derive(Debug, Args)]
struct CreateCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

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
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[command(flatten)]
    pagination: PaginationArgs,

    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct ShowCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(value_name = PoolArg::VALUE_NAME, help = PoolArg::HELP)]
    pool: PoolArg,

    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct RenameCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(value_name = PoolArg::VALUE_NAME, help = PoolArg::HELP)]
    pool: PoolArg,

    #[arg(long, help = "Set the exact runner pool name")]
    name: String,

    #[command(flatten)]
    options: CloudOptions,
}
// jscpd:ignore-end

#[derive(Debug, Args)]
struct DeleteCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(value_name = PoolArg::VALUE_NAME, help = PoolArg::HELP)]
    pool: PoolArg,

    #[command(flatten)]
    confirmation: super::super::ConfirmationArgs,

    #[command(flatten)]
    options: CloudOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let Some(command) = self.command else {
            return super::super::print_help(COMMAND_PATH);
        };
        match command {
            PoolCommand::Create(command) => super::execute_cloud(command, CreateCommand::execute),
            PoolCommand::List(command) => super::execute_cloud(command, ListCommand::execute),
            PoolCommand::Show(command) => super::execute_cloud(command, ShowCommand::execute),
            PoolCommand::Rename(command) => super::execute_cloud(command, RenameCommand::execute),
            PoolCommand::Delete(command) => command.execute(),
        }
    }
}

impl CreateCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key =
            generate_idempotency_key().context("generate runner pool creation request identity")?;
        let result = cloud::with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.create_pool(&self.organization, &key, &self.name),
        )?;
        // Pool creation uses the shared runner failure contract but retains its own result body.
        // jscpd:ignore-start
        cloud::write_pool_create(
            deployment.fingerprint().api_url(),
            &result,
            self.options.authentication.kind(),
            self.options.json,
        )
        // jscpd:ignore-end
    }
}

impl ListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = cloud::with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                api.list_pools(
                    &self.organization,
                    self.pagination.limit,
                    self.pagination.cursor.as_deref(),
                )
            },
        )?;
        cloud::write_pool_list(
            deployment.fingerprint().api_url(),
            &result,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl ShowCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = cloud::with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.get_pool(&self.organization, &self.pool),
        )?;
        cloud::write_pool_show(
            deployment.fingerprint().api_url(),
            &result,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl RenameCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key =
            generate_idempotency_key().context("generate runner pool rename request identity")?;
        let result = cloud::with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.rename_pool(&self.organization, &self.pool, &key, &self.name),
        )?;
        cloud::write_pool_rename(
            deployment.fingerprint().api_url(),
            &result,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl DeleteCommand {
    fn execute(self) -> super::super::CommandResult {
        super::execute_pool_deletion(self.organization, self.pool, self.options)
    }
}
