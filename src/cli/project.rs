mod output;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand, builder::NonEmptyStringValueParser};

use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;
use scherzo_cloud_api::{
    CreateProjectInput, HttpClient, HttpTransportPolicy, ProjectApi, ProjectFailure, RunnerFailure,
};

use super::{InstallationArg, OrganizationArg, PoolArg, ProjectArg, RepositoryArg};

pub(super) const ABOUT: &str = "Manage Scherzo Cloud projects";
const NAME: &str = "project";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<ProjectCommand>,
}

#[derive(Debug, Subcommand)]
enum ProjectCommand {
    #[command(about = "Create a Scherzo Cloud project")]
    Create(CreateCommand),
    #[command(about = "List Scherzo Cloud projects")]
    List(ListCommand),
    #[command(about = "Show a Scherzo Cloud project")]
    Show(ShowCommand),
    #[command(about = "Rename a Scherzo Cloud project")]
    Rename(RenameCommand),
    #[command(about = "Manage a project's repository binding")]
    Repository(RepositoryCommand),
    #[command(about = "Manage a project's runner pool")]
    RunnerPool(RunnerPoolCommand),
}

type Options = super::CommonArgs<super::ProjectJson, super::PrincipalAuthenticationArgs>;

#[derive(Debug, Args)]
struct RepositorySelectionArgs {
    #[arg(
        long,
        value_name = InstallationArg::VALUE_NAME,
        help = InstallationArg::HELP
    )]
    installation_id: InstallationArg,

    #[arg(
        long,
        value_name = RepositoryArg::VALUE_NAME,
        help = RepositoryArg::HELP
    )]
    repository_id: RepositoryArg,

    #[arg(
        long,
        value_parser = NonEmptyStringValueParser::new(),
        help = "Set the configured default branch (the provider default when omitted)"
    )]
    default_branch: Option<String>,
}

// Project creation has repository and optional pool inputs that must remain distinct
// from runner-pool creation despite their shared organization/name shell.
// jscpd:ignore-start
#[derive(Debug, Args)]
struct CreateCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(long, help = "Set the canonical project name")]
    name: String,

    #[command(flatten)]
    repository: RepositorySelectionArgs,

    #[arg(long, value_name = PoolArg::VALUE_NAME, help = PoolArg::HELP)]
    pool: Option<PoolArg>,

    #[command(flatten)]
    options: Options,
}
// jscpd:ignore-end

// Project leaves intentionally keep their Clap identities explicit so each help page
// names the exact resource accepted by the corresponding public API operation.
// jscpd:ignore-start
#[derive(Debug, Args)]
struct ListCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[command(flatten)]
    pagination: super::PaginationArgs,

    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct ProjectReference {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(value_name = ProjectArg::VALUE_NAME, help = ProjectArg::HELP)]
    project_id: ProjectArg,
}

#[derive(Debug, Args)]
struct ShowCommand {
    #[command(flatten)]
    project: ProjectReference,

    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct RenameCommand {
    #[command(flatten)]
    project: ProjectReference,

    #[arg(long, help = "Set the canonical project name")]
    name: String,

    #[command(flatten)]
    options: Options,
}
// jscpd:ignore-end

#[derive(Debug, Args)]
struct RepositoryCommand {
    #[command(subcommand)]
    command: Option<RepositorySubcommand>,
}

#[derive(Debug, Subcommand)]
enum RepositorySubcommand {
    #[command(about = "List repositories selected for a GitHub installation")]
    List(RepositoryListCommand),
    #[command(about = "Show a project's repository binding")]
    Show(RepositoryShowCommand),
    #[command(about = "Bind or replace a project's repository")]
    Set(RepositorySetCommand),
    #[command(about = "Change a project's configured default branch")]
    Update(RepositoryUpdateCommand),
    #[command(about = "Detach a project's repository")]
    Detach(RepositoryDetachCommand),
    #[command(about = "Discover GitHub installation bindings")]
    Installation(InstallationCommand),
}

#[derive(Debug, Args)]
struct InstallationCommand {
    #[command(subcommand)]
    command: Option<InstallationSubcommand>,
}

#[derive(Debug, Subcommand)]
enum InstallationSubcommand {
    #[command(about = "List GitHub installation bindings")]
    List(InstallationListCommand),
}

#[derive(Debug, Args)]
struct InstallationListCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct RepositoryListCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(value_name = InstallationArg::VALUE_NAME, help = InstallationArg::HELP)]
    installation_id: InstallationArg,

    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct RepositoryShowCommand {
    #[command(flatten)]
    project: ProjectReference,

    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct RepositorySetCommand {
    #[command(flatten)]
    project: ProjectReference,

    #[command(flatten)]
    repository: RepositorySelectionArgs,

    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct RepositoryUpdateCommand {
    #[command(flatten)]
    project: ProjectReference,

    #[arg(
        long,
        value_parser = NonEmptyStringValueParser::new(),
        help = "Set the configured default branch"
    )]
    default_branch: String,

    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct RepositoryDetachCommand {
    #[command(flatten)]
    project: ProjectReference,

    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct RunnerPoolCommand {
    #[command(subcommand)]
    command: Option<RunnerPoolSubcommand>,
}

#[derive(Debug, Subcommand)]
enum RunnerPoolSubcommand {
    #[command(about = "Assign or replace a project's runner pool")]
    Set(RunnerPoolSetCommand),
    #[command(about = "Remove a project's runner pool")]
    Remove(RunnerPoolRemoveCommand),
}

#[derive(Debug, Args)]
struct RunnerPoolSetCommand {
    #[command(flatten)]
    project: ProjectReference,

    #[arg(value_name = PoolArg::VALUE_NAME, help = PoolArg::HELP)]
    pool: PoolArg,

    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct RunnerPoolRemoveCommand {
    #[command(flatten)]
    project: ProjectReference,

    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        let Some(command) = self.command else {
            return super::print_help(&[NAME]);
        };
        match command {
            ProjectCommand::Create(command) => execute_leaf(command, CreateCommand::execute),
            ProjectCommand::List(command) => execute_leaf(command, ListCommand::execute),
            ProjectCommand::Show(command) => execute_leaf(command, ShowCommand::execute),
            ProjectCommand::Rename(command) => execute_leaf(command, RenameCommand::execute),
            ProjectCommand::Repository(command) => command.execute(),
            ProjectCommand::RunnerPool(command) => command.execute(),
        }
    }
}

impl RepositoryCommand {
    fn execute(self) -> super::CommandResult {
        let Some(command) = self.command else {
            return super::print_help(&[NAME, "repository"]);
        };
        match command {
            RepositorySubcommand::List(command) => {
                execute_leaf(command, RepositoryListCommand::execute)
            }
            RepositorySubcommand::Show(command) => {
                execute_leaf(command, RepositoryShowCommand::execute)
            }
            RepositorySubcommand::Set(command) => {
                execute_leaf(command, RepositorySetCommand::execute)
            }
            RepositorySubcommand::Update(command) => {
                execute_leaf(command, RepositoryUpdateCommand::execute)
            }
            RepositorySubcommand::Detach(command) => {
                execute_leaf(command, RepositoryDetachCommand::execute)
            }
            RepositorySubcommand::Installation(command) => command.execute(),
        }
    }
}

impl InstallationCommand {
    fn execute(self) -> super::CommandResult {
        let Some(command) = self.command else {
            return super::print_help(&[NAME, "repository", "installation"]);
        };
        match command {
            InstallationSubcommand::List(command) => {
                execute_leaf(command, InstallationListCommand::execute)
            }
        }
    }
}

impl RunnerPoolCommand {
    fn execute(self) -> super::CommandResult {
        let Some(command) = self.command else {
            return super::print_help(&[NAME, "runner-pool"]);
        };
        match command {
            RunnerPoolSubcommand::Set(command) => {
                execute_leaf(command, RunnerPoolSetCommand::execute)
            }
            RunnerPoolSubcommand::Remove(command) => {
                execute_leaf(command, RunnerPoolRemoveCommand::execute)
            }
        }
    }
}

// This wrapper supplies project-specific deployment diagnostics while organization
// commands retain their independent organization wording.
// jscpd:ignore-start
fn execute_leaf<T>(
    command: T,
    execute: impl FnOnce(T, &Deployment) -> anyhow::Result<ExitCode>,
) -> super::CommandResult {
    super::execute_deployment_command(
        Some(command),
        &[NAME],
        "configure Scherzo Cloud project access",
        |command, deployment| execute(command, deployment).map_err(Into::into),
    )
}
// jscpd:ignore-end

impl CreateCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let transport_policy = self.options.http.transport_policy();
        let runner_pool_id = match self.pool.as_ref() {
            Some(pool) => match resolve_pool_id(
                deployment,
                transport_policy,
                &self.options.authentication,
                &self.organization,
                pool,
            )? {
                Ok(pool_id) => Some(pool_id),
                Err(failure) => {
                    return super::runner::write_failure(
                        deployment,
                        &failure,
                        &self.options.authentication,
                        self.options.json,
                    );
                }
            },
            None => None,
        };
        let key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate project creation request identity")?;
        let result = with_api(
            deployment,
            transport_policy,
            &self.options.authentication,
            |api| {
                api.create(
                    &self.organization,
                    &key,
                    CreateProjectInput {
                        name: &self.name,
                        installation_id: &self.repository.installation_id,
                        repository_id: &self.repository.repository_id,
                        default_branch: self.repository.default_branch.as_deref(),
                        runner_pool_id: runner_pool_id.as_deref(),
                    },
                )
            },
        )?;
        output::write_project(
            deployment.fingerprint().api_url(),
            result,
            "created",
            "✓ Project created.",
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

// Each read command binds a distinct public projection and machine envelope; keeping
// these mappings explicit makes the user-visible outcome contract reviewable.
// jscpd:ignore-start
impl ListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                api.list(
                    &self.organization,
                    self.pagination.limit,
                    self.pagination.cursor.as_deref(),
                )
            },
        )?;
        output::write_project_list(
            deployment.fingerprint().api_url(),
            result,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl ShowCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.get(&self.project.organization, &self.project.project_id),
        )?;
        output::write_project(
            deployment.fingerprint().api_url(),
            result,
            "found",
            "✓ Project found.",
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl RenameCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate project rename request identity")?;
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                api.rename(
                    &self.project.organization,
                    &self.project.project_id,
                    &key,
                    &self.name,
                )
            },
        )?;
        output::write_project(
            deployment.fingerprint().api_url(),
            result,
            "renamed",
            "✓ Project renamed.",
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}
// jscpd:ignore-end

impl InstallationListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.list_installations(&self.organization),
        )?;
        output::write_installations(
            deployment.fingerprint().api_url(),
            result,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl RepositoryListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.list_repositories(&self.organization, &self.installation_id),
        )?;
        output::write_repositories(
            deployment.fingerprint().api_url(),
            result,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl RepositoryShowCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.get_repository(&self.project.organization, &self.project.project_id),
        )?;
        output::write_repository(
            deployment.fingerprint().api_url(),
            result,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl RepositorySetCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate project repository request identity")?;
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                api.set_repository(
                    &self.project.organization,
                    &self.project.project_id,
                    &key,
                    &self.repository.installation_id,
                    &self.repository.repository_id,
                    self.repository.default_branch.as_deref(),
                )
            },
        )?;
        output::write_project(
            deployment.fingerprint().api_url(),
            result,
            "repository_set",
            "✓ Project repository set.",
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl RepositoryUpdateCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate project repository update request identity")?;
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                api.update_repository(
                    &self.project.organization,
                    &self.project.project_id,
                    &key,
                    &self.default_branch,
                )
            },
        )?;
        output::write_project(
            deployment.fingerprint().api_url(),
            result,
            "repository_updated",
            "✓ Project repository updated.",
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl RepositoryDetachCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate project repository detachment request identity")?;
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.detach_repository(&self.project.organization, &self.project.project_id, &key),
        )?;
        output::write_project(
            deployment.fingerprint().api_url(),
            result,
            "repository_detached",
            "✓ Project repository detached.",
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl RunnerPoolSetCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let transport_policy = self.options.http.transport_policy();
        let runner_pool_id = match resolve_pool_id(
            deployment,
            transport_policy,
            &self.options.authentication,
            &self.project.organization,
            &self.pool,
        )? {
            Ok(pool_id) => pool_id,
            Err(failure) => {
                return super::runner::write_failure(
                    deployment,
                    &failure,
                    &self.options.authentication,
                    self.options.json,
                );
            }
        };
        let key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate project runner pool request identity")?;
        let result = with_api(
            deployment,
            transport_policy,
            &self.options.authentication,
            |api| {
                api.set_runner_pool(
                    &self.project.organization,
                    &self.project.project_id,
                    &key,
                    &runner_pool_id,
                )
            },
        )?;
        output::write_project(
            deployment.fingerprint().api_url(),
            result,
            "runner_pool_set",
            "✓ Project runner pool set.",
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl RunnerPoolRemoveCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate project runner pool removal request identity")?;
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                api.remove_runner_pool(&self.project.organization, &self.project.project_id, &key)
            },
        )?;
        output::write_project(
            deployment.fingerprint().api_url(),
            result,
            "runner_pool_removed",
            "✓ Project runner pool removed.",
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

fn resolve_pool_id(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    authentication: &super::PrincipalAuthenticationArgs,
    organization: &str,
    pool: &PoolArg,
) -> anyhow::Result<Result<String, RunnerFailure>> {
    super::runner::with_api(deployment, transport_policy, authentication, |api| {
        pool.resolve_id(api, organization)
    })
}

// Human-session orchestration stays failure-domain-specific so project protocol
// rejection cannot be confused with a Cloud run outcome.
// jscpd:ignore-start
pub(in crate::cli) fn with_api<T>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    authentication: &super::PrincipalAuthenticationArgs,
    mut operation: impl FnMut(&ProjectApi) -> Result<T, ProjectFailure>,
) -> anyhow::Result<Result<T, ProjectFailure>> {
    let client = HttpClient::new(transport_policy)
        .map_err(|error| anyhow!(error))
        .context("prepare human session networking")?;
    super::execute_selected_api_operation(
        super::principal_api_context(
            &client,
            deployment,
            authentication,
            "acquire human session for project operation",
        ),
        |access_token| {
            let api = ProjectApi::new(
                deployment.fingerprint().api_url(),
                access_token,
                transport_policy,
            )
            .map_err(|error| anyhow!(error))
            .context("prepare project management networking")?;
            Ok(operation(&api))
        },
        ProjectFailure::credential_rejected,
        || ProjectFailure::Unauthenticated,
        ProjectFailure::Unreachable,
    )
}
// jscpd:ignore-end
