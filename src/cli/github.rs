mod output;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};

use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;
use scherzo_cloud_api::{GitHubApi, GitHubFailure, HttpClient, HttpTransportPolicy};

use super::{InstallationArg, OrganizationArg};

pub(super) const ABOUT: &str = "Manage GitHub connections";
const NAME: &str = "github";
const ERROR_CONTEXT: &str = "configure Scherzo Cloud GitHub access";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<GitHubCommand>,
}

#[derive(Debug, Subcommand)]
enum GitHubCommand {
    #[command(about = "Manage GitHub installation bindings")]
    Installation(InstallationCommand),
    #[command(about = "Work with GitHub repositories")]
    Repository(RepositoryCommand),
    #[command(about = "Manage GitHub setup for organizations")]
    Setup(SetupCommand),
}

#[derive(Debug, Args)]
struct SetupCommand {
    #[command(subcommand)]
    command: Option<SetupLeaf>,
}

#[derive(Debug, Subcommand)]
enum SetupLeaf {
    #[command(about = "Begin browser-based GitHub setup")]
    Begin(OrganizationTarget),
    #[command(about = "Complete browser-based GitHub setup")]
    Complete(CompleteCommand),
}

#[derive(Debug, Args)]
struct InstallationCommand {
    #[command(subcommand)]
    command: Option<InstallationLeaf>,
}

#[derive(Debug, Subcommand)]
enum InstallationLeaf {
    #[command(about = "Disconnect a GitHub installation binding")]
    Disconnect(InstallationDisconnectCommand),
    #[command(about = "List GitHub installation bindings")]
    List(OrganizationTarget),
}

#[derive(Debug, Args)]
struct RepositoryCommand {
    #[command(subcommand)]
    command: Option<RepositoryLeaf>,
}

#[derive(Debug, Subcommand)]
enum RepositoryLeaf {
    #[command(about = "List repositories authorized for an installation")]
    List(InstallationTarget),
}

type GitHubOptions = super::CommonArgs<super::GithubJson, super::PrincipalAuthenticationArgs>;

#[derive(Debug, Args)]
struct OrganizationTarget {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[command(flatten)]
    options: GitHubOptions,
}

#[derive(Debug, Args)]
struct CompleteCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(
        value_name = "SETUP_SESSION",
        help = "GitHub setup session ID returned by setup begin"
    )]
    setup_session: String,

    #[arg(
        long,
        value_name = "INSTALLATION_ID",
        help = "Decimal GitHub installation ID returned after browser setup"
    )]
    provider_installation_id: String,

    #[command(flatten)]
    options: GitHubOptions,
}

#[derive(Debug, Args)]
struct InstallationTarget {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(value_name = InstallationArg::VALUE_NAME, help = InstallationArg::HELP)]
    installation: InstallationArg,

    #[command(flatten)]
    options: GitHubOptions,
}

#[derive(Debug, Args)]
struct InstallationDisconnectCommand {
    #[command(flatten)]
    target: InstallationTarget,

    #[command(flatten)]
    confirmation: super::ConfirmationArgs,
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(GitHubCommand::Setup(command)) => command.execute(),
            Some(GitHubCommand::Installation(command)) => command.execute(),
            Some(GitHubCommand::Repository(command)) => command.execute(),
        }
    }
}

impl SetupCommand {
    fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME, "setup"]),
            Some(SetupLeaf::Begin(command)) => super::execute_deployment_leaf(
                command,
                &[NAME],
                ERROR_CONTEXT,
                OrganizationTarget::begin_setup,
            ),
            Some(SetupLeaf::Complete(command)) => super::execute_deployment_leaf(
                command,
                &[NAME],
                ERROR_CONTEXT,
                CompleteCommand::execute,
            ),
        }
    }
}

impl InstallationCommand {
    fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME, "installation"]),
            Some(InstallationLeaf::List(command)) => super::execute_deployment_leaf(
                command,
                &[NAME],
                ERROR_CONTEXT,
                OrganizationTarget::list_installations,
            ),
            Some(InstallationLeaf::Disconnect(command)) => super::execute_deployment_leaf(
                command,
                &[NAME],
                ERROR_CONTEXT,
                InstallationDisconnectCommand::disconnect,
            ),
        }
    }
}

impl RepositoryCommand {
    fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME, "repository"]),
            Some(RepositoryLeaf::List(command)) => super::execute_deployment_leaf(
                command,
                &[NAME],
                ERROR_CONTEXT,
                InstallationTarget::list_repositories,
            ),
        }
    }
}

impl OrganizationTarget {
    fn begin_setup(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.begin_setup(&self.organization),
        )?;
        output::write_setup_begin(
            deployment.fingerprint().api_url(),
            &self.organization,
            &result,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl CompleteCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        // GitHub setup keeps its multi-field completion request explicit rather than sharing
        // project repository mutation plumbing with a different failure and output contract.
        // jscpd:ignore-start
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                api.complete_setup(
                    &self.organization,
                    &self.setup_session,
                    &self.provider_installation_id,
                )
            },
        )?;
        // jscpd:ignore-end
        output::write_installation(
            deployment.fingerprint().api_url(),
            &self.organization,
            &result,
            output::InstallationAction::SetupCompleted,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl OrganizationTarget {
    fn list_installations(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.list_installations(&self.organization),
        )?;
        output::write_installation_list(
            deployment.fingerprint().api_url(),
            &self.organization,
            &result,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

impl InstallationDisconnectCommand {
    fn disconnect(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let target = self.target;
        let result = with_api(
            deployment,
            target.options.http.transport_policy(),
            &target.options.authentication,
            |api| api.disconnect_installation(&target.organization, &target.installation),
        )?;
        output::write_installation(
            deployment.fingerprint().api_url(),
            &target.organization,
            &result,
            output::InstallationAction::Disconnected,
            target.options.authentication.kind(),
            target.options.json,
        )
    }
}

impl InstallationTarget {
    fn list_repositories(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.list_repositories(&self.organization, &self.installation),
        )?;
        output::write_repository_list(
            deployment.fingerprint().api_url(),
            &self.organization,
            &result,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}

fn with_api<T>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    authentication: &super::PrincipalAuthenticationArgs,
    mut operation: impl FnMut(&GitHubApi) -> Result<T, GitHubFailure>,
) -> anyhow::Result<Result<T, GitHubFailure>> {
    let session_client = HttpClient::new(transport_policy)
        .map_err(|error| anyhow!(error))
        .context("prepare human session networking")?;
    super::execute_selected_api_operation(
        super::principal_api_context(
            &session_client,
            deployment,
            authentication,
            "acquire human session",
        ),
        |access_token| {
            let api = GitHubApi::new(
                deployment.fingerprint().api_url(),
                access_token,
                transport_policy,
            )
            .map_err(|error| anyhow!(error))
            .context("prepare GitHub connection networking")?;
            Ok(operation(&api))
        },
        GitHubFailure::credential_rejected,
        || GitHubFailure::Unauthenticated,
        GitHubFailure::Unreachable,
    )
}
