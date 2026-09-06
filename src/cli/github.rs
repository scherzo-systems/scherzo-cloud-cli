mod output;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};

use crate::api::{GitHubApi, GitHubFailure, HttpClient, HttpTransportPolicy};
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;
use crate::human_auth::session::{self, RequiredOperation};

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
    #[command(about = "Connect organizations to GitHub")]
    Setup(SetupCommand),
    #[command(about = "Manage GitHub installation bindings")]
    Installation(InstallationCommand),
    #[command(about = "Discover GitHub repositories")]
    Repository(RepositoryCommand),
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
    #[command(about = "List GitHub installation bindings")]
    List(OrganizationTarget),
    #[command(about = "Disconnect a GitHub installation binding")]
    Disconnect(InstallationTarget),
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

#[derive(Debug, Args)]
struct GitHubOptions {
    #[arg(long, help = "Print the GitHub result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::HttpOptions,
}

#[derive(Debug, Args)]
struct OrganizationTarget {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[command(flatten)]
    options: GitHubOptions,
}

#[derive(Debug, Args)]
struct CompleteCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

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
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(value_name = "INSTALLATION", help = "GitHub installation binding ID")]
    installation: String,

    #[command(flatten)]
    options: GitHubOptions,
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
                InstallationTarget::disconnect,
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
        let result = with_api(deployment, self.options.http.transport_policy(), |api| {
            api.begin_setup(&self.organization)
        })?;
        output::write_setup_begin(
            deployment.fingerprint().api_url(),
            &self.organization,
            &result,
            self.options.json,
        )
    }
}

impl CompleteCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(deployment, self.options.http.transport_policy(), |api| {
            api.complete_setup(
                &self.organization,
                &self.setup_session,
                &self.provider_installation_id,
            )
        })?;
        output::write_installation(
            deployment.fingerprint().api_url(),
            &self.organization,
            &result,
            output::InstallationAction::SetupCompleted,
            self.options.json,
        )
    }
}

impl OrganizationTarget {
    fn list_installations(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(deployment, self.options.http.transport_policy(), |api| {
            api.list_installations(&self.organization)
        })?;
        output::write_installation_list(
            deployment.fingerprint().api_url(),
            &self.organization,
            &result,
            self.options.json,
        )
    }
}

impl InstallationTarget {
    fn disconnect(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(deployment, self.options.http.transport_policy(), |api| {
            api.disconnect_installation(&self.organization, &self.installation)
        })?;
        output::write_installation(
            deployment.fingerprint().api_url(),
            &self.organization,
            &result,
            output::InstallationAction::Disconnected,
            self.options.json,
        )
    }
}

impl InstallationTarget {
    fn list_repositories(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(deployment, self.options.http.transport_policy(), |api| {
            api.list_repositories(&self.organization, &self.installation)
        })?;
        output::write_repository_list(
            deployment.fingerprint().api_url(),
            &self.organization,
            &result,
            self.options.json,
        )
    }
}

fn with_api<T>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    mut operation: impl FnMut(&GitHubApi) -> Result<T, GitHubFailure>,
) -> anyhow::Result<Result<T, GitHubFailure>> {
    let session_client = HttpClient::new(transport_policy)
        .map_err(|error| anyhow!(error))
        .context("prepare human session networking")?;
    match session::execute_required(
        &session_client,
        deployment,
        |access_token| {
            let api = GitHubApi::new(
                deployment.fingerprint().api_url(),
                access_token.expose(),
                transport_policy,
            )
            .map_err(|error| anyhow!(error))
            .context("prepare GitHub connection networking")?;
            Ok(operation(&api))
        },
        |result| {
            result.as_ref().is_ok_and(|operation| {
                operation
                    .as_ref()
                    .is_err_and(GitHubFailure::credential_rejected)
            })
        },
    ) {
        Ok(RequiredOperation::Unauthenticated) => Ok(Err(GitHubFailure::Unauthenticated)),
        Ok(RequiredOperation::Completed(result)) => result,
        Err(error) => match error.unreachable_category() {
            Some(category) => Ok(Err(GitHubFailure::Unreachable(category))),
            None => Err(anyhow!(error).context("acquire human session")),
        },
    }
}
