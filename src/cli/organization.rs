mod create;
mod list;
mod members;
mod output;
mod show;
mod update;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};

use crate::api::{
    CommonOrganizationFailure, CreateOrganizationOutcome, GetOrganizationOutcome, HttpClient,
    HttpTransportPolicy, ListOrganizationMembershipsOutcome, OrganizationError,
    UpdateOrganizationOutcome,
};
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;
use crate::human_auth::session::{self, RequiredOperation};

pub(super) const ABOUT: &str = "Manage Scherzo Cloud organizations";
const NAME: &str = "organization";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<OrganizationCommand>,
}

#[derive(Debug, Subcommand)]
enum OrganizationCommand {
    #[command(about = create::ABOUT)]
    Create(create::Command),
    #[command(about = list::ABOUT)]
    List(list::Command),
    #[command(about = show::ABOUT)]
    Show(show::Command),
    #[command(about = update::ABOUT)]
    Update(update::Command),
    #[command(about = members::ABOUT)]
    Members(members::Command),
}

#[derive(Debug, Args)]
struct LeafOptions {
    #[arg(long, help = "Print the organization result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::HttpOptions,
}

impl LeafOptions {
    fn execute<O>(
        self,
        deployment: &Deployment,
        operation: impl FnMut(&HttpClient, &str, &str) -> Result<O, OrganizationError>,
        write: impl FnOnce(&str, &O, bool) -> anyhow::Result<ExitCode>,
    ) -> anyhow::Result<ExitCode>
    where
        O: HumanCredentialOutcome,
    {
        let outcome = with_human_credential(deployment, self.http.transport_policy(), operation)?;
        write(deployment.fingerprint().api_url(), &outcome, self.json)
            .context("write organization result")
    }

    fn execute_mutation<O>(
        self,
        deployment: &Deployment,
        mut operation: impl FnMut(&HttpClient, &str, &str, &str) -> Result<O, OrganizationError>,
        write: impl FnOnce(&str, &O, bool) -> anyhow::Result<ExitCode>,
    ) -> anyhow::Result<ExitCode>
    where
        O: HumanCredentialOutcome,
    {
        let idempotency_key = crate::idempotency::generate_idempotency_key()
            .context("generate organization mutation request identity")?;
        self.execute(
            deployment,
            |client, api_url, access_token| {
                operation(client, api_url, access_token, &idempotency_key)
            },
            write,
        )
    }
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(OrganizationCommand::Create(command)) => {
                execute_leaf(command, create::Command::execute)
            }
            Some(OrganizationCommand::List(command)) => {
                execute_leaf(command, list::Command::execute)
            }
            Some(OrganizationCommand::Show(command)) => {
                execute_leaf(command, show::Command::execute)
            }
            Some(OrganizationCommand::Update(command)) => {
                execute_leaf(command, update::Command::execute)
            }
            Some(OrganizationCommand::Members(command)) => command.execute(),
        }
    }
}

fn execute_leaf<T>(
    command: T,
    execute: impl FnOnce(T, &Deployment) -> anyhow::Result<ExitCode>,
) -> super::CommandResult {
    super::execute_deployment_leaf(
        command,
        &[NAME],
        "configure Scherzo Cloud organization access",
        execute,
    )
}

trait HumanCredentialOutcome: Sized {
    fn unauthenticated() -> Self;
    fn unreachable(category: crate::api::UnreachableCategory) -> Self;
    fn is_unauthenticated(&self) -> bool;
}

macro_rules! impl_human_credential_outcome {
    ($($outcome:ty),+ $(,)?) => {
        $(
            impl HumanCredentialOutcome for $outcome {
                fn unauthenticated() -> Self {
                    Self::Common(CommonOrganizationFailure::Unauthenticated)
                }

                fn unreachable(category: crate::api::UnreachableCategory) -> Self {
                    Self::Common(CommonOrganizationFailure::Unreachable(category))
                }

                fn is_unauthenticated(&self) -> bool {
                    matches!(
                        self,
                        Self::Common(CommonOrganizationFailure::Unauthenticated)
                    )
                }
            }
        )+
    };
}

impl_human_credential_outcome!(
    CreateOrganizationOutcome,
    GetOrganizationOutcome,
    UpdateOrganizationOutcome,
    ListOrganizationMembershipsOutcome,
);

fn with_human_credential<O>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    mut operation: impl FnMut(&HttpClient, &str, &str) -> Result<O, OrganizationError>,
) -> anyhow::Result<O>
where
    O: HumanCredentialOutcome,
{
    let client = HttpClient::new(transport_policy)
        .map_err(|error| anyhow!(error))
        .context("prepare organization networking")?;
    match session::execute_required(
        &client,
        deployment,
        |access_token| {
            operation(
                &client,
                deployment.fingerprint().api_url(),
                access_token.expose(),
            )
        },
        |result| {
            result.as_ref().is_ok_and(O::is_unauthenticated)
                || result
                    .as_ref()
                    .is_err_and(OrganizationError::credential_rejected)
        },
    ) {
        Ok(RequiredOperation::Unauthenticated) => Ok(O::unauthenticated()),
        Ok(RequiredOperation::Completed(result)) => {
            result.map_err(|error| anyhow!(error)).with_context(|| {
                format!(
                    "contact organization API at {}",
                    deployment.fingerprint().api_url()
                )
            })
        }
        Err(error) => match error.unreachable_category() {
            Some(category) => Ok(O::unreachable(category)),
            None => Err(anyhow!(error).context("acquire human session")),
        },
    }
}
