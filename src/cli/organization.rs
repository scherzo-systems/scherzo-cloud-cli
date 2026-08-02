mod create;
mod members;
mod output;
mod show;
mod update;

use std::fmt;
use std::process::ExitCode;

use clap::{Args, Subcommand};

use crate::api::{
    CommonOrganizationFailure, CreateOrganizationOutcome, GetOrganizationOutcome, HttpClient,
    HttpClientError, HttpTransportPolicy, ListOrganizationMembershipsOutcome, OrganizationError,
    UpdateOrganizationOutcome,
};
use crate::human_auth::credentials::{CredentialError, CredentialStore};
use crate::human_auth::deployment::Deployment;

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
        operation: impl FnOnce(&HttpClient, &str, &str) -> Result<O, CommandError>,
        write: impl FnOnce(&str, &O, bool) -> Result<ExitCode, output::OutputError>,
    ) -> Result<ExitCode, CommandError>
    where
        O: HumanCredentialOutcome,
    {
        let outcome = with_human_credential(deployment, self.http.transport_policy(), operation)?;
        write(deployment.fingerprint().api_url(), &outcome, self.json).map_err(CommandError::Output)
    }

    fn execute_mutation<O>(
        self,
        deployment: &Deployment,
        operation: impl FnOnce(&HttpClient, &str, &str, &str) -> Result<O, CommandError>,
        write: impl FnOnce(&str, &O, bool) -> Result<ExitCode, output::OutputError>,
    ) -> Result<ExitCode, CommandError>
    where
        O: HumanCredentialOutcome,
    {
        self.execute(
            deployment,
            |client, api_url, access_token| {
                let idempotency_key =
                    crate::api::generate_idempotency_key().map_err(CommandError::Random)?;
                operation(client, api_url, access_token, &idempotency_key)
            },
            write,
        )
    }
}

impl Command {
    pub(super) fn execute(self) -> ExitCode {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(OrganizationCommand::Create(command)) => {
                execute_leaf(command, create::Command::execute)
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
    execute: impl FnOnce(T, &Deployment) -> Result<ExitCode, CommandError>,
) -> ExitCode {
    super::execute_deployment_command(
        Some(command),
        &[NAME],
        "configure Scherzo Cloud organization access",
        |command, deployment| super::finish_command(execute(command, deployment)),
    )
}

trait HumanCredentialOutcome: Sized {
    fn unauthenticated() -> Self;
    fn is_unauthenticated(&self) -> bool;
}

macro_rules! impl_human_credential_outcome {
    ($($outcome:ty),+ $(,)?) => {
        $(
            impl HumanCredentialOutcome for $outcome {
                fn unauthenticated() -> Self {
                    Self::Common(CommonOrganizationFailure::Unauthenticated)
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
    operation: impl FnOnce(&HttpClient, &str, &str) -> Result<O, CommandError>,
) -> Result<O, CommandError>
where
    O: HumanCredentialOutcome,
{
    let store = CredentialStore::from_environment().map_err(CommandError::CredentialStore)?;
    let Some(credential) = store
        .selected(deployment.fingerprint(), crate::timing::utc_now())
        .map_err(CommandError::CredentialStore)?
    else {
        return Ok(O::unauthenticated());
    };
    let client = HttpClient::new(transport_policy).map_err(CommandError::HttpClient)?;
    let result = operation(
        &client,
        deployment.fingerprint().api_url(),
        credential.access_token(),
    );
    let credential_rejected = result.as_ref().is_ok_and(O::is_unauthenticated)
        || result
            .as_ref()
            .is_err_and(CommandError::credential_rejected);
    if credential_rejected {
        store
            .remove_if_access_token_matches(deployment.fingerprint(), credential.access_token())
            .map_err(CommandError::CredentialStore)?;
    }
    result
}

#[derive(Debug)]
enum CommandError {
    CredentialStore(CredentialError),
    HttpClient(HttpClientError),
    Random(getrandom::Error),
    Organization(OrganizationError),
    Output(output::OutputError),
}

impl CommandError {
    fn credential_rejected(&self) -> bool {
        matches!(self, Self::Organization(error) if error.credential_rejected())
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CredentialStore(error) => write!(formatter, "access credential store: {error}"),
            Self::HttpClient(error) => {
                write!(formatter, "prepare organization networking: {error}")
            }
            Self::Random(error) => {
                write!(
                    formatter,
                    "generate organization mutation request identity: {error}"
                )
            }
            Self::Organization(error) => write!(formatter, "contact organization API: {error}"),
            Self::Output(error) => write!(formatter, "write organization result: {error}"),
        }
    }
}
