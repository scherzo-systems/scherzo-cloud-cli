mod assembly;
mod download;
mod list;
mod validate;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};

use crate::api::{ArtifactApi, ArtifactApiError, HttpClient, HttpTransportPolicy};
use crate::human_auth::deployment::Deployment;
use crate::human_auth::session::{self, RequiredOperation};

pub(super) const ABOUT: &str = "Work with portable workflow artifacts";
const NAME: &str = "artifact";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<ArtifactCommand>,
}

#[derive(Debug, Subcommand)]
enum ArtifactCommand {
    #[command(about = download::ABOUT)]
    Download(download::Command),
    #[command(about = list::ABOUT)]
    List(list::Command),
    #[command(about = validate::ABOUT)]
    Validate(validate::Command),
}

#[derive(Debug, Args)]
pub(super) struct RemoteArtifactCommand<O: Args> {
    #[command(flatten)]
    operation: O,

    #[command(flatten)]
    remote: RemoteArtifactOptions,
}

#[derive(Debug, Args)]
struct RemoteArtifactOptions {
    #[command(flatten)]
    run: RunArtifactReference,

    #[command(flatten)]
    http: super::HttpOptions,
}

#[derive(Debug, Args)]
pub(super) struct RunArtifactReference {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: super::OrganizationRef,

    #[arg(
        value_name = "RUN",
        help = "Run identifier containing the Artifact Set"
    )]
    run_id: String,
}

pub(super) trait RemoteArtifactOperation {
    type Output;
    type Error: From<ArtifactApiError>;

    const SESSION_CONTEXT: &'static str;

    fn request(
        &self,
        api: &mut ArtifactApi,
        run: &RunArtifactReference,
    ) -> Result<Self::Output, Self::Error>;
    fn credential_rejected(error: &Self::Error) -> bool;
    fn write_result(
        &self,
        deployment: &str,
        run: &RunArtifactReference,
        result: Result<Self::Output, Self::Error>,
    ) -> anyhow::Result<crate::exit_code::ExitCode>;
}

impl<O: RemoteArtifactOperation + Args> RemoteArtifactCommand<O> {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<crate::exit_code::ExitCode> {
        execute_with_api(
            deployment,
            self.remote.http.transport_policy(),
            O::SESSION_CONTEXT,
            |api| self.operation.request(api, &self.remote.run),
            O::credential_rejected,
            |deployment, result| {
                self.operation
                    .write_result(deployment, &self.remote.run, result)
            },
        )
    }
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        self.command
            .map_or_else(|| super::print_help(&[NAME]), ArtifactCommand::execute)
    }
}

impl ArtifactCommand {
    fn execute(self) -> super::CommandResult {
        match self {
            Self::Download(command) => execute_remote(command),
            Self::List(command) => execute_remote(command),
            Self::Validate(command) => command.execute(),
        }
    }
}

fn execute_remote<O: RemoteArtifactOperation + Args>(
    command: RemoteArtifactCommand<O>,
) -> super::CommandResult {
    super::execute_deployment_leaf(
        command,
        &[NAME],
        "configure Scherzo Cloud Artifact Set access",
        RemoteArtifactCommand::execute,
    )
}

fn execute_with_api<T, E>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    session_context: &'static str,
    operation: impl FnMut(&mut ArtifactApi) -> Result<T, E>,
    credential_rejected: impl Fn(&E) -> bool,
    write_result: impl FnOnce(&str, Result<T, E>) -> anyhow::Result<crate::exit_code::ExitCode>,
) -> anyhow::Result<crate::exit_code::ExitCode>
where
    E: From<ArtifactApiError>,
{
    let result = with_api(
        deployment,
        transport_policy,
        session_context,
        operation,
        credential_rejected,
    )?;
    write_result(deployment.fingerprint().api_url(), result)
}

fn with_api<T, E>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    session_context: &'static str,
    mut operation: impl FnMut(&mut ArtifactApi) -> Result<T, E>,
    credential_rejected: impl Fn(&E) -> bool,
) -> anyhow::Result<Result<T, E>>
where
    E: From<ArtifactApiError>,
{
    let session_client = HttpClient::new(transport_policy)
        .map_err(|error| anyhow!(error))
        .context("prepare Artifact Set networking")?;
    match session::execute_required(
        &session_client,
        deployment,
        |access_token| {
            let mut api = ArtifactApi::new(
                deployment.fingerprint().api_url(),
                access_token.expose(),
                transport_policy,
            )
            .map_err(E::from)?;
            operation(&mut api)
        },
        |result| result.as_ref().is_err_and(&credential_rejected),
    ) {
        Ok(RequiredOperation::Unauthenticated) => {
            Ok(Err(E::from(ArtifactApiError::Unauthenticated)))
        }
        Ok(RequiredOperation::Completed(result)) => Ok(result),
        Err(error) => match error.unreachable_category() {
            Some(category) => Ok(Err(E::from(ArtifactApiError::Unreachable(category)))),
            None => Err(anyhow!(error).context(session_context)),
        },
    }
}
