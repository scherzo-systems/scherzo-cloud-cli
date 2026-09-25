mod assembly;
mod download;
mod list;
mod validate;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};

use scherzo_cloud_api::{ArtifactApi, ArtifactApiError, HttpClient, HttpTransportPolicy};
use scherzo_cloud_human_auth::Deployment;

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
    common: super::CommonArgs<super::ArtifactJson, super::PrincipalAuthenticationArgs>,
}

impl std::ops::Deref for RemoteArtifactOptions {
    type Target = super::CommonArgs<super::ArtifactJson, super::PrincipalAuthenticationArgs>;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

#[derive(Debug, Args)]
pub(super) struct RunArtifactReference {
    #[arg(
        value_name = super::OrganizationArg::VALUE_NAME,
        help = super::OrganizationArg::HELP
    )]
    organization: super::OrganizationArg,

    #[arg(
        value_name = "RUN",
        help = "Run identifier containing the Artifact Set"
    )]
    run_id: String,
}

pub(super) trait RemoteArtifactError: From<ArtifactApiError> {
    fn credential_rejected(&self) -> bool;
}

impl RemoteArtifactError for ArtifactApiError {
    fn credential_rejected(&self) -> bool {
        self.credential_rejected()
    }
}

pub(super) struct RemoteArtifactResult<'a, T, E> {
    deployment: &'a str,
    run: &'a RunArtifactReference,
    authentication: super::PrincipalAuthenticationKind,
    result: Result<T, E>,
    json: bool,
}

pub(super) trait RemoteArtifactOperation {
    type Output;
    type Error: RemoteArtifactError;

    const SESSION_CONTEXT: &'static str;

    fn request(
        &self,
        api: &mut ArtifactApi,
        run: &RunArtifactReference,
    ) -> Result<Self::Output, Self::Error>;
    fn write_result(
        &self,
        output: RemoteArtifactResult<'_, Self::Output, Self::Error>,
    ) -> anyhow::Result<crate::exit_code::ExitCode>;
}

impl<O: RemoteArtifactOperation + Args> RemoteArtifactCommand<O> {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<crate::exit_code::ExitCode> {
        execute_with_api(
            deployment,
            self.remote.http.transport_policy(),
            &self.remote.authentication,
            O::SESSION_CONTEXT,
            |api| self.operation.request(api, &self.remote.run),
            RemoteArtifactError::credential_rejected,
            |deployment, result| {
                self.operation.write_result(RemoteArtifactResult {
                    deployment,
                    run: &self.remote.run,
                    authentication: self.remote.authentication.kind(),
                    result,
                    json: self.remote.json,
                })
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
    authentication: &super::PrincipalAuthenticationArgs,
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
        authentication,
        session_context,
        operation,
        credential_rejected,
    )?;
    write_result(deployment.fingerprint().api_url(), result)
}

fn with_api<T, E>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    authentication: &super::PrincipalAuthenticationArgs,
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
    super::execute_selected_api_operation(
        super::principal_api_context(&session_client, deployment, authentication, session_context),
        |access_token| {
            let mut api = match ArtifactApi::new(
                deployment.fingerprint().api_url(),
                access_token,
                transport_policy,
            ) {
                Ok(api) => api,
                Err(error) => return Ok(Err(E::from(error))),
            };
            Ok(operation(&mut api))
        },
        credential_rejected,
        || E::from(ArtifactApiError::Unauthenticated),
        |category| E::from(ArtifactApiError::Unreachable(category)),
    )
}
