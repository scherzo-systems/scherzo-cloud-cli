mod link;
mod list;
mod output;
mod remove;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};

use crate::api::{
    CommonIdentityFailure, HttpClient, IdentityApiError, LinkIdentityOutcome,
    ListIdentitiesOutcome, RemoveIdentityOutcome, UnreachableCategory,
};
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;
use crate::human_auth::session::{
    self, BoundRequiredOperation, LocalCredentialState, RequiredOperationWithBinding,
    SessionBinding,
};

pub(super) const ABOUT: &str = "Manage your linked sign-in identities";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<IdentityCommand>,
}

#[derive(Debug, Subcommand)]
enum IdentityCommand {
    #[command(about = list::ABOUT)]
    List(list::Command),
    #[command(about = link::ABOUT)]
    Link(link::Command),
    #[command(about = remove::ABOUT)]
    Remove(remove::Command),
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::super::execute_deployment_command(
            self.command,
            &["auth", "identities"],
            "configure Scherzo Cloud identity access",
            |command, deployment| match command {
                IdentityCommand::List(command) => {
                    execute_leaf(command, deployment, list::Command::run)
                }
                IdentityCommand::Link(command) => command.execute(deployment),
                IdentityCommand::Remove(command) => {
                    execute_leaf(command, deployment, remove::Command::run)
                }
            },
        )
    }
}

fn execute_leaf<T>(
    command: T,
    deployment: &Deployment,
    execute: impl FnOnce(T, &Deployment) -> anyhow::Result<ExitCode>,
) -> super::super::CommandResult {
    execute(command, deployment).map_err(Into::into)
}

#[derive(Debug, Args)]
struct OutputOptions {
    #[arg(long, help = "Print the identity result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::super::HttpOptions,
}

impl OutputOptions {
    fn client(&self) -> anyhow::Result<HttpClient> {
        HttpClient::new(self.http.transport_policy())
            .map_err(|error| anyhow!(error))
            .context("prepare identity networking")
    }
}

trait HumanIdentityOutcome: Sized {
    fn unauthenticated() -> Self;
    fn unreachable(category: UnreachableCategory) -> Self;
    fn is_unauthenticated(&self) -> bool;
}

macro_rules! impl_human_identity_outcome {
    ($($outcome:ty),+ $(,)?) => {
        $(
            impl HumanIdentityOutcome for $outcome {
                fn unauthenticated() -> Self {
                    Self::Common(CommonIdentityFailure::Unauthenticated)
                }

                fn unreachable(category: UnreachableCategory) -> Self {
                    Self::Common(CommonIdentityFailure::Unreachable(category))
                }

                fn is_unauthenticated(&self) -> bool {
                    matches!(
                        self,
                        Self::Common(CommonIdentityFailure::Unauthenticated)
                    )
                }
            }
        )+
    };
}

impl_human_identity_outcome!(
    ListIdentitiesOutcome,
    LinkIdentityOutcome,
    RemoveIdentityOutcome,
);

fn with_human_session<O>(
    client: &HttpClient,
    deployment: &Deployment,
    mut operation: impl FnMut(&str) -> Result<O, IdentityApiError>,
) -> anyhow::Result<O>
where
    O: HumanIdentityOutcome,
{
    super::super::execute_human_api_operation(
        client,
        deployment,
        |access_token| operation(access_token),
        credential_rejected::<O>,
        super::super::HumanApiOutcomeAdapters {
            unauthenticated: O::unauthenticated,
            unreachable: O::unreachable,
            operation_error: identity_api_error,
        },
        identity_api_context(deployment),
    )
}

fn with_human_session_binding<O>(
    client: &HttpClient,
    deployment: &Deployment,
    mut operation: impl FnMut(&str) -> Result<O, IdentityApiError>,
) -> anyhow::Result<(O, Option<SessionBinding>)>
where
    O: HumanIdentityOutcome,
{
    match session::execute_required_with_binding(
        client,
        deployment,
        |access_token| operation(access_token.expose()),
        credential_rejected::<O>,
    ) {
        Ok(RequiredOperationWithBinding::Completed { result, binding }) => result
            .map(|outcome| (outcome, Some(binding)))
            .map_err(identity_api_error)
            .context(identity_api_context(deployment)),
        Ok(RequiredOperationWithBinding::Unauthenticated) => Ok((O::unauthenticated(), None)),
        Err(error) => match error.unreachable_category() {
            Some(category) => Ok((O::unreachable(category), None)),
            None => Err(anyhow!(error).context("acquire human session")),
        },
    }
}

enum BoundHumanSession<O> {
    Outcome {
        outcome: O,
        credential_state: LocalCredentialState,
    },
    ActingSessionChanged,
}

fn with_bound_human_session<O>(
    client: &HttpClient,
    deployment: &Deployment,
    binding: &SessionBinding,
    mut operation: impl FnMut(&str) -> Result<O, IdentityApiError>,
) -> anyhow::Result<BoundHumanSession<O>>
where
    O: HumanIdentityOutcome,
{
    match session::execute_bound_required(
        client,
        deployment,
        binding,
        |access_token| operation(access_token.expose()),
        credential_rejected::<O>,
    ) {
        Ok(BoundRequiredOperation::Completed {
            result,
            credential_state,
        }) => result
            .map(|outcome| BoundHumanSession::Outcome {
                outcome,
                credential_state,
            })
            .map_err(identity_api_error)
            .context(identity_api_context(deployment)),
        Ok(BoundRequiredOperation::Unauthenticated { credential_state }) => {
            Ok(BoundHumanSession::Outcome {
                outcome: O::unauthenticated(),
                credential_state,
            })
        }
        Ok(BoundRequiredOperation::ActingSessionChanged) => {
            Ok(BoundHumanSession::ActingSessionChanged)
        }
        Err(error) => match error.unreachable_category() {
            Some(category) => Ok(BoundHumanSession::Outcome {
                outcome: O::unreachable(category),
                credential_state: LocalCredentialState::Retained,
            }),
            None => Err(anyhow!(error).context("acquire bound human session")),
        },
    }
}

fn credential_rejected<O: HumanIdentityOutcome>(result: &Result<O, IdentityApiError>) -> bool {
    result.as_ref().is_ok_and(O::is_unauthenticated)
        || result
            .as_ref()
            .is_err_and(IdentityApiError::credential_rejected)
}

fn identity_api_context(deployment: &Deployment) -> String {
    format!(
        "contact identity API at {}",
        deployment.fingerprint().api_url()
    )
}

fn identity_api_error(error: IdentityApiError) -> anyhow::Error {
    anyhow!(error)
}
