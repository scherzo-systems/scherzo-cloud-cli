macro_rules! impl_authenticated_account_outcome {
    ($outcome:ty, $error:ty) => {
        impl super::super::HumanCredentialOutcome for $outcome {
            type Error = $error;

            fn unauthenticated() -> Self {
                Self::Unauthenticated
            }

            fn unreachable(category: crate::api::UnreachableCategory) -> Self {
                Self::Unreachable(category)
            }

            fn is_unauthenticated(&self) -> bool {
                matches!(self, Self::Unauthenticated)
            }

            fn credential_rejected(error: &Self::Error) -> bool {
                error.credential_rejected()
            }
        }
    };
}

mod signup;
mod update;

use anyhow::Context;
use clap::{ArgGroup, Args, Subcommand};

use crate::api::{HttpClient, HttpTransportPolicy, signup_human, update_current_principal};
use crate::human_auth::deployment::Deployment;
use crate::idempotency::generate_idempotency_key;

pub(super) const ABOUT: &str = "Manage your Scherzo Cloud account";
const NAME: &str = "account";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<AccountCommand>,
}

#[derive(Debug, Subcommand)]
enum AccountCommand {
    #[command(about = signup::ABOUT)]
    Signup(SignupCommand),
    #[command(about = update::ABOUT)]
    Update(UpdateCommand),
}

#[derive(Debug, Args)]
struct SignupCommand {
    #[command(flatten)]
    options: LeafOptions,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("display_name_change")
        .required(true)
        .multiple(false)
        .args(["display_name", "clear_display_name"])
))]
struct UpdateCommand {
    #[arg(long, value_name = "NAME", help = "Set your account display name")]
    display_name: Option<String>,

    #[arg(long, help = "Clear your account display name")]
    clear_display_name: bool,

    #[command(flatten)]
    options: LeafOptions,
}

#[derive(Debug, Args)]
struct LeafOptions {
    #[arg(long, help = "Print the account result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::HttpOptions,
}

fn execute_mutation<O>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    request_identity_context: &'static str,
    network_context: &'static str,
    api_context: &'static str,
    mut operation: impl FnMut(&HttpClient, &str, &str, &str) -> Result<O, O::Error>,
) -> anyhow::Result<O>
where
    O: super::HumanCredentialOutcome,
{
    let idempotency_key = generate_idempotency_key().context(request_identity_context)?;
    super::execute_with_human_credential(
        deployment,
        transport_policy,
        network_context,
        api_context,
        |client, api_url, access_token| operation(client, api_url, access_token, &idempotency_key),
    )
}

fn execute_signup(
    command: SignupCommand,
    deployment: &Deployment,
) -> anyhow::Result<crate::exit_code::ExitCode> {
    let outcome = execute_mutation(
        deployment,
        command.options.http.transport_policy(),
        "create signup request identity",
        "prepare signup networking",
        "create Scherzo Cloud account through",
        signup_human,
    )?;
    signup::write_outcome(
        command.options.json,
        deployment.fingerprint().api_url(),
        &outcome,
    )
}

fn execute_update(
    command: UpdateCommand,
    deployment: &Deployment,
) -> anyhow::Result<crate::exit_code::ExitCode> {
    let outcome = execute_mutation(
        deployment,
        command.options.http.transport_policy(),
        "create account update request identity",
        "prepare account update networking",
        "update Scherzo Cloud account through",
        |client, api_url, access_token, idempotency_key| {
            update_current_principal(
                client,
                api_url,
                access_token,
                idempotency_key,
                command.display_name.as_deref(),
            )
        },
    )?;
    update::write_outcome(
        command.options.json,
        command.clear_display_name,
        deployment.fingerprint().api_url(),
        &outcome,
    )
}

// Account and auth retain separate command enums so each family owns its public dispatch surface.
// jscpd:ignore-start
impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        super::execute_deployment_command(
            self.command,
            &[NAME],
            "configure Scherzo Cloud account",
            |command, deployment| match command {
                AccountCommand::Signup(command) => {
                    execute_signup(command, deployment).map_err(Into::into)
                }
                AccountCommand::Update(command) => {
                    execute_update(command, deployment).map_err(Into::into)
                }
            },
        )
    }
}
// jscpd:ignore-end
