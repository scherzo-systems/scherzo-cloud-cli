mod audit;
mod create;
mod leave;
mod list;
mod members;
mod output;
mod show;
mod update;

use anyhow::Context;
use clap::{Args, Subcommand};

use crate::api::{
    CreateOrganizationOutcome, GetOrganizationOutcome, HttpClient,
    ListCurrentPrincipalMembershipsOutcome, ListOrganizationAuditRecordsOutcome,
    ListOrganizationMembershipHistoryOutcome, ListOrganizationMembershipsOutcome,
    MembershipTerminationOutcome, OrganizationError, UpdateOrganizationMembershipOutcome,
    UpdateOrganizationOutcome,
};
use crate::exit_code::ExitCode;
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
    #[command(about = audit::ABOUT)]
    Audit(audit::Command),
    #[command(about = create::ABOUT)]
    Create(create::Command),
    #[command(about = super::deletion::organization_about())]
    Deletion(super::deletion::OrganizationCommand),
    #[command(about = leave::ABOUT)]
    Leave(leave::Command),
    #[command(about = list::ABOUT)]
    List(list::Command),
    #[command(about = "Manage organization invitations")]
    Invitations(super::invitation::OrganizationCommand),
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
        O: super::HumanCredentialOutcome<Error = OrganizationError>,
    {
        let outcome = super::execute_with_human_credential(
            deployment,
            self.http.transport_policy(),
            "prepare organization networking",
            "contact organization API at",
            operation,
        )?;
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
        O: super::HumanCredentialOutcome<Error = OrganizationError>,
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
            Some(OrganizationCommand::Audit(command)) => command.execute(),
            Some(OrganizationCommand::Create(command)) => {
                execute_leaf(command, create::Command::execute)
            }
            Some(OrganizationCommand::Deletion(command)) => command.execute(),
            Some(OrganizationCommand::Leave(command)) => {
                execute_leaf(command, leave::Command::execute)
            }
            Some(OrganizationCommand::List(command)) => {
                execute_leaf(command, list::Command::execute)
            }
            Some(OrganizationCommand::Invitations(command)) => command.execute(),
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

super::impl_organization_human_credential_outcome!(
    CreateOrganizationOutcome,
    GetOrganizationOutcome,
    UpdateOrganizationOutcome,
    ListCurrentPrincipalMembershipsOutcome,
    ListOrganizationAuditRecordsOutcome,
    ListOrganizationMembershipHistoryOutcome,
    ListOrganizationMembershipsOutcome,
    MembershipTerminationOutcome,
    UpdateOrganizationMembershipOutcome,
);
