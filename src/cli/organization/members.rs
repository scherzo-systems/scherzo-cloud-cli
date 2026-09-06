use clap::{Args, Subcommand, ValueEnum};

use crate::api::{
    HttpClient, MembershipRole, OrganizationError, end_organization_membership,
    list_organization_membership_history, list_organization_memberships,
    update_organization_membership_role,
};
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

use super::{LeafOptions, output};
use crate::cli::PaginationArgs;

pub(super) const ABOUT: &str = "Manage Scherzo Cloud organization members";
const LIST_ABOUT: &str = "List organization members";
const HISTORY_ABOUT: &str = "List organization membership history";
const UPDATE_ABOUT: &str = "Update an organization member's role";
const REMOVE_ABOUT: &str = "Remove an organization member";
const HISTORY_AFTER_HELP: &str = "Authorization:\n  Only an active organization owner can list membership history.\n\nPagination:\n  This command returns one page. Pass --cursor <CURSOR> to continue.";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<MembersCommand>,
}

#[derive(Debug, Subcommand)]
enum MembersCommand {
    #[command(about = LIST_ABOUT)]
    List(PageCommand),
    #[command(about = HISTORY_ABOUT, after_help = HISTORY_AFTER_HELP)]
    History(PageCommand),
    #[command(about = UPDATE_ABOUT)]
    Update(UpdateCommand),
    #[command(about = REMOVE_ABOUT)]
    Remove(RemoveCommand),
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::super::execute_deployment_command(
            self.command,
            &["organization", "members"],
            "configure Scherzo Cloud organization access",
            |command, deployment| match command {
                MembersCommand::List(command) => command
                    .execute(
                        deployment,
                        |client, api_url, access_token, organization_ref, pagination| {
                            list_organization_memberships(
                                client,
                                api_url,
                                access_token,
                                organization_ref,
                                pagination.limit,
                                pagination.cursor.as_deref(),
                            )
                        },
                        output::write_members_list,
                    )
                    .map_err(Into::into),
                MembersCommand::History(command) => command
                    .execute(
                        deployment,
                        |client, api_url, access_token, organization_ref, pagination| {
                            list_organization_membership_history(
                                client,
                                api_url,
                                access_token,
                                organization_ref,
                                pagination.limit,
                                pagination.cursor.as_deref(),
                            )
                        },
                        output::write_members_history,
                    )
                    .map_err(Into::into),
                MembersCommand::Update(command) => command.execute(deployment).map_err(Into::into),
                MembersCommand::Remove(command) => command.execute(deployment).map_err(Into::into),
            },
        )
    }
}

#[derive(Debug, Args)]
struct PageCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization_ref: crate::cli::OrganizationRef,

    #[command(flatten)]
    pagination: PaginationArgs,

    #[command(flatten)]
    options: LeafOptions,
}

impl PageCommand {
    fn execute<O>(
        self,
        deployment: &Deployment,
        mut operation: impl FnMut(
            &HttpClient,
            &str,
            &str,
            &str,
            &PaginationArgs,
        ) -> Result<O, OrganizationError>,
        write: impl FnOnce(&str, &O, bool) -> anyhow::Result<ExitCode>,
    ) -> anyhow::Result<ExitCode>
    where
        O: super::super::HumanCredentialOutcome<Error = OrganizationError>,
    {
        self.options.execute(
            deployment,
            |client, api_url, access_token| {
                operation(
                    client,
                    api_url,
                    access_token,
                    &self.organization_ref,
                    &self.pagination,
                )
            },
            write,
        )
    }
}

#[derive(Debug, Args)]
struct MembershipTarget {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization_ref: crate::cli::OrganizationRef,

    #[arg(
        value_name = "MEMBERSHIP",
        value_parser = parse_membership_id,
        help = "Exact membership ID"
    )]
    membership_id: String,
}

fn parse_membership_id(value: &str) -> Result<String, String> {
    if crate::public_id::valid_typed_id(value, "mem_") {
        Ok(value.to_owned())
    } else {
        Err("membership must be an exact mem_ identifier".to_owned())
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum RoleArgument {
    Owner,
    Member,
}

impl From<RoleArgument> for MembershipRole {
    fn from(value: RoleArgument) -> Self {
        match value {
            RoleArgument::Owner => Self::Owner,
            RoleArgument::Member => Self::Member,
        }
    }
}

#[derive(Debug, Args)]
struct UpdateCommand {
    #[command(flatten)]
    target: MembershipTarget,

    #[arg(long, value_enum, help = "Set the organization role")]
    role: RoleArgument,

    #[command(flatten)]
    options: LeafOptions,
}

impl UpdateCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let Self {
            target,
            role,
            options,
        } = self;
        options.execute_mutation(
            deployment,
            |client, api_url, access_token, idempotency_key| {
                update_organization_membership_role(
                    client,
                    api_url,
                    access_token,
                    &target.organization_ref,
                    &target.membership_id,
                    idempotency_key,
                    role.into(),
                )
            },
            output::write_members_update,
        )
    }
}

#[derive(Debug, Args)]
struct RemoveCommand {
    #[command(flatten)]
    target: MembershipTarget,

    #[arg(
        long,
        required = true,
        action = clap::ArgAction::SetTrue,
        help = "Confirm permanent membership removal"
    )]
    yes: bool,

    #[command(flatten)]
    options: LeafOptions,
}

impl RemoveCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let Self {
            target,
            yes: _,
            options,
        } = self;
        options.execute_mutation(
            deployment,
            |client, api_url, access_token, idempotency_key| {
                end_organization_membership(
                    client,
                    api_url,
                    access_token,
                    &target.organization_ref,
                    &target.membership_id,
                    idempotency_key,
                )
            },
            |deployment, outcome, json| {
                output::write_member_removal(
                    deployment,
                    &target.organization_ref,
                    &target.membership_id,
                    outcome,
                    json,
                )
            },
        )
    }
}
