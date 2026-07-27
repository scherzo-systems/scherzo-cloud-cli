use std::process::ExitCode;

use clap::{Args, Subcommand, builder::NonEmptyStringValueParser};

use crate::api::list_organization_memberships;
use crate::human_auth::deployment::Deployment;

use super::{CommandError, LeafOptions, output};

pub(super) const ABOUT: &str = "Manage Scherzo Cloud organization members";
const LIST_ABOUT: &str = "List one page of organization members";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<MembersCommand>,
}

#[derive(Debug, Subcommand)]
enum MembersCommand {
    #[command(about = LIST_ABOUT)]
    List(ListCommand),
}

impl Command {
    pub(super) fn execute(self) -> ExitCode {
        super::super::execute_deployment_command(
            self.command,
            &["organization", "members"],
            "configure Scherzo Cloud organization access",
            |command, deployment| {
                super::super::finish_command(match command {
                    MembersCommand::List(command) => command.execute(deployment),
                })
            },
        )
    }
}

#[derive(Debug, Args)]
struct ListCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization_ref: String,

    #[arg(
        long,
        value_parser = clap::value_parser!(u16).range(1..=200),
        help = "Maximum members to return (1-200)"
    )]
    limit: Option<u16>,

    #[arg(
        long,
        value_parser = NonEmptyStringValueParser::new(),
        help = "Opaque continuation cursor"
    )]
    cursor: Option<String>,

    // Clap input ownership remains operation-local; shared execution policy lives in LeafOptions.
    // jscpd:ignore-start
    #[command(flatten)]
    options: LeafOptions,
}

impl ListCommand {
    fn execute(self, deployment: &Deployment) -> Result<ExitCode, CommandError> {
        let Self {
            organization_ref,
            limit,
            cursor,
            options,
        } = self;
        // jscpd:ignore-end
        options.execute(
            deployment,
            |client, api_url, access_token| {
                list_organization_memberships(
                    client,
                    api_url,
                    access_token,
                    &organization_ref,
                    limit,
                    cursor.as_deref(),
                )
                .map_err(CommandError::Organization)
            },
            output::write_members_list,
        )
    }
}
