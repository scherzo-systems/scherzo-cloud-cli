use clap::{Args, Subcommand, builder::NonEmptyStringValueParser};

use crate::api::list_organization_memberships;
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

use super::{LeafOptions, output};

pub(super) const ABOUT: &str = "Manage Scherzo Cloud organization members";
const LIST_ABOUT: &str = "List organization members";

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
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::super::execute_deployment_command(
            self.command,
            &["organization", "members"],
            "configure Scherzo Cloud organization access",
            |command, deployment| match command {
                MembersCommand::List(command) => command.execute(deployment).map_err(Into::into),
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
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
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
            },
            output::write_members_list,
        )
    }
}
