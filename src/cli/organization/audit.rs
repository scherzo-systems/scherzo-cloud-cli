use clap::{Args, Subcommand};

use crate::cli::OrganizationArg;
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;
use scherzo_cloud_api::{HttpClient, list_organization_audit_records};

use super::{LeafOptions, output};

pub(super) const ABOUT: &str = "Work with organization audit records";
const LIST_ABOUT: &str = "List organization audit records";
const LIST_AFTER_HELP: &str = "Authorization:\n  Only an active organization owner can list audit records.\n\nPrivacy:\n  Records with unavailable details retain only record, occurrence, and retention metadata.\n  The command does not infer actor, action, or target details.";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<AuditCommand>,
}

#[derive(Debug, Subcommand)]
enum AuditCommand {
    #[command(about = LIST_ABOUT, after_help = LIST_AFTER_HELP)]
    List(ListCommand),
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::super::execute_deployment_command(
            self.command,
            &["organization", "audit"],
            "configure Scherzo Cloud organization audit access",
            |command, deployment| match command {
                AuditCommand::List(command) => command.execute(deployment).map_err(Into::into),
            },
        )
    }
}

#[derive(Debug, Args)]
struct ListCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization_ref: OrganizationArg,

    #[command(flatten)]
    pagination: crate::cli::PaginationArgs<100>,

    #[command(flatten)]
    options: LeafOptions,
}

impl ListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        self.options.execute(
            deployment,
            |client: &HttpClient, api_url, access_token| {
                list_organization_audit_records(
                    client,
                    api_url,
                    access_token,
                    &self.organization_ref,
                    self.pagination.limit,
                    self.pagination.cursor.as_deref(),
                )
            },
            output::write_audit_list,
        )
    }
}
