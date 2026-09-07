use clap::{Args, Subcommand};

use crate::api::{HttpClient, list_organization_audit_records};
use crate::cli::ContinuationCursor;
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

use super::{LeafOptions, output};

pub(super) const ABOUT: &str = "Work with organization audit records";
const LIST_ABOUT: &str = "List organization audit records";
const LIST_AFTER_HELP: &str = "Authorization:\n  Only an active organization owner can list audit records.\n\nPagination:\n  This command returns one page. Pass --cursor <CURSOR> to continue.\n\nPrivacy:\n  Records with unavailable details retain only record, occurrence, and retention metadata.\n  The command does not infer actor, action, or target details.";

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
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization_ref: crate::cli::OrganizationRef,

    #[arg(
        long,
        value_parser = parse_audit_list_limit,
        help = "Maximum audit records to return (1-100)"
    )]
    limit: Option<u16>,

    #[arg(long, help = "Opaque continuation cursor")]
    cursor: Option<ContinuationCursor>,

    #[command(flatten)]
    options: LeafOptions,
}

fn parse_audit_list_limit(value: &str) -> Result<u16, String> {
    let limit = value
        .parse::<u16>()
        .map_err(|_| "must be an integer from 1 through 100".to_owned())?;
    if (1..=100).contains(&limit) {
        Ok(limit)
    } else {
        Err("must be an integer from 1 through 100".to_owned())
    }
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
                    self.limit,
                    self.cursor.as_deref(),
                )
            },
            output::write_audit_list,
        )
    }
}
