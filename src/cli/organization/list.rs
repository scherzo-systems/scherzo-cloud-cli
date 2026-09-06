use clap::Args;

use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

use super::LeafOptions;

pub(super) const ABOUT: &str = "List your Scherzo Cloud organization memberships";
const AFTER_HELP: &str = "Visibility:\n  Organization names and slugs appear only for active memberships in active\n  organizations. Historical rows retain organization IDs and lifecycle states.\n\nPagination:\n  This command returns one page. Pass --cursor <CURSOR> to continue.";

#[derive(Debug, Args)]
#[command(after_help = AFTER_HELP)]
pub(super) struct Command {
    #[command(flatten)]
    pagination: super::super::PaginationArgs,

    #[command(flatten)]
    options: LeafOptions,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        self.options.execute(
            deployment,
            |client, api_url, access_token| {
                crate::api::list_current_principal_memberships(
                    client,
                    api_url,
                    access_token,
                    self.pagination.limit,
                    self.pagination.cursor.as_deref(),
                )
            },
            super::output::write_list,
        )
    }
}
