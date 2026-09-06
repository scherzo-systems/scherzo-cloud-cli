use clap::Args;

use crate::api::list_identities;
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

use super::{OutputOptions, output, with_human_session};
use crate::cli::PaginationArgs;

pub(super) const ABOUT: &str = "List linked sign-in identities";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    pagination: PaginationArgs,

    #[command(flatten)]
    options: OutputOptions,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let client = self.options.client()?;
        let outcome = with_human_session(&client, deployment, |access_token| {
            list_identities(
                &client,
                deployment.fingerprint().api_url(),
                access_token,
                self.pagination.limit,
                self.pagination.cursor.as_deref(),
            )
        })?;
        output::write_list(
            deployment.fingerprint().api_url(),
            &outcome,
            self.options.json,
        )
    }
}
