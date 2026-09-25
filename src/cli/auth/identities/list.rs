use clap::Args;

use crate::exit_code::ExitCode;
use scherzo_cloud_api::list_identities;
use scherzo_cloud_human_auth::Deployment;

use super::{OutputOptions, output, with_principal_credential};
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
        let outcome = with_principal_credential(
            &client,
            deployment,
            &self.options.authentication,
            |access_token| {
                list_identities(
                    &client,
                    deployment.fingerprint().api_url(),
                    access_token,
                    self.pagination.limit,
                    self.pagination.cursor.as_deref(),
                )
            },
        )?;
        output::write_list(
            deployment.fingerprint().api_url(),
            &outcome,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}
