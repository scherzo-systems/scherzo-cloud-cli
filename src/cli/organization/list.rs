use clap::Args;

use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

use super::LeafOptions;

pub(super) const ABOUT: &str = "List your Scherzo Cloud organization memberships";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    pagination: super::super::PaginationArgs,

    #[command(flatten)]
    options: LeafOptions,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = super::super::project::with_api(
            deployment,
            self.options.http.transport_policy(),
            |api| {
                api.list_organization_memberships(
                    self.pagination.limit,
                    self.pagination.cursor.as_deref(),
                )
            },
        )?;
        super::super::project::write_organization_list(
            deployment.fingerprint().api_url(),
            result,
            self.options.json,
        )
    }
}
