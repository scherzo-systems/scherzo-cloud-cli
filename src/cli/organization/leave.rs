use clap::Args;

use crate::api::leave_organization;
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

use super::{LeafOptions, output};

pub(super) const ABOUT: &str = "Leave a Scherzo Cloud organization";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization_ref: crate::cli::OrganizationRef,

    #[arg(
        long,
        required = true,
        action = clap::ArgAction::SetTrue,
        help = "Confirm permanent organization departure"
    )]
    yes: bool,

    // Self-leave keeps an operation-local Clap type because its terminal confirmation and
    // bodyless success projection differ from every read and targeted-member command.
    // jscpd:ignore-start
    #[command(flatten)]
    options: LeafOptions,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let organization_ref = self.organization_ref;
        // jscpd:ignore-end
        self.options.execute_mutation(
            deployment,
            |client, api_url, access_token, idempotency_key| {
                leave_organization(
                    client,
                    api_url,
                    access_token,
                    &organization_ref,
                    idempotency_key,
                )
            },
            |deployment, outcome, json| {
                output::write_leave(deployment, &organization_ref, outcome, json)
            },
        )
    }
}
