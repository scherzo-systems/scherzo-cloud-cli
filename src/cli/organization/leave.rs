use clap::Args;

use crate::cli::OrganizationArg;
use crate::exit_code::ExitCode;
use scherzo_cloud_api::leave_organization;
use scherzo_cloud_human_auth::Deployment;

use super::{LeafOptions, output};

pub(super) const ABOUT: &str = "Leave a Scherzo Cloud organization";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization_ref: OrganizationArg,

    #[command(flatten)]
    confirmation: super::super::ConfirmationArgs,

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
            |deployment, outcome, authentication, json| {
                output::write_leave(deployment, &organization_ref, outcome, authentication, json)
            },
        )
    }
}
