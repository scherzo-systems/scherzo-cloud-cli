use clap::Args;

use crate::api::get_organization;
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

use super::{LeafOptions, output};

pub(super) const ABOUT: &str = "Show a Scherzo Cloud organization";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization_ref: String,

    // Clap input ownership remains operation-local; shared execution policy lives in LeafOptions.
    // jscpd:ignore-start
    #[command(flatten)]
    options: LeafOptions,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let Self {
            organization_ref,
            options,
        } = self;
        // jscpd:ignore-end
        options.execute(
            deployment,
            |client, api_url, access_token| {
                get_organization(client, api_url, access_token, &organization_ref)
            },
            output::write_show,
        )
    }
}
