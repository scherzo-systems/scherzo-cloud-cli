use clap::{ArgGroup, Args};

use crate::api::update_organization;
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

use super::{LeafOptions, output};

pub(super) const ABOUT: &str = "Update a Scherzo Cloud organization";

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("profile")
        .required(true)
        .multiple(true)
        .args(["display_name", "slug"])
))]
pub(super) struct Command {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization_ref: String,

    #[arg(long, help = "Set the organization display name")]
    display_name: Option<String>,

    #[arg(long, help = "Set the exact organization slug")]
    slug: Option<String>,

    // Clap input ownership remains operation-local; shared execution policy lives in LeafOptions.
    // jscpd:ignore-start
    #[command(flatten)]
    options: LeafOptions,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let Self {
            organization_ref,
            display_name,
            slug,
            options,
        } = self;
        // jscpd:ignore-end
        options.execute_mutation(
            deployment,
            |client, api_url, access_token, idempotency_key| {
                update_organization(
                    client,
                    api_url,
                    access_token,
                    &organization_ref,
                    idempotency_key,
                    display_name.as_deref(),
                    slug.as_deref(),
                )
            },
            output::write_update,
        )
    }
}
