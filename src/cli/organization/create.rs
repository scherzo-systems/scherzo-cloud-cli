use clap::Args;

use crate::api::create_organization;
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

use super::{LeafOptions, output};

pub(super) const ABOUT: &str = "Create a Scherzo Cloud organization";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, help = "Set the organization display name")]
    display_name: String,

    #[arg(long, help = "Request an exact organization slug")]
    slug: Option<String>,

    // Clap input ownership remains operation-local; shared execution policy lives in LeafOptions.
    // jscpd:ignore-start
    #[command(flatten)]
    options: LeafOptions,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let Self {
            display_name,
            slug,
            options,
        } = self;
        // jscpd:ignore-end
        options.execute_mutation(
            deployment,
            |client, api_url, access_token, idempotency_key| {
                create_organization(
                    client,
                    api_url,
                    access_token,
                    idempotency_key,
                    &display_name,
                    slug.as_deref(),
                )
            },
            output::write_create,
        )
    }
}
