use std::process::ExitCode;

use clap::Args;

use crate::api::{create_organization, generate_idempotency_key};
use crate::human_auth::deployment::Deployment;

use super::{CommandError, LeafOptions, output};

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
    pub(super) fn execute(self, deployment: &Deployment) -> Result<ExitCode, CommandError> {
        let Self {
            display_name,
            slug,
            options,
        } = self;
        // jscpd:ignore-end
        options.execute(
            deployment,
            |client, api_url, access_token| {
                let idempotency_key = generate_idempotency_key().map_err(CommandError::Random)?;
                create_organization(
                    client,
                    api_url,
                    access_token,
                    &idempotency_key,
                    &display_name,
                    slug.as_deref(),
                )
                .map_err(CommandError::Organization)
            },
            output::write_create,
        )
    }
}
