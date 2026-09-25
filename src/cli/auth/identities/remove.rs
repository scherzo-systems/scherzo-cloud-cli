use anyhow::Context;
use clap::Args;

use crate::exit_code::ExitCode;
use scherzo_cloud_api::remove_identity;
use scherzo_cloud_human_auth::Deployment;

use super::{OutputOptions, output, with_principal_credential};

pub(super) const ABOUT: &str = "Remove a linked sign-in identity";

// List and remove remain separate Clap leaves because only removal owns a target and
// idempotent mutation, while each leaf retains a distinct result contract.
// jscpd:ignore-start
#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "IDENTITY_ID", help = "Exact linked identity ID")]
    identity_id: String,

    #[command(flatten)]
    confirmation: super::super::super::ConfirmationArgs,

    #[command(flatten)]
    options: OutputOptions,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let client = self.options.client()?;
        // jscpd:ignore-end
        let idempotency_key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate identity-removal request identity")?;
        let outcome = with_principal_credential(
            &client,
            deployment,
            &self.options.authentication,
            |access_token| {
                remove_identity(
                    &client,
                    deployment.fingerprint().api_url(),
                    access_token,
                    &self.identity_id,
                    &idempotency_key,
                )
            },
        )?;
        output::write_remove(
            deployment.fingerprint().api_url(),
            &self.identity_id,
            &outcome,
            self.options.authentication.kind(),
            self.options.json,
        )
    }
}
