use anyhow::Context;
use clap::Args;

use crate::api::remove_identity;
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

use super::{OutputOptions, output, with_human_session};

pub(super) const ABOUT: &str = "Remove a linked sign-in identity";

// List and remove remain separate Clap leaves because only removal owns a target and
// idempotent mutation, while each leaf retains a distinct result contract.
// jscpd:ignore-start
#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "IDENTITY_ID", help = "Exact linked identity ID")]
    identity_id: String,

    #[command(flatten)]
    options: OutputOptions,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let client = self.options.client()?;
        // jscpd:ignore-end
        let idempotency_key = crate::idempotency::generate_idempotency_key()
            .context("generate identity-removal request identity")?;
        let outcome = with_human_session(&client, deployment, |access_token| {
            remove_identity(
                &client,
                deployment.fingerprint().api_url(),
                access_token,
                &self.identity_id,
                &idempotency_key,
            )
        })?;
        output::write_remove(
            deployment.fingerprint().api_url(),
            &self.identity_id,
            &outcome,
            self.options.json,
        )
    }
}
