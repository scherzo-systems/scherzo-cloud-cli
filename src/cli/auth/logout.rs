use std::io::{self, Write};

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;

use crate::exit_code::ExitCode;
use crate::human_auth::credentials::CredentialStore;
use crate::human_auth::deployment::Deployment;

pub(super) const ABOUT: &str = "Sign out of Scherzo Cloud on this device";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, help = "Print the sign-out result as JSON")]
    json: bool,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> super::super::CommandResult {
        self.run(deployment)?;
        Ok(ExitCode::Success)
    }

    fn run(self, deployment: &Deployment) -> anyhow::Result<()> {
        let store = CredentialStore::from_environment()
            .map_err(|error| anyhow!(error))
            .context("access credential store")?;
        let credential_removed = store
            .remove(deployment.fingerprint())
            .map_err(|error| anyhow!(error))
            .context("access credential store")?;

        if self.json {
            write_json_result(deployment, credential_removed)
        } else {
            write_human_result(credential_removed)
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LogoutResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    credential_removed: bool,
}

fn write_json_result(deployment: &Deployment, credential_removed: bool) -> anyhow::Result<()> {
    let result = LogoutResult {
        schema_version: 1,
        deployment: deployment.fingerprint().api_url(),
        credential_removed,
    };
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &result).context("write JSON sign-out result")?;
    writeln!(stdout).context("write sign-out result")
}

fn write_human_result(credential_removed: bool) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    if credential_removed {
        writeln!(stdout, "✓ Signed out of Scherzo Cloud on this device.")
    } else {
        writeln!(
            stdout,
            "You're already signed out of Scherzo Cloud on this device."
        )
    }
    .context("write sign-out result")
}
