use std::io::{self, Write};

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;

use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;
use crate::human_auth::session::{self, LogoutOutcome, RevocationState};

pub(super) const ABOUT: &str = "Sign out of Scherzo Cloud on this device";

// Logout and status intentionally retain command-local output contracts even
// though both expose JSON and transport-policy flags.
// jscpd:ignore-start
#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, help = "Print the sign-out result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::super::HttpOptions,
}
// jscpd:ignore-end

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> super::super::CommandResult {
        self.run(deployment)?;
        Ok(ExitCode::Success)
    }

    fn run(self, deployment: &Deployment) -> anyhow::Result<()> {
        let outcome = session::logout(deployment, self.http.transport_policy())
            .map_err(|error| anyhow!(error))
            .context("sign out human session")?;

        if self.json {
            write_json_result(deployment, &outcome)
        } else {
            write_human_result(&outcome)
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LogoutResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    credential_removed: bool,
    revocation: &'static str,
}

fn write_json_result(deployment: &Deployment, outcome: &LogoutOutcome) -> anyhow::Result<()> {
    let result = LogoutResult {
        schema_version: 1,
        deployment: deployment.fingerprint().api_url(),
        credential_removed: outcome.credential_removed(),
        revocation: revocation_name(outcome.revocation()),
    };
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &result).context("write JSON sign-out result")?;
    writeln!(stdout).context("write sign-out result")
}

fn write_human_result(outcome: &LogoutOutcome) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    match (outcome.credential_removed(), outcome.revocation()) {
        (false, _) => writeln!(
            stdout,
            "You're already signed out of Scherzo Cloud on this device."
        ),
        (true, RevocationState::Confirmed) => {
            writeln!(stdout, "✓ Signed out of Scherzo Cloud.")
        }
        (true, RevocationState::Unconfirmed) => writeln!(
            stdout,
            "✓ Signed out of Scherzo Cloud on this device.\n! Server sign-out wasn't confirmed."
        ),
        (true, RevocationState::NotApplicable) => {
            writeln!(stdout, "✓ Signed out of Scherzo Cloud on this device.")
        }
    }
    .context("write sign-out result")
}

const fn revocation_name(state: RevocationState) -> &'static str {
    match state {
        RevocationState::Confirmed => "confirmed",
        RevocationState::Unconfirmed => "unconfirmed",
        RevocationState::NotApplicable => "not_applicable",
    }
}
