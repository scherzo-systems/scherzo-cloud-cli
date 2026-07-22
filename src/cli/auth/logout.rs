use std::fmt;
use std::io::{self, Write};
use std::process::ExitCode;

use clap::Args;
use serde::Serialize;

use crate::human_auth::credentials::{CredentialError, CredentialStore};
use crate::human_auth::deployment::Deployment;

pub const ABOUT: &str = "Sign out of Scherzo Cloud on this device";

#[derive(Debug, Args)]
pub struct Command {
    #[arg(long, help = "Print the sign-out result as JSON")]
    json: bool,
}

impl Command {
    pub fn execute(self, deployment: &Deployment) -> ExitCode {
        match self.run(deployment) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("Error: {error}");
                ExitCode::FAILURE
            }
        }
    }

    fn run(self, deployment: &Deployment) -> Result<(), LogoutError> {
        let store = CredentialStore::from_environment().map_err(LogoutError::CredentialStore)?;
        let credential_removed = store
            .remove(deployment.fingerprint())
            .map_err(LogoutError::CredentialStore)?;

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

fn write_json_result(deployment: &Deployment, credential_removed: bool) -> Result<(), LogoutError> {
    let result = LogoutResult {
        schema_version: 1,
        deployment: deployment.fingerprint().api_url(),
        credential_removed,
    };
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &result).map_err(LogoutError::WriteJson)?;
    writeln!(stdout).map_err(LogoutError::WriteOutput)
}

fn write_human_result(credential_removed: bool) -> Result<(), LogoutError> {
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
    .map_err(LogoutError::WriteOutput)
}

#[derive(Debug)]
enum LogoutError {
    CredentialStore(CredentialError),
    WriteJson(serde_json::Error),
    WriteOutput(io::Error),
}

impl fmt::Display for LogoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CredentialStore(error) => write!(formatter, "access credential store: {error}"),
            Self::WriteJson(error) => write!(formatter, "write JSON sign-out result: {error}"),
            Self::WriteOutput(error) => write!(formatter, "write sign-out result: {error}"),
        }
    }
}
