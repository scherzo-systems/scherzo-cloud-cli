use std::fmt;
use std::io::{self, Write};
use std::process::ExitCode;

use clap::Args;
use serde::Serialize;

use crate::api::{HttpClient, HttpClientError, HumanPrincipal};
use crate::human_auth::deployment::Deployment;
use crate::human_auth::status::{self, AuthenticationState, AuthenticationStatus, StatusError};

pub const ABOUT: &str = "Show your Scherzo Cloud sign-in status";
const UNAUTHENTICATED_EXIT_CODE: u8 = 2;
const UNREACHABLE_EXIT_CODE: u8 = 3;

#[derive(Debug, Args)]
pub struct Command {
    #[arg(long, help = "Print sign-in status as JSON")]
    json: bool,
}

impl Command {
    pub fn execute(self, deployment: &Deployment) -> ExitCode {
        match self.run(deployment) {
            Ok(exit_code) => exit_code,
            Err(error) => {
                eprintln!("Error: {error}");
                ExitCode::FAILURE
            }
        }
    }

    fn run(self, deployment: &Deployment) -> Result<ExitCode, CommandError> {
        let client = HttpClient::new().map_err(CommandError::HttpClient)?;
        let status = status::check(&client, deployment).map_err(CommandError::Status)?;
        let exit_code = match status.state() {
            AuthenticationState::Authenticated(_) | AuthenticationState::SignupRequired { .. } => {
                ExitCode::SUCCESS
            }
            AuthenticationState::Unauthenticated => ExitCode::from(UNAUTHENTICATED_EXIT_CODE),
            AuthenticationState::Unreachable(_) => ExitCode::from(UNREACHABLE_EXIT_CODE),
        };
        if self.json {
            write_json_status(&status)?;
        } else {
            write_human_status(&status).map_err(CommandError::from)?;
        }
        Ok(exit_code)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StatusResult<'a> {
    schema_version: u8,
    #[serde(flatten)]
    body: StatusBody<'a>,
}

#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum StatusBody<'a> {
    Authenticated {
        deployment: &'a str,
        principal: PrincipalResult<'a>,
    },
    SignupRequired {
        deployment: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        actions: Option<&'a [serde_json::Value]>,
    },
    Unauthenticated {
        deployment: &'a str,
    },
    Unreachable {
        deployment: &'a str,
        category: &'static str,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PrincipalResult<'a> {
    id: &'a str,
    r#type: &'static str,
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<&'a str>,
}

impl<'a> PrincipalResult<'a> {
    fn from_principal(principal: &'a HumanPrincipal) -> Self {
        Self {
            id: &principal.id,
            r#type: "human",
            state: "active",
            display_name: principal.display_name.as_deref(),
        }
    }
}

impl<'a> StatusResult<'a> {
    pub(super) fn from_status(status: &'a AuthenticationStatus) -> Self {
        let body = match status.state() {
            AuthenticationState::Authenticated(principal) => StatusBody::Authenticated {
                deployment: status.deployment(),
                principal: PrincipalResult::from_principal(principal),
            },
            AuthenticationState::SignupRequired { actions } => StatusBody::SignupRequired {
                deployment: status.deployment(),
                actions: actions.as_deref(),
            },
            AuthenticationState::Unauthenticated => StatusBody::Unauthenticated {
                deployment: status.deployment(),
            },
            AuthenticationState::Unreachable(category) => StatusBody::Unreachable {
                deployment: status.deployment(),
                category: category.as_str(),
            },
        };
        Self {
            schema_version: 1,
            body,
        }
    }
}

fn write_json_status(status: &AuthenticationStatus) -> Result<(), CommandError> {
    let result = StatusResult::from_status(status);
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &result).map_err(CommandError::WriteJson)?;
    writeln!(stdout).map_err(CommandError::WriteOutput)
}

pub(super) fn write_human_status(status: &AuthenticationStatus) -> Result<(), HumanStatusError> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    match status.state() {
        AuthenticationState::Authenticated(principal) => {
            let account = principal.display_name.as_ref().unwrap_or(&principal.id);
            writeln!(stdout, "✓ Signed in as {account}.").map_err(HumanStatusError::Output)
        }
        AuthenticationState::SignupRequired { actions } => {
            writeln!(stdout, "✓ Signed in to Scherzo Cloud.").map_err(HumanStatusError::Output)?;
            writeln!(
                stdout,
                "! Your Scherzo Cloud account still needs to be set up."
            )
            .map_err(HumanStatusError::Output)?;
            if let Some(actions) = actions {
                for action in actions {
                    serde_json::to_writer(&mut stdout, action).map_err(HumanStatusError::Json)?;
                    writeln!(stdout).map_err(HumanStatusError::Output)?;
                }
            }
            Ok(())
        }
        AuthenticationState::Unauthenticated => {
            writeln!(stdout, "! You're not signed in to Scherzo Cloud.")
                .map_err(HumanStatusError::Output)
        }
        AuthenticationState::Unreachable(category) => writeln!(
            stdout,
            "! Couldn't reach Scherzo Cloud ({}).",
            category.as_str()
        )
        .map_err(HumanStatusError::Output),
    }
}

pub(super) enum HumanStatusError {
    Json(serde_json::Error),
    Output(io::Error),
}

// Keep this command-local adapter explicit even though login maps the same
// human-status errors into its larger command error type.
// jscpd:ignore-start
#[derive(Debug)]
enum CommandError {
    HttpClient(HttpClientError),
    Status(StatusError),
    WriteJson(serde_json::Error),
    WriteOutput(io::Error),
}

impl From<HumanStatusError> for CommandError {
    fn from(error: HumanStatusError) -> Self {
        match error {
            HumanStatusError::Json(error) => Self::WriteJson(error),
            HumanStatusError::Output(error) => Self::WriteOutput(error),
        }
    }
}
// jscpd:ignore-end

impl fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HttpClient(error) => write!(formatter, "prepare status networking: {error}"),
            Self::Status(error) => write!(formatter, "check sign-in status: {error}"),
            Self::WriteJson(error) => write!(formatter, "write JSON sign-in status: {error}"),
            Self::WriteOutput(error) => write!(formatter, "write sign-in status: {error}"),
        }
    }
}
