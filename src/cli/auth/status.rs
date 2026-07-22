use std::fmt;
use std::io::{self, Write};
use std::process::ExitCode;

use clap::Args;
use serde::Serialize;

use crate::api::{HttpClient, HttpClientError, HumanPrincipal};
use crate::human_auth::deployment::Deployment;
use crate::human_auth::status::{self, AuthenticationState, AuthenticationStatus, StatusError};

pub const ABOUT: &str = "Show your Scherzo Cloud sign-in status";

#[derive(Debug, Args)]
pub struct Command {
    #[arg(long, help = "Print sign-in status as JSON")]
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

    fn run(self, deployment: &Deployment) -> Result<(), CommandError> {
        let client = HttpClient::new().map_err(CommandError::HttpClient)?;
        let status = status::check(&client, deployment).map_err(CommandError::Status)?;
        if self.json {
            write_json_status(&status)
        } else {
            write_human_status(&status)
        }
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

fn write_human_status(status: &AuthenticationStatus) -> Result<(), CommandError> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    match status.state() {
        AuthenticationState::Authenticated(principal) => {
            let account = principal.display_name.as_ref().unwrap_or(&principal.id);
            writeln!(stdout, "✓ Signed in as {account}.").map_err(CommandError::WriteOutput)
        }
        AuthenticationState::SignupRequired { actions } => {
            writeln!(stdout, "✓ Signed in to Scherzo Cloud.").map_err(CommandError::WriteOutput)?;
            writeln!(
                stdout,
                "! Your Scherzo Cloud account still needs to be set up."
            )
            .map_err(CommandError::WriteOutput)?;
            if let Some(actions) = actions {
                for action in actions {
                    serde_json::to_writer(&mut stdout, action).map_err(CommandError::WriteJson)?;
                    writeln!(stdout).map_err(CommandError::WriteOutput)?;
                }
            }
            Ok(())
        }
        AuthenticationState::Unauthenticated => {
            writeln!(stdout, "! You're not signed in to Scherzo Cloud.")
                .map_err(CommandError::WriteOutput)
        }
        AuthenticationState::Unreachable(category) => writeln!(
            stdout,
            "! Couldn't reach Scherzo Cloud ({}).",
            category.as_str()
        )
        .map_err(CommandError::WriteOutput),
    }
}

#[derive(Debug)]
enum CommandError {
    HttpClient(HttpClientError),
    Status(StatusError),
    WriteJson(serde_json::Error),
    WriteOutput(io::Error),
}

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
