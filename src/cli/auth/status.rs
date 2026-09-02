use std::io::{self, Write};

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;

use crate::api::HttpClient;
use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::deployment::Deployment;
use crate::human_auth::status::{self, AuthenticationState, AuthenticationStatus};

use super::super::principal::PrincipalResult;

pub(super) const ABOUT: &str = "Show your Scherzo Cloud sign-in status";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, help = "Print sign-in status as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::super::HttpOptions,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> super::super::CommandResult {
        self.run(deployment).map_err(Into::into)
    }

    fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let client = HttpClient::new(self.http.transport_policy())
            .map_err(|error| anyhow!(error))
            .context("prepare status networking")?;
        let status = status::check(&client, deployment)
            .map_err(|error| anyhow!(error))
            .with_context(|| {
                format!(
                    "check sign-in status through {}",
                    deployment.fingerprint().api_url()
                )
            })?;
        let outcome = match status.state() {
            AuthenticationState::Authenticated(_) | AuthenticationState::SignupRequired { .. } => {
                OutcomeClass::Success
            }
            AuthenticationState::Unauthenticated => OutcomeClass::Unauthenticated,
            AuthenticationState::Unreachable(category) => {
                super::super::unreachable_outcome_class(*category)
            }
        };
        if self.json {
            write_json_status(&status)?;
        } else {
            write_human_status(&status)?;
        }
        Ok(outcome.exit_code())
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
        #[serde(skip_serializing_if = "Option::is_none")]
        actions: Option<&'a [serde_json::Value]>,
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

impl<'a> StatusResult<'a> {
    pub(super) fn from_status(status: &'a AuthenticationStatus) -> Self {
        let body = match status.state() {
            AuthenticationState::Authenticated(authenticated) => StatusBody::Authenticated {
                deployment: status.deployment(),
                principal: PrincipalResult::from_principal(&authenticated.principal),
                actions: authenticated.actions.as_deref(),
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

fn write_json_status(status: &AuthenticationStatus) -> anyhow::Result<()> {
    let result = StatusResult::from_status(status);
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &result).context("write JSON sign-in status")?;
    writeln!(stdout).context("write sign-in status")
}

pub(super) fn write_human_status(status: &AuthenticationStatus) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    match status.state() {
        AuthenticationState::Authenticated(authenticated) => {
            let account = authenticated
                .principal
                .display_name
                .as_ref()
                .unwrap_or(&authenticated.principal.id);
            writeln!(stdout, "✓ Signed in as {account}.").context("write sign-in status")?;
            write_human_actions(&mut stdout, authenticated.actions.as_deref())
        }
        AuthenticationState::SignupRequired { actions } => {
            writeln!(stdout, "✓ Signed in to Scherzo Cloud.").context("write sign-in status")?;
            writeln!(
                stdout,
                "! Your Scherzo Cloud account still needs to be set up."
            )
            .context("write sign-in status")?;
            write_human_actions(&mut stdout, actions.as_deref())
        }
        AuthenticationState::Unauthenticated => {
            writeln!(stdout, "! You're not signed in to Scherzo Cloud.")
                .context("write sign-in status")
        }
        AuthenticationState::Unreachable(category) => writeln!(
            stdout,
            "! Couldn't reach Scherzo Cloud ({}).",
            category.as_str()
        )
        .context("write sign-in status"),
    }
}

fn write_human_actions(
    output: &mut impl Write,
    actions: Option<&[serde_json::Value]>,
) -> anyhow::Result<()> {
    if let Some(actions) = actions {
        for action in actions {
            serde_json::to_writer(&mut *output, action).context("write sign-in status action")?;
            writeln!(output).context("write sign-in status")?;
        }
    }
    Ok(())
}
