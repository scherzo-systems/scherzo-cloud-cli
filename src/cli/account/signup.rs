// Signup and status have independent output contracts, so their command-local
// adapters stay separate rather than coupling unrelated command behavior.
// jscpd:ignore-start
use std::io::{self, Write};

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;
// jscpd:ignore-end

use crate::api::{HttpClient, HumanPrincipal, SignupError, SignupOutcome, signup_human};
use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::deployment::Deployment;
use crate::human_auth::session::{self, RequiredOperation};
use crate::idempotency::generate_idempotency_key;

use super::super::principal::PrincipalResult;

pub(super) const ABOUT: &str = "Create your Scherzo Cloud account";

// Signup keeps its idempotency and output contracts local while shared session
// acquisition owns refresh, bounded authentication retry, and rejection cleanup.
// jscpd:ignore-start
#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, help = "Print the signup result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::super::HttpOptions,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> super::super::CommandResult {
        self.run(deployment).map_err(Into::into)
    }

    fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let idempotency_key =
            generate_idempotency_key().context("create signup request identity")?;
        // jscpd:ignore-end
        let client = HttpClient::new(self.http.transport_policy())
            .map_err(|error| anyhow!(error))
            .context("prepare signup networking")?;
        let outcome = match session::execute_required(
            &client,
            deployment,
            |access_token| {
                signup_human(
                    &client,
                    deployment.fingerprint().api_url(),
                    access_token,
                    &idempotency_key,
                )
            },
            |outcome| {
                matches!(outcome, Ok(SignupOutcome::Unauthenticated))
                    || outcome
                        .as_ref()
                        .is_err_and(SignupError::credential_rejected)
            },
        ) {
            Ok(RequiredOperation::Unauthenticated) => SignupOutcome::Unauthenticated,
            Ok(RequiredOperation::Completed(outcome)) => {
                outcome.map_err(|error| anyhow!(error)).with_context(|| {
                    format!(
                        "create Scherzo Cloud account through {}",
                        deployment.fingerprint().api_url()
                    )
                })?
            }
            Err(error) => match error.unreachable_category() {
                Some(category) => SignupOutcome::Unreachable(category),
                None => return Err(anyhow!(error).context("acquire human session")),
            },
        };
        self.write_outcome(deployment, &outcome)
    }

    fn write_outcome(
        self,
        deployment: &Deployment,
        outcome: &SignupOutcome,
    ) -> anyhow::Result<ExitCode> {
        if self.json {
            write_json_result(deployment.fingerprint().api_url(), outcome)?;
        } else {
            write_human_result(deployment.fingerprint().api_url(), outcome)?;
        }
        Ok(outcome_class(outcome).exit_code())
    }
}

fn outcome_class(outcome: &SignupOutcome) -> OutcomeClass {
    match outcome {
        SignupOutcome::Authenticated(_) => OutcomeClass::Success,
        SignupOutcome::Unauthenticated => OutcomeClass::Unauthenticated,
        SignupOutcome::Unreachable(category) => super::super::unreachable_outcome_class(*category),
        SignupOutcome::SignupNotPermitted => OutcomeClass::Forbidden,
        SignupOutcome::AlreadyProvisioned | SignupOutcome::IdempotencyConflict => {
            OutcomeClass::GeneralFailure
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SignupResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    #[serde(flatten)]
    body: SignupResultBody<'a>,
}

#[derive(Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum SignupResultBody<'a> {
    Authenticated { principal: PrincipalResult<'a> },
    Unauthenticated,
    SignupNotPermitted,
    AlreadyProvisioned,
    IdempotencyConflict,
    Unreachable { category: &'static str },
}

impl<'a> SignupResult<'a> {
    fn new(deployment: &'a str, outcome: &'a SignupOutcome) -> Self {
        let body = match outcome {
            SignupOutcome::Authenticated(principal) => SignupResultBody::Authenticated {
                principal: PrincipalResult::from_principal(principal),
            },
            SignupOutcome::Unauthenticated => SignupResultBody::Unauthenticated,
            SignupOutcome::SignupNotPermitted => SignupResultBody::SignupNotPermitted,
            SignupOutcome::AlreadyProvisioned => SignupResultBody::AlreadyProvisioned,
            SignupOutcome::IdempotencyConflict => SignupResultBody::IdempotencyConflict,
            SignupOutcome::Unreachable(category) => SignupResultBody::Unreachable {
                category: category.as_str(),
            },
        };
        Self {
            schema_version: 1,
            deployment,
            body,
        }
    }
}

fn write_json_result(deployment: &str, outcome: &SignupOutcome) -> anyhow::Result<()> {
    let result = SignupResult::new(deployment, outcome);
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &result).context("write JSON signup result")?;
    writeln!(stdout).context("write signup result")
}

fn write_human_result(deployment: &str, outcome: &SignupOutcome) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    match outcome {
        SignupOutcome::Authenticated(principal) => {
            write_created_account(&mut stdout, deployment, principal)
        }
        SignupOutcome::Unauthenticated => writeln!(
            stdout,
            "! You're not signed in to Scherzo Cloud.\n\nSign in to create your account:\n  scherzo-cloud auth login"
        ),
        SignupOutcome::SignupNotPermitted => writeln!(
            stdout,
            "! Account signup is not available for this Scherzo Cloud deployment."
        ),
        SignupOutcome::AlreadyProvisioned => writeln!(
            stdout,
            "! This identity already has a Scherzo Cloud account.\n\nRun:\n  scherzo-cloud auth status"
        ),
        SignupOutcome::IdempotencyConflict => writeln!(
            stdout,
            "! Account signup could not be completed because its request conflicted."
        ),
        SignupOutcome::Unreachable(category) => writeln!(
            stdout,
            "! Couldn't confirm Scherzo Cloud account creation ({}).\n\nRun before trying again:\n  scherzo-cloud auth status",
            category.as_str()
        ),
    }
    .context("write signup result")
}

fn write_created_account(
    output: &mut impl Write,
    deployment: &str,
    principal: &HumanPrincipal,
) -> io::Result<()> {
    writeln!(output, "✓ Scherzo Cloud account created.\n")?;
    if let Some(display_name) = &principal.display_name {
        writeln!(output, "  Account:    {display_name}")?;
    }
    writeln!(output, "  Principal:  {}", principal.id)?;
    writeln!(output, "  Deployment: {deployment}")
}
