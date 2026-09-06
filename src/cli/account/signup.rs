use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use crate::api::{HumanPrincipal, SignupError, SignupOutcome};
use crate::exit_code::{ExitCode, OutcomeClass};

use super::super::principal::PrincipalResult;

pub(super) const ABOUT: &str = "Create your Scherzo Cloud account";

impl_authenticated_account_outcome!(SignupOutcome, SignupError);

pub(super) fn write_outcome(
    json: bool,
    deployment: &str,
    outcome: &SignupOutcome,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json_result(deployment, outcome)?;
    } else {
        write_human_result(deployment, outcome)?;
    }
    Ok(outcome_class(outcome).exit_code())
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
