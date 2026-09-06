use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use crate::api::{HumanPrincipal, UpdateProfileError, UpdateProfileOutcome};
use crate::exit_code::{ExitCode, OutcomeClass};

use super::super::principal::PrincipalResult;

pub(super) const ABOUT: &str = "Update your Scherzo Cloud account";

impl_authenticated_account_outcome!(UpdateProfileOutcome, UpdateProfileError);

pub(super) fn write_outcome(
    json: bool,
    clear_display_name: bool,
    deployment: &str,
    outcome: &UpdateProfileOutcome,
) -> anyhow::Result<ExitCode> {
    let operation = if clear_display_name {
        DisplayNameOperation::Clear
    } else {
        DisplayNameOperation::Set
    };
    if json {
        write_json_result(deployment, operation, outcome)?;
    } else {
        write_human_result(deployment, operation, outcome)?;
    }
    Ok(outcome_class(outcome).exit_code())
}

#[derive(Clone, Copy)]
enum DisplayNameOperation {
    Set,
    Clear,
}

impl DisplayNameOperation {
    const fn outcome(self) -> &'static str {
        match self {
            Self::Set => "set",
            Self::Clear => "cleared",
        }
    }
}

fn outcome_class(outcome: &UpdateProfileOutcome) -> OutcomeClass {
    match outcome {
        UpdateProfileOutcome::Updated(_) => OutcomeClass::Success,
        UpdateProfileOutcome::Unauthenticated => OutcomeClass::Unauthenticated,
        UpdateProfileOutcome::Forbidden => OutcomeClass::Forbidden,
        UpdateProfileOutcome::Unreachable(category) => {
            super::super::unreachable_outcome_class(*category)
        }
        UpdateProfileOutcome::InvalidDisplayName
        | UpdateProfileOutcome::IdempotencyConflict
        | UpdateProfileOutcome::RequestTooLarge
        | UpdateProfileOutcome::UnsupportedMediaType => OutcomeClass::GeneralFailure,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdateResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    #[serde(flatten)]
    body: UpdateResultBody<'a>,
}

#[derive(Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum UpdateResultBody<'a> {
    Set { principal: PrincipalResult<'a> },
    Cleared { principal: PrincipalResult<'a> },
    InvalidDisplayName,
    Unauthenticated,
    Forbidden,
    IdempotencyConflict,
    RequestTooLarge,
    UnsupportedMediaType,
    Unreachable { category: &'static str },
}

impl<'a> UpdateResult<'a> {
    fn new(
        deployment: &'a str,
        operation: DisplayNameOperation,
        outcome: &'a UpdateProfileOutcome,
    ) -> Self {
        let body = match outcome {
            UpdateProfileOutcome::Updated(principal) => match operation {
                DisplayNameOperation::Set => UpdateResultBody::Set {
                    principal: PrincipalResult::from_principal(principal),
                },
                DisplayNameOperation::Clear => UpdateResultBody::Cleared {
                    principal: PrincipalResult::from_principal(principal),
                },
            },
            UpdateProfileOutcome::InvalidDisplayName => UpdateResultBody::InvalidDisplayName,
            UpdateProfileOutcome::Unauthenticated => UpdateResultBody::Unauthenticated,
            UpdateProfileOutcome::Forbidden => UpdateResultBody::Forbidden,
            UpdateProfileOutcome::IdempotencyConflict => UpdateResultBody::IdempotencyConflict,
            UpdateProfileOutcome::RequestTooLarge => UpdateResultBody::RequestTooLarge,
            UpdateProfileOutcome::UnsupportedMediaType => UpdateResultBody::UnsupportedMediaType,
            UpdateProfileOutcome::Unreachable(category) => UpdateResultBody::Unreachable {
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

fn write_json_result(
    deployment: &str,
    operation: DisplayNameOperation,
    outcome: &UpdateProfileOutcome,
) -> anyhow::Result<()> {
    let result = UpdateResult::new(deployment, operation, outcome);
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &result)
        .context("write JSON account update result")?;
    writeln!(stdout).context("write account update result")
}

fn write_human_result(
    deployment: &str,
    operation: DisplayNameOperation,
    outcome: &UpdateProfileOutcome,
) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    match outcome {
        UpdateProfileOutcome::Updated(principal) => {
            write_updated_account(&mut stdout, deployment, operation, principal)
        }
        UpdateProfileOutcome::InvalidDisplayName => writeln!(
            stdout,
            "! Your display name was rejected.\n\nUse a name that normalizes to 1 through 200 Unicode scalar values and contains no control characters."
        ),
        UpdateProfileOutcome::Unauthenticated => writeln!(
            stdout,
            "! You're not signed in to Scherzo Cloud.\n\nSign in to update your account:\n  scherzo-cloud auth login"
        ),
        UpdateProfileOutcome::Forbidden => writeln!(
            stdout,
            "! Your account is not permitted to update its display name.\n\nNo self-service remedy is available."
        ),
        UpdateProfileOutcome::IdempotencyConflict => writeln!(
            stdout,
            "! Your account update request conflicted with another request.\n\nRun the command again to create a new request."
        ),
        UpdateProfileOutcome::RequestTooLarge => writeln!(
            stdout,
            "! Your account update request is too large.\n\nUse a shorter display name."
        ),
        UpdateProfileOutcome::UnsupportedMediaType => writeln!(
            stdout,
            "! The deployment rejected the account update request format.\n\nUse a CLI version supported by this deployment."
        ),
        UpdateProfileOutcome::Unreachable(category) => writeln!(
            stdout,
            "! Your account display name update is unconfirmed ({}).\n\nCheck your account before trying again:\n  scherzo-cloud auth status",
            category.as_str()
        ),
    }
    .context("write account update result")
}

fn write_updated_account(
    output: &mut impl Write,
    deployment: &str,
    operation: DisplayNameOperation,
    principal: &HumanPrincipal,
) -> io::Result<()> {
    writeln!(
        output,
        "✓ Your account display name is {}.\n",
        operation.outcome()
    )?;
    if let Some(display_name) = &principal.display_name {
        writeln!(output, "  Display name: {display_name}")?;
    }
    writeln!(output, "  Principal:    {}", principal.id)?;
    writeln!(output, "  Deployment:   {deployment}")
}
