use std::fmt;
use std::io::{self, Write};
use std::process::ExitCode;

use clap::Args;
use serde::Serialize;
use time::OffsetDateTime;

use crate::api::{
    HttpClient, HttpClientError, HumanPrincipal, SignupError, SignupOutcome, signup_human,
};
use crate::human_auth::credentials::{CredentialError, CredentialStore};
use crate::human_auth::deployment::Deployment;

use super::super::principal::PrincipalResult;

pub const ABOUT: &str = "Create your Scherzo Cloud account";
const UNAUTHENTICATED_EXIT_CODE: u8 = 2;
const UNREACHABLE_EXIT_CODE: u8 = 3;
const RANDOM_KEY_BYTES: usize = 32;
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

// Signup owns credential mutation while status is read-only; keeping these
// command adapters local makes their different cleanup and output paths explicit.
// jscpd:ignore-start
#[derive(Debug, Args)]
pub struct Command {
    #[arg(long, help = "Print the signup result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::super::HttpOptions,
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
        let store = CredentialStore::from_environment().map_err(CommandError::CredentialStore)?;
        // jscpd:ignore-end
        let Some(credential) = store
            .selected(deployment.fingerprint(), OffsetDateTime::now_utc())
            .map_err(CommandError::CredentialStore)?
        else {
            return self.write_outcome(deployment, &SignupOutcome::Unauthenticated);
        };
        let idempotency_key = generate_idempotency_key().map_err(CommandError::Random)?;
        let client =
            HttpClient::new(self.http.transport_policy()).map_err(CommandError::HttpClient)?;
        let outcome = signup_human(
            &client,
            deployment.fingerprint().api_url(),
            credential.access_token(),
            &idempotency_key,
        );

        let credential_rejected = matches!(&outcome, Ok(SignupOutcome::Unauthenticated))
            || outcome
                .as_ref()
                .is_err_and(SignupError::credential_rejected);
        if credential_rejected {
            store
                .remove_if_access_token_matches(deployment.fingerprint(), credential.access_token())
                .map_err(CommandError::CredentialStore)?;
        }

        let outcome = outcome.map_err(CommandError::Signup)?;
        self.write_outcome(deployment, &outcome)
    }

    fn write_outcome(
        self,
        deployment: &Deployment,
        outcome: &SignupOutcome,
    ) -> Result<ExitCode, CommandError> {
        if self.json {
            write_json_result(deployment.fingerprint().api_url(), outcome)?;
        } else {
            write_human_result(deployment.fingerprint().api_url(), outcome)?;
        }
        Ok(exit_code(outcome))
    }
}

fn generate_idempotency_key() -> Result<String, getrandom::Error> {
    let mut random = [0_u8; RANDOM_KEY_BYTES];
    getrandom::fill(&mut random)?;
    let mut key = String::with_capacity(RANDOM_KEY_BYTES * 2);
    for byte in random {
        key.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        key.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    Ok(key)
}

fn exit_code(outcome: &SignupOutcome) -> ExitCode {
    match outcome {
        SignupOutcome::Authenticated(_) => ExitCode::SUCCESS,
        SignupOutcome::Unauthenticated => ExitCode::from(UNAUTHENTICATED_EXIT_CODE),
        SignupOutcome::Unreachable(_) => ExitCode::from(UNREACHABLE_EXIT_CODE),
        SignupOutcome::SignupNotPermitted
        | SignupOutcome::AlreadyProvisioned
        | SignupOutcome::IdempotencyConflict => ExitCode::FAILURE,
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

fn write_json_result(deployment: &str, outcome: &SignupOutcome) -> Result<(), CommandError> {
    let result = SignupResult::new(deployment, outcome);
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &result).map_err(CommandError::WriteJson)?;
    writeln!(stdout).map_err(CommandError::WriteOutput)
}

fn write_human_result(deployment: &str, outcome: &SignupOutcome) -> Result<(), CommandError> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    match outcome {
        SignupOutcome::Authenticated(principal) => {
            write_created_account(&mut stdout, deployment, principal)
        }
        SignupOutcome::Unauthenticated => writeln!(
            stdout,
            "! You must sign in before creating a Scherzo Cloud account.\n\nRun:\n  scherzo-cloud auth login"
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
    .map_err(CommandError::WriteOutput)
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

#[derive(Debug)]
enum CommandError {
    CredentialStore(CredentialError),
    HttpClient(HttpClientError),
    Random(getrandom::Error),
    Signup(SignupError),
    WriteJson(serde_json::Error),
    WriteOutput(io::Error),
}

impl fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CredentialStore(error) => write!(formatter, "access credential store: {error}"),
            Self::HttpClient(error) => write!(formatter, "prepare signup networking: {error}"),
            Self::Random(error) => write!(formatter, "create signup request identity: {error}"),
            Self::Signup(error) => write!(formatter, "create Scherzo Cloud account: {error}"),
            Self::WriteJson(error) => write!(formatter, "write JSON signup result: {error}"),
            Self::WriteOutput(error) => write!(formatter, "write signup result: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::generate_idempotency_key;

    #[test]
    fn idempotency_keys_are_opaque_visible_ascii() {
        let first = generate_idempotency_key().expect("random key should be available");
        let second = generate_idempotency_key().expect("random key should be available");

        assert_eq!(first.len(), 64);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(first, second);
    }
}
