use std::fmt;
use std::io::{self, Write};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::Args;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::api::{CurrentPrincipalError, HttpClient, HttpClientError, UnreachableCategory};
use crate::human_auth::cancellation::{Cancellation, CancellationError};
use crate::human_auth::credentials::{CredentialError, CredentialStore};
use crate::human_auth::deployment::Deployment;
use crate::human_auth::device_authorization::{
    self, AuthorizationError, AuthorizationLocalError, DeviceAuthorization, IssuedToken, TokenPoll,
};
use crate::human_auth::status::{self, AuthenticationState, AuthenticationStatus, StatusError};

use super::status::{HumanStatusError, StatusResult, write_human_status};

pub const ABOUT: &str = "Sign in to Scherzo Cloud";
const CANCELLED_EXIT_CODE: u8 = 130;
const SLOW_DOWN_INCREMENT: Duration = Duration::from_secs(5);

#[derive(Debug, Args)]
pub struct Command {
    #[arg(long, help = "Emit newline-delimited JSON events")]
    json: bool,

    #[arg(long, help = "Start a new sign-in even if you're already signed in")]
    force: bool,
}

impl Command {
    pub fn execute(self, deployment: &Deployment) -> ExitCode {
        match self.run(deployment) {
            Ok(Completion::Success) => ExitCode::SUCCESS,
            Ok(Completion::Failure) => ExitCode::FAILURE,
            Ok(Completion::Cancelled) => ExitCode::from(CANCELLED_EXIT_CODE),
            Err(error) => {
                eprintln!("Error: {error}");
                ExitCode::FAILURE
            }
        }
    }

    fn run(self, deployment: &Deployment) -> Result<Completion, CommandError> {
        let cancellation = Cancellation::install().map_err(CommandError::Cancellation)?;
        let store = CredentialStore::from_environment().map_err(CommandError::CredentialStore)?;
        let client = HttpClient::new().map_err(CommandError::HttpClient)?;
        let mut output = LoginOutput { json: self.json };

        if self.force {
            // Validate store access and prune an expired selected credential
            // before starting its replacement login.
            store
                .selected(deployment.fingerprint(), OffsetDateTime::now_utc())
                .map_err(CommandError::CredentialStore)?;
        } else {
            let existing_status = status::check(&client, deployment);
            if cancellation.is_cancelled() {
                output.cancelled(deployment)?;
                return Ok(Completion::Cancelled);
            }
            match existing_status {
                Ok(existing) => match existing.state() {
                    AuthenticationState::Authenticated(_)
                    | AuthenticationState::SignupRequired { .. } => {
                        output.status(&existing)?;
                        return Ok(Completion::Success);
                    }
                    AuthenticationState::Unauthenticated => {}
                    AuthenticationState::Unreachable(category) => {
                        output.failed(
                            deployment,
                            FailureOutcome::Unreachable,
                            Phase::ExistingCredentialCheck,
                            Some(*category),
                        )?;
                        return Ok(Completion::Failure);
                    }
                },
                Err(error) => {
                    return handle_status_error(
                        &mut output,
                        deployment,
                        Phase::ExistingCredentialCheck,
                        error,
                    );
                }
            }
        }

        if cancellation.is_cancelled() {
            output.cancelled(deployment)?;
            return Ok(Completion::Cancelled);
        }

        let authorization = match device_authorization::authorize(&client, deployment) {
            Ok(authorization) => authorization,
            Err(error) => {
                if cancellation.is_cancelled() {
                    output.cancelled(deployment)?;
                    return Ok(Completion::Cancelled);
                }
                return handle_authorization_error(
                    &mut output,
                    deployment,
                    Phase::DeviceAuthorization,
                    error,
                );
            }
        };
        if cancellation.is_cancelled() {
            output.cancelled(deployment)?;
            return Ok(Completion::Cancelled);
        }
        let Some(mut schedule) = PollSchedule::new(
            Instant::now(),
            authorization.interval(),
            authorization.expires_in(),
        ) else {
            output.failed(
                deployment,
                FailureOutcome::ProtocolError,
                Phase::DeviceAuthorization,
                None,
            )?;
            return Ok(Completion::Failure);
        };
        let Some(activation_expires_at) = expiration_after(authorization.expires_in()) else {
            output.failed(
                deployment,
                FailureOutcome::ProtocolError,
                Phase::DeviceAuthorization,
                None,
            )?;
            return Ok(Completion::Failure);
        };
        output.activation(deployment, &authorization, activation_expires_at)?;

        loop {
            if cancellation.is_cancelled() {
                output.cancelled(deployment)?;
                return Ok(Completion::Cancelled);
            }
            let Some(wait) = schedule.next_wait(Instant::now()) else {
                output.failed(
                    deployment,
                    FailureOutcome::Expired,
                    Phase::TokenPolling,
                    None,
                )?;
                return Ok(Completion::Failure);
            };
            if cancellation.wait(wait) {
                output.cancelled(deployment)?;
                return Ok(Completion::Cancelled);
            }
            if let Some(completion) =
                polling_interruption(&mut output, deployment, &cancellation, &schedule)?
            {
                return Ok(completion);
            }

            let poll = match device_authorization::poll_token(
                &client,
                deployment,
                authorization.device_code(),
            ) {
                Ok(poll) => poll,
                Err(error) => {
                    if cancellation.is_cancelled() {
                        output.cancelled(deployment)?;
                        return Ok(Completion::Cancelled);
                    }
                    return handle_authorization_error(
                        &mut output,
                        deployment,
                        Phase::TokenPolling,
                        error,
                    );
                }
            };
            if let Some(completion) =
                polling_interruption(&mut output, deployment, &cancellation, &schedule)?
            {
                return Ok(completion);
            }
            match poll {
                TokenPoll::Pending => {}
                TokenPoll::SlowDown => schedule.slow_down(),
                TokenPoll::Denied => {
                    output.failed(
                        deployment,
                        FailureOutcome::Denied,
                        Phase::TokenPolling,
                        None,
                    )?;
                    return Ok(Completion::Failure);
                }
                TokenPoll::Expired => {
                    output.failed(
                        deployment,
                        FailureOutcome::Expired,
                        Phase::TokenPolling,
                        None,
                    )?;
                    return Ok(Completion::Failure);
                }
                TokenPoll::Issued(token) => {
                    return finish_login(
                        &mut output,
                        &client,
                        deployment,
                        &store,
                        &cancellation,
                        token,
                    );
                }
            }
        }
    }
}

fn polling_interruption(
    output: &mut LoginOutput,
    deployment: &Deployment,
    cancellation: &Cancellation,
    schedule: &PollSchedule,
) -> Result<Option<Completion>, CommandError> {
    if cancellation.is_cancelled() {
        output.cancelled(deployment)?;
        return Ok(Some(Completion::Cancelled));
    }
    if schedule.expired(Instant::now()) {
        output.failed(
            deployment,
            FailureOutcome::Expired,
            Phase::TokenPolling,
            None,
        )?;
        return Ok(Some(Completion::Failure));
    }
    Ok(None)
}

fn finish_login(
    output: &mut LoginOutput,
    client: &HttpClient,
    deployment: &Deployment,
    store: &CredentialStore,
    cancellation: &Cancellation,
    token: IssuedToken,
) -> Result<Completion, CommandError> {
    let Some(expires_at) = expiration_after(token.expires_in()) else {
        output.failed(
            deployment,
            FailureOutcome::ProtocolError,
            Phase::TokenPolling,
            None,
        )?;
        return Ok(Completion::Failure);
    };
    if cancellation.is_cancelled() {
        output.cancelled(deployment)?;
        return Ok(Completion::Cancelled);
    }

    // Credential persistence commits the login. Ignore later interrupts so a
    // cancelled result can never conceal a newly stored token.
    store
        .replace(deployment.fingerprint(), token.access_token(), expires_at)
        .map_err(CommandError::CredentialStore)?;

    let status = status::check(client, deployment);
    match status {
        Ok(status) => match status.state() {
            AuthenticationState::Authenticated(_) | AuthenticationState::SignupRequired { .. } => {
                output.status(&status)?;
                Ok(Completion::Success)
            }
            AuthenticationState::Unauthenticated => {
                output.status(&status)?;
                Ok(Completion::Failure)
            }
            AuthenticationState::Unreachable(category) => {
                output.failed(
                    deployment,
                    FailureOutcome::Unreachable,
                    Phase::PrincipalConfirmation,
                    Some(*category),
                )?;
                Ok(Completion::Failure)
            }
        },
        Err(error) => handle_status_error(output, deployment, Phase::PrincipalConfirmation, error),
    }
}

fn handle_status_error(
    output: &mut LoginOutput,
    deployment: &Deployment,
    phase: Phase,
    error: StatusError,
) -> Result<Completion, CommandError> {
    match error {
        StatusError::CredentialStore(error) => Err(CommandError::CredentialStore(error)),
        StatusError::PublicApi(error) if error.is_local() => Err(CommandError::PublicApi(error)),
        StatusError::PublicApi(_) => {
            output.failed(deployment, FailureOutcome::ProtocolError, phase, None)?;
            Ok(Completion::Failure)
        }
    }
}

fn handle_authorization_error(
    output: &mut LoginOutput,
    deployment: &Deployment,
    phase: Phase,
    error: AuthorizationError,
) -> Result<Completion, CommandError> {
    match error {
        AuthorizationError::Local(error) => Err(CommandError::Authorization(error)),
        AuthorizationError::Unreachable(category) => {
            output.failed(
                deployment,
                FailureOutcome::Unreachable,
                phase,
                Some(category),
            )?;
            Ok(Completion::Failure)
        }
        AuthorizationError::Protocol { .. } => {
            output.failed(deployment, FailureOutcome::ProtocolError, phase, None)?;
            Ok(Completion::Failure)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Completion {
    Success,
    Failure,
    Cancelled,
}

struct PollSchedule {
    interval: Duration,
    deadline: Instant,
}

impl PollSchedule {
    fn new(started_at: Instant, interval: Duration, lifetime: Duration) -> Option<Self> {
        Some(Self {
            interval,
            deadline: started_at.checked_add(lifetime)?,
        })
    }

    fn next_wait(&self, now: Instant) -> Option<Duration> {
        let remaining = self.deadline.checked_duration_since(now)?;
        (!remaining.is_zero()).then_some(self.interval.min(remaining))
    }

    fn expired(&self, now: Instant) -> bool {
        now >= self.deadline
    }

    fn slow_down(&mut self) {
        self.interval = self
            .interval
            .checked_add(SLOW_DOWN_INCREMENT)
            .unwrap_or(Duration::MAX);
    }
}

fn expiration_after(duration: Duration) -> Option<OffsetDateTime> {
    let seconds = i64::try_from(duration.as_secs()).ok()?;
    OffsetDateTime::now_utc().checked_add(time::Duration::seconds(seconds))
}

#[derive(Clone, Copy)]
enum FailureOutcome {
    Denied,
    Expired,
    Unreachable,
    ProtocolError,
}

impl FailureOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Denied => "denied",
            Self::Expired => "expired",
            Self::Unreachable => "unreachable",
            Self::ProtocolError => "protocol_error",
        }
    }
}

#[derive(Clone, Copy)]
enum Phase {
    ExistingCredentialCheck,
    DeviceAuthorization,
    TokenPolling,
    PrincipalConfirmation,
}

impl Phase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExistingCredentialCheck => "existing_credential_check",
            Self::DeviceAuthorization => "device_authorization",
            Self::TokenPolling => "token_polling",
            Self::PrincipalConfirmation => "principal_confirmation",
        }
    }
}

struct LoginOutput {
    json: bool,
}

impl LoginOutput {
    fn activation(
        &mut self,
        deployment: &Deployment,
        authorization: &DeviceAuthorization,
        expires_at: OffsetDateTime,
    ) -> Result<(), CommandError> {
        if self.json {
            let expires_at = expires_at
                .format(&Rfc3339)
                .map_err(CommandError::FormatTime)?;
            self.json_line(&ActivationEvent {
                schema_version: 1,
                event: "activation_required",
                deployment: deployment.fingerprint().api_url(),
                verification_uri: authorization.verification_uri(),
                verification_uri_complete: authorization.verification_uri_complete(),
                user_code: authorization.user_code(),
                expires_at: &expires_at,
            })
        } else {
            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            let activation_uri = authorization
                .verification_uri_complete()
                .unwrap_or_else(|| authorization.verification_uri());
            writeln!(stdout, "Sign in to Scherzo Cloud\n").map_err(CommandError::WriteOutput)?;
            writeln!(stdout, "  Open: {activation_uri}").map_err(CommandError::WriteOutput)?;
            writeln!(stdout, "  Code: {}", authorization.user_code())
                .map_err(CommandError::WriteOutput)?;
            writeln!(stdout, "\nWaiting for authorization...\n")
                .map_err(CommandError::WriteOutput)?;
            stdout.flush().map_err(CommandError::WriteOutput)
        }
    }

    fn status(&mut self, status: &AuthenticationStatus) -> Result<(), CommandError> {
        if self.json {
            self.json_line(&StatusEvent {
                schema_version: 1,
                event: "status",
                status: StatusResult::from_status(status),
            })
        } else {
            write_human_status(status).map_err(CommandError::from)
        }
    }

    fn failed(
        &mut self,
        deployment: &Deployment,
        outcome: FailureOutcome,
        phase: Phase,
        category: Option<UnreachableCategory>,
    ) -> Result<(), CommandError> {
        if self.json {
            self.json_line(&FailedEvent {
                schema_version: 1,
                event: "failed",
                deployment: deployment.fingerprint().api_url(),
                outcome: outcome.as_str(),
                phase: phase.as_str(),
                category: category.map(UnreachableCategory::as_str),
            })
        } else {
            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            if let Some(category) = category {
                writeln!(
                    stdout,
                    "Sign-in failed during {}: {} ({}).",
                    phase.as_str(),
                    outcome.as_str(),
                    category.as_str()
                )
            } else {
                writeln!(
                    stdout,
                    "Sign-in failed during {}: {}.",
                    phase.as_str(),
                    outcome.as_str()
                )
            }
            .map_err(CommandError::WriteOutput)
        }
    }

    fn cancelled(&mut self, deployment: &Deployment) -> Result<(), CommandError> {
        if self.json {
            self.json_line(&CancelledEvent {
                schema_version: 1,
                event: "cancelled",
                deployment: deployment.fingerprint().api_url(),
            })
        } else {
            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            writeln!(stdout, "! Sign-in cancelled.").map_err(CommandError::WriteOutput)
        }
    }

    fn json_line<T: Serialize>(&mut self, event: &T) -> Result<(), CommandError> {
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        serde_json::to_writer(&mut stdout, event).map_err(CommandError::WriteJson)?;
        writeln!(stdout).map_err(CommandError::WriteOutput)?;
        stdout.flush().map_err(CommandError::WriteOutput)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActivationEvent<'a> {
    schema_version: u8,
    event: &'static str,
    deployment: &'a str,
    verification_uri: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    verification_uri_complete: Option<&'a str>,
    user_code: &'a str,
    expires_at: &'a str,
}

#[derive(Serialize)]
struct StatusEvent<'a> {
    #[serde(rename = "schemaVersion")]
    schema_version: u8,
    event: &'static str,
    status: StatusResult<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FailedEvent<'a> {
    schema_version: u8,
    event: &'static str,
    deployment: &'a str,
    outcome: &'static str,
    phase: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelledEvent<'a> {
    schema_version: u8,
    event: &'static str,
    deployment: &'a str,
}

#[derive(Debug)]
enum CommandError {
    Cancellation(CancellationError),
    CredentialStore(CredentialError),
    HttpClient(HttpClientError),
    Authorization(AuthorizationLocalError),
    PublicApi(CurrentPrincipalError),
    FormatTime(time::error::Format),
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

impl fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancellation(error) => write!(formatter, "prepare sign-in cancellation: {error}"),
            Self::CredentialStore(error) => write!(formatter, "access credential store: {error}"),
            Self::HttpClient(error) => write!(formatter, "prepare sign-in networking: {error}"),
            Self::Authorization(error) => write!(formatter, "prepare OAuth request: {error}"),
            Self::PublicApi(error) => write!(formatter, "confirm sign-in: {error}"),
            Self::FormatTime(error) => write!(formatter, "format sign-in expiration: {error}"),
            Self::WriteJson(error) => write!(formatter, "write JSON sign-in event: {error}"),
            Self::WriteOutput(error) => write!(formatter, "write sign-in output: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_schedule_honors_interval_and_slow_down() {
        let start = Instant::now();
        let mut schedule =
            PollSchedule::new(start, Duration::from_secs(2), Duration::from_secs(30)).unwrap();

        assert_eq!(schedule.next_wait(start), Some(Duration::from_secs(2)));
        schedule.slow_down();
        assert_eq!(schedule.next_wait(start), Some(Duration::from_secs(7)));
        assert_eq!(
            schedule.next_wait(start + Duration::from_secs(29)),
            Some(Duration::from_secs(1))
        );
        assert!(
            schedule
                .next_wait(start + Duration::from_secs(30))
                .is_none()
        );
    }
}
