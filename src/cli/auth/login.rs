use std::io::{self, Write};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::api::{HttpClient, UnreachableCategory};
use crate::exit_code::ExitCode;
use crate::human_auth::cancellation::Cancellation;
use crate::human_auth::credentials::CredentialStore;
use crate::human_auth::deployment::Deployment;
use crate::human_auth::device_authorization::{
    self, AuthorizationError, DeviceAuthorization, IssuedToken, TokenPoll,
};
use crate::human_auth::status::{self, AuthenticationState, AuthenticationStatus, StatusError};

use super::status::{StatusResult, write_human_status};

pub(super) const ABOUT: &str = "Sign in to Scherzo Cloud";
const SLOW_DOWN_INCREMENT: Duration = Duration::from_secs(5);

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, help = "Emit newline-delimited JSON events")]
    json: bool,

    // Login keeps its force option and three-way completion mapping local;
    // sharing this command shell with status would couple distinct result contracts.
    // jscpd:ignore-start
    #[arg(long, help = "Start a new sign-in even if you're already signed in")]
    force: bool,

    #[command(flatten)]
    http: super::super::HttpOptions,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> super::super::CommandResult {
        self.run(deployment)
            .map(|completion| match completion {
                Completion::Success => ExitCode::Success,
                Completion::Failure => ExitCode::GeneralFailure,
                Completion::Cancelled => ExitCode::Interrupted,
            })
            .map_err(Into::into)
    }
    // jscpd:ignore-end

    fn run(self, deployment: &Deployment) -> anyhow::Result<Completion> {
        let cancellation = Cancellation::install()
            .map_err(|error| anyhow!(error))
            .context("prepare sign-in cancellation")?;
        let store = CredentialStore::from_environment()
            .map_err(|error| anyhow!(error))
            .context("access credential store")?;
        let client = HttpClient::new(self.http.transport_policy())
            .map_err(|error| anyhow!(error))
            .context("prepare sign-in networking")?;
        let mut output = LoginOutput { json: self.json };

        if self.force {
            // Validate store access and prune an expired selected credential
            // before starting its replacement login.
            store
                .selected(deployment.fingerprint())
                .map_err(|error| anyhow!(error))
                .context("access credential store")?;
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
                        return handle_unreachable(
                            &mut output,
                            deployment,
                            Phase::ExistingCredentialCheck,
                            *category,
                        );
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
            crate::timing::monotonic_now(),
            authorization.interval(),
            authorization.expires_in(),
        ) else {
            return handle_protocol_error(
                &mut output,
                deployment,
                Phase::DeviceAuthorization,
                anyhow!("the device-authorization expiration is out of range"),
            );
        };
        let Some(activation_expires_at) = expiration_after(authorization.expires_in()) else {
            return handle_protocol_error(
                &mut output,
                deployment,
                Phase::DeviceAuthorization,
                anyhow!("the device-authorization expiration is out of range"),
            );
        };
        output.activation(deployment, &authorization, activation_expires_at)?;

        loop {
            if cancellation.is_cancelled() {
                output.cancelled(deployment)?;
                return Ok(Completion::Cancelled);
            }
            let Some(wait) = schedule.next_wait(crate::timing::monotonic_now()) else {
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
) -> anyhow::Result<Option<Completion>> {
    if cancellation.is_cancelled() {
        output.cancelled(deployment)?;
        return Ok(Some(Completion::Cancelled));
    }
    if schedule.expired(crate::timing::monotonic_now()) {
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
) -> anyhow::Result<Completion> {
    let Some(expires_at) = expiration_after(token.expires_in()) else {
        return handle_protocol_error(
            output,
            deployment,
            Phase::TokenPolling,
            anyhow!("the token expiration is out of range"),
        );
    };
    if cancellation.is_cancelled() {
        output.cancelled(deployment)?;
        return Ok(Completion::Cancelled);
    }

    // Credential persistence commits the login. Ignore later interrupts so a
    // cancelled result can never conceal a newly stored token.
    store
        .replace(
            deployment.fingerprint(),
            token.access_token(),
            expires_at,
            token.refresh_token(),
        )
        .map_err(|error| anyhow!(error))
        .context("access credential store")?;

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
                handle_unreachable(output, deployment, Phase::PrincipalConfirmation, *category)
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
) -> anyhow::Result<Completion> {
    match error {
        StatusError::Session(error) => Err(anyhow!(error).context("acquire human session")),
        StatusError::PublicApi(error) if error.is_local() => {
            Err(anyhow!(error).context(phase.operation_context(deployment)))
        }
        StatusError::PublicApi(error) => {
            handle_protocol_error(output, deployment, phase, anyhow!(error))
        }
    }
}

fn handle_unreachable(
    output: &mut LoginOutput,
    deployment: &Deployment,
    phase: Phase,
    category: UnreachableCategory,
) -> anyhow::Result<Completion> {
    if output.json {
        output.failed(
            deployment,
            FailureOutcome::Unreachable,
            phase,
            Some(category),
        )?;
        Ok(Completion::Failure)
    } else {
        let error = match phase {
            Phase::ExistingCredentialCheck | Phase::PrincipalConfirmation => {
                anyhow!("Scherzo Cloud is unreachable ({})", category.as_str())
            }
            Phase::DeviceAuthorization | Phase::TokenPolling => anyhow!(
                "authorization server is unreachable ({})",
                category.as_str()
            ),
        };
        Err(error.context(phase.operation_context(deployment)))
    }
}

fn handle_protocol_error(
    output: &mut LoginOutput,
    deployment: &Deployment,
    phase: Phase,
    error: anyhow::Error,
) -> anyhow::Result<Completion> {
    if output.json {
        output.failed(deployment, FailureOutcome::ProtocolError, phase, None)?;
        Ok(Completion::Failure)
    } else {
        Err(error.context(phase.operation_context(deployment)))
    }
}

fn handle_authorization_error(
    output: &mut LoginOutput,
    deployment: &Deployment,
    phase: Phase,
    error: AuthorizationError,
) -> anyhow::Result<Completion> {
    match error {
        AuthorizationError::Local(error) => {
            Err(anyhow!(error).context(phase.operation_context(deployment)))
        }
        AuthorizationError::Unreachable(category) => {
            handle_unreachable(output, deployment, phase, category)
        }
        error @ AuthorizationError::Protocol { .. } => {
            handle_protocol_error(output, deployment, phase, anyhow!(error))
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
    crate::timing::utc_now().checked_add(time::Duration::seconds(seconds))
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

    fn operation_context(self, deployment: &Deployment) -> String {
        match self {
            Self::ExistingCredentialCheck => format!(
                "check existing sign-in through public API {}",
                deployment.fingerprint().api_url()
            ),
            Self::DeviceAuthorization => format!(
                "request device authorization from OAuth issuer {}",
                deployment.fingerprint().issuer()
            ),
            Self::TokenPolling => format!(
                "request sign-in token from OAuth issuer {}",
                deployment.fingerprint().issuer()
            ),
            Self::PrincipalConfirmation => format!(
                "confirm sign-in through public API {}",
                deployment.fingerprint().api_url()
            ),
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
    ) -> anyhow::Result<()> {
        if self.json {
            let expires_at = expires_at
                .format(&Rfc3339)
                .context("format sign-in expiration")?;
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
            writeln!(stdout, "Sign in to Scherzo Cloud\n").context("write sign-in output")?;
            writeln!(stdout, "  Open: {activation_uri}").context("write sign-in output")?;
            writeln!(stdout, "  Code: {}", authorization.user_code())
                .context("write sign-in output")?;
            writeln!(stdout, "\nWaiting for authorization...\n").context("write sign-in output")?;
            stdout.flush().context("write sign-in output")
        }
    }

    fn status(&mut self, status: &AuthenticationStatus) -> anyhow::Result<()> {
        if self.json {
            self.json_line(&StatusEvent {
                schema_version: 1,
                event: "status",
                status: StatusResult::from_status(status),
            })
        } else {
            write_human_status(status).context("write sign-in status")
        }
    }

    fn failed(
        &mut self,
        deployment: &Deployment,
        outcome: FailureOutcome,
        phase: Phase,
        category: Option<UnreachableCategory>,
    ) -> anyhow::Result<()> {
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
            .context("write sign-in output")
        }
    }

    fn cancelled(&mut self, deployment: &Deployment) -> anyhow::Result<()> {
        if self.json {
            self.json_line(&CancelledEvent {
                schema_version: 1,
                event: "cancelled",
                deployment: deployment.fingerprint().api_url(),
            })
        } else {
            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            writeln!(stdout, "! Sign-in cancelled.").context("write sign-in output")
        }
    }

    fn json_line<T: Serialize>(&mut self, event: &T) -> anyhow::Result<()> {
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        serde_json::to_writer(&mut stdout, event).context("write JSON sign-in event")?;
        writeln!(stdout).context("write sign-in output")?;
        stdout.flush().context("write sign-in output")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_schedule_honors_interval_and_slow_down() {
        let start = crate::timing::monotonic_now();
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
