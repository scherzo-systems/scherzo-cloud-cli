use super::super::ObservationControl;
use std::io::{self, Write};
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;
use um_api::{
    RetryConflict, RunFailure, RunRetryReceipt, RunRetryRejection, RunRetryState,
    UnreachableCategory,
};
use um_human_auth::{BoundRequiredOperation, Deployment};

use crate::exit_code::ExitCode;

use super::{RunOptions, RunReference, failure_code, finish_operation, start_cloud_observation};

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: RunReference,
    #[arg(long, value_name = "VERSION", value_parser = clap::value_parser!(i64).range(1..),
        help = "Require this positive run version at admission")]
    expected_version: Option<i64>,
    #[arg(long, value_name = "DURATION", value_parser = super::super::parse_wait_timeout,
        help = "Stop waiting after a positive duration (units: ms, s, m, or h)")]
    timeout: Option<Duration>,
    #[command(flatten)]
    options: RunOptions,
}

#[derive(Clone)]
struct Recovery {
    key: String,
    receipt: Option<RunRetryReceipt>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResultDocument<'a> {
    schema_version: u8,
    operation: &'static str,
    deployment: &'a str,
    organization_ref: &'a str,
    run_id: &'a str,
    outcome: &'a str,
    request_id: Option<&'a str>,
    idempotency_key: Option<&'a str>,
    receipt: Option<&'a RunRetryReceipt>,
    code: Option<&'a str>,
}

impl Command {
    pub(super) fn execute(self, deployment: Deployment) -> super::super::CommandResult {
        let key = um_support::generate_idempotency_key()
            .context("generate Cloud run retry request identity")?;
        let signal_deployment = deployment.clone();
        let timeout_deployment = deployment.clone();
        let signal_org = self.run.organization.clone();
        let timeout_org = signal_org.clone();
        let signal_run = self.run.run_id.clone();
        let timeout_run = signal_run.clone();
        let json = self.options.json;
        super::super::execute_mutation_with_signals_and_deferred_timeout(
            "Cloud run retry",
            Recovery { key, receipt: None },
            self.timeout,
            move |control, timer| self.execute_blocking(&deployment, control, timer),
            move |signal, snapshot| {
                let recovery = snapshot.recovery;
                render(
                    (&signal_deployment, &signal_org, &signal_run, json),
                    &recovery,
                    if recovery.receipt.is_some() {
                        "observation_stopped"
                    } else {
                        "acceptance_unknown"
                    },
                    None,
                    signal,
                )
                .map_err(Into::into)
            },
            move |snapshot| {
                render(
                    (&timeout_deployment, &timeout_org, &timeout_run, json),
                    &snapshot.recovery,
                    "timed_out",
                    None,
                    ExitCode::GeneralFailure,
                )
                .map_err(Into::into)
            },
        )
    }

    fn execute_blocking(
        self,
        deployment: &Deployment,
        control: &super::super::OperationControl<Recovery>,
        timer: &super::super::DeferredObservationTimeoutStart,
    ) -> super::super::CommandResult {
        let mut recovery = control.recovery();
        let submit = pinned_submission(deployment, &self, control, &recovery.key);
        let receipt = match submit {
            Ok(Ok(receipt)) => receipt,
            Ok(Err(failure)) => {
                let (outcome, code, exit) = submission_failure(
                    &failure,
                    control.dispatched(),
                    self.options.authentication.kind(),
                );
                return self.finish_result(
                    deployment,
                    control,
                    &recovery,
                    outcome,
                    Some(code),
                    exit,
                );
            }
            Err(_) => {
                let dispatched = control.dispatched();
                return self.finish_result(
                    deployment,
                    control,
                    &recovery,
                    if dispatched {
                        "acceptance_unknown"
                    } else {
                        "error"
                    },
                    Some(if dispatched {
                        "acceptance_unknown"
                    } else {
                        "submission_failed"
                    }),
                    if dispatched {
                        ExitCode::Unavailable
                    } else {
                        ExitCode::GeneralFailure
                    },
                );
            }
        };
        recovery.receipt = Some(receipt);
        if !control.update_recovery(recovery.clone()) {
            return Ok(ExitCode::GeneralFailure);
        }
        let (clock, started) = start_cloud_observation(timer);
        let deadline = self
            .timeout
            .map(|timeout| started.checked_add(timeout).unwrap_or(started));
        let mut consecutive_failures = 0;
        loop {
            let remaining = super::super::remaining_observation_wait(
                self.timeout,
                started,
                super::super::ObservationClock::now(&clock),
            );
            let Some(remaining) = remaining else {
                return finish_operation(control, || {
                    render(
                        (
                            deployment,
                            &self.run.organization,
                            &self.run.run_id,
                            self.options.json,
                        ),
                        &recovery,
                        "timed_out",
                        None,
                        ExitCode::GeneralFailure,
                    )
                });
            };
            if !control.admit_read() {
                return Ok(ExitCode::GeneralFailure);
            };
            let request_id = recovery
                .receipt
                .as_ref()
                .map(|receipt| receipt.id.as_str())
                .unwrap_or_default();
            let observation = super::with_api_until(
                deployment,
                self.options.http.transport_policy(),
                &self.options.authentication,
                deadline,
                |api, budget| {
                    api.get_retry(&self.run.organization, &self.run.run_id, request_id, budget)
                },
            );
            let delay = match observation {
                Ok(Ok(receipt)) => {
                    let state = receipt.state;
                    recovery.receipt = Some(receipt);
                    if !control.update_recovery(recovery.clone()) {
                        return Ok(ExitCode::GeneralFailure);
                    }
                    consecutive_failures = 0;
                    match state {
                        RunRetryState::Applied => {
                            return finish_operation(control, || {
                                render(
                                    (
                                        deployment,
                                        &self.run.organization,
                                        &self.run.run_id,
                                        self.options.json,
                                    ),
                                    &recovery,
                                    "applied",
                                    None,
                                    ExitCode::Success,
                                )
                            });
                        }
                        RunRetryState::Rejected => {
                            return finish_operation(control, || {
                                let code = recovery
                                    .receipt
                                    .as_ref()
                                    .and_then(|receipt| receipt.rejection)
                                    .map(rejection_code)
                                    .unwrap_or("protocol_error");
                                render(
                                    (
                                        deployment,
                                        &self.run.organization,
                                        &self.run.run_id,
                                        self.options.json,
                                    ),
                                    &recovery,
                                    "rejected",
                                    Some(code),
                                    ExitCode::GeneralFailure,
                                )
                            });
                        }
                        RunRetryState::Pending => super::super::OBSERVATION_POLL_INTERVAL,
                    }
                }
                Ok(Err(RunFailure::Unreachable(UnreachableCategory::Timeout)))
                    if self.timeout.is_some()
                        && super::super::remaining_observation_wait(
                            self.timeout,
                            started,
                            super::super::ObservationClock::now(&clock),
                        )
                        .is_none() =>
                {
                    return finish_operation(control, || {
                        render(
                            (
                                deployment,
                                &self.run.organization,
                                &self.run.run_id,
                                self.options.json,
                            ),
                            &recovery,
                            "timed_out",
                            None,
                            ExitCode::GeneralFailure,
                        )
                    });
                }
                Ok(Err(RunFailure::RetryAfter(delay))) => delay,
                Ok(Err(failure))
                    if failure.retryable_observation() && consecutive_failures == 0 =>
                {
                    consecutive_failures += 1;
                    super::super::OBSERVATION_POLL_INTERVAL
                }
                Ok(Err(failure)) => {
                    let (code, exit) = failure_code(&failure, self.options.authentication.kind());
                    return self.finish_error(deployment, control, &recovery, code, exit);
                }
                Err(_) => {
                    return self.finish_error(
                        deployment,
                        control,
                        &recovery,
                        "observation_failed",
                        ExitCode::GeneralFailure,
                    );
                }
            };
            // Check ownership between short blocking-worker sleeps. A long Retry-After
            // cannot delay process shutdown after a signal or timeout.
            let mut remaining_delay = bounded_poll_delay(delay, self.timeout.map(|_| remaining));
            while !control.is_stopped() && !remaining_delay.is_zero() {
                let slice = remaining_delay.min(Duration::from_millis(100));
                super::super::ObservationClock::sleep(&clock, slice);
                remaining_delay = remaining_delay.saturating_sub(slice);
            }
        }
    }

    fn finish_error(
        &self,
        deployment: &Deployment,
        control: &super::super::OperationControl<Recovery>,
        recovery: &Recovery,
        code: &str,
        exit: ExitCode,
    ) -> super::super::CommandResult {
        self.finish_result(deployment, control, recovery, "error", Some(code), exit)
    }

    fn finish_result(
        &self,
        deployment: &Deployment,
        control: &super::super::OperationControl<Recovery>,
        recovery: &Recovery,
        outcome: &str,
        code: Option<&str>,
        exit: ExitCode,
    ) -> super::super::CommandResult {
        finish_operation(control, || {
            render(
                (
                    deployment,
                    &self.run.organization,
                    &self.run.run_id,
                    self.options.json,
                ),
                recovery,
                outcome,
                code,
                exit,
            )
        })
    }
}

fn bounded_poll_delay(delay: Duration, deadline_remaining: Option<Duration>) -> Duration {
    deadline_remaining.map_or(delay, |remaining| delay.min(remaining))
}

fn pinned_submission(
    deployment: &Deployment,
    command: &Command,
    control: &super::super::OperationControl<Recovery>,
    key: &str,
) -> anyhow::Result<Result<RunRetryReceipt, RunFailure>> {
    let policy = command.options.http.transport_policy();
    let client = super::super::human_session_client(policy)?;
    let submit = |token: &str| {
        let api = super::run_api(&client, deployment, token, policy)?;
        Ok(api.request_retry(
            &command.run.organization,
            &command.run.run_id,
            key,
            command.expected_version,
            || control.begin_dispatch(),
        ))
    };
    if let Some(api_key) = command.options.authentication.service_api_key()? {
        return submit(api_key.expose());
    }
    match um_human_auth::execute_pinned_required(
        &client,
        deployment,
        |token| submit(token.expose()),
        |result| {
            result
                .as_ref()
                .is_ok_and(|value| value.as_ref().is_err_and(RunFailure::credential_rejected))
        },
    ) {
        Ok(BoundRequiredOperation::Completed { result, .. }) => result,
        Ok(
            BoundRequiredOperation::Unauthenticated { .. }
            | BoundRequiredOperation::ActingSessionChanged,
        ) => Ok(Err(RunFailure::Unauthenticated)),
        Err(error) => match error.unreachable_category() {
            Some(category) => Ok(Err(RunFailure::Unreachable(category))),
            None => Err(anyhow!(error).context("acquire human session for Cloud run retry")),
        },
    }
}

fn rejection_code(rejection: RunRetryRejection) -> &'static str {
    match rejection {
        RunRetryRejection::RunNotFound => "run_not_found",
        RunRetryRejection::IdempotencyConflict => "idempotency_conflict",
        RunRetryRejection::ExpectedRunVersionConflict => "expected_run_version_conflict",
        RunRetryRejection::AttemptActive => "attempt_active",
        RunRetryRejection::LatestAttemptSucceeded => "latest_attempt_succeeded",
        RunRetryRejection::SourceUnavailable => "source_unavailable",
        RunRetryRejection::LatestAttemptRejected => "latest_attempt_rejected",
        RunRetryRejection::RunInputRetryUnavailable => "run_input_retry_unavailable",
    }
}

fn submission_failure(
    failure: &RunFailure,
    dispatched: bool,
    auth: super::super::PrincipalAuthenticationKind,
) -> (&'static str, &'static str, ExitCode) {
    match failure {
        RunFailure::RetryConflict(RetryConflict::TriggerSlot) => (
            "trigger_slot_conflict",
            "trigger_slot_conflict",
            ExitCode::GeneralFailure,
        ),
        RunFailure::RetryConflict(RetryConflict::Pending) => {
            ("retry_pending", "retry_pending", ExitCode::GeneralFailure)
        }
        RunFailure::RetryConflict(RetryConflict::Idempotency) => {
            ("error", "idempotency_conflict", ExitCode::GeneralFailure)
        }
        RunFailure::Unreachable(UnreachableCategory::RateLimited) => {
            ("error", "rate_limited", ExitCode::Unavailable)
        }
        RunFailure::RetryAmbiguousRateLimited | RunFailure::RetryAmbiguousAuthentication => (
            "acceptance_unknown",
            "acceptance_unknown",
            ExitCode::Unavailable,
        ),
        RunFailure::Unreachable(_)
        | RunFailure::Protocol {
            credential_rejected: false,
        }
        | RunFailure::RetryAfter(_)
            if dispatched =>
        {
            (
                "acceptance_unknown",
                "acceptance_unknown",
                ExitCode::Unavailable,
            )
        }
        _ => {
            let (code, exit) = failure_code(failure, auth);
            ("error", code, exit)
        }
    }
}

fn render(
    (deployment, org, run, json): (&Deployment, &str, &str, bool),
    recovery: &Recovery,
    outcome: &str,
    code: Option<&str>,
    exit: ExitCode,
) -> anyhow::Result<ExitCode> {
    let receipt = recovery.receipt.as_ref();
    let request_id = receipt.map(|receipt| receipt.id.as_str());
    if json {
        let document = ResultDocument {
            schema_version: 1,
            operation: "retry",
            deployment: deployment.fingerprint().api_url(),
            organization_ref: org,
            run_id: run,
            outcome,
            request_id,
            idempotency_key: (receipt.is_none() || outcome == "acceptance_unknown")
                .then_some(recovery.key.as_str()),
            receipt,
            code,
        };
        let mut stdout = io::stdout().lock();
        serde_json::to_writer(&mut stdout, &document)
            .context("serialize Cloud run retry result")?;
        writeln!(stdout).context("write Cloud run retry result")?;
    } else if outcome == "applied" {
        writeln!(
            io::stdout().lock(),
            "✓ Run retry applied.\n\nrun: {run}\nrequest: {}",
            request_id.unwrap_or("not established")
        )?;
    } else {
        let mut stderr = io::stderr().lock();
        let diagnostic = match outcome {
            "rejected" => "run retry rejected",
            "trigger_slot_conflict" | "retry_pending" => "run retry conflicts with current state",
            "timed_out" => "run retry observation timed out",
            "observation_stopped" => "run retry observation stopped",
            "acceptance_unknown" => "run retry acceptance unconfirmed",
            _ => "run retry not confirmed",
        };
        writeln!(
            stderr,
            "error: {diagnostic}\n\nrun: {run}\norganization: {org}\ncode: {}",
            code.unwrap_or(outcome)
        )?;
        if let Some(id) = request_id {
            writeln!(stderr, "request: {id}")?;
        } else {
            writeln!(stderr, "idempotency key: {}", recovery.key)?;
        }
        writeln!(
            stderr,
            "\n{}",
            match code {
                Some("authentication_required") => "Sign in first, then show the run and request.",
                Some("forbidden") => "Ask an organization owner to check access to this run.",
                _ => "Show the run and known request before requesting another retry.",
            }
        )?;
    }
    Ok(exit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_is_only_capped_by_an_actual_deadline() {
        let retry_after = Duration::from_secs(60);
        assert_eq!(bounded_poll_delay(retry_after, None), retry_after);
        assert_eq!(
            bounded_poll_delay(retry_after, Some(Duration::from_secs(3))),
            Duration::from_secs(3)
        );
    }
}
