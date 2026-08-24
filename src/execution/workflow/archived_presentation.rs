use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;

use serde::Serialize;

use super::archived_attempt::{
    ArchivedAttemptIneligibilityReason, ArchivedAttemptLoadError,
    ArchivedAttemptOperationalErrorCode, ArchivedAttemptState, ArchivedAttemptTrigger,
    ArchivedCancellationReason, ArchivedFailure, ArchivedFailureCause, ArchivedFailurePhase,
    ArchivedStep, ArchivedStepDetail, ArchivedStepState, ArchivedWorkflowOutcome,
    LoadedLocalArchivedAttempt, LocalArchivedAttempt,
};
use super::document::{FailurePolicy, Output};
use super::presentation::{human_duration, styled_terminal_text as styled};
use super::presentation_feed::{WorkflowPresentationStep, normalize_terminal_scalar};
use super::publication::{FinalizationTriggerV1, WorkflowResultV1};
use super::validated::WorkflowNodeRole;
use crate::exit_code::ExitCode;

const COMMAND: &str = "scherzo-cloud workflow view";
const STYLE_PRIMARY: &str = "38;2;205;214;244";
const STYLE_SECONDARY: &str = "38;2;166;173;200";
const STYLE_MUTED: &str = "38;2;127;132;156";
const STYLE_SUCCESS: &str = "38;2;166;227;161";
const STYLE_FAILURE: &str = "38;2;243;139;168";
const STYLE_BLOCKED: &str = "38;2;250;179;135";

#[derive(Debug)]
pub(crate) enum ArchivedViewOutput {
    Plain {
        attempt: Box<LocalArchivedAttempt>,
        color: bool,
    },
    JsonSuccess(Box<LoadedLocalArchivedAttempt>),
    JsonError(ArchivedAttemptLoadError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchivedViewOutputFailure {
    InvalidProjection,
    Serialization,
    Write(io::ErrorKind),
    Flush(io::ErrorKind),
}

impl std::fmt::Display for ArchivedViewOutputFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidProjection => formatter.write_str("invalid archived workflow projection"),
            Self::Serialization => formatter.write_str("serialize workflow view output"),
            Self::Write(kind) => write!(formatter, "write workflow view output ({kind:?})"),
            Self::Flush(kind) => write!(formatter, "flush workflow view output ({kind:?})"),
        }
    }
}

impl std::error::Error for ArchivedViewOutputFailure {}

impl ArchivedViewOutput {
    pub(crate) fn write_stdout(self) -> Result<ExitCode, ArchivedViewOutputFailure> {
        let (bytes, exit) = match self {
            Self::Plain { attempt, color } => (
                render_plain(&attempt, color)?.into_bytes(),
                ExitCode::Success,
            ),
            Self::JsonSuccess(loaded) => (serialize_json_success(&loaded)?, ExitCode::Success),
            Self::JsonError(error) => (serialize_json_error(&error)?, ExitCode::GeneralFailure),
        };
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        stdout
            .write_all(&bytes)
            .map_err(|error| ArchivedViewOutputFailure::Write(error.kind()))?;
        stdout
            .flush()
            .map_err(|error| ArchivedViewOutputFailure::Flush(error.kind()))?;
        Ok(exit)
    }
}

pub(crate) fn render_plain(
    attempt: &LocalArchivedAttempt,
    color: bool,
) -> Result<String, ArchivedViewOutputFailure> {
    let selection = if attempt.attempt_number == attempt.current_attempt_number {
        "current"
    } else {
        "historical"
    };
    let outcome = archived_outcome(attempt.outcome);
    let mut rendered = String::new();
    rendered.push_str(&format!(
        "{} {}\n{} {}\n",
        styled("run", STYLE_MUTED, color),
        styled(&safe_path(&attempt.run_directory), STYLE_PRIMARY, color),
        styled("result", STYLE_MUTED, color),
        styled(&safe_path(&attempt.result_directory), STYLE_PRIMARY, color),
    ));
    rendered.push_str(&format!(
        "attempt {} of {} · {selection} · {} · state {} · outcome {}\n",
        attempt.attempt_number,
        attempt.current_attempt_number,
        archived_trigger(attempt.trigger),
        styled(
            archived_attempt_state(attempt.state),
            attempt_state_style(attempt.state),
            color,
        ),
        styled(outcome, outcome_style(attempt.outcome), color),
    ));
    rendered.push_str(&format!(
        "workflow {} · {} {} · concurrency {}\n",
        safe_text(&attempt.workflow_path),
        attempt.steps.len(),
        if attempt.steps.len() == 1 {
            "node"
        } else {
            "nodes"
        },
        attempt.execution.maximum_parallel_steps,
    ));
    rendered.push_str(&format!(
        "attempt created {} · started {} · settled {}\n",
        timestamp(attempt.created_at),
        attempt.started_at.map_or_else(|| "—".to_owned(), timestamp),
        timestamp(attempt.settled_at),
    ));
    rendered.push_str(&format!(
        "execution started {} · finished {} · duration {}\n\n",
        timestamp(attempt.execution.started_at),
        timestamp(attempt.execution.finished_at),
        human_duration(attempt.execution.duration),
    ));

    rendered.push_str(&styled("ordinary phase", STYLE_SECONDARY, color));
    rendered.push('\n');
    let finalization_start = attempt.workflow.finalization_start;
    for (index, step) in attempt.steps.iter().enumerate() {
        if finalization_start == Some(index) {
            let trigger = attempt
                .finalization
                .as_ref()
                .map(|finalization| archived_finalization_trigger(finalization.trigger))
                .ok_or(ArchivedViewOutputFailure::InvalidProjection)?;
            rendered.push('\n');
            rendered.push_str(&styled(
                &format!("finalization phase · trigger {trigger}"),
                STYLE_SECONDARY,
                color,
            ));
            rendered.push('\n');
        }
        let definition = attempt
            .workflow
            .steps
            .get(&step.id)
            .ok_or(ArchivedViewOutputFailure::InvalidProjection)?;
        rendered.push_str(&plain_step_row(step, definition, color));
        rendered.push('\n');
        if let Some(recovery) = &step.recovery {
            let terminal_failure = match &step.detail {
                ArchivedStepDetail::Failed(failure) => Some(archived_failure_detail(failure)),
                _ => None,
            };
            rendered.push_str(&format!(
                "  recovery {} · rounds {}/{} · {} invocations\n",
                super::presentation::terminal_recovery_detail(
                    recovery,
                    terminal_failure.as_deref(),
                ),
                recovery.rounds.len(),
                recovery.configured_retries,
                step.invocations.len(),
            ));
            for invocation in &step.invocations {
                rendered.push_str(&format!(
                    "    invocation {} · {} · {} · usage input {} output {} · {} diagnostics\n",
                    invocation.invocation_id,
                    match invocation.role {
                        super::publication::RecoveryInvocationRoleV1::Target => format!(
                            "target execution {}",
                            invocation.target_execution.unwrap_or_default()
                        ),
                        super::publication::RecoveryInvocationRoleV1::RecoveryHandler => format!(
                            "recovery_handler round {}",
                            invocation.recovery_round.unwrap_or_default()
                        ),
                    },
                    match invocation.state {
                        super::publication::RecoveryInvocationStateV1::Settled => "settled",
                        super::publication::RecoveryInvocationStateV1::Cancelled => "cancelled",
                    },
                    invocation.usage.input_tokens,
                    invocation.usage.output_tokens,
                    invocation.diagnostics.len(),
                ));
            }
        }
    }

    let counts = terminal_counts(&attempt.steps);
    rendered.push_str(&format!(
        "\nterminal counts: {} succeeded · {} failed · {} blocked · {} not-run · {} cancelled · {} advisory issues\n",
        counts.succeeded,
        counts.failed,
        counts.blocked,
        counts.not_run,
        counts.cancelled,
        counts.advisory_issues,
    ));
    rendered.push_str(&format!(
        "result {} · {} total\n",
        styled(outcome, outcome_style(attempt.outcome), color),
        human_duration(attempt.execution.duration),
    ));
    if let Some(primary) = &attempt.primary_failure {
        rendered.push_str(&format!(
            "{} {} {} · {}\n",
            styled("primary failure:", STYLE_FAILURE, color),
            archived_role(primary.role),
            safe_text(&primary.step),
            archived_failure_detail(&primary.failure),
        ));
    }
    if let Some(cancellation) = &attempt.cancellation {
        rendered.push_str(&format!(
            "{} {} · requested {} · force-stop {}\n",
            styled("cancellation:", STYLE_BLOCKED, color),
            archived_cancellation_reason(cancellation.reason),
            timestamp(cancellation.requested_at),
            timestamp(cancellation.force_stop_deadline),
        ));
    }
    if let Some(finalization) = &attempt.finalization {
        let cleanup = if finalization.force_abort {
            "incomplete · force abort accepted".to_owned()
        } else if let Some(cancellation) = &finalization.cancellation {
            let deadline = cancellation.force_stop_deadline.map_or_else(
                || "no force-stop deadline".to_owned(),
                |deadline| format!("force-stop {}", timestamp(deadline)),
            );
            format!(
                "incomplete · cancelled {} · {deadline}",
                archived_cancellation_reason(cancellation.reason)
            )
        } else {
            "complete".to_owned()
        };
        let style = if finalization.force_abort || finalization.cancellation.is_some() {
            STYLE_BLOCKED
        } else if finalization.issues.is_empty() {
            STYLE_SUCCESS
        } else {
            STYLE_FAILURE
        };
        rendered.push_str(&format!(
            "{} trigger {} · {} issues · {}\n",
            styled("finalization:", style, color),
            archived_finalization_trigger(finalization.trigger),
            finalization.issues.len(),
            cleanup,
        ));
        if !finalization.issues.is_empty() {
            let issues = finalization
                .issues
                .iter()
                .map(|(id, policy)| {
                    format!("{} ({})", safe_text(id), archived_failure_policy(*policy))
                })
                .collect::<Vec<_>>()
                .join(", ");
            rendered.push_str(&format!("finalization issues: {issues}\n"));
        }
    }
    Ok(rendered)
}

fn plain_step_row(
    step: &ArchivedStep,
    definition: &WorkflowPresentationStep,
    color: bool,
) -> String {
    let state = archived_step_state(step.state);
    let duration = step.duration.map_or_else(|| "—".to_owned(), human_duration);
    format!(
        "- node {} · role {} · kind {} · policy {} · state {} · duration {} · {}",
        safe_text(&step.id),
        archived_role(step.role),
        archived_step_kind(definition),
        archived_failure_policy(step.failure_policy),
        styled(state, step_state_style(step.state), color),
        duration,
        archived_step_detail(step, definition),
    )
}

fn archived_step_detail(step: &ArchivedStep, definition: &WorkflowPresentationStep) -> String {
    match &step.detail {
        ArchivedStepDetail::Succeeded => match definition {
            WorkflowPresentationStep::Command { .. } => "exit 0".to_owned(),
            WorkflowPresentationStep::Agent { outputs, .. } => {
                if outputs
                    .values()
                    .any(|output| matches!(output, Output::AgentResponse))
                {
                    "response captured".to_owned()
                } else if outputs
                    .values()
                    .any(|output| matches!(output, Output::AgentResult { .. }))
                {
                    "result captured".to_owned()
                } else {
                    "no requested agent value".to_owned()
                }
            }
        },
        ArchivedStepDetail::Failed(failure) => archived_failure_detail(failure),
        ArchivedStepDetail::Blocked { dependency } => {
            format!("blocked by {}", safe_text(dependency))
        }
        ArchivedStepDetail::InputUnavailable { references } => format!(
            "inputs unavailable: {}",
            references
                .iter()
                .map(|reference| safe_text(reference))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ArchivedStepDetail::NotRun => "failure_stop".to_owned(),
        ArchivedStepDetail::TriggerNotSelected => "finalizer_trigger_not_selected".to_owned(),
        ArchivedStepDetail::Cancelled { reason } => {
            archived_cancellation_reason(*reason).to_owned()
        }
    }
}

#[derive(Default)]
struct TerminalCounts {
    succeeded: usize,
    failed: usize,
    blocked: usize,
    not_run: usize,
    cancelled: usize,
    advisory_issues: usize,
}

fn terminal_counts(steps: &[ArchivedStep]) -> TerminalCounts {
    let mut counts = TerminalCounts::default();
    for step in steps {
        match step.state {
            ArchivedStepState::Succeeded => counts.succeeded += 1,
            ArchivedStepState::Failed => counts.failed += 1,
            ArchivedStepState::Blocked => counts.blocked += 1,
            ArchivedStepState::NotRun => counts.not_run += 1,
            ArchivedStepState::Cancelled => counts.cancelled += 1,
        }
        if step.failure_policy == FailurePolicy::Advisory
            && matches!(
                step.state,
                ArchivedStepState::Failed | ArchivedStepState::Blocked
            )
        {
            counts.advisory_issues += 1;
        }
    }
    counts
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowViewSuccess<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    run_directory: &'a str,
    current_attempt_number: u64,
    attempt_number: u64,
    selection: &'static str,
    trigger: &'static str,
    attempt_state: &'static str,
    result_directory: &'a str,
    result: &'a WorkflowResultV1,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowViewError<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_directory: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_number: Option<u64>,
    error: WorkflowViewErrorDetail,
}

#[derive(Serialize)]
struct WorkflowViewErrorDetail {
    code: &'static str,
    message: &'static str,
}

fn serialize_json_success(
    loaded: &LoadedLocalArchivedAttempt,
) -> Result<Vec<u8>, ArchivedViewOutputFailure> {
    let attempt = &loaded.projection;
    let run_directory = attempt
        .run_directory
        .to_str()
        .ok_or(ArchivedViewOutputFailure::InvalidProjection)?;
    let result_directory = attempt
        .result_directory
        .to_str()
        .ok_or(ArchivedViewOutputFailure::InvalidProjection)?;
    serialize_json(&WorkflowViewSuccess {
        schema_version: 1,
        command: COMMAND,
        outcome: "view",
        exit_status: ExitCode::Success.as_u8(),
        run_directory,
        current_attempt_number: attempt.current_attempt_number,
        attempt_number: attempt.attempt_number,
        selection: if attempt.attempt_number == attempt.current_attempt_number {
            "current"
        } else {
            "historical"
        },
        trigger: archived_trigger(attempt.trigger),
        attempt_state: archived_attempt_state(attempt.state),
        result_directory,
        result: &loaded.result,
    })
}

fn serialize_json_error(
    error: &ArchivedAttemptLoadError,
) -> Result<Vec<u8>, ArchivedViewOutputFailure> {
    let (run_directory, attempt_number, code, message) = match error {
        ArchivedAttemptLoadError::Operational(error) => (
            error.run_directory.as_deref().and_then(Path::to_str),
            None,
            operational_error_code(error.code),
            operational_error_message(error.code),
        ),
        ArchivedAttemptLoadError::Ineligible(error) => (
            error.run_directory.to_str(),
            Some(error.attempt_number),
            ineligibility_code(error.reason),
            ineligibility_message(error.reason),
        ),
    };
    serialize_json(&WorkflowViewError {
        schema_version: 1,
        command: COMMAND,
        outcome: "error",
        exit_status: ExitCode::GeneralFailure.as_u8(),
        run_directory,
        attempt_number,
        error: WorkflowViewErrorDetail { code, message },
    })
}

fn serialize_json(value: &impl Serialize) -> Result<Vec<u8>, ArchivedViewOutputFailure> {
    let mut bytes =
        serde_json::to_vec_pretty(value).map_err(|_| ArchivedViewOutputFailure::Serialization)?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) const fn operational_error_code(
    code: ArchivedAttemptOperationalErrorCode,
) -> &'static str {
    match code {
        ArchivedAttemptOperationalErrorCode::RunDirectoryUnavailable => "run_directory_unavailable",
        ArchivedAttemptOperationalErrorCode::RunDirectoryInvalid => "run_directory_invalid",
        ArchivedAttemptOperationalErrorCode::RecoverySchemaUnsupported => {
            "recovery_schema_unsupported"
        }
        ArchivedAttemptOperationalErrorCode::LockQueryFailed => "lock_query_failed",
        ArchivedAttemptOperationalErrorCode::StatusSnapshotUnstable => "status_snapshot_unstable",
        ArchivedAttemptOperationalErrorCode::PublishedResultUnavailable => {
            "published_result_unavailable"
        }
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid => "published_result_invalid",
        ArchivedAttemptOperationalErrorCode::RetainedWorkflowInvalid => "retained_workflow_invalid",
    }
}

const fn operational_error_message(code: ArchivedAttemptOperationalErrorCode) -> &'static str {
    match code {
        ArchivedAttemptOperationalErrorCode::RunDirectoryUnavailable => {
            "The run directory is unavailable. Check the path and permissions, then try again."
        }
        ArchivedAttemptOperationalErrorCode::RunDirectoryInvalid => {
            "The run directory is invalid. Select a supported workflow run directory."
        }
        ArchivedAttemptOperationalErrorCode::RecoverySchemaUnsupported => {
            "The recovery summary version is unsupported. Select a schema-1 workflow attempt."
        }
        ArchivedAttemptOperationalErrorCode::LockQueryFailed => {
            "Run ownership cannot be inspected safely. Check the run lock and try again."
        }
        ArchivedAttemptOperationalErrorCode::StatusSnapshotUnstable => {
            "A stable run snapshot is unavailable. Wait for the current update, then try again."
        }
        ArchivedAttemptOperationalErrorCode::PublishedResultUnavailable => {
            "The published result is unavailable. Restore the selected immutable result and try again."
        }
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid => {
            "The published result is invalid. Select an intact published attempt."
        }
        ArchivedAttemptOperationalErrorCode::RetainedWorkflowInvalid => {
            "The retained workflow is invalid. Select an intact workflow run directory."
        }
    }
}

pub(crate) const fn ineligibility_code(reason: ArchivedAttemptIneligibilityReason) -> &'static str {
    match reason {
        ArchivedAttemptIneligibilityReason::Unknown => "attempt_unknown",
        ArchivedAttemptIneligibilityReason::Nonterminal => "attempt_nonterminal",
        ArchivedAttemptIneligibilityReason::Interrupted => "attempt_interrupted",
        ArchivedAttemptIneligibilityReason::Rejected => "attempt_rejected",
        ArchivedAttemptIneligibilityReason::PublicationFailed => "attempt_publication_failed",
        ArchivedAttemptIneligibilityReason::Unpublished => "attempt_unpublished",
    }
}

const fn ineligibility_message(reason: ArchivedAttemptIneligibilityReason) -> &'static str {
    match reason {
        ArchivedAttemptIneligibilityReason::Unknown => {
            "The selected attempt does not exist. Choose an attempt recorded by this run."
        }
        ArchivedAttemptIneligibilityReason::Nonterminal => {
            "The selected attempt is not terminal. Wait for it to settle, then try again."
        }
        ArchivedAttemptIneligibilityReason::Interrupted => {
            "The selected attempt was interrupted without a published result. Choose a published attempt."
        }
        ArchivedAttemptIneligibilityReason::Rejected => {
            "The selected attempt was rejected without a published result. Choose a published attempt."
        }
        ArchivedAttemptIneligibilityReason::PublicationFailed => {
            "The selected attempt result was not published. Choose an attempt with a published result."
        }
        ArchivedAttemptIneligibilityReason::Unpublished => {
            "The selected attempt has no published result. Wait for publication or choose another attempt."
        }
    }
}

pub(crate) const fn archived_trigger(trigger: ArchivedAttemptTrigger) -> &'static str {
    match trigger {
        ArchivedAttemptTrigger::Initial => "initial",
        ArchivedAttemptTrigger::ExplicitRetry => "explicit_retry",
    }
}

pub(crate) const fn archived_attempt_state(state: ArchivedAttemptState) -> &'static str {
    match state {
        ArchivedAttemptState::Succeeded => "succeeded",
        ArchivedAttemptState::WorkflowFailed => "workflow_failed",
        ArchivedAttemptState::Cancelled => "cancelled",
    }
}

pub(crate) const fn archived_outcome(outcome: ArchivedWorkflowOutcome) -> &'static str {
    match outcome {
        ArchivedWorkflowOutcome::Succeeded => "succeeded",
        ArchivedWorkflowOutcome::Failed => "failed",
        ArchivedWorkflowOutcome::Cancelled => "cancelled",
    }
}

pub(crate) const fn archived_role(role: WorkflowNodeRole) -> &'static str {
    match role {
        WorkflowNodeRole::Step => "step",
        WorkflowNodeRole::Finalizer => "finalizer",
    }
}

pub(crate) const fn archived_step_state(state: ArchivedStepState) -> &'static str {
    match state {
        ArchivedStepState::Succeeded => "succeeded",
        ArchivedStepState::Failed => "failed",
        ArchivedStepState::Blocked => "blocked",
        ArchivedStepState::NotRun => "not-run",
        ArchivedStepState::Cancelled => "cancelled",
    }
}

pub(crate) const fn archived_failure_policy(policy: FailurePolicy) -> &'static str {
    match policy {
        FailurePolicy::Required => "required",
        FailurePolicy::Advisory => "advisory",
    }
}

pub(crate) const fn archived_step_kind(step: &WorkflowPresentationStep) -> &'static str {
    match step {
        WorkflowPresentationStep::Command { .. } => "cmd",
        WorkflowPresentationStep::Agent { .. } => "agent",
    }
}

pub(crate) const fn archived_finalization_trigger(trigger: FinalizationTriggerV1) -> &'static str {
    match trigger {
        FinalizationTriggerV1::Succeeded => "succeeded",
        FinalizationTriggerV1::Failed => "failed",
        FinalizationTriggerV1::Cancelled => "cancelled",
    }
}

pub(crate) fn archived_failure_detail(failure: &ArchivedFailure) -> String {
    let phase = match failure.phase {
        ArchivedFailurePhase::Start => "start",
        ArchivedFailurePhase::Execution => "execution",
        ArchivedFailurePhase::OutputCapture => "output_capture",
    };
    format!("{phase} · {}", archived_failure_cause(&failure.cause))
}

fn archived_failure_cause(cause: &ArchivedFailureCause) -> String {
    let mut detail = snake_case_debug(cause.code);
    if let Some(input) = &cause.input {
        detail.push_str(" · input ");
        detail.push_str(&safe_text(input));
    }
    if let Some(index) = cause.collection_index {
        detail.push_str(&format!(" · collection index {index}"));
    }
    if let Some(output) = &cause.output {
        detail.push_str(" · output ");
        detail.push_str(&safe_text(output));
    }
    if let Some(exit_code) = cause.exit_code {
        detail.push_str(&format!(" · exit {exit_code}"));
    }
    detail
}

fn snake_case_debug(value: impl std::fmt::Debug) -> String {
    super::presentation::snake_case_debug(value)
}

pub(crate) const fn archived_cancellation_reason(
    reason: ArchivedCancellationReason,
) -> &'static str {
    match reason {
        ArchivedCancellationReason::UserRequest => "user_request",
        ArchivedCancellationReason::TerminationRequest => "termination_request",
        ArchivedCancellationReason::CallerOutputFailure => "caller_output_failure",
        ArchivedCancellationReason::RunnerShutdown => "runner_shutdown",
        ArchivedCancellationReason::ExecutionLeaseExpired => "execution_lease_expired",
        ArchivedCancellationReason::FinalizationForceAbort => "finalization_force_abort",
    }
}

pub(crate) fn safe_text(value: &str) -> String {
    normalize_terminal_scalar(value.as_bytes())
}

pub(crate) fn safe_path(value: &Path) -> String {
    normalize_terminal_scalar(value.as_os_str().as_bytes())
}

fn timestamp(value: time::OffsetDateTime) -> String {
    super::presentation::header_timestamp(value)
}

const fn attempt_state_style(state: ArchivedAttemptState) -> &'static str {
    match state {
        ArchivedAttemptState::Succeeded => STYLE_SUCCESS,
        ArchivedAttemptState::WorkflowFailed => STYLE_FAILURE,
        ArchivedAttemptState::Cancelled => STYLE_BLOCKED,
    }
}

const fn outcome_style(outcome: ArchivedWorkflowOutcome) -> &'static str {
    match outcome {
        ArchivedWorkflowOutcome::Succeeded => STYLE_SUCCESS,
        ArchivedWorkflowOutcome::Failed => STYLE_FAILURE,
        ArchivedWorkflowOutcome::Cancelled => STYLE_BLOCKED,
    }
}

const fn step_state_style(state: ArchivedStepState) -> &'static str {
    match state {
        ArchivedStepState::Succeeded => STYLE_SUCCESS,
        ArchivedStepState::Failed => STYLE_FAILURE,
        ArchivedStepState::Blocked | ArchivedStepState::Cancelled => STYLE_BLOCKED,
        ArchivedStepState::NotRun => STYLE_MUTED,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::Duration;

    use super::*;
    use crate::execution::workflow::archived_attempt::{
        ArchivedAttemptIneligible, ArchivedAttemptOperationalError, ArchivedExecution,
    };
    use crate::execution::workflow::presentation_feed::WorkflowPresentationDefinition;
    use crate::execution::workflow::resolution::{ContentDigestAlgorithm, WorkflowContentDigest};

    #[test]
    fn archived_presentation_plain_is_frozen_safe_and_fact_complete() {
        let attempt = succeeded_attempt();
        let plain = render_plain(&attempt, false).unwrap();

        for expected in [
            "run /tmp/archive-run",
            "result /tmp/archive-run/attempts/000001/result",
            "attempt 1 of 1 · current · initial · state succeeded · outcome succeeded",
            "workflow workflows/archive.yaml · 1 node · concurrency 2",
            "attempt created 2026-08-06 12:00:00Z · started 2026-08-06 12:00:01Z · settled 2026-08-06 12:00:05Z",
            "execution started 2026-08-06 12:00:01Z · finished 2026-08-06 12:00:04Z · duration 3.0s",
            "ordinary phase",
            "node prepare · role step · kind cmd · policy required · state succeeded · duration 1.0s · exit 0",
            "terminal counts: 1 succeeded · 0 failed · 0 blocked · 0 not-run · 0 cancelled · 0 advisory issues",
            "result succeeded · 3.0s total",
        ] {
            assert!(plain.contains(expected), "missing {expected:?}: {plain:?}");
        }
        for excluded in [
            "retained stdout",
            "retained stderr",
            "command:",
            "exports",
            "artifact",
            "observation",
            "transition",
        ] {
            assert!(
                !plain.contains(excluded),
                "included {excluded:?}: {plain:?}"
            );
        }
        assert!(!plain.contains('\u{1b}'));
        assert!(render_plain(&attempt, true).unwrap().contains('\u{1b}'));
    }

    #[test]
    fn archived_presentation_json_embeds_the_exact_validated_wire_value() {
        let projection = succeeded_attempt();
        let result_value = serde_json::json!({
            "schemaVersion": 1,
            "attemptNumber": 1,
            "workflow": {
                "path": "workflows/archive.yaml",
                "provenance": { "kind": "local", "sourceRoot": "/tmp/source" },
                "digest": { "algorithm": "sha256", "value": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
            },
            "execution": {
                "executionRoot": "/tmp/execution",
                "maximumParallelSteps": 2,
                "startedAt": "2026-08-06T12:00:01Z",
                "finishedAt": "2026-08-06T12:00:04Z",
                "durationMilliseconds": 3000
            },
            "commandOutputPolicy": {
                "encoding": "base64",
                "maximumRetainedBytesPerStream": 4194304
            },
            "outcome": "succeeded",
            "steps": [{
                "id": "prepare",
                "role": "step",
                "kind": "cmd",
                "failurePolicy": "required",
                "state": "succeeded",
                "startedAt": "2026-08-06T12:00:01Z",
                "durationMilliseconds": 1000,
                "commandOutput": {
                    "stdout": { "encoding": "base64", "data": "", "retainedBytes": 0, "discardedBytes": 0, "truncated": false, "fullyDrained": true },
                    "stderr": { "encoding": "base64", "data": "", "retainedBytes": 0, "discardedBytes": 0, "truncated": false, "fullyDrained": true }
                }
            }],
            "exports": {}
        });
        let loaded = LoadedLocalArchivedAttempt {
            projection,
            result: serde_json::from_value(result_value.clone()).unwrap(),
        };

        let bytes = serialize_json_success(&loaded).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert!(bytes.ends_with(b"\n"));
        assert!(!bytes.contains(&0x1b));
        assert_eq!(value["schemaVersion"], 1);
        assert_eq!(value["command"], COMMAND);
        assert_eq!(value["outcome"], "view");
        assert_eq!(value["exitStatus"], 0);
        assert_eq!(value["currentAttemptNumber"], 1);
        assert_eq!(value["attemptNumber"], 1);
        assert_eq!(value["selection"], "current");
        assert_eq!(value["trigger"], "initial");
        assert_eq!(value["attemptState"], "succeeded");
        assert_eq!(value["result"], result_value);
    }

    #[test]
    fn archived_presentation_json_errors_are_closed_bounded_and_identity_aware() {
        let operational = [
            ArchivedAttemptOperationalErrorCode::RunDirectoryUnavailable,
            ArchivedAttemptOperationalErrorCode::RunDirectoryInvalid,
            ArchivedAttemptOperationalErrorCode::RecoverySchemaUnsupported,
            ArchivedAttemptOperationalErrorCode::LockQueryFailed,
            ArchivedAttemptOperationalErrorCode::StatusSnapshotUnstable,
            ArchivedAttemptOperationalErrorCode::PublishedResultUnavailable,
            ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
            ArchivedAttemptOperationalErrorCode::RetainedWorkflowInvalid,
        ];
        for code in operational {
            let error = ArchivedAttemptLoadError::Operational(ArchivedAttemptOperationalError {
                code,
                run_directory: Some(PathBuf::from("/tmp/archive-run")),
            });
            assert_error_document(&error, operational_error_code(code), false);
        }

        let ineligible = [
            ArchivedAttemptIneligibilityReason::Unknown,
            ArchivedAttemptIneligibilityReason::Nonterminal,
            ArchivedAttemptIneligibilityReason::Interrupted,
            ArchivedAttemptIneligibilityReason::Rejected,
            ArchivedAttemptIneligibilityReason::PublicationFailed,
            ArchivedAttemptIneligibilityReason::Unpublished,
        ];
        for reason in ineligible {
            let error = ArchivedAttemptLoadError::Ineligible(ArchivedAttemptIneligible {
                run_directory: PathBuf::from("/tmp/archive-run"),
                attempt_number: 3,
                reason,
            });
            assert_error_document(&error, ineligibility_code(reason), true);
        }
    }

    fn assert_error_document(
        error: &ArchivedAttemptLoadError,
        expected_code: &str,
        has_attempt: bool,
    ) {
        let bytes = serialize_json_error(error).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let object = value.as_object().unwrap();
        assert_eq!(
            object.keys().cloned().collect::<Vec<_>>(),
            if has_attempt {
                vec![
                    "attemptNumber",
                    "command",
                    "error",
                    "exitStatus",
                    "outcome",
                    "runDirectory",
                    "schemaVersion",
                ]
            } else {
                vec![
                    "command",
                    "error",
                    "exitStatus",
                    "outcome",
                    "runDirectory",
                    "schemaVersion",
                ]
            }
        );
        assert_eq!(value["error"]["code"], expected_code);
        let message = value["error"]["message"].as_str().unwrap();
        assert!(!message.is_empty());
        assert!(message.len() <= 256);
        assert!(!message.contains('\u{1b}'));
        assert_eq!(value["exitStatus"], 1);
        assert_eq!(value["runDirectory"], "/tmp/archive-run");
        assert_eq!(value.get("attemptNumber").is_some(), has_attempt);
    }

    fn succeeded_attempt() -> LocalArchivedAttempt {
        let started = timestamp_value("2026-08-06T12:00:01Z");
        let definition = WorkflowPresentationStep::Command {
            argv: vec!["printf".to_owned(), "not rendered".to_owned()],
            cwd: None,
            failure_policy: FailurePolicy::Required,
            direct_dependencies: Vec::new(),
            outputs: BTreeMap::new(),
        };
        // This compact pure-renderer fixture intentionally owns different terminal facts
        // from the richer archived-TUI interaction fixture.
        // jscpd:ignore-start
        LocalArchivedAttempt {
            run_directory: PathBuf::from("/tmp/archive-run"),
            current_attempt_number: 1,
            attempt_number: 1,
            prior_attempt_number: None,
            result_directory: PathBuf::from("/tmp/archive-run/attempts/000001/result"),
            trigger: ArchivedAttemptTrigger::Initial,
            state: ArchivedAttemptState::Succeeded,
            created_at: timestamp_value("2026-08-06T12:00:00Z"),
            started_at: Some(started),
            settled_at: timestamp_value("2026-08-06T12:00:05Z"),
            workflow_path: "workflows/archive.yaml".to_owned(),
            source_root: PathBuf::from("/tmp/source"),
            workflow_digest: WorkflowContentDigest {
                algorithm: ContentDigestAlgorithm::Sha256,
                value: "a".repeat(64),
            },
            workflow: WorkflowPresentationDefinition {
                workflow_path: "workflows/archive.yaml".to_owned(),
                presentation_order: vec!["prepare".to_owned()],
                finalization_start: None,
                steps: BTreeMap::from([("prepare".to_owned(), definition)]),
                node_roles: BTreeMap::from([("prepare".to_owned(), WorkflowNodeRole::Step)]),
            },
            // jscpd:ignore-end
            execution: ArchivedExecution {
                execution_root: PathBuf::from("/tmp/execution"),
                maximum_parallel_steps: 2,
                started_at: started,
                finished_at: timestamp_value("2026-08-06T12:00:04Z"),
                duration: Duration::from_secs(3),
            },
            outcome: ArchivedWorkflowOutcome::Succeeded,
            primary_failure: None,
            cancellation: None,
            finalization: None,
            steps: vec![ArchivedStep {
                id: "prepare".to_owned(),
                role: WorkflowNodeRole::Step,
                failure_policy: FailurePolicy::Required,
                state: ArchivedStepState::Succeeded,
                started_at: Some(started),
                duration: Some(Duration::from_secs(1)),
                detail: ArchivedStepDetail::Succeeded,
                command_output: None,
                recovery: None,
                invocations: Vec::new(),
            }],
        }
    }

    fn timestamp_value(value: &str) -> time::OffsetDateTime {
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).unwrap()
    }
}
