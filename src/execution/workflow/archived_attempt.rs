use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::num::NonZeroU64;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use rustix::fd::OwnedFd;
use rustix::fs::{AtFlags, FileType, Mode, OFlags, fstat, openat, statat};
use rustix::io::dup;
use time::OffsetDateTime;

use super::artifact_set;
use super::document::{FailurePolicy, Output};
use super::local_run::{
    AttemptResultV1, AttemptStateV1, AttemptStepStateV1, AttemptTriggerV1, LocalAttemptV1,
    LocalStatusError, LocalStatusErrorCode, RetainedReadBudget, StableLocalRunSnapshot,
    load_retained_execution_with_budget, open_directory_at, read_stable_local_run_snapshot,
};
use super::presentation_feed::WorkflowPresentationDefinition;
use super::publication::{
    CancellationReasonV1, CommandOutputV1, DiagnosticStreamV1, ExportUnavailableReasonV1, ExportV1,
    FailureCauseV1, FailureCodeV1, FailurePhaseV1, FailureV1, PrimaryFailureV1, StepReasonV1,
    WorkflowOutcomeV1, WorkflowResultV1, WorkflowStepStateV1, WorkflowStepV1,
};
use super::resolution::WorkflowContentDigest;
use super::result_metadata;
use super::schema_common::{
    is_canonical_absolute_path, is_canonical_relative_path, is_lowercase_hex,
    parse_canonical_utc_timestamp,
};
use super::validated::{ResolvedDirectPrerequisite, ValidatedStep, WorkflowValueType};

const RESULT_FILE: &str = "result.json";
const SHA256_ALGORITHM: &str = "sha256";
const BASE64_ENCODING: &str = "base64";
const LOCAL_PROVENANCE: &str = "local";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchivedAttemptOperationalErrorCode {
    RunDirectoryUnavailable,
    RunDirectoryInvalid,
    LockQueryFailed,
    StatusSnapshotUnstable,
    PublishedResultUnavailable,
    PublishedResultInvalid,
    RetainedWorkflowInvalid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArchivedAttemptOperationalError {
    pub(crate) code: ArchivedAttemptOperationalErrorCode,
    pub(crate) run_directory: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchivedAttemptIneligibilityReason {
    Unknown,
    Nonterminal,
    Interrupted,
    Rejected,
    PublicationFailed,
    Unpublished,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArchivedAttemptIneligible {
    pub(crate) run_directory: PathBuf,
    pub(crate) attempt_number: u64,
    pub(crate) reason: ArchivedAttemptIneligibilityReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ArchivedAttemptLoadError {
    Operational(ArchivedAttemptOperationalError),
    Ineligible(ArchivedAttemptIneligible),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchivedAttemptTrigger {
    Initial,
    ExplicitRetry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchivedAttemptState {
    Succeeded,
    WorkflowFailed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchivedWorkflowOutcome {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchivedStepState {
    Succeeded,
    Failed,
    Blocked,
    NotRun,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchivedFailurePhase {
    Start,
    Execution,
    OutputCapture,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchivedCancellationReason {
    UserRequest,
    TerminationRequest,
    CallerOutputFailure,
    RunnerShutdown,
}

pub(crate) type ArchivedFailureCode = FailureCodeV1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArchivedFailureCause {
    pub(crate) code: ArchivedFailureCode,
    pub(crate) input: Option<String>,
    pub(crate) collection_index: Option<usize>,
    pub(crate) output: Option<String>,
    pub(crate) exit_code: Option<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArchivedFailure {
    pub(crate) phase: ArchivedFailurePhase,
    pub(crate) cause: ArchivedFailureCause,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArchivedPrimaryFailure {
    pub(crate) step: String,
    pub(crate) failure: ArchivedFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArchivedCancellation {
    pub(crate) reason: ArchivedCancellationReason,
    pub(crate) requested_at: OffsetDateTime,
    pub(crate) force_stop_deadline: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArchivedExecution {
    pub(crate) execution_root: PathBuf,
    pub(crate) maximum_parallel_steps: usize,
    pub(crate) started_at: OffsetDateTime,
    pub(crate) finished_at: OffsetDateTime,
    pub(crate) duration: Duration,
}

// The archive projection owns decoded bytes rather than the result wire encoding, so a
// separate type keeps untrusted deserialization out of the presentation model.
// jscpd:ignore-start
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArchivedDiagnosticStream {
    pub(crate) bytes: Arc<[u8]>,
    pub(crate) retained_bytes: u64,
    pub(crate) discarded_bytes: u64,
    pub(crate) truncated: bool,
    pub(crate) fully_drained: bool,
}
// jscpd:ignore-end

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArchivedCommandOutput {
    pub(crate) stdout: ArchivedDiagnosticStream,
    pub(crate) stderr: ArchivedDiagnosticStream,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ArchivedStepDetail {
    Succeeded,
    Failed(ArchivedFailure),
    Blocked { dependency: String },
    NotRun,
    Cancelled { reason: ArchivedCancellationReason },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArchivedStep {
    pub(crate) id: String,
    pub(crate) failure_policy: FailurePolicy,
    pub(crate) state: ArchivedStepState,
    pub(crate) started_at: Option<OffsetDateTime>,
    pub(crate) duration: Option<Duration>,
    pub(crate) detail: ArchivedStepDetail,
    pub(crate) command_output: Option<ArchivedCommandOutput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalArchivedAttempt {
    pub(crate) run_directory: PathBuf,
    pub(crate) current_attempt_number: u64,
    pub(crate) attempt_number: u64,
    pub(crate) prior_attempt_number: Option<u64>,
    pub(crate) result_directory: PathBuf,
    pub(crate) trigger: ArchivedAttemptTrigger,
    pub(crate) state: ArchivedAttemptState,
    pub(crate) created_at: OffsetDateTime,
    pub(crate) started_at: Option<OffsetDateTime>,
    pub(crate) settled_at: OffsetDateTime,
    pub(crate) workflow_path: String,
    pub(crate) source_root: PathBuf,
    pub(crate) workflow_digest: WorkflowContentDigest,
    pub(crate) workflow: WorkflowPresentationDefinition,
    pub(crate) execution: ArchivedExecution,
    pub(crate) outcome: ArchivedWorkflowOutcome,
    pub(crate) primary_failure: Option<ArchivedPrimaryFailure>,
    pub(crate) cancellation: Option<ArchivedCancellation>,
    pub(crate) steps: Vec<ArchivedStep>,
}

pub(crate) fn load_local_archived_attempt(
    requested: &Path,
    requested_attempt: Option<NonZeroU64>,
) -> Result<LocalArchivedAttempt, ArchivedAttemptLoadError> {
    load_local_archived_attempt_with(requested, requested_attempt, &mut NoopArchiveReadObserver)
}

trait ArchiveReadObserver {
    fn stable_snapshot_acquired(&mut self, _run_directory: &Path) {}

    fn result_file_opened(&mut self, _result_directory: &Path) {}
}

struct NoopArchiveReadObserver;

impl ArchiveReadObserver for NoopArchiveReadObserver {}

#[cfg(test)]
pub(crate) fn load_local_archived_attempt_observed<Snapshot, ResultFile>(
    requested: &Path,
    requested_attempt: Option<NonZeroU64>,
    snapshot_acquired: Snapshot,
    result_file_opened: ResultFile,
) -> Result<LocalArchivedAttempt, ArchivedAttemptLoadError>
where
    Snapshot: FnMut(&Path),
    ResultFile: FnMut(&Path),
{
    load_local_archived_attempt_with(
        requested,
        requested_attempt,
        &mut CallbackArchiveReadObserver {
            snapshot_acquired,
            result_file_opened,
        },
    )
}

#[cfg(test)]
struct CallbackArchiveReadObserver<Snapshot, ResultFile> {
    snapshot_acquired: Snapshot,
    result_file_opened: ResultFile,
}

#[cfg(test)]
impl<Snapshot, ResultFile> ArchiveReadObserver for CallbackArchiveReadObserver<Snapshot, ResultFile>
where
    Snapshot: FnMut(&Path),
    ResultFile: FnMut(&Path),
{
    fn stable_snapshot_acquired(&mut self, run_directory: &Path) {
        (self.snapshot_acquired)(run_directory);
    }

    fn result_file_opened(&mut self, result_directory: &Path) {
        (self.result_file_opened)(result_directory);
    }
}

fn load_local_archived_attempt_with(
    requested: &Path,
    requested_attempt: Option<NonZeroU64>,
    observer: &mut impl ArchiveReadObserver,
) -> Result<LocalArchivedAttempt, ArchivedAttemptLoadError> {
    let snapshot = read_stable_local_run_snapshot(requested).map_err(map_status_error)?;
    observer.stable_snapshot_acquired(&snapshot.run_directory);
    let selected_number =
        requested_attempt.map_or(snapshot.state.current_attempt_number, u64::from);
    let attempt = select_published_attempt(&snapshot, selected_number)?.clone();
    let AttemptResultV1::Published {
        relative_directory: relative_result_directory,
    } = &attempt.result
    else {
        return Err(result_invalid(&snapshot.run_directory));
    };
    let relative_result_directory = relative_result_directory.clone();
    let result_directory = snapshot
        .run_directory
        .join(Path::new(&relative_result_directory));
    let result_root = open_relative_directory(&snapshot.root, &relative_result_directory)
        .map_err(|()| result_unavailable(&snapshot.run_directory))?;
    let mut retained_budget = RetainedReadBudget::with_bytes(snapshot.retained_json_bytes)
        .map_err(|_| result_invalid(&snapshot.run_directory))?;
    let (workflow, _, maximum_parallel_steps) =
        load_retained_execution_with_budget(&snapshot.root, &snapshot.run, &mut retained_budget)
            .map_err(|_| {
                ArchivedAttemptLoadError::Operational(ArchivedAttemptOperationalError {
                    code: ArchivedAttemptOperationalErrorCode::RetainedWorkflowInvalid,
                    run_directory: Some(snapshot.run_directory.clone()),
                })
            })?;
    let result_bytes = read_immutable_result(
        &result_root,
        observer,
        &result_directory,
        result_metadata::MAXIMUM_RESULT_JSON_BYTES,
    )
    .map_err(|()| result_unavailable(&snapshot.run_directory))?;
    retained_budget
        .account(&result_bytes)
        .map_err(|_| result_invalid(&snapshot.run_directory))?;
    let result =
        decode_result(&result_bytes).map_err(|()| result_invalid(&snapshot.run_directory))?;
    artifact_set::validate(&result_root, &result)
        .map_err(|_| result_invalid(&snapshot.run_directory))?;
    let validated = validate_and_project_result(
        &snapshot,
        &attempt,
        &result,
        &workflow,
        maximum_parallel_steps,
    )
    .map_err(|()| result_invalid(&snapshot.run_directory))?;

    Ok(LocalArchivedAttempt {
        run_directory: snapshot.run_directory.clone(),
        current_attempt_number: snapshot.state.current_attempt_number,
        attempt_number: attempt.attempt_number,
        prior_attempt_number: attempt.prior_attempt_number,
        result_directory,
        trigger: match attempt.trigger {
            AttemptTriggerV1::Initial => ArchivedAttemptTrigger::Initial,
            AttemptTriggerV1::ExplicitRetry => ArchivedAttemptTrigger::ExplicitRetry,
        },
        state: validated.state,
        created_at: parse_canonical_utc_timestamp(&attempt.created_at)
            .ok_or_else(|| result_invalid(&snapshot.run_directory))?,
        started_at: match attempt.started_at.as_deref() {
            Some(value) => Some(
                parse_canonical_utc_timestamp(value)
                    .ok_or_else(|| result_invalid(&snapshot.run_directory))?,
            ),
            None => None,
        },
        settled_at: attempt
            .settled_at
            .as_deref()
            .and_then(parse_canonical_utc_timestamp)
            .ok_or_else(|| result_invalid(&snapshot.run_directory))?,
        workflow_path: workflow.source.workflow_path.clone(),
        source_root: workflow.source.source_root.clone(),
        workflow_digest: workflow.content_digest.clone(),
        workflow: WorkflowPresentationDefinition::from_workflow(&workflow),
        execution: validated.execution,
        outcome: validated.outcome,
        primary_failure: validated.primary_failure,
        cancellation: validated.cancellation,
        steps: validated.steps,
    })
}

fn select_published_attempt(
    snapshot: &StableLocalRunSnapshot,
    selected_number: u64,
) -> Result<&LocalAttemptV1, ArchivedAttemptLoadError> {
    let ineligible = |reason| {
        ArchivedAttemptLoadError::Ineligible(ArchivedAttemptIneligible {
            run_directory: snapshot.run_directory.clone(),
            attempt_number: selected_number,
            reason,
        })
    };
    let attempt = snapshot
        .state
        .attempts
        .iter()
        .find(|attempt| attempt.attempt_number == selected_number)
        .ok_or_else(|| ineligible(ArchivedAttemptIneligibilityReason::Unknown))?;
    match attempt.state {
        AttemptStateV1::Created | AttemptStateV1::Running | AttemptStateV1::Cancelling => {
            return Err(ineligible(ArchivedAttemptIneligibilityReason::Nonterminal));
        }
        AttemptStateV1::Interrupted => {
            return Err(ineligible(ArchivedAttemptIneligibilityReason::Interrupted));
        }
        AttemptStateV1::Rejected => {
            return Err(ineligible(ArchivedAttemptIneligibilityReason::Rejected));
        }
        AttemptStateV1::Succeeded | AttemptStateV1::WorkflowFailed | AttemptStateV1::Cancelled => {}
    }
    match attempt.result {
        AttemptResultV1::Published { .. } => Ok(attempt),
        AttemptResultV1::PublicationFailed { .. } => Err(ineligible(
            ArchivedAttemptIneligibilityReason::PublicationFailed,
        )),
        AttemptResultV1::NotPublished { .. } => {
            Err(ineligible(ArchivedAttemptIneligibilityReason::Unpublished))
        }
    }
}

fn map_status_error(error: LocalStatusError) -> ArchivedAttemptLoadError {
    let code = match error.code {
        LocalStatusErrorCode::RunDirectoryUnavailable => {
            ArchivedAttemptOperationalErrorCode::RunDirectoryUnavailable
        }
        LocalStatusErrorCode::RunDirectoryInvalid => {
            ArchivedAttemptOperationalErrorCode::RunDirectoryInvalid
        }
        LocalStatusErrorCode::LockQueryFailed => {
            ArchivedAttemptOperationalErrorCode::LockQueryFailed
        }
        LocalStatusErrorCode::StatusSnapshotUnstable => {
            ArchivedAttemptOperationalErrorCode::StatusSnapshotUnstable
        }
    };
    ArchivedAttemptLoadError::Operational(ArchivedAttemptOperationalError {
        code,
        run_directory: error.run_directory,
    })
}

fn result_unavailable(run_directory: &Path) -> ArchivedAttemptLoadError {
    ArchivedAttemptLoadError::Operational(ArchivedAttemptOperationalError {
        code: ArchivedAttemptOperationalErrorCode::PublishedResultUnavailable,
        run_directory: Some(run_directory.to_owned()),
    })
}

fn result_invalid(run_directory: &Path) -> ArchivedAttemptLoadError {
    ArchivedAttemptLoadError::Operational(ArchivedAttemptOperationalError {
        code: ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
        run_directory: Some(run_directory.to_owned()),
    })
}

fn open_relative_directory(root: &OwnedFd, relative: &str) -> Result<OwnedFd, ()> {
    let mut directory = dup(root).map_err(|_| ())?;
    for component in Path::new(relative).components() {
        let Component::Normal(name) = component else {
            return Err(());
        };
        directory = open_directory_at(&directory, name).map_err(|_| ())?;
    }
    Ok(directory)
}

fn read_immutable_result(
    result_root: &OwnedFd,
    observer: &mut impl ArchiveReadObserver,
    result_directory: &Path,
    maximum_bytes: u64,
) -> Result<Vec<u8>, ()> {
    let descriptor = openat(
        result_root,
        RESULT_FILE,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ())?;
    let opened = fstat(&descriptor).map_err(|_| ())?;
    let opened_size = u64::try_from(opened.st_size).map_err(|_| ())?;
    if FileType::from_raw_mode(opened.st_mode) != FileType::RegularFile
        || opened_size > maximum_bytes
    {
        return Err(());
    }
    observer.result_file_opened(result_directory);
    let mut file = File::from(descriptor);
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum_bytes.checked_add(1).ok_or(())?)
        .read_to_end(&mut bytes)
        .map_err(|_| ())?;
    if u64::try_from(bytes.len())
        .ok()
        .is_none_or(|size| size > maximum_bytes || size != opened_size)
    {
        return Err(());
    }
    let opened_after = fstat(&file).map_err(|_| ())?;
    let named_after =
        statat(result_root, RESULT_FILE, AtFlags::SYMLINK_NOFOLLOW).map_err(|_| ())?;
    if FileType::from_raw_mode(named_after.st_mode) != FileType::RegularFile
        || opened.st_dev != opened_after.st_dev
        || opened.st_ino != opened_after.st_ino
        || opened.st_dev != named_after.st_dev
        || opened.st_ino != named_after.st_ino
        || opened.st_size != opened_after.st_size
    {
        return Err(());
    }
    Ok(bytes)
}

fn decode_result(bytes: &[u8]) -> Result<WorkflowResultV1, ()> {
    result_metadata::decode(bytes).map_err(|_| ())
}

struct ProjectedResult {
    state: ArchivedAttemptState,
    execution: ArchivedExecution,
    outcome: ArchivedWorkflowOutcome,
    primary_failure: Option<ArchivedPrimaryFailure>,
    cancellation: Option<ArchivedCancellation>,
    steps: Vec<ArchivedStep>,
}

fn validate_and_project_result(
    snapshot: &StableLocalRunSnapshot,
    attempt: &LocalAttemptV1,
    result: &WorkflowResultV1,
    workflow: &super::resolution::ResolvedWorkflow,
    maximum_parallel_steps: usize,
) -> Result<ProjectedResult, ()> {
    if result.attempt_number != attempt.attempt_number
        || result.workflow.path != workflow.source.workflow_path
        || result.workflow.provenance.kind != LOCAL_PROVENANCE
        || Path::new(&result.workflow.provenance.source_root) != workflow.source.source_root
        || result.workflow.digest.algorithm != SHA256_ALGORITHM
        || result.workflow.digest.value != workflow.content_digest.value
        || result.workflow.digest.algorithm != snapshot.run.workflow_digest.algorithm
        || result.workflow.digest.value != snapshot.run.workflow_digest.value
        || result.execution.execution_root != attempt.execution_root
        || result.execution.maximum_parallel_steps != maximum_parallel_steps
        || result.command_output_policy.encoding != BASE64_ENCODING
        || result
            .command_output_policy
            .maximum_retained_bytes_per_stream
            != super::MAXIMUM_RETAINED_BYTES_PER_STREAM
        || !is_canonical_relative_path(&result.workflow.path)
        || !is_canonical_absolute_path(&result.workflow.provenance.source_root)
        || !is_canonical_absolute_path(&result.execution.execution_root)
        || !valid_digest(
            &result.workflow.digest.algorithm,
            &result.workflow.digest.value,
        )
    {
        return Err(());
    }

    let state = match (attempt.state, result.outcome) {
        (AttemptStateV1::Succeeded, WorkflowOutcomeV1::Succeeded) => {
            ArchivedAttemptState::Succeeded
        }
        (AttemptStateV1::WorkflowFailed, WorkflowOutcomeV1::Failed) => {
            ArchivedAttemptState::WorkflowFailed
        }
        (AttemptStateV1::Cancelled, WorkflowOutcomeV1::Cancelled) => {
            ArchivedAttemptState::Cancelled
        }
        _ => return Err(()),
    };
    let outcome = match result.outcome {
        WorkflowOutcomeV1::Succeeded => ArchivedWorkflowOutcome::Succeeded,
        WorkflowOutcomeV1::Failed => ArchivedWorkflowOutcome::Failed,
        WorkflowOutcomeV1::Cancelled => ArchivedWorkflowOutcome::Cancelled,
    };
    let execution = ArchivedExecution {
        execution_root: PathBuf::from(&result.execution.execution_root),
        maximum_parallel_steps,
        started_at: parse_canonical_utc_timestamp(&result.execution.started_at).ok_or(())?,
        finished_at: parse_canonical_utc_timestamp(&result.execution.finished_at).ok_or(())?,
        duration: Duration::from_millis(result.execution.duration_milliseconds),
    };
    let steps = project_steps(attempt, result, workflow)?;
    let primary_failure = project_primary_failure(result, &steps)?;
    validate_terminal_step_facts(result.outcome, &steps, workflow)?;
    let cancellation = project_cancellation(attempt, result)?;
    if steps.iter().any(|step| {
        matches!(
            &step.detail,
            ArchivedStepDetail::Cancelled { reason }
                if cancellation.as_ref().map(|value| value.reason) != Some(*reason)
        )
    }) {
        return Err(());
    }
    validate_exports(result, workflow, &steps)?;

    Ok(ProjectedResult {
        state,
        execution,
        outcome,
        primary_failure,
        cancellation,
        steps,
    })
}

fn project_steps(
    attempt: &LocalAttemptV1,
    result: &WorkflowResultV1,
    workflow: &super::resolution::ResolvedWorkflow,
) -> Result<Vec<ArchivedStep>, ()> {
    if result.steps.len() != workflow.definition.presentation_order.len()
        || attempt.progress.steps.len() != result.steps.len()
    {
        return Err(());
    }
    let maximum_stream_bytes = super::maximum_retained_bytes_per_stream(result.steps.len());
    result
        .steps
        .iter()
        .zip(&attempt.progress.steps)
        .zip(&workflow.definition.presentation_order)
        .map(|((step, durable), expected_id)| {
            let definition = workflow.definition.steps.get(expected_id).ok_or(())?;
            if step.id != *expected_id
                || durable.id != *expected_id
                || step.failure_policy != step_failure_policy(definition)
                || durable.failure_policy != step_failure_policy(definition)
                || !step_kind_matches(&step.kind, definition)
                || !step_state_matches(step.state, durable.state)
            {
                return Err(());
            }
            project_step(step, definition, maximum_stream_bytes)
        })
        .collect()
}

fn validate_terminal_step_facts(
    outcome: WorkflowOutcomeV1,
    steps: &[ArchivedStep],
    workflow: &super::resolution::ResolvedWorkflow,
) -> Result<(), ()> {
    for step in steps {
        match &step.detail {
            ArchivedStepDetail::Blocked { dependency } => {
                let definition = workflow.definition.steps.get(&step.id).ok_or(())?;
                let recorded_prerequisite = direct_prerequisites(definition)
                    .iter()
                    .find(|prerequisite| prerequisite.producer == *dependency)
                    .ok_or(())?;
                if prerequisite_satisfied(recorded_prerequisite, steps) {
                    return Err(());
                }
            }
            ArchivedStepDetail::NotRun => {
                let definition = workflow.definition.steps.get(&step.id).ok_or(())?;
                if direct_prerequisites(definition)
                    .iter()
                    .any(|prerequisite| !prerequisite_satisfied(prerequisite, steps))
                {
                    return Err(());
                }
            }
            ArchivedStepDetail::Succeeded
            | ArchivedStepDetail::Failed(_)
            | ArchivedStepDetail::Cancelled { .. } => {}
        }
    }

    let valid_outcome = match outcome {
        WorkflowOutcomeV1::Succeeded => steps.iter().all(step_succeeds_workflow),
        WorkflowOutcomeV1::Failed => true,
        WorkflowOutcomeV1::Cancelled => {
            steps.iter().all(|step| {
                step_succeeds_workflow(step) || step.state == ArchivedStepState::Cancelled
            }) && steps
                .iter()
                .any(|step| step.state == ArchivedStepState::Cancelled)
        }
    };
    valid_outcome.then_some(()).ok_or(())
}

fn project_step(
    step: &WorkflowStepV1,
    definition: &ValidatedStep,
    maximum_stream_bytes: u64,
) -> Result<ArchivedStep, ()> {
    let (started_at, duration) = match (&step.started_at, step.duration_milliseconds) {
        (Some(started_at), Some(duration)) => (
            Some(parse_canonical_utc_timestamp(started_at).ok_or(())?),
            Some(Duration::from_millis(duration)),
        ),
        (None, None) => (None, None),
        (Some(_), None) | (None, Some(_)) => return Err(()),
    };
    let (state, detail) = match step.state {
        WorkflowStepStateV1::Succeeded => {
            exact_step_fields(step, false, false, false)?;
            (ArchivedStepState::Succeeded, ArchivedStepDetail::Succeeded)
        }
        WorkflowStepStateV1::Failed => {
            exact_step_fields(step, true, false, false)?;
            let failure = project_failure(step.failure.as_ref().ok_or(())?)?;
            validate_failure_binding(&failure.cause, definition)?;
            (
                ArchivedStepState::Failed,
                ArchivedStepDetail::Failed(failure),
            )
        }
        WorkflowStepStateV1::Blocked => {
            exact_step_fields(step, false, true, false)?;
            let dependency = step.dependency.clone().ok_or(())?;
            if !direct_prerequisites(definition)
                .iter()
                .any(|prerequisite| prerequisite.producer == dependency)
            {
                return Err(());
            }
            (
                ArchivedStepState::Blocked,
                ArchivedStepDetail::Blocked { dependency },
            )
        }
        WorkflowStepStateV1::NotRun => {
            exact_step_fields(step, false, false, true)?;
            if step.reason != Some(StepReasonV1::FailureStop) {
                return Err(());
            }
            (ArchivedStepState::NotRun, ArchivedStepDetail::NotRun)
        }
        WorkflowStepStateV1::Cancelled => {
            exact_step_fields(step, false, false, true)?;
            let reason = cancellation_step_reason(step.reason.ok_or(())?)?;
            (
                ArchivedStepState::Cancelled,
                ArchivedStepDetail::Cancelled { reason },
            )
        }
    };
    let command_output = match (&step.command_output, definition) {
        (Some(output), ValidatedStep::Command(_)) => {
            Some(project_command_output(output, maximum_stream_bytes)?)
        }
        (None, ValidatedStep::Command(_) | ValidatedStep::Agent(_)) => None,
        (Some(_), ValidatedStep::Agent(_)) => return Err(()),
    };
    let timing_present = started_at.is_some();
    let output_present = command_output.is_some();
    let valid_timing = match state {
        ArchivedStepState::Succeeded | ArchivedStepState::Failed => timing_present,
        ArchivedStepState::Blocked | ArchivedStepState::NotRun => !timing_present,
        ArchivedStepState::Cancelled => !output_present || timing_present,
    };
    let valid_output = match (definition, &detail) {
        (ValidatedStep::Agent(_), _) => !output_present,
        (ValidatedStep::Command(_), ArchivedStepDetail::Succeeded) => output_present,
        (ValidatedStep::Command(_), ArchivedStepDetail::Failed(failure)) => {
            output_present == (failure.phase != ArchivedFailurePhase::Start)
        }
        (
            ValidatedStep::Command(_),
            ArchivedStepDetail::Blocked { .. } | ArchivedStepDetail::NotRun,
        ) => !output_present,
        (ValidatedStep::Command(_), ArchivedStepDetail::Cancelled { .. }) => true,
    };
    if !valid_timing || !valid_output {
        return Err(());
    }
    Ok(ArchivedStep {
        id: step.id.clone(),
        failure_policy: step.failure_policy,
        state,
        started_at,
        duration,
        detail,
        command_output,
    })
}

fn exact_step_fields(
    step: &WorkflowStepV1,
    failure: bool,
    dependency: bool,
    reason: bool,
) -> Result<(), ()> {
    if step.failure.is_some() != failure
        || step.dependency.is_some() != dependency
        || step.reason.is_some() != reason
    {
        return Err(());
    }
    Ok(())
}

fn project_command_output(
    output: &CommandOutputV1,
    maximum_stream_bytes: u64,
) -> Result<ArchivedCommandOutput, ()> {
    Ok(ArchivedCommandOutput {
        stdout: project_diagnostic_stream(&output.stdout, maximum_stream_bytes)?,
        stderr: project_diagnostic_stream(&output.stderr, maximum_stream_bytes)?,
    })
}

fn project_diagnostic_stream(
    stream: &DiagnosticStreamV1,
    maximum_stream_bytes: u64,
) -> Result<ArchivedDiagnosticStream, ()> {
    if stream.encoding != BASE64_ENCODING
        || stream.retained_bytes > maximum_stream_bytes
        || stream.truncated != (stream.discarded_bytes != 0)
        || (stream.discarded_bytes != 0 && stream.retained_bytes != maximum_stream_bytes)
    {
        return Err(());
    }
    let bytes = BASE64_STANDARD.decode(&stream.data).map_err(|_| ())?;
    if u64::try_from(bytes.len()).map_err(|_| ())? != stream.retained_bytes
        || BASE64_STANDARD.encode(&bytes) != stream.data
    {
        return Err(());
    }
    Ok(ArchivedDiagnosticStream {
        bytes: Arc::from(bytes),
        retained_bytes: stream.retained_bytes,
        discarded_bytes: stream.discarded_bytes,
        truncated: stream.truncated,
        fully_drained: stream.fully_drained,
    })
}

fn project_primary_failure(
    result: &WorkflowResultV1,
    steps: &[ArchivedStep],
) -> Result<Option<ArchivedPrimaryFailure>, ()> {
    match result.outcome {
        WorkflowOutcomeV1::Succeeded | WorkflowOutcomeV1::Cancelled
            if result.primary_failure.is_some() =>
        {
            Err(())
        }
        WorkflowOutcomeV1::Failed => {
            let primary = result.primary_failure.as_ref().ok_or(())?;
            let projected = project_primary(primary)?;
            let step = steps
                .iter()
                .find(|step| step.id == primary.step)
                .ok_or(())?;
            if step.failure_policy != FailurePolicy::Required
                || !matches!(&step.detail, ArchivedStepDetail::Failed(failure) if *failure == projected.failure)
            {
                return Err(());
            }
            Ok(Some(projected))
        }
        WorkflowOutcomeV1::Succeeded | WorkflowOutcomeV1::Cancelled => Ok(None),
    }
}

fn project_primary(primary: &PrimaryFailureV1) -> Result<ArchivedPrimaryFailure, ()> {
    Ok(ArchivedPrimaryFailure {
        step: primary.step.clone(),
        failure: project_failure(&FailureV1 {
            phase: primary.phase,
            cause: primary.cause.clone(),
        })?,
    })
}

fn project_failure(failure: &FailureV1) -> Result<ArchivedFailure, ()> {
    let phase = match failure.phase {
        FailurePhaseV1::Start => ArchivedFailurePhase::Start,
        FailurePhaseV1::Execution => ArchivedFailurePhase::Execution,
        FailurePhaseV1::OutputCapture => ArchivedFailurePhase::OutputCapture,
    };
    let cause = project_failure_cause(&failure.cause);
    Ok(ArchivedFailure { phase, cause })
}

fn project_failure_cause(cause: &FailureCauseV1) -> ArchivedFailureCause {
    ArchivedFailureCause {
        code: cause.code,
        input: cause.input.clone(),
        collection_index: cause.collection_index,
        output: cause.output.clone(),
        exit_code: cause.exit_code,
    }
}

fn validate_failure_binding(
    cause: &ArchivedFailureCause,
    definition: &ValidatedStep,
) -> Result<(), ()> {
    if result_metadata::is_input_failure_code(cause.code) {
        let ValidatedStep::Command(command) = definition else {
            return Err(());
        };
        if cause.code == ArchivedFailureCode::InputInvalidName {
            return Ok(());
        }
        let binding = match cause.input.as_deref() {
            Some(input) => Some(command.inputs.get(input).ok_or(())?),
            None if cause.collection_index.is_none() => None,
            None => return Err(()),
        };
        if cause.collection_index.is_some()
            && binding
                .is_none_or(|binding| binding.value_type != WorkflowValueType::AttachmentCollection)
        {
            return Err(());
        }
    } else if result_metadata::is_output_failure_code(cause.code) {
        let output = cause.output.as_deref().ok_or(())?;
        let outputs = match definition {
            ValidatedStep::Command(command) => &command.common.outputs,
            ValidatedStep::Agent(agent) => &agent.common.outputs,
        };
        if !outputs.contains_key(output) {
            return Err(());
        }
    }
    Ok(())
}

fn project_cancellation(
    attempt: &LocalAttemptV1,
    result: &WorkflowResultV1,
) -> Result<Option<ArchivedCancellation>, ()> {
    let required = match result.outcome {
        WorkflowOutcomeV1::Succeeded => false,
        WorkflowOutcomeV1::Failed => attempt.cancellation.is_some(),
        WorkflowOutcomeV1::Cancelled => true,
    };
    if result.cancellation.is_some() != required || attempt.cancellation.is_some() != required {
        return Err(());
    }
    let Some(result_cancellation) = &result.cancellation else {
        return Ok(None);
    };
    let durable = attempt.cancellation.as_ref().ok_or(())?;
    let reason = cancellation_reason(result_cancellation.reason)?;
    if durable.reason != result_cancellation.reason
        || durable.force_stop_deadline != result_cancellation.force_stop_deadline
    {
        return Err(());
    }
    Ok(Some(ArchivedCancellation {
        reason,
        requested_at: parse_canonical_utc_timestamp(&durable.requested_at).ok_or(())?,
        force_stop_deadline: parse_canonical_utc_timestamp(
            &result_cancellation.force_stop_deadline,
        )
        .ok_or(())?,
    }))
}

fn cancellation_reason(reason: CancellationReasonV1) -> Result<ArchivedCancellationReason, ()> {
    match reason {
        CancellationReasonV1::UserRequest => Ok(ArchivedCancellationReason::UserRequest),
        CancellationReasonV1::TerminationRequest => {
            Ok(ArchivedCancellationReason::TerminationRequest)
        }
        CancellationReasonV1::CallerOutputFailure => {
            Ok(ArchivedCancellationReason::CallerOutputFailure)
        }
        CancellationReasonV1::RunnerShutdown => Ok(ArchivedCancellationReason::RunnerShutdown),
        CancellationReasonV1::ExecutionLeaseExpired => Err(()),
    }
}

fn cancellation_step_reason(reason: StepReasonV1) -> Result<ArchivedCancellationReason, ()> {
    match reason {
        StepReasonV1::UserRequest => Ok(ArchivedCancellationReason::UserRequest),
        StepReasonV1::TerminationRequest => Ok(ArchivedCancellationReason::TerminationRequest),
        StepReasonV1::CallerOutputFailure => Ok(ArchivedCancellationReason::CallerOutputFailure),
        StepReasonV1::RunnerShutdown => Ok(ArchivedCancellationReason::RunnerShutdown),
        StepReasonV1::FailureStop | StepReasonV1::ExecutionLeaseExpired => Err(()),
    }
}

fn validate_exports(
    result: &WorkflowResultV1,
    workflow: &super::resolution::ResolvedWorkflow,
    steps: &[ArchivedStep],
) -> Result<(), ()> {
    if !result.exports.keys().eq(workflow.definition.exports.keys()) {
        return Err(());
    }

    let mut owner_ordinals = BTreeMap::<(String, String), usize>::new();
    for (index, source) in workflow.definition.exports.values().enumerate() {
        let source_step = steps.iter().find(|step| step.id == source.step).ok_or(())?;
        if source_step.state == ArchivedStepState::Succeeded {
            owner_ordinals
                .entry((source.step.clone(), source.output.clone()))
                .or_insert(index.checked_add(1).ok_or(())?);
        }
    }

    let mut paths_by_source = BTreeMap::<(String, String), String>::new();
    let mut sources_by_path = BTreeMap::<String, (String, String)>::new();
    for ((name, export), (expected_name, source)) in
        result.exports.iter().zip(&workflow.definition.exports)
    {
        if name != expected_name {
            return Err(());
        }
        let source_step = steps.iter().find(|step| step.id == source.step).ok_or(())?;
        match export {
            ExportV1::Available {
                kind,
                media_type,
                path,
                size_bytes: _,
                digest,
            } => {
                let identity = (source.step.clone(), source.output.clone());
                let owner = *owner_ordinals.get(&identity).ok_or(())?;
                if source_step.state != ArchivedStepState::Succeeded
                    || kind != export_kind(source.value_type)
                    || media_type != export_media_type(workflow, source)?
                    || *path != format!("exports/{owner:04}")
                    || !valid_digest(&digest.algorithm, &digest.value)
                    || paths_by_source
                        .insert(identity.clone(), path.clone())
                        .is_some_and(|retained| retained != *path)
                    || sources_by_path
                        .insert(path.clone(), identity.clone())
                        .is_some_and(|retained| retained != identity)
                {
                    return Err(());
                }
            }
            ExportV1::GitBranch { carrier, .. } => {
                let identity = (source.step.clone(), source.output.clone());
                let owner = *owner_ordinals.get(&identity).ok_or(())?;
                if source_step.state != ArchivedStepState::Succeeded
                    || source.value_type != WorkflowValueType::GitBranch
                {
                    return Err(());
                }
                if let Some(carrier) = carrier
                    && (carrier.path != format!("exports/{owner:04}")
                        || !valid_digest(&carrier.digest.algorithm, &carrier.digest.value)
                        || paths_by_source
                            .insert(identity.clone(), carrier.path.clone())
                            .is_some_and(|retained| retained != carrier.path)
                        || sources_by_path
                            .insert(carrier.path.clone(), identity.clone())
                            .is_some_and(|retained| retained != identity))
                {
                    return Err(());
                }
            }
            ExportV1::Unavailable { reason } => {
                let expected = match source_step.state {
                    ArchivedStepState::Failed => ExportUnavailableReasonV1::Failed,
                    ArchivedStepState::Blocked => ExportUnavailableReasonV1::Blocked,
                    ArchivedStepState::NotRun => ExportUnavailableReasonV1::NotRun,
                    ArchivedStepState::Cancelled => ExportUnavailableReasonV1::Cancelled,
                    ArchivedStepState::Succeeded => return Err(()),
                };
                if *reason != expected {
                    return Err(());
                }
            }
        }
    }
    Ok(())
}

fn export_kind(value_type: WorkflowValueType) -> &'static str {
    match value_type {
        WorkflowValueType::File => "file",
        WorkflowValueType::Text => "text",
        WorkflowValueType::Json => "json",
        WorkflowValueType::GitBranch => "git_branch",
        WorkflowValueType::AttachmentCollection => "unsupported",
    }
}

fn export_media_type<'a>(
    workflow: &'a super::resolution::ResolvedWorkflow,
    source: &super::validated::ResolvedOutputSource,
) -> Result<&'a str, ()> {
    let step = workflow.definition.steps.get(&source.step).ok_or(())?;
    let output = match step {
        ValidatedStep::Command(command) => command.common.outputs.get(&source.output),
        ValidatedStep::Agent(agent) => agent.common.outputs.get(&source.output),
    }
    .ok_or(())?;
    Ok(match &output.definition {
        Output::File { media_type, .. } => media_type,
        Output::AgentResponse => "text/plain; charset=utf-8",
        Output::AgentResult { .. } => "application/json",
        Output::GitBranch => "application/vnd.git.bundle",
    })
}

fn step_kind_matches(kind: &str, definition: &ValidatedStep) -> bool {
    matches!(
        (kind, definition),
        ("cmd", ValidatedStep::Command(_)) | ("agent", ValidatedStep::Agent(_))
    )
}

fn step_state_matches(result: WorkflowStepStateV1, durable: AttemptStepStateV1) -> bool {
    matches!(
        (result, durable),
        (
            WorkflowStepStateV1::Succeeded,
            AttemptStepStateV1::Succeeded
        ) | (WorkflowStepStateV1::Failed, AttemptStepStateV1::Failed)
            | (WorkflowStepStateV1::Blocked, AttemptStepStateV1::Blocked)
            | (WorkflowStepStateV1::NotRun, AttemptStepStateV1::NotRun)
            | (
                WorkflowStepStateV1::Cancelled,
                AttemptStepStateV1::Cancelled
            )
    )
}

fn step_failure_policy(step: &ValidatedStep) -> FailurePolicy {
    match step {
        ValidatedStep::Command(command) => command.common.failure_policy,
        ValidatedStep::Agent(agent) => agent.common.failure_policy,
    }
}

fn direct_prerequisites(step: &ValidatedStep) -> &[ResolvedDirectPrerequisite] {
    match step {
        ValidatedStep::Command(command) => &command.common.prerequisites,
        ValidatedStep::Agent(agent) => &agent.common.prerequisites,
    }
}

fn prerequisite_satisfied(
    prerequisite: &ResolvedDirectPrerequisite,
    steps: &[ArchivedStep],
) -> bool {
    let Some(producer) = steps
        .iter()
        .find(|candidate| candidate.id == prerequisite.producer)
    else {
        return false;
    };
    let succeeded = producer.state == ArchivedStepState::Succeeded;
    let control_satisfied = succeeded
        || (producer.failure_policy == FailurePolicy::Advisory
            && matches!(
                producer.state,
                ArchivedStepState::Failed | ArchivedStepState::Blocked
            ));
    (!prerequisite.control || control_satisfied) && (!prerequisite.data || succeeded)
}

fn step_succeeds_workflow(step: &ArchivedStep) -> bool {
    step.state == ArchivedStepState::Succeeded
        || (step.failure_policy == FailurePolicy::Advisory
            && matches!(
                step.state,
                ArchivedStepState::Failed | ArchivedStepState::Blocked
            ))
}

fn valid_digest(algorithm: &str, value: &str) -> bool {
    algorithm == SHA256_ALGORITHM && is_lowercase_hex(value, 64)
}
