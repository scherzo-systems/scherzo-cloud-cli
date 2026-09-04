use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, ready};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use time::OffsetDateTime;
use tokio::sync::watch;

use super::admission::CancellationReason;
use super::observation::{
    CommandOutputSource, ExecutionObservation, ExecutionObserver, ObservedStepTransition,
};
#[cfg(test)]
use super::presentation_feed::MAX_NORMALIZED_CHILD_RECORD_BYTES;
use super::presentation_feed::{
    AcceptedRecordOrder, AgentPresentationObservationKind, DisplayDeadline,
    NormalizedAgentObservation, NormalizedChildOutput, PresentationRecord, PresentationRecordKind,
    PresentationTransition, WorkflowPresentationFeed, WorkflowPresentationStep,
};
use super::publication::{
    LocalPublicationError, LocalPublicationFailureKind, LocalPublicationPhase,
    WorkflowRunFinalization, WorkflowRunResult, WorkflowRunTiming, WorkflowStepTiming,
};
use super::resolution::ResolvedWorkflow;
use super::run_timing::{
    ObservationClock, ObservationTime, RunTimingObservation, RunTimingSnapshot,
};
use super::runtime::{
    RunOutcome, SchedulingGate, StepState, StepStateKind, TransitionEvent, WorkflowState,
};
use super::validated::WorkflowNodeRole;

const MAXIMUM_LOG_RECORDS_PER_STEP: usize = 4_096;
const RUN_LOG_RECORD_BUDGET: usize = 262_144;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StepLogCapacity {
    maximum_records: usize,
    maximum_bytes: usize,
}

impl StepLogCapacity {
    #[cfg(test)]
    pub(crate) fn new(maximum_records: usize, maximum_bytes: usize) -> Option<Self> {
        (maximum_records != 0 && maximum_bytes >= MAX_NORMALIZED_CHILD_RECORD_BYTES).then_some(
            Self {
                maximum_records,
                maximum_bytes,
            },
        )
    }

    fn for_step_count(step_count: usize) -> Self {
        let step_count = step_count.max(1);
        Self {
            maximum_records: MAXIMUM_LOG_RECORDS_PER_STEP.min(RUN_LOG_RECORD_BUDGET / step_count),
            maximum_bytes: usize::try_from(super::maximum_retained_bytes_per_stream(step_count))
                .unwrap_or(usize::MAX),
        }
    }

    pub(crate) const fn maximum_records(self) -> usize {
        self.maximum_records
    }

    pub(crate) const fn maximum_bytes(self) -> usize {
        self.maximum_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunLogSource {
    Command(CommandOutputSource),
    Agent(AgentPresentationObservationKind),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunLogRecord {
    pub(crate) accepted_order: AcceptedRecordOrder,
    pub(crate) observed_at: OffsetDateTime,
    pub(crate) invocation: super::runtime::ActionId,
    pub(crate) source: WorkflowRunLogSource,
    pub(crate) source_sequence: u64,
    pub(crate) payload: Arc<str>,
    pub(crate) continuation: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunStepLog {
    pub(crate) records: Vec<WorkflowRunLogRecord>,
    pub(crate) observed_records: u64,
    pub(crate) retained_records: u64,
    pub(crate) retained_bytes: u64,
    pub(crate) discarded_records: u64,
    pub(crate) discarded_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunOutputUnavailableReason {
    Failed,
    Blocked,
    Skipped,
    NotRun,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunOutputDisposition {
    Pending,
    Committed,
    Unavailable(WorkflowRunOutputUnavailableReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunElapsed {
    pub(crate) started_at: OffsetDateTime,
    pub(crate) duration: Duration,
    pub(crate) frozen: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunStepView {
    pub(crate) id: String,
    pub(crate) role: WorkflowNodeRole,
    pub(crate) definition: WorkflowPresentationStep,
    pub(crate) state: StepStateKind,
    pub(crate) fact: Option<ObservedStepTransition>,
    pub(crate) timing: Option<WorkflowRunElapsed>,
    pub(crate) outputs: BTreeMap<String, WorkflowRunOutputDisposition>,
    pub(crate) log: WorkflowRunStepLog,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunCancellationView {
    pub(crate) reason: CancellationReason,
    pub(crate) force_stop_deadline: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunPublicationFailure {
    pub(crate) phase: LocalPublicationPhase,
    pub(crate) kind: LocalPublicationFailureKind,
    pub(crate) export: Option<String>,
}

impl From<&LocalPublicationError> for WorkflowRunPublicationFailure {
    fn from(error: &LocalPublicationError) -> Self {
        Self {
            phase: error.phase(),
            kind: error.kind(),
            export: error.export().map(str::to_owned),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunPublicationResult {
    Succeeded { result_directory: String },
    Failed(WorkflowRunPublicationFailure),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunPublicationState {
    NotStarted,
    Publishing,
    Completed(WorkflowRunPublicationResult),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunCleanupResult {
    Succeeded,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunCleanupState {
    NotStarted,
    Cleaning,
    Completed(WorkflowRunCleanupResult),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunViewSnapshot {
    pub(crate) generation: u64,
    pub(crate) workflow_path: String,
    pub(crate) maximum_parallel_steps: usize,
    pub(crate) workflow: WorkflowState<OffsetDateTime>,
    pub(crate) timing: WorkflowRunElapsed,
    pub(crate) steps: Vec<WorkflowRunStepView>,
    pub(crate) finalization_start: Option<usize>,
    pub(crate) cancellation: Option<WorkflowRunCancellationView>,
    pub(crate) finalization: Option<WorkflowRunFinalization>,
    pub(crate) authoritative_result: bool,
    pub(crate) quiescent: bool,
    pub(crate) publication: WorkflowRunPublicationState,
    pub(crate) cleanup: WorkflowRunCleanupState,
    pub(crate) quit_eligible: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunViewModelError {
    AlreadyReconciled,
    InvalidTerminalResult,
}

pub(crate) struct WorkflowRunViewModel<Clock> {
    inner: Arc<Mutex<WorkflowRunViewState>>,
    timing: RunTimingObservation,
    clock: Clock,
    changes: watch::Sender<u64>,
}

impl<Clock: Clone> Clone for WorkflowRunViewModel<Clock> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            timing: self.timing.clone(),
            clock: self.clock.clone(),
            changes: self.changes.clone(),
        }
    }
}

impl<Clock> WorkflowRunViewModel<Clock>
where
    Clock: ObservationClock,
{
    pub(crate) fn new(
        workflow: &ResolvedWorkflow,
        maximum_parallel_steps: usize,
        timing: RunTimingObservation,
        clock: Clock,
    ) -> Self {
        let log_capacity = StepLogCapacity::for_step_count(
            workflow.definition.presentation_order.len()
                + workflow.definition.finalizer_presentation_order.len(),
        );
        Self::with_log_capacity(
            workflow,
            maximum_parallel_steps,
            timing,
            clock,
            log_capacity,
        )
    }

    fn with_log_capacity(
        workflow: &ResolvedWorkflow,
        maximum_parallel_steps: usize,
        timing: RunTimingObservation,
        clock: Clock,
        log_capacity: StepLogCapacity,
    ) -> Self {
        let feed = WorkflowPresentationFeed::new(workflow);
        let steps = feed
            .definition()
            .presentation_order
            .iter()
            .filter_map(|id| {
                feed.definition()
                    .steps
                    .get(id)
                    .cloned()
                    .zip(feed.definition().node_roles.get(id).copied())
                    .map(|(definition, role)| {
                        WorkflowRunStepViewState::new(id.clone(), role, definition)
                    })
            })
            .collect::<Vec<_>>();
        let step_indexes = steps
            .iter()
            .enumerate()
            .map(|(index, step)| (step.id.clone(), index))
            .collect();
        let finalization_start = feed.definition().finalization_start;
        let (changes, _) = watch::channel(0);
        Self {
            inner: Arc::new(Mutex::new(WorkflowRunViewState {
                workflow_path: feed.definition().workflow_path.clone(),
                maximum_parallel_steps,
                feed,
                step_indexes,
                steps,
                finalization_start,
                workflow: WorkflowState::Executing {
                    gate: SchedulingGate::Open,
                },
                cancellation: None,
                finalization: None,
                authoritative_result: false,
                terminal_timing: None,
                quiescent: false,
                publication: WorkflowRunPublicationState::NotStarted,
                cleanup: WorkflowRunCleanupState::NotStarted,
                adapter_lifecycle_completed: false,
                log_capacity,
                last_accepted_order: None,
                generation: 0,
            })),
            timing,
            clock,
            changes,
        }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    pub(crate) fn timing_observation(&self) -> RunTimingObservation {
        self.timing.clone()
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> WorkflowRunViewSnapshot {
        self.snapshot_with_logs(None)
    }

    pub(crate) fn snapshot_for_render(&self, selected_step: usize) -> WorkflowRunViewSnapshot {
        self.snapshot_with_logs(Some(selected_step))
    }

    fn snapshot_with_logs(&self, selected_step: Option<usize>) -> WorkflowRunViewSnapshot {
        let now = self.clock.sample();
        let state = lock_state(&self.inner);
        let timing = self.timing.snapshot();
        let workflow_timing = state.terminal_timing.as_ref().map_or_else(
            || observed_run_elapsed(&timing, now),
            |terminal| WorkflowRunElapsed {
                started_at: terminal.started_at,
                duration: terminal.duration,
                frozen: true,
            },
        );
        let steps = state
            .steps
            .iter()
            .enumerate()
            .map(|(index, step)| {
                step.snapshot(
                    &timing,
                    now,
                    state.authoritative_result,
                    selected_step.is_none_or(|selected| selected == index),
                )
            })
            .collect();
        WorkflowRunViewSnapshot {
            generation: state.generation,
            workflow_path: state.workflow_path.clone(),
            maximum_parallel_steps: state.maximum_parallel_steps,
            workflow: state.workflow.clone(),
            timing: workflow_timing,
            steps,
            finalization_start: state.finalization_start,
            cancellation: state.cancellation.clone(),
            finalization: state.finalization.clone(),
            authoritative_result: state.authoritative_result,
            quiescent: state.quiescent,
            publication: state.publication.clone(),
            cleanup: state.cleanup,
            quit_eligible: state.adapter_lifecycle_completed,
        }
    }

    pub(crate) fn reconcile_terminal_result(
        &self,
        run: &WorkflowRunResult,
    ) -> Result<(), WorkflowRunViewModelError> {
        let generation = {
            let mut state = lock_state(&self.inner);
            if state.authoritative_result {
                return Err(WorkflowRunViewModelError::AlreadyReconciled);
            }
            validate_terminal_result(&state, run)?;
            let closing_records = state.feed.finish_child_streams(run.timing.finished_at);
            for record in closing_records {
                state.apply_record(record);
            }

            state.workflow = workflow_state_from_outcome(&run.outcome);
            state.cancellation =
                run.cancellation
                    .as_ref()
                    .map(|cancellation| WorkflowRunCancellationView {
                        reason: cancellation.reason,
                        force_stop_deadline: cancellation.force_stop_deadline,
                    });
            for (view, terminal) in state.steps.iter_mut().zip(run_nodes(run)) {
                view.reconcile(terminal);
            }
            state.finalization = run.finalization.clone();
            state.terminal_timing = Some(run.timing.clone());
            state.authoritative_result = true;
            state.advance_generation()
        };
        self.changes.send_replace(generation);
        Ok(())
    }

    pub(crate) fn mark_quiescent(&self) {
        let generation = {
            let mut state = lock_state(&self.inner);
            if state.quiescent {
                return;
            }
            let observed_at = self.clock.sample();
            self.timing.mark_quiesced(observed_at);
            state.quiescent = true;
            state.advance_generation()
        };
        self.changes.send_replace(generation);
    }

    pub(crate) fn begin_publication(&self) {
        self.update(|state| {
            if state.publication != WorkflowRunPublicationState::NotStarted {
                return false;
            }
            state.publication = WorkflowRunPublicationState::Publishing;
            true
        });
    }

    pub(crate) fn complete_publication(&self, result: WorkflowRunPublicationResult) {
        self.update(|state| {
            if state.publication != WorkflowRunPublicationState::Publishing {
                return false;
            }
            state.publication = WorkflowRunPublicationState::Completed(result);
            true
        });
    }

    pub(crate) fn begin_cleanup(&self) {
        self.update(|state| {
            if state.cleanup != WorkflowRunCleanupState::NotStarted {
                return false;
            }
            state.cleanup = WorkflowRunCleanupState::Cleaning;
            true
        });
    }

    pub(crate) fn complete_cleanup(&self, result: WorkflowRunCleanupResult) {
        self.update(|state| {
            if state.cleanup != WorkflowRunCleanupState::Cleaning {
                return false;
            }
            state.cleanup = WorkflowRunCleanupState::Completed(result);
            true
        });
    }

    pub(crate) fn mark_adapter_lifecycle_completed(&self) {
        self.update(|state| {
            if state.adapter_lifecycle_completed {
                return false;
            }
            state.adapter_lifecycle_completed = true;
            true
        });
    }

    fn update(&self, update: impl FnOnce(&mut WorkflowRunViewState) -> bool) {
        let generation = {
            let mut state = lock_state(&self.inner);
            update(&mut state).then(|| state.advance_generation())
        };
        if let Some(generation) = generation {
            self.changes.send_replace(generation);
        }
    }
}

impl<Clock, Deadline> ExecutionObserver<Deadline> for WorkflowRunViewModel<Clock>
where
    Clock: ObservationClock,
    Deadline: DisplayDeadline,
{
    fn observe(
        &self,
        observation: ExecutionObservation<Deadline>,
    ) -> impl Future<Output = ()> + Send {
        let generation = {
            let mut state = lock_state(&self.inner);
            let observed_at = self.clock.sample();
            self.timing.record(&observation, observed_at);
            let records = state.feed.accept(observed_at.utc, observation);
            if records.is_empty() {
                None
            } else {
                for record in records {
                    state.apply_record(record);
                }
                Some(state.advance_generation())
            }
        };
        if let Some(generation) = generation {
            self.changes.send_replace(generation);
        }
        ready(())
    }
}

struct WorkflowRunViewState {
    workflow_path: String,
    maximum_parallel_steps: usize,
    feed: WorkflowPresentationFeed,
    step_indexes: BTreeMap<String, usize>,
    steps: Vec<WorkflowRunStepViewState>,
    finalization_start: Option<usize>,
    workflow: WorkflowState<OffsetDateTime>,
    cancellation: Option<WorkflowRunCancellationView>,
    finalization: Option<WorkflowRunFinalization>,
    authoritative_result: bool,
    terminal_timing: Option<WorkflowRunTiming>,
    quiescent: bool,
    publication: WorkflowRunPublicationState,
    cleanup: WorkflowRunCleanupState,
    adapter_lifecycle_completed: bool,
    log_capacity: StepLogCapacity,
    last_accepted_order: Option<AcceptedRecordOrder>,
    generation: u64,
}

impl WorkflowRunViewState {
    fn apply_record(&mut self, record: PresentationRecord) {
        self.last_accepted_order = Some(record.accepted_order);
        match record.kind {
            PresentationRecordKind::Transition(transition) => {
                self.apply_transition(*transition);
            }
            PresentationRecordKind::ChildOutput(output) => {
                let Some(index) = self.step_indexes.get(&output.step).copied() else {
                    return;
                };
                self.steps[index].log.push_command(
                    record.accepted_order,
                    record.observed_at,
                    output,
                    self.log_capacity,
                );
            }
            PresentationRecordKind::AgentObservation(observation) => {
                let Some(index) = self.step_indexes.get(&observation.step).copied() else {
                    return;
                };
                self.steps[index].log.push_agent(
                    record.accepted_order,
                    record.observed_at,
                    observation,
                    self.log_capacity,
                );
            }
        }
    }

    fn apply_transition(&mut self, transition: PresentationTransition) {
        match transition.event {
            TransitionEvent::Step { step, to, .. } => {
                let Some(index) = self.step_indexes.get(&step).copied() else {
                    return;
                };
                self.steps[index].apply_transition(to, transition.step);
            }
            TransitionEvent::Workflow { to, .. } => {
                self.workflow = *to;
            }
            TransitionEvent::CancellationAccepted {
                reason, deadline, ..
            } => {
                let prior_issue = match &self.workflow {
                    WorkflowState::Executing {
                        gate: SchedulingGate::FailureStopped { primary_issue },
                    }
                    | WorkflowState::Executing {
                        gate:
                            SchedulingGate::Cancelling {
                                prior_issue: Some(primary_issue),
                                ..
                            },
                    } => Some(primary_issue.clone()),
                    WorkflowState::Executing { .. }
                    | WorkflowState::Finalizing { .. }
                    | WorkflowState::Succeeded
                    | WorkflowState::Failed { .. }
                    | WorkflowState::Cancelled { .. } => None,
                };
                self.workflow = WorkflowState::Executing {
                    gate: SchedulingGate::Cancelling {
                        reason,
                        prior_issue,
                    },
                };
                self.cancellation = Some(WorkflowRunCancellationView {
                    reason,
                    force_stop_deadline: deadline,
                });
            }
            TransitionEvent::FinalizationCancellationAccepted {
                reason, deadline, ..
            } => {
                let WorkflowState::Finalizing {
                    trigger,
                    primary_issue,
                    ..
                } = &self.workflow
                else {
                    return;
                };
                self.workflow = WorkflowState::Finalizing {
                    trigger: *trigger,
                    gate: super::runtime::FinalizationGate::Cancelling {
                        reason,
                        deadline: Some(deadline),
                        force_abort: false,
                    },
                    primary_issue: primary_issue.clone(),
                };
            }
            TransitionEvent::ForceAbortAccepted { reason, .. } => {
                let WorkflowState::Finalizing {
                    trigger,
                    gate,
                    primary_issue,
                } = &self.workflow
                else {
                    return;
                };
                let deadline = match gate {
                    super::runtime::FinalizationGate::Open => None,
                    super::runtime::FinalizationGate::Cancelling { deadline, .. } => *deadline,
                };
                self.workflow = WorkflowState::Finalizing {
                    trigger: *trigger,
                    gate: super::runtime::FinalizationGate::Cancelling {
                        reason,
                        deadline,
                        force_abort: true,
                    },
                    primary_issue: primary_issue.clone(),
                };
            }
        }
    }

    fn advance_generation(&mut self) -> u64 {
        self.generation = self.generation.saturating_add(1);
        self.generation
    }
}

struct WorkflowRunStepViewState {
    id: String,
    role: WorkflowNodeRole,
    definition: WorkflowPresentationStep,
    state: StepStateKind,
    fact: Option<ObservedStepTransition>,
    outputs: BTreeMap<String, WorkflowRunOutputDisposition>,
    log: StepLogRing,
    terminal_timing: Option<WorkflowStepTiming>,
}

impl WorkflowRunStepViewState {
    fn new(id: String, role: WorkflowNodeRole, definition: WorkflowPresentationStep) -> Self {
        let outputs = definition
            .outputs()
            .keys()
            .map(|name| (name.clone(), WorkflowRunOutputDisposition::Pending))
            .collect();
        Self {
            id,
            role,
            definition,
            state: StepStateKind::Pending,
            fact: None,
            outputs,
            log: StepLogRing::default(),
            terminal_timing: None,
        }
    }

    fn apply_transition(&mut self, state: StepStateKind, detail: Option<ObservedStepTransition>) {
        self.state = state;
        self.fact = match &detail {
            Some(ObservedStepTransition::OutputsCommitted { .. }) | None => None,
            Some(_) => detail.clone(),
        };
        match state {
            StepStateKind::Succeeded => {
                if let Some(ObservedStepTransition::OutputsCommitted { outputs }) = detail {
                    for output in outputs {
                        if let Some(disposition) = self.outputs.get_mut(&output) {
                            *disposition = WorkflowRunOutputDisposition::Committed;
                        }
                    }
                }
            }
            StepStateKind::Failed => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Failed);
            }
            StepStateKind::Blocked => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Blocked);
            }
            StepStateKind::Skipped => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Skipped);
            }
            StepStateKind::NotRun => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::NotRun);
            }
            StepStateKind::Cancelled => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Cancelled);
            }
            StepStateKind::Pending
            | StepStateKind::Starting
            | StepStateKind::Running
            | StepStateKind::CapturingOutputs
            | StepStateKind::Recovering
            | StepStateKind::Cancelling => {}
        }
    }

    fn reconcile(&mut self, terminal: &super::publication::WorkflowRunStep) {
        self.state = terminal_state_kind(&terminal.state);
        self.fact = terminal_step_fact(&terminal.state);
        match &terminal.state {
            StepState::Succeeded { outputs } => {
                for (name, disposition) in &mut self.outputs {
                    *disposition = if outputs.contains_key(name) {
                        WorkflowRunOutputDisposition::Committed
                    } else {
                        WorkflowRunOutputDisposition::Pending
                    };
                }
            }
            StepState::Failed { .. } => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Failed);
            }
            StepState::Blocked { .. } => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Blocked);
            }
            StepState::Skipped { .. } => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Skipped);
            }
            StepState::NotRun { .. } => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::NotRun);
            }
            StepState::Cancelled { .. } => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Cancelled);
            }
            StepState::Pending
            | StepState::Starting
            | StepState::Running
            | StepState::CapturingOutputs
            | StepState::Recovering { .. }
            | StepState::Cancelling { .. } => {}
        }
        self.terminal_timing = terminal.timing.clone();
    }

    fn make_outputs_unavailable(&mut self, reason: WorkflowRunOutputUnavailableReason) {
        for disposition in self.outputs.values_mut() {
            *disposition = WorkflowRunOutputDisposition::Unavailable(reason);
        }
    }

    fn snapshot(
        &self,
        timing: &RunTimingSnapshot,
        now: ObservationTime,
        authoritative_result: bool,
        include_log_records: bool,
    ) -> WorkflowRunStepView {
        let step_timing = if authoritative_result {
            self.terminal_timing
                .as_ref()
                .map(|terminal| WorkflowRunElapsed {
                    started_at: terminal.started_at,
                    duration: terminal.duration,
                    frozen: true,
                })
        } else {
            observed_step_elapsed(timing, &self.id, now)
        };
        WorkflowRunStepView {
            id: self.id.clone(),
            role: self.role,
            definition: self.definition.clone(),
            state: self.state,
            fact: self.fact.clone(),
            timing: step_timing,
            outputs: self.outputs.clone(),
            log: self.log.snapshot(include_log_records),
        }
    }
}

#[derive(Default)]
struct StepLogRing {
    records: VecDeque<WorkflowRunLogRecord>,
    retained_bytes: u64,
    observed_records: u64,
    discarded_records: u64,
    discarded_bytes: u64,
}

impl StepLogRing {
    fn push_command(
        &mut self,
        accepted_order: AcceptedRecordOrder,
        observed_at: OffsetDateTime,
        output: NormalizedChildOutput,
        capacity: StepLogCapacity,
    ) {
        self.push(
            accepted_order,
            observed_at,
            output.invocation,
            WorkflowRunLogSource::Command(output.source),
            output.source_sequence.get(),
            output.payload,
            output.continuation,
            capacity,
        );
    }

    fn push_agent(
        &mut self,
        accepted_order: AcceptedRecordOrder,
        observed_at: OffsetDateTime,
        observation: NormalizedAgentObservation,
        capacity: StepLogCapacity,
    ) {
        self.push(
            accepted_order,
            observed_at,
            observation.invocation,
            WorkflowRunLogSource::Agent(observation.kind),
            observation.observation_sequence,
            observation.payload,
            observation.continuation,
            capacity,
        );
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the bounded log record keeps normalized source identity explicit"
    )]
    fn push(
        &mut self,
        accepted_order: AcceptedRecordOrder,
        observed_at: OffsetDateTime,
        invocation: super::runtime::ActionId,
        source: WorkflowRunLogSource,
        source_sequence: u64,
        payload: String,
        continuation: bool,
        capacity: StepLogCapacity,
    ) {
        let payload_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        let maximum_bytes = u64::try_from(capacity.maximum_bytes()).unwrap_or(u64::MAX);
        while !self.records.is_empty()
            && (self.records.len() >= capacity.maximum_records()
                || self.retained_bytes.saturating_add(payload_bytes) > maximum_bytes)
        {
            self.discard_oldest();
        }

        self.observed_records = self.observed_records.saturating_add(1);
        self.retained_bytes = self.retained_bytes.saturating_add(payload_bytes);
        self.records.push_back(WorkflowRunLogRecord {
            accepted_order,
            observed_at,
            invocation,
            source,
            source_sequence,
            payload: Arc::from(payload),
            continuation,
        });
    }

    fn discard_oldest(&mut self) {
        let Some(discarded) = self.records.pop_front() else {
            return;
        };
        let discarded_bytes = u64::try_from(discarded.payload.len()).unwrap_or(u64::MAX);
        self.retained_bytes = self.retained_bytes.saturating_sub(discarded_bytes);
        self.discarded_records = self.discarded_records.saturating_add(1);
        self.discarded_bytes = self.discarded_bytes.saturating_add(discarded_bytes);
    }

    fn snapshot(&self, include_records: bool) -> WorkflowRunStepLog {
        WorkflowRunStepLog {
            records: if include_records {
                self.records.iter().cloned().collect()
            } else {
                Vec::new()
            },
            observed_records: self.observed_records,
            retained_records: u64::try_from(self.records.len()).unwrap_or(u64::MAX),
            retained_bytes: self.retained_bytes,
            discarded_records: self.discarded_records,
            discarded_bytes: self.discarded_bytes,
        }
    }
}

fn validate_terminal_result(
    state: &WorkflowRunViewState,
    run: &WorkflowRunResult,
) -> Result<(), WorkflowRunViewModelError> {
    let nodes = run_nodes(run).collect::<Vec<_>>();
    if run.workflow_path != state.workflow_path
        || nodes.len() != state.steps.len()
        || run.finalization.is_some() != state.finalization_start.is_some()
    {
        return Err(WorkflowRunViewModelError::InvalidTerminalResult);
    }
    for (view, terminal) in state.steps.iter().zip(nodes) {
        if view.id != terminal.id
            || view.role != terminal.role
            || terminal.failure_policy != view.definition.failure_policy()
            || !terminal_step_is_valid(view, terminal)
        {
            return Err(WorkflowRunViewModelError::InvalidTerminalResult);
        }
    }
    Ok(())
}

fn run_nodes(
    run: &WorkflowRunResult,
) -> impl Iterator<Item = &super::publication::WorkflowRunStep> {
    run.steps.iter().chain(
        run.finalization
            .iter()
            .flat_map(|finalization| &finalization.finalizers),
    )
}

fn terminal_step_is_valid(
    view: &WorkflowRunStepViewState,
    terminal: &super::publication::WorkflowRunStep,
) -> bool {
    match &terminal.state {
        StepState::Succeeded { outputs } => outputs.keys().eq(view.outputs.keys()),
        StepState::Failed { .. }
        | StepState::Blocked { .. }
        | StepState::Skipped { .. }
        | StepState::NotRun { .. }
        | StepState::Cancelled { .. } => true,
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Cancelling { .. } => false,
    }
}

fn terminal_state_kind(state: &StepState<super::value::CapturedValue>) -> StepStateKind {
    match state {
        StepState::Pending => StepStateKind::Pending,
        StepState::Starting => StepStateKind::Starting,
        StepState::Running => StepStateKind::Running,
        StepState::CapturingOutputs => StepStateKind::CapturingOutputs,
        StepState::Recovering { .. } => StepStateKind::Recovering,
        StepState::Cancelling { .. } => StepStateKind::Cancelling,
        StepState::Succeeded { .. } => StepStateKind::Succeeded,
        StepState::Failed { .. } => StepStateKind::Failed,
        StepState::Blocked { .. } => StepStateKind::Blocked,
        StepState::Skipped { .. } => StepStateKind::Skipped,
        StepState::NotRun { .. } => StepStateKind::NotRun,
        StepState::Cancelled { .. } => StepStateKind::Cancelled,
    }
}

fn terminal_step_fact(
    state: &StepState<super::value::CapturedValue>,
) -> Option<ObservedStepTransition> {
    match state {
        StepState::Failed { detail } => Some(ObservedStepTransition::Failed {
            detail: detail.clone(),
        }),
        StepState::Blocked { detail } => Some(ObservedStepTransition::Blocked {
            detail: detail.clone(),
        }),
        StepState::Skipped { detail } => Some(ObservedStepTransition::Skipped {
            detail: detail.clone(),
        }),
        StepState::NotRun { detail } => Some(ObservedStepTransition::NotRun { detail: *detail }),
        StepState::Cancelling { detail } => {
            Some(ObservedStepTransition::Cancelling { detail: *detail })
        }
        StepState::Cancelled { detail } => {
            Some(ObservedStepTransition::Cancelled { detail: *detail })
        }
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Succeeded { .. } => None,
    }
}

fn workflow_state_from_outcome(outcome: &RunOutcome) -> WorkflowState<OffsetDateTime> {
    match outcome {
        RunOutcome::Succeeded => WorkflowState::Succeeded,
        RunOutcome::Failed {
            primary_issue,
            later_cancellation,
        } => WorkflowState::Failed {
            primary_issue: primary_issue.clone(),
            later_cancellation: *later_cancellation,
        },
        RunOutcome::Cancelled { reason } => WorkflowState::Cancelled { reason: *reason },
    }
}

fn observed_run_elapsed(timing: &RunTimingSnapshot, now: ObservationTime) -> WorkflowRunElapsed {
    let Some(started) = timing.execution_started else {
        return WorkflowRunElapsed {
            started_at: timing.presentation_opened.utc,
            duration: Duration::ZERO,
            frozen: true,
        };
    };
    let finished = timing.terminal.or(timing.quiesced);
    let finished_at = finished.map_or(now.monotonic, |point| point.monotonic);
    WorkflowRunElapsed {
        started_at: started.utc,
        duration: finished_at.saturating_duration_since(started.monotonic),
        frozen: finished.is_some(),
    }
}

fn observed_step_elapsed(
    timing: &RunTimingSnapshot,
    step: &str,
    now: ObservationTime,
) -> Option<WorkflowRunElapsed> {
    let observed = timing.steps.get(step)?;
    let finished = observed
        .finished
        .or_else(|| timing.quiesced.map(|point| point.monotonic));
    Some(WorkflowRunElapsed {
        started_at: observed.started.utc,
        duration: finished
            .unwrap_or(now.monotonic)
            .saturating_duration_since(observed.started.monotonic),
        frozen: finished.is_some(),
    })
}

fn lock_state(state: &Mutex<WorkflowRunViewState>) -> MutexGuard<'_, WorkflowRunViewState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests;
