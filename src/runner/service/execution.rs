use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Add;
use std::os::fd::OwnedFd;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::Sleeper;
use super::assignment::{
    AcceptedAssignment, AssignmentObservation, ExecutionReport, LeaseAuthority, ManagerEvent,
    ObservationOutbox,
};
use crate::execution::workflow::admission::CancellationReason;
use crate::execution::workflow::agent::dispatch::production_agent_dispatcher;
use crate::execution::workflow::agent::{
    AgentFailureCause, AgentHarnessFailureDetail, AgentInputKind, WorkflowRunId,
};
use crate::execution::workflow::agent_diagnostics::AgentDiagnosticSessionStore;
use crate::execution::workflow::agent_input::{AgentInputStaging, AgentInputStartFailure};
use crate::execution::workflow::artifact::{ArtifactStaging, CaptureFailureKind};
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::execution::{NoopCommitPort, execute_workflow};
use crate::execution::workflow::input::{InputPreparationFailureKind, InputStaging};
use crate::execution::workflow::observation::{
    ExecutionObservation, ExecutionObserver, ObservedStepTransition, TransitionObservation,
};
use crate::execution::workflow::process_group::{
    AuthenticatedProcessGroup, DurableProcessGuardStore, ProcessGuardRegistry,
    ProcessIdentityInspector, ProcessIdentityObservation, SystemProcessIdentityInspector,
    terminate_authenticated_process_group,
};
use crate::execution::workflow::runtime::{
    FailurePhase, NotRunReason, RunOutcome, SchedulingGate, StepFailure, StepStateKind,
    TransitionEvent, WorkflowState,
};
use crate::execution::workflow::step_runtime::{
    AgentExecution, CommandExecutionFailure, CommandLaunchFailure, CommandPreparationFailure,
    OutputCaptureFailure, StepExecutionFailure, StepFailureCause, StepStartFailure,
    WorkingDirectoryFailure,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GuardLifecycle {
    Prepared,
    Released,
    Quiesced,
}

struct GuardRecord {
    identity: AuthenticatedProcessGroup,
    lifecycle: GuardLifecycle,
}

struct ProcessGuardState {
    next_id: u64,
    records: BTreeMap<String, GuardRecord>,
    forced_containment_started: bool,
}

#[derive(Clone)]
pub(super) struct AssignmentProcessGuards {
    state: Arc<Mutex<ProcessGuardState>>,
}

impl AssignmentProcessGuards {
    pub(super) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ProcessGuardState {
                next_id: 1,
                records: BTreeMap::new(),
                forced_containment_started: false,
            })),
        }
    }

    pub(super) fn registry(&self, guarded: bool) -> ProcessGuardRegistry {
        if guarded {
            let store: Arc<dyn DurableProcessGuardStore> = Arc::new(self.clone());
            ProcessGuardRegistry::durable(store)
        } else {
            ProcessGuardRegistry::default()
        }
    }

    fn begin_forced_containment(&self) {
        let identities = {
            let mut state = self.lock();
            state.forced_containment_started = true;
            state
                .records
                .values()
                .filter(|record| record.lifecycle != GuardLifecycle::Quiesced)
                .map(|record| record.identity.clone())
                .collect::<Vec<_>>()
        };
        for identity in identities {
            let _ = terminate_authenticated_process_group(&identity);
        }
    }

    fn is_quiescent(&self) -> bool {
        let inspector = SystemProcessIdentityInspector;
        self.lock().records.values().all(|record| {
            record.lifecycle == GuardLifecycle::Quiesced
                || matches!(
                    inspector.observe(&record.identity),
                    ProcessIdentityObservation::Absent
                )
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ProcessGuardState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    fn forced_containment_started(&self) -> bool {
        self.lock().forced_containment_started
    }
}

impl DurableProcessGuardStore for AssignmentProcessGuards {
    fn register(
        &self,
        step: &str,
        action_id: u64,
        identity: &AuthenticatedProcessGroup,
    ) -> Result<String, ()> {
        let mut state = self.lock();
        if state.forced_containment_started
            || state.records.values().any(|record| {
                record.lifecycle != GuardLifecycle::Quiesced && record.identity == *identity
            })
        {
            return Err(());
        }
        let id = format!("{step}:{action_id}:{}", state.next_id);
        state.next_id = state.next_id.checked_add(1).ok_or(())?;
        state.records.insert(
            id.clone(),
            GuardRecord {
                identity: identity.clone(),
                lifecycle: GuardLifecycle::Prepared,
            },
        );
        Ok(id)
    }

    fn mark_released(&self, guard_id: &str) -> Result<(), ()> {
        let mut state = self.lock();
        if state.forced_containment_started {
            return Err(());
        }
        let record = state.records.get_mut(guard_id).ok_or(())?;
        match record.lifecycle {
            GuardLifecycle::Prepared => record.lifecycle = GuardLifecycle::Released,
            GuardLifecycle::Released => {}
            GuardLifecycle::Quiesced => return Err(()),
        }
        Ok(())
    }

    fn mark_quiesced(&self, guard_id: &str) -> Result<(), ()> {
        let mut state = self.lock();
        let record = state.records.get_mut(guard_id).ok_or(())?;
        record.lifecycle = GuardLifecycle::Quiesced;
        Ok(())
    }
}

#[derive(Clone)]
struct PostStopFence {
    fenced: Arc<Mutex<bool>>,
}

impl PostStopFence {
    fn new() -> Self {
        Self {
            fenced: Arc::new(Mutex::new(false)),
        }
    }

    fn fence(&self) {
        *self.lock() = true;
    }

    fn is_fenced(&self) -> bool {
        *self.lock()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, bool> {
        self.fenced
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct ExecutionCompletion {
    final_observation_id: Option<u64>,
    final_delivery_deadline: Option<Instant>,
}

impl ExecutionCompletion {
    fn selected(final_observation_id: Option<u64>) -> Self {
        Self {
            final_observation_id,
            final_delivery_deadline: None,
        }
    }

    fn ordinary(final_observation_id: Option<u64>) -> Self {
        Self::selected(final_observation_id)
    }

    fn fenced(final_observation_id: Option<u64>, _delivery_budget: Option<Duration>) -> Self {
        Self::selected(final_observation_id)
    }

    fn with_budget(final_observation_id: Option<u64>, _delivery_budget: Option<Duration>) -> Self {
        Self::selected(final_observation_id)
    }

    fn without_report() -> Self {
        Self::selected(None)
    }
}

pub(super) struct ExecutionJob {
    accepted: AcceptedAssignment,
    outbox: ObservationOutbox,
    manager_events: tokio::sync::mpsc::UnboundedSender<ManagerEvent>,
    sleeper: Arc<dyn Sleeper>,
    pub(super) authority_updates: tokio::sync::watch::Receiver<LeaseAuthority>,
}

impl ExecutionJob {
    pub(super) fn new(
        accepted: AcceptedAssignment,
        outbox: ObservationOutbox,
        manager_events: tokio::sync::mpsc::UnboundedSender<ManagerEvent>,
        sleeper: Arc<dyn Sleeper>,
        authority_updates: tokio::sync::watch::Receiver<LeaseAuthority>,
    ) -> Self {
        Self {
            accepted,
            outbox,
            manager_events,
            sleeper,
            authority_updates,
        }
    }

    pub(super) fn spawn(self) {
        tokio::spawn(self.run());
    }

    async fn run(self) {
        let assignment_id = self.accepted.assignment_id().to_owned();
        let attempt_id = self.accepted.attempt_id().to_owned();
        let run_id = self.accepted.run_id().to_owned();
        let mut completion = self
            .run_workflow(&assignment_id, &attempt_id, &run_id)
            .await;
        if completion.final_observation_id.is_some() {
            completion.final_delivery_deadline = Some(self.terminal_report_deadline());
        }
        drop(self.accepted);
        let _ = self.manager_events.send(ManagerEvent::Finished {
            assignment_id,
            final_observation_id: completion.final_observation_id,
            final_delivery_deadline: completion.final_delivery_deadline,
        });
        self.outbox.wake();
    }

    async fn run_workflow(
        &self,
        assignment_id: &str,
        attempt_id: &str,
        run_id: &str,
    ) -> ExecutionCompletion {
        if !self.has_execution_authority() {
            return ExecutionCompletion::without_report();
        }
        if self
            .enqueue(assignment_id, attempt_id, ExecutionReport::Started)
            .is_none()
        {
            return ExecutionCompletion::ordinary(self.abort(
                assignment_id,
                attempt_id,
                0,
                "runner_internal_failure",
            ));
        }

        let artifacts = match ArtifactStaging::create(
            self.accepted.admitted.execution(),
            &self.accepted.root.private,
        ) {
            Ok(staging) => staging,
            Err(_) => {
                return ExecutionCompletion::ordinary(self.abort(
                    assignment_id,
                    attempt_id,
                    0,
                    "execution_environment_lost",
                ));
            }
        };
        let inputs = match InputStaging::create(
            self.accepted.admitted.execution(),
            &self.accepted.root.private,
        ) {
            Ok(staging) => staging,
            Err(_) => {
                let _ = artifacts.release();
                return ExecutionCompletion::ordinary(self.abort(
                    assignment_id,
                    attempt_id,
                    0,
                    "execution_environment_lost",
                ));
            }
        };
        let agent_staging = if self.accepted.admitted.agent_steps().is_empty() {
            None
        } else {
            match AgentInputStaging::create(
                self.accepted.admitted.execution(),
                &self.accepted.root.private,
            ) {
                Ok(staging) => Some(staging),
                Err(_) => {
                    let _ = inputs.release();
                    let _ = artifacts.release();
                    return ExecutionCompletion::ordinary(self.abort(
                        assignment_id,
                        attempt_id,
                        0,
                        "execution_environment_lost",
                    ));
                }
            }
        };

        let agent_diagnostic_sessions = if agent_staging.is_some() {
            let attempt_handle = std::fs::File::open(&self.accepted.root.private)
                .map(OwnedFd::from)
                .ok();
            match attempt_handle.and_then(|attempt_handle| {
                AgentDiagnosticSessionStore::create_transient(
                    &attempt_handle,
                    &self.accepted.root.private,
                )
                .ok()
            }) {
                Some(sessions) => Some(sessions),
                None => {
                    let _ = release_staging(&inputs, agent_staging.as_ref(), &artifacts);
                    return ExecutionCompletion::ordinary(self.abort(
                        assignment_id,
                        attempt_id,
                        0,
                        "execution_environment_lost",
                    ));
                }
            }
        } else {
            None
        };

        if !self.has_execution_authority() {
            let _ = release_staging(&inputs, agent_staging.as_ref(), &artifacts);
            return ExecutionCompletion::without_report();
        }

        let post_stop_fence = PostStopFence::new();
        let observer = RunnerExecutionObserver::new(
            assignment_id.to_owned(),
            attempt_id.to_owned(),
            self.accepted.transition_budget,
            self.outbox.clone(),
            post_stop_fence.clone(),
        );
        let diagnostics = StepDiagnosticLog::default();
        let cancellation = self
            .accepted
            .admitted
            .execution()
            .cancellation()
            .source()
            .clone();
        let process_guard_registry = self
            .accepted
            .process_guards
            .registry(self.accepted.guard_processes);
        let execution = if let (Some(agent_staging), Some(diagnostic_sessions)) =
            (&agent_staging, agent_diagnostic_sessions)
        {
            let maximum_log_bytes = self
                .accepted
                .admitted
                .execution()
                .limits()
                .maximum_step_log_bytes();
            let Ok(dispatcher) = production_agent_dispatcher(
                diagnostics.clone(),
                maximum_log_bytes,
                RunnerExecutionClock,
                observer.clone(),
            ) else {
                let _ = release_staging(&inputs, Some(agent_staging), &artifacts);
                return ExecutionCompletion::ordinary(self.abort(
                    assignment_id,
                    attempt_id,
                    observer.last_sequence(),
                    "runner_internal_failure",
                ));
            };
            let agents = AgentExecution::enabled(
                WorkflowRunId::from(Arc::from(run_id)),
                agent_staging.clone(),
                diagnostic_sessions,
                dispatcher,
            );
            // Enabled and disabled execution carry distinct static dispatcher types;
            // keeping each engine call explicit avoids a dynamic adapter boundary.
            // jscpd:ignore-start
            let result = run_under_lease(
                execute_workflow(
                    self.accepted.admitted.clone(),
                    &artifacts,
                    &inputs,
                    &diagnostics,
                    agents,
                    RunnerExecutionClock,
                    NoopCommitPort,
                    observer.clone(),
                    process_guard_registry,
                ),
                &cancellation,
                self.sleeper.as_ref(),
                self.authority_updates.clone(),
                &self.outbox,
                assignment_id,
                attempt_id,
                &post_stop_fence,
                &self.accepted.process_guards,
            )
            .await;
            // jscpd:ignore-end
            result
        } else {
            // See the enabled branch: the no-agent dispatcher is intentionally a different type.
            // jscpd:ignore-start
            let result = run_under_lease(
                execute_workflow(
                    self.accepted.admitted.clone(),
                    &artifacts,
                    &inputs,
                    &diagnostics,
                    AgentExecution::disabled(),
                    RunnerExecutionClock,
                    NoopCommitPort,
                    observer.clone(),
                    process_guard_registry,
                ),
                &cancellation,
                self.sleeper.as_ref(),
                self.authority_updates.clone(),
                &self.outbox,
                assignment_id,
                attempt_id,
                &post_stop_fence,
                &self.accepted.process_guards,
            )
            .await;
            // jscpd:ignore-end
            result
        };
        let cleanup_failed = release_staging(&inputs, agent_staging.as_ref(), &artifacts);

        let (result, final_delivery_budget) = match execution {
            LeaseExecution::Completed {
                output: Ok(result),
                final_delivery_budget,
            } => (result, final_delivery_budget),
            LeaseExecution::Completed {
                output: Err(_),
                final_delivery_budget,
            } => {
                return self.abort_unless_fenced(
                    &post_stop_fence,
                    assignment_id,
                    attempt_id,
                    observer.last_sequence(),
                    "runner_internal_failure",
                    final_delivery_budget,
                );
            }
            LeaseExecution::ContainmentDeadline => {
                return ExecutionCompletion::fenced(None, None);
            }
        };
        if cleanup_failed {
            return self.abort_unless_fenced(
                &post_stop_fence,
                assignment_id,
                attempt_id,
                observer.last_sequence(),
                "execution_environment_lost",
                final_delivery_budget,
            );
        }
        if observer.faulted() {
            return self.abort_unless_fenced(
                &post_stop_fence,
                assignment_id,
                attempt_id,
                observer.last_sequence(),
                "runner_internal_failure",
                final_delivery_budget,
            );
        }
        let last_sequence = observer.last_sequence();
        if last_sequence == 0
            || observer.terminal_sequence() != Some(last_sequence)
            || !terminal_result_agrees(observer.terminal_state().as_ref(), &result.outcome)
            || !result.exports.is_empty()
        {
            return self.abort_unless_fenced(
                &post_stop_fence,
                assignment_id,
                attempt_id,
                last_sequence,
                "engine_result_inconsistent",
                final_delivery_budget,
            );
        }

        let report = match result.outcome {
            RunOutcome::Succeeded => ExecutionReport::Finished {
                final_execution_event_sequence: last_sequence,
                outcome: json!({ "outcome": "succeeded" }),
            },
            RunOutcome::Failed {
                primary_failure, ..
            } => ExecutionReport::Finished {
                final_execution_event_sequence: last_sequence,
                outcome: json!({
                    "outcome": "failed",
                    "failure": workflow_failure(&primary_failure),
                }),
            },
            RunOutcome::Cancelled {
                reason: CancellationReason::ExecutionLeaseExpired,
            } => ExecutionReport::Interrupted {
                final_execution_event_sequence: last_sequence,
                reason: "execution_lease_expired".to_owned(),
                terminal_outcome: json!({
                    "outcome": "cancelled",
                    "reason": "execution_lease_expired",
                }),
            },
            RunOutcome::Cancelled {
                reason: CancellationReason::RunnerShutdown,
            } => ExecutionReport::Interrupted {
                final_execution_event_sequence: last_sequence,
                reason: "graceful_shutdown".to_owned(),
                terminal_outcome: json!({
                    "outcome": "cancelled",
                    "reason": "runner_shutdown",
                }),
            },
            RunOutcome::Cancelled { .. } => {
                return self.abort_unless_fenced(
                    &post_stop_fence,
                    assignment_id,
                    attempt_id,
                    last_sequence,
                    "runner_internal_failure",
                    final_delivery_budget,
                );
            }
        };
        ExecutionCompletion::with_budget(
            self.enqueue(assignment_id, attempt_id, report),
            final_delivery_budget,
        )
    }

    fn has_execution_authority(&self) -> bool {
        let authority = self.authority_updates.borrow();
        !authority.revoked && self.sleeper.now() < authority.cancellation_deadline
    }

    fn terminal_report_deadline(&self) -> Instant {
        let selected_at = self.sleeper.now();
        let authority = self.authority_updates.borrow();
        selected_at
            .checked_add(authority.terminal_report_delivery_budget)
            .unwrap_or(authority.expires_deadline)
            .min(authority.expires_deadline)
    }

    fn abort_unless_fenced(
        &self,
        post_stop_fence: &PostStopFence,
        assignment_id: &str,
        attempt_id: &str,
        last_execution_event_sequence: u64,
        reason: &str,
        final_delivery_budget: Option<Duration>,
    ) -> ExecutionCompletion {
        if post_stop_fence.is_fenced() {
            ExecutionCompletion::fenced(None, final_delivery_budget)
        } else {
            ExecutionCompletion::with_budget(
                self.abort(
                    assignment_id,
                    attempt_id,
                    last_execution_event_sequence,
                    reason,
                ),
                final_delivery_budget,
            )
        }
    }

    fn enqueue(
        &self,
        assignment_id: &str,
        attempt_id: &str,
        report: ExecutionReport,
    ) -> Option<u64> {
        self.outbox
            .enqueue(AssignmentObservation::Execution {
                assignment_id: assignment_id.to_owned(),
                attempt_id: attempt_id.to_owned(),
                report,
            })
            .ok()
    }

    fn abort(
        &self,
        assignment_id: &str,
        attempt_id: &str,
        last_execution_event_sequence: u64,
        reason: &str,
    ) -> Option<u64> {
        self.enqueue(
            assignment_id,
            attempt_id,
            ExecutionReport::Aborted {
                last_execution_event_sequence,
                reason: reason.to_owned(),
            },
        )
    }
}

enum LeaseExecution<Output> {
    Completed {
        output: Output,
        final_delivery_budget: Option<Duration>,
    },
    ContainmentDeadline,
}

#[expect(
    clippy::too_many_arguments,
    reason = "lease supervision receives every authority and containment boundary explicitly"
)]
async fn run_under_lease<F, Output>(
    execution: F,
    cancellation: &crate::execution::workflow::admission::CancellationSource,
    sleeper: &dyn Sleeper,
    mut authority_updates: tokio::sync::watch::Receiver<LeaseAuthority>,
    outbox: &ObservationOutbox,
    assignment_id: &str,
    attempt_id: &str,
    post_stop_fence: &PostStopFence,
    process_guards: &AssignmentProcessGuards,
) -> LeaseExecution<Output>
where
    F: Future<Output = Output>,
{
    tokio::pin!(execution);
    loop {
        let authority = authority_updates.borrow_and_update().clone();
        if authority.revoked || sleeper.now() >= authority.cancellation_deadline {
            return finish_after_lease_loss(
                &mut execution,
                cancellation,
                sleeper,
                &authority,
                post_stop_fence,
                process_guards,
            )
            .await;
        }
        tokio::select! {
            biased;
            () = sleeper.sleep(duration_until(sleeper, authority.renewal_deadline)) => {
                if outbox
                    .enqueue(AssignmentObservation::LeaseRenewalRequested {
                        assignment_id: assignment_id.to_owned(),
                        attempt_id: attempt_id.to_owned(),
                        current_lease_sequence: authority.sequence,
                    })
                    .is_err()
                {
                    cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
                    return LeaseExecution::Completed {
                        output: execution.await,
                        final_delivery_budget: None,
                    };
                }
                tokio::select! {
                    biased;
                    () = sleeper.sleep(duration_until(sleeper, authority.cancellation_deadline)) => {
                        return finish_after_lease_loss(
                            &mut execution,
                            cancellation,
                            sleeper,
                            &authority,
                            post_stop_fence,
                            process_guards,
                        ).await;
                    }
                    changed = authority_updates.changed() => {
                        if changed.is_err() {
                            return finish_after_lease_loss(
                                &mut execution,
                                cancellation,
                                sleeper,
                                &authority,
                                post_stop_fence,
                                process_guards,
                            ).await;
                        }
                    }
                    result = &mut execution => {
                        return LeaseExecution::Completed {
                            output: result,
                            final_delivery_budget: None,
                        };
                    }
                }
            }
            changed = authority_updates.changed() => {
                if changed.is_err() {
                    return finish_after_lease_loss(
                        &mut execution,
                        cancellation,
                        sleeper,
                        &authority,
                        post_stop_fence,
                        process_guards,
                    ).await;
                }
            }
            result = &mut execution => {
                return LeaseExecution::Completed {
                    output: result,
                    final_delivery_budget: None,
                };
            }
        }
    }
}

async fn finish_after_lease_loss<F, Output>(
    execution: &mut std::pin::Pin<&mut F>,
    cancellation: &crate::execution::workflow::admission::CancellationSource,
    sleeper: &dyn Sleeper,
    authority: &LeaseAuthority,
    post_stop_fence: &PostStopFence,
    process_guards: &AssignmentProcessGuards,
) -> LeaseExecution<Output>
where
    F: Future<Output = Output>,
{
    cancellation.request_cancellation(CancellationReason::ExecutionLeaseExpired);
    if sleeper.now() < authority.stop_deadline {
        tokio::select! {
            biased;
            () = sleeper.sleep(duration_until(sleeper, authority.stop_deadline)) => {}
            output = execution.as_mut() => {
                return LeaseExecution::Completed {
                    output,
                    final_delivery_budget: Some(authority.terminal_report_delivery_budget),
                };
            }
        }
    }

    post_stop_fence.fence();
    process_guards.begin_forced_containment();
    if sleeper.now() > authority.force_stop_deadline {
        return LeaseExecution::ContainmentDeadline;
    }
    tokio::select! {
        biased;
        output = execution.as_mut() => {
            if process_guards.is_quiescent() {
                LeaseExecution::Completed {
                    output,
                    final_delivery_budget: Some(authority.terminal_report_delivery_budget),
                }
            } else {
                LeaseExecution::ContainmentDeadline
            }
        }
        () = sleeper.sleep(duration_until(sleeper, authority.force_stop_deadline)) => {
            LeaseExecution::ContainmentDeadline
        }
    }
}

fn duration_until(sleeper: &dyn Sleeper, deadline: Instant) -> Duration {
    deadline.saturating_duration_since(sleeper.now())
}

fn release_staging(
    inputs: &InputStaging,
    agent: Option<&AgentInputStaging>,
    artifacts: &ArtifactStaging,
) -> bool {
    agent.is_some_and(|staging| staging.release().is_err())
        | inputs.release().is_err()
        | artifacts.release().is_err()
}

#[derive(Clone)]
struct RunnerExecutionObserver {
    assignment_id: String,
    attempt_id: String,
    transition_budget: usize,
    outbox: ObservationOutbox,
    post_stop_fence: PostStopFence,
    state: Arc<Mutex<ObserverState>>,
}

struct ObserverState {
    transition_count: usize,
    last_sequence: u64,
    terminal_sequence: Option<u64>,
    terminal_state: Option<WorkflowState<StepFailureCause>>,
    faulted: bool,
}

impl RunnerExecutionObserver {
    fn new(
        assignment_id: String,
        attempt_id: String,
        transition_budget: usize,
        outbox: ObservationOutbox,
        post_stop_fence: PostStopFence,
    ) -> Self {
        Self {
            assignment_id,
            attempt_id,
            transition_budget,
            outbox,
            post_stop_fence,
            state: Arc::new(Mutex::new(ObserverState {
                transition_count: 0,
                last_sequence: 0,
                terminal_sequence: None,
                terminal_state: None,
                faulted: false,
            })),
        }
    }

    fn last_sequence(&self) -> u64 {
        self.lock().last_sequence
    }

    fn terminal_sequence(&self) -> Option<u64> {
        self.lock().terminal_sequence
    }

    fn terminal_state(&self) -> Option<WorkflowState<StepFailureCause>> {
        self.lock().terminal_state.clone()
    }

    fn faulted(&self) -> bool {
        self.lock().faulted
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ObserverState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl ExecutionObserver<RunnerExecutionInstant> for RunnerExecutionObserver {
    fn observe(
        &self,
        observation: ExecutionObservation<RunnerExecutionInstant>,
    ) -> impl Future<Output = ()> + Send {
        let observer = self.clone();
        async move {
            let ExecutionObservation::Transition(transition) = observation else {
                // Invocation-level command streams and agent transcript activity remain local.
                return;
            };
            let fence = observer.post_stop_fence.lock();
            if *fence && !is_lease_loss_terminal_transition(&transition) {
                return;
            }
            let mut state = observer.lock();
            if state.transition_count == observer.transition_budget || state.faulted {
                state.faulted = true;
                return;
            }
            let Some(sequence) = state.last_sequence.checked_add(1) else {
                state.faulted = true;
                return;
            };
            let terminal = match &transition.event {
                TransitionEvent::Workflow { to, .. }
                    if !matches!(to, WorkflowState::Executing { .. }) =>
                {
                    Some(to.clone())
                }
                _ => None,
            };
            if terminal.is_some() && state.terminal_sequence.is_some() {
                state.faulted = true;
                return;
            }
            let workflow_event = workflow_event(&transition);
            let enqueued = observer.outbox.enqueue(AssignmentObservation::Execution {
                assignment_id: observer.assignment_id.clone(),
                attempt_id: observer.attempt_id.clone(),
                report: ExecutionReport::Transition {
                    execution_event_sequence: sequence,
                    workflow_event,
                },
            });
            if enqueued.is_err() {
                state.faulted = true;
                return;
            }
            state.transition_count += 1;
            state.last_sequence = sequence;
            if let Some(terminal) = terminal {
                state.terminal_sequence = Some(sequence);
                state.terminal_state = Some(terminal);
            }
        }
    }
}

fn is_lease_loss_terminal_transition(
    transition: &TransitionObservation<RunnerExecutionInstant>,
) -> bool {
    matches!(
        &transition.event,
        TransitionEvent::Workflow {
            to: WorkflowState::Cancelled {
                reason: CancellationReason::ExecutionLeaseExpired,
            },
            ..
        }
    )
}

fn workflow_event(transition: &TransitionObservation<RunnerExecutionInstant>) -> Value {
    match &transition.event {
        TransitionEvent::Step {
            sequence,
            step,
            failure_policy,
            from,
            to,
        } => {
            let mut event = serde_json::Map::from_iter([
                ("eventVersion".to_owned(), json!(1)),
                ("eventType".to_owned(), json!("step_state_changed")),
                ("transitionSequence".to_owned(), json!(sequence.get())),
                ("stepId".to_owned(), json!(step)),
                ("failurePolicy".to_owned(), json!(failure_policy)),
                ("from".to_owned(), json!(step_state_name(*from))),
                ("to".to_owned(), json!(step_state_name(*to))),
            ]);
            if let Some(observed) = &transition.step {
                match observed {
                    ObservedStepTransition::OutputsCommitted { .. } => {}
                    ObservedStepTransition::Failed { phase, cause } => {
                        event.insert("failure".to_owned(), failure_evidence(*phase, cause));
                    }
                    ObservedStepTransition::Blocked { dependency } => {
                        event.insert("dependency".to_owned(), json!(dependency));
                    }
                    ObservedStepTransition::NotRun { reason } => {
                        event.insert("reason".to_owned(), json!(not_run_reason(*reason)));
                    }
                    ObservedStepTransition::Cancelling { reason }
                    | ObservedStepTransition::Cancelled { reason } => {
                        event.insert("reason".to_owned(), json!(cancellation_reason(*reason)));
                    }
                }
            }
            Value::Object(event)
        }
        TransitionEvent::Workflow { sequence, from, to } => json!({
            "eventVersion": 1,
            "eventType": "workflow_state_changed",
            "transitionSequence": sequence.get(),
            "from": workflow_state(from),
            "to": workflow_state(to),
        }),
        TransitionEvent::CancellationAccepted {
            sequence,
            reason,
            deadline,
        } => json!({
            "eventVersion": 1,
            "eventType": "cancellation_accepted",
            "transitionSequence": sequence.get(),
            "reason": cancellation_reason(*reason),
            "deadline": format_utc(deadline.utc),
        }),
    }
}

fn workflow_state(state: &WorkflowState<StepFailureCause>) -> Value {
    match state {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => json!({ "state": "executing", "gate": "open" }),
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { primary_failure },
        } => json!({
            "state": "executing",
            "gate": "failure_stopped",
            "primaryFailure": workflow_failure(primary_failure),
        }),
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_failure: None,
                },
        } => json!({
            "state": "executing",
            "gate": "cancelling",
            "reason": cancellation_reason(*reason),
        }),
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_failure: Some(prior_failure),
                },
        } => json!({
            "state": "executing",
            "gate": "cancelling",
            "reason": cancellation_reason(*reason),
            "priorFailure": workflow_failure(prior_failure),
        }),
        WorkflowState::Succeeded => json!({ "state": "succeeded" }),
        WorkflowState::Failed {
            primary_failure,
            later_cancellation: None,
        } => json!({
            "state": "failed",
            "primaryFailure": workflow_failure(primary_failure),
        }),
        WorkflowState::Failed {
            primary_failure,
            later_cancellation: Some(later_cancellation),
        } => json!({
            "state": "failed",
            "primaryFailure": workflow_failure(primary_failure),
            "laterCancellation": cancellation_reason(*later_cancellation),
        }),
        WorkflowState::Cancelled { reason } => json!({
            "state": "cancelled",
            "reason": cancellation_reason(*reason),
        }),
    }
}

fn workflow_failure(failure: &StepFailure<StepFailureCause>) -> Value {
    json!({
        "stepId": failure.step,
        "failure": failure_evidence(failure.phase, &failure.cause),
    })
}

fn failure_evidence(phase: FailurePhase, cause: &StepFailureCause) -> Value {
    match (phase, cause) {
        (FailurePhase::Start, StepFailureCause::Start(cause)) => {
            let (cause, exit_code) = start_failure(cause);
            failure_value("start", cause, exit_code)
        }
        (FailurePhase::Execution, StepFailureCause::Execution(cause)) => {
            let (cause, exit_code) = execution_failure(cause);
            failure_value("execution", cause, exit_code)
        }
        (FailurePhase::OutputCapture, StepFailureCause::OutputCapture(cause)) => {
            failure_value("output_capture", output_capture_failure(cause), None)
        }
        _ => failure_value("start", "step_unavailable", None),
    }
}

fn failure_value(phase: &str, cause: &str, exit_code: Option<i32>) -> Value {
    match exit_code {
        Some(exit_code) => json!({ "phase": phase, "cause": cause, "exitCode": exit_code }),
        None => json!({ "phase": phase, "cause": cause }),
    }
}

fn start_failure(failure: &StepStartFailure) -> (&'static str, Option<i32>) {
    let cause = match failure {
        StepStartFailure::StepUnavailable => "step_unavailable",
        StepStartFailure::PreparationTaskUnavailable => "preparation_task_unavailable",
        StepStartFailure::InputsUnavailable => "inputs_unavailable",
        StepStartFailure::InputPreparation(failure) => input_preparation_failure(failure.kind()),
        StepStartFailure::AgentInput(failure) => agent_input_failure(failure),
        StepStartFailure::Agent(failure) => agent_failure(failure),
        StepStartFailure::AgentRuntimeUnavailable => "agent_runtime_unavailable",
        StepStartFailure::OutputsUnsupported => "outputs_unsupported",
        StepStartFailure::WorkingDirectory(failure) => working_directory_failure(*failure),
        StepStartFailure::CommandPreparation(failure) => match failure {
            CommandPreparationFailure::InvalidArgv => "invalid_argv",
            CommandPreparationFailure::PathNotConfigured => "path_not_configured",
            CommandPreparationFailure::ExecutableNotFound => "executable_not_found",
            CommandPreparationFailure::ExecutableUnavailable => "executable_unavailable",
        },
        StepStartFailure::CommandLaunch(failure) => match failure {
            CommandLaunchFailure::NotFound => "command_not_found",
            CommandLaunchFailure::PermissionDenied => "command_permission_denied",
            CommandLaunchFailure::InvalidInput => "command_invalid_input",
            CommandLaunchFailure::Other => "command_launch_failed",
        },
    };
    (cause, None)
}

fn input_preparation_failure(failure: InputPreparationFailureKind) -> &'static str {
    match failure {
        InputPreparationFailureKind::InvalidInputName => "input_invalid_name",
        InputPreparationFailureKind::ValueCountLimitExceeded => "input_value_count_limit_exceeded",
        InputPreparationFailureKind::ValueSizeLimitExceeded => "input_value_size_limit_exceeded",
        InputPreparationFailureKind::TotalSizeLimitExceeded => "input_total_size_limit_exceeded",
        InputPreparationFailureKind::CollectionOrdinalLimitExceeded => {
            "input_collection_ordinal_limit_exceeded"
        }
        InputPreparationFailureKind::ValueTypeMismatch => "input_value_type_mismatch",
        InputPreparationFailureKind::SourceUnavailable => "input_source_unavailable",
        InputPreparationFailureKind::StagingUnavailable => "input_staging_unavailable",
        InputPreparationFailureKind::LiveLimitExceeded => "input_live_limit_exceeded",
    }
}

fn agent_input_failure(failure: &AgentInputStartFailure) -> &'static str {
    match failure {
        AgentInputStartFailure::StepUnavailable => "agent_input_step_unavailable",
        AgentInputStartFailure::AgentAdmissionUnavailable => "agent_admission_unavailable",
        AgentInputStartFailure::InputsUnavailable => "agent_inputs_unavailable",
        AgentInputStartFailure::MissingUpstreamValue { .. } => "agent_missing_upstream_value",
        AgentInputStartFailure::ValueTypeMismatch { .. } => "agent_value_type_mismatch",
        AgentInputStartFailure::RetainedSourceUnavailable { .. } => {
            "agent_retained_source_unavailable"
        }
        AgentInputStartFailure::InvalidRetainedText { .. } => "agent_invalid_retained_text",
        AgentInputStartFailure::ResultSchemaUnavailable { .. } => "agent_result_schema_unavailable",
        AgentInputStartFailure::InvalidValueMode => "agent_invalid_value_mode",
        AgentInputStartFailure::AttachmentCountLimitExceeded { .. } => {
            "agent_attachment_count_limit_exceeded"
        }
        AgentInputStartFailure::AttachmentBytesLimitExceeded { .. } => {
            "agent_attachment_bytes_limit_exceeded"
        }
        AgentInputStartFailure::WorkingDirectory(failure) => working_directory_failure(*failure),
        AgentInputStartFailure::ArtifactStagingMismatch => "agent_artifact_staging_mismatch",
        AgentInputStartFailure::AgentStagingMismatch => "agent_staging_mismatch",
        AgentInputStartFailure::StagingUnavailable => "agent_staging_unavailable",
    }
}

fn working_directory_failure(failure: WorkingDirectoryFailure) -> &'static str {
    match failure {
        WorkingDirectoryFailure::ExecutionRootRebound => "execution_root_rebound",
        WorkingDirectoryFailure::Unavailable => "working_directory_unavailable",
        WorkingDirectoryFailure::EscapesExecutionRoot => "working_directory_escape",
        WorkingDirectoryFailure::NotDirectory => "working_directory_not_directory",
    }
}

fn execution_failure(failure: &StepExecutionFailure) -> (&'static str, Option<i32>) {
    match failure {
        StepExecutionFailure::Command(CommandExecutionFailure::UnsuccessfulExit {
            code: Some(code),
        }) => ("command_unsuccessful_exit", Some(*code)),
        StepExecutionFailure::Command(CommandExecutionFailure::UnsuccessfulExit { code: None }) => {
            ("command_terminated", None)
        }
        StepExecutionFailure::Command(CommandExecutionFailure::Wait) => {
            ("command_wait_failed", None)
        }
        StepExecutionFailure::Agent(failure) => (agent_failure(failure), None),
    }
}

fn agent_failure(failure: &AgentFailureCause) -> &'static str {
    match failure {
        AgentFailureCause::HarnessStartFailed => "agent_harness_start_failed",
        AgentFailureCause::HarnessInputTooLarge {
            input: AgentInputKind::SystemPrompt,
            ..
        } => "agent_system_prompt_too_large",
        AgentFailureCause::HarnessInputTooLarge {
            input: AgentInputKind::Message,
            ..
        } => "agent_message_too_large",
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::ModelOutputTruncated,
        } => "agent_harness_model_output_truncated",
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::UnexpectedTerminalToolUse,
        } => "agent_harness_unexpected_terminal_tool_use",
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::ModelError,
        } => "agent_harness_model_error",
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::ModelAborted,
        } => "agent_harness_model_aborted",
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::UnsuccessfulExit,
        } => "agent_harness_unsuccessful_exit",
        AgentFailureCause::HarnessProtocolFailed => "agent_harness_protocol_failed",
        AgentFailureCause::MissingResponse => "agent_missing_response",
        AgentFailureCause::MissingResult => "agent_missing_result",
        AgentFailureCause::ResultValidationLimitExceeded { .. } => {
            "agent_result_validation_limit_exceeded"
        }
        AgentFailureCause::CapturedValueTooLarge => "agent_captured_value_too_large",
        AgentFailureCause::ResultSettlementFailed => "agent_result_settlement_failed",
    }
}

fn output_capture_failure(failure: &OutputCaptureFailure) -> &'static str {
    match failure {
        OutputCaptureFailure::StepUnavailable => "output_step_unavailable",
        OutputCaptureFailure::UnsupportedOutput => "output_unsupported",
        OutputCaptureFailure::Capture(failure) => capture_failure(failure.kind()),
        OutputCaptureFailure::Git { .. } => "output_git_capture_failed",
        OutputCaptureFailure::TaskUnavailable => "output_task_unavailable",
    }
}

fn capture_failure(failure: CaptureFailureKind) -> &'static str {
    match failure {
        CaptureFailureKind::AbsolutePath => "output_absolute_path",
        CaptureFailureKind::LexicalEscape => "output_lexical_escape",
        CaptureFailureKind::EmptyPath => "output_empty_path",
        CaptureFailureKind::Missing => "output_missing",
        CaptureFailureKind::SymbolicLink => "output_symbolic_link",
        CaptureFailureKind::NotDirectory => "output_not_directory",
        CaptureFailureKind::NotRegularFile => "output_not_regular_file",
        CaptureFailureKind::SourceUnavailable => "output_source_unavailable",
        CaptureFailureKind::FileCountLimitExceeded => "output_file_count_limit_exceeded",
        CaptureFailureKind::FileSizeLimitExceeded => "output_file_size_limit_exceeded",
        CaptureFailureKind::TotalSizeLimitExceeded => "output_total_size_limit_exceeded",
        CaptureFailureKind::GitCarrierCountLimitExceeded => {
            "output_git_carrier_count_limit_exceeded"
        }
        CaptureFailureKind::GitCarrierSizeLimitExceeded => "output_git_carrier_size_limit_exceeded",
        CaptureFailureKind::TotalGitCarrierSizeLimitExceeded => {
            "output_total_git_carrier_size_limit_exceeded"
        }
        CaptureFailureKind::CarrierProducerUnavailable => "output_carrier_producer_unavailable",
        CaptureFailureKind::StagingUnavailable => "output_staging_unavailable",
    }
}

fn terminal_result_agrees(
    terminal: Option<&WorkflowState<StepFailureCause>>,
    outcome: &RunOutcome<StepFailureCause>,
) -> bool {
    match (terminal, outcome) {
        (Some(WorkflowState::Succeeded), RunOutcome::Succeeded) => true,
        (
            Some(WorkflowState::Failed {
                primary_failure: left_failure,
                later_cancellation: left_cancellation,
            }),
            RunOutcome::Failed {
                primary_failure: right_failure,
                later_cancellation: right_cancellation,
            },
        ) => left_failure == right_failure && left_cancellation == right_cancellation,
        (
            Some(WorkflowState::Cancelled { reason: left }),
            RunOutcome::Cancelled { reason: right },
        ) => left == right,
        _ => false,
    }
}

fn step_state_name(state: StepStateKind) -> &'static str {
    match state {
        StepStateKind::Pending => "pending",
        StepStateKind::Starting => "starting",
        StepStateKind::Running => "running",
        StepStateKind::CapturingOutputs => "capturing_outputs",
        StepStateKind::Cancelling => "cancelling",
        StepStateKind::Succeeded => "succeeded",
        StepStateKind::Failed => "failed",
        StepStateKind::Blocked => "blocked",
        StepStateKind::NotRun => "not_run",
        StepStateKind::Cancelled => "cancelled",
    }
}

fn not_run_reason(reason: NotRunReason) -> &'static str {
    match reason {
        NotRunReason::FailureStop => "failure_stop",
    }
}

fn cancellation_reason(reason: CancellationReason) -> &'static str {
    reason.as_str()
}

fn format_utc(value: OffsetDateTime) -> String {
    value
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

// Runner Serve's transport-independent clock intentionally stays separate from the
// local command's publication-aware execution clock.
// jscpd:ignore-start
#[derive(Clone, Copy, Debug)]
struct RunnerExecutionInstant {
    monotonic: Instant,
    utc: OffsetDateTime,
}

impl Add<Duration> for RunnerExecutionInstant {
    type Output = Self;

    fn add(self, duration: Duration) -> Self::Output {
        Self {
            monotonic: self.monotonic + duration,
            utc: self.utc + duration,
        }
    }
}
// jscpd:ignore-end

#[derive(Clone, Copy)]
struct RunnerExecutionClock;

impl CoordinatorClock for RunnerExecutionClock {
    type Instant = RunnerExecutionInstant;

    #[expect(
        clippy::disallowed_methods,
        reason = "RunnerExecutionClock is the Runner Serve workflow clock boundary"
    )]
    fn now(&mut self) -> Self::Instant {
        RunnerExecutionInstant {
            monotonic: Instant::now(),
            utc: OffsetDateTime::now_utc(),
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "RunnerExecutionClock is the Runner Serve deadline wait boundary"
    )]
    fn wait_until(&self, deadline: Self::Instant) -> impl Future<Output = ()> + Send {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.monotonic))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroU64;

    use super::*;
    use crate::execution::workflow::agent::PositiveDuration;
    use crate::execution::workflow::validated::{
        ResolvedOutputSource, WorkflowNode, WorkflowNodeRole, WorkflowValueType,
    };
    use crate::runner::service::test_support::{
        SleepRelease, controlled_sleeper, sleep_request, with_watchdog,
    };
    use crate::runner_protocol::{RunnerEnvelope, encode_runner_frame};

    fn lease_authority(now: Instant) -> LeaseAuthority {
        LeaseAuthority {
            sequence: 4,
            renewal_deadline: now.checked_add(Duration::from_secs(2)).unwrap(),
            cancellation_deadline: now.checked_add(Duration::from_secs(4)).unwrap(),
            stop_deadline: now.checked_add(Duration::from_secs(5)).unwrap(),
            force_stop_deadline: now.checked_add(Duration::from_secs(8)).unwrap(),
            expires_deadline: now.checked_add(Duration::from_secs(12)).unwrap(),
            terminal_report_delivery_budget: Duration::from_secs(7),
            revoked: false,
        }
    }

    struct SupervisedLeaseFixture {
        result: tokio::sync::oneshot::Sender<&'static str>,
        task: tokio::task::JoinHandle<LeaseExecution<&'static str>>,
        sleeps: tokio::sync::mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
        cancellation: crate::execution::workflow::admission::CancellationSource,
        fence: PostStopFence,
        guards: AssignmentProcessGuards,
        _authority: tokio::sync::watch::Sender<LeaseAuthority>,
    }

    struct SupervisedExecution<Output> {
        task: tokio::task::JoinHandle<LeaseExecution<Output>>,
        cancellation: crate::execution::workflow::admission::CancellationSource,
        fence: PostStopFence,
        guards: AssignmentProcessGuards,
        authority: tokio::sync::watch::Sender<LeaseAuthority>,
    }

    fn supervise_execution<Execution, Output>(
        sleeper: Arc<dyn Sleeper>,
        authority: LeaseAuthority,
        execution: Execution,
    ) -> SupervisedExecution<Output>
    where
        Execution: Future<Output = Output> + Send + 'static,
        Output: Send + 'static,
    {
        let cancellation = crate::execution::workflow::admission::CancellationSource::new();
        let observed_cancellation = cancellation.clone();
        let outbox = ObservationOutbox::new();
        let fence = PostStopFence::new();
        let observed_fence = fence.clone();
        let guards = AssignmentProcessGuards::new();
        let observed_guards = guards.clone();
        let (authority_sender, authority_updates) = tokio::sync::watch::channel(authority);
        let task = tokio::spawn(async move {
            run_under_lease(
                execution,
                &cancellation,
                sleeper.as_ref(),
                authority_updates,
                &outbox,
                "asn_01k0z6r1w8f4jy2m7q9v3x5abc",
                "atm_01k0z6r1w8f4jy2m7q9v3x5abc",
                &fence,
                &guards,
            )
            .await
        });
        SupervisedExecution {
            task,
            cancellation: observed_cancellation,
            fence: observed_fence,
            guards: observed_guards,
            authority: authority_sender,
        }
    }

    fn supervised_lease_fixture() -> SupervisedLeaseFixture {
        let (sleeper, sleeps) = controlled_sleeper();
        let (result_sender, result) = tokio::sync::oneshot::channel();
        let supervised = supervise_execution(
            Arc::clone(&sleeper),
            lease_authority(sleeper.now()),
            async { result.await.expect("fixture result") },
        );
        SupervisedLeaseFixture {
            result: result_sender,
            task: supervised.task,
            sleeps,
            cancellation: supervised.cancellation,
            fence: supervised.fence,
            guards: supervised.guards,
            _authority: supervised.authority,
        }
    }

    #[tokio::test]
    async fn sixty_second_lease_grace_allows_clean_exit_after_thirty_seconds() {
        let (sleeper, mut sleeps) = controlled_sleeper();
        let now = sleeper.now();
        let execution_sleeper = Arc::clone(&sleeper);
        let supervised = supervise_execution(
            Arc::clone(&sleeper),
            LeaseAuthority {
                sequence: 1,
                renewal_deadline: now,
                cancellation_deadline: now,
                stop_deadline: now.checked_add(Duration::from_secs(60)).unwrap(),
                force_stop_deadline: now.checked_add(Duration::from_secs(65)).unwrap(),
                expires_deadline: now.checked_add(Duration::from_secs(72)).unwrap(),
                terminal_report_delivery_budget: Duration::from_secs(7),
                revoked: false,
            },
            async move {
                execution_sleeper.sleep(Duration::from_secs(30)).await;
                "clean-exit"
            },
        );

        assert_eq!(
            with_watchdog(supervised.cancellation.wait_for_cancellation())
                .await
                .expect("lease cancellation was not requested"),
            CancellationReason::ExecutionLeaseExpired
        );
        sleep_request(&mut sleeps, Duration::from_secs(30))
            .await
            .release();

        assert!(matches!(
            with_watchdog(supervised.task)
                .await
                .expect("lease supervision timed out")
                .expect("lease supervision task failed"),
            LeaseExecution::Completed {
                output: "clean-exit",
                ..
            }
        ));
        assert!(!supervised.fence.is_fenced());
        assert!(!supervised.guards.forced_containment_started());
    }

    #[tokio::test]
    async fn exact_stop_boundary_fences_before_a_ready_late_result() {
        let mut fixture = supervised_lease_fixture();
        sleep_request(&mut fixture.sleeps, Duration::from_secs(2))
            .await
            .release();
        sleep_request(&mut fixture.sleeps, Duration::from_secs(2))
            .await
            .release();
        assert_eq!(
            with_watchdog(fixture.cancellation.wait_for_cancellation())
                .await
                .expect("lease cancellation was not requested"),
            CancellationReason::ExecutionLeaseExpired
        );
        let stop_boundary = sleep_request(&mut fixture.sleeps, Duration::from_secs(1)).await;
        fixture.result.send("late-success").unwrap();
        stop_boundary.release();

        let result = with_watchdog(fixture.task)
            .await
            .expect("lease supervision timed out")
            .expect("lease supervision task failed");
        assert!(fixture.fence.is_fenced());
        assert!(fixture.guards.forced_containment_started());
        assert!(matches!(
            result,
            LeaseExecution::Completed {
                output: "late-success",
                final_delivery_budget: Some(duration),
            } if duration == Duration::from_secs(7)
        ));
    }

    #[tokio::test]
    async fn force_reap_accepts_exact_boundary_and_rejects_late_completion() {
        let mut exact = supervised_lease_fixture();
        for duration in [
            Duration::from_secs(2),
            Duration::from_secs(2),
            Duration::from_secs(1),
        ] {
            sleep_request(&mut exact.sleeps, duration).await.release();
        }
        let reap_boundary = sleep_request(&mut exact.sleeps, Duration::from_secs(3)).await;
        exact.result.send("exact-boundary").unwrap();
        reap_boundary.release();

        assert!(matches!(
            with_watchdog(exact.task)
                .await
                .expect("lease supervision timed out")
                .expect("lease supervision task failed"),
            LeaseExecution::Completed {
                output: "exact-boundary",
                ..
            }
        ));

        let mut late = supervised_lease_fixture();
        for duration in [
            Duration::from_secs(2),
            Duration::from_secs(2),
            Duration::from_secs(1),
            Duration::from_secs(3),
        ] {
            sleep_request(&mut late.sleeps, duration).await.release();
        }
        assert!(matches!(
            with_watchdog(late.task)
                .await
                .expect("late lease supervision timed out")
                .expect("late lease supervision task failed"),
            LeaseExecution::ContainmentDeadline
        ));
        assert!(late.result.send("one-unit-late").is_err());
    }

    #[tokio::test]
    async fn post_stop_fence_rejects_late_success_but_allows_lease_terminal() {
        let outbox = ObservationOutbox::new();
        let fence = PostStopFence::new();
        let observer = RunnerExecutionObserver::new(
            "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            2,
            outbox.clone(),
            fence.clone(),
        );
        fence.fence();
        observer
            .observe(ExecutionObservation::Transition(TransitionObservation {
                event: TransitionEvent::Workflow {
                    sequence: Default::default(),
                    from: WorkflowState::Executing {
                        gate: SchedulingGate::Open,
                    },
                    to: WorkflowState::Succeeded,
                },
                step: None,
            }))
            .await;
        assert_eq!(observer.last_sequence(), 0);
        assert!(outbox.pending(&BTreeSet::new(), 1).is_empty());

        let lease_terminal = TransitionObservation {
            event: TransitionEvent::Workflow {
                sequence: Default::default(),
                from: WorkflowState::Executing {
                    gate: SchedulingGate::Cancelling {
                        reason: CancellationReason::ExecutionLeaseExpired,
                        prior_failure: None,
                    },
                },
                to: WorkflowState::Cancelled {
                    reason: CancellationReason::ExecutionLeaseExpired,
                },
            },
            step: None,
        };
        assert!(is_lease_loss_terminal_transition(&lease_terminal));
    }

    #[test]
    fn assignment_supervisor_registers_before_release_and_closes_on_containment() {
        let guards = AssignmentProcessGuards::new();
        let registry = guards.registry(true);
        let identity = AuthenticatedProcessGroup::new(
            rustix::process::Pid::from_raw(41).unwrap(),
            "fixture-start".to_owned(),
        )
        .unwrap();
        let mut registration = registry.register("step", 9, &identity).unwrap();

        registration.mark_released().unwrap();
        guards.begin_forced_containment();
        assert!(guards.forced_containment_started());
        assert!(registry.register("later", 10, &identity).is_err());
        registration.mark_quiesced().unwrap();
        assert!(guards.is_quiescent());
    }

    #[test]
    fn advisory_step_transition_preserves_policy_and_raw_disposition() {
        let transition = TransitionObservation::<RunnerExecutionInstant> {
            event: TransitionEvent::Step {
                sequence: crate::execution::workflow::runtime::TransitionSequence::default(),
                step: "lint".to_owned(),
                failure_policy: crate::execution::workflow::document::FailurePolicy::Advisory,
                from: StepStateKind::Pending,
                to: StepStateKind::Blocked,
            },
            step: Some(ObservedStepTransition::Blocked {
                dependency: "analyze".to_owned(),
            }),
        };

        assert_eq!(
            workflow_event(&transition),
            json!({
                "eventVersion": 1,
                "eventType": "step_state_changed",
                "transitionSequence": 0,
                "stepId": "lint",
                "failurePolicy": "advisory",
                "from": "pending",
                "to": "blocked",
                "dependency": "analyze",
            })
        );
    }

    #[test]
    fn closed_failure_vocabulary_projects_and_encodes() {
        let source = ResolvedOutputSource {
            node: WorkflowNode {
                id: "upstream".to_owned(),
                role: WorkflowNodeRole::Step,
            },
            output: "value".to_owned(),
            value_type: WorkflowValueType::Text,
        };
        let mut start_failures = vec![
            (StepStartFailure::StepUnavailable, "step_unavailable"),
            (
                StepStartFailure::PreparationTaskUnavailable,
                "preparation_task_unavailable",
            ),
            (StepStartFailure::InputsUnavailable, "inputs_unavailable"),
            (
                StepStartFailure::AgentInput(Box::new(AgentInputStartFailure::StepUnavailable)),
                "agent_input_step_unavailable",
            ),
            (
                StepStartFailure::AgentInput(Box::new(
                    AgentInputStartFailure::AgentAdmissionUnavailable,
                )),
                "agent_admission_unavailable",
            ),
            (
                StepStartFailure::AgentInput(Box::new(AgentInputStartFailure::InputsUnavailable)),
                "agent_inputs_unavailable",
            ),
            (
                StepStartFailure::AgentInput(Box::new(
                    AgentInputStartFailure::MissingUpstreamValue {
                        source: source.clone(),
                    },
                )),
                "agent_missing_upstream_value",
            ),
            (
                StepStartFailure::AgentInput(Box::new(AgentInputStartFailure::ValueTypeMismatch {
                    source: source.clone(),
                })),
                "agent_value_type_mismatch",
            ),
            (
                StepStartFailure::AgentInput(Box::new(
                    AgentInputStartFailure::RetainedSourceUnavailable {
                        path: "source".to_owned(),
                    },
                )),
                "agent_retained_source_unavailable",
            ),
            (
                StepStartFailure::AgentInput(Box::new(
                    AgentInputStartFailure::InvalidRetainedText {
                        path: "source".to_owned(),
                    },
                )),
                "agent_invalid_retained_text",
            ),
            (
                StepStartFailure::AgentInput(Box::new(
                    AgentInputStartFailure::ResultSchemaUnavailable {
                        output: "result".to_owned(),
                    },
                )),
                "agent_result_schema_unavailable",
            ),
            (
                StepStartFailure::AgentInput(Box::new(AgentInputStartFailure::InvalidValueMode)),
                "agent_invalid_value_mode",
            ),
            (
                StepStartFailure::AgentInput(Box::new(
                    AgentInputStartFailure::AttachmentCountLimitExceeded { maximum: 1 },
                )),
                "agent_attachment_count_limit_exceeded",
            ),
            (
                StepStartFailure::AgentInput(Box::new(
                    AgentInputStartFailure::AttachmentBytesLimitExceeded { maximum: 1 },
                )),
                "agent_attachment_bytes_limit_exceeded",
            ),
            (
                StepStartFailure::AgentInput(Box::new(
                    AgentInputStartFailure::ArtifactStagingMismatch,
                )),
                "agent_artifact_staging_mismatch",
            ),
            (
                StepStartFailure::AgentInput(Box::new(
                    AgentInputStartFailure::AgentStagingMismatch,
                )),
                "agent_staging_mismatch",
            ),
            (
                StepStartFailure::AgentInput(Box::new(AgentInputStartFailure::StagingUnavailable)),
                "agent_staging_unavailable",
            ),
            (
                StepStartFailure::AgentRuntimeUnavailable,
                "agent_runtime_unavailable",
            ),
            (StepStartFailure::OutputsUnsupported, "outputs_unsupported"),
        ];
        for (failure, expected) in [
            (
                WorkingDirectoryFailure::ExecutionRootRebound,
                "execution_root_rebound",
            ),
            (
                WorkingDirectoryFailure::Unavailable,
                "working_directory_unavailable",
            ),
            (
                WorkingDirectoryFailure::EscapesExecutionRoot,
                "working_directory_escape",
            ),
            (
                WorkingDirectoryFailure::NotDirectory,
                "working_directory_not_directory",
            ),
        ] {
            start_failures.push((StepStartFailure::WorkingDirectory(failure), expected));
        }
        for (failure, expected) in [
            (CommandPreparationFailure::InvalidArgv, "invalid_argv"),
            (
                CommandPreparationFailure::PathNotConfigured,
                "path_not_configured",
            ),
            (
                CommandPreparationFailure::ExecutableNotFound,
                "executable_not_found",
            ),
            (
                CommandPreparationFailure::ExecutableUnavailable,
                "executable_unavailable",
            ),
        ] {
            start_failures.push((StepStartFailure::CommandPreparation(failure), expected));
        }
        for (failure, expected) in [
            (CommandLaunchFailure::NotFound, "command_not_found"),
            (
                CommandLaunchFailure::PermissionDenied,
                "command_permission_denied",
            ),
            (CommandLaunchFailure::InvalidInput, "command_invalid_input"),
            (CommandLaunchFailure::Other, "command_launch_failed"),
        ] {
            start_failures.push((StepStartFailure::CommandLaunch(failure), expected));
        }
        for (failure, expected) in start_failures {
            assert_projection(
                FailurePhase::Start,
                StepFailureCause::Start(failure),
                "start",
                expected,
                None,
            );
        }

        for (failure, expected) in agent_failures() {
            assert_projection(
                FailurePhase::Start,
                StepFailureCause::Start(StepStartFailure::Agent(failure.clone())),
                "start",
                expected,
                None,
            );
            assert_projection(
                FailurePhase::Execution,
                StepFailureCause::Execution(StepExecutionFailure::Agent(failure)),
                "execution",
                expected,
                None,
            );
        }
        for (failure, expected, exit_code) in [
            (
                CommandExecutionFailure::UnsuccessfulExit { code: Some(23) },
                "command_unsuccessful_exit",
                Some(23),
            ),
            (
                CommandExecutionFailure::UnsuccessfulExit { code: None },
                "command_terminated",
                None,
            ),
            (CommandExecutionFailure::Wait, "command_wait_failed", None),
        ] {
            assert_projection(
                FailurePhase::Execution,
                StepFailureCause::Execution(StepExecutionFailure::Command(failure)),
                "execution",
                expected,
                exit_code,
            );
        }
        for (failure, expected) in [
            (
                OutputCaptureFailure::StepUnavailable,
                "output_step_unavailable",
            ),
            (
                OutputCaptureFailure::UnsupportedOutput,
                "output_unsupported",
            ),
            (
                OutputCaptureFailure::TaskUnavailable,
                "output_task_unavailable",
            ),
        ] {
            assert_projection(
                FailurePhase::OutputCapture,
                StepFailureCause::OutputCapture(failure),
                "output_capture",
                expected,
                None,
            );
        }

        for (kind, expected) in [
            (
                InputPreparationFailureKind::InvalidInputName,
                "input_invalid_name",
            ),
            (
                InputPreparationFailureKind::ValueCountLimitExceeded,
                "input_value_count_limit_exceeded",
            ),
            (
                InputPreparationFailureKind::ValueSizeLimitExceeded,
                "input_value_size_limit_exceeded",
            ),
            (
                InputPreparationFailureKind::TotalSizeLimitExceeded,
                "input_total_size_limit_exceeded",
            ),
            (
                InputPreparationFailureKind::CollectionOrdinalLimitExceeded,
                "input_collection_ordinal_limit_exceeded",
            ),
            (
                InputPreparationFailureKind::ValueTypeMismatch,
                "input_value_type_mismatch",
            ),
            (
                InputPreparationFailureKind::SourceUnavailable,
                "input_source_unavailable",
            ),
            (
                InputPreparationFailureKind::StagingUnavailable,
                "input_staging_unavailable",
            ),
            (
                InputPreparationFailureKind::LiveLimitExceeded,
                "input_live_limit_exceeded",
            ),
        ] {
            assert_eq!(input_preparation_failure(kind), expected);
            assert_encoded_evidence("start", expected, None);
        }
        for (kind, expected) in [
            (CaptureFailureKind::AbsolutePath, "output_absolute_path"),
            (CaptureFailureKind::LexicalEscape, "output_lexical_escape"),
            (CaptureFailureKind::EmptyPath, "output_empty_path"),
            (CaptureFailureKind::Missing, "output_missing"),
            (CaptureFailureKind::SymbolicLink, "output_symbolic_link"),
            (CaptureFailureKind::NotDirectory, "output_not_directory"),
            (
                CaptureFailureKind::NotRegularFile,
                "output_not_regular_file",
            ),
            (
                CaptureFailureKind::SourceUnavailable,
                "output_source_unavailable",
            ),
            (
                CaptureFailureKind::FileCountLimitExceeded,
                "output_file_count_limit_exceeded",
            ),
            (
                CaptureFailureKind::FileSizeLimitExceeded,
                "output_file_size_limit_exceeded",
            ),
            (
                CaptureFailureKind::TotalSizeLimitExceeded,
                "output_total_size_limit_exceeded",
            ),
            (
                CaptureFailureKind::StagingUnavailable,
                "output_staging_unavailable",
            ),
        ] {
            assert_eq!(capture_failure(kind), expected);
            assert_encoded_evidence("output_capture", expected, None);
        }
    }

    fn agent_failures() -> Vec<(AgentFailureCause, &'static str)> {
        vec![
            (
                AgentFailureCause::HarnessStartFailed,
                "agent_harness_start_failed",
            ),
            (
                AgentFailureCause::HarnessInputTooLarge {
                    input: AgentInputKind::SystemPrompt,
                    admitted_bytes: NonZeroU64::new(1).unwrap(),
                    observed_bytes: 2,
                },
                "agent_system_prompt_too_large",
            ),
            (
                AgentFailureCause::HarnessInputTooLarge {
                    input: AgentInputKind::Message,
                    admitted_bytes: NonZeroU64::new(1).unwrap(),
                    observed_bytes: 2,
                },
                "agent_message_too_large",
            ),
            (
                AgentFailureCause::HarnessFailed {
                    detail: AgentHarnessFailureDetail::ModelOutputTruncated,
                },
                "agent_harness_model_output_truncated",
            ),
            (
                AgentFailureCause::HarnessFailed {
                    detail: AgentHarnessFailureDetail::UnexpectedTerminalToolUse,
                },
                "agent_harness_unexpected_terminal_tool_use",
            ),
            (
                AgentFailureCause::HarnessFailed {
                    detail: AgentHarnessFailureDetail::ModelError,
                },
                "agent_harness_model_error",
            ),
            (
                AgentFailureCause::HarnessFailed {
                    detail: AgentHarnessFailureDetail::ModelAborted,
                },
                "agent_harness_model_aborted",
            ),
            (
                AgentFailureCause::HarnessFailed {
                    detail: AgentHarnessFailureDetail::UnsuccessfulExit,
                },
                "agent_harness_unsuccessful_exit",
            ),
            (
                AgentFailureCause::HarnessProtocolFailed,
                "agent_harness_protocol_failed",
            ),
            (AgentFailureCause::MissingResponse, "agent_missing_response"),
            (AgentFailureCause::MissingResult, "agent_missing_result"),
            (
                AgentFailureCause::ResultValidationLimitExceeded {
                    deadline: PositiveDuration::new(Duration::from_secs(1)).unwrap(),
                },
                "agent_result_validation_limit_exceeded",
            ),
            (
                AgentFailureCause::CapturedValueTooLarge,
                "agent_captured_value_too_large",
            ),
            (
                AgentFailureCause::ResultSettlementFailed,
                "agent_result_settlement_failed",
            ),
        ]
    }

    fn assert_projection(
        phase: FailurePhase,
        cause: StepFailureCause,
        expected_phase: &str,
        expected_cause: &str,
        exit_code: Option<i32>,
    ) {
        let evidence = failure_evidence(phase, &cause);
        assert_eq!(
            evidence,
            failure_value(expected_phase, expected_cause, exit_code)
        );
        let failure = StepFailure {
            step: "step".to_owned(),
            phase,
            cause,
        };
        assert_encoded_evidence_value(expected_phase, evidence, workflow_failure(&failure));
    }

    fn assert_encoded_evidence(phase: &str, cause: &str, exit_code: Option<i32>) {
        let evidence = failure_value(phase, cause, exit_code);
        let terminal = json!({
            "stepId": "step",
            "failure": evidence.clone(),
        });
        assert_encoded_evidence_value(phase, evidence, terminal);
    }

    fn assert_encoded_evidence_value(phase: &str, evidence: Value, terminal: Value) {
        let from = match phase {
            "start" => "starting",
            "execution" => "running",
            "output_capture" => "capturing_outputs",
            _ => unreachable!(),
        };
        let observations = [
            AssignmentObservation::Execution {
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                report: ExecutionReport::Transition {
                    execution_event_sequence: 1,
                    workflow_event: json!({
                        "eventVersion": 1,
                        "eventType": "step_state_changed",
                        "transitionSequence": 1,
                        "stepId": "step",
                        "failurePolicy": "required",
                        "from": from,
                        "to": "failed",
                        "failure": evidence,
                    }),
                },
            },
            AssignmentObservation::Execution {
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                report: ExecutionReport::Finished {
                    final_execution_event_sequence: 1,
                    outcome: json!({
                        "outcome": "failed",
                        "failure": terminal,
                    }),
                },
            },
        ];
        for (index, observation) in observations.into_iter().enumerate() {
            let frame = observation.runner_frame(RunnerEnvelope {
                message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                sequence: u64::try_from(index + 1).unwrap(),
                sent_at: "2026-07-23T00:00:00Z".to_owned(),
            });
            encode_runner_frame(&frame).expect("mapped failure must satisfy the runner protocol");
        }
    }
}
