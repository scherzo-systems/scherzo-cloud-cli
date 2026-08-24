use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Add;
use std::os::fd::OwnedFd;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::artifact_delivery::{
    ArtifactDeliveryBroker, ArtifactDeliveryOutcome, ArtifactDeliverySpec,
    ClosedArtifactDeliveryFailure,
};
use super::assignment::{
    AcceptedAssignment, AssignmentObservation, CausalLease, ExecutionReport, LeaseAuthority,
    ManagerEvent, ObservationOutbox, RenewalRequestFailure,
};
use super::lease_clock::{
    LeaseClock, LeaseClockError, LeaseInstant, LeaseWait, LeaseWaitCancellation,
};
use crate::execution::workflow::admission::CancellationReason;
use crate::execution::workflow::agent::dispatch::production_agent_dispatcher;
use crate::execution::workflow::agent::{
    AgentFailure, AgentFailureCause, AgentHarnessFailureDetail, AgentInputKind,
    AgentProtocolRejectionDiagnostic, WorkflowRunId,
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
use crate::execution::workflow::publication::{
    WorkflowRunCancellation, WorkflowRunFinalization, WorkflowRunFinalizationCancellation,
    WorkflowRunResult, WorkflowRunStep, WorkflowRunStepKind, WorkflowRunTiming, WorkflowStepTiming,
    prepare_cloud_workflow_result, summary_disposition_matches,
};
use crate::execution::workflow::runtime::{
    FailurePhase, FinalizationGate, FinalizationSummary, FinalizerResult, NotRunReason, RunOutcome,
    SchedulingGate, StepFailure, StepState, StepStateKind, TransitionEvent, WorkflowState,
};
use crate::execution::workflow::step_runtime::{
    AgentExecution, CommandExecutionFailure, CommandLaunchFailure, CommandPreparationFailure,
    OutputCaptureFailure, StepExecutionFailure, StepFailureCause, StepStartFailure,
    WorkingDirectoryFailure,
};
use crate::execution::workflow::validated::WorkflowNodeRole;

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
    #[cfg(test)]
    quiescence_fixture: Option<Arc<std::sync::atomic::AtomicBool>>,
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
                #[cfg(test)]
                quiescence_fixture: None,
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
        let state = self.lock();
        #[cfg(test)]
        if let Some(quiescent) = &state.quiescence_fixture {
            return quiescent.load(std::sync::atomic::Ordering::Acquire);
        }
        let inspector = SystemProcessIdentityInspector;
        state.records.values().all(|record| {
            record.lifecycle == GuardLifecycle::Quiesced
                || matches!(
                    inspector.observe(&record.identity),
                    ProcessIdentityObservation::Absent
                )
        })
    }

    #[cfg(test)]
    fn use_quiescence_fixture(&self, quiescent: Arc<std::sync::atomic::AtomicBool>) {
        self.lock().quiescence_fixture = Some(quiescent);
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
    final_delivery_deadline: Option<LeaseInstant>,
    lease_clock_failed: bool,
}

impl ExecutionCompletion {
    fn selected(final_observation_id: Option<u64>) -> Self {
        Self {
            final_observation_id,
            final_delivery_deadline: None,
            lease_clock_failed: false,
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

    fn lease_clock_failed(final_observation_id: Option<u64>) -> Self {
        Self {
            final_observation_id,
            final_delivery_deadline: None,
            lease_clock_failed: true,
        }
    }
}

pub(super) struct ExecutionJob {
    accepted: AcceptedAssignment,
    outbox: ObservationOutbox,
    artifact_delivery: ArtifactDeliveryBroker,
    manager_events: tokio::sync::mpsc::UnboundedSender<ManagerEvent>,
    lease_clock: LeaseClock,
    causal_lease: CausalLease,
    pub(super) authority_updates: tokio::sync::watch::Receiver<LeaseAuthority>,
}

impl ExecutionJob {
    pub(super) fn new(
        accepted: AcceptedAssignment,
        outbox: ObservationOutbox,
        artifact_delivery: ArtifactDeliveryBroker,
        manager_events: tokio::sync::mpsc::UnboundedSender<ManagerEvent>,
        lease_clock: LeaseClock,
        causal_lease: CausalLease,
        authority_updates: tokio::sync::watch::Receiver<LeaseAuthority>,
    ) -> Self {
        Self {
            accepted,
            outbox,
            artifact_delivery,
            manager_events,
            lease_clock,
            causal_lease,
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
        if completion.final_observation_id.is_some() && !completion.lease_clock_failed {
            match self.terminal_report_deadline() {
                Ok(deadline) => completion.final_delivery_deadline = Some(deadline),
                Err(_) => completion.lease_clock_failed = true,
            }
        }
        let retained_root = self.accepted.root;
        let _ = self.manager_events.send(ManagerEvent::Finished {
            assignment_id,
            final_observation_id: completion.final_observation_id,
            final_delivery_deadline: completion.final_delivery_deadline,
            lease_clock_failed: completion.lease_clock_failed,
            retained_root: Some(retained_root),
        });
        self.outbox.wake();
    }

    async fn run_workflow(
        &self,
        assignment_id: &str,
        attempt_id: &str,
        run_id: &str,
    ) -> ExecutionCompletion {
        let post_stop_fence = PostStopFence::new();
        let cancellation = self
            .accepted
            .admitted
            .execution()
            .cancellation()
            .source()
            .clone();
        match self.has_execution_authority() {
            Ok(true) => {}
            Ok(false) => return ExecutionCompletion::without_report(),
            Err(_) => {
                return self.fail_before_execution(
                    &cancellation,
                    &post_stop_fence,
                    assignment_id,
                    attempt_id,
                );
            }
        }
        let initial_authority = self.authority_updates.borrow().clone();
        let initial_wait = match self
            .lease_clock
            .start_wait(initial_authority.renewal_request)
        {
            Ok(wait) => wait,
            Err(_) => {
                return self.fail_before_execution(
                    &cancellation,
                    &post_stop_fence,
                    assignment_id,
                    attempt_id,
                );
            }
        };
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

        match self.has_execution_authority() {
            Ok(true) => {}
            Ok(false) => {
                let _ = release_staging(&inputs, agent_staging.as_ref(), &artifacts);
                return ExecutionCompletion::without_report();
            }
            Err(_) => {
                let _ = release_staging(&inputs, agent_staging.as_ref(), &artifacts);
                return self.fail_before_execution(
                    &cancellation,
                    &post_stop_fence,
                    assignment_id,
                    attempt_id,
                );
            }
        }

        let started_at = RunnerExecutionClock.now();
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

        let observer = RunnerExecutionObserver::new(
            assignment_id.to_owned(),
            attempt_id.to_owned(),
            self.accepted.transition_budget,
            self.outbox.clone(),
            post_stop_fence.clone(),
            cancellation.clone(),
        );
        let diagnostics = StepDiagnosticLog::default();
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
                &self.lease_clock,
                self.authority_updates.clone(),
                Some((initial_authority.sequence, initial_wait)),
                &self.causal_lease,
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
                &self.lease_clock,
                self.authority_updates.clone(),
                Some((initial_authority.sequence, initial_wait)),
                &self.causal_lease,
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
        let support_cleanup_failed = release_support_staging(&inputs, agent_staging.as_ref());

        let (result, final_delivery_budget) = match execution {
            LeaseExecution::Completed {
                output: Ok(result),
                final_delivery_budget,
            } => (result, final_delivery_budget),
            LeaseExecution::Completed {
                output: Err(_),
                final_delivery_budget,
            } => {
                let _ = artifacts.release();
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
                let _ = artifacts.release();
                return ExecutionCompletion::fenced(None, None);
            }
            LeaseExecution::LeaseClockFailed { quiescent } => {
                let _ = artifacts.release();
                let report = quiescent.then(|| {
                    self.abort(
                        assignment_id,
                        attempt_id,
                        observer.last_sequence(),
                        "runner_internal_failure",
                    )
                });
                return ExecutionCompletion::lease_clock_failed(report.flatten());
            }
        };
        if support_cleanup_failed {
            let _ = artifacts.release();
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
            let _ = artifacts.release();
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
        let has_finalizers = !self
            .accepted
            .admitted
            .workflow()
            .definition
            .finalizers
            .is_empty();
        if last_sequence == 0
            || observer.terminal_sequence() != Some(last_sequence)
            || !terminal_result_agrees(observer.terminal_state().as_ref(), &result.outcome)
            || has_finalizers != result.finalization_summary.is_some()
        {
            let _ = artifacts.release();
            return self.abort_unless_fenced(
                &post_stop_fence,
                assignment_id,
                attempt_id,
                last_sequence,
                "engine_result_inconsistent",
                final_delivery_budget,
            );
        }

        let finished_at = RunnerExecutionClock.now();
        let prepared = self
            .runner_result(
                &diagnostics,
                result.clone(),
                &observer,
                started_at,
                finished_at,
            )
            .and_then(|run| {
                prepare_cloud_workflow_result(
                    &run,
                    self.accepted.project_id().to_owned(),
                    self.accepted.repository_connection_id().to_owned(),
                    self.accepted.source_object_format().to_owned(),
                    self.accepted.source_commit_oid().to_owned(),
                )
                .ok()
            });
        let delivery = match prepared {
            Some(prepared) => {
                self.deliver_artifacts(assignment_id, attempt_id, &artifacts, prepared)
                    .await
            }
            None => Ok(internal_delivery_failure("preparation")),
        };
        let delivery = match delivery {
            Ok(delivery) => delivery,
            Err(_) => {
                post_stop_fence.fence();
                self.accepted.process_guards.begin_forced_containment();
                let _ = artifacts.release();
                return ExecutionCompletion::lease_clock_failed(self.abort(
                    assignment_id,
                    attempt_id,
                    last_sequence,
                    "runner_internal_failure",
                ));
            }
        };
        if delivery == ArtifactDeliveryOutcome::AuthorityLost {
            let _ = artifacts.release();
            return ExecutionCompletion::without_report();
        }
        let _ = artifacts.release();
        let artifact_delivery = artifact_delivery_result(&delivery);

        let finalization = result
            .finalization_summary
            .as_ref()
            .map(finalization_summary);
        let report = match result.outcome {
            RunOutcome::Succeeded => ExecutionReport::Finished {
                final_execution_event_sequence: last_sequence,
                outcome: terminal_outcome("succeeded", None, None, finalization),
                artifact_delivery,
            },
            RunOutcome::Failed {
                primary_failure, ..
            } => ExecutionReport::Finished {
                final_execution_event_sequence: last_sequence,
                outcome: terminal_outcome(
                    "failed",
                    Some(workflow_failure(&primary_failure)),
                    None,
                    finalization,
                ),
                artifact_delivery,
            },
            RunOutcome::Cancelled {
                reason: CancellationReason::ExecutionLeaseExpired,
            } => ExecutionReport::Interrupted {
                final_execution_event_sequence: last_sequence,
                reason: "execution_lease_expired".to_owned(),
                terminal_outcome: terminal_outcome(
                    "cancelled",
                    None,
                    Some("execution_lease_expired"),
                    finalization,
                ),
                artifact_delivery,
            },
            RunOutcome::Cancelled {
                reason: CancellationReason::RunnerShutdown,
            } => ExecutionReport::Interrupted {
                final_execution_event_sequence: last_sequence,
                reason: "graceful_shutdown".to_owned(),
                terminal_outcome: terminal_outcome(
                    "cancelled",
                    None,
                    Some("runner_shutdown"),
                    finalization,
                ),
                artifact_delivery,
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

    fn runner_result(
        &self,
        diagnostics: &StepDiagnosticLog,
        execution: crate::execution::workflow::execution::WorkflowExecutionResult<
            RunnerExecutionInstant,
        >,
        observer: &RunnerExecutionObserver,
        started_at: RunnerExecutionInstant,
        finished_at: RunnerExecutionInstant,
    ) -> Option<WorkflowRunResult> {
        let workflow = self.accepted.admitted.workflow();
        let cancellation =
            observed_workflow_cancellation(&execution.outcome, observer.cancellation())?;
        let mut states = execution.steps;
        let mut steps = Vec::with_capacity(states.len());
        for id in &workflow.definition.presentation_order {
            let state = states.remove(id)?;
            let (kind, failure_policy) =
                workflow_step_kind_policy(workflow.definition.steps.get(id)?);
            steps.push(WorkflowRunStep {
                id: id.clone(),
                role: WorkflowNodeRole::Step,
                kind,
                failure_policy,
                state,
                timing: observer.step_timing(id),
                command_output: (kind == WorkflowRunStepKind::Command)
                    .then(|| diagnostics.get(id))
                    .flatten(),
                recovery: None,
                invocations: Vec::new(),
            });
        }
        let finalization = match (
            workflow.definition.finalizers.is_empty(),
            execution.finalization_summary,
        ) {
            (true, None) => None,
            (false, Some(summary)) => {
                let mut summarized = summary
                    .finalizers
                    .into_iter()
                    .map(|result| (result.finalizer.clone(), result))
                    .collect::<BTreeMap<_, _>>();
                let mut finalizers = Vec::with_capacity(summarized.len());
                for id in &workflow.definition.finalizer_presentation_order {
                    let state = states.remove(id)?;
                    let summary = summarized.remove(id)?;
                    let finalizer = workflow.definition.finalizers.get(id)?;
                    let (kind, failure_policy) = workflow_step_kind_policy(&finalizer.body);
                    if summary.failure_policy != failure_policy
                        || !summary_disposition_matches(&summary.disposition, &state)
                    {
                        return None;
                    }
                    finalizers.push(WorkflowRunStep {
                        id: id.clone(),
                        role: WorkflowNodeRole::Finalizer,
                        kind,
                        failure_policy,
                        state,
                        timing: observer.step_timing(id),
                        command_output: (kind == WorkflowRunStepKind::Command)
                            .then(|| diagnostics.get(id))
                            .flatten(),
                        recovery: None,
                        invocations: Vec::new(),
                    });
                }
                if !summarized.is_empty() {
                    return None;
                }
                Some(WorkflowRunFinalization {
                    trigger: summary.trigger,
                    finalizers,
                    cancellation: summary.cancellation.map(|cancellation| {
                        WorkflowRunFinalizationCancellation {
                            reason: cancellation.reason,
                            force_stop_deadline: cancellation.deadline.map(|deadline| deadline.utc),
                        }
                    }),
                    force_abort: summary.force_abort,
                })
            }
            (true, Some(_)) | (false, None) => return None,
        };
        if !states.is_empty() {
            return None;
        }
        Some(WorkflowRunResult {
            run_directory: self.accepted.root.private.clone(),
            attempt_number: 1,
            workflow_path: execution.provenance.workflow_path,
            source_root: execution.provenance.source_root,
            content_digest: execution.content_digest,
            execution_root: self.accepted.admitted.execution().root().to_owned(),
            maximum_parallel_steps: self
                .accepted
                .admitted
                .execution()
                .limits()
                .maximum_parallel_steps(),
            timing: WorkflowRunTiming {
                started_at: started_at.utc,
                finished_at: finished_at.utc,
                duration: finished_at
                    .monotonic
                    .saturating_duration_since(started_at.monotonic),
            },
            outcome: execution.outcome,
            cancellation,
            steps,
            finalization,
            exports: execution.exports,
            export_sources: workflow.definition.exports.clone(),
        })
    }

    async fn deliver_artifacts(
        &self,
        assignment_id: &str,
        attempt_id: &str,
        artifacts: &ArtifactStaging,
        prepared: crate::execution::workflow::publication::PreparedCloudWorkflowResult,
    ) -> Result<ArtifactDeliveryOutcome, LeaseClockError> {
        for carrier in prepared.carriers {
            let delivery = ArtifactDeliverySpec::cloud_carrier(
                assignment_id.to_owned(),
                attempt_id.to_owned(),
                artifacts,
                carrier,
            );
            let outcome = self.await_delivery(assignment_id, delivery).await?;
            if !matches!(outcome, ArtifactDeliveryOutcome::Delivered { .. }) {
                return Ok(outcome);
            }
        }
        self.await_delivery(
            assignment_id,
            ArtifactDeliverySpec::result(
                assignment_id.to_owned(),
                attempt_id.to_owned(),
                prepared.result_json,
            ),
        )
        .await
    }

    async fn await_delivery(
        &self,
        assignment_id: &str,
        delivery: ArtifactDeliverySpec,
    ) -> Result<ArtifactDeliveryOutcome, LeaseClockError> {
        let Ok(mut completion) = self.artifact_delivery.start(delivery) else {
            return Ok(internal_delivery_failure("registration"));
        };
        let mut authority_updates = self.authority_updates.clone();
        loop {
            let authority = authority_updates.borrow_and_update().clone();
            let now = self.lease_clock.now()?;
            if authority.revoked
                || !matches!(
                    now.checked_cmp(authority.local_expiry)?,
                    std::cmp::Ordering::Less
                )
            {
                self.artifact_delivery.cancel_assignment(assignment_id);
                return Ok(ArtifactDeliveryOutcome::AuthorityLost);
            }
            if !matches!(
                now.checked_cmp(authority.renewal_request)?,
                std::cmp::Ordering::Less
            ) {
                match self.causal_lease.request_renewal(
                    authority.sequence,
                    assignment_id,
                    self.accepted.attempt_id(),
                    &self.lease_clock,
                    &self.outbox,
                ) {
                    Ok(()) => {}
                    Err(RenewalRequestFailure::LeaseClock) => {
                        return Err(LeaseClockError::ClockUnavailable);
                    }
                    Err(RenewalRequestFailure::Outbox | RenewalRequestFailure::Sequence) => {
                        return Ok(internal_delivery_failure("preparation"));
                    }
                }
                tokio::select! {
                    result = &mut completion => return Ok(result
                        .unwrap_or_else(|_| internal_delivery_failure("preparation"))),
                    changed = authority_updates.changed() => {
                        if changed.is_err() {
                            self.artifact_delivery.cancel_assignment(assignment_id);
                            return Ok(ArtifactDeliveryOutcome::AuthorityLost);
                        }
                    }
                    result = wait_for_lease_deadline(&self.lease_clock, authority.local_expiry) => {
                        result?;
                        self.artifact_delivery.cancel_assignment(assignment_id);
                        return Ok(ArtifactDeliveryOutcome::AuthorityLost);
                    }
                }
                continue;
            }
            tokio::select! {
                result = &mut completion => return Ok(result
                    .unwrap_or_else(|_| internal_delivery_failure("preparation"))),
                changed = authority_updates.changed() => {
                    if changed.is_err() {
                        self.artifact_delivery.cancel_assignment(assignment_id);
                        return Ok(ArtifactDeliveryOutcome::AuthorityLost);
                    }
                }
                result = wait_for_lease_deadline(&self.lease_clock, authority.renewal_request) => {
                    result?;
                }
            }
        }
    }

    fn fail_before_execution(
        &self,
        cancellation: &crate::execution::workflow::admission::CancellationSource,
        post_stop_fence: &PostStopFence,
        assignment_id: &str,
        attempt_id: &str,
    ) -> ExecutionCompletion {
        begin_forced_containment(cancellation, post_stop_fence, &self.accepted.process_guards);
        ExecutionCompletion::lease_clock_failed(self.abort(
            assignment_id,
            attempt_id,
            0,
            "runner_internal_failure",
        ))
    }

    fn has_execution_authority(&self) -> Result<bool, LeaseClockError> {
        let authority = self.authority_updates.borrow();
        Ok(!authority.revoked
            && matches!(
                self.lease_clock
                    .now()?
                    .checked_cmp(authority.cancellation_start)?,
                std::cmp::Ordering::Less
            ))
    }

    fn terminal_report_deadline(&self) -> Result<LeaseInstant, LeaseClockError> {
        let selected_at = self.lease_clock.now()?;
        let authority = self.authority_updates.borrow();
        let budget_end = selected_at.checked_add(authority.terminal_report_delivery_budget)?;
        match budget_end.checked_cmp(authority.local_expiry)? {
            std::cmp::Ordering::Greater => Ok(authority.local_expiry),
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal => Ok(budget_end),
        }
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

fn observed_workflow_cancellation(
    outcome: &RunOutcome<StepFailureCause>,
    cancellation: Option<(CancellationReason, RunnerExecutionInstant)>,
) -> Option<Option<WorkflowRunCancellation>> {
    match (cancellation, outcome) {
        (
            None,
            RunOutcome::Succeeded
            | RunOutcome::Failed {
                later_cancellation: None,
                ..
            },
        ) => Some(None),
        (
            Some((reason, deadline)),
            RunOutcome::Failed {
                later_cancellation: Some(later),
                ..
            },
        ) if reason == *later => Some(Some(WorkflowRunCancellation {
            reason,
            force_stop_deadline: deadline.utc,
        })),
        (
            Some((reason, deadline)),
            RunOutcome::Cancelled {
                reason: outcome_reason,
            },
        ) if reason == *outcome_reason => Some(Some(WorkflowRunCancellation {
            reason,
            force_stop_deadline: deadline.utc,
        })),
        _ => None,
    }
}

fn internal_delivery_failure(phase: &str) -> ArtifactDeliveryOutcome {
    ArtifactDeliveryOutcome::Failed(ClosedArtifactDeliveryFailure {
        phase: phase.to_owned(),
        code: "delivery_internal_failure".to_owned(),
    })
}

fn workflow_step_kind_policy(
    step: &crate::execution::workflow::validated::ValidatedStep,
) -> (
    WorkflowRunStepKind,
    crate::execution::workflow::document::FailurePolicy,
) {
    match step {
        crate::execution::workflow::validated::ValidatedStep::Command(command) => {
            (WorkflowRunStepKind::Command, command.common.failure_policy)
        }
        crate::execution::workflow::validated::ValidatedStep::Agent(agent) => {
            (WorkflowRunStepKind::Agent, agent.common.failure_policy)
        }
    }
}

fn artifact_delivery_result(delivery: &ArtifactDeliveryOutcome) -> Value {
    match delivery {
        ArtifactDeliveryOutcome::Prepared { artifact_set_id } => json!({
            "outcome": "prepared",
            "artifactSetId": artifact_set_id,
        }),
        ArtifactDeliveryOutcome::Failed(failure) => json!({
            "outcome": "failed",
            "phase": failure.phase,
            "code": failure.code,
        }),
        ArtifactDeliveryOutcome::Delivered { .. } | ArtifactDeliveryOutcome::AuthorityLost => {
            unreachable!("only a closed whole-set delivery can be terminal")
        }
    }
}

#[derive(Clone, Copy)]
struct LeaseFailureContext<'a> {
    cancellation: &'a crate::execution::workflow::admission::CancellationSource,
    post_stop_fence: &'a PostStopFence,
    process_guards: &'a AssignmentProcessGuards,
}

enum LeaseExecution<Output> {
    Completed {
        output: Output,
        final_delivery_budget: Option<Duration>,
    },
    ContainmentDeadline,
    LeaseClockFailed {
        quiescent: bool,
    },
}

#[expect(
    clippy::too_many_arguments,
    reason = "lease supervision receives every authority and containment boundary explicitly"
)]
async fn run_under_lease<F, Output>(
    execution: F,
    cancellation: &crate::execution::workflow::admission::CancellationSource,
    lease_clock: &LeaseClock,
    mut authority_updates: tokio::sync::watch::Receiver<LeaseAuthority>,
    mut initial_wait: Option<(u64, LeaseWait)>,
    causal_lease: &CausalLease,
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
        let failure = LeaseFailureContext {
            cancellation,
            post_stop_fence,
            process_guards,
        };
        let now = match lease_clock.now() {
            Ok(now) => now,
            Err(_) => {
                return fail_lease_clock(cancellation, post_stop_fence, process_guards);
            }
        };
        let cancellation_due = match now.checked_cmp(authority.cancellation_start) {
            Ok(ordering) => ordering != std::cmp::Ordering::Less,
            Err(_) => return fail_lease_clock(cancellation, post_stop_fence, process_guards),
        };
        if authority.revoked || cancellation_due {
            return finish_after_lease_loss(
                &mut execution,
                cancellation,
                lease_clock,
                &authority,
                post_stop_fence,
                process_guards,
            )
            .await;
        }
        let armed_wait = match initial_wait.take() {
            Some((sequence, wait)) if sequence == authority.sequence => Some(wait),
            Some(_) | None => None,
        };
        tokio::select! {
            biased;
            wait = wait_for_lease_deadline_or_armed(
                lease_clock,
                authority.renewal_request,
                armed_wait,
            ) => {
                if wait.is_err() {
                    return fail_lease_timer(&mut execution, failure).await;
                }
                let now = match lease_clock.now() {
                    Ok(now) => now,
                    Err(_) => return fail_lease_clock(cancellation, post_stop_fence, process_guards),
                };
                if !matches!(
                    now.checked_cmp(authority.cancellation_start),
                    Ok(std::cmp::Ordering::Less)
                ) {
                    return finish_after_lease_loss(
                        &mut execution,
                        cancellation,
                        lease_clock,
                        &authority,
                        post_stop_fence,
                        process_guards,
                    ).await;
                }
                match causal_lease.request_renewal(
                    authority.sequence,
                    assignment_id,
                    attempt_id,
                    lease_clock,
                    outbox,
                ) {
                    Ok(()) => {}
                    Err(RenewalRequestFailure::LeaseClock) => {
                        return fail_lease_clock(cancellation, post_stop_fence, process_guards);
                    }
                    Err(RenewalRequestFailure::Outbox | RenewalRequestFailure::Sequence) => {
                        return finish_after_lease_loss(
                            &mut execution,
                            cancellation,
                            lease_clock,
                            &authority,
                            post_stop_fence,
                            process_guards,
                        ).await;
                    }
                }
                tokio::select! {
                    biased;
                    wait = wait_for_lease_deadline(lease_clock, authority.cancellation_start) => {
                        if wait.is_err() {
                            return fail_lease_timer(&mut execution, failure).await;
                        }
                        return finish_after_lease_loss(
                            &mut execution,
                            cancellation,
                            lease_clock,
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
                                lease_clock,
                                &authority,
                                post_stop_fence,
                                process_guards,
                            ).await;
                        }
                    }
                    result = &mut execution => {
                        return complete_ready_execution(
                            result,
                            cancellation,
                            lease_clock,
                            &authority,
                            post_stop_fence,
                            process_guards,
                            false,
                        );
                    }
                }
            }
            changed = authority_updates.changed() => {
                if changed.is_err() {
                    return finish_after_lease_loss(
                        &mut execution,
                        cancellation,
                        lease_clock,
                        &authority,
                        post_stop_fence,
                        process_guards,
                    ).await;
                }
            }
            result = &mut execution => {
                return complete_ready_execution(
                    result,
                    cancellation,
                    lease_clock,
                    &authority,
                    post_stop_fence,
                    process_guards,
                    false,
                );
            }
        }
    }
}

fn complete_ready_execution<Output>(
    output: Output,
    cancellation: &crate::execution::workflow::admission::CancellationSource,
    lease_clock: &LeaseClock,
    authority: &LeaseAuthority,
    post_stop_fence: &PostStopFence,
    process_guards: &AssignmentProcessGuards,
    lease_already_lost: bool,
) -> LeaseExecution<Output> {
    let now = match lease_clock.now() {
        Ok(now) => now,
        Err(_) => return fail_lease_clock(cancellation, post_stop_fence, process_guards),
    };
    if !lease_already_lost {
        match now.checked_cmp(authority.cancellation_start) {
            Ok(std::cmp::Ordering::Less) if !authority.revoked => {
                return LeaseExecution::Completed {
                    output,
                    final_delivery_budget: None,
                };
            }
            Ok(_) => {}
            Err(_) => return fail_lease_clock(cancellation, post_stop_fence, process_guards),
        }
    }
    cancellation.request_cancellation(CancellationReason::ExecutionLeaseExpired);
    match now.checked_cmp(authority.force_stop_start) {
        Ok(std::cmp::Ordering::Less) => {
            return LeaseExecution::Completed {
                output,
                final_delivery_budget: Some(authority.terminal_report_delivery_budget),
            };
        }
        Ok(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater) => {
            begin_forced_containment(cancellation, post_stop_fence, process_guards);
        }
        Err(_) => return fail_lease_clock(cancellation, post_stop_fence, process_guards),
    }
    match lease_clock
        .now()
        .and_then(|now| now.checked_cmp(authority.force_stop_end))
    {
        Ok(std::cmp::Ordering::Greater) => LeaseExecution::ContainmentDeadline,
        Ok(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            if process_guards.is_quiescent() =>
        {
            LeaseExecution::Completed {
                output,
                final_delivery_budget: Some(authority.terminal_report_delivery_budget),
            }
        }
        Ok(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => {
            LeaseExecution::ContainmentDeadline
        }
        Err(_) => LeaseExecution::LeaseClockFailed {
            quiescent: process_guards.is_quiescent(),
        },
    }
}

async fn finish_after_lease_loss<F, Output>(
    execution: &mut std::pin::Pin<&mut F>,
    cancellation: &crate::execution::workflow::admission::CancellationSource,
    lease_clock: &LeaseClock,
    authority: &LeaseAuthority,
    post_stop_fence: &PostStopFence,
    process_guards: &AssignmentProcessGuards,
) -> LeaseExecution<Output>
where
    F: Future<Output = Output>,
{
    cancellation.request_cancellation(CancellationReason::ExecutionLeaseExpired);
    let failure = LeaseFailureContext {
        cancellation,
        post_stop_fence,
        process_guards,
    };
    let now = match lease_clock.now() {
        Ok(now) => now,
        Err(_) => return fail_lease_clock(cancellation, post_stop_fence, process_guards),
    };
    let before_force_stop = match now.checked_cmp(authority.force_stop_start) {
        Ok(std::cmp::Ordering::Less) => true,
        Ok(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater) => false,
        Err(_) => return fail_lease_clock(cancellation, post_stop_fence, process_guards),
    };
    if before_force_stop {
        tokio::select! {
            biased;
            wait = wait_for_lease_deadline(lease_clock, authority.force_stop_start) => {
                if wait.is_err() {
                    return fail_lease_timer(execution, failure).await;
                }
            }
            output = execution.as_mut() => {
                return complete_ready_execution(
                    output,
                    cancellation,
                    lease_clock,
                    authority,
                    post_stop_fence,
                    process_guards,
                    true,
                );
            }
        }
    }

    begin_forced_containment(cancellation, post_stop_fence, process_guards);
    let now = match lease_clock.now() {
        Ok(now) => now,
        Err(_) => {
            return LeaseExecution::LeaseClockFailed {
                quiescent: process_guards.is_quiescent(),
            };
        }
    };
    match now.checked_cmp(authority.force_stop_end) {
        Ok(std::cmp::Ordering::Greater) => return LeaseExecution::ContainmentDeadline,
        Ok(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => {}
        Err(_) => {
            return LeaseExecution::LeaseClockFailed {
                quiescent: process_guards.is_quiescent(),
            };
        }
    }
    tokio::select! {
        biased;
        output = execution.as_mut() => complete_ready_execution(
            output,
            cancellation,
            lease_clock,
            authority,
            post_stop_fence,
            process_guards,
            true,
        ),
        wait = wait_for_lease_deadline(lease_clock, authority.force_stop_end) => {
            match wait {
                Ok(()) => LeaseExecution::ContainmentDeadline,
                Err(_) => fail_lease_timer(execution, failure).await,
            }
        }
    }
}

async fn fail_lease_timer<F, Output>(
    execution: &mut std::pin::Pin<&mut F>,
    failure: LeaseFailureContext<'_>,
) -> LeaseExecution<Output>
where
    F: Future<Output = Output>,
{
    let LeaseFailureContext {
        cancellation,
        post_stop_fence,
        process_guards,
    } = failure;
    begin_forced_containment(cancellation, post_stop_fence, process_guards);
    if !process_guards.is_quiescent() {
        let _ = execution.as_mut().await;
    }
    LeaseExecution::LeaseClockFailed {
        quiescent: process_guards.is_quiescent(),
    }
}

fn fail_lease_clock<Output>(
    cancellation: &crate::execution::workflow::admission::CancellationSource,
    post_stop_fence: &PostStopFence,
    process_guards: &AssignmentProcessGuards,
) -> LeaseExecution<Output> {
    begin_forced_containment(cancellation, post_stop_fence, process_guards);
    LeaseExecution::LeaseClockFailed {
        quiescent: process_guards.is_quiescent(),
    }
}

fn begin_forced_containment(
    cancellation: &crate::execution::workflow::admission::CancellationSource,
    post_stop_fence: &PostStopFence,
    process_guards: &AssignmentProcessGuards,
) {
    cancellation.request_cancellation(CancellationReason::ExecutionLeaseExpired);
    post_stop_fence.fence();
    process_guards.begin_forced_containment();
}

async fn wait_for_lease_deadline(
    lease_clock: &LeaseClock,
    deadline: LeaseInstant,
) -> Result<(), LeaseClockError> {
    wait_for_lease_deadline_or_armed(lease_clock, deadline, None).await
}

async fn wait_for_lease_deadline_or_armed(
    lease_clock: &LeaseClock,
    deadline: LeaseInstant,
    armed: Option<LeaseWait>,
) -> Result<(), LeaseClockError> {
    let wait = match armed {
        Some(wait) => wait,
        None => lease_clock.start_wait(deadline)?,
    };
    let cancellation = LeaseWaitCancellation::default();
    wait.wait(&cancellation).await.map(|_| ())
}

fn release_staging(
    inputs: &InputStaging,
    agent: Option<&AgentInputStaging>,
    artifacts: &ArtifactStaging,
) -> bool {
    release_support_staging(inputs, agent) | artifacts.release().is_err()
}

fn release_support_staging(inputs: &InputStaging, agent: Option<&AgentInputStaging>) -> bool {
    agent.is_some_and(|staging| staging.release().is_err()) | inputs.release().is_err()
}

#[derive(Clone)]
struct RunnerExecutionObserver {
    assignment_id: String,
    attempt_id: String,
    transition_budget: usize,
    outbox: ObservationOutbox,
    post_stop_fence: PostStopFence,
    cancellation: crate::execution::workflow::admission::CancellationSource,
    state: Arc<Mutex<ObserverState>>,
}

struct ObserverState {
    transition_count: usize,
    last_sequence: u64,
    terminal_sequence: Option<u64>,
    terminal_state: Option<WorkflowState<StepFailureCause>>,
    cancellation: Option<(CancellationReason, RunnerExecutionInstant)>,
    step_timings: BTreeMap<String, RunnerStepTiming>,
    faulted: bool,
}

#[derive(Clone, Copy)]
struct RunnerStepTiming {
    started_at: RunnerExecutionInstant,
    finished_at: Option<RunnerExecutionInstant>,
}

impl RunnerExecutionObserver {
    fn new(
        assignment_id: String,
        attempt_id: String,
        transition_budget: usize,
        outbox: ObservationOutbox,
        post_stop_fence: PostStopFence,
        cancellation: crate::execution::workflow::admission::CancellationSource,
    ) -> Self {
        Self {
            assignment_id,
            attempt_id,
            transition_budget,
            outbox,
            post_stop_fence,
            cancellation,
            state: Arc::new(Mutex::new(ObserverState {
                transition_count: 0,
                last_sequence: 0,
                terminal_sequence: None,
                terminal_state: None,
                cancellation: None,
                step_timings: BTreeMap::new(),
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

    fn cancellation(&self) -> Option<(CancellationReason, RunnerExecutionInstant)> {
        self.lock().cancellation
    }

    fn step_timing(&self, step: &str) -> Option<WorkflowStepTiming> {
        let timing = *self.lock().step_timings.get(step)?;
        let finished_at = timing.finished_at?;
        Some(WorkflowStepTiming {
            started_at: timing.started_at.utc,
            duration: finished_at
                .monotonic
                .saturating_duration_since(timing.started_at.monotonic),
        })
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
            let observed_at = RunnerExecutionClock.now();
            let phase_cancellation = match &transition.event {
                TransitionEvent::Workflow {
                    to: WorkflowState::Finalizing { .. },
                    ..
                } => observer
                    .cancellation
                    .cancellation_reason()
                    .filter(|reason| {
                        matches!(
                            reason,
                            CancellationReason::RunnerShutdown
                                | CancellationReason::ExecutionLeaseExpired
                        )
                    }),
                _ => None,
            };
            let fence = observer.post_stop_fence.lock();
            if *fence && !is_lease_loss_terminal_transition(&transition) {
                drop(fence);
                if let Some(reason) = phase_cancellation {
                    observer.cancellation.request_cancellation(reason);
                }
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
            let cancellation = match &transition.event {
                TransitionEvent::CancellationAccepted {
                    reason, deadline, ..
                } => Some((*reason, *deadline)),
                _ => None,
            };
            let terminal = match &transition.event {
                TransitionEvent::Workflow { to, .. }
                    if matches!(
                        to,
                        WorkflowState::Succeeded
                            | WorkflowState::Failed { .. }
                            | WorkflowState::Cancelled { .. }
                    ) =>
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
            match &transition.event {
                TransitionEvent::Step {
                    step,
                    to: StepStateKind::Starting,
                    ..
                } => {
                    state
                        .step_timings
                        .entry(step.clone())
                        .or_insert(RunnerStepTiming {
                            started_at: observed_at,
                            finished_at: None,
                        });
                }
                TransitionEvent::Step {
                    step,
                    to:
                        StepStateKind::Succeeded
                        | StepStateKind::Failed
                        | StepStateKind::Blocked
                        | StepStateKind::NotRun
                        | StepStateKind::Cancelled,
                    ..
                } => {
                    if let Some(timing) = state.step_timings.get_mut(step) {
                        timing.finished_at.get_or_insert(observed_at);
                    }
                }
                TransitionEvent::Step { .. }
                | TransitionEvent::Workflow { .. }
                | TransitionEvent::CancellationAccepted { .. }
                | TransitionEvent::FinalizationCancellationAccepted { .. }
                | TransitionEvent::ForceAbortAccepted { .. } => {}
            }
            state.transition_count += 1;
            state.last_sequence = sequence;
            if let Some(terminal) = terminal {
                state.terminal_sequence = Some(sequence);
                state.terminal_state = Some(terminal.map_deadline(|_| ()));
            }
            if let Some(cancellation) = cancellation
                && state.cancellation.replace(cancellation).is_some()
            {
                state.faulted = true;
            }
            drop(state);
            if let Some(reason) = phase_cancellation {
                observer.cancellation.request_cancellation(reason);
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

fn terminal_outcome(
    outcome: &str,
    failure: Option<Value>,
    reason: Option<&str>,
    finalization: Option<Value>,
) -> Value {
    let mut object = serde_json::Map::from_iter([("outcome".to_owned(), json!(outcome))]);
    if let Some(failure) = failure {
        object.insert("failure".to_owned(), failure);
    }
    if let Some(reason) = reason {
        object.insert("reason".to_owned(), json!(reason));
    }
    if let Some(finalization) = finalization {
        object.insert("finalization".to_owned(), finalization);
    }
    Value::Object(object)
}

fn finalization_summary(
    summary: &FinalizationSummary<StepFailureCause, RunnerExecutionInstant>,
) -> Value {
    let finalizers = summary
        .finalizers
        .iter()
        .map(finalizer_result)
        .collect::<Vec<_>>();
    let issues = summary
        .finalizers
        .iter()
        .filter(|result| {
            matches!(
                result.disposition,
                StepState::Failed { .. } | StepState::InputUnavailable { .. }
            )
        })
        .map(|result| {
            json!({
                "node": { "id": result.finalizer, "role": "finalizer" },
                "impact": result.failure_policy,
            })
        })
        .collect::<Vec<_>>();
    let mut object = serde_json::Map::from_iter([
        ("trigger".to_owned(), json!(summary.trigger.as_str())),
        ("finalizers".to_owned(), Value::Array(finalizers)),
        ("issues".to_owned(), Value::Array(issues)),
        ("forceAbort".to_owned(), json!(summary.force_abort)),
    ]);
    if let Some(cancellation) = &summary.cancellation {
        let mut value = serde_json::Map::from_iter([(
            "reason".to_owned(),
            json!(cancellation_reason(cancellation.reason)),
        )]);
        if let Some(deadline) = cancellation.deadline {
            value.insert(
                "forceStopDeadline".to_owned(),
                json!(format_utc(deadline.utc)),
            );
        }
        object.insert("cancellation".to_owned(), Value::Object(value));
    }
    Value::Object(object)
}

fn finalizer_result(result: &FinalizerResult<StepFailureCause>) -> Value {
    let mut object = serde_json::Map::from_iter([
        ("id".to_owned(), json!(result.finalizer)),
        ("role".to_owned(), json!("finalizer")),
        ("failurePolicy".to_owned(), json!(result.failure_policy)),
    ]);
    match &result.disposition {
        StepState::Succeeded { .. } => {
            object.insert("state".to_owned(), json!("succeeded"));
        }
        StepState::Failed { phase, cause } => {
            object.insert("state".to_owned(), json!("failed"));
            object.insert("failure".to_owned(), failure_evidence(*phase, cause));
        }
        StepState::Blocked { dependency } => {
            object.insert("state".to_owned(), json!("blocked"));
            object.insert("dependency".to_owned(), json!(dependency));
        }
        StepState::InputUnavailable { references } => {
            object.insert("state".to_owned(), json!("blocked"));
            object.insert("reason".to_owned(), json!("input_unavailable"));
            object.insert("unavailableReferences".to_owned(), json!(references));
        }
        StepState::NotRun { reason } => {
            object.insert("state".to_owned(), json!("not_run"));
            object.insert("reason".to_owned(), json!(not_run_reason(*reason)));
        }
        StepState::Cancelled { reason } => {
            object.insert("state".to_owned(), json!("cancelled"));
            object.insert("reason".to_owned(), json!(cancellation_reason(*reason)));
        }
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Cancelling { .. } => {
            object.insert("state".to_owned(), json!("incomplete"));
        }
    }
    Value::Object(object)
}

fn workflow_event(transition: &TransitionObservation<RunnerExecutionInstant>) -> Value {
    match &transition.event {
        TransitionEvent::Step {
            sequence,
            step,
            role,
            failure_policy,
            from,
            to,
        } => {
            let mut event = serde_json::Map::from_iter([
                ("eventVersion".to_owned(), json!(1)),
                ("eventType".to_owned(), json!("step_state_changed")),
                ("transitionSequence".to_owned(), json!(sequence.get())),
                ("stepId".to_owned(), json!(step)),
                ("role".to_owned(), json!(node_role(*role))),
                ("failurePolicy".to_owned(), json!(failure_policy)),
                ("from".to_owned(), json!(step_state_name(*from))),
                ("to".to_owned(), json!(step_state_name(*to))),
            ]);
            if let Some(observed) = &transition.step {
                match observed {
                    ObservedStepTransition::Recovery { .. }
                    | ObservedStepTransition::OutputsCommitted { .. } => {}
                    ObservedStepTransition::Failed { phase, cause } => {
                        event.insert("failure".to_owned(), failure_evidence(*phase, cause));
                    }
                    ObservedStepTransition::Blocked { dependency } => {
                        event.insert("dependency".to_owned(), json!(dependency));
                    }
                    ObservedStepTransition::InputUnavailable { references } => {
                        event.insert("reason".to_owned(), json!("input_unavailable"));
                        event.insert("unavailableReferences".to_owned(), json!(references));
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
        TransitionEvent::FinalizationCancellationAccepted {
            sequence,
            reason,
            deadline,
        } => json!({
            "eventVersion": 1,
            "eventType": "finalization_cancellation_accepted",
            "transitionSequence": sequence.get(),
            "reason": cancellation_reason(*reason),
            "deadline": format_utc(deadline.utc),
        }),
        TransitionEvent::ForceAbortAccepted { sequence, reason } => json!({
            "eventVersion": 1,
            "eventType": "force_abort_accepted",
            "transitionSequence": sequence.get(),
            "reason": cancellation_reason(*reason),
        }),
    }
}

fn workflow_state(state: &WorkflowState<StepFailureCause, RunnerExecutionInstant>) -> Value {
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
        WorkflowState::Finalizing {
            trigger,
            gate,
            primary_failure,
        } => {
            let mut object = serde_json::Map::from_iter([
                ("state".to_owned(), json!("finalizing")),
                ("trigger".to_owned(), json!(trigger.as_str())),
            ]);
            match gate {
                FinalizationGate::Open => {
                    object.insert("gate".to_owned(), json!("open"));
                }
                FinalizationGate::Cancelling {
                    reason,
                    deadline,
                    force_abort,
                } => {
                    object.insert("gate".to_owned(), json!("cancelling"));
                    object.insert("reason".to_owned(), json!(cancellation_reason(*reason)));
                    object.insert("forceAbort".to_owned(), json!(force_abort));
                    if let Some(deadline) = deadline {
                        object.insert(
                            "forceStopDeadline".to_owned(),
                            json!(format_utc(deadline.utc)),
                        );
                    }
                }
            }
            if let Some(primary_failure) = primary_failure {
                object.insert(
                    "primaryFailure".to_owned(),
                    workflow_failure(primary_failure),
                );
            }
            Value::Object(object)
        }
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
        "node": { "id": failure.step, "role": node_role(failure.role) },
        "failure": failure_evidence(failure.phase, &failure.cause),
    })
}

fn node_role(role: WorkflowNodeRole) -> &'static str {
    match role {
        WorkflowNodeRole::Step => "step",
        WorkflowNodeRole::Finalizer => "finalizer",
    }
}

fn failure_evidence(phase: FailurePhase, cause: &StepFailureCause) -> Value {
    match (phase, cause) {
        (FailurePhase::Start, StepFailureCause::Start(cause)) => {
            let protocol_rejection = match cause {
                StepStartFailure::Agent(failure) => failure.protocol_rejection(),
                _ => None,
            };
            let (code, exit_code) = start_failure(cause);
            failure_value("start", code, exit_code, protocol_rejection)
        }
        (FailurePhase::Execution, StepFailureCause::Execution(cause)) => {
            let (code, exit_code) = execution_failure(cause);
            let protocol_rejection = match cause {
                StepExecutionFailure::Agent(failure) => failure.protocol_rejection(),
                StepExecutionFailure::Command(_) => None,
            };
            failure_value("execution", code, exit_code, protocol_rejection)
        }
        (FailurePhase::OutputCapture, StepFailureCause::OutputCapture(cause)) => {
            failure_value("output_capture", output_capture_failure(cause), None, None)
        }
        _ => failure_value("start", "step_unavailable", None, None),
    }
}

fn failure_value(
    phase: &str,
    cause: &str,
    exit_code: Option<i32>,
    protocol_rejection: Option<&AgentProtocolRejectionDiagnostic>,
) -> Value {
    match (exit_code, protocol_rejection) {
        (Some(exit_code), None) => {
            json!({ "phase": phase, "cause": cause, "exitCode": exit_code })
        }
        (None, Some(protocol_rejection)) => json!({
            "phase": phase,
            "cause": cause,
            "protocolRejection": protocol_rejection,
        }),
        (None, None) => json!({ "phase": phase, "cause": cause }),
        (Some(_), Some(_)) => unreachable!("agent protocol rejections have no command exit code"),
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

fn agent_failure(failure: &AgentFailure) -> &'static str {
    match failure.cause() {
        AgentFailureCause::HarnessStartFailed | AgentFailureCause::HarnessSetupFailed { .. } => {
            "agent_harness_start_failed"
        }
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
        OutputCaptureFailure::Git { failure, .. } => git_capture_failure(failure),
        OutputCaptureFailure::TaskUnavailable => "output_task_unavailable",
    }
}

fn git_capture_failure(
    failure: &crate::execution::workflow::git_capture::GitCaptureFailure,
) -> &'static str {
    use crate::execution::workflow::git_capture::GitCaptureFailure;

    match failure {
        GitCaptureFailure::Cancelled
        | GitCaptureFailure::StagingMismatch
        | GitCaptureFailure::Artifact(_) => "output_staging_unavailable",
        GitCaptureFailure::ExecutionRootRebound => "output_git_execution_root_rebound",
        GitCaptureFailure::HeadUnavailable => "output_git_head_unavailable",
        GitCaptureFailure::BaselineNotAncestor => "output_git_baseline_not_ancestor",
        GitCaptureFailure::CleanlinessUnavailable => "output_git_cleanliness_unavailable",
        GitCaptureFailure::WorkspaceDirty => "output_git_workspace_dirty",
        GitCaptureFailure::TreeUnavailable => "output_git_tree_unavailable",
        GitCaptureFailure::RequiredObjectsUnavailable => "output_git_required_objects_unavailable",
        GitCaptureFailure::SourceAuthorityChanged => "output_git_source_authority_changed",
        GitCaptureFailure::GitStructureLimitExceeded => "output_git_structure_limit_exceeded",
        GitCaptureFailure::CommandTimedOut(_) => "output_git_command_timed_out",
        GitCaptureFailure::BundleGenerationFailed => "output_git_bundle_generation_failed",
        GitCaptureFailure::BundleProfileInvalid => "output_git_bundle_profile_invalid",
        GitCaptureFailure::BundleVerificationFailed => "output_git_bundle_verification_failed",
        GitCaptureFailure::WorkspaceChanged => "output_git_workspace_changed",
        GitCaptureFailure::TemporaryStorageUnavailable => {
            "output_git_temporary_storage_unavailable"
        }
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
        StepStateKind::Recovering => "recovering",
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
        NotRunReason::FinalizerTriggerNotSelected => "finalizer_trigger_not_selected",
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
    use std::sync::Arc;

    use super::*;
    use crate::execution::workflow::agent::{
        AgentFailure, AgentOutcome, AgentValueKind, PositiveDuration,
    };
    use crate::execution::workflow::claude_code_stream_json_v1::ClaudeCodeStreamJsonV1Parser;
    use crate::execution::workflow::pi_json_v1::{PiJsonV1Parser, PiJsonV1ProcessCompletion};
    use crate::execution::workflow::validated::{
        ResolvedOutputSource, WorkflowNode, WorkflowNodeRole, WorkflowValueType,
    };
    use crate::runner::service::lease_clock::{LeaseTimerRelease, controlled_lease_clock};
    use crate::runner::service::test_support::{controlled_sleeper, sleep_request, with_watchdog};
    use crate::runner_protocol::{RunnerEnvelope, encode_runner_frame};

    fn lease_authority(basis: LeaseInstant) -> LeaseAuthority {
        LeaseAuthority {
            sequence: 4,
            basis,
            renewal_request: basis.checked_add(Duration::from_secs(2)).unwrap(),
            cancellation_start: basis.checked_add(Duration::from_secs(4)).unwrap(),
            force_stop_start: basis.checked_add(Duration::from_secs(5)).unwrap(),
            force_stop_end: basis.checked_add(Duration::from_secs(8)).unwrap(),
            local_expiry: basis.checked_add(Duration::from_secs(12)).unwrap(),
            terminal_report_delivery_budget: Duration::from_secs(7),
            revoked: false,
        }
    }

    struct SupervisedLeaseFixture {
        result: tokio::sync::oneshot::Sender<&'static str>,
        task: tokio::task::JoinHandle<LeaseExecution<&'static str>>,
        waits: tokio::sync::mpsc::UnboundedReceiver<(Duration, LeaseTimerRelease)>,
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
        lease_clock: LeaseClock,
        authority: LeaseAuthority,
        execution: Execution,
    ) -> SupervisedExecution<Output>
    where
        Execution: Future<Output = Output> + Send + 'static,
        Output: Send + 'static,
    {
        supervise_execution_with_guards(
            lease_clock,
            authority,
            execution,
            AssignmentProcessGuards::new(),
        )
    }

    fn supervise_execution_with_guards<Execution, Output>(
        lease_clock: LeaseClock,
        authority: LeaseAuthority,
        execution: Execution,
        guards: AssignmentProcessGuards,
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
        let observed_guards = guards.clone();
        let causal_lease = CausalLease::new(authority.basis);
        let (authority_sender, authority_updates) = tokio::sync::watch::channel(authority);
        let task = tokio::spawn(async move {
            run_under_lease(
                execution,
                &cancellation,
                &lease_clock,
                authority_updates,
                None,
                &causal_lease,
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
        let (lease_clock, _control, waits) = controlled_lease_clock();
        let basis = lease_clock.now().unwrap();
        let (result_sender, result) = tokio::sync::oneshot::channel();
        let supervised = supervise_execution(lease_clock, lease_authority(basis), async {
            result.await.expect("fixture result")
        });
        SupervisedLeaseFixture {
            result: result_sender,
            task: supervised.task,
            waits,
            cancellation: supervised.cancellation,
            fence: supervised.fence,
            guards: supervised.guards,
            _authority: supervised.authority,
        }
    }

    async fn lease_wait_request(
        waits: &mut tokio::sync::mpsc::UnboundedReceiver<(Duration, LeaseTimerRelease)>,
        expected: Duration,
    ) -> LeaseTimerRelease {
        loop {
            let (duration, release) = waits
                .recv()
                .await
                .expect("controlled lease clock closed before the expected timer");
            if duration == expected {
                return release;
            }
        }
    }

    fn assert_forced_containment(
        cancellation: &crate::execution::workflow::admission::CancellationSource,
        fence: &PostStopFence,
        guards: &AssignmentProcessGuards,
    ) {
        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::ExecutionLeaseExpired)
        );
        assert!(fence.is_fenced());
        assert!(guards.forced_containment_started());
    }

    fn unavailable_timer_fixture() -> (LeaseClock, LeaseAuthority) {
        let (lease_clock, control, _waits) = controlled_lease_clock();
        let authority = lease_authority(lease_clock.now().unwrap());
        control.make_timer_unavailable();
        (lease_clock, authority)
    }

    async fn lease_clock_failure_outcome<Output: Send + 'static>(
        task: tokio::task::JoinHandle<LeaseExecution<Output>>,
    ) -> LeaseExecution<Output> {
        with_watchdog(task)
            .await
            .expect("timer failure supervision timed out")
            .expect("timer failure supervision task failed")
    }

    async fn assert_lease_clock_failure<Output: Send + 'static>(
        supervised: SupervisedExecution<Output>,
    ) {
        assert!(matches!(
            lease_clock_failure_outcome(supervised.task).await,
            LeaseExecution::LeaseClockFailed { quiescent: true }
        ));
        assert_forced_containment(
            &supervised.cancellation,
            &supervised.fence,
            &supervised.guards,
        );
    }

    #[tokio::test]
    async fn sixty_second_lease_grace_allows_clean_exit_after_thirty_seconds() {
        let (lease_clock, _control, _lease_waits) = controlled_lease_clock();
        let basis = lease_clock.now().unwrap();
        let (execution_sleeper, mut execution_sleeps) = controlled_sleeper();
        let workflow_sleeper = Arc::clone(&execution_sleeper);
        let supervised = supervise_execution(
            lease_clock,
            LeaseAuthority {
                sequence: 1,
                basis,
                renewal_request: basis,
                cancellation_start: basis,
                force_stop_start: basis.checked_add(Duration::from_secs(60)).unwrap(),
                force_stop_end: basis.checked_add(Duration::from_secs(65)).unwrap(),
                local_expiry: basis.checked_add(Duration::from_secs(72)).unwrap(),
                terminal_report_delivery_budget: Duration::from_secs(7),
                revoked: false,
            },
            async move {
                workflow_sleeper.sleep(Duration::from_secs(30)).await;
                "clean-exit"
            },
        );

        assert_eq!(
            with_watchdog(supervised.cancellation.wait_for_cancellation())
                .await
                .expect("lease cancellation was not requested"),
            CancellationReason::ExecutionLeaseExpired
        );
        sleep_request(&mut execution_sleeps, Duration::from_secs(30))
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
    async fn lease_timer_failure_contains_without_accepting_ready_progress() {
        let (lease_clock, authority) = unavailable_timer_fixture();
        let (result_sender, result) = tokio::sync::oneshot::channel();
        let supervised = supervise_execution(lease_clock, authority, async {
            result.await.expect("fixture result")
        });

        assert_lease_clock_failure(supervised).await;
        assert!(result_sender.send("ready-late").is_err());
    }

    #[tokio::test]
    async fn persistent_lease_timer_failure_waits_for_observed_quiescence() {
        let (lease_clock, authority) = unavailable_timer_fixture();
        let quiescent = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guards = AssignmentProcessGuards::new();
        guards.use_quiescence_fixture(Arc::clone(&quiescent));
        let completion_quiescent = Arc::clone(&quiescent);
        let (result_sender, result) = tokio::sync::oneshot::channel();
        let supervised = supervise_execution_with_guards(
            lease_clock,
            authority,
            async move {
                result.await.expect("fixture result");
                completion_quiescent.store(true, std::sync::atomic::Ordering::Release);
            },
            guards,
        );

        assert_eq!(
            with_watchdog(supervised.cancellation.wait_for_cancellation())
                .await
                .expect("timer failure did not begin containment"),
            CancellationReason::ExecutionLeaseExpired
        );
        result_sender
            .send(())
            .expect("timer failure dropped execution before quiescence");
        assert_lease_clock_failure(supervised).await;
    }

    #[tokio::test]
    async fn exact_stop_boundary_fences_before_a_ready_late_result() {
        let mut fixture = supervised_lease_fixture();
        lease_wait_request(&mut fixture.waits, Duration::from_secs(2))
            .await
            .release();
        lease_wait_request(&mut fixture.waits, Duration::from_secs(2))
            .await
            .release();
        assert_eq!(
            with_watchdog(fixture.cancellation.wait_for_cancellation())
                .await
                .expect("lease cancellation was not requested"),
            CancellationReason::ExecutionLeaseExpired
        );
        let stop_boundary = lease_wait_request(&mut fixture.waits, Duration::from_secs(1)).await;
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
    async fn overlong_suspend_contains_before_ready_completion() {
        let (lease_clock, control, mut waits) = controlled_lease_clock();
        let basis = lease_clock.now().unwrap();
        let (result_sender, result) = tokio::sync::oneshot::channel();
        let supervised = supervise_execution(lease_clock, lease_authority(basis), async {
            result.await.expect("fixture result")
        });

        lease_wait_request(&mut waits, Duration::from_secs(2))
            .await
            .release();
        let _delayed_cancellation_wake =
            lease_wait_request(&mut waits, Duration::from_secs(2)).await;
        control.simulate_suspend(Duration::from_secs(4));
        result_sender.send("ready-after-suspend").unwrap();

        let outcome = with_watchdog(supervised.task)
            .await
            .expect("lease supervision timed out")
            .expect("lease supervision task failed");
        assert!(matches!(outcome, LeaseExecution::Completed { .. }));
        assert_eq!(
            supervised.cancellation.cancellation_reason(),
            Some(CancellationReason::ExecutionLeaseExpired),
            "the first runner action after a suspend past force-stop start must cancel"
        );
        assert!(
            supervised.fence.is_fenced(),
            "the first runner action after a suspend past force-stop start must fence output"
        );
        assert!(
            supervised.guards.forced_containment_started(),
            "the first runner action after a suspend past force-stop start must contain processes"
        );
    }

    #[tokio::test]
    async fn force_reap_accepts_exact_boundary_and_rejects_late_completion() {
        let mut exact = supervised_lease_fixture();
        for duration in [
            Duration::from_secs(2),
            Duration::from_secs(2),
            Duration::from_secs(1),
        ] {
            lease_wait_request(&mut exact.waits, duration)
                .await
                .release();
        }
        let reap_boundary = lease_wait_request(&mut exact.waits, Duration::from_secs(3)).await;
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
            lease_wait_request(&mut late.waits, duration)
                .await
                .release();
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
            crate::execution::workflow::admission::CancellationSource::new(),
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

    #[tokio::test]
    async fn post_stop_fence_rearms_lease_loss_at_finalization_boundary() {
        let cancellation = crate::execution::workflow::admission::CancellationSource::new();
        assert!(cancellation.request_cancellation(CancellationReason::ExecutionLeaseExpired));
        assert!(cancellation.fixture_begin_finalization_arm());

        let fence = PostStopFence::new();
        fence.fence();
        let observer = RunnerExecutionObserver::new(
            "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            1,
            ObservationOutbox::new(),
            fence,
            cancellation.clone(),
        );
        observer
            .observe(ExecutionObservation::Transition(TransitionObservation {
                event: TransitionEvent::Workflow {
                    sequence: Default::default(),
                    from: WorkflowState::Executing {
                        gate: SchedulingGate::Cancelling {
                            reason: CancellationReason::ExecutionLeaseExpired,
                            prior_failure: None,
                        },
                    },
                    to: WorkflowState::Finalizing {
                        trigger:
                            crate::execution::workflow::document::FinalizationTrigger::Cancelled,
                        gate: FinalizationGate::Open,
                        primary_failure: None,
                    },
                },
                step: None,
            }))
            .await;
        assert!(cancellation.fixture_complete_finalization_arm());

        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::ExecutionLeaseExpired),
            "lease loss must close the newly committed finalization gate even after the post-stop observation fence",
        );
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
                role: WorkflowNodeRole::Step,
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
                "role": "step",
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
                StepFailureCause::Start(StepStartFailure::Agent(failure.clone().into())),
                "start",
                expected,
                None,
            );
            assert_projection(
                FailurePhase::Execution,
                StepFailureCause::Execution(StepExecutionFailure::Agent(failure.into())),
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
                CaptureFailureKind::GitCarrierCountLimitExceeded,
                "output_git_carrier_count_limit_exceeded",
            ),
            (
                CaptureFailureKind::GitCarrierSizeLimitExceeded,
                "output_git_carrier_size_limit_exceeded",
            ),
            (
                CaptureFailureKind::TotalGitCarrierSizeLimitExceeded,
                "output_total_git_carrier_size_limit_exceeded",
            ),
            (
                CaptureFailureKind::CarrierProducerUnavailable,
                "output_carrier_producer_unavailable",
            ),
            (
                CaptureFailureKind::StagingUnavailable,
                "output_staging_unavailable",
            ),
        ] {
            assert_eq!(capture_failure(kind), expected);
            assert_encoded_evidence("output_capture", expected, None);
        }
        for (failure, expected) in [
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::ExecutionRootRebound,
                "output_git_execution_root_rebound",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::HeadUnavailable,
                "output_git_head_unavailable",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::BaselineNotAncestor,
                "output_git_baseline_not_ancestor",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::CleanlinessUnavailable,
                "output_git_cleanliness_unavailable",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::WorkspaceDirty,
                "output_git_workspace_dirty",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::TreeUnavailable,
                "output_git_tree_unavailable",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::RequiredObjectsUnavailable,
                "output_git_required_objects_unavailable",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::SourceAuthorityChanged,
                "output_git_source_authority_changed",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::GitStructureLimitExceeded,
                "output_git_structure_limit_exceeded",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::BundleGenerationFailed,
                "output_git_bundle_generation_failed",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::BundleProfileInvalid,
                "output_git_bundle_profile_invalid",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::BundleVerificationFailed,
                "output_git_bundle_verification_failed",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::WorkspaceChanged,
                "output_git_workspace_changed",
            ),
            (
                crate::execution::workflow::git_capture::GitCaptureFailure::TemporaryStorageUnavailable,
                "output_git_temporary_storage_unavailable",
            ),
        ] {
            let failure = OutputCaptureFailure::Git {
                output: "branch".to_owned(),
                failure,
            };
            assert_projection(
                FailurePhase::OutputCapture,
                StepFailureCause::OutputCapture(failure),
                "output_capture",
                expected,
                None,
            );
        }
        assert_encoded_evidence("output_capture", "output_git_command_timed_out", None);
    }

    fn assert_agent_protocol_rejection_evidence(failure: AgentFailure) -> Value {
        let expected = serde_json::to_value(failure.protocol_rejection().unwrap()).unwrap();
        let evidence = failure_evidence(
            FailurePhase::Execution,
            &StepFailureCause::Execution(StepExecutionFailure::Agent(failure)),
        );
        assert_eq!(evidence["cause"], "agent_harness_protocol_failed");
        assert_eq!(evidence["protocolRejection"], expected);
        expected
    }

    #[test]
    fn runner_failure_evidence_carries_the_agent_protocol_rejection() {
        let mut parser =
            PiJsonV1Parser::profile(Arc::from("/execution/worktree"), AgentValueKind::None);
        let frames = concat!(
            "{\"type\":\"session\",\"version\":3,",
            "\"id\":\"00000000-0000-4000-8000-00000000000d\",",
            "\"timestamp\":\"2026-07-30T12:00:00Z\",",
            "\"cwd\":\"/execution/worktree\"}\n",
            "{\"type\":\"agent_start\"}\n",
            "{\"type\":\"agent_start\"}\n",
        );
        assert!(parser.push_stdout(frames.as_bytes(), drop).is_err());
        let AgentOutcome::Failed(failure) = parser.finish(PiJsonV1ProcessCompletion::exited(false))
        else {
            panic!("invalid agent lifecycle must fail");
        };
        assert_agent_protocol_rejection_evidence(failure);
    }

    #[test]
    fn runner_failure_evidence_carries_the_claude_protocol_rejection() {
        let mut parser = ClaudeCodeStreamJsonV1Parser::profile(
            Arc::from("/execution/worktree"),
            Arc::from("scherzo-loopback"),
            Arc::from("00000000-0000-4000-8000-000000000001"),
            Arc::from("2.1.234"),
            AgentValueKind::None,
            NonZeroU64::new(1024).unwrap(),
        );
        let initialization = concat!(
            "{\"type\":\"system\",\"subtype\":\"init\",",
            "\"cwd\":\"/execution/worktree\",",
            "\"session_id\":\"00000000-0000-4000-8000-000000000001\",",
            "\"model\":\"scherzo-loopback\",",
            "\"permissionMode\":\"bypassPermissions\",",
            "\"claude_code_version\":\"2.1.234\"}\n",
        );
        parser.push_stdout(initialization.as_bytes(), drop).unwrap();
        let AgentOutcome::Failed(failure) = parser.finish(true) else {
            panic!("incomplete Claude exchange must fail");
        };
        let expected = assert_agent_protocol_rejection_evidence(failure);
        serde_json::from_value::<
            crate::runner_protocol::generated::AgentProtocolRejectionDiagnostic,
        >(expected.clone())
        .unwrap();
        assert_eq!(
            expected["detail"]["reason"],
            "end_of_stream_invariant_invalid"
        );
    }

    #[test]
    fn runner_failure_evidence_accepts_revised_pi_rejection() {
        let events = [
            json!({"type": "session", "version": 3, "id": "00000000-0000-4000-8000-00000000000d", "timestamp": "2026-07-30T12:00:00Z", "cwd": "/execution/worktree"}),
            json!({"type": "agent_start"}),
            json!({"type": "agent_start"}),
        ];
        let mut parser =
            PiJsonV1Parser::profile(Arc::from("/execution/worktree"), AgentValueKind::None);
        for event in events {
            let mut frame = serde_json::to_vec(&event).unwrap();
            frame.push(b'\n');
            if parser.push_stdout(&frame, drop).is_err() {
                break;
            }
        }
        let AgentOutcome::Failed(failure) = parser.finish(PiJsonV1ProcessCompletion::exited(false))
        else {
            panic!("contradictory agent lifecycle must fail");
        };
        let rejection = serde_json::to_value(failure.protocol_rejection().unwrap()).unwrap();
        assert_eq!(rejection["detail"]["reason"], "event_transition_invalid");
        assert_eq!(
            rejection["detail"]["state"],
            json!({
                "sessionHeaderSeen": true,
                "agentStarted": true,
                "terminalCandidateRetained": false,
                "resultAccepted": false,
                "settled": false
            })
        );

        let evidence = failure_evidence(
            FailurePhase::Execution,
            &StepFailureCause::Execution(StepExecutionFailure::Agent(failure.clone())),
        );
        let terminal = workflow_failure(&StepFailure {
            role: WorkflowNodeRole::Step,
            step: "step".to_owned(),
            phase: FailurePhase::Execution,
            cause: StepFailureCause::Execution(StepExecutionFailure::Agent(failure)),
        });

        assert_encoded_evidence_value("execution", evidence, terminal);
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
            failure_value(expected_phase, expected_cause, exit_code, None)
        );
        let failure = StepFailure {
            role: WorkflowNodeRole::Step,
            step: "step".to_owned(),
            phase,
            cause,
        };
        assert_encoded_evidence_value(expected_phase, evidence, workflow_failure(&failure));
    }

    fn assert_encoded_evidence(phase: &str, cause: &str, exit_code: Option<i32>) {
        let evidence = failure_value(phase, cause, exit_code, None);
        let terminal = json!({
            "node": { "id": "step", "role": "step" },
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
                        "role": "step",
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
                    artifact_delivery: json!({
                        "outcome": "prepared",
                        "artifactSetId": "ats_01k0z6r1w8f4jy2m7q9v3x5abc",
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
