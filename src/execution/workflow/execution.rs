use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::{Future, ready};

use super::admission::AdmittedWorkflow;
use super::artifact::ArtifactStaging;
use super::coordinator::{CommitPort, CommittedReduction, CoordinationError, CoordinatorClock};
use super::diagnostic::StepDiagnosticLog;
use super::input::InputStaging;
use super::observation::{
    ExecutionObservation, ExecutionObserver, ObservedStepTransition, TransitionObservation,
};
use super::process_group::ProcessGuardRegistry;
use super::resolution::{WorkflowContentDigest, WorkflowSourceProvenance};
use super::runtime::{
    ExportSet, RunOutcome, RuntimeState, StepState, StepStateKind, TransitionEvent, WorkflowState,
};
use super::step_runtime::{
    AgentExecution, StepFailureCause, WorkflowAgentDispatcher, WorkflowCommitPort,
    execute_workflow_observed,
};
use super::value::CapturedValue;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowExecutionResult<Deadline = ()> {
    pub(crate) outcome: RunOutcome<StepFailureCause>,
    pub(crate) steps: BTreeMap<String, StepState<StepFailureCause, CapturedValue>>,
    pub(crate) finalization_summary:
        Option<super::runtime::FinalizationSummary<StepFailureCause, Deadline>>,
    pub(crate) exports: ExportSet<CapturedValue>,
    pub(crate) provenance: WorkflowSourceProvenance,
    pub(crate) content_digest: WorkflowContentDigest,
}

pub(crate) struct NoopCommitPort;

impl<Commit> CommitPort<Commit> for NoopCommitPort {
    type Error = Infallible;

    fn commit(&mut self, _commit: Commit) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Ok(()))
    }
}

struct DurableObserverCommitPort<Commits, Observer> {
    commits: Commits,
    observer: Observer,
}

impl<Deadline, Commits, Observer>
    CommitPort<CommittedReduction<StepFailureCause, CapturedValue, Deadline>>
    for DurableObserverCommitPort<Commits, Observer>
where
    Deadline: Clone + Send + 'static,
    Commits: CommitPort<CommittedReduction<StepFailureCause, CapturedValue, Deadline>>,
    Observer: ExecutionObserver<Deadline>,
{
    type Error = Commits::Error;

    fn commit(
        &mut self,
        commit: CommittedReduction<StepFailureCause, CapturedValue, Deadline>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        let state = commit.state.clone();
        let events = commit.events.clone();
        let committed = self.commits.commit(commit);
        let observer = self.observer.clone();
        async move {
            committed.await?;
            for event in events {
                let step = observed_step_transition(&event, &state);
                observer
                    .observe(ExecutionObservation::Transition(TransitionObservation {
                        event,
                        step,
                    }))
                    .await;
            }
            Ok(())
        }
    }
}

fn observed_step_transition<Deadline>(
    event: &TransitionEvent<StepFailureCause, Deadline>,
    state: &RuntimeState<StepFailureCause, CapturedValue, Deadline>,
) -> Option<ObservedStepTransition> {
    let TransitionEvent::Step { step, to, .. } = event else {
        return None;
    };
    let runtime = state.steps.get(step)?;
    match (to, &runtime.state) {
        (StepStateKind::Succeeded, StepState::Succeeded { outputs }) => {
            Some(ObservedStepTransition::OutputsCommitted {
                outputs: outputs.keys().cloned().collect(),
            })
        }
        (StepStateKind::Failed, StepState::Failed { phase, cause }) => {
            Some(ObservedStepTransition::Failed {
                phase: *phase,
                cause: cause.clone(),
            })
        }
        (StepStateKind::Blocked, StepState::Blocked { dependency }) => {
            Some(ObservedStepTransition::Blocked {
                dependency: dependency.clone(),
            })
        }
        (StepStateKind::Blocked, StepState::InputUnavailable { references }) => {
            Some(ObservedStepTransition::InputUnavailable {
                references: references.clone(),
            })
        }
        (StepStateKind::NotRun, StepState::NotRun { reason }) => {
            Some(ObservedStepTransition::NotRun { reason: *reason })
        }
        (StepStateKind::Cancelling, StepState::Cancelling { reason }) => {
            Some(ObservedStepTransition::Cancelling { reason: *reason })
        }
        (StepStateKind::Cancelled, StepState::Cancelled { reason }) => {
            Some(ObservedStepTransition::Cancelled { reason: *reason })
        }
        _ => None,
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the local adapter additionally supplies durable process-guard registration"
)]
pub(crate) async fn execute_workflow<Clock, Commits, Observer, Dispatcher>(
    admitted: AdmittedWorkflow,
    artifacts: &ArtifactStaging,
    inputs: &InputStaging,
    diagnostics: &StepDiagnosticLog,
    agents: AgentExecution<Dispatcher>,
    clock: Clock,
    commits: Commits,
    observer: Observer,
    process_guards: ProcessGuardRegistry,
) -> Result<WorkflowExecutionResult<Clock::Instant>, CoordinationError>
// This result projection intentionally repeats the shared runtime's generic port
// constraints so it can preserve its distinct domain result.
// jscpd:ignore-start
where
    Clock: CoordinatorClock,
    Clock::Instant: Sync,
    Commits: WorkflowCommitPort<Clock>,
    Observer: ExecutionObserver<Clock::Instant>,
    Dispatcher: WorkflowAgentDispatcher<Clock::Instant, Observer>,
    // jscpd:ignore-end
{
    if admitted.has_recovery() && admitted.recovery_execution_guard().is_some() {
        return Err(CoordinationError::RecoveryExecutionGuardActive);
    }
    let provenance = admitted.workflow().source.clone();
    let content_digest = admitted.workflow().content_digest.clone();
    let coordinated = execute_workflow_observed(
        admitted,
        artifacts,
        inputs,
        diagnostics,
        clock,
        DurableObserverCommitPort {
            commits,
            observer: observer.clone(),
        },
        observer,
        agents,
        process_guards,
    )
    .await?;
    let outcome = match coordinated.state.workflow {
        WorkflowState::Succeeded => RunOutcome::Succeeded,
        WorkflowState::Failed {
            primary_failure,
            later_cancellation,
        } => RunOutcome::Failed {
            primary_failure,
            later_cancellation,
        },
        WorkflowState::Cancelled { reason } => RunOutcome::Cancelled { reason },
        WorkflowState::Executing { .. } | WorkflowState::Finalizing { .. } => {
            return Err(CoordinationError::ReducerStateUnavailable);
        }
    };
    let steps = coordinated
        .state
        .steps
        .into_iter()
        .map(|(step, runtime)| (step, runtime.state))
        .collect();
    let finalization_summary = coordinated.state.finalization_summary;
    let exports = coordinated
        .state
        .exports
        .ok_or(CoordinationError::ReducerStateUnavailable)?;
    Ok(WorkflowExecutionResult {
        outcome,
        steps,
        finalization_summary,
        exports,
        provenance,
        content_digest,
    })
}

#[cfg(test)]
mod tests;
