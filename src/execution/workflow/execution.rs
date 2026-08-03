use std::collections::BTreeMap;
use std::future::Future;

use super::admission::AdmittedWorkflow;
use super::artifact::ArtifactStaging;
use super::coordinator::{CommitPort, CommittedReduction, CoordinationError, CoordinatorClock};
use super::diagnostic::StepDiagnosticLog;
use super::input::InputStaging;
use super::observation::{
    ExecutionObservation, ExecutionObserver, ObservedStepTransition, TransitionObservation,
};
use super::resolution::{WorkflowContentDigest, WorkflowSourceProvenance};
use super::runtime::{
    ExportSet, RunOutcome, RuntimeState, StepState, StepStateKind, TransitionEvent, WorkflowState,
};
use super::step_runtime::{StepFailureCause, execute_workflow_observed};
use super::value::CapturedValue;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowExecutionResult {
    pub(crate) outcome: RunOutcome<StepFailureCause>,
    pub(crate) steps: BTreeMap<String, StepState<StepFailureCause, CapturedValue>>,
    pub(crate) exports: ExportSet<CapturedValue>,
    pub(crate) provenance: WorkflowSourceProvenance,
    pub(crate) content_digest: WorkflowContentDigest,
}

#[derive(Clone)]
struct ObserverCommitPort<Observer> {
    observer: Observer,
}

impl<Deadline, Observer> CommitPort<CommittedReduction<StepFailureCause, CapturedValue, Deadline>>
    for ObserverCommitPort<Observer>
where
    Deadline: Send + 'static,
    Observer: ExecutionObserver<Deadline>,
{
    fn commit(
        &mut self,
        commit: CommittedReduction<StepFailureCause, CapturedValue, Deadline>,
    ) -> impl Future<Output = ()> {
        let observer = self.observer.clone();
        async move {
            for event in commit.events {
                let step = observed_step_transition(&event, &commit.state);
                observer
                    .observe(ExecutionObservation::Transition(TransitionObservation {
                        event,
                        step,
                    }))
                    .await;
            }
        }
    }
}

fn observed_step_transition<Deadline>(
    event: &TransitionEvent<StepFailureCause, Deadline>,
    state: &RuntimeState<StepFailureCause, CapturedValue>,
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

pub(crate) async fn execute_workflow<Clock, Observer>(
    admitted: AdmittedWorkflow,
    artifacts: &ArtifactStaging,
    inputs: &InputStaging,
    diagnostics: &StepDiagnosticLog,
    clock: Clock,
    observer: Observer,
) -> Result<WorkflowExecutionResult, CoordinationError>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
{
    let provenance = admitted.workflow().source.clone();
    let content_digest = admitted.workflow().content_digest.clone();
    let coordinated = execute_workflow_observed(
        admitted,
        artifacts,
        inputs,
        diagnostics,
        clock,
        ObserverCommitPort {
            observer: observer.clone(),
        },
        observer,
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
        WorkflowState::Executing { .. } => {
            return Err(CoordinationError::ReducerStateUnavailable);
        }
    };
    let steps = coordinated
        .state
        .steps
        .into_iter()
        .map(|(step, runtime)| (step, runtime.state))
        .collect();
    let exports = coordinated
        .state
        .exports
        .ok_or(CoordinationError::ReducerStateUnavailable)?;
    Ok(WorkflowExecutionResult {
        outcome,
        steps,
        exports,
        provenance,
        content_digest,
    })
}

#[cfg(test)]
mod tests;
