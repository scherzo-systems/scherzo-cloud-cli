use std::future::Future;
use std::num::NonZeroUsize;
use std::ops::Add;
use std::time::Duration;

use tokio::sync::mpsc;

use super::admission::AdmittedWorkflow;
use super::runtime::{
    self, ActionId, CancellationRequest, Occurrence, OutputSet, Reduction, RequestedAction,
    RuntimeState, TransitionEvent, WorkflowState,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct OccurrenceOrdinal(u64);

impl OccurrenceOrdinal {
    pub(crate) const fn get(self) -> u64 {
        self.0
    }

    fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DriverDeadline {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DriverOccurrence<Provisional, Cause, Output>(
    Occurrence<Provisional, Cause, Output, DriverDeadline>,
);

impl<Provisional, Cause, Output> DriverOccurrence<Provisional, Cause, Output> {
    pub(crate) fn step_started(step: String, action: ActionId) -> Self {
        Self(Occurrence::StepStarted { step, action })
    }

    pub(crate) fn step_start_failed(step: String, action: ActionId, cause: Cause) -> Self {
        Self(Occurrence::StepStartFailed {
            step,
            action,
            cause,
        })
    }

    pub(crate) fn step_execution_completed(
        step: String,
        action: ActionId,
        provisional: Provisional,
    ) -> Self {
        Self(Occurrence::StepExecutionCompleted {
            step,
            action,
            provisional,
        })
    }

    pub(crate) fn step_execution_failed(step: String, action: ActionId, cause: Cause) -> Self {
        Self(Occurrence::StepExecutionFailed {
            step,
            action,
            cause,
        })
    }

    pub(crate) fn outputs_captured(
        step: String,
        action: ActionId,
        outputs: OutputSet<Output>,
    ) -> Self {
        Self(Occurrence::OutputsCaptured {
            step,
            action,
            outputs,
        })
    }

    pub(crate) fn output_capture_failed(step: String, action: ActionId, cause: Cause) -> Self {
        Self(Occurrence::OutputCaptureFailed {
            step,
            action,
            cause,
        })
    }

    pub(crate) fn step_quiesced(step: String, action: ActionId) -> Self {
        Self(Occurrence::StepQuiesced { step, action })
    }

    pub(crate) fn into_runtime<Deadline>(self) -> Occurrence<Provisional, Cause, Output, Deadline> {
        match self.0 {
            Occurrence::StepStarted { step, action } => Occurrence::StepStarted { step, action },
            Occurrence::StepStartFailed {
                step,
                action,
                cause,
            } => Occurrence::StepStartFailed {
                step,
                action,
                cause,
            },
            Occurrence::StepExecutionCompleted {
                step,
                action,
                provisional,
            } => Occurrence::StepExecutionCompleted {
                step,
                action,
                provisional,
            },
            Occurrence::StepExecutionFailed {
                step,
                action,
                cause,
            } => Occurrence::StepExecutionFailed {
                step,
                action,
                cause,
            },
            Occurrence::OutputsCaptured {
                step,
                action,
                outputs,
            } => Occurrence::OutputsCaptured {
                step,
                action,
                outputs,
            },
            Occurrence::OutputCaptureFailed {
                step,
                action,
                cause,
            } => Occurrence::OutputCaptureFailed {
                step,
                action,
                cause,
            },
            Occurrence::StepQuiesced { step, action } => Occurrence::StepQuiesced { step, action },
            Occurrence::CancellationRequested { deadline, .. } => match deadline {},
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OccurrenceSender<Provisional, Cause, Output> {
    sender: mpsc::Sender<DriverOccurrence<Provisional, Cause, Output>>,
}

impl<Provisional, Cause, Output> OccurrenceSender<Provisional, Cause, Output> {
    pub(crate) async fn send(
        &self,
        occurrence: DriverOccurrence<Provisional, Cause, Output>,
    ) -> Result<(), DriverOccurrence<Provisional, Cause, Output>> {
        self.sender
            .send(occurrence)
            .await
            .map_err(|failure| failure.0)
    }
}

pub(crate) struct OccurrenceReceiver<Provisional, Cause, Output> {
    receiver: mpsc::Receiver<DriverOccurrence<Provisional, Cause, Output>>,
}

impl<Provisional, Cause, Output> OccurrenceReceiver<Provisional, Cause, Output> {
    pub(crate) async fn recv(&mut self) -> Option<DriverOccurrence<Provisional, Cause, Output>> {
        self.receiver.recv().await
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Option<DriverOccurrence<Provisional, Cause, Output>> {
        self.receiver.try_recv().ok()
    }
}

pub(crate) fn occurrence_channel<Provisional, Cause, Output>(
    capacity: NonZeroUsize,
) -> (
    OccurrenceSender<Provisional, Cause, Output>,
    OccurrenceReceiver<Provisional, Cause, Output>,
) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (OccurrenceSender { sender }, OccurrenceReceiver { receiver })
}

pub(crate) trait CoordinatorClock: Clone + Send + Sync + 'static {
    type Instant: Add<Duration, Output = Self::Instant> + Clone + Send + 'static;

    fn now(&mut self) -> Self::Instant;

    fn wait_until(&self, deadline: Self::Instant) -> impl Future<Output = ()> + Send;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommittedReduction<Cause, Output> {
    pub(crate) occurrence_ordinal: OccurrenceOrdinal,
    pub(crate) state: RuntimeState<Cause, Output>,
    pub(crate) events: Vec<TransitionEvent<Cause>>,
}

pub(crate) trait CommitPort<Commit> {
    fn commit(&mut self, commit: Commit) -> impl Future<Output = ()>;
}

pub(crate) trait ActionPort<Action> {
    fn release(&mut self, action: Action) -> impl Future<Output = ()>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoordinationError {
    ArtifactStagingMismatch,
    OccurrenceChannelClosed,
    OccurrenceOrdinalExhausted,
    ReducerStateUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CoordinationResult<Cause, Output> {
    pub(crate) state: RuntimeState<Cause, Output>,
    pub(crate) last_occurrence_ordinal: OccurrenceOrdinal,
}

pub(crate) struct Coordinator<Provisional, Cause, Output, Clock, Commits, Actions>
where
    Clock: CoordinatorClock,
{
    admitted: AdmittedWorkflow,
    occurrences: OccurrenceReceiver<Provisional, Cause, Output>,
    clock: Clock,
    commits: Commits,
    actions: Actions,
    state: Option<RuntimeState<Cause, Output>>,
}

impl<Provisional, Cause, Output, Clock, Commits, Actions>
    Coordinator<Provisional, Cause, Output, Clock, Commits, Actions>
where
    Cause: Clone,
    Output: Clone,
    Clock: CoordinatorClock,
    Commits: CommitPort<CommittedReduction<Cause, Output>>,
    Actions: ActionPort<RequestedAction<Provisional, Cause, Output, Clock::Instant>>,
{
    pub(crate) fn new(
        admitted: AdmittedWorkflow,
        occurrences: OccurrenceReceiver<Provisional, Cause, Output>,
        clock: Clock,
        commits: Commits,
        actions: Actions,
    ) -> Self {
        Self {
            admitted,
            occurrences,
            clock,
            commits,
            actions,
            state: None,
        }
    }

    pub(crate) async fn run(
        mut self,
    ) -> Result<CoordinationResult<Cause, Output>, CoordinationError> {
        let mut cancellation = self
            .admitted
            .execution()
            .cancellation()
            .source()
            .subscribe();
        let grace = self.admitted.execution().cancellation().grace();
        let initial_reason = *cancellation.borrow_and_update();
        let mut cancellation_admitted = initial_reason.is_some();
        let initial_cancellation = initial_reason.map(|reason| CancellationRequest {
            reason,
            deadline: self.clock.now() + grace,
        });
        let mut ordinal = OccurrenceOrdinal(0)
            .next()
            .ok_or(CoordinationError::OccurrenceOrdinalExhausted)?;
        let initialization = runtime::initialize::<Provisional, Cause, Output, Clock::Instant>(
            &self.admitted,
            initial_cancellation,
        );
        if self.commit(ordinal, initialization).await? {
            return self.result(ordinal);
        }

        loop {
            let occurrence = tokio::select! {
                biased;
                changed = cancellation.changed(), if !cancellation_admitted => {
                    if changed.is_err() {
                        return Err(CoordinationError::OccurrenceChannelClosed);
                    }
                    let Some(reason) = *cancellation.borrow_and_update() else {
                        continue;
                    };
                    cancellation_admitted = true;
                    Occurrence::CancellationRequested {
                        reason,
                        deadline: self.clock.now() + grace,
                    }
                }
                driver_occurrence = self.occurrences.receiver.recv() => {
                    let Some(driver_occurrence) = driver_occurrence else {
                        return Err(CoordinationError::OccurrenceChannelClosed);
                    };
                    driver_occurrence.into_runtime()
                }
            };
            ordinal = ordinal
                .next()
                .ok_or(CoordinationError::OccurrenceOrdinalExhausted)?;
            let Some(state) = self.state.as_ref() else {
                return Err(CoordinationError::ReducerStateUnavailable);
            };
            let reduction = runtime::reduce(state, occurrence);
            if self.commit(ordinal, reduction).await? {
                return self.result(ordinal);
            }
        }
    }

    async fn commit(
        &mut self,
        occurrence_ordinal: OccurrenceOrdinal,
        reduction: Reduction<Provisional, Cause, Output, Clock::Instant>,
    ) -> Result<bool, CoordinationError> {
        let Reduction {
            state,
            events,
            actions,
        } = reduction;
        let terminal = !matches!(&state.workflow, WorkflowState::Executing { .. });
        self.state = Some(state);
        let Some(committed_state) = self.state.clone() else {
            return Err(CoordinationError::ReducerStateUnavailable);
        };
        self.commits
            .commit(CommittedReduction {
                occurrence_ordinal,
                state: committed_state,
                events,
            })
            .await;
        if terminal {
            self.occurrences.receiver.close();
        }
        for action in actions {
            self.actions.release(action).await;
        }
        Ok(terminal)
    }

    fn result(
        &self,
        last_occurrence_ordinal: OccurrenceOrdinal,
    ) -> Result<CoordinationResult<Cause, Output>, CoordinationError> {
        let Some(state) = self.state.clone() else {
            return Err(CoordinationError::ReducerStateUnavailable);
        };
        Ok(CoordinationResult {
            state,
            last_occurrence_ordinal,
        })
    }
}

#[cfg(test)]
mod tests;
