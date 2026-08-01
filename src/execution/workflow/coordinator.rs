use std::future::Future;
use std::num::NonZeroUsize;
use std::ops::Add;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

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

enum ClaimResolution {
    Publish,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DriverOccurrenceClaimError {
    ReceiverClosed,
}

// The receiver acknowledges a claim before the driver waits for adapter quiescence.
// Resolving this token then either publishes the lifecycle occurrence or discards it.
pub(crate) struct DriverOccurrenceClaim {
    resolution: oneshot::Sender<ClaimResolution>,
}

impl DriverOccurrenceClaim {
    pub(crate) fn publish(self) -> Result<(), DriverOccurrenceClaimError> {
        self.resolution
            .send(ClaimResolution::Publish)
            .map_err(|_| DriverOccurrenceClaimError::ReceiverClosed)
    }

    pub(crate) fn discard(self) {}
}

enum DriverOccurrenceDelivery<Provisional, Cause, Output> {
    Ready(DriverOccurrence<Provisional, Cause, Output>),
    Claimed {
        occurrence: DriverOccurrence<Provisional, Cause, Output>,
        observed: oneshot::Sender<()>,
        resolution: oneshot::Receiver<ClaimResolution>,
    },
}

impl<Provisional, Cause, Output> DriverOccurrenceDelivery<Provisional, Cause, Output> {
    async fn resolve(self) -> Option<DriverOccurrence<Provisional, Cause, Output>> {
        match self {
            Self::Ready(occurrence) => Some(occurrence),
            Self::Claimed {
                occurrence,
                observed,
                resolution,
            } => {
                observed.send(()).ok()?;
                matches!(resolution.await, Ok(ClaimResolution::Publish)).then_some(occurrence)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OccurrenceSender<Provisional, Cause, Output> {
    sender: mpsc::Sender<DriverOccurrenceDelivery<Provisional, Cause, Output>>,
}

impl<Provisional, Cause, Output> OccurrenceSender<Provisional, Cause, Output> {
    pub(crate) async fn send(
        &self,
        occurrence: DriverOccurrence<Provisional, Cause, Output>,
    ) -> Result<(), DriverOccurrence<Provisional, Cause, Output>> {
        self.sender
            .send(DriverOccurrenceDelivery::Ready(occurrence))
            .await
            .map_err(|failure| match failure.0 {
                DriverOccurrenceDelivery::Ready(occurrence)
                | DriverOccurrenceDelivery::Claimed { occurrence, .. } => occurrence,
            })
    }

    pub(crate) async fn claim(
        &self,
        occurrence: DriverOccurrence<Provisional, Cause, Output>,
    ) -> Result<DriverOccurrenceClaim, DriverOccurrenceClaimError> {
        let (observed, observation) = oneshot::channel();
        let (resolution, resolved) = oneshot::channel();
        self.sender
            .send(DriverOccurrenceDelivery::Claimed {
                occurrence,
                observed,
                resolution: resolved,
            })
            .await
            .map_err(|_| DriverOccurrenceClaimError::ReceiverClosed)?;
        observation
            .await
            .map_err(|_| DriverOccurrenceClaimError::ReceiverClosed)?;
        Ok(DriverOccurrenceClaim { resolution })
    }
}

pub(crate) struct OccurrenceReceiver<Provisional, Cause, Output> {
    receiver: mpsc::Receiver<DriverOccurrenceDelivery<Provisional, Cause, Output>>,
    pending: Option<DriverOccurrenceDelivery<Provisional, Cause, Output>>,
}

impl<Provisional, Cause, Output> OccurrenceReceiver<Provisional, Cause, Output> {
    pub(crate) async fn recv(&mut self) -> Option<DriverOccurrence<Provisional, Cause, Output>> {
        loop {
            let delivery = self.recv_delivery().await?;
            if let Some(occurrence) = delivery.resolve().await {
                return Some(occurrence);
            }
        }
    }

    async fn recv_delivery(
        &mut self,
    ) -> Option<DriverOccurrenceDelivery<Provisional, Cause, Output>> {
        match self.pending.take() {
            Some(delivery) => Some(delivery),
            None => self.receiver.recv().await,
        }
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Option<DriverOccurrence<Provisional, Cause, Output>> {
        if self.pending.is_some() {
            return None;
        }
        match self.receiver.try_recv().ok()? {
            DriverOccurrenceDelivery::Ready(occurrence) => Some(occurrence),
            claimed @ DriverOccurrenceDelivery::Claimed { .. } => {
                self.pending = Some(claimed);
                None
            }
        }
    }
}

pub(crate) fn occurrence_channel<Provisional, Cause, Output>(
    capacity: NonZeroUsize,
) -> (
    OccurrenceSender<Provisional, Cause, Output>,
    OccurrenceReceiver<Provisional, Cause, Output>,
) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (
        OccurrenceSender { sender },
        OccurrenceReceiver {
            receiver,
            pending: None,
        },
    )
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

enum CoordinatorInput<Provisional, Cause, Output, Deadline> {
    Cancellation(Occurrence<Provisional, Cause, Output, Deadline>),
    Driver(DriverOccurrenceDelivery<Provisional, Cause, Output>),
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
            let occurrence = loop {
                let input = tokio::select! {
                    biased;
                    changed = cancellation.changed(), if !cancellation_admitted => {
                        if changed.is_err() {
                            return Err(CoordinationError::OccurrenceChannelClosed);
                        }
                        let Some(reason) = *cancellation.borrow_and_update() else {
                            continue;
                        };
                        cancellation_admitted = true;
                        CoordinatorInput::Cancellation(Occurrence::CancellationRequested {
                            reason,
                            deadline: self.clock.now() + grace,
                        })
                    }
                    driver_occurrence = self.occurrences.recv_delivery() => {
                        let Some(driver_occurrence) = driver_occurrence else {
                            return Err(CoordinationError::OccurrenceChannelClosed);
                        };
                        CoordinatorInput::Driver(driver_occurrence)
                    }
                };
                match input {
                    CoordinatorInput::Cancellation(occurrence) => break occurrence,
                    CoordinatorInput::Driver(delivery) => {
                        // A selected claim owns arbitration until its adapter settles, so a
                        // later cancellation cannot gain priority from diagnostic drain timing.
                        let Some(occurrence) = delivery.resolve().await else {
                            continue;
                        };
                        break occurrence.into_runtime();
                    }
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
