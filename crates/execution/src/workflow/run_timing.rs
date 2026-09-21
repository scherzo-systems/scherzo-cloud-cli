use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use time::OffsetDateTime;

use super::admission::CancellationReason;
use super::observation::ExecutionObservation;
use super::presentation_feed::DisplayDeadline;
use super::runtime::{StepStateKind, TransitionEvent, WorkflowState};

#[derive(Clone, Copy, Debug)]
pub struct ObservationTime {
    pub utc: OffsetDateTime,
    pub monotonic: Instant,
}

pub trait ObservationClock: Clone + Send + Sync + 'static {
    fn sample(&self) -> ObservationTime;
}

#[derive(Clone, Copy, Debug)]
pub struct ObservedStepTiming {
    pub started: ObservationTime,
    pub finished: Option<Instant>,
}

#[derive(Clone, Debug)]
pub struct RunTimingSnapshot {
    pub(crate) presentation_opened: ObservationTime,
    pub execution_started: Option<ObservationTime>,
    pub steps: BTreeMap<String, ObservedStepTiming>,
    pub cancellation: Option<(CancellationReason, OffsetDateTime)>,
    pub terminal: Option<ObservationTime>,
    pub(crate) quiesced: Option<ObservationTime>,
}

#[derive(Clone)]
pub struct RunTimingObservation {
    state: Arc<Mutex<RunTimingSnapshot>>,
}

impl RunTimingObservation {
    pub fn new(presentation_opened: ObservationTime) -> Self {
        Self {
            state: Arc::new(Mutex::new(RunTimingSnapshot {
                presentation_opened,
                execution_started: None,
                steps: BTreeMap::new(),
                cancellation: None,
                terminal: None,
                quiesced: None,
            })),
        }
    }

    pub fn mark_execution_started(&self, observed_at: ObservationTime) {
        lock_timing(&self.state)
            .execution_started
            .get_or_insert(observed_at);
    }

    pub fn observe<Deadline: DisplayDeadline>(
        &self,
        observation: &ExecutionObservation<Deadline>,
        clock: &impl ObservationClock,
    ) {
        if observation_needs_timing_sample(observation) {
            self.record(observation, clock.sample());
        }
    }

    pub fn record<Deadline: DisplayDeadline>(
        &self,
        observation: &ExecutionObservation<Deadline>,
        observed_at: ObservationTime,
    ) {
        let ExecutionObservation::Transition(transition) = observation else {
            return;
        };
        let mut timing = lock_timing(&self.state);
        match &transition.event {
            TransitionEvent::Step { step, to, .. } if *to == StepStateKind::Starting => {
                timing
                    .steps
                    .entry(step.clone())
                    .or_insert(ObservedStepTiming {
                        started: observed_at,
                        finished: None,
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
                if let Some(step) = timing.steps.get_mut(step) {
                    step.finished.get_or_insert(observed_at.monotonic);
                }
            }
            TransitionEvent::CancellationAccepted {
                reason, deadline, ..
            } => {
                timing
                    .cancellation
                    .get_or_insert((*reason, deadline.deadline_utc()));
            }
            TransitionEvent::Workflow { to, .. }
                if matches!(
                    to.as_ref(),
                    WorkflowState::Succeeded
                        | WorkflowState::Failed { .. }
                        | WorkflowState::Cancelled { .. }
                ) =>
            {
                timing.terminal.get_or_insert(observed_at);
            }
            TransitionEvent::Step { .. }
            | TransitionEvent::Workflow { .. }
            | TransitionEvent::FinalizationCancellationAccepted { .. }
            | TransitionEvent::ForceAbortAccepted { .. } => {}
        }
    }

    pub(crate) fn mark_quiesced(&self, observed_at: ObservationTime) {
        lock_timing(&self.state).quiesced.get_or_insert(observed_at);
    }

    pub fn snapshot(&self) -> RunTimingSnapshot {
        lock_timing(&self.state).clone()
    }
}

fn observation_needs_timing_sample<Deadline>(observation: &ExecutionObservation<Deadline>) -> bool {
    let ExecutionObservation::Transition(transition) = observation else {
        return false;
    };
    match &transition.event {
        TransitionEvent::Step {
            to:
                StepStateKind::Starting
                | StepStateKind::Succeeded
                | StepStateKind::Failed
                | StepStateKind::Blocked
                | StepStateKind::NotRun
                | StepStateKind::Cancelled,
            ..
        }
        | TransitionEvent::CancellationAccepted { .. }
        | TransitionEvent::FinalizationCancellationAccepted { .. }
        | TransitionEvent::ForceAbortAccepted { .. } => true,
        TransitionEvent::Workflow { to, .. } => matches!(
            to.as_ref(),
            WorkflowState::Succeeded
                | WorkflowState::Failed { .. }
                | WorkflowState::Cancelled { .. }
        ),
        TransitionEvent::Step { .. } => false,
    }
}

fn lock_timing(timing: &Mutex<RunTimingSnapshot>) -> MutexGuard<'_, RunTimingSnapshot> {
    timing
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
