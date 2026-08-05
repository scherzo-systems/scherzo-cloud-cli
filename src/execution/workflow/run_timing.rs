use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use time::OffsetDateTime;

use super::admission::CancellationReason;
use super::observation::ExecutionObservation;
use super::presentation_feed::DisplayDeadline;
use super::runtime::{StepStateKind, TransitionEvent, WorkflowState};

#[derive(Clone, Copy, Debug)]
pub(crate) struct ObservationTime {
    pub(crate) utc: OffsetDateTime,
    pub(crate) monotonic: Instant,
}

pub(crate) trait ObservationClock: Clone + Send + Sync + 'static {
    fn sample(&self) -> ObservationTime;
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ObservedStepTiming {
    pub(crate) started: ObservationTime,
    pub(crate) finished: Option<Instant>,
}

#[derive(Clone, Debug)]
pub(crate) struct RunTimingSnapshot {
    pub(crate) presentation_opened: ObservationTime,
    pub(crate) execution_started: Option<ObservationTime>,
    pub(crate) steps: BTreeMap<String, ObservedStepTiming>,
    pub(crate) cancellation: Option<(CancellationReason, OffsetDateTime)>,
    pub(crate) terminal: Option<ObservationTime>,
    pub(crate) quiesced: Option<ObservationTime>,
}

#[derive(Clone)]
pub(crate) struct RunTimingObservation {
    state: Arc<Mutex<RunTimingSnapshot>>,
}

impl RunTimingObservation {
    pub(crate) fn new(presentation_opened: ObservationTime) -> Self {
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

    pub(crate) fn mark_execution_started(&self, observed_at: ObservationTime) {
        lock_timing(&self.state)
            .execution_started
            .get_or_insert(observed_at);
    }

    pub(crate) fn observe<Deadline: DisplayDeadline>(
        &self,
        observation: &ExecutionObservation<Deadline>,
        clock: &impl ObservationClock,
    ) {
        if observation_needs_timing_sample(observation) {
            self.record(observation, clock.sample());
        }
    }

    pub(crate) fn record<Deadline: DisplayDeadline>(
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
                if !matches!(to, WorkflowState::Executing { .. }) =>
            {
                timing.terminal.get_or_insert(observed_at);
            }
            TransitionEvent::Step { .. } | TransitionEvent::Workflow { .. } => {}
        }
    }

    pub(crate) fn mark_quiesced(&self, observed_at: ObservationTime) {
        lock_timing(&self.state).quiesced.get_or_insert(observed_at);
    }

    pub(crate) fn snapshot(&self) -> RunTimingSnapshot {
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
        | TransitionEvent::CancellationAccepted { .. } => true,
        TransitionEvent::Workflow { to, .. } => !matches!(to, WorkflowState::Executing { .. }),
        TransitionEvent::Step { .. } => false,
    }
}

fn lock_timing(timing: &Mutex<RunTimingSnapshot>) -> MutexGuard<'_, RunTimingSnapshot> {
    timing
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
