use std::future::{Future, pending, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use super::PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL;
use crate::workflow::agent::{AgentObservation, AgentObservationEnvelope, AgentObservationSink};
use crate::workflow::coordinator::CoordinatorClock;
use crate::workflow::result_validation::{
    ResultValidationWorker, RunningResultValidation, ValidationWorkerDecision,
    ValidationWorkerRequest,
};

#[derive(Clone, Copy)]
pub(crate) struct InlineValidationWorker;

pub(crate) struct InlineValidation(pub(crate) Option<Result<ValidationWorkerDecision, ()>>);

impl ResultValidationWorker for InlineValidationWorker {
    type Running = InlineValidation;

    fn start(&self, request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        Ok(InlineValidation(Some(request.evaluate())))
    }
}

impl RunningResultValidation for InlineValidation {
    fn wait(&mut self) -> impl Future<Output = Result<ValidationWorkerDecision, ()>> + Send {
        ready(self.0.take().expect("inline validation is awaited once"))
    }

    fn request_stop(&mut self) {}

    fn quiesce(self) -> impl Future<Output = ()> + Send {
        ready(())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PendingClock;

impl CoordinatorClock for PendingClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        if deadline == PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL {
            let probe = tokio::spawn(async {});
            let _ = probe.await;
        } else {
            pending().await
        }
    }
}

#[derive(Clone)]
pub(crate) struct ControlledClock {
    pub(crate) registrations: mpsc::UnboundedSender<Duration>,
    pub(crate) release: watch::Receiver<bool>,
}

pub(crate) struct ClockControl {
    pub(crate) deadlines: mpsc::UnboundedReceiver<Duration>,
    pub(crate) expired: watch::Sender<bool>,
}

impl ControlledClock {
    pub(crate) fn new() -> (Self, ClockControl) {
        let (registrations, deadlines) = mpsc::unbounded_channel();
        let (expired, release) = watch::channel(false);
        (
            Self {
                registrations,
                release,
            },
            ClockControl { deadlines, expired },
        )
    }
}

impl CoordinatorClock for ControlledClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        if deadline == PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL {
            let probe = tokio::spawn(async {});
            let _ = probe.await;
            return;
        }
        let _ = self.registrations.send(deadline);
        let mut release = self.release.clone();
        while !*release.borrow_and_update() {
            if release.changed().await.is_err() {
                return;
            }
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct RecordingObservationSink(Arc<Mutex<Vec<AgentObservationEnvelope>>>);

impl RecordingObservationSink {
    /// Native text and reasoning arrive as arbitrarily split deltas.
    pub(crate) fn concatenated_text(
        &self,
        select: impl Fn(&AgentObservation) -> Option<&str>,
    ) -> String {
        self.snapshot()
            .iter()
            .filter_map(|envelope| select(envelope.observation()))
            .collect()
    }

    pub(crate) fn snapshot(&self) -> Vec<AgentObservationEnvelope> {
        self.0.lock().expect("observation sink lock").clone()
    }
}

impl AgentObservationSink for RecordingObservationSink {
    fn observe(&self, observation: AgentObservationEnvelope) -> impl Future<Output = ()> + Send {
        self.0
            .lock()
            .expect("observation sink lock")
            .push(observation);
        ready(())
    }
}
