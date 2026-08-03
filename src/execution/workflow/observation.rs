use std::future::{Future, ready};
use std::sync::Arc;

use super::admission::CancellationReason;
use super::runtime::{ActionId, FailurePhase, NotRunReason, TransitionEvent};
use super::step_runtime::StepFailureCause;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandOutputSource {
    StandardOutput,
    StandardError,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SourceSequence(u64);

impl SourceSequence {
    pub(crate) const fn get(self) -> u64 {
        self.0
    }

    pub(super) const fn first() -> Self {
        Self(1)
    }

    pub(super) fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandOutputObservation {
    pub(crate) step: String,
    pub(crate) invocation: ActionId,
    pub(crate) source: CommandOutputSource,
    pub(crate) sequence: SourceSequence,
    pub(crate) bytes: Arc<[u8]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandOutputClosedObservation {
    pub(crate) step: String,
    pub(crate) invocation: ActionId,
    pub(crate) source: CommandOutputSource,
    pub(crate) sequence: SourceSequence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ObservedStepTransition {
    OutputsCommitted {
        outputs: Vec<String>,
    },
    Failed {
        phase: FailurePhase,
        cause: StepFailureCause,
    },
    Blocked {
        dependency: String,
    },
    NotRun {
        reason: NotRunReason,
    },
    Cancelling {
        reason: CancellationReason,
    },
    Cancelled {
        reason: CancellationReason,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransitionObservation<Deadline> {
    pub(crate) event: TransitionEvent<StepFailureCause, Deadline>,
    pub(crate) step: Option<ObservedStepTransition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionObservation<Deadline> {
    Transition(TransitionObservation<Deadline>),
    CommandOutput(CommandOutputObservation),
    CommandOutputClosed(CommandOutputClosedObservation),
}

pub(crate) trait ExecutionObserver<Deadline>: Clone + Send + Sync + 'static {
    fn observe(
        &self,
        observation: ExecutionObservation<Deadline>,
    ) -> impl Future<Output = ()> + Send;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NoopExecutionObserver;

impl<Deadline> ExecutionObserver<Deadline> for NoopExecutionObserver {
    fn observe(
        &self,
        _observation: ExecutionObservation<Deadline>,
    ) -> impl Future<Output = ()> + Send {
        ready(())
    }
}
