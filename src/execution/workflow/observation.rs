use std::future::{Future, ready};
use std::sync::Arc;

use super::agent::AgentObservationEnvelope;
use super::evidence::{BlockedDetail, CancellationDetail, FailureDetail, NonExecutionDetail};
use super::runtime::{
    ActionId, ActiveStepInvocation, RecoveryDecisionKind, RecoveryHandlerActivity,
    RecoveryHandlerKind, TransitionEvent,
};

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
    Recovery {
        active: ActiveStepInvocation,
        active_invocation_id: ActionId,
        settled_invocation: Option<(ActionId, ActiveStepInvocation)>,
        configured_rounds: u8,
        handler_kind: Option<RecoveryHandlerKind>,
        handler_state: Option<RecoveryHandlerActivity>,
        decision: Option<RecoveryDecisionKind>,
    },
    OutputsCommitted {
        outputs: Vec<String>,
    },
    Failed {
        detail: FailureDetail,
    },
    Blocked {
        detail: BlockedDetail,
    },
    NotRun {
        detail: NonExecutionDetail,
    },
    Cancelling {
        detail: CancellationDetail,
    },
    Cancelled {
        detail: CancellationDetail,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransitionObservation<Deadline> {
    pub(crate) event: TransitionEvent<Deadline>,
    pub(crate) step: Option<ObservedStepTransition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionObservation<Deadline> {
    Transition(TransitionObservation<Deadline>),
    CommandOutput(CommandOutputObservation),
    CommandOutputClosed(CommandOutputClosedObservation),
    Agent(AgentObservationEnvelope),
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
