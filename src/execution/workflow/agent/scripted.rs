use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use super::dispatch::AgentInvocationDispatcher;
use super::{
    AgentCompatibilityProfile, AgentFailureCause, AgentInvocationIdentity, AgentObservation,
    AgentObservationEmissionError, AgentObservationSink, AgentOutcome, AgentStartCallback,
    AgentStartReportError, AgentTerminalCallback, AgentTerminalReportError, AgentValueKind,
    BoundedAgentResponse, BoundedSchemaValidAgentResult, CompletedAgentInvocation,
};
use crate::execution::workflow::agent_input::ClosedAgentInvocation;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ScriptedAgentValue {
    Response(Arc<str>),
    Result(Arc<Value>),
}

impl ScriptedAgentValue {
    fn kind(&self) -> AgentValueKind {
        match self {
            Self::Response(_) => AgentValueKind::Response,
            Self::Result(_) => AgentValueKind::Result,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScriptedInvocationStarted {
    identity: AgentInvocationIdentity,
    profile: AgentCompatibilityProfile,
    message: Arc<str>,
    attachments: Arc<[super::StagedAgentAttachment]>,
    value_kind: AgentValueKind,
    control: ScriptedInvocationControl,
}

impl ScriptedInvocationStarted {
    pub(crate) fn identity(&self) -> &AgentInvocationIdentity {
        &self.identity
    }

    pub(crate) fn profile(&self) -> AgentCompatibilityProfile {
        self.profile
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn attachments(&self) -> &[super::StagedAgentAttachment] {
        &self.attachments
    }

    pub(crate) fn value_kind(&self) -> AgentValueKind {
        self.value_kind
    }

    pub(crate) fn control(&self) -> &ScriptedInvocationControl {
        &self.control
    }
}

#[derive(Clone)]
pub(crate) struct ScriptedAgentAdapter {
    started: mpsc::UnboundedSender<ScriptedInvocationStarted>,
}

pub(crate) struct ScriptedAgentControl {
    current: Option<ScriptedInvocationControl>,
    started: mpsc::UnboundedReceiver<ScriptedInvocationStarted>,
}

#[derive(Clone, Debug)]
pub(crate) struct ScriptedInvocationControl {
    commands: mpsc::UnboundedSender<ScriptedCommand>,
}

type CommandAcknowledgement = oneshot::Sender<Result<(), ScriptedAgentError>>;

enum ScriptedCommand {
    Start {
        acknowledged: CommandAcknowledgement,
    },
    Barrier {
        reached: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
    Observe {
        observation: AgentObservation,
        acknowledged: CommandAcknowledgement,
    },
    Propose {
        value: ScriptedAgentValue,
        acknowledged: CommandAcknowledgement,
    },
    Complete {
        acknowledged: CommandAcknowledgement,
    },
    Fail {
        cause: AgentFailureCause,
        acknowledged: CommandAcknowledgement,
    },
}

pub(crate) struct ScriptedBarrier {
    reached: oneshot::Receiver<()>,
    release: Option<oneshot::Sender<()>>,
}

impl ScriptedBarrier {
    pub(crate) async fn wait_until_blocked(&mut self) -> Result<(), ScriptedAgentError> {
        (&mut self.reached)
            .await
            .map_err(|_| ScriptedAgentError::AdapterStopped)
    }

    pub(crate) fn release(mut self) -> Result<(), ScriptedAgentError> {
        self.release
            .take()
            .ok_or(ScriptedAgentError::BarrierAlreadyReleased)?
            .send(())
            .map_err(|_| ScriptedAgentError::AdapterStopped)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScriptedAgentError {
    AdapterStopped,
    BarrierAlreadyReleased,
    CompletionModeMismatch,
    InvocationAlreadyStarted,
    InvocationCancelled,
    InvocationNotStarted,
    ObservationSequenceExhausted,
    TerminalAlreadyReported,
    TerminalReceiverClosed,
    ValueAlreadyProposed,
    ValueTooLarge,
    WrongValueMode,
}

impl ScriptedAgentAdapter {
    pub(crate) fn new() -> (Self, ScriptedAgentControl) {
        let (started_sender, started) = mpsc::unbounded_channel();
        (
            Self {
                started: started_sender,
            },
            ScriptedAgentControl {
                current: None,
                started,
            },
        )
    }
}

impl ScriptedAgentControl {
    pub(crate) async fn wait_until_started(
        &mut self,
    ) -> Result<ScriptedInvocationStarted, ScriptedAgentError> {
        let started = self
            .started
            .recv()
            .await
            .ok_or(ScriptedAgentError::AdapterStopped)?;
        self.current = Some(started.control.clone());
        Ok(started)
    }

    fn current(&self) -> Result<&ScriptedInvocationControl, ScriptedAgentError> {
        self.current
            .as_ref()
            .ok_or(ScriptedAgentError::AdapterStopped)
    }

    pub(crate) async fn start(&self) -> Result<(), ScriptedAgentError> {
        self.current()?.start().await
    }

    pub(crate) fn block(&self) -> Result<ScriptedBarrier, ScriptedAgentError> {
        self.current()?.block()
    }

    pub(crate) async fn observe(
        &self,
        observation: AgentObservation,
    ) -> Result<(), ScriptedAgentError> {
        self.current()?.observe(observation).await
    }

    pub(crate) async fn propose(
        &self,
        value: ScriptedAgentValue,
    ) -> Result<(), ScriptedAgentError> {
        self.current()?.propose(value).await
    }

    pub(crate) async fn complete(&self) -> Result<(), ScriptedAgentError> {
        self.current()?.complete().await
    }

    pub(crate) async fn fail(&self, cause: AgentFailureCause) -> Result<(), ScriptedAgentError> {
        self.current()?.fail(cause).await
    }
}

impl ScriptedInvocationControl {
    pub(crate) async fn start(&self) -> Result<(), ScriptedAgentError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.commands
            .send(ScriptedCommand::Start { acknowledged })
            .map_err(|_| ScriptedAgentError::AdapterStopped)?;
        receive_acknowledgement(acknowledgement).await
    }

    pub(crate) fn block(&self) -> Result<ScriptedBarrier, ScriptedAgentError> {
        let (reached, wait_for_reached) = oneshot::channel();
        let (release, wait_for_release) = oneshot::channel();
        self.commands
            .send(ScriptedCommand::Barrier {
                reached,
                release: wait_for_release,
            })
            .map_err(|_| ScriptedAgentError::AdapterStopped)?;
        Ok(ScriptedBarrier {
            reached: wait_for_reached,
            release: Some(release),
        })
    }

    pub(crate) async fn observe(
        &self,
        observation: AgentObservation,
    ) -> Result<(), ScriptedAgentError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.commands
            .send(ScriptedCommand::Observe {
                observation,
                acknowledged,
            })
            .map_err(|_| ScriptedAgentError::AdapterStopped)?;
        receive_acknowledgement(acknowledgement).await
    }

    pub(crate) async fn propose(
        &self,
        value: ScriptedAgentValue,
    ) -> Result<(), ScriptedAgentError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.commands
            .send(ScriptedCommand::Propose {
                value,
                acknowledged,
            })
            .map_err(|_| ScriptedAgentError::AdapterStopped)?;
        receive_acknowledgement(acknowledgement).await
    }

    pub(crate) async fn complete(&self) -> Result<(), ScriptedAgentError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.commands
            .send(ScriptedCommand::Complete { acknowledged })
            .map_err(|_| ScriptedAgentError::AdapterStopped)?;
        receive_acknowledgement(acknowledgement).await
    }

    pub(crate) async fn fail(&self, cause: AgentFailureCause) -> Result<(), ScriptedAgentError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.commands
            .send(ScriptedCommand::Fail {
                cause,
                acknowledged,
            })
            .map_err(|_| ScriptedAgentError::AdapterStopped)?;
        receive_acknowledgement(acknowledgement).await
    }
}

async fn receive_acknowledgement(
    acknowledgement: oneshot::Receiver<Result<(), ScriptedAgentError>>,
) -> Result<(), ScriptedAgentError> {
    acknowledgement
        .await
        .map_err(|_| ScriptedAgentError::AdapterStopped)?
}

impl<Sink> AgentInvocationDispatcher<Sink> for ScriptedAgentAdapter
where
    Sink: AgentObservationSink,
{
    async fn invoke(
        &self,
        invocation: ClosedAgentInvocation<Sink>,
        started: AgentStartCallback,
        terminal: AgentTerminalCallback,
    ) {
        let (command_sender, mut commands) = mpsc::unbounded_channel();
        let mut cancellation = invocation.cancellation().subscribe();
        if let Some(reason) = *cancellation.borrow_and_update() {
            let _ = terminal.report(AgentOutcome::Cancelled { reason });
            return;
        }
        if self
            .started
            .send(ScriptedInvocationStarted {
                identity: invocation.identity().clone(),
                profile: invocation.profile(),
                message: Arc::from(invocation.prompt().message()),
                attachments: Arc::from(invocation.attachments()),
                value_kind: invocation.value_mode().kind(),
                control: ScriptedInvocationControl {
                    commands: command_sender,
                },
            })
            .is_err()
        {
            let _ = terminal.report(AgentOutcome::Failed {
                cause: AgentFailureCause::HarnessProtocolFailed,
            });
            return;
        }

        let mut lifecycle_started = false;
        let mut provisional = None;
        loop {
            if let Some(reason) = invocation.cancellation().cancellation_reason() {
                drop(provisional.take());
                let _ = terminal.report(AgentOutcome::Cancelled { reason });
                return;
            }

            tokio::select! {
                biased;
                changed = cancellation.changed() => {
                    if changed.is_err() {
                        let _ = terminal.report(AgentOutcome::Failed {
                            cause: AgentFailureCause::HarnessProtocolFailed,
                        });
                        return;
                    }
                    if let Some(reason) = *cancellation.borrow_and_update() {
                        drop(provisional.take());
                        let _ = terminal.report(AgentOutcome::Cancelled { reason });
                        return;
                    }
                }
                command = commands.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    match command {
                        ScriptedCommand::Start { acknowledged } => {
                            let result = if lifecycle_started {
                                Err(ScriptedAgentError::InvocationAlreadyStarted)
                            } else {
                                started.report().map_err(ScriptedAgentError::from)
                            };
                            if result.is_ok() {
                                lifecycle_started = true;
                            }
                            let _ = acknowledged.send(result);
                        }
                        ScriptedCommand::Barrier { reached, release } => {
                            let _ = reached.send(());
                            let _ = release.await;
                        }
                        ScriptedCommand::Observe {
                            observation,
                            acknowledged,
                        } => {
                            let result = if lifecycle_started {
                                invocation
                                    .observations()
                                    .emit(observation)
                                    .await
                                    .map_err(ScriptedAgentError::from)
                            } else {
                                Err(ScriptedAgentError::InvocationNotStarted)
                            };
                            let _ = acknowledged.send(result);
                        }
                        ScriptedCommand::Propose {
                            value,
                            acknowledged,
                        } => {
                            let result = if lifecycle_started {
                                propose_value(&invocation, &mut provisional, value)
                            } else {
                                Err(ScriptedAgentError::InvocationNotStarted)
                            };
                            let _ = acknowledged.send(result);
                        }
                        ScriptedCommand::Complete { acknowledged } => {
                            if !lifecycle_started {
                                let _ = acknowledged.send(Err(
                                    ScriptedAgentError::InvocationNotStarted,
                                ));
                                continue;
                            }
                            let outcome = cancellation_outcome(&invocation).unwrap_or_else(|| {
                                completed_outcome(&invocation, provisional.take())
                            });
                            let result = terminal.report(outcome).map_err(ScriptedAgentError::from);
                            let _ = acknowledged.send(result);
                            return;
                        }
                        ScriptedCommand::Fail {
                            cause,
                            acknowledged,
                        } => {
                            let outcome = cancellation_outcome(&invocation)
                                .unwrap_or(AgentOutcome::Failed { cause });
                            let result = terminal.report(outcome).map_err(ScriptedAgentError::from);
                            let _ = acknowledged.send(result);
                            return;
                        }
                    }
                }
            }
        }

        let outcome = cancellation_outcome(&invocation).unwrap_or(AgentOutcome::Failed {
            cause: AgentFailureCause::HarnessProtocolFailed,
        });
        let _ = terminal.report(outcome);
    }
}

fn propose_value<Sink>(
    invocation: &ClosedAgentInvocation<Sink>,
    provisional: &mut Option<CompletedAgentInvocation>,
    value: ScriptedAgentValue,
) -> Result<(), ScriptedAgentError>
where
    Sink: AgentObservationSink,
{
    if invocation.cancellation().is_cancelled() {
        return Err(ScriptedAgentError::InvocationCancelled);
    }
    if provisional.is_some() {
        return Err(ScriptedAgentError::ValueAlreadyProposed);
    }
    if invocation.value_mode().kind() != value.kind() {
        return Err(ScriptedAgentError::WrongValueMode);
    }

    let completed = match value {
        ScriptedAgentValue::Response(value) => {
            if u64::try_from(value.len()).map_or(true, |bytes| {
                bytes > invocation.maximum_response_bytes().get()
            }) {
                return Err(ScriptedAgentError::ValueTooLarge);
            }
            CompletedAgentInvocation::Response(BoundedAgentResponse::from_bounded(value))
        }
        ScriptedAgentValue::Result(value) => {
            let bytes = serde_json::to_vec(value.as_ref())
                .map_err(|_| ScriptedAgentError::WrongValueMode)?;
            if u64::try_from(bytes.len()).map_or(true, |bytes| {
                bytes > invocation.maximum_result_bytes().get()
            }) {
                return Err(ScriptedAgentError::ValueTooLarge);
            }
            CompletedAgentInvocation::Result(BoundedSchemaValidAgentResult::fixture(
                value,
                Arc::from(bytes),
            ))
        }
    };
    *provisional = Some(completed);
    Ok(())
}

fn completed_outcome<Sink>(
    invocation: &ClosedAgentInvocation<Sink>,
    provisional: Option<CompletedAgentInvocation>,
) -> AgentOutcome
where
    Sink: AgentObservationSink,
{
    match (invocation.value_mode().kind(), provisional) {
        (AgentValueKind::None, None) => AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
        (AgentValueKind::Response, Some(completed @ CompletedAgentInvocation::Response(_)))
        | (AgentValueKind::Result, Some(completed @ CompletedAgentInvocation::Result(_))) => {
            AgentOutcome::Completed(completed)
        }
        (AgentValueKind::Response, None) => AgentOutcome::Failed {
            cause: AgentFailureCause::MissingResponse,
        },
        (AgentValueKind::Result, None) => AgentOutcome::Failed {
            cause: AgentFailureCause::MissingResult,
        },
        (AgentValueKind::None, Some(_))
        | (AgentValueKind::Response, Some(_))
        | (AgentValueKind::Result, Some(_)) => AgentOutcome::Failed {
            cause: AgentFailureCause::HarnessProtocolFailed,
        },
    }
}

fn cancellation_outcome<Sink>(invocation: &ClosedAgentInvocation<Sink>) -> Option<AgentOutcome>
where
    Sink: AgentObservationSink,
{
    invocation
        .cancellation()
        .cancellation_reason()
        .map(|reason| AgentOutcome::Cancelled { reason })
}

impl From<AgentObservationEmissionError> for ScriptedAgentError {
    fn from(value: AgentObservationEmissionError) -> Self {
        match value {
            AgentObservationEmissionError::SequenceExhausted => Self::ObservationSequenceExhausted,
        }
    }
}

impl From<AgentStartReportError> for ScriptedAgentError {
    fn from(value: AgentStartReportError) -> Self {
        match value {
            AgentStartReportError::AlreadyReported => Self::InvocationAlreadyStarted,
            AgentStartReportError::ReceiverClosed => Self::AdapterStopped,
        }
    }
}

impl From<AgentTerminalReportError> for ScriptedAgentError {
    fn from(value: AgentTerminalReportError) -> Self {
        match value {
            AgentTerminalReportError::AlreadyReported => Self::TerminalAlreadyReported,
            AgentTerminalReportError::CompletionModeMismatch => Self::CompletionModeMismatch,
            AgentTerminalReportError::ReceiverClosed => Self::TerminalReceiverClosed,
        }
    }
}
