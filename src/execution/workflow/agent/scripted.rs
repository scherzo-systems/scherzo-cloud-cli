use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{Mutex, mpsc, oneshot};

use super::{
    AgentAdapter, AgentFailureCause, AgentInvocation, AgentInvocationIdentity, AgentObservation,
    AgentObservationEmissionError, AgentObservationSink, AgentOutcome, AgentTerminalCallback,
    AgentTerminalReportError, AgentValueKind, BoundedAgentResponse, BoundedSchemaValidAgentResult,
    CompletedAgentInvocation,
};

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScriptedInvocationStarted {
    identity: AgentInvocationIdentity,
    value_kind: AgentValueKind,
}

impl ScriptedInvocationStarted {
    pub(crate) fn identity(&self) -> &AgentInvocationIdentity {
        &self.identity
    }

    pub(crate) fn value_kind(&self) -> AgentValueKind {
        self.value_kind
    }
}

#[derive(Clone)]
pub(crate) struct ScriptedAgentAdapter {
    commands: Arc<Mutex<Option<mpsc::UnboundedReceiver<ScriptedCommand>>>>,
    started: mpsc::UnboundedSender<ScriptedInvocationStarted>,
}

pub(crate) struct ScriptedAgentControl {
    commands: mpsc::UnboundedSender<ScriptedCommand>,
    started: mpsc::UnboundedReceiver<ScriptedInvocationStarted>,
}

type CommandAcknowledgement = oneshot::Sender<Result<(), ScriptedAgentError>>;

enum ScriptedCommand {
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
    InvocationCancelled,
    ObservationSequenceExhausted,
    TerminalAlreadyReported,
    TerminalReceiverClosed,
    ValueAlreadyProposed,
    ValueTooLarge,
    WrongValueMode,
}

impl ScriptedAgentAdapter {
    pub(crate) fn new() -> (Self, ScriptedAgentControl) {
        let (command_sender, commands) = mpsc::unbounded_channel();
        let (started_sender, started) = mpsc::unbounded_channel();
        (
            Self {
                commands: Arc::new(Mutex::new(Some(commands))),
                started: started_sender,
            },
            ScriptedAgentControl {
                commands: command_sender,
                started,
            },
        )
    }
}

impl ScriptedAgentControl {
    pub(crate) async fn wait_until_started(
        &mut self,
    ) -> Result<ScriptedInvocationStarted, ScriptedAgentError> {
        self.started
            .recv()
            .await
            .ok_or(ScriptedAgentError::AdapterStopped)
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

impl<Sink> AgentAdapter<Sink> for ScriptedAgentAdapter
where
    Sink: AgentObservationSink,
{
    type NativeConfiguration = ();
    type ProtocolLimits = ();

    async fn invoke(
        &self,
        invocation: AgentInvocation<Self::NativeConfiguration, Self::ProtocolLimits, Sink>,
        terminal: AgentTerminalCallback,
    ) {
        let Some(mut commands) = self.commands.lock().await.take() else {
            let _ = terminal.report(AgentOutcome::Failed {
                cause: AgentFailureCause::HarnessProtocolFailed,
            });
            return;
        };
        let mut cancellation = invocation.cancellation().subscribe();
        if let Some(reason) = *cancellation.borrow_and_update() {
            let _ = terminal.report(AgentOutcome::Cancelled { reason });
            return;
        }
        if self
            .started
            .send(ScriptedInvocationStarted {
                identity: invocation.identity().clone(),
                value_kind: invocation.value_mode().kind(),
            })
            .is_err()
        {
            let _ = terminal.report(AgentOutcome::Failed {
                cause: AgentFailureCause::HarnessProtocolFailed,
            });
            return;
        }

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
                        ScriptedCommand::Barrier { reached, release } => {
                            let _ = reached.send(());
                            let _ = release.await;
                        }
                        ScriptedCommand::Observe {
                            observation,
                            acknowledged,
                        } => {
                            let result = invocation
                                .observations()
                                .emit(observation)
                                .await
                                .map_err(ScriptedAgentError::from);
                            let _ = acknowledged.send(result);
                        }
                        ScriptedCommand::Propose {
                            value,
                            acknowledged,
                        } => {
                            let result = propose_value(&invocation, &mut provisional, value);
                            let _ = acknowledged.send(result);
                        }
                        ScriptedCommand::Complete { acknowledged } => {
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
    invocation: &AgentInvocation<(), (), Sink>,
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
                bytes > invocation.limits().maximum_response_bytes().get()
            }) {
                return Err(ScriptedAgentError::ValueTooLarge);
            }
            CompletedAgentInvocation::Response(BoundedAgentResponse::from_bounded(value))
        }
        ScriptedAgentValue::Result(value) => {
            let bytes = serde_json::to_vec(value.as_ref())
                .map_err(|_| ScriptedAgentError::WrongValueMode)?;
            if u64::try_from(bytes.len()).map_or(true, |bytes| {
                bytes > invocation.limits().maximum_result_bytes().get()
            }) {
                return Err(ScriptedAgentError::ValueTooLarge);
            }
            CompletedAgentInvocation::Result(BoundedSchemaValidAgentResult::from_validated(
                value,
                Arc::from(bytes),
            ))
        }
    };
    *provisional = Some(completed);
    Ok(())
}

fn completed_outcome<Sink>(
    invocation: &AgentInvocation<(), (), Sink>,
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

fn cancellation_outcome<Sink>(invocation: &AgentInvocation<(), (), Sink>) -> Option<AgentOutcome>
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

impl From<AgentTerminalReportError> for ScriptedAgentError {
    fn from(value: AgentTerminalReportError) -> Self {
        match value {
            AgentTerminalReportError::AlreadyReported => Self::TerminalAlreadyReported,
            AgentTerminalReportError::CompletionModeMismatch => Self::CompletionModeMismatch,
            AgentTerminalReportError::ReceiverClosed => Self::TerminalReceiverClosed,
        }
    }
}
