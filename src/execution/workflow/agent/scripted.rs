use std::marker::PhantomData;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use super::dispatch::ClosedAgentDispatcher;
use super::{
    AgentAdapter, AgentCompatibilityProfile, AgentFailureCause, AgentInvocation,
    AgentInvocationIdentity, AgentObservation, AgentObservationEmissionError, AgentObservationSink,
    AgentOutcome, AgentStartCallback, AgentStartReportError, AgentTerminalCallback,
    AgentTerminalReportError, AgentValueKind, AgentValueMode, BoundedAgentResponse, CapturedJson,
    CompletedAgentInvocation, failed_agent_outcome,
};
use crate::execution::workflow::canonical_json;
use crate::execution::workflow::claude_code::ClaudeCodeConfig;
use crate::execution::workflow::claude_code_stream_json_v1::ClaudeCodeStreamJsonV1ProtocolLimits;
use crate::execution::workflow::codex::CodexConfig;
use crate::execution::workflow::codex_app_server_v1::CodexAppServerV1ProtocolLimits;
use crate::execution::workflow::pi::PiConfig;
use crate::execution::workflow::pi_json_v1::PiJsonV1ProtocolLimits;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ScriptedAgentValue {
    Response(Arc<str>),
    Result(Arc<Value>),
    RawResult(Arc<[u8]>),
}

impl ScriptedAgentValue {
    fn kind(&self) -> AgentValueKind {
        match self {
            Self::Response(_) => AgentValueKind::Response,
            Self::Result(_) | Self::RawResult(_) => AgentValueKind::Result,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScriptedInvocationStarted {
    identity: AgentInvocationIdentity,
    profile: AgentCompatibilityProfile,
    system_prompt: Arc<str>,
    message: Arc<str>,
    working_directory: std::path::PathBuf,
    result_endpoint_directory: std::path::PathBuf,
    diagnostic_directory: std::path::PathBuf,
    environment: crate::execution::workflow::admission::EnvironmentSnapshot,
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

    // The scripted adapter exposes prompt fields for protocol-order assertions; these accessors
    // intentionally mirror the immutable production prompt without sharing its authority type.
    // jscpd:ignore-start
    pub(crate) fn system_prompt(&self) -> &str {
        &self.system_prompt
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
    // jscpd:ignore-end

    pub(crate) fn working_directory(&self) -> &std::path::Path {
        &self.working_directory
    }

    pub(crate) fn result_endpoint_directory(&self) -> &std::path::Path {
        &self.result_endpoint_directory
    }

    pub(crate) fn diagnostic_directory(&self) -> &std::path::Path {
        &self.diagnostic_directory
    }

    pub(crate) fn environment(
        &self,
    ) -> &crate::execution::workflow::admission::EnvironmentSnapshot {
        &self.environment
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

pub(crate) struct ScriptedNativeAdapter<Configuration, ProtocolLimits> {
    started: mpsc::UnboundedSender<ScriptedInvocationStarted>,
    native_types: PhantomData<fn() -> (Configuration, ProtocolLimits)>,
}

impl<Configuration, ProtocolLimits> Clone for ScriptedNativeAdapter<Configuration, ProtocolLimits> {
    fn clone(&self) -> Self {
        Self {
            started: self.started.clone(),
            native_types: PhantomData,
        }
    }
}

pub(crate) type ScriptedAgentDispatcher = ClosedAgentDispatcher<
    ScriptedNativeAdapter<PiConfig, PiJsonV1ProtocolLimits>,
    ScriptedNativeAdapter<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits>,
    ScriptedNativeAdapter<CodexConfig, CodexAppServerV1ProtocolLimits>,
>;

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

pub(crate) fn scripted_agent_dispatcher() -> (ScriptedAgentDispatcher, ScriptedAgentControl) {
    let (started_sender, started) = mpsc::unbounded_channel();
    let pi = ScriptedNativeAdapter::<PiConfig, PiJsonV1ProtocolLimits> {
        started: started_sender.clone(),
        native_types: PhantomData,
    };
    let claude_code =
        ScriptedNativeAdapter::<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits> {
            started: started_sender.clone(),
            native_types: PhantomData,
        };
    let codex = ScriptedNativeAdapter::<CodexConfig, CodexAppServerV1ProtocolLimits> {
        started: started_sender,
        native_types: PhantomData,
    };
    (
        ClosedAgentDispatcher::new(pi, claude_code, codex),
        ScriptedAgentControl {
            current: None,
            started,
        },
    )
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

impl<Sink, Configuration, ProtocolLimits> AgentAdapter<Sink>
    for ScriptedNativeAdapter<Configuration, ProtocolLimits>
where
    Sink: AgentObservationSink,
    Configuration: Send + Sync + 'static,
    ProtocolLimits: Send + Sync + 'static,
{
    type NativeConfiguration = Configuration;
    type ProtocolLimits = ProtocolLimits;

    async fn invoke(
        &self,
        invocation: AgentInvocation<Configuration, ProtocolLimits, Sink>,
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
                profile: invocation.adapter().profile(),
                system_prompt: Arc::from(invocation.prompt().system_prompt()),
                message: Arc::from(invocation.prompt().message()),
                working_directory: invocation.process().cwd().to_owned(),
                result_endpoint_directory: invocation
                    .staging()
                    .result_endpoint_directory()
                    .to_owned(),
                diagnostic_directory: invocation.diagnostic_session().directory().to_owned(),
                environment: invocation.process().environment().clone(),
                attachments: Arc::from(invocation.attachments()),
                value_kind: invocation.value_mode().kind(),
                control: ScriptedInvocationControl {
                    commands: command_sender,
                },
            })
            .is_err()
        {
            let _ = terminal.report(failed_agent_outcome(
                AgentFailureCause::HarnessProtocolFailed,
            ));
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
                        let _ = terminal.report(failed_agent_outcome(
                            AgentFailureCause::HarnessProtocolFailed,
                        ));
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
                                .unwrap_or_else(|| failed_agent_outcome(cause));
                            let result = terminal.report(outcome).map_err(ScriptedAgentError::from);
                            let _ = acknowledged.send(result);
                            return;
                        }
                    }
                }
            }
        }

        let outcome = cancellation_outcome(&invocation)
            .unwrap_or_else(|| failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed));
        let _ = terminal.report(outcome);
    }
}

fn propose_value<Configuration, ProtocolLimits, Sink>(
    invocation: &AgentInvocation<Configuration, ProtocolLimits, Sink>,
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
            CompletedAgentInvocation::Result(captured_json(invocation, value)?)
        }
        ScriptedAgentValue::RawResult(bytes) => {
            if u64::try_from(bytes.len()).map_or(true, |bytes| {
                bytes > invocation.limits().maximum_result_bytes().get()
            }) {
                return Err(ScriptedAgentError::ValueTooLarge);
            }
            serde_json::from_slice::<Value>(&bytes)
                .map_err(|_| ScriptedAgentError::WrongValueMode)?;
            CompletedAgentInvocation::RawResult(bytes)
        }
    };
    *provisional = Some(completed);
    Ok(())
}

fn captured_json<Configuration, ProtocolLimits, Sink>(
    invocation: &AgentInvocation<Configuration, ProtocolLimits, Sink>,
    value: Arc<Value>,
) -> Result<CapturedJson, ScriptedAgentError>
where
    Sink: AgentObservationSink,
{
    let AgentValueMode::Result { schema, .. } = invocation.value_mode() else {
        return Err(ScriptedAgentError::WrongValueMode);
    };
    let carrier =
        canonical_json::to_bounded_bytes(&value, invocation.limits().maximum_result_bytes().get())
            .map_err(|failure| match failure {
                canonical_json::CanonicalJsonError::SizeLimitExceeded => {
                    ScriptedAgentError::ValueTooLarge
                }
                canonical_json::CanonicalJsonError::SerializationFailed => {
                    ScriptedAgentError::WrongValueMode
                }
            })?;
    Ok(CapturedJson::from_validated(value, carrier, schema.clone()))
}

fn completed_outcome<Configuration, ProtocolLimits, Sink>(
    invocation: &AgentInvocation<Configuration, ProtocolLimits, Sink>,
    provisional: Option<CompletedAgentInvocation>,
) -> AgentOutcome
where
    Sink: AgentObservationSink,
{
    match (invocation.value_mode().kind(), provisional) {
        (AgentValueKind::None, None) => AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
        (AgentValueKind::Response, Some(completed @ CompletedAgentInvocation::Response(_)))
        | (AgentValueKind::Result, Some(completed @ CompletedAgentInvocation::Result(_)))
        | (AgentValueKind::Result, Some(completed @ CompletedAgentInvocation::RawResult(_))) => {
            AgentOutcome::Completed(completed)
        }
        (AgentValueKind::Response, None) => {
            failed_agent_outcome(AgentFailureCause::MissingResponse)
        }
        (AgentValueKind::Result, None) => failed_agent_outcome(AgentFailureCause::MissingResult),
        (AgentValueKind::None, Some(_))
        | (AgentValueKind::Response, Some(_))
        | (AgentValueKind::Result, Some(_)) => {
            failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed)
        }
    }
}

fn cancellation_outcome<Configuration, ProtocolLimits, Sink>(
    invocation: &AgentInvocation<Configuration, ProtocolLimits, Sink>,
) -> Option<AgentOutcome>
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
