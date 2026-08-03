use std::future::{Future, ready};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Mutex as AsyncMutex, oneshot};

use super::admission::{CancellationReason, CancellationSource, EnvironmentSnapshot};
use super::runtime::ActionId;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct WorkflowRunId(Arc<str>);

impl From<Arc<str>> for WorkflowRunId {
    fn from(value: Arc<str>) -> Self {
        Self(value)
    }
}

impl AsRef<str> for WorkflowRunId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentInvocationIdentity {
    run: WorkflowRunId,
    step: Arc<str>,
    invocation: ActionId,
}

impl AgentInvocationIdentity {
    pub(crate) fn new(run: WorkflowRunId, step: Arc<str>, invocation: ActionId) -> Self {
        Self {
            run,
            step,
            invocation,
        }
    }

    pub(crate) fn run(&self) -> &WorkflowRunId {
        &self.run
    }

    pub(crate) fn step(&self) -> &str {
        &self.step
    }

    pub(crate) fn invocation(&self) -> ActionId {
        self.invocation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentCompatibilityProfile {
    PiJsonV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AdmittedAgentAdapter<NativeConfiguration> {
    profile: AgentCompatibilityProfile,
    executable: PathBuf,
    version: Arc<str>,
    native_configuration: NativeConfiguration,
}

impl<NativeConfiguration> AdmittedAgentAdapter<NativeConfiguration> {
    pub(crate) fn new(
        profile: AgentCompatibilityProfile,
        executable: PathBuf,
        version: Arc<str>,
        native_configuration: NativeConfiguration,
    ) -> Self {
        Self {
            profile,
            executable,
            version,
            native_configuration,
        }
    }

    pub(crate) fn profile(&self) -> AgentCompatibilityProfile {
        self.profile
    }

    pub(crate) fn executable(&self) -> &Path {
        &self.executable
    }

    pub(crate) fn version(&self) -> &str {
        &self.version
    }

    pub(crate) fn native_configuration(&self) -> &NativeConfiguration {
        &self.native_configuration
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AgentProcessContext {
    cwd: PathBuf,
    environment: EnvironmentSnapshot,
}

impl AgentProcessContext {
    pub(crate) fn new(cwd: PathBuf, environment: EnvironmentSnapshot) -> Self {
        Self { cwd, environment }
    }

    pub(crate) fn cwd(&self) -> &Path {
        &self.cwd
    }

    pub(crate) fn environment(&self) -> &EnvironmentSnapshot {
        &self.environment
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentPrompt {
    system_prompt: Arc<str>,
    message: Arc<str>,
}

impl AgentPrompt {
    pub(crate) fn new(system_prompt: Arc<str>, message: Arc<str>) -> Self {
        Self {
            system_prompt,
            message,
        }
    }

    pub(crate) fn system_prompt(&self) -> &str {
        &self.system_prompt
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StagedAgentAttachment {
    path: PathBuf,
    media_type: Arc<str>,
    diagnostic_source_name: Option<Arc<str>>,
}

impl StagedAgentAttachment {
    pub(crate) fn new(
        path: PathBuf,
        media_type: Arc<str>,
        diagnostic_source_name: Option<Arc<str>>,
    ) -> Self {
        Self {
            path,
            media_type,
            diagnostic_source_name,
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn media_type(&self) -> &str {
        &self.media_type
    }

    pub(crate) fn diagnostic_source_name(&self) -> Option<&str> {
        self.diagnostic_source_name.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RetainedResultSchema {
    bytes: Arc<[u8]>,
    document: Arc<Value>,
}

impl RetainedResultSchema {
    pub(crate) fn new(bytes: Arc<[u8]>, document: Arc<Value>) -> Self {
        Self { bytes, document }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn document(&self) -> &Value {
        &self.document
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AgentValueMode {
    None,
    Response {
        output: Arc<str>,
    },
    Result {
        output: Arc<str>,
        schema: RetainedResultSchema,
    },
}

impl AgentValueMode {
    pub(crate) fn kind(&self) -> AgentValueKind {
        match self {
            Self::None => AgentValueKind::None,
            Self::Response { .. } => AgentValueKind::Response,
            Self::Result { .. } => AgentValueKind::Result,
        }
    }

    pub(crate) fn output(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Response { output } | Self::Result { output, .. } => Some(output),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentValueKind {
    None,
    Response,
    Result,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PositiveDuration(Duration);

impl PositiveDuration {
    pub(crate) fn new(duration: Duration) -> Option<Self> {
        (!duration.is_zero()).then_some(Self(duration))
    }

    pub(crate) fn get(self) -> Duration {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentInvocationLimits<AdapterProtocolLimits> {
    maximum_system_prompt_bytes: NonZeroU64,
    maximum_message_bytes: NonZeroU64,
    maximum_response_bytes: NonZeroU64,
    maximum_result_bytes: NonZeroU64,
    maximum_result_rejection_feedback_bytes: NonZeroU64,
    result_validation_deadline: PositiveDuration,
    result_settlement_grace: PositiveDuration,
    adapter_protocol: AdapterProtocolLimits,
}

impl<AdapterProtocolLimits> AgentInvocationLimits<AdapterProtocolLimits> {
    #[expect(
        clippy::too_many_arguments,
        reason = "the immutable harness contract carries each admitted limit explicitly"
    )]
    pub(crate) fn new(
        maximum_system_prompt_bytes: NonZeroU64,
        maximum_message_bytes: NonZeroU64,
        maximum_response_bytes: NonZeroU64,
        maximum_result_bytes: NonZeroU64,
        maximum_result_rejection_feedback_bytes: NonZeroU64,
        result_validation_deadline: PositiveDuration,
        result_settlement_grace: PositiveDuration,
        adapter_protocol: AdapterProtocolLimits,
    ) -> Self {
        Self {
            maximum_system_prompt_bytes,
            maximum_message_bytes,
            maximum_response_bytes,
            maximum_result_bytes,
            maximum_result_rejection_feedback_bytes,
            result_validation_deadline,
            result_settlement_grace,
            adapter_protocol,
        }
    }

    pub(crate) fn maximum_system_prompt_bytes(&self) -> NonZeroU64 {
        self.maximum_system_prompt_bytes
    }

    pub(crate) fn maximum_message_bytes(&self) -> NonZeroU64 {
        self.maximum_message_bytes
    }

    pub(crate) fn maximum_response_bytes(&self) -> NonZeroU64 {
        self.maximum_response_bytes
    }

    pub(crate) fn maximum_result_bytes(&self) -> NonZeroU64 {
        self.maximum_result_bytes
    }

    pub(crate) fn maximum_result_rejection_feedback_bytes(&self) -> NonZeroU64 {
        self.maximum_result_rejection_feedback_bytes
    }

    pub(crate) fn result_validation_deadline(&self) -> PositiveDuration {
        self.result_validation_deadline
    }

    pub(crate) fn result_settlement_grace(&self) -> PositiveDuration {
        self.result_settlement_grace
    }

    pub(crate) fn adapter_protocol(&self) -> &AdapterProtocolLimits {
        &self.adapter_protocol
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct InvocationObservationSequence(u64);

impl InvocationObservationSequence {
    const FIRST: u64 = 1;

    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentToolCallPhase {
    Started,
    Updated,
    Completed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentDiagnosticLevel {
    Information,
    Warning,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentLifecycleMilestone {
    SessionEstablished,
    HarnessStarted,
    MessageStarted,
    MessageUpdated,
    MessageCompleted,
    TurnStarted,
    TurnCompleted,
    RetryStarted,
    RetryCompleted,
    CompactionStarted,
    CompactionCompleted,
    QueueUpdated,
    HarnessCompleted,
    HarnessQuiescent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AgentObservation {
    AssistantText {
        text: Arc<str>,
    },
    Reasoning {
        text: Arc<str>,
    },
    ToolCall {
        call_id: Arc<str>,
        name: Arc<str>,
        phase: AgentToolCallPhase,
    },
    ToolResult {
        call_id: Arc<str>,
        is_error: bool,
        content: Arc<str>,
    },
    Diagnostic {
        level: AgentDiagnosticLevel,
        message: Arc<str>,
    },
    Usage {
        input_tokens: u64,
        output_tokens: u64,
    },
    Model {
        name: Arc<str>,
    },
    Lifecycle {
        milestone: AgentLifecycleMilestone,
    },
    ValueRejected {
        kind: AgentValueKind,
        feedback: Arc<str>,
    },
    UnrecognizedHarnessEvent {
        event: Arc<Value>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentObservationEnvelope {
    identity: AgentInvocationIdentity,
    sequence: InvocationObservationSequence,
    observation: AgentObservation,
}

impl AgentObservationEnvelope {
    pub(crate) fn run(&self) -> &WorkflowRunId {
        self.identity.run()
    }

    pub(crate) fn step(&self) -> &str {
        self.identity.step()
    }

    pub(crate) fn invocation(&self) -> ActionId {
        self.identity.invocation()
    }

    pub(crate) fn sequence(&self) -> InvocationObservationSequence {
        self.sequence
    }

    pub(crate) fn observation(&self) -> &AgentObservation {
        &self.observation
    }
}

pub(crate) trait AgentObservationSink: Clone + Send + Sync + 'static {
    fn observe(&self, observation: AgentObservationEnvelope) -> impl Future<Output = ()> + Send;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NoopAgentObservationSink;

impl AgentObservationSink for NoopAgentObservationSink {
    fn observe(&self, _observation: AgentObservationEnvelope) -> impl Future<Output = ()> + Send {
        ready(())
    }
}

#[derive(Clone)]
pub(crate) struct OrderedAgentObservationSink<Sink> {
    identity: AgentInvocationIdentity,
    sink: Sink,
    next_sequence: Arc<AsyncMutex<Option<u64>>>,
}

impl<Sink> OrderedAgentObservationSink<Sink>
where
    Sink: AgentObservationSink,
{
    fn new(identity: AgentInvocationIdentity, sink: Sink) -> Self {
        Self {
            identity,
            sink,
            next_sequence: Arc::new(AsyncMutex::new(Some(InvocationObservationSequence::FIRST))),
        }
    }

    pub(crate) async fn emit(
        &self,
        observation: AgentObservation,
    ) -> Result<(), AgentObservationEmissionError> {
        let mut next_sequence = self.next_sequence.lock().await;
        let sequence = next_sequence.ok_or(AgentObservationEmissionError::SequenceExhausted)?;
        *next_sequence = sequence.checked_add(1);
        self.sink
            .observe(AgentObservationEnvelope {
                identity: self.identity.clone(),
                sequence: InvocationObservationSequence(sequence),
                observation,
            })
            .await;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentObservationEmissionError {
    SequenceExhausted,
}

pub(crate) struct AgentInvocation<NativeConfiguration, AdapterProtocolLimits, ObservationSink> {
    identity: AgentInvocationIdentity,
    adapter: AdmittedAgentAdapter<NativeConfiguration>,
    process: AgentProcessContext,
    prompt: AgentPrompt,
    attachments: Arc<[StagedAgentAttachment]>,
    value_mode: AgentValueMode,
    limits: AgentInvocationLimits<AdapterProtocolLimits>,
    cancellation: CancellationSource,
    observations: OrderedAgentObservationSink<ObservationSink>,
}

impl<NativeConfiguration, AdapterProtocolLimits, ObservationSink>
    AgentInvocation<NativeConfiguration, AdapterProtocolLimits, ObservationSink>
where
    ObservationSink: AgentObservationSink,
{
    #[expect(
        clippy::too_many_arguments,
        reason = "construction makes every immutable invocation-envelope field explicit"
    )]
    pub(crate) fn new(
        identity: AgentInvocationIdentity,
        adapter: AdmittedAgentAdapter<NativeConfiguration>,
        process: AgentProcessContext,
        prompt: AgentPrompt,
        attachments: Arc<[StagedAgentAttachment]>,
        value_mode: AgentValueMode,
        limits: AgentInvocationLimits<AdapterProtocolLimits>,
        cancellation: CancellationSource,
        observation_sink: ObservationSink,
    ) -> Self {
        let observations = OrderedAgentObservationSink::new(identity.clone(), observation_sink);
        Self {
            identity,
            adapter,
            process,
            prompt,
            attachments,
            value_mode,
            limits,
            cancellation,
            observations,
        }
    }

    pub(crate) fn identity(&self) -> &AgentInvocationIdentity {
        &self.identity
    }

    pub(crate) fn adapter(&self) -> &AdmittedAgentAdapter<NativeConfiguration> {
        &self.adapter
    }

    pub(crate) fn process(&self) -> &AgentProcessContext {
        &self.process
    }

    pub(crate) fn prompt(&self) -> &AgentPrompt {
        &self.prompt
    }

    pub(crate) fn attachments(&self) -> &[StagedAgentAttachment] {
        &self.attachments
    }

    pub(crate) fn value_mode(&self) -> &AgentValueMode {
        &self.value_mode
    }

    pub(crate) fn limits(&self) -> &AgentInvocationLimits<AdapterProtocolLimits> {
        &self.limits
    }

    pub(crate) fn cancellation(&self) -> &CancellationSource {
        &self.cancellation
    }

    pub(crate) fn observations(&self) -> &OrderedAgentObservationSink<ObservationSink> {
        &self.observations
    }
}

pub(crate) trait AgentAdapter<Sink>: Clone + Send + Sync + 'static
where
    Sink: AgentObservationSink,
{
    type NativeConfiguration: Send + Sync + 'static;
    type ProtocolLimits: Send + Sync + 'static;

    fn invoke(
        &self,
        invocation: AgentInvocation<Self::NativeConfiguration, Self::ProtocolLimits, Sink>,
        terminal: AgentTerminalCallback,
    ) -> impl Future<Output = ()> + Send;
}

pub(crate) async fn invoke_agent_adapter<Adapter, Sink>(
    adapter: &Adapter,
    invocation: AgentInvocation<Adapter::NativeConfiguration, Adapter::ProtocolLimits, Sink>,
    terminal: AgentTerminalCallback,
) where
    Adapter: AgentAdapter<Sink>,
    Sink: AgentObservationSink,
{
    let unreported_return = terminal.clone();
    adapter.invoke(invocation, terminal).await;
    let _ = unreported_return.report(AgentOutcome::Failed {
        cause: AgentFailureCause::HarnessProtocolFailed,
    });
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundedAgentResponse(Arc<str>);

impl BoundedAgentResponse {
    pub(crate) fn from_bounded(value: Arc<str>) -> Self {
        Self(value)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundedSchemaValidAgentResult {
    value: Arc<Value>,
    canonical_json: Arc<[u8]>,
}

impl BoundedSchemaValidAgentResult {
    pub(crate) fn from_validated(value: Arc<Value>, canonical_json: Arc<[u8]>) -> Self {
        Self {
            value,
            canonical_json,
        }
    }

    pub(crate) fn value(&self) -> &Value {
        &self.value
    }

    pub(crate) fn canonical_json(&self) -> &[u8] {
        &self.canonical_json
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CompletedAgentInvocation {
    NoValue,
    Response(BoundedAgentResponse),
    Result(BoundedSchemaValidAgentResult),
}

impl CompletedAgentInvocation {
    fn kind(&self) -> AgentValueKind {
        match self {
            Self::NoValue => AgentValueKind::None,
            Self::Response(_) => AgentValueKind::Response,
            Self::Result(_) => AgentValueKind::Result,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentInputKind {
    SystemPrompt,
    Message,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentHarnessFailureDetail {
    ModelOutputTruncated,
    UnexpectedTerminalToolUse,
    ModelError,
    ModelAborted,
    UnsuccessfulExit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AgentFailureCause {
    HarnessStartFailed,
    HarnessInputTooLarge {
        input: AgentInputKind,
        admitted_bytes: NonZeroU64,
        observed_bytes: u64,
    },
    HarnessFailed {
        detail: AgentHarnessFailureDetail,
    },
    HarnessProtocolFailed,
    MissingResponse,
    MissingResult,
    ResultValidationLimitExceeded {
        deadline: PositiveDuration,
    },
    CapturedValueTooLarge,
    ResultSettlementFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AgentOutcome {
    Completed(CompletedAgentInvocation),
    Failed { cause: AgentFailureCause },
    Cancelled { reason: CancellationReason },
}

#[derive(Clone)]
pub(crate) struct AgentTerminalCallback {
    state: Arc<Mutex<Option<oneshot::Sender<AgentOutcome>>>>,
    expected_value_kind: AgentValueKind,
}

impl AgentTerminalCallback {
    pub(crate) fn report(&self, outcome: AgentOutcome) -> Result<(), AgentTerminalReportError> {
        if let AgentOutcome::Completed(completed) = &outcome
            && completed.kind() != self.expected_value_kind
        {
            return Err(AgentTerminalReportError::CompletionModeMismatch);
        }

        let mut sender = match self.state.lock() {
            Ok(sender) => sender,
            Err(poisoned) => poisoned.into_inner(),
        };
        let sender = sender
            .take()
            .ok_or(AgentTerminalReportError::AlreadyReported)?;
        sender
            .send(outcome)
            .map_err(|_| AgentTerminalReportError::ReceiverClosed)
    }

    #[cfg(test)]
    fn has_reported(&self) -> bool {
        match self.state.lock() {
            Ok(sender) => sender.is_none(),
            Err(poisoned) => poisoned.into_inner().is_none(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentTerminalReportError {
    AlreadyReported,
    CompletionModeMismatch,
    ReceiverClosed,
}

pub(crate) struct AgentTerminalReceiver {
    outcome: oneshot::Receiver<AgentOutcome>,
}

impl AgentTerminalReceiver {
    pub(crate) async fn receive(self) -> Result<AgentOutcome, AgentTerminalReceiveError> {
        self.outcome
            .await
            .map_err(|_| AgentTerminalReceiveError::CallbackDropped)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentTerminalReceiveError {
    CallbackDropped,
}

pub(crate) fn agent_terminal_channel(
    value_mode: &AgentValueMode,
) -> (AgentTerminalCallback, AgentTerminalReceiver) {
    let (terminal, outcome) = oneshot::channel();
    (
        AgentTerminalCallback {
            state: Arc::new(Mutex::new(Some(terminal))),
            expected_value_kind: value_mode.kind(),
        },
        AgentTerminalReceiver { outcome },
    )
}

#[cfg(test)]
pub(crate) mod scripted;

#[cfg(test)]
mod tests;
