pub(crate) mod adapter;
mod input;

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use serde_json::{Map, Value, json};

use super::agent::{
    AgentDiagnosticLevel, AgentFailureCause, AgentHarnessFailureDetail, AgentHarnessSetupStage,
    AgentLifecycleMilestone, AgentObservation, AgentOutcome, AgentProtocolRejectionDiagnostic,
    AgentToolCallPhase, AgentValueKind, BoundedAgentResponse, CapturedJson,
    CompletedAgentInvocation,
};
use super::strict_json;

const MAXIMUM_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const MAXIMUM_CORRELATION_BYTES: u64 = 64 * 1024;
const MAXIMUM_RETAINED_AGENT_MESSAGE_BYTES: u64 = MAXIMUM_FRAME_BYTES;
const MAXIMUM_RETAINED_DIAGNOSTIC_BYTES: u64 = 64 * 1024;
const MAXIMUM_IDENTITY_BYTES: usize = 256;
const STANDARD_INPUT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const POST_FAILURE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const INITIALIZE_REQUEST_ID: RequestId = RequestId(1);
const CONFIG_READ_REQUEST_ID: RequestId = RequestId(2);
const THREAD_START_REQUEST_ID: RequestId = RequestId(3);
const TURN_START_REQUEST_ID: RequestId = RequestId(4);
const TURN_INTERRUPT_REQUEST_ID: RequestId = RequestId(5);
const CORRECTION_TURN_START_REQUEST_ID: RequestId = RequestId(6);
const MAXIMUM_CORRECTION_TURNS: u8 = 1;
const CLIENT_NAME: &str = "scherzo-cloud";
const CLIENT_VERSION: &str = crate::build_info::VERSION;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CodexAppServerV1ProtocolLimits {
    maximum_frame_bytes: NonZeroU64,
    maximum_correlation_bytes: NonZeroU64,
    maximum_retained_agent_message_bytes: NonZeroU64,
    maximum_retained_diagnostic_bytes: NonZeroU64,
    standard_input_write_timeout: Duration,
    post_failure_cleanup_timeout: Duration,
}

impl CodexAppServerV1ProtocolLimits {
    pub(crate) const fn profile() -> Self {
        let maximum_frame_bytes = match NonZeroU64::new(MAXIMUM_FRAME_BYTES) {
            Some(value) => value,
            None => NonZeroU64::MIN,
        };
        let maximum_correlation_bytes = match NonZeroU64::new(MAXIMUM_CORRELATION_BYTES) {
            Some(value) => value,
            None => NonZeroU64::MIN,
        };
        let maximum_retained_agent_message_bytes =
            match NonZeroU64::new(MAXIMUM_RETAINED_AGENT_MESSAGE_BYTES) {
                Some(value) => value,
                None => NonZeroU64::MIN,
            };
        let maximum_retained_diagnostic_bytes =
            match NonZeroU64::new(MAXIMUM_RETAINED_DIAGNOSTIC_BYTES) {
                Some(value) => value,
                None => NonZeroU64::MIN,
            };
        Self {
            maximum_frame_bytes,
            maximum_correlation_bytes,
            maximum_retained_agent_message_bytes,
            maximum_retained_diagnostic_bytes,
            standard_input_write_timeout: STANDARD_INPUT_WRITE_TIMEOUT,
            post_failure_cleanup_timeout: POST_FAILURE_CLEANUP_TIMEOUT,
        }
    }

    #[cfg(test)]
    const fn with_limits(
        maximum_frame_bytes: NonZeroU64,
        maximum_correlation_bytes: NonZeroU64,
    ) -> Self {
        Self {
            maximum_frame_bytes,
            maximum_correlation_bytes,
            maximum_retained_agent_message_bytes: maximum_frame_bytes,
            maximum_retained_diagnostic_bytes: maximum_frame_bytes,
            standard_input_write_timeout: STANDARD_INPUT_WRITE_TIMEOUT,
            post_failure_cleanup_timeout: POST_FAILURE_CLEANUP_TIMEOUT,
        }
    }

    pub(crate) const fn maximum_frame_bytes(self) -> NonZeroU64 {
        self.maximum_frame_bytes
    }

    pub(crate) const fn maximum_correlation_bytes(self) -> NonZeroU64 {
        self.maximum_correlation_bytes
    }

    pub(crate) const fn standard_input_write_timeout(self) -> Duration {
        self.standard_input_write_timeout
    }

    pub(crate) const fn post_failure_cleanup_timeout(self) -> Duration {
        self.post_failure_cleanup_timeout
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ThreadId(Arc<str>);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TurnId(Arc<str>);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ItemId(Arc<str>);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RequestId(u64);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ServerRequestId {
    Number(i64),
    String(Arc<str>),
}

impl ServerRequestId {
    fn value(&self) -> Value {
        match self {
            Self::Number(value) => Value::from(*value),
            Self::String(value) => Value::String(value.to_string()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SetupState {
    Initialize,
    ConfigRead,
    ThreadStart,
    TurnStart,
    StartAcknowledgement,
    Running,
    ResultValidation,
    Terminal,
}

impl SetupState {
    const fn protocol_stage(self) -> CodexAppServerV1ProtocolStage {
        match self {
            Self::Initialize => CodexAppServerV1ProtocolStage::Initialize,
            Self::ConfigRead => CodexAppServerV1ProtocolStage::ConfigRead,
            Self::ThreadStart => CodexAppServerV1ProtocolStage::ThreadStart,
            Self::TurnStart => CodexAppServerV1ProtocolStage::TurnStart,
            Self::StartAcknowledgement => CodexAppServerV1ProtocolStage::StartAcknowledgement,
            Self::Running => CodexAppServerV1ProtocolStage::Running,
            Self::ResultValidation => CodexAppServerV1ProtocolStage::ResultValidation,
            Self::Terminal => CodexAppServerV1ProtocolStage::Terminal,
        }
    }

    const fn failure_stage(self) -> Option<AgentHarnessSetupStage> {
        match self {
            Self::Initialize => Some(AgentHarnessSetupStage::Initialization),
            Self::ConfigRead => Some(AgentHarnessSetupStage::EffectiveConfiguration),
            Self::ThreadStart => Some(AgentHarnessSetupStage::ThreadStart),
            Self::TurnStart => Some(AgentHarnessSetupStage::TurnStart),
            Self::StartAcknowledgement => Some(AgentHarnessSetupStage::StartAcknowledgement),
            Self::Running | Self::ResultValidation | Self::Terminal => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CodexAppServerV1ProtocolRejection {
    reason: CodexAppServerV1RejectionReason,
    stage: CodexAppServerV1ProtocolStage,
    thread_established: bool,
    turn_established: bool,
    start_acknowledged: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CodexAppServerV1RejectionReason {
    FrameEmpty,
    FrameTooLarge,
    FrameDecodeFailed,
    FrameNotObject,
    JsonRpcEnvelopeUnsupported,
    FrameEnvelopeInvalid,
    ResponseEnvelopeInvalid,
    ResponseCorrelationInvalid,
    ResponseRejected,
    ResponseTransitionInvalid,
    InitializationResponseInvalid,
    ConfigurationResponseInvalid,
    EffectiveInstructionsInvalid,
    ThreadStartResponseInvalid,
    ThreadCorrelationInvalid,
    TurnStartResponseInvalid,
    TurnCorrelationInvalid,
    ServerRequestEnvelopeInvalid,
    ServerRequestRepeated,
    ServerRequestCorrelationInvalid,
    NotificationEnvelopeInvalid,
    NotificationTransitionInvalid,
    ItemCorrelationInvalid,
    ItemTransitionInvalid,
    MessageInvalid,
    UsageInvalid,
    HookCorrelationInvalid,
    HookTransitionInvalid,
    NativeErrorInvalid,
    TurnCompletionInvalid,
    TurnSummaryInvalid,
    ResultEnvelopeInvalid,
    ResultTransitionInvalid,
    IdentityInvalid,
    RetainedCorrelationLimitExceeded,
    RetainedAgentMessageLimitExceeded,
    OutboundLimitExceeded,
    OutboundTransitionInvalid,
    PartialFrameAtEndOfStream,
    TerminalInvariantInvalid,
    StandardInputWriteFailed,
    StandardInputWriteTimedOut,
    StandardInputCloseFailed,
    ObservationDeliveryFailed,
    StartAcknowledgementFailed,
    ProcessOutputReadFailed,
    ProcessWaitFailed,
    ProcessSettlementFailed,
    ResultValidatorMissing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum CodexAppServerV1ProtocolStage {
    Initialize,
    ConfigRead,
    ThreadStart,
    TurnStart,
    StartAcknowledgement,
    Running,
    ResultValidation,
    Terminal,
}

#[derive(Clone, Debug)]
struct ActiveItem {
    kind: Arc<str>,
}

#[derive(Clone, Debug)]
struct CompletedItem {
    kind: Arc<str>,
    agent_message: Option<CompletedAgentMessage>,
    value_eligible: bool,
}

#[derive(Clone, Debug)]
struct CompletedAgentMessage {
    text: Arc<str>,
    phase: Option<AgentMessagePhase>,
    delivery: Option<AgentMessageDelivery>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AgentMessagePhase {
    Commentary,
    FinalAnswer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AgentMessageDelivery {
    Async,
}

#[derive(Clone, Debug)]
struct ActiveHook {
    event: Arc<str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeTerminal {
    Completed,
    Failed(AgentHarnessFailureDetail),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CodexErrorKind {
    ContextWindowExceeded,
    SessionBudgetExceeded,
    UsageLimitExceeded,
    ServerOverloaded,
    CyberPolicy,
    InternalServerError,
    Unauthorized,
    BadRequest,
    ThreadRollbackFailed,
    SandboxError,
    Other,
    HttpConnectionFailed,
    ResponseStreamConnectionFailed,
    ResponseStreamDisconnected,
    ResponseTooManyFailedAttempts,
    ActiveTurnNotSteerable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActiveTurnKind {
    Review,
    Compact,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CodexErrorInfo {
    kind: CodexErrorKind,
    http_status_code: Option<u16>,
    active_turn_kind: Option<ActiveTurnKind>,
}

impl CodexErrorInfo {
    const fn failure_detail(self) -> AgentHarnessFailureDetail {
        match self.kind {
            CodexErrorKind::ResponseStreamDisconnected => {
                AgentHarnessFailureDetail::ModelOutputTruncated
            }
            _ => AgentHarnessFailureDetail::ModelError,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NativeErrorObservation {
    info: Option<CodexErrorInfo>,
    will_retry: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ParserProgress {
    pub(super) start_acknowledged: bool,
    pub(super) close_standard_input: bool,
}

#[derive(Clone, Debug)]
pub(super) struct CodexAppServerV1Parser {
    expected_cwd: Arc<str>,
    codex_home: Arc<str>,
    sqlite_home: Arc<str>,
    codex_version: Arc<str>,
    model: Arc<str>,
    effort: Arc<str>,
    system_prompt: Arc<str>,
    initial_input: Vec<Value>,
    synthetic_model_provider: Option<Arc<str>>,
    value_kind: AgentValueKind,
    maximum_response_bytes: NonZeroU64,
    limits: CodexAppServerV1ProtocolLimits,
    frame: Vec<u8>,
    state: SetupState,
    outstanding_request: Option<RequestId>,
    turn_start_request: RequestId,
    correction_turns_started: u8,
    invocation_start_acknowledged: bool,
    completed_requests: BTreeSet<RequestId>,
    completed_server_requests: BTreeSet<ServerRequestId>,
    outbound: VecDeque<Vec<u8>>,
    pending_outbound_bytes: u64,
    thread_id: Option<ThreadId>,
    turn_id: Option<TurnId>,
    effective_model_provider: Option<Arc<str>>,
    thread_started_seen: bool,
    turn_started_seen: bool,
    active_items: BTreeMap<ItemId, ActiveItem>,
    completed_items: BTreeMap<ItemId, CompletedItem>,
    active_hooks: BTreeMap<Arc<str>, ActiveHook>,
    retained_correlation_bytes: u64,
    retained_agent_message_bytes: u64,
    retained_diagnostic_bytes: u64,
    selected_response: Option<Arc<str>>,
    final_answer_seen: bool,
    values_enabled: bool,
    interrupt_requested: bool,
    retry_active: bool,
    native_error: Option<NativeErrorObservation>,
    truncated_provider_stream_seen: bool,
    pending_result_candidate: Option<Arc<Value>>,
    accepted_result: Option<CapturedJson>,
    native_terminal: Option<NativeTerminal>,
    active_rejection_reason: Cell<CodexAppServerV1RejectionReason>,
    rejection_reason: Cell<Option<CodexAppServerV1RejectionReason>>,
    completion_rejection_reason: Cell<Option<CodexAppServerV1RejectionReason>>,
    failure: Option<AgentFailureCause>,
    observations: Vec<AgentObservation>,
}

impl CodexAppServerV1Parser {
    #[expect(
        clippy::too_many_arguments,
        reason = "the parser retains each admitted transport boundary explicitly"
    )]
    pub(super) fn profile(
        expected_cwd: Arc<str>,
        codex_home: Arc<str>,
        sqlite_home: Arc<str>,
        codex_version: Arc<str>,
        model: Arc<str>,
        effort: Arc<str>,
        system_prompt: Arc<str>,
        initial_input: Vec<Value>,
        synthetic_model_provider: Option<Arc<str>>,
        value_kind: AgentValueKind,
        maximum_response_bytes: NonZeroU64,
        limits: CodexAppServerV1ProtocolLimits,
    ) -> Result<Self, AgentFailureCause> {
        let mut parser = Self {
            expected_cwd,
            codex_home,
            sqlite_home,
            codex_version,
            model,
            effort,
            system_prompt,
            initial_input,
            synthetic_model_provider,
            value_kind,
            maximum_response_bytes,
            limits,
            frame: Vec::new(),
            state: SetupState::Initialize,
            outstanding_request: None,
            turn_start_request: TURN_START_REQUEST_ID,
            correction_turns_started: 0,
            invocation_start_acknowledged: false,
            completed_requests: BTreeSet::new(),
            completed_server_requests: BTreeSet::new(),
            outbound: VecDeque::new(),
            pending_outbound_bytes: 0,
            thread_id: None,
            turn_id: None,
            effective_model_provider: None,
            thread_started_seen: false,
            turn_started_seen: false,
            active_items: BTreeMap::new(),
            completed_items: BTreeMap::new(),
            active_hooks: BTreeMap::new(),
            retained_correlation_bytes: 0,
            retained_agent_message_bytes: 0,
            retained_diagnostic_bytes: 0,
            selected_response: None,
            final_answer_seen: false,
            values_enabled: true,
            interrupt_requested: false,
            retry_active: false,
            native_error: None,
            truncated_provider_stream_seen: false,
            pending_result_candidate: None,
            accepted_result: None,
            native_terminal: None,
            active_rejection_reason: Cell::new(
                CodexAppServerV1RejectionReason::OutboundTransitionInvalid,
            ),
            rejection_reason: Cell::new(None),
            completion_rejection_reason: Cell::new(None),
            failure: None,
            observations: Vec::new(),
        };
        parser.queue_request(
            INITIALIZE_REQUEST_ID,
            "initialize",
            json!({
                "clientInfo": {
                    "name": CLIENT_NAME,
                    "version": CLIENT_VERSION,
                }
            }),
        )?;
        Ok(parser)
    }

    pub(super) fn take_outbound(&mut self) -> Option<Vec<u8>> {
        let frame = self.outbound.pop_front()?;
        self.pending_outbound_bytes = self
            .pending_outbound_bytes
            .saturating_sub(u64::try_from(frame.len()).unwrap_or(u64::MAX));
        Some(frame)
    }

    pub(super) fn prevent_value_commit(&mut self) {
        self.values_enabled = false;
        self.invalidate_value();
    }

    pub(super) fn request_turn_interrupt(&mut self) -> Result<bool, AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::OutboundTransitionInvalid);
        self.prevent_value_commit();
        if self.interrupt_requested
            || !matches!(
                self.state,
                SetupState::StartAcknowledgement | SetupState::Running
            )
        {
            return Ok(false);
        }
        let thread_id = self
            .thread_id
            .as_ref()
            .ok_or_else(|| self.failure_for_current_phase())?;
        let turn_id = self
            .turn_id
            .as_ref()
            .ok_or_else(|| self.failure_for_current_phase())?;
        let params = json!({
            "threadId": thread_id.0.as_ref(),
            "turnId": turn_id.0.as_ref(),
        });
        self.queue_request(TURN_INTERRUPT_REQUEST_ID, "turn/interrupt", params)?;
        self.interrupt_requested = true;
        Ok(true)
    }

    pub(super) const fn start_acknowledged(&self) -> bool {
        self.invocation_start_acknowledged
    }

    pub(super) fn take_result_candidate(&mut self) -> Option<Arc<Value>> {
        self.pending_result_candidate.take()
    }

    pub(super) fn take_observations(&mut self) -> Vec<AgentObservation> {
        self.observations.drain(..).collect()
    }

    pub(super) fn accept_result(
        &mut self,
        result: CapturedJson,
    ) -> Result<ParserProgress, AgentFailureCause> {
        if !self.result_validation_is_ready() {
            return self
                .fail_current_phase(CodexAppServerV1RejectionReason::ResultTransitionInvalid);
        }
        self.accepted_result = Some(result);
        self.completed_items.clear();
        self.state = SetupState::Terminal;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::HarnessCompleted));
        Ok(ParserProgress {
            start_acknowledged: false,
            close_standard_input: true,
        })
    }

    pub(super) fn reject_result(
        &mut self,
        feedback: Arc<str>,
    ) -> Result<ParserProgress, AgentFailureCause> {
        if !self.result_validation_is_ready() {
            return self
                .fail_current_phase(CodexAppServerV1RejectionReason::ResultTransitionInvalid);
        }
        if self.correction_turns_started >= MAXIMUM_CORRECTION_TURNS {
            self.completed_items.clear();
            self.state = SetupState::Terminal;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::HarnessCompleted));
            return Ok(ParserProgress {
                start_acknowledged: false,
                close_standard_input: true,
            });
        }

        self.correction_turns_started += 1;
        self.turn_start_request = RequestId(
            CORRECTION_TURN_START_REQUEST_ID
                .0
                .checked_add(u64::from(self.correction_turns_started - 1))
                .ok_or_else(|| self.failure_for_current_phase())?,
        );
        self.reset_completed_turn();
        self.queue_turn_start(vec![json!({
            "type": "text",
            "text": feedback.as_ref(),
        })])?;
        self.state = SetupState::TurnStart;
        Ok(ParserProgress::default())
    }

    fn result_validation_is_ready(&self) -> bool {
        self.value_kind == AgentValueKind::Result
            && self.state == SetupState::ResultValidation
            && self.pending_result_candidate.is_none()
            && self.accepted_result.is_none()
            && self.native_terminal == Some(NativeTerminal::Completed)
    }

    pub(super) fn prepare_completion_rejection(&self) {
        if self.completion_rejection_reason.get().is_none() {
            let reason = if self.frame.is_empty() {
                CodexAppServerV1RejectionReason::TerminalInvariantInvalid
            } else {
                CodexAppServerV1RejectionReason::PartialFrameAtEndOfStream
            };
            self.completion_rejection_reason.set(Some(reason));
        }
    }

    fn protocol_rejection(&self) -> AgentProtocolRejectionDiagnostic {
        let reason = self
            .rejection_reason
            .get()
            .or(self.completion_rejection_reason.get())
            .unwrap_or(CodexAppServerV1RejectionReason::TerminalInvariantInvalid);
        AgentProtocolRejectionDiagnostic::codex_app_server_v1(CodexAppServerV1ProtocolRejection {
            reason,
            stage: self.state.protocol_stage(),
            thread_established: self.thread_id.is_some(),
            turn_established: self.turn_id.is_some(),
            start_acknowledged: self.invocation_start_acknowledged,
        })
    }

    fn prepare_rejection(&self, reason: CodexAppServerV1RejectionReason) {
        self.active_rejection_reason.set(reason);
    }

    pub(super) fn record_rejection(&self, reason: CodexAppServerV1RejectionReason) {
        if self.rejection_reason.get().is_none() {
            self.rejection_reason.set(Some(reason));
        }
    }

    pub(super) fn failure_for(&self, reason: CodexAppServerV1RejectionReason) -> AgentFailureCause {
        self.record_rejection(reason);
        self.phase_failure_cause()
    }

    pub(super) fn failure_for_current_phase(&self) -> AgentFailureCause {
        self.record_rejection(self.active_rejection_reason.get());
        self.phase_failure_cause()
    }

    fn phase_failure_cause(&self) -> AgentFailureCause {
        if self.invocation_start_acknowledged {
            return AgentFailureCause::HarnessProtocolFailed;
        }
        self.state
            .failure_stage()
            .map_or(AgentFailureCause::HarnessProtocolFailed, |stage| {
                AgentFailureCause::HarnessSetupFailed { stage }
            })
    }

    pub(super) fn push_stdout(
        &mut self,
        bytes: &[u8],
        mut observe: impl FnMut(AgentObservation),
    ) -> Result<ParserProgress, AgentFailureCause> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        let mut progress = ParserProgress::default();
        for &byte in bytes {
            if byte == b'\n' {
                let frame = std::mem::take(&mut self.frame);
                match self.parse_frame(&frame) {
                    Ok(frame_progress) => {
                        progress.start_acknowledged |= frame_progress.start_acknowledged;
                        progress.close_standard_input |= frame_progress.close_standard_input;
                    }
                    Err(failure) => {
                        self.invalidate_value();
                        self.observations.clear();
                        self.failure = Some(failure.clone());
                        return Err(failure);
                    }
                }
                for observation in self.observations.drain(..) {
                    observe(observation);
                }
                continue;
            }
            let retained = u64::try_from(self.frame.len()).unwrap_or(u64::MAX);
            if retained >= self.limits.maximum_frame_bytes().get() {
                return self.fail_current_phase(CodexAppServerV1RejectionReason::FrameTooLarge);
            }
            self.frame.push(byte);
        }
        Ok(progress)
    }

    pub(super) fn finish(&mut self, exit_success: bool) -> AgentOutcome {
        if self.failure.is_none() && !self.frame.is_empty() {
            self.failure =
                Some(self.failure_for(CodexAppServerV1RejectionReason::PartialFrameAtEndOfStream));
            self.invalidate_value();
        }
        if let Some(failure) = self.failure.take() {
            return failed(failure);
        }
        if !matches!(self.state, SetupState::Terminal)
            || self.outstanding_request.is_some()
            || !self.outbound.is_empty()
            || !self.active_items.is_empty()
            || !self.active_hooks.is_empty()
        {
            return failed(
                self.failure_for(CodexAppServerV1RejectionReason::TerminalInvariantInvalid),
            );
        }
        match self.native_terminal {
            Some(NativeTerminal::Failed(detail)) => {
                return failed(AgentFailureCause::HarnessFailed { detail });
            }
            Some(NativeTerminal::Completed) => {}
            None => return failed(AgentFailureCause::HarnessProtocolFailed),
        }
        // Codex keeps native terminal/exit precedence and no-response semantics local;
        // sharing this final match would couple independent harness protocol authority.
        // jscpd:ignore-start
        if !exit_success {
            return failed(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            });
        }
        match self.value_kind {
            AgentValueKind::None => AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
            AgentValueKind::Response => self.selected_response.take().map_or_else(
                || AgentOutcome::Completed(CompletedAgentInvocation::NoResponse),
                |response| {
                    AgentOutcome::Completed(CompletedAgentInvocation::Response(
                        BoundedAgentResponse::from_bounded(response),
                    ))
                },
            ),
            AgentValueKind::Result => self.accepted_result.take().map_or_else(
                || failed(AgentFailureCause::MissingResult),
                |result| AgentOutcome::Completed(CompletedAgentInvocation::Result(result)),
            ),
        }
        // jscpd:ignore-end
    }

    fn parse_frame(&mut self, frame: &[u8]) -> Result<ParserProgress, AgentFailureCause> {
        if frame.is_empty() {
            return Err(self.failure_for(CodexAppServerV1RejectionReason::FrameEmpty));
        }
        if u64::try_from(frame.len()).unwrap_or(u64::MAX) > self.limits.maximum_frame_bytes().get()
        {
            return Err(self.failure_for(CodexAppServerV1RejectionReason::FrameTooLarge));
        }
        let value = strict_json::from_slice(frame)
            .map_err(|_| self.failure_for(CodexAppServerV1RejectionReason::FrameDecodeFailed))?;
        let object = value
            .as_object()
            .ok_or_else(|| self.failure_for(CodexAppServerV1RejectionReason::FrameNotObject))?;
        if object.contains_key("jsonrpc") {
            return Err(
                self.failure_for(CodexAppServerV1RejectionReason::JsonRpcEnvelopeUnsupported)
            );
        }
        match (object.get("id"), object.get("method")) {
            (Some(_), None) => self.parse_response(object),
            (Some(_), Some(_)) => self.parse_server_request(object, &value),
            (None, Some(_)) => self.parse_notification(object, &value),
            _ => Err(self.failure_for(CodexAppServerV1RejectionReason::FrameEnvelopeInvalid)),
        }
    }

    fn parse_response(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ParserProgress, AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::ResponseEnvelopeInvalid);
        let id = RequestId(
            object
                .get("id")
                .and_then(Value::as_u64)
                .ok_or_else(|| self.failure_for_current_phase())?,
        );
        if self.completed_requests.contains(&id) || self.outstanding_request != Some(id) {
            return Err(
                self.failure_for(CodexAppServerV1RejectionReason::ResponseCorrelationInvalid)
            );
        }
        let result = object.get("result");
        let error = object.get("error");
        if result.is_some() == error.is_some() {
            return Err(self.failure_for_current_phase());
        }
        if error.is_some() {
            return Err(self.failure_for(CodexAppServerV1RejectionReason::ResponseRejected));
        }
        let result = result
            .and_then(Value::as_object)
            .ok_or_else(|| self.failure_for_current_phase())?;
        self.completed_requests.insert(id);
        self.outstanding_request = None;
        let mut progress = ParserProgress::default();
        match id {
            INITIALIZE_REQUEST_ID if self.state == SetupState::Initialize => {
                self.parse_initialize_response(result)?;
                self.state = SetupState::ConfigRead;
                self.queue_notification("initialized", json!({}))?;
                self.queue_request(
                    CONFIG_READ_REQUEST_ID,
                    "config/read",
                    json!({
                        "cwd": self.expected_cwd.as_ref(),
                        "includeLayers": true,
                    }),
                )?;
            }
            CONFIG_READ_REQUEST_ID if self.state == SetupState::ConfigRead => {
                self.parse_config_read_response(result)?;
                let developer_instructions = self.effective_developer_instructions(result)?;
                let mut params = json!({
                    "model": self.model.as_ref(),
                    "cwd": self.expected_cwd.as_ref(),
                    "approvalPolicy": "never",
                    "sandbox": "danger-full-access",
                    "developerInstructions": developer_instructions,
                    "ephemeral": true,
                    "config": {
                        "bypass_hook_trust": true,
                    },
                });
                if let Some(provider) = &self.synthetic_model_provider {
                    params["modelProvider"] = Value::String(provider.to_string());
                }
                self.queue_request(THREAD_START_REQUEST_ID, "thread/start", params)?;
                self.state = SetupState::ThreadStart;
            }
            THREAD_START_REQUEST_ID if self.state == SetupState::ThreadStart => {
                self.parse_thread_start_response(result)?;
                self.state = SetupState::TurnStart;
                let initial_input = std::mem::take(&mut self.initial_input);
                self.queue_turn_start(initial_input)?;
            }
            request
                if request == self.turn_start_request && self.state == SetupState::TurnStart =>
            {
                self.parse_turn_start_response(result)?;
                if self.turn_started_seen {
                    progress = self.acknowledge_turn_started();
                } else {
                    self.state = SetupState::StartAcknowledgement;
                }
            }
            TURN_INTERRUPT_REQUEST_ID
                if self.interrupt_requested
                    && matches!(
                        self.state,
                        SetupState::StartAcknowledgement
                            | SetupState::Running
                            | SetupState::Terminal
                    )
                    && result.is_empty() => {}
            _ => {
                return Err(
                    self.failure_for(CodexAppServerV1RejectionReason::ResponseTransitionInvalid)
                );
            }
        }
        Ok(progress)
    }

    fn parse_server_request(
        &mut self,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<ParserProgress, AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::ServerRequestEnvelopeInvalid);
        let id = self.retain_server_request_id(object.get("id").ok_or_else(|| {
            self.failure_for(CodexAppServerV1RejectionReason::ServerRequestEnvelopeInvalid)
        })?)?;
        if self.completed_server_requests.contains(&id) {
            return Err(self.failure_for(CodexAppServerV1RejectionReason::ServerRequestRepeated));
        }
        let method = required_nonempty_string(object, "method").ok_or_else(|| {
            self.failure_for(CodexAppServerV1RejectionReason::ServerRequestEnvelopeInvalid)
        })?;
        let response = match method {
            "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval"
            | "item/tool/requestUserInput" => {
                let params = required_object(object, "params").ok_or_else(|| {
                    self.failure_for(CodexAppServerV1RejectionReason::ServerRequestEnvelopeInvalid)
                })?;
                self.require_interactive_request(params)?;
                Some(match method {
                    "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
                        json!({"decision": "decline"})
                    }
                    "item/permissions/requestApproval" => json!({"permissions": {}}),
                    "item/tool/requestUserInput" => json!({"answers": {}}),
                    _ => {
                        return Err(self.failure_for(
                            CodexAppServerV1RejectionReason::ServerRequestEnvelopeInvalid,
                        ));
                    }
                })
            }
            "mcpServer/elicitation/request" => {
                let params = required_object(object, "params").ok_or_else(|| {
                    self.failure_for(CodexAppServerV1RejectionReason::ServerRequestEnvelopeInvalid)
                })?;
                self.require_mcp_elicitation_correlation(params)?;
                Some(json!({"action": "decline"}))
            }
            _ => {
                if let Some(params) = object.get("params").and_then(Value::as_object) {
                    self.require_additive_correlation(params)?;
                }
                None
            }
        };
        if let Some(response) = response {
            self.queue_server_response(&id, response)?;
        } else {
            self.queue_server_error(&id, -32601, "Method not found")?;
        }
        self.completed_server_requests.insert(id);
        self.observe_unrecognized(value);
        Ok(ParserProgress::default())
    }

    fn require_interactive_request(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_running_correlation(params)?;
        let item_id = required_nonempty_string(params, "itemId").ok_or_else(|| {
            self.failure_for(CodexAppServerV1RejectionReason::ItemCorrelationInvalid)
        })?;
        if !self.active_items.contains_key(&ItemId(Arc::from(item_id))) {
            return Err(self.failure_for(CodexAppServerV1RejectionReason::ItemCorrelationInvalid));
        }
        Ok(())
    }

    fn require_mcp_elicitation_correlation(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if self.state != SetupState::Running {
            return Err(
                self.failure_for(CodexAppServerV1RejectionReason::ServerRequestCorrelationInvalid)
            );
        }
        self.require_thread(params)?;
        self.require_additive_correlation(params)
    }

    fn require_additive_correlation(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if let Some(thread_id) = params.get("threadId") {
            let Some(thread_id) = thread_id.as_str().filter(|id| !id.is_empty()) else {
                return Err(
                    self.failure_for(CodexAppServerV1RejectionReason::ThreadCorrelationInvalid)
                );
            };
            self.require_thread_value(thread_id)?;
        }
        if let Some(turn_id) = params.get("turnId") {
            let Some(turn_id) = turn_id.as_str().filter(|id| !id.is_empty()) else {
                return Err(
                    self.failure_for(CodexAppServerV1RejectionReason::TurnCorrelationInvalid)
                );
            };
            if self.turn_id.as_ref().map(|id| id.0.as_ref()) != Some(turn_id) {
                return Err(
                    self.failure_for(CodexAppServerV1RejectionReason::TurnCorrelationInvalid)
                );
            }
        }
        if let Some(item_id) = params.get("itemId") {
            let Some(item_id) = item_id.as_str().filter(|id| !id.is_empty()) else {
                return Err(
                    self.failure_for(CodexAppServerV1RejectionReason::ItemCorrelationInvalid)
                );
            };
            let item_id = ItemId(Arc::from(item_id));
            if !self.active_items.contains_key(&item_id)
                && !self.completed_items.contains_key(&item_id)
            {
                return Err(
                    self.failure_for(CodexAppServerV1RejectionReason::ItemCorrelationInvalid)
                );
            }
        }
        Ok(())
    }

    fn parse_initialize_response(
        &self,
        result: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::InitializationResponseInvalid);
        if !result
            .get("userAgent")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
            || required_string(result, "codexHome") != Some(self.codex_home.as_ref())
        {
            return Err(self.failure_for_current_phase());
        }
        Ok(())
    }

    fn parse_config_read_response(
        &mut self,
        result: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::ConfigurationResponseInvalid);
        let config =
            required_object(result, "config").ok_or_else(|| self.failure_for_current_phase())?;
        optional_string(config, "developer_instructions")
            .ok_or_else(|| self.failure_for_current_phase())?;
        if required_string(config, "sqlite_home") != Some(self.sqlite_home.as_ref()) {
            return Err(self.failure_for_current_phase());
        }
        let provider = optional_string(config, "model_provider")
            .ok_or_else(|| self.failure_for_current_phase())?;
        self.effective_model_provider = provider
            .filter(|provider| !provider.is_empty())
            .map(Arc::from);
        if self.synthetic_model_provider.is_some()
            && self.synthetic_model_provider.as_deref() != self.effective_model_provider.as_deref()
        {
            return Err(self.failure_for_current_phase());
        }
        if !result.get("origins").is_some_and(Value::is_object) {
            return Err(self.failure_for_current_phase());
        }
        Ok(())
    }

    fn effective_developer_instructions(
        &self,
        result: &Map<String, Value>,
    ) -> Result<String, AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::EffectiveInstructionsInvalid);
        let config =
            required_object(result, "config").ok_or_else(|| self.failure_for_current_phase())?;
        let native = optional_string(config, "developer_instructions")
            .ok_or_else(|| self.failure_for_current_phase())?
            .filter(|instructions| !instructions.is_empty());
        let system = (!self.system_prompt.is_empty()).then_some(self.system_prompt.as_ref());
        let combined = match (native, system) {
            (Some(native), Some(system)) => format!("{native}\n\n{system}"),
            (Some(native), None) => native.to_owned(),
            (None, Some(system)) => system.to_owned(),
            (None, None) => return Err(self.failure_for_current_phase()),
        };
        let combined_bytes = u64::try_from(combined.len()).unwrap_or(u64::MAX);
        if combined_bytes > self.limits.maximum_frame_bytes().get() {
            return Err(self.failure_for_current_phase());
        }
        Ok(combined)
    }

    fn parse_thread_start_response(
        &mut self,
        result: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::ThreadStartResponseInvalid);
        let thread = required_object(result, "thread").ok_or_else(|| {
            self.failure_for(CodexAppServerV1RejectionReason::ThreadStartResponseInvalid)
        })?;
        let raw_thread_id = required_nonempty_string(thread, "id")
            .filter(|id| is_codex_thread_id(id))
            .ok_or_else(|| {
                self.failure_for(CodexAppServerV1RejectionReason::ThreadCorrelationInvalid)
            })?;
        if self
            .thread_id
            .as_ref()
            .is_some_and(|started| started.0.as_ref() != raw_thread_id)
        {
            return Err(self.failure_for(CodexAppServerV1RejectionReason::ThreadCorrelationInvalid));
        }
        if self.thread_id.is_none() {
            self.thread_id = Some(self.retain_thread_id(raw_thread_id)?);
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::SessionEstablished));
        Ok(())
    }

    fn parse_turn_start_response(
        &mut self,
        result: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::TurnStartResponseInvalid);
        let turn = required_object(result, "turn").ok_or_else(|| {
            self.failure_for(CodexAppServerV1RejectionReason::TurnStartResponseInvalid)
        })?;
        let raw_turn_id = required_nonempty_string(turn, "id").ok_or_else(|| {
            self.failure_for(CodexAppServerV1RejectionReason::TurnCorrelationInvalid)
        })?;
        self.correlate_turn_id(raw_turn_id)?;
        Ok(())
    }

    fn required_notification_params<'a>(
        &self,
        object: &'a Map<String, Value>,
    ) -> Result<&'a Map<String, Value>, AgentFailureCause> {
        required_object(object, "params").ok_or_else(|| {
            self.failure_for(CodexAppServerV1RejectionReason::NotificationEnvelopeInvalid)
        })
    }

    fn parse_notification(
        &mut self,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<ParserProgress, AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::NotificationEnvelopeInvalid);
        let method = required_nonempty_string(object, "method")
            .ok_or_else(|| self.failure_for_current_phase())?;
        match method {
            "turn/started" => {
                let params = self.required_notification_params(object)?;
                self.parse_turn_started(params)
            }
            "thread/started" => {
                let params = self.required_notification_params(object)?;
                self.parse_thread_started(params)?;
                Ok(ParserProgress::default())
            }
            "item/started" => {
                let params = self.required_notification_params(object)?;
                self.parse_item_started(params, value)?;
                Ok(ParserProgress::default())
            }
            "item/completed" => {
                let params = self.required_notification_params(object)?;
                self.parse_item_completed(params, value)?;
                Ok(ParserProgress::default())
            }
            "item/agentMessage/delta" => {
                let params = self.required_notification_params(object)?;
                self.parse_item_delta(params, ItemDeltaKind::Assistant)?;
                Ok(ParserProgress::default())
            }
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                let params = self.required_notification_params(object)?;
                self.parse_item_delta(params, ItemDeltaKind::Reasoning)?;
                Ok(ParserProgress::default())
            }
            "thread/tokenUsage/updated" => {
                let params = self.required_notification_params(object)?;
                self.parse_usage(params)?;
                Ok(ParserProgress::default())
            }
            "hook/started" => {
                let params = self.required_notification_params(object)?;
                self.parse_hook_started(params)?;
                Ok(ParserProgress::default())
            }
            "hook/completed" => {
                let params = self.required_notification_params(object)?;
                self.parse_hook_completed(params)?;
                Ok(ParserProgress::default())
            }
            "mcpServer/startupStatus/updated" => {
                let recognized = match object.get("params").and_then(Value::as_object) {
                    Some(params) => self.parse_mcp_status(params)?,
                    None => false,
                };
                if !recognized {
                    self.observe_unrecognized(value);
                }
                Ok(ParserProgress::default())
            }
            "warning" => {
                let recognized = match object.get("params").and_then(Value::as_object) {
                    Some(params) => self.parse_warning(params)?,
                    None => false,
                };
                if !recognized {
                    self.observe_unrecognized(value);
                }
                Ok(ParserProgress::default())
            }
            "error" => {
                let params = self.required_notification_params(object)?;
                self.parse_native_error(params)?;
                Ok(ParserProgress::default())
            }
            "turn/completed" => {
                let params = self.required_notification_params(object)?;
                self.parse_turn_completed(params)
            }
            "project/changed" => {
                let params = self.required_notification_params(object)?;
                self.parse_project_changed(params)?;
                self.observe_unrecognized(value);
                Ok(ParserProgress::default())
            }
            "thread/project/updated" => {
                let params = self.required_notification_params(object)?;
                self.require_thread(params)?;
                self.require_additive_correlation(params)?;
                self.observe_unrecognized(value);
                Ok(ParserProgress::default())
            }
            "autoApprovalReview/strictReviewRequired" => {
                let params = self.required_notification_params(object)?;
                self.parse_strict_review_required(object, params)?;
                self.observe_unrecognized(value);
                Ok(ParserProgress::default())
            }
            "configWarning" => {
                let recognized =
                    if let Some(params) = object.get("params").and_then(Value::as_object) {
                        self.require_additive_correlation(params)?;
                        if let Some(summary) = required_nonempty_string(params, "summary") {
                            if let Some(message) = self.retain_diagnostic(summary) {
                                self.observations.push(AgentObservation::Diagnostic {
                                    level: AgentDiagnosticLevel::Warning,
                                    message,
                                });
                            }
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                if !recognized {
                    self.observe_unrecognized(value);
                }
                Ok(ParserProgress::default())
            }
            "remoteControl/status/changed" | "account/rateLimits/updated" => {
                let _params = self.required_notification_params(object)?;
                self.observe_unrecognized(value);
                Ok(ParserProgress::default())
            }
            "thread/status/changed" => {
                let params = self.required_notification_params(object)?;
                self.require_thread(params)?;
                self.observe_unrecognized(value);
                Ok(ParserProgress::default())
            }
            _ => {
                if let Some(params) = object.get("params").and_then(Value::as_object) {
                    self.require_additive_correlation(params)?;
                }
                self.observe_unrecognized(value);
                Ok(ParserProgress::default())
            }
        }
    }

    fn parse_turn_started(
        &mut self,
        params: &Map<String, Value>,
    ) -> Result<ParserProgress, AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::TurnCorrelationInvalid);
        if self.turn_started_seen
            || !matches!(
                self.state,
                SetupState::TurnStart | SetupState::StartAcknowledgement
            )
        {
            return Err(self.failure_for_current_phase());
        }
        self.require_thread(params)?;
        let turn =
            required_object(params, "turn").ok_or_else(|| self.failure_for_current_phase())?;
        let raw_turn_id =
            required_nonempty_string(turn, "id").ok_or_else(|| self.failure_for_current_phase())?;
        self.correlate_turn_id(raw_turn_id)?;
        self.turn_started_seen = true;
        if self.state == SetupState::TurnStart {
            return Ok(ParserProgress::default());
        }
        Ok(self.acknowledge_turn_started())
    }

    fn acknowledge_turn_started(&mut self) -> ParserProgress {
        self.state = SetupState::Running;
        let first_turn = !self.invocation_start_acknowledged;
        if first_turn {
            self.invocation_start_acknowledged = true;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::HarnessStarted));
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::TurnStarted));
        ParserProgress {
            start_acknowledged: first_turn,
            close_standard_input: false,
        }
    }

    fn parse_thread_started(
        &mut self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::ThreadCorrelationInvalid);
        if self.thread_started_seen
            || !matches!(
                self.state,
                SetupState::ThreadStart
                    | SetupState::TurnStart
                    | SetupState::StartAcknowledgement
                    | SetupState::Running
                    | SetupState::ResultValidation
                    | SetupState::Terminal
            )
        {
            return Err(self.failure_for_current_phase());
        }
        let thread =
            required_object(params, "thread").ok_or_else(|| self.failure_for_current_phase())?;
        let raw_thread_id = required_nonempty_string(thread, "id")
            .filter(|id| is_codex_thread_id(id))
            .ok_or_else(|| {
                self.failure_for(CodexAppServerV1RejectionReason::ThreadCorrelationInvalid)
            })?;
        self.correlate_thread_value(raw_thread_id)?;
        self.thread_started_seen = true;
        Ok(())
    }

    fn parse_item_started(
        &mut self,
        params: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::ItemTransitionInvalid);
        self.require_running_correlation(params)?;
        let (_item, raw_id, kind) = self.required_item(params)?;
        let id = self.retain_item_id(raw_id)?;
        if self.active_items.contains_key(&id) || self.completed_items.contains_key(&id) {
            return Err(self.failure_for_current_phase());
        }
        self.retain_correlation(kind.len())?;
        self.active_items.insert(
            id.clone(),
            ActiveItem {
                kind: Arc::from(kind),
            },
        );
        self.complete_retry(true);
        match kind {
            "agentMessage" => self
                .observations
                .push(lifecycle(AgentLifecycleMilestone::MessageStarted)),
            "commandExecution" | "fileChange" | "mcpToolCall" => {
                self.observations.push(AgentObservation::ToolCall {
                    call_id: Arc::clone(&id.0),
                    name: Arc::from(kind),
                    phase: AgentToolCallPhase::Started,
                });
            }
            "userMessage" | "reasoning" => {}
            _ => self.observe_unrecognized(value),
        }
        Ok(())
    }

    fn parse_item_completed(
        &mut self,
        params: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::ItemTransitionInvalid);
        self.require_running_correlation(params)?;
        let (item, raw_id, kind) = self.required_item(params)?;
        let id = ItemId(Arc::from(raw_id));
        let active = self
            .active_items
            .remove(&id)
            .ok_or_else(|| self.failure_for_current_phase())?;
        if active.kind.as_ref() != kind || self.completed_items.contains_key(&id) {
            return Err(self.failure_for_current_phase());
        }
        let (agent_message, value_eligible) = if kind == "agentMessage" {
            let message = self.parse_completed_agent_message(item)?;
            let value_eligible = message.delivery != Some(AgentMessageDelivery::Async);
            if !value_eligible {
                self.invalidate_earlier_value_candidates();
            }
            self.retain_agent_message(message.text.len())?;
            self.select_response_candidate(&message)?;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::MessageCompleted));
            (Some(message), value_eligible)
        } else {
            match kind {
                "reasoning" => self.observe_completed_reasoning(item)?,
                "commandExecution" | "fileChange" | "mcpToolCall" => {
                    self.observe_completed_tool(&id, kind, item)?;
                }
                "userMessage" => {}
                _ => self.observe_unrecognized(value),
            }
            (None, false)
        };
        self.completed_items.insert(
            id,
            CompletedItem {
                kind: active.kind,
                agent_message,
                value_eligible,
            },
        );
        Ok(())
    }

    fn parse_completed_agent_message(
        &self,
        item: &Map<String, Value>,
    ) -> Result<CompletedAgentMessage, AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::MessageInvalid);
        let text = required_string(item, "text").ok_or_else(|| self.failure_for_current_phase())?;
        let phase = match item.get("phase") {
            None | Some(Value::Null) => None,
            Some(Value::String(phase)) if phase == "commentary" => {
                Some(AgentMessagePhase::Commentary)
            }
            Some(Value::String(phase)) if phase == "final_answer" => {
                Some(AgentMessagePhase::FinalAnswer)
            }
            _ => return Err(self.failure_for_current_phase()),
        };
        let delivery = match item.get("delivery") {
            None | Some(Value::Null) => None,
            Some(Value::String(delivery)) if delivery == "async" => {
                Some(AgentMessageDelivery::Async)
            }
            _ => return Err(self.failure_for_current_phase()),
        };
        Ok(CompletedAgentMessage {
            text: Arc::from(text),
            phase,
            delivery,
        })
    }

    fn invalidate_earlier_value_candidates(&mut self) {
        for item in self.completed_items.values_mut() {
            item.value_eligible = false;
        }
        self.selected_response = None;
        self.final_answer_seen = false;
    }

    fn select_response_candidate(
        &mut self,
        message: &CompletedAgentMessage,
    ) -> Result<(), AgentFailureCause> {
        if !self.values_enabled
            || self.value_kind != AgentValueKind::Response
            || message.delivery == Some(AgentMessageDelivery::Async)
        {
            return Ok(());
        }
        if message.phase == Some(AgentMessagePhase::Commentary) {
            return Ok(());
        }
        let is_final = message.phase == Some(AgentMessagePhase::FinalAnswer);
        if self.final_answer_seen && !is_final {
            return Ok(());
        }
        let observed_bytes = u64::try_from(message.text.len()).unwrap_or(u64::MAX);
        if observed_bytes > self.maximum_response_bytes.get() {
            return Err(AgentFailureCause::CapturedValueTooLarge);
        }
        if is_final {
            self.final_answer_seen = true;
        }
        self.selected_response = (!message.text.is_empty()).then(|| Arc::clone(&message.text));
        Ok(())
    }

    fn observe_completed_reasoning(
        &mut self,
        item: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::MessageInvalid);
        for field in ["summary", "content"] {
            let values =
                required_array(item, field).ok_or_else(|| self.failure_for_current_phase())?;
            for value in values {
                let text = value
                    .as_str()
                    .ok_or_else(|| self.failure_for_current_phase())?;
                if !text.is_empty() {
                    self.observations.push(AgentObservation::Reasoning {
                        text: Arc::from(text),
                    });
                }
            }
        }
        Ok(())
    }

    fn observe_completed_tool(
        &mut self,
        id: &ItemId,
        kind: &str,
        item: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::ItemTransitionInvalid);
        let status = required_nonempty_string(item, "status")
            .ok_or_else(|| self.failure_for_current_phase())?;
        let is_error = match (kind, status) {
            ("commandExecution" | "fileChange", "completed") | ("mcpToolCall", "completed") => {
                false
            }
            ("commandExecution" | "fileChange", "failed" | "declined")
            | ("mcpToolCall", "failed") => true,
            _ => return Err(self.failure_for_current_phase()),
        };
        let content = if kind == "commandExecution" {
            optional_string(item, "aggregatedOutput")
                .ok_or_else(|| self.failure_for_current_phase())?
                .unwrap_or_default()
        } else {
            status
        };
        self.observations.push(AgentObservation::ToolCall {
            call_id: Arc::clone(&id.0),
            name: Arc::from(kind),
            phase: AgentToolCallPhase::Completed,
        });
        self.observations.push(AgentObservation::ToolResult {
            call_id: Arc::clone(&id.0),
            is_error,
            content: Arc::from(content),
        });
        Ok(())
    }

    fn parse_item_delta(
        &mut self,
        params: &Map<String, Value>,
        kind: ItemDeltaKind,
    ) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::ItemCorrelationInvalid);
        self.require_running_correlation(params)?;
        let item_id = required_nonempty_string(params, "itemId")
            .ok_or_else(|| self.failure_for_current_phase())?;
        let item = self
            .active_items
            .get(&ItemId(Arc::from(item_id)))
            .ok_or_else(|| self.failure_for_current_phase())?;
        let expected_kind = match kind {
            ItemDeltaKind::Assistant => "agentMessage",
            ItemDeltaKind::Reasoning => "reasoning",
        };
        if item.kind.as_ref() != expected_kind {
            return Err(self.failure_for_current_phase());
        }
        let delta =
            required_string(params, "delta").ok_or_else(|| self.failure_for_current_phase())?;
        if !delta.is_empty() {
            self.observations.push(match kind {
                ItemDeltaKind::Assistant => AgentObservation::AssistantText {
                    text: Arc::from(delta),
                },
                ItemDeltaKind::Reasoning => AgentObservation::Reasoning {
                    text: Arc::from(delta),
                },
            });
        }
        Ok(())
    }

    fn parse_usage(&mut self, params: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::UsageInvalid);
        self.require_running_correlation(params)?;
        let total = required_object(params, "tokenUsage")
            .and_then(|usage| required_object(usage, "total"))
            .ok_or_else(|| self.failure_for_current_phase())?;
        let input_tokens =
            required_u64(total, "inputTokens").ok_or_else(|| self.failure_for_current_phase())?;
        let output_tokens =
            required_u64(total, "outputTokens").ok_or_else(|| self.failure_for_current_phase())?;
        self.observations.push(AgentObservation::Usage {
            input_tokens,
            output_tokens,
        });
        Ok(())
    }

    fn required_hook_run<'a>(
        &self,
        params: &'a Map<String, Value>,
    ) -> Result<(&'a Map<String, Value>, &'a str, &'a str), AgentFailureCause> {
        let run = required_object(params, "run").ok_or_else(|| self.failure_for_current_phase())?;
        let id =
            required_nonempty_string(run, "id").ok_or_else(|| self.failure_for_current_phase())?;
        let event = required_nonempty_string(run, "eventName")
            .ok_or_else(|| self.failure_for_current_phase())?;
        Ok((run, id, event))
    }

    fn parse_hook_started(&mut self, params: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::HookTransitionInvalid);
        self.require_hook_correlation(params)?;
        let (_run, id, event) = self.required_hook_run(params)?;
        let id = self.retain_identity(id)?;
        let event = self.retain_identity(event)?;
        if self.active_hooks.contains_key(&id) {
            return Err(self.failure_for_current_phase());
        }
        self.active_hooks.insert(
            Arc::clone(&id),
            ActiveHook {
                event: Arc::clone(&event),
            },
        );
        self.observations.push(AgentObservation::ToolCall {
            call_id: id,
            name: Arc::from(format!("hook:{event}")),
            phase: AgentToolCallPhase::Started,
        });
        Ok(())
    }

    fn parse_hook_completed(
        &mut self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::HookTransitionInvalid);
        self.require_hook_correlation(params)?;
        let (run, id, event) = self.required_hook_run(params)?;
        let active = self
            .active_hooks
            .remove(id)
            .ok_or_else(|| self.failure_for_current_phase())?;
        if active.event.as_ref() != event {
            return Err(self.failure_for_current_phase());
        }
        let status =
            optional_string(run, "status").ok_or_else(|| self.failure_for_current_phase())?;
        if let Some(status) = status
            && !matches!(status, "completed" | "failed" | "blocked" | "stopped")
        {
            return Err(self.failure_for_current_phase());
        }
        self.observations.push(AgentObservation::ToolCall {
            call_id: Arc::from(id),
            name: Arc::from(format!("hook:{event}")),
            phase: AgentToolCallPhase::Completed,
        });
        if status.is_some_and(|status| status != "completed") {
            let message = optional_string(run, "statusMessage")
                .ok_or_else(|| self.failure_for_current_phase())?
                .unwrap_or("hook failed");
            if let Some(message) = self.retain_diagnostic(message) {
                self.observations.push(AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Error,
                    message,
                });
            }
        }
        Ok(())
    }

    fn parse_mcp_status(&mut self, params: &Map<String, Value>) -> Result<bool, AgentFailureCause> {
        self.require_additive_correlation(params)?;
        let Some(name) = required_nonempty_string(params, "name")
            .filter(|name| name.len() <= MAXIMUM_IDENTITY_BYTES)
            .filter(|name| !name.chars().any(char::is_control))
        else {
            return Ok(false);
        };
        let Some(status) = required_nonempty_string(params, "status")
            .filter(|status| matches!(*status, "starting" | "ready" | "failed" | "cancelled"))
        else {
            return Ok(false);
        };
        let Some(error) = optional_string(params, "error") else {
            return Ok(false);
        };
        let Some(failure_reason) = optional_string(params, "failureReason") else {
            return Ok(false);
        };
        if failure_reason.is_some_and(|reason| reason != "reauthenticationRequired")
            || status != "failed" && (error.is_some() || failure_reason.is_some())
        {
            return Ok(false);
        }
        let summary = format!("MCP server {name}: {status}");
        let level = if status == "failed" {
            AgentDiagnosticLevel::Error
        } else {
            AgentDiagnosticLevel::Information
        };
        let message = self.retain_diagnostic(&summary);
        if let Some(message) = message {
            self.observations
                .push(AgentObservation::Diagnostic { level, message });
        }
        if let Some(error) = error
            && let Some(message) = self.retain_diagnostic(error)
        {
            self.observations.push(AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Error,
                message,
            });
        }
        Ok(true)
    }

    fn parse_warning(&mut self, params: &Map<String, Value>) -> Result<bool, AgentFailureCause> {
        if let Some(thread_id) = params.get("threadId") {
            let Some(thread_id) = thread_id.as_str().filter(|thread_id| !thread_id.is_empty())
            else {
                return Err(
                    self.failure_for(CodexAppServerV1RejectionReason::ThreadCorrelationInvalid)
                );
            };
            self.correlate_thread_value(thread_id)?;
        }
        self.require_additive_correlation(params)?;
        let Some(message) = required_nonempty_string(params, "message") else {
            return Ok(false);
        };
        if let Some(message) = self.retain_diagnostic(message) {
            self.observations.push(AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Warning,
                message,
            });
        }
        Ok(true)
    }

    fn parse_project_changed(
        &mut self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_additive_correlation(params)
    }

    fn parse_strict_review_required(
        &self,
        notification: &Map<String, Value>,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let _ = notification;
        self.require_running_correlation(params)
    }

    fn parse_native_error(&mut self, params: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::NativeErrorInvalid);
        self.require_active_turn_correlation(params)?;
        let error =
            required_object(params, "error").ok_or_else(|| self.failure_for_current_phase())?;
        let message = required_nonempty_string(error, "message")
            .ok_or_else(|| self.failure_for_current_phase())?;
        let info = self.parse_optional_codex_error_info(error.get("codexErrorInfo"))?;
        let will_retry =
            required_bool(params, "willRetry").ok_or_else(|| self.failure_for_current_phase())?;
        self.complete_retry(false);
        if info.is_some_and(|info| info.kind == CodexErrorKind::ResponseStreamDisconnected) {
            self.truncated_provider_stream_seen = true;
        }
        self.native_error = Some(NativeErrorObservation { info, will_retry });
        if will_retry {
            self.retry_active = true;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::RetryStarted));
        }
        if let Some(message) = self.retain_diagnostic(message) {
            self.observations.push(AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Error,
                message,
            });
        }
        Ok(())
    }

    fn parse_turn_completed(
        &mut self,
        params: &Map<String, Value>,
    ) -> Result<ParserProgress, AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::TurnCompletionInvalid);
        if !matches!(
            self.state,
            SetupState::StartAcknowledgement | SetupState::Running
        ) || !self.active_items.is_empty()
            || !self.active_hooks.is_empty()
            || self.native_terminal.is_some()
        {
            return Err(self.failure_for_current_phase());
        }
        self.require_thread(params)?;
        let turn =
            required_object(params, "turn").ok_or_else(|| self.failure_for_current_phase())?;
        let status = required_nonempty_string(turn, "status")
            .ok_or_else(|| self.failure_for_current_phase())?;
        self.require_turn_object(turn, status)?;
        let terminal_info = match turn.get("error") {
            None | Some(Value::Null) => None,
            Some(Value::Object(error)) if status == "failed" => {
                let message = required_nonempty_string(error, "message")
                    .ok_or_else(|| self.failure_for_current_phase())?;
                let info = self.parse_optional_codex_error_info(error.get("codexErrorInfo"))?;
                if let Some(message) = self.retain_diagnostic(message) {
                    self.observations.push(AgentObservation::Diagnostic {
                        level: AgentDiagnosticLevel::Error,
                        message,
                    });
                }
                info
            }
            _ => return Err(self.failure_for_current_phase()),
        };
        let terminal = match status {
            "completed"
                if self.state == SetupState::Running
                    && terminal_info.is_none()
                    && (self.retry_active || self.native_error.is_none()) =>
            {
                self.complete_retry(true);
                NativeTerminal::Completed
            }
            "interrupted" if terminal_info.is_none() => {
                self.complete_retry(false);
                NativeTerminal::Failed(AgentHarnessFailureDetail::ModelAborted)
            }
            "failed" => {
                self.complete_retry(false);
                NativeTerminal::Failed(self.correlate_native_failure(terminal_info)?)
            }
            _ => return Err(self.failure_for_current_phase()),
        };
        self.correlate_turn_summary(turn)?;
        let candidate = if status == "completed" && self.value_kind == AgentValueKind::Result {
            self.extract_result_candidate()?
        } else {
            None
        };
        self.native_terminal = Some(terminal);
        if status != "completed" {
            self.invalidate_value();
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::TurnCompleted));
        if let Some(candidate) = candidate {
            self.pending_result_candidate = Some(candidate);
            self.state = SetupState::ResultValidation;
            Ok(ParserProgress::default())
        } else {
            self.state = SetupState::Terminal;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::HarnessCompleted));
            Ok(ParserProgress {
                start_acknowledged: false,
                close_standard_input: true,
            })
        }
    }

    fn correlate_turn_summary(&self, turn: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::TurnSummaryInvalid);
        let items =
            required_array(turn, "items").ok_or_else(|| self.failure_for_current_phase())?;
        let mut summary_ids = BTreeSet::new();
        for item in items {
            let item = item
                .as_object()
                .ok_or_else(|| self.failure_for_current_phase())?;
            let id = required_nonempty_string(item, "id")
                .ok_or_else(|| self.failure_for_current_phase())?;
            let kind = required_nonempty_string(item, "type")
                .ok_or_else(|| self.failure_for_current_phase())?;
            let id = ItemId(Arc::from(id));
            let completed = self
                .completed_items
                .get(&id)
                .ok_or_else(|| self.failure_for_current_phase())?;
            if !summary_ids.insert(id) || completed.kind.as_ref() != kind {
                return Err(self.failure_for_current_phase());
            }
            if let Some(message) = &completed.agent_message {
                let summary = self.parse_completed_agent_message(item)?;
                self.prepare_rejection(CodexAppServerV1RejectionReason::TurnSummaryInvalid);
                if summary.text != message.text
                    || summary.phase != message.phase
                    || summary.delivery != message.delivery
                {
                    return Err(self.failure_for_current_phase());
                }
            }
        }
        if self
            .completed_items
            .iter()
            .filter(|(_, item)| item.agent_message.is_some())
            .any(|(id, _)| !summary_ids.contains(id))
        {
            return Err(self.failure_for_current_phase());
        }
        Ok(())
    }

    fn extract_result_candidate(&self) -> Result<Option<Arc<Value>>, AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::ResultEnvelopeInvalid);
        let mut candidates = self
            .completed_items
            .values()
            .filter(|item| item.value_eligible)
            .filter_map(|item| item.agent_message.as_ref())
            .filter(|message| message.phase != Some(AgentMessagePhase::Commentary));
        let Some(candidate) = candidates.next() else {
            return Ok(None);
        };
        if candidates.next().is_some() {
            return Err(self.failure_for_current_phase());
        }
        parse_weak_result_envelope(&candidate.text)
            .map(Arc::new)
            .map(Some)
            .map_err(|()| self.failure_for_current_phase())
    }

    fn reset_completed_turn(&mut self) {
        self.turn_id = None;
        self.turn_started_seen = false;
        self.completed_items.clear();
        self.selected_response = None;
        self.final_answer_seen = false;
        self.pending_result_candidate = None;
        self.native_terminal = None;
    }

    fn required_item<'a>(
        &self,
        params: &'a Map<String, Value>,
    ) -> Result<(&'a Map<String, Value>, &'a str, &'a str), AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::ItemCorrelationInvalid);
        let item =
            required_object(params, "item").ok_or_else(|| self.failure_for_current_phase())?;
        let id =
            required_nonempty_string(item, "id").ok_or_else(|| self.failure_for_current_phase())?;
        let kind = required_nonempty_string(item, "type")
            .ok_or_else(|| self.failure_for_current_phase())?;
        Ok((item, id, kind))
    }

    fn require_thread(&self, params: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let thread_id = required_nonempty_string(params, "threadId").ok_or_else(|| {
            self.failure_for(CodexAppServerV1RejectionReason::ThreadCorrelationInvalid)
        })?;
        self.require_thread_value(thread_id)
    }

    fn require_thread_value(&self, thread_id: &str) -> Result<(), AgentFailureCause> {
        if self.thread_id.as_ref().map(|id| id.0.as_ref()) == Some(thread_id) {
            Ok(())
        } else {
            Err(self.failure_for(CodexAppServerV1RejectionReason::ThreadCorrelationInvalid))
        }
    }

    fn correlate_thread_value(&mut self, thread_id: &str) -> Result<(), AgentFailureCause> {
        if self.thread_id.is_some() {
            return self.require_thread_value(thread_id);
        }
        if self.state != SetupState::ThreadStart || !is_codex_thread_id(thread_id) {
            return Err(self.failure_for(CodexAppServerV1RejectionReason::ThreadCorrelationInvalid));
        }
        self.thread_id = Some(self.retain_thread_id(thread_id)?);
        Ok(())
    }

    fn correlate_turn_id(&mut self, turn_id: &str) -> Result<(), AgentFailureCause> {
        if let Some(expected) = &self.turn_id {
            if expected.0.as_ref() != turn_id {
                return Err(
                    self.failure_for(CodexAppServerV1RejectionReason::TurnCorrelationInvalid)
                );
            }
        } else {
            self.turn_id = Some(self.retain_turn_id(turn_id)?);
        }
        Ok(())
    }

    fn require_running_correlation(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if self.state != SetupState::Running {
            return Err(
                self.failure_for(CodexAppServerV1RejectionReason::NotificationTransitionInvalid)
            );
        }
        self.require_turn_correlation(params)
    }

    fn require_active_turn_correlation(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if !matches!(
            self.state,
            SetupState::StartAcknowledgement | SetupState::Running
        ) {
            return Err(
                self.failure_for(CodexAppServerV1RejectionReason::NotificationTransitionInvalid)
            );
        }
        self.require_turn_correlation(params)
    }

    fn require_turn_correlation(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_thread(params)?;
        let turn_id = required_nonempty_string(params, "turnId").ok_or_else(|| {
            self.failure_for(CodexAppServerV1RejectionReason::TurnCorrelationInvalid)
        })?;
        if self.turn_id.as_ref().map(|id| id.0.as_ref()) != Some(turn_id) {
            return Err(self.failure_for(CodexAppServerV1RejectionReason::TurnCorrelationInvalid));
        }
        Ok(())
    }

    fn require_hook_correlation(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if !matches!(
            self.state,
            SetupState::TurnStart | SetupState::StartAcknowledgement | SetupState::Running
        ) {
            return Err(self.failure_for(CodexAppServerV1RejectionReason::HookCorrelationInvalid));
        }
        self.require_thread(params)?;
        match optional_string(params, "turnId").ok_or_else(|| {
            self.failure_for(CodexAppServerV1RejectionReason::HookCorrelationInvalid)
        })? {
            Some(turn_id) => {
                if self.turn_id.as_ref().map(|id| id.0.as_ref()) != Some(turn_id) {
                    return Err(
                        self.failure_for(CodexAppServerV1RejectionReason::HookCorrelationInvalid)
                    );
                }
            }
            None if !matches!(
                self.state,
                SetupState::TurnStart | SetupState::StartAcknowledgement
            ) =>
            {
                return Err(
                    self.failure_for(CodexAppServerV1RejectionReason::HookCorrelationInvalid)
                );
            }
            None => {}
        }
        Ok(())
    }

    fn require_turn_object(
        &self,
        turn: &Map<String, Value>,
        expected_status: &str,
    ) -> Result<(), AgentFailureCause> {
        let expected = self
            .turn_id
            .as_ref()
            .ok_or_else(|| self.failure_for_current_phase())?;
        if required_string(turn, "id") != Some(expected.0.as_ref())
            || required_string(turn, "status") != Some(expected_status)
            || required_array(turn, "items").is_none()
        {
            return Err(self.failure_for_current_phase());
        }
        Ok(())
    }

    fn parse_optional_codex_error_info(
        &self,
        value: Option<&Value>,
    ) -> Result<Option<CodexErrorInfo>, AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::NativeErrorInvalid);
        let Some(value) = value.filter(|value| !value.is_null()) else {
            return Ok(None);
        };
        let kind = if let Some(value) = value.as_str() {
            let kind = match value {
                "contextWindowExceeded" => CodexErrorKind::ContextWindowExceeded,
                "sessionBudgetExceeded" => CodexErrorKind::SessionBudgetExceeded,
                "usageLimitExceeded" => CodexErrorKind::UsageLimitExceeded,
                "serverOverloaded" => CodexErrorKind::ServerOverloaded,
                "cyberPolicy" => CodexErrorKind::CyberPolicy,
                "internalServerError" => CodexErrorKind::InternalServerError,
                "unauthorized" => CodexErrorKind::Unauthorized,
                "badRequest" => CodexErrorKind::BadRequest,
                "threadRollbackFailed" => CodexErrorKind::ThreadRollbackFailed,
                "sandboxError" => CodexErrorKind::SandboxError,
                "other" => CodexErrorKind::Other,
                _ => return Err(self.failure_for_current_phase()),
            };
            return Ok(Some(CodexErrorInfo {
                kind,
                http_status_code: None,
                active_turn_kind: None,
            }));
        } else {
            value
                .as_object()
                .ok_or_else(|| self.failure_for_current_phase())?
        };
        if kind.len() != 1 {
            return Err(self.failure_for_current_phase());
        }
        let (name, detail) = kind
            .iter()
            .next()
            .ok_or_else(|| self.failure_for_current_phase())?;
        let detail = detail
            .as_object()
            .ok_or_else(|| self.failure_for_current_phase())?;
        if name == "activeTurnNotSteerable" {
            if !has_exact_fields(detail, &["turnKind"])
                || !matches!(
                    required_string(detail, "turnKind"),
                    Some("review" | "compact")
                )
            {
                return Err(self.failure_for_current_phase());
            }
            let active_turn_kind = match required_string(detail, "turnKind") {
                Some("review") => ActiveTurnKind::Review,
                Some("compact") => ActiveTurnKind::Compact,
                _ => return Err(self.failure_for_current_phase()),
            };
            return Ok(Some(CodexErrorInfo {
                kind: CodexErrorKind::ActiveTurnNotSteerable,
                http_status_code: None,
                active_turn_kind: Some(active_turn_kind),
            }));
        }
        if !has_exact_fields(detail, &[]) && !has_exact_fields(detail, &["httpStatusCode"]) {
            return Err(self.failure_for_current_phase());
        }
        let http_status_code = match detail.get("httpStatusCode") {
            None | Some(Value::Null) => None,
            Some(value) => Some(
                value
                    .as_u64()
                    .and_then(|value| u16::try_from(value).ok())
                    .ok_or_else(|| self.failure_for_current_phase())?,
            ),
        };
        let kind = match name.as_str() {
            "httpConnectionFailed" => CodexErrorKind::HttpConnectionFailed,
            "responseStreamConnectionFailed" => CodexErrorKind::ResponseStreamConnectionFailed,
            "responseStreamDisconnected" => CodexErrorKind::ResponseStreamDisconnected,
            "responseTooManyFailedAttempts" => CodexErrorKind::ResponseTooManyFailedAttempts,
            _ => return Err(self.failure_for_current_phase()),
        };
        Ok(Some(CodexErrorInfo {
            kind,
            http_status_code,
            active_turn_kind: None,
        }))
    }

    fn correlate_native_failure(
        &self,
        terminal: Option<CodexErrorInfo>,
    ) -> Result<AgentHarnessFailureDetail, AgentFailureCause> {
        self.prepare_rejection(CodexAppServerV1RejectionReason::NativeErrorInvalid);
        let native = self.native_error.and_then(|error| error.info);
        if let (Some(native), Some(terminal)) = (native, terminal)
            && native != terminal
            && !(terminal.kind == CodexErrorKind::ResponseTooManyFailedAttempts
                && self.native_error.is_some_and(|error| error.will_retry))
        {
            return Err(self.failure_for_current_phase());
        }
        if terminal.is_none() && self.native_error.is_some_and(|error| error.will_retry) {
            return Err(self.failure_for_current_phase());
        }
        let selected = terminal.or(native);
        if selected.is_some_and(|info| {
            info.kind == CodexErrorKind::ResponseStreamDisconnected
                || info.kind == CodexErrorKind::ResponseTooManyFailedAttempts
                    && self.truncated_provider_stream_seen
        }) {
            Ok(AgentHarnessFailureDetail::ModelOutputTruncated)
        } else {
            Ok(selected.map_or(
                AgentHarnessFailureDetail::ModelError,
                CodexErrorInfo::failure_detail,
            ))
        }
    }

    fn complete_retry(&mut self, recovered: bool) {
        let retry_was_active = self.retry_active;
        if retry_was_active {
            self.retry_active = false;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::RetryCompleted));
        }
        if recovered && retry_was_active {
            self.native_error = None;
            self.truncated_provider_stream_seen = false;
        }
    }

    fn retain_server_request_id(
        &mut self,
        value: &Value,
    ) -> Result<ServerRequestId, AgentFailureCause> {
        match value {
            Value::String(value) => self.retain_identity(value).map(ServerRequestId::String),
            Value::Number(value) => {
                let value = value
                    .as_i64()
                    .ok_or_else(|| self.failure_for_current_phase())?;
                self.retain_correlation(std::mem::size_of::<i64>())?;
                Ok(ServerRequestId::Number(value))
            }
            _ => Err(self.failure_for_current_phase()),
        }
    }

    fn retain_thread_id(&mut self, raw: &str) -> Result<ThreadId, AgentFailureCause> {
        self.retain_identity(raw).map(ThreadId)
    }

    fn retain_turn_id(&mut self, raw: &str) -> Result<TurnId, AgentFailureCause> {
        self.retain_identity(raw).map(TurnId)
    }

    fn retain_item_id(&mut self, raw: &str) -> Result<ItemId, AgentFailureCause> {
        self.retain_identity(raw).map(ItemId)
    }

    fn retain_identity(&mut self, raw: &str) -> Result<Arc<str>, AgentFailureCause> {
        if raw.is_empty() || raw.len() > MAXIMUM_IDENTITY_BYTES || raw.chars().any(char::is_control)
        {
            return Err(self.failure_for(CodexAppServerV1RejectionReason::IdentityInvalid));
        }
        self.retain_correlation(raw.len())?;
        Ok(Arc::from(raw))
    }

    fn retain_correlation(&mut self, bytes: usize) -> Result<(), AgentFailureCause> {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.retained_correlation_bytes = self
            .retained_correlation_bytes
            .checked_add(bytes)
            .filter(|retained| *retained <= self.limits.maximum_correlation_bytes().get())
            .ok_or_else(|| {
                self.failure_for(CodexAppServerV1RejectionReason::RetainedCorrelationLimitExceeded)
            })?;
        Ok(())
    }

    fn retain_agent_message(&mut self, bytes: usize) -> Result<(), AgentFailureCause> {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.retained_agent_message_bytes = self
            .retained_agent_message_bytes
            .checked_add(bytes)
            .filter(|retained| *retained <= self.limits.maximum_retained_agent_message_bytes.get())
            .ok_or_else(|| {
                self.failure_for(CodexAppServerV1RejectionReason::RetainedAgentMessageLimitExceeded)
            })?;
        Ok(())
    }

    fn retain_diagnostic(&mut self, message: &str) -> Option<Arc<str>> {
        let remaining = self
            .limits
            .maximum_retained_diagnostic_bytes
            .get()
            .saturating_sub(self.retained_diagnostic_bytes);
        let remaining = usize::try_from(remaining).unwrap_or(usize::MAX);
        let (message, _) = content_safe_diagnostic(message, remaining);
        if message.is_empty() {
            return None;
        }
        let bytes = u64::try_from(message.len()).ok()?;
        self.retained_diagnostic_bytes = self.retained_diagnostic_bytes.checked_add(bytes)?;
        Some(Arc::from(message))
    }

    fn queue_turn_start(&mut self, input: Vec<Value>) -> Result<(), AgentFailureCause> {
        let thread_id = self
            .thread_id
            .as_ref()
            .ok_or_else(|| self.failure_for_current_phase())?;
        let mut params = json!({
            "threadId": thread_id.0.as_ref(),
            "input": input,
            "cwd": self.expected_cwd.as_ref(),
            "approvalPolicy": "never",
            "sandboxPolicy": {
                "type": "externalSandbox",
                "networkAccess": "enabled",
            },
            "model": self.model.as_ref(),
            "effort": self.effort.as_ref(),
        });
        if self.value_kind == AgentValueKind::Result {
            params["outputSchema"] = weak_json_schema();
        }
        self.queue_request(self.turn_start_request, "turn/start", params)
    }
    fn queue_request(
        &mut self,
        id: RequestId,
        method: &str,
        params: Value,
    ) -> Result<(), AgentFailureCause> {
        if self.outstanding_request.is_some() || self.completed_requests.contains(&id) {
            return Err(self.failure_for_current_phase());
        }
        let frame = framed_json(&json!({"id": id.0, "method": method, "params": params}))?;
        self.queue_frame(frame);
        self.outstanding_request = Some(id);
        Ok(())
    }

    fn queue_notification(&mut self, method: &str, params: Value) -> Result<(), AgentFailureCause> {
        let frame = framed_json(&json!({"method": method, "params": params}))?;
        self.queue_frame(frame);
        Ok(())
    }

    fn queue_server_response(
        &mut self,
        id: &ServerRequestId,
        result: Value,
    ) -> Result<(), AgentFailureCause> {
        self.queue_bounded_server_frame(json!({"id": id.value(), "result": result}))
    }

    fn queue_server_error(
        &mut self,
        id: &ServerRequestId,
        code: i64,
        message: &str,
    ) -> Result<(), AgentFailureCause> {
        self.queue_bounded_server_frame(json!({
            "id": id.value(),
            "error": {"code": code, "message": message},
        }))
    }

    fn queue_bounded_server_frame(&mut self, value: Value) -> Result<(), AgentFailureCause> {
        let frame = framed_json(&value)?;
        let frame_bytes = u64::try_from(frame.len()).unwrap_or(u64::MAX);
        let payload_bytes = frame_bytes.saturating_sub(1);
        if payload_bytes > self.limits.maximum_frame_bytes().get()
            || self
                .pending_outbound_bytes
                .checked_add(frame_bytes)
                .is_none_or(|pending| {
                    pending > self.limits.maximum_frame_bytes().get().saturating_add(1)
                })
        {
            return Err(self.failure_for(CodexAppServerV1RejectionReason::OutboundLimitExceeded));
        }
        self.queue_frame(frame);
        Ok(())
    }

    fn queue_frame(&mut self, frame: Vec<u8>) {
        // Trusted generated requests may exceed the inbound native-frame limit when they
        // contain valid Workflow V1 attachments. Untrusted server-triggered responses are
        // bounded separately before reaching this queue.
        self.pending_outbound_bytes = self
            .pending_outbound_bytes
            .saturating_add(u64::try_from(frame.len()).unwrap_or(u64::MAX));
        self.outbound.push_back(frame);
    }

    // Codex owns additive-event retention and provisional-value invalidation locally so
    // another harness cannot silently change this profile's protocol authority.
    // jscpd:ignore-start
    fn observe_unrecognized(&mut self, value: &Value) {
        self.observations
            .push(AgentObservation::UnrecognizedHarnessEvent {
                event: Arc::new(value.clone()),
            });
    }

    fn invalidate_value(&mut self) {
        self.selected_response = None;
        self.pending_result_candidate = None;
        self.accepted_result = None;
    }

    fn fail_current_phase<T>(
        &mut self,
        reason: CodexAppServerV1RejectionReason,
    ) -> Result<T, AgentFailureCause> {
        let failure = self.failure_for(reason);
        self.invalidate_value();
        self.failure = Some(failure.clone());
        Err(failure)
    }
    // jscpd:ignore-end
}

#[derive(Clone, Copy)]
enum ItemDeltaKind {
    Assistant,
    Reasoning,
}

fn content_safe_diagnostic(message: &str, maximum_bytes: usize) -> (String, bool) {
    let mut safe = String::with_capacity(message.len().min(maximum_bytes));
    for character in message.chars() {
        if character.is_control() {
            let escaped = character.escape_default().to_string();
            if safe.len().saturating_add(escaped.len()) > maximum_bytes {
                return (safe, true);
            }
            safe.push_str(&escaped);
        } else {
            if safe.len().saturating_add(character.len_utf8()) > maximum_bytes {
                return (safe, true);
            }
            safe.push(character);
        }
    }
    (safe, false)
}

struct WeakResultEnvelope(String);

impl<'de> Deserialize<'de> for WeakResultEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(WeakResultEnvelopeVisitor)
    }
}

struct WeakResultEnvelopeVisitor;

impl<'de> Visitor<'de> for WeakResultEnvelopeVisitor {
    type Value = WeakResultEnvelope;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("one structured-result envelope")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut result = None;
        while let Some(key) = map.next_key::<String>()? {
            if key != "result" {
                return Err(de::Error::unknown_field(&key, &["result"]));
            }
            if result.is_some() {
                return Err(de::Error::duplicate_field("result"));
            }
            result = Some(map.next_value::<String>()?);
        }
        result
            .map(WeakResultEnvelope)
            .ok_or_else(|| de::Error::missing_field("result"))
    }
}

fn parse_weak_result_envelope(text: &str) -> Result<Value, ()> {
    let envelope = serde_json::from_str::<WeakResultEnvelope>(text).map_err(|_| ())?;
    strict_json::from_str(&envelope.0).map_err(|_| ())
}

fn weak_json_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "result": {
                "type": "string",
                "description": "JSON-encode the structured workflow result as one string.",
            }
        },
        "required": ["result"],
        "additionalProperties": false,
    })
}
fn framed_json(value: &Value) -> Result<Vec<u8>, AgentFailureCause> {
    let mut bytes =
        serde_json::to_vec(value).map_err(|_| AgentFailureCause::HarnessProtocolFailed)?;
    bytes.push(b'\n');
    Ok(bytes)
}

// App Server's admitted field shapes remain in its private parser rather than coupling
// native schema evolution to the Pi or Claude transport parsers.
// jscpd:ignore-start
fn required_object<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Option<&'a Map<String, Value>> {
    object.get(key)?.as_object()
}

fn has_exact_fields(object: &Map<String, Value>, expected: &[&str]) -> bool {
    object.len() == expected.len() && expected.iter().all(|field| object.contains_key(*field))
}

fn required_array<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a [Value]> {
    object.get(key)?.as_array().map(Vec::as_slice)
}

fn is_codex_thread_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase(),
        })
        && bytes[14] == b'7'
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b')
}

fn required_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key)?.as_str()
}

fn required_nonempty_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    required_string(object, key).filter(|value| !value.is_empty())
}

fn optional_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<Option<&'a str>> {
    match object.get(key) {
        None | Some(Value::Null) => Some(None),
        Some(Value::String(value)) => Some(Some(value)),
        _ => None,
    }
}

fn required_bool(object: &Map<String, Value>, key: &str) -> Option<bool> {
    object.get(key)?.as_bool()
}

fn required_u64(object: &Map<String, Value>, key: &str) -> Option<u64> {
    object.get(key)?.as_u64()
}
// jscpd:ignore-end

// These small constructors preserve Codex-specific terminal and observation authority
// without introducing a shared native-parser result layer.
// jscpd:ignore-start
fn lifecycle(milestone: AgentLifecycleMilestone) -> AgentObservation {
    AgentObservation::Lifecycle { milestone }
}

fn failed(cause: AgentFailureCause) -> AgentOutcome {
    AgentOutcome::Failed(cause.into())
}
// jscpd:ignore-end

#[cfg(test)]
mod tests;

#[cfg(test)]
mod adapter_tests;
