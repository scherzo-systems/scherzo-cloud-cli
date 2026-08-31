pub(crate) mod adapter;
mod input;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU64;
use std::sync::Arc;

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
        }
    }

    pub(crate) const fn maximum_frame_bytes(self) -> NonZeroU64 {
        self.maximum_frame_bytes
    }

    pub(crate) const fn maximum_correlation_bytes(self) -> NonZeroU64 {
        self.maximum_correlation_bytes
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
enum CodexAppServerV1RejectionReason {
    ProtocolInvariantInvalid,
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
            return self.fail_current_phase();
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
            return self.fail_current_phase();
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

    fn protocol_rejection(&self) -> AgentProtocolRejectionDiagnostic {
        AgentProtocolRejectionDiagnostic::codex_app_server_v1(CodexAppServerV1ProtocolRejection {
            reason: CodexAppServerV1RejectionReason::ProtocolInvariantInvalid,
            stage: self.state.protocol_stage(),
            thread_established: self.thread_id.is_some(),
            turn_established: self.turn_id.is_some(),
            start_acknowledged: self.invocation_start_acknowledged,
        })
    }

    pub(super) fn failure_for_current_phase(&self) -> AgentFailureCause {
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
                return self.fail_current_phase();
            }
            self.frame.push(byte);
        }
        Ok(progress)
    }

    pub(super) fn finish(mut self, exit_success: bool) -> AgentOutcome {
        if self.failure.is_none() && !self.frame.is_empty() {
            self.failure = Some(self.failure_for_current_phase());
            self.invalidate_value();
        }
        if let Some(failure) = self.failure {
            return failed(failure);
        }
        if !matches!(self.state, SetupState::Terminal)
            || self.outstanding_request.is_some()
            || !self.outbound.is_empty()
            || !self.active_items.is_empty()
            || !self.active_hooks.is_empty()
        {
            return failed(self.failure_for_current_phase());
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
            AgentValueKind::Response => self.selected_response.map_or_else(
                || AgentOutcome::Completed(CompletedAgentInvocation::NoResponse),
                |response| {
                    AgentOutcome::Completed(CompletedAgentInvocation::Response(
                        BoundedAgentResponse::from_bounded(response),
                    ))
                },
            ),
            AgentValueKind::Result => self.accepted_result.map_or_else(
                || failed(AgentFailureCause::MissingResult),
                |result| AgentOutcome::Completed(CompletedAgentInvocation::Result(result)),
            ),
        }
        // jscpd:ignore-end
    }

    fn parse_frame(&mut self, frame: &[u8]) -> Result<ParserProgress, AgentFailureCause> {
        if frame.is_empty()
            || u64::try_from(frame.len()).unwrap_or(u64::MAX)
                > self.limits.maximum_frame_bytes().get()
        {
            return Err(self.failure_for_current_phase());
        }
        let value = strict_json::from_slice(frame).map_err(|_| self.failure_for_current_phase())?;
        let object = value
            .as_object()
            .ok_or_else(|| self.failure_for_current_phase())?;
        if object.contains_key("jsonrpc") {
            return Err(self.failure_for_current_phase());
        }
        match (object.get("id"), object.get("method")) {
            (Some(_), None)
                if has_exact_fields(object, &["id", "result"])
                    || has_exact_fields(object, &["error", "id"]) =>
            {
                self.parse_response(object)
            }
            (Some(_), Some(_)) if has_exact_fields(object, &["id", "method", "params"]) => {
                self.parse_server_request(object)
            }
            (None, Some(_)) if has_notification_fields(object) => {
                self.parse_notification(object, &value)
            }
            _ => Err(self.failure_for_current_phase()),
        }
    }

    fn parse_response(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ParserProgress, AgentFailureCause> {
        let id = RequestId(
            object
                .get("id")
                .and_then(Value::as_u64)
                .ok_or_else(|| self.failure_for_current_phase())?,
        );
        if self.completed_requests.contains(&id) || self.outstanding_request != Some(id) {
            return Err(self.failure_for_current_phase());
        }
        let result = object.get("result");
        let error = object.get("error");
        if result.is_some() == error.is_some() {
            return Err(self.failure_for_current_phase());
        }
        if error.is_some() {
            return Err(self.failure_for_current_phase());
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
            _ => return Err(self.failure_for_current_phase()),
        }
        Ok(progress)
    }

    fn parse_server_request(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ParserProgress, AgentFailureCause> {
        let id = self.retain_server_request_id(
            object
                .get("id")
                .ok_or_else(|| self.failure_for_current_phase())?,
        )?;
        if self.completed_server_requests.contains(&id) {
            return Err(self.failure_for_current_phase());
        }
        let method = required_nonempty_string(object, "method")
            .ok_or_else(|| self.failure_for_current_phase())?;
        let params =
            required_object(object, "params").ok_or_else(|| self.failure_for_current_phase())?;
        let response = match method {
            "item/commandExecution/requestApproval" => {
                self.require_command_approval(params)?;
                json!({"decision": "decline"})
            }
            "item/fileChange/requestApproval" => {
                self.require_file_change_approval(params)?;
                json!({"decision": "decline"})
            }
            "item/permissions/requestApproval" => {
                self.require_permissions_approval(params)?;
                json!({"permissions": {}})
            }
            "item/tool/requestUserInput" => {
                self.require_user_input_request(params)?;
                json!({"answers": {}})
            }
            "mcpServer/elicitation/request" => {
                self.require_mcp_elicitation(params)?;
                json!({"action": "decline"})
            }
            _ => {
                let _ = self.request_turn_interrupt()?;
                return Err(self.failure_for_current_phase());
            }
        };
        self.queue_server_response(&id, response)?;
        self.completed_server_requests.insert(id);
        Ok(ParserProgress::default())
    }

    fn require_interactive_request(
        &self,
        params: &Map<String, Value>,
        expected_kind: Option<&str>,
    ) -> Result<(), AgentFailureCause> {
        self.require_running_correlation(params)?;
        let item_id = required_nonempty_string(params, "itemId")
            .filter(|id| id.len() <= MAXIMUM_IDENTITY_BYTES && !id.chars().any(char::is_control))
            .ok_or_else(|| self.failure_for_current_phase())?;
        let item = self
            .active_items
            .get(&ItemId(Arc::from(item_id)))
            .ok_or_else(|| self.failure_for_current_phase())?;
        if expected_kind.is_some_and(|expected| item.kind.as_ref() != expected) {
            return Err(self.failure_for_current_phase());
        }
        Ok(())
    }

    fn require_command_approval(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_interactive_request(params, Some("commandExecution"))?;
        self.require_server_request_shape(
            required_i64(params, "startedAtMs").is_some()
                && optional_null_or(params, "approvalId", Value::is_string)
                && optional_null_or(params, "command", Value::is_string)
                && optional_null_or(params, "commandActions", is_command_action_array)
                && optional_null_or(params, "cwd", Value::is_string)
                && optional_null_or(params, "environmentId", Value::is_string)
                && optional_null_or(
                    params,
                    "networkApprovalContext",
                    is_network_approval_context,
                )
                && optional_null_or(params, "proposedExecpolicyAmendment", is_string_array)
                && optional_null_or(
                    params,
                    "proposedNetworkPolicyAmendments",
                    is_network_policy_amendment_array,
                )
                && optional_null_or(params, "reason", Value::is_string),
        )
    }

    fn require_file_change_approval(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_interactive_request(params, Some("fileChange"))?;
        self.require_server_request_shape(
            required_i64(params, "startedAtMs").is_some()
                && optional_null_or(params, "grantRoot", Value::is_string)
                && optional_null_or(params, "reason", Value::is_string),
        )
    }

    fn require_permissions_approval(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_interactive_request(params, None)?;
        self.require_server_request_shape(
            required_i64(params, "startedAtMs").is_some()
                && required_string(params, "cwd").is_some_and(|cwd| cwd.starts_with('/'))
                && params.get("permissions").is_some_and(is_permission_profile)
                && optional_null_or(params, "environmentId", Value::is_string)
                && optional_null_or(params, "reason", Value::is_string),
        )
    }

    fn require_user_input_request(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_interactive_request(params, None)?;
        self.require_server_request_shape(
            required_bool(params, "isBlocking").is_some()
                && required_array(params, "questions")
                    .is_some_and(|questions| questions.iter().all(is_user_input_question))
                && optional_null_or(params, "autoResolutionMs", Value::is_u64),
        )
    }

    fn require_server_request_shape(&self, valid: bool) -> Result<(), AgentFailureCause> {
        valid
            .then_some(())
            .ok_or_else(|| self.failure_for_current_phase())
    }

    fn require_mcp_elicitation(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if self.state != SetupState::Running
            || required_nonempty_string(params, "serverName").is_none()
        {
            return Err(self.failure_for_current_phase());
        }
        self.require_thread(params)?;
        match optional_string(params, "turnId").ok_or_else(|| self.failure_for_current_phase())? {
            Some(turn_id) if self.turn_id.as_ref().map(|id| id.0.as_ref()) == Some(turn_id) => {}
            None => {}
            Some(_) => return Err(self.failure_for_current_phase()),
        }
        let valid = match required_string(params, "mode") {
            Some("form") => {
                required_string(params, "message").is_some()
                    && params
                        .get("requestedSchema")
                        .is_some_and(is_mcp_elicitation_schema)
            }
            Some("openai/form") => {
                required_string(params, "message").is_some()
                    && params.contains_key("requestedSchema")
            }
            Some("url") => {
                required_string(params, "elicitationId").is_some()
                    && required_string(params, "message").is_some()
                    && required_string(params, "url").is_some()
            }
            _ => false,
        };
        if !valid {
            return Err(self.failure_for_current_phase());
        }
        Ok(())
    }

    fn parse_initialize_response(
        &self,
        result: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
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
        let thread =
            required_object(result, "thread").ok_or_else(|| self.failure_for_current_phase())?;
        let raw_thread_id = required_nonempty_string(thread, "id")
            .filter(|id| is_codex_thread_id(id))
            .ok_or_else(|| self.failure_for_current_phase())?;
        if self
            .thread_id
            .as_ref()
            .is_some_and(|started| started.0.as_ref() != raw_thread_id)
        {
            return Err(self.failure_for_current_phase());
        }
        let provider = required_nonempty_string(result, "modelProvider")
            .ok_or_else(|| self.failure_for_current_phase())?;
        let sandbox =
            required_object(result, "sandbox").ok_or_else(|| self.failure_for_current_phase())?;
        if required_string(result, "model") != Some(self.model.as_ref())
            || required_string(result, "cwd") != Some(self.expected_cwd.as_ref())
            || required_string(result, "approvalPolicy") != Some("never")
            || required_string(sandbox, "type") != Some("dangerFullAccess")
            || required_bool(thread, "ephemeral") != Some(true)
            || optional_string(thread, "path") != Some(None)
            || required_string(thread, "sessionId") != Some(raw_thread_id)
            || optional_string(thread, "forkedFromId") != Some(None)
            || optional_string(thread, "parentThreadId") != Some(None)
            || required_string(thread, "cliVersion") != Some(self.codex_version.as_ref())
            || optional_string(thread, "projectId") != Some(None)
            || !required_array(thread, "turns").is_some_and(<[_]>::is_empty)
            || required_string(thread, "cwd") != Some(self.expected_cwd.as_ref())
            || required_string(thread, "modelProvider") != Some(provider)
            || self
                .effective_model_provider
                .as_deref()
                .is_some_and(|effective| effective != provider)
        {
            return Err(self.failure_for_current_phase());
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
        let turn =
            required_object(result, "turn").ok_or_else(|| self.failure_for_current_phase())?;
        let raw_turn_id =
            required_nonempty_string(turn, "id").ok_or_else(|| self.failure_for_current_phase())?;
        if let Some(started_turn_id) = &self.turn_id {
            if started_turn_id.0.as_ref() != raw_turn_id {
                return Err(self.failure_for_current_phase());
            }
        } else {
            self.turn_id = Some(self.retain_turn_id(raw_turn_id)?);
        }
        if required_string(turn, "status") != Some("inProgress")
            || !required_array(turn, "items").is_some_and(<[_]>::is_empty)
        {
            return Err(self.failure_for_current_phase());
        }
        Ok(())
    }

    fn parse_notification(
        &mut self,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<ParserProgress, AgentFailureCause> {
        let method = required_nonempty_string(object, "method")
            .ok_or_else(|| self.failure_for_current_phase())?;
        let params =
            required_object(object, "params").ok_or_else(|| self.failure_for_current_phase())?;
        match method {
            "turn/started" => self.parse_turn_started(params),
            "thread/started" => {
                self.parse_thread_started(params)?;
                Ok(ParserProgress::default())
            }
            "item/started" => {
                self.parse_item_started(params, value)?;
                Ok(ParserProgress::default())
            }
            "item/completed" => {
                self.parse_item_completed(params, value)?;
                Ok(ParserProgress::default())
            }
            "item/agentMessage/delta" => {
                self.parse_item_delta(params, ItemDeltaKind::Assistant)?;
                Ok(ParserProgress::default())
            }
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                self.parse_item_delta(params, ItemDeltaKind::Reasoning)?;
                Ok(ParserProgress::default())
            }
            "thread/tokenUsage/updated" => {
                self.parse_usage(params)?;
                Ok(ParserProgress::default())
            }
            "hook/started" => {
                self.parse_hook_started(params)?;
                Ok(ParserProgress::default())
            }
            "hook/completed" => {
                self.parse_hook_completed(params)?;
                Ok(ParserProgress::default())
            }
            "mcpServer/startupStatus/updated" => {
                self.parse_mcp_status(params)?;
                Ok(ParserProgress::default())
            }
            "warning" => {
                self.parse_warning(params)?;
                Ok(ParserProgress::default())
            }
            "error" => {
                self.parse_native_error(params)?;
                Ok(ParserProgress::default())
            }
            "turn/completed" => self.parse_turn_completed(params),
            "project/changed" => {
                self.parse_project_changed(params)?;
                self.observe_unrecognized(value);
                Ok(ParserProgress::default())
            }
            "thread/project/updated" => {
                self.reject_thread_project_update(params)?;
                Ok(ParserProgress::default())
            }
            "autoApprovalReview/strictReviewRequired" => {
                self.parse_strict_review_required(object, params)?;
                self.observe_unrecognized(value);
                Ok(ParserProgress::default())
            }
            "configWarning" => {
                let summary = required_nonempty_string(params, "summary")
                    .ok_or_else(|| self.failure_for_current_phase())?;
                let message = self.retain_diagnostic(summary)?;
                self.observations.push(AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Warning,
                    message,
                });
                Ok(ParserProgress::default())
            }
            "remoteControl/status/changed" | "account/rateLimits/updated" => {
                self.observe_unrecognized(value);
                Ok(ParserProgress::default())
            }
            "thread/status/changed" => {
                self.require_thread(params)?;
                self.observe_unrecognized(value);
                Ok(ParserProgress::default())
            }
            _ => Err(self.failure_for_current_phase()),
        }
    }

    fn parse_turn_started(
        &mut self,
        params: &Map<String, Value>,
    ) -> Result<ParserProgress, AgentFailureCause> {
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
        if let Some(turn_id) = &self.turn_id {
            if turn_id.0.as_ref() != raw_turn_id {
                return Err(self.failure_for_current_phase());
            }
        } else {
            self.turn_id = Some(self.retain_turn_id(raw_turn_id)?);
        }
        if required_string(turn, "status") != Some("inProgress")
            || required_array(turn, "items").is_none()
        {
            return Err(self.failure_for_current_phase());
        }
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
            .ok_or_else(|| self.failure_for_current_phase())?;
        self.correlate_thread_value(raw_thread_id)?;
        if required_string(thread, "sessionId") != Some(raw_thread_id)
            || required_bool(thread, "ephemeral") != Some(true)
            || optional_string(thread, "path") != Some(None)
            || required_string(thread, "cliVersion") != Some(self.codex_version.as_ref())
            || optional_string(thread, "projectId") != Some(None)
            || required_string(thread, "cwd") != Some(self.expected_cwd.as_ref())
        {
            return Err(self.failure_for_current_phase());
        }
        self.thread_started_seen = true;
        Ok(())
    }

    fn parse_item_started(
        &mut self,
        params: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
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

    fn parse_hook_started(&mut self, params: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        self.require_hook_correlation(params)?;
        let run = required_object(params, "run").ok_or_else(|| self.failure_for_current_phase())?;
        let id =
            required_nonempty_string(run, "id").ok_or_else(|| self.failure_for_current_phase())?;
        let event = required_nonempty_string(run, "eventName")
            .ok_or_else(|| self.failure_for_current_phase())?;
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
        self.require_hook_correlation(params)?;
        let run = required_object(params, "run").ok_or_else(|| self.failure_for_current_phase())?;
        let id =
            required_nonempty_string(run, "id").ok_or_else(|| self.failure_for_current_phase())?;
        let event = required_nonempty_string(run, "eventName")
            .ok_or_else(|| self.failure_for_current_phase())?;
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
            if let Some(message) = self.retain_failure_diagnostic(message) {
                self.observations.push(AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Error,
                    message,
                });
            }
        }
        Ok(())
    }

    fn parse_mcp_status(&mut self, params: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if let Some(thread_id) =
            optional_string(params, "threadId").ok_or_else(|| self.failure_for_current_phase())?
        {
            self.require_thread_value(thread_id)?;
        }
        let name = required_nonempty_string(params, "name")
            .ok_or_else(|| self.failure_for_current_phase())?;
        if name.len() > MAXIMUM_IDENTITY_BYTES || name.chars().any(char::is_control) {
            return Err(self.failure_for_current_phase());
        }
        let status = required_nonempty_string(params, "status")
            .filter(|status| matches!(*status, "starting" | "ready" | "failed" | "cancelled"))
            .ok_or_else(|| self.failure_for_current_phase())?;
        let error =
            optional_string(params, "error").ok_or_else(|| self.failure_for_current_phase())?;
        let failure_reason = optional_string(params, "failureReason")
            .ok_or_else(|| self.failure_for_current_phase())?;
        if failure_reason.is_some_and(|reason| reason != "reauthenticationRequired")
            || status != "failed" && (error.is_some() || failure_reason.is_some())
        {
            return Err(self.failure_for_current_phase());
        }
        let summary = format!("MCP server {name}: {status}");
        let level = if status == "failed" {
            AgentDiagnosticLevel::Error
        } else {
            AgentDiagnosticLevel::Information
        };
        let message = if status == "failed" {
            self.retain_failure_diagnostic(&summary)
        } else {
            Some(self.retain_diagnostic(&summary)?)
        };
        if let Some(message) = message {
            self.observations
                .push(AgentObservation::Diagnostic { level, message });
        }
        if let Some(error) = error
            && let Some(message) = self.retain_failure_diagnostic(error)
        {
            self.observations.push(AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Error,
                message,
            });
        }
        Ok(())
    }

    fn parse_warning(&mut self, params: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if let Some(thread_id) =
            optional_string(params, "threadId").ok_or_else(|| self.failure_for_current_phase())?
        {
            self.correlate_thread_value(thread_id)?;
        }
        let message = required_nonempty_string(params, "message")
            .ok_or_else(|| self.failure_for_current_phase())?;
        let message = self.retain_diagnostic(message)?;
        self.observations.push(AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Warning,
            message,
        });
        Ok(())
    }

    fn parse_project_changed(
        &mut self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let project_id = required_nonempty_string(params, "projectId")
            .ok_or_else(|| self.failure_for_current_phase())?;
        if project_id.len() > MAXIMUM_IDENTITY_BYTES || project_id.chars().any(char::is_control) {
            return Err(self.failure_for_current_phase());
        }
        if !matches!(
            required_string(params, "changeType"),
            Some("created" | "updated" | "deleted")
        ) {
            return Err(self.failure_for_current_phase());
        }
        Ok(())
    }

    fn reject_thread_project_update(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_thread(params)?;
        if !params.contains_key("projectId") || optional_string(params, "projectId").is_none() {
            return Err(self.failure_for_current_phase());
        }
        Err(self.failure_for_current_phase())
    }

    fn parse_strict_review_required(
        &self,
        notification: &Map<String, Value>,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_running_correlation(params)?;
        let started_at = required_i64(params, "startedAtMs")
            .filter(|started_at| *started_at >= 0)
            .and_then(|started_at| u64::try_from(started_at).ok())
            .ok_or_else(|| self.failure_for_current_phase())?;
        let emitted_at = required_u64(notification, "emittedAtMs")
            .ok_or_else(|| self.failure_for_current_phase())?;
        if started_at > emitted_at {
            return Err(self.failure_for_current_phase());
        }
        Ok(())
    }

    fn parse_native_error(&mut self, params: &Map<String, Value>) -> Result<(), AgentFailureCause> {
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
        if let Some(message) = self.retain_failure_diagnostic(message) {
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
                if let Some(message) = self.retain_failure_diagnostic(message) {
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
        let item =
            required_object(params, "item").ok_or_else(|| self.failure_for_current_phase())?;
        let id =
            required_nonempty_string(item, "id").ok_or_else(|| self.failure_for_current_phase())?;
        let kind = required_nonempty_string(item, "type")
            .ok_or_else(|| self.failure_for_current_phase())?;
        Ok((item, id, kind))
    }

    fn require_thread(&self, params: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let thread_id = required_nonempty_string(params, "threadId")
            .ok_or_else(|| self.failure_for_current_phase())?;
        self.require_thread_value(thread_id)
    }

    fn require_thread_value(&self, thread_id: &str) -> Result<(), AgentFailureCause> {
        if self.thread_id.as_ref().map(|id| id.0.as_ref()) == Some(thread_id) {
            Ok(())
        } else {
            Err(self.failure_for_current_phase())
        }
    }

    fn correlate_thread_value(&mut self, thread_id: &str) -> Result<(), AgentFailureCause> {
        if self.thread_id.is_some() {
            return self.require_thread_value(thread_id);
        }
        if self.state != SetupState::ThreadStart || !is_codex_thread_id(thread_id) {
            return Err(self.failure_for_current_phase());
        }
        self.thread_id = Some(self.retain_thread_id(thread_id)?);
        Ok(())
    }

    fn require_running_correlation(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if self.state != SetupState::Running {
            return Err(self.failure_for_current_phase());
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
            return Err(self.failure_for_current_phase());
        }
        self.require_turn_correlation(params)
    }

    fn require_turn_correlation(
        &self,
        params: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_thread(params)?;
        let turn_id = required_nonempty_string(params, "turnId")
            .ok_or_else(|| self.failure_for_current_phase())?;
        if self.turn_id.as_ref().map(|id| id.0.as_ref()) != Some(turn_id) {
            return Err(self.failure_for_current_phase());
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
            return Err(self.failure_for_current_phase());
        }
        self.require_thread(params)?;
        match optional_string(params, "turnId").ok_or_else(|| self.failure_for_current_phase())? {
            Some(turn_id) => {
                if self.turn_id.as_ref().map(|id| id.0.as_ref()) != Some(turn_id) {
                    return Err(self.failure_for_current_phase());
                }
            }
            None if !matches!(
                self.state,
                SetupState::TurnStart | SetupState::StartAcknowledgement
            ) =>
            {
                return Err(self.failure_for_current_phase());
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
            return Err(self.failure_for_current_phase());
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
            .ok_or_else(|| self.failure_for_current_phase())?;
        Ok(())
    }

    fn retain_agent_message(&mut self, bytes: usize) -> Result<(), AgentFailureCause> {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.retained_agent_message_bytes = self
            .retained_agent_message_bytes
            .checked_add(bytes)
            .filter(|retained| *retained <= self.limits.maximum_retained_agent_message_bytes.get())
            .ok_or_else(|| self.failure_for_current_phase())?;
        Ok(())
    }

    fn retain_diagnostic(&mut self, message: &str) -> Result<Arc<str>, AgentFailureCause> {
        if message.is_empty() {
            return Err(self.failure_for_current_phase());
        }
        let remaining = self
            .limits
            .maximum_retained_diagnostic_bytes
            .get()
            .saturating_sub(self.retained_diagnostic_bytes);
        let remaining = usize::try_from(remaining).unwrap_or(usize::MAX);
        let (message, truncated) = content_safe_diagnostic(message, remaining);
        if truncated {
            return Err(self.failure_for_current_phase());
        }
        let bytes = u64::try_from(message.len()).unwrap_or(u64::MAX);
        self.retained_diagnostic_bytes = self
            .retained_diagnostic_bytes
            .checked_add(bytes)
            .ok_or_else(|| self.failure_for_current_phase())?;
        Ok(Arc::from(message))
    }

    fn retain_failure_diagnostic(&mut self, message: &str) -> Option<Arc<str>> {
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
        let frame = framed_json(&json!({"id": id.value(), "result": result}))?;
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
            return Err(self.failure_for_current_phase());
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

    fn fail_current_phase<T>(&mut self) -> Result<T, AgentFailureCause> {
        let failure = self.failure_for_current_phase();
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

fn has_notification_fields(object: &Map<String, Value>) -> bool {
    has_exact_fields(object, &["method", "params"])
        || (object.len() == 3
            && object.contains_key("method")
            && object.contains_key("params")
            && object.get("emittedAtMs").is_some_and(Value::is_u64))
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

fn required_i64(object: &Map<String, Value>, key: &str) -> Option<i64> {
    object.get(key)?.as_i64()
}

fn optional_null_or(
    object: &Map<String, Value>,
    key: &str,
    predicate: impl FnOnce(&Value) -> bool,
) -> bool {
    match object.get(key) {
        None | Some(Value::Null) => true,
        Some(value) => predicate(value),
    }
}

fn is_string_array(value: &Value) -> bool {
    value
        .as_array()
        .is_some_and(|values| values.iter().all(Value::is_string))
}

fn is_command_action_array(value: &Value) -> bool {
    value
        .as_array()
        .is_some_and(|actions| actions.iter().all(is_command_action))
}

fn is_command_action(value: &Value) -> bool {
    let Some(action) = value.as_object() else {
        return false;
    };
    if required_string(action, "command").is_none() {
        return false;
    }
    match required_string(action, "type") {
        Some("read") => {
            required_string(action, "name").is_some() && required_string(action, "path").is_some()
        }
        Some("listFiles") => optional_null_or(action, "path", Value::is_string),
        Some("search") => {
            optional_null_or(action, "path", Value::is_string)
                && optional_null_or(action, "query", Value::is_string)
        }
        Some("unknown") => true,
        _ => false,
    }
}

fn is_network_approval_context(value: &Value) -> bool {
    let Some(context) = value.as_object() else {
        return false;
    };
    required_string(context, "host").is_some()
        && matches!(
            required_string(context, "protocol"),
            Some("http" | "https" | "socks5Tcp" | "socks5Udp")
        )
}

fn is_network_policy_amendment_array(value: &Value) -> bool {
    value.as_array().is_some_and(|amendments| {
        amendments.iter().all(|amendment| {
            amendment.as_object().is_some_and(|amendment| {
                matches!(required_string(amendment, "action"), Some("allow" | "deny"))
                    && required_string(amendment, "host").is_some()
            })
        })
    })
}

fn is_permission_profile(value: &Value) -> bool {
    let Some(profile) = value.as_object() else {
        return false;
    };
    profile
        .keys()
        .all(|key| matches!(key.as_str(), "fileSystem" | "network"))
        && optional_null_or(profile, "fileSystem", is_file_system_permissions)
        && optional_null_or(profile, "network", is_network_permissions)
}

fn is_file_system_permissions(value: &Value) -> bool {
    let Some(permissions) = value.as_object() else {
        return false;
    };
    optional_null_or(permissions, "entries", |value| {
        value
            .as_array()
            .is_some_and(|entries| entries.iter().all(Value::is_object))
    }) && optional_null_or(permissions, "globScanMaxDepth", |value| {
        value.as_u64().is_some_and(|depth| depth > 0)
    }) && optional_null_or(permissions, "read", is_string_array)
        && optional_null_or(permissions, "write", is_string_array)
}

fn is_network_permissions(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|permissions| optional_null_or(permissions, "enabled", Value::is_boolean))
}

fn is_user_input_question(value: &Value) -> bool {
    let Some(question) = value.as_object() else {
        return false;
    };
    required_string(question, "header").is_some()
        && required_string(question, "id").is_some()
        && required_string(question, "question").is_some()
        && question.get("isOther").is_none_or(Value::is_boolean)
        && question.get("isSecret").is_none_or(Value::is_boolean)
        && optional_null_or(question, "options", |value| {
            value.as_array().is_some_and(|options| {
                options.iter().all(|option| {
                    option.as_object().is_some_and(|option| {
                        required_string(option, "description").is_some()
                            && required_string(option, "label").is_some()
                    })
                })
            })
        })
}

fn is_mcp_elicitation_schema(value: &Value) -> bool {
    let Some(schema) = value.as_object() else {
        return false;
    };
    schema
        .keys()
        .all(|key| matches!(key.as_str(), "$schema" | "properties" | "required" | "type"))
        && required_string(schema, "type") == Some("object")
        && required_object(schema, "properties")
            .is_some_and(|properties| properties.values().all(Value::is_object))
        && optional_null_or(schema, "$schema", Value::is_string)
        && optional_null_or(schema, "required", is_string_array)
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
