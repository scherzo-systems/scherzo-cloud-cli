pub(crate) mod adapter;

use std::ffi::OsString;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::agent::{
    AgentDiagnosticLevel, AgentFailure, AgentFailureCause, AgentHarnessFailureDetail,
    AgentLifecycleMilestone, AgentObservation, AgentOutcome, AgentProtocolRejectionDiagnostic,
    AgentToolCallPhase, AgentValueKind, BoundedAgentResponse, BoundedSchemaValidAgentResult,
    CompletedAgentInvocation, failed_agent_outcome, tool_call_observation,
};
use crate::execution::claude_code::CLAUDE_CODE_STREAM_JSON_V1_VERSION;

const MAXIMUM_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const MAXIMUM_DIAGNOSTIC_SEQUENCE: u64 = i64::MAX as u64;
const PERMISSION_MODE: &str = "bypassPermissions";
const STRUCTURED_OUTPUT_TOOL_NAME: &str = "StructuredOutput";
pub(crate) const RESULT_ENVELOPE_SCHEMA: &str = r#"{"type":"object","required":["result"],"properties":{"result":{}},"additionalProperties":true}"#;

pub(crate) const FIXED_INVOCATION_ENVIRONMENT: [(&str, &str); 5] = [
    ("DISABLE_UPDATES", "1"),
    ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
    ("CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL", "1"),
    ("CLAUDE_CODE_DISABLE_AUTO_MEMORY", "1"),
    ("CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS", "1"),
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ClaudeCodeStreamJsonV1ProtocolRejection {
    reason: ClaudeCodeStreamJsonV1RejectionReason,
    stage: ClaudeCodeStreamJsonV1ProtocolStage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outer_event: Option<ClaudeCodeStreamJsonV1EventType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stream_event: Option<ClaudeCodeStreamJsonV1StreamEventType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_block: Option<ClaudeCodeStreamJsonV1ContentBlockKind>,
    state: ClaudeCodeStreamJsonV1RejectionState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1ProtocolStage {
    FrameRead,
    FrameDecode,
    Initialization,
    ExchangeLifecycle,
    EventEnvelope,
    SessionCorrelation,
    StreamEventEnvelope,
    ActiveStreamParent,
    MessageStart,
    MessageTransition,
    ContentBlockStart,
    ContentBlockDelta,
    ContentBlockStop,
    AssistantEvent,
    UserEvent,
    SystemEvent,
    ToolProgressEvent,
    ResultCorrelation,
    TerminalDrain,
    EndOfStream,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1RejectionReason {
    FrameTooLarge,
    FrameDecodeFailed,
    FrameNotObject,
    InitializationInvalid,
    ExchangeInitializationMissing,
    ExchangeLifecycleInvalid,
    EventEnvelopeInvalid,
    SessionCorrelationInvalid,
    StreamEventEnvelopeInvalid,
    ActiveStreamParentMismatch,
    MessageStartInvalid,
    MessageTransitionInvalid,
    ContentBlockStartInvalid,
    ContentBlockDeltaInvalid,
    ContentBlockStopInvalid,
    ContentBlockIndexMismatch,
    ContentBlockTypeTransitionInvalid,
    ContentBlockCorrelationInvalid,
    AssistantEventInvalid,
    UserEventInvalid,
    SystemEventInvalid,
    ToolProgressEventInvalid,
    // Pi uses some matching terminal labels, but each closed profile must own and evolve
    // its rejection taxonomy independently.
    // jscpd:ignore-start
    ResultCorrelationInvalid,
    TerminalDrainEventInvalid,
    EndOfStreamInvariantInvalid,
    PartialFrameAtEndOfStream,
    RetainedStateLimitExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1EventType {
    // jscpd:ignore-end
    System,
    StreamEvent,
    Assistant,
    User,
    RateLimitEvent,
    ToolProgress,
    Result,
    Unrecognized,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1StreamEventType {
    MessageStart,
    ContentBlockStart,
    ContentBlockDelta,
    ContentBlockStop,
    MessageDelta,
    MessageStop,
    Unrecognized,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1ContentBlockKind {
    Text,
    Thinking,
    ToolUse,
    Unrecognized,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ClaudeCodeStreamJsonV1RejectionState {
    initialized: bool,
    exchange_initialized: bool,
    exchange_active: bool,
    completed_exchanges: u64,
    active_message: ClaudeCodeStreamJsonV1ActiveMessageState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_content_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completed_content_blocks: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    open_content_block: Option<ClaudeCodeStreamJsonV1OpenContentBlockState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_message_delta_seen: Option<bool>,
    final_main_message: bool,
    exchange_structured_output_candidates: u64,
    completed_result_exchange: ClaudeCodeStreamJsonV1CompletedResultExchangeState,
    result_decision_pending: bool,
    result_accepted: bool,
    native_failure: bool,
    retry_active: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1ActiveMessageState {
    None,
    Main,
    Subagent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ClaudeCodeStreamJsonV1OpenContentBlockState {
    kind: ClaudeCodeStreamJsonV1ContentBlockKind,
    content_index: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1CompletedResultExchangeState {
    None,
    Candidate,
    AmbiguousCandidate,
    MissingCandidate,
    NativeFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClaudeCodeStreamJsonV1RejectionContext {
    stage: ClaudeCodeStreamJsonV1ProtocolStage,
    outer_event: Option<ClaudeCodeStreamJsonV1EventType>,
    stream_event: Option<ClaudeCodeStreamJsonV1StreamEventType>,
    content_index: Option<u64>,
    content_block: Option<ClaudeCodeStreamJsonV1ContentBlockKind>,
}

impl Default for ClaudeCodeStreamJsonV1RejectionContext {
    fn default() -> Self {
        Self {
            stage: ClaudeCodeStreamJsonV1ProtocolStage::FrameRead,
            outer_event: None,
            stream_event: None,
            content_index: None,
            content_block: None,
        }
    }
}

// Keep each closed profile's limits beside its native parser so future profile changes
// cannot silently alter another harness's admission contract.
// jscpd:ignore-start
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClaudeCodeStreamJsonV1ProtocolLimits {
    maximum_frame_bytes: NonZeroU64,
}

impl ClaudeCodeStreamJsonV1ProtocolLimits {
    pub(crate) const fn profile() -> Self {
        let Some(maximum_frame_bytes) = NonZeroU64::new(MAXIMUM_FRAME_BYTES) else {
            unreachable!();
        };
        Self {
            maximum_frame_bytes,
        }
    }
    // jscpd:ignore-end

    #[cfg(test)]
    const fn with_maximum_frame_bytes(maximum_frame_bytes: NonZeroU64) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub(crate) const fn maximum_frame_bytes(self) -> NonZeroU64 {
        self.maximum_frame_bytes
    }
}

pub(crate) fn normal_mode_arguments(
    model: &str,
    effort: &str,
    session_id: &str,
    system_prompt_file: &Path,
) -> Vec<OsString> {
    [
        OsString::from("-p"),
        OsString::from("--input-format"),
        OsString::from("stream-json"),
        OsString::from("--output-format"),
        OsString::from("stream-json"),
        OsString::from("--verbose"),
        OsString::from("--include-partial-messages"),
        OsString::from("--forward-subagent-text"),
        OsString::from("--session-id"),
        OsString::from(session_id),
        OsString::from("--permission-mode"),
        OsString::from(PERMISSION_MODE),
        OsString::from("--setting-sources"),
        OsString::from("user,project,local"),
        OsString::from("--model"),
        OsString::from(model),
        OsString::from("--effort"),
        OsString::from(effort),
        OsString::from("--append-system-prompt-file"),
        system_prompt_file.as_os_str().to_owned(),
    ]
    .into()
}

pub(crate) fn result_mode_arguments(
    model: &str,
    effort: &str,
    session_id: &str,
    system_prompt_file: &Path,
) -> Vec<OsString> {
    let mut arguments = normal_mode_arguments(model, effort, session_id, system_prompt_file);
    arguments.extend([
        OsString::from("--json-schema"),
        OsString::from(RESULT_ENVELOPE_SCHEMA),
    ]);
    arguments
}

pub(crate) fn user_content_frame(content: Vec<Value>) -> Result<Vec<u8>, serde_json::Error> {
    let mut frame = serde_json::to_vec(&json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": content,
        },
    }))?;
    frame.push(b'\n');
    Ok(frame)
}

pub(crate) fn initial_user_text_frame(message: &str) -> Result<Vec<u8>, serde_json::Error> {
    user_content_frame(vec![json!({
        "type": "text",
        "text": message,
    })])
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CompletedResultExchange {
    Candidate(Arc<Value>),
    AmbiguousCandidate,
    MissingCandidate,
    NativeFailure,
}

pub(crate) struct ClaudeCodeStreamJsonV1Parser {
    expected_cwd: Arc<str>,
    expected_model: Arc<str>,
    value_kind: AgentValueKind,
    maximum_response_bytes: NonZeroU64,
    limits: ClaudeCodeStreamJsonV1ProtocolLimits,
    frame: Vec<u8>,
    expected_session_id: Arc<str>,
    session_id: Option<Arc<str>>,
    exchange_initialized: bool,
    exchange_active: bool,
    completed_exchanges: u64,
    active_message: Option<StreamedMessage>,
    final_main_message: Option<CompletedMainMessage>,
    exchange_structured_output_candidates: u64,
    completed_result_exchange: Option<CompletedResultExchange>,
    result_decision_pending: bool,
    accepted_result: Option<BoundedSchemaValidAgentResult>,
    native_failure: Option<AgentHarnessFailureDetail>,
    retry_active: bool,
    observations: Vec<AgentObservation>,
    rejection_context: ClaudeCodeStreamJsonV1RejectionContext,
    rejection_state_snapshot: Option<ClaudeCodeStreamJsonV1RejectionState>,
    protocol_rejection: Option<AgentProtocolRejectionDiagnostic>,
    failure: Option<AgentFailureCause>,
}

impl ClaudeCodeStreamJsonV1Parser {
    pub(crate) fn profile(
        expected_cwd: Arc<str>,
        expected_model: Arc<str>,
        expected_session_id: Arc<str>,
        value_kind: AgentValueKind,
        maximum_response_bytes: NonZeroU64,
    ) -> Self {
        Self::new(
            expected_cwd,
            expected_model,
            expected_session_id,
            value_kind,
            maximum_response_bytes,
            ClaudeCodeStreamJsonV1ProtocolLimits::profile(),
        )
    }

    fn new(
        expected_cwd: Arc<str>,
        expected_model: Arc<str>,
        expected_session_id: Arc<str>,
        value_kind: AgentValueKind,
        maximum_response_bytes: NonZeroU64,
        limits: ClaudeCodeStreamJsonV1ProtocolLimits,
    ) -> Self {
        Self {
            expected_cwd,
            expected_model,
            value_kind,
            maximum_response_bytes,
            limits,
            frame: Vec::new(),
            expected_session_id,
            session_id: None,
            exchange_initialized: false,
            exchange_active: true,
            completed_exchanges: 0,
            active_message: None,
            final_main_message: None,
            exchange_structured_output_candidates: 0,
            completed_result_exchange: None,
            result_decision_pending: false,
            accepted_result: None,
            native_failure: None,
            retry_active: false,
            observations: Vec::new(),
            rejection_context: ClaudeCodeStreamJsonV1RejectionContext::default(),
            rejection_state_snapshot: None,
            protocol_rejection: None,
            failure: None,
        }
    }

    #[cfg(test)]
    fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    #[cfg(test)]
    const fn completed_exchanges(&self) -> u64 {
        self.completed_exchanges
    }

    pub(crate) fn take_completed_result_exchange(&mut self) -> Option<CompletedResultExchange> {
        self.completed_result_exchange.take()
    }

    pub(crate) fn reject_result_candidate(&mut self) -> Result<(), AgentFailureCause> {
        if self.value_kind != AgentValueKind::Result
            || !self.result_decision_pending
            || self.accepted_result.is_some()
        {
            return self.fail_protocol(
                ClaudeCodeStreamJsonV1RejectionReason::ResultCorrelationInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::ResultCorrelation,
            );
        }
        self.result_decision_pending = false;
        Ok(())
    }

    pub(crate) fn accept_result(
        &mut self,
        result: BoundedSchemaValidAgentResult,
    ) -> Result<(), AgentFailureCause> {
        if self.value_kind != AgentValueKind::Result
            || !self.result_decision_pending
            || self.accepted_result.is_some()
            || self.exchange_active
            || self.exchange_initialized
        {
            return self.fail_protocol(
                ClaudeCodeStreamJsonV1RejectionReason::ResultCorrelationInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::ResultCorrelation,
            );
        }
        self.result_decision_pending = false;
        self.accepted_result = Some(result);
        Ok(())
    }

    /// Begins the next serialized user exchange after the preceding result was rejected.
    /// The same process and session remain authoritative for the invocation.
    pub(crate) fn begin_exchange(&mut self) -> Result<(), AgentFailureCause> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        if self.session_id.is_none()
            || self.exchange_active
            || self.exchange_initialized
            || self.active_message.is_some()
            || self.completed_result_exchange.is_some()
            || self.result_decision_pending
            || self.accepted_result.is_some()
        {
            return self.fail_protocol(
                ClaudeCodeStreamJsonV1RejectionReason::ExchangeLifecycleInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::ExchangeLifecycle,
            );
        }
        self.exchange_active = true;
        self.final_main_message = None;
        self.exchange_structured_output_candidates = 0;
        self.native_failure = None;
        Ok(())
    }

    /// Consumes arbitrary stdout chunks while retaining at most one bounded JSONL frame.
    // Pi and Claude intentionally retain independent native state machines; sharing
    // this byte loop would couple their profile-specific failure transitions.
    // jscpd:ignore-start
    pub(crate) fn push_stdout(
        &mut self,
        bytes: &[u8],
        mut observe: impl FnMut(AgentObservation),
    ) -> Result<(), AgentFailureCause> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }

        for &byte in bytes {
            if byte == b'\n' {
                let frame = std::mem::take(&mut self.frame);
                if let Err(failure) = self.parse_frame(&frame) {
                    self.invalidate_values();
                    self.observations.clear();
                    self.failure = Some(failure.clone());
                    return Err(failure);
                }
                for observation in self.observations.drain(..) {
                    observe(observation);
                }
                continue;
            }

            let retained = u64::try_from(self.frame.len()).unwrap_or(u64::MAX);
            if retained >= self.limits.maximum_frame_bytes().get() {
                return self.fail_protocol(
                    ClaudeCodeStreamJsonV1RejectionReason::FrameTooLarge,
                    ClaudeCodeStreamJsonV1ProtocolStage::FrameRead,
                );
            }
            self.frame.push(byte);
        }
        Ok(())
    }
    // jscpd:ignore-end

    pub(crate) fn finish(mut self, exit_success: bool) -> AgentOutcome {
        if self.failure.is_none() && !self.frame.is_empty() {
            self.prepare_rejection(ClaudeCodeStreamJsonV1ProtocolStage::EndOfStream);
            self.record_rejection(
                ClaudeCodeStreamJsonV1RejectionReason::PartialFrameAtEndOfStream,
                ClaudeCodeStreamJsonV1ProtocolStage::EndOfStream,
            );
            self.failure = Some(self.protocol_failure());
            self.invalidate_values();
        }
        if let Some(failure) = self.failure.clone() {
            return AgentOutcome::Failed(self.agent_failure(failure));
        }
        if self.session_id.is_none() {
            return self.protocol_failure_outcome(
                AgentFailureCause::HarnessStartFailed,
                ClaudeCodeStreamJsonV1RejectionReason::EndOfStreamInvariantInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::EndOfStream,
            );
        }
        if self.exchange_active
            || self.exchange_initialized
            || self.active_message.is_some()
            || self.completed_result_exchange.is_some()
            || self.result_decision_pending
            || self.completed_exchanges == 0
        {
            return self.protocol_failure_outcome(
                AgentFailureCause::HarnessProtocolFailed,
                ClaudeCodeStreamJsonV1RejectionReason::EndOfStreamInvariantInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::EndOfStream,
            );
        }
        if let Some(detail) = self.native_failure {
            return failed(AgentFailureCause::HarnessFailed { detail });
        }
        if !exit_success {
            return failed(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            });
        }

        match self.value_kind {
            AgentValueKind::None => AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
            AgentValueKind::Response => {
                let Some(message) = self.final_main_message else {
                    return failed(AgentFailureCause::MissingResponse);
                };
                if message.text_blocks == 0 {
                    return failed(AgentFailureCause::MissingResponse);
                }
                AgentOutcome::Completed(CompletedAgentInvocation::Response(
                    BoundedAgentResponse::from_bounded(Arc::from(message.response)),
                ))
            }
            AgentValueKind::Result => self.accepted_result.map_or_else(
                || failed(AgentFailureCause::MissingResult),
                |result| AgentOutcome::Completed(CompletedAgentInvocation::Result(result)),
            ),
        }
    }

    // Native parser state stays profile-local even though both profiles snapshot before
    // decoding and install one fallback diagnostic for otherwise unclassified rejections.
    // jscpd:ignore-start
    fn parse_frame(&mut self, frame: &[u8]) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(ClaudeCodeStreamJsonV1ProtocolStage::FrameRead);
        let result = self.parse_frame_inner(frame);
        let parser_owned_rejection = result.as_ref().is_err_and(|cause| {
            matches!(
                cause,
                AgentFailureCause::HarnessStartFailed | AgentFailureCause::HarnessProtocolFailed
            )
        });
        if parser_owned_rejection && self.protocol_rejection.is_none() {
            let stage = self.rejection_context.stage;
            self.record_rejection(default_rejection_reason(stage), stage);
        }
        self.rejection_state_snapshot = None;
        result
    }
    // jscpd:ignore-end

    fn parse_frame_inner(&mut self, frame: &[u8]) -> Result<(), AgentFailureCause> {
        // This profile owns decoding failure classification because Claude's init boundary
        // differs from Pi's session-header and agent-start boundaries.
        // jscpd:ignore-start
        let frame_bytes = u64::try_from(frame.len()).unwrap_or(u64::MAX);
        if frame_bytes > self.limits.maximum_frame_bytes().get() {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::FrameTooLarge,
                ClaudeCodeStreamJsonV1ProtocolStage::FrameRead,
            );
        }
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::FrameDecode;
        let value = serde_json::from_slice::<Value>(frame).map_err(|_| self.protocol_failure())?;
        let Some(object) = value.as_object() else {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::FrameNotObject,
                ClaudeCodeStreamJsonV1ProtocolStage::FrameDecode,
            );
        };
        // jscpd:ignore-end

        self.rejection_context.outer_event = required_string(object, "type").map(claude_event_type);
        if self.session_id.is_none() {
            self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::Initialization;
            return self.parse_initialization(object);
        }

        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::EventEnvelope;
        let event_type = required_string(object, "type").ok_or_else(|| self.protocol_failure())?;
        if event_type == "stream_event" {
            self.record_stream_event_identity(object);
        }
        if event_type == "system" && required_string(object, "subtype") == Some("init") {
            self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::Initialization;
            return self.parse_initialization(object);
        }

        if !self.exchange_active {
            self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::TerminalDrain;
            return self.parse_terminal_drain(event_type, object, &value);
        }
        if !self.exchange_initialized {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::ExchangeInitializationMissing,
                ClaudeCodeStreamJsonV1ProtocolStage::ExchangeLifecycle,
            );
        }

        match event_type {
            "stream_event" => self.parse_stream_event(object, &value),
            "assistant" => {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::AssistantEvent;
                self.parse_assistant(object, &value)
            }
            "user" => {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::UserEvent;
                self.parse_user(object, &value)
            }
            "system" => {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::SystemEvent;
                self.parse_system(object, &value)
            }
            "rate_limit_event" => self.parse_rate_limit_event(object),
            "tool_progress" => {
                self.rejection_context.stage =
                    ClaudeCodeStreamJsonV1ProtocolStage::ToolProgressEvent;
                self.parse_tool_progress(object)
            }
            "result" => {
                self.rejection_context.stage =
                    ClaudeCodeStreamJsonV1ProtocolStage::ResultCorrelation;
                self.parse_result(object)
            }
            _ => {
                self.require_matching_session(object)?;
                self.observe_unrecognized(&value);
                Ok(())
            }
        }
    }

    fn parse_terminal_drain(
        &mut self,
        event_type: &str,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.require_matching_session(object)?;
        match event_type {
            "system" if required_string(object, "subtype") == Some("status") => {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::SystemEvent;
                self.parse_system(object, value)
            }
            "stream_event" | "assistant" | "user" | "tool_progress" | "result" => self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::TerminalDrainEventInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::TerminalDrain,
            ),
            _ => {
                self.observe_unrecognized(value);
                Ok(())
            }
        }
    }

    fn parse_initialization(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::Initialization;
        let session_id = required_string(object, "session_id");
        if !self.exchange_active
            || self.exchange_initialized
            || self.active_message.is_some()
            || required_string(object, "type") != Some("system")
            || required_string(object, "subtype") != Some("init")
            || required_string(object, "claude_code_version")
                != Some(CLAUDE_CODE_STREAM_JSON_V1_VERSION)
            || required_string(object, "cwd") != Some(self.expected_cwd.as_ref())
            || required_string(object, "model") != Some(self.expected_model.as_ref())
            || required_string(object, "permissionMode") != Some(PERMISSION_MODE)
            || !session_id.is_some_and(|session_id| {
                session_id == self.expected_session_id.as_ref() && valid_session_id(session_id)
            })
        {
            return Err(self.protocol_failure());
        }

        let first_initialization = self.session_id.is_none();
        match (&self.session_id, session_id) {
            (None, Some(session_id)) => self.session_id = Some(Arc::from(session_id)),
            (Some(expected), Some(session_id)) if expected.as_ref() == session_id => {}
            _ => return Err(self.protocol_failure()),
        }
        self.exchange_initialized = true;
        if first_initialization {
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::SessionEstablished));
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::HarnessStarted));
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::TurnStarted));
        Ok(())
    }

    fn parse_stream_event(
        &mut self,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::StreamEventEnvelope;
        self.require_matching_session(object)?;
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::StreamEventEnvelope;
        let parent_tool_use_id =
            parent_tool_use_id(object).ok_or_else(|| self.protocol_failure())?;
        let event = required_object(object, "event").ok_or_else(|| self.protocol_failure())?;
        let event_type = required_string(event, "type").ok_or_else(|| self.protocol_failure())?;
        if event_type != "message_start" {
            self.require_active_stream_parent(parent_tool_use_id)?;
        }
        match event_type {
            "message_start" => {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::MessageStart;
                self.stream_message_start(event, parent_tool_use_id)
            }
            "content_block_start" => self.stream_content_block_start(event, value),
            "content_block_delta" => self.stream_content_block_delta(event, value),
            "content_block_stop" => self.stream_content_block_stop(event),
            "message_delta" => {
                self.rejection_context.stage =
                    ClaudeCodeStreamJsonV1ProtocolStage::MessageTransition;
                self.stream_message_delta(event)
            }
            "message_stop" => {
                self.rejection_context.stage =
                    ClaudeCodeStreamJsonV1ProtocolStage::MessageTransition;
                self.stream_message_stop(event)
            }
            _ => {
                self.observe_unrecognized(value);
                Ok(())
            }
        }
    }

    fn record_stream_event_identity(&mut self, object: &Map<String, Value>) {
        let Some(event) = required_object(object, "event") else {
            return;
        };
        let Some(event_type) = required_string(event, "type") else {
            return;
        };
        self.rejection_context.stream_event = Some(claude_stream_event_type(event_type));
        if matches!(
            event_type,
            "content_block_start" | "content_block_delta" | "content_block_stop"
        ) {
            self.rejection_context.content_index =
                required_u64(event, "index").map(bounded_sequence);
        }
        self.rejection_context.content_block = match event_type {
            "content_block_start" => required_object(event, "content_block")
                .and_then(|block| required_string(block, "type"))
                .map(claude_content_block_kind),
            "content_block_delta" => required_object(event, "delta")
                .and_then(|delta| required_string(delta, "type"))
                .map(claude_delta_content_block_kind),
            _ => None,
        };
    }

    fn require_active_stream_parent(
        &mut self,
        parent_tool_use_id: Option<&str>,
    ) -> Result<(), AgentFailureCause> {
        let matches = self
            .active_message
            .as_ref()
            .is_some_and(|message| message.parent_tool_use_id.as_deref() == parent_tool_use_id);
        if !matches {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::ActiveStreamParentMismatch,
                ClaudeCodeStreamJsonV1ProtocolStage::ActiveStreamParent,
            );
        }
        Ok(())
    }

    fn stream_message_start(
        &mut self,
        event: &Map<String, Value>,
        parent_tool_use_id: Option<&str>,
    ) -> Result<(), AgentFailureCause> {
        if self.active_message.is_some() {
            return Err(self.protocol_failure());
        }
        let message = required_object(event, "message").ok_or_else(|| self.protocol_failure())?;
        let id = required_nonempty_string(message, "id").ok_or_else(|| self.protocol_failure())?;
        let model =
            required_nonempty_string(message, "model").ok_or_else(|| self.protocol_failure())?;
        let usage = required_object(message, "usage").ok_or_else(|| self.protocol_failure())?;
        let input_tokens =
            required_u64(usage, "input_tokens").ok_or_else(|| self.protocol_failure())?;
        let output_tokens =
            required_u64(usage, "output_tokens").ok_or_else(|| self.protocol_failure())?;
        if required_string(message, "type") != Some("message")
            || required_string(message, "role") != Some("assistant")
            || model != self.expected_model.as_ref()
            || required_array(message, "content").is_none_or(|content| !content.is_empty())
        {
            return Err(self.protocol_failure());
        }
        self.complete_retry();
        self.active_message = Some(StreamedMessage {
            id: Arc::from(id),
            parent_tool_use_id: parent_tool_use_id.map(Arc::from),
            next_index: 0,
            active_block: None,
            completed_blocks: 0,
            text_blocks: 0,
            structured_output_candidates: Vec::new(),
            structured_output_call_ids: Vec::new(),
            structured_output_acknowledgements: 0,
            successful_structured_output_acknowledgements: 0,
            response: String::new(),
            input_tokens,
            output_tokens,
            terminal_delta_seen: false,
        });
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageStarted));
        self.observations.push(AgentObservation::Model {
            name: Arc::from(model),
        });
        self.observations.push(AgentObservation::Usage {
            input_tokens,
            output_tokens,
        });
        Ok(())
    }

    fn stream_content_block_start(
        &mut self,
        event: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStart;
        self.rejection_context.content_index = required_u64(event, "index").map(bounded_sequence);
        let index = required_u64(event, "index").ok_or_else(|| self.protocol_failure())?;
        let block =
            required_object(event, "content_block").ok_or_else(|| self.protocol_failure())?;
        let block_type = required_string(block, "type").ok_or_else(|| self.protocol_failure())?;
        self.rejection_context.content_block = Some(claude_content_block_kind(block_type));
        let Some(message) = self.active_message.as_ref() else {
            return Err(self.protocol_failure());
        };
        if message.active_block.is_some() || index != message.next_index {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::ContentBlockIndexMismatch,
                ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStart,
            );
        }

        let maximum_correlation_bytes = self.limits.maximum_frame_bytes().get();
        let active_block = match block_type {
            "text" => {
                let text = required_string(block, "text").ok_or_else(|| self.protocol_failure())?;
                let mut streamed_text = String::new();
                if !bounded_append(&mut streamed_text, text, maximum_correlation_bytes) {
                    return self.reject(
                        ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStart,
                    );
                }
                self.append_assistant_text(text)?;
                let failure = self.protocol_failure();
                let Some(message) = self.active_message.as_mut() else {
                    return Err(failure);
                };
                let Some(text_blocks) = message.text_blocks.checked_add(1) else {
                    return Err(failure);
                };
                message.text_blocks = text_blocks;
                ActiveContentBlock::Text {
                    text: streamed_text,
                    nominal_seen: false,
                }
            }
            "thinking" => {
                let thinking =
                    required_string(block, "thinking").ok_or_else(|| self.protocol_failure())?;
                let mut streamed_thinking = String::new();
                if !bounded_append(&mut streamed_thinking, thinking, maximum_correlation_bytes) {
                    return self.reject(
                        ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStart,
                    );
                }
                if !thinking.is_empty() {
                    self.observations.push(AgentObservation::Reasoning {
                        text: Arc::from(thinking),
                    });
                }
                ActiveContentBlock::Thinking {
                    thinking: streamed_thinking,
                    nominal_seen: false,
                }
            }
            "tool_use" => {
                let call_id =
                    required_nonempty_string(block, "id").ok_or_else(|| self.protocol_failure())?;
                let name = required_nonempty_string(block, "name")
                    .ok_or_else(|| self.protocol_failure())?;
                let Some(initial_input) = block.get("input").filter(|input| input.is_object())
                else {
                    return Err(self.protocol_failure());
                };
                self.observations.push(AgentObservation::ToolCall {
                    call_id: Arc::from(call_id),
                    name: Arc::from(name),
                    phase: AgentToolCallPhase::Started,
                });
                ActiveContentBlock::ToolUse {
                    call_id: Arc::from(call_id),
                    name: Arc::from(name),
                    initial_input: initial_input.clone(),
                    input_json: String::new(),
                    input_delta_seen: false,
                    nominal_seen: false,
                }
            }
            _ => {
                self.observe_unrecognized(value);
                ActiveContentBlock::Unknown {
                    initial_block: Value::Object(block.clone()),
                    delta_seen: false,
                    nominal_seen: false,
                }
            }
        };
        let Some(message) = self.active_message.as_mut() else {
            return Err(AgentFailureCause::HarnessProtocolFailed);
        };
        message.active_block = Some(active_block);
        Ok(())
    }

    fn stream_content_block_delta(
        &mut self,
        event: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta;
        self.rejection_context.content_index = required_u64(event, "index").map(bounded_sequence);
        let index = required_u64(event, "index").ok_or_else(|| self.protocol_failure())?;
        let delta = required_object(event, "delta").ok_or_else(|| self.protocol_failure())?;
        let delta_type = required_string(delta, "type").ok_or_else(|| self.protocol_failure())?;
        self.rejection_context.content_block = Some(claude_delta_content_block_kind(delta_type));
        let Some(message) = self.active_message.as_ref() else {
            return Err(self.protocol_failure());
        };
        if index != message.next_index {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::ContentBlockIndexMismatch,
                ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta,
            );
        }

        match (&message.active_block, delta_type) {
            (Some(ActiveContentBlock::Text { .. }), "text_delta") => {
                let text = required_string(delta, "text").ok_or_else(|| self.protocol_failure())?;
                let maximum_correlation_bytes = self.limits.maximum_frame_bytes().get();
                let failure = self.protocol_failure();
                let Some(message) = self.active_message.as_mut() else {
                    return Err(failure);
                };
                let Some(ActiveContentBlock::Text { text: streamed, .. }) =
                    message.active_block.as_mut()
                else {
                    return Err(failure);
                };
                if !bounded_append(streamed, text, maximum_correlation_bytes) {
                    return self.reject(
                        ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta,
                    );
                }
                self.append_assistant_text(text)
            }
            (Some(ActiveContentBlock::Thinking { .. }), "thinking_delta") => {
                let thinking =
                    required_string(delta, "thinking").ok_or_else(|| self.protocol_failure())?;
                let maximum_correlation_bytes = self.limits.maximum_frame_bytes().get();
                let failure = self.protocol_failure();
                let Some(message) = self.active_message.as_mut() else {
                    return Err(failure);
                };
                let Some(ActiveContentBlock::Thinking {
                    thinking: streamed, ..
                }) = message.active_block.as_mut()
                else {
                    return Err(failure);
                };
                if !bounded_append(streamed, thinking, maximum_correlation_bytes) {
                    return self.reject(
                        ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta,
                    );
                }
                if !thinking.is_empty() {
                    self.observations.push(AgentObservation::Reasoning {
                        text: Arc::from(thinking),
                    });
                }
                Ok(())
            }
            (Some(ActiveContentBlock::Thinking { .. }), "signature_delta") => {
                required_string(delta, "signature")
                    .map(|_| ())
                    .ok_or_else(|| self.protocol_failure())
            }
            (Some(ActiveContentBlock::ToolUse { call_id, name, .. }), "input_json_delta") => {
                let partial_json = required_string(delta, "partial_json")
                    .ok_or_else(|| self.protocol_failure())?;
                let call_id = Arc::clone(call_id);
                let name = Arc::clone(name);
                let maximum_correlation_bytes = self.limits.maximum_frame_bytes().get();
                let failure = self.protocol_failure();
                let Some(message) = self.active_message.as_mut() else {
                    return Err(failure);
                };
                let Some(ActiveContentBlock::ToolUse {
                    input_json,
                    input_delta_seen,
                    ..
                }) = message.active_block.as_mut()
                else {
                    return Err(failure);
                };
                if !bounded_append(input_json, partial_json, maximum_correlation_bytes) {
                    return self.reject(
                        ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta,
                    );
                }
                *input_delta_seen = true;
                self.observations.push(AgentObservation::ToolCall {
                    call_id,
                    name,
                    phase: AgentToolCallPhase::Updated,
                });
                Ok(())
            }
            (Some(ActiveContentBlock::Unknown { .. }), _) => {
                let failure = self.protocol_failure();
                let Some(message) = self.active_message.as_mut() else {
                    return Err(failure);
                };
                let Some(ActiveContentBlock::Unknown { delta_seen, .. }) =
                    message.active_block.as_mut()
                else {
                    return Err(failure);
                };
                *delta_seen = true;
                self.observe_unrecognized(value);
                Ok(())
            }
            _ => self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::ContentBlockTypeTransitionInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta,
            ),
        }
    }

    fn stream_content_block_stop(
        &mut self,
        event: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStop;
        self.rejection_context.content_index = required_u64(event, "index").map(bounded_sequence);
        let index = required_u64(event, "index").ok_or_else(|| self.protocol_failure())?;
        let Some(message) = self.active_message.as_ref() else {
            return Err(self.protocol_failure());
        };
        if index != message.next_index {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::ContentBlockIndexMismatch,
                ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStop,
            );
        }
        if message.active_block.is_none() {
            return Err(self.protocol_failure());
        }
        if message.next_index == u64::MAX || message.completed_blocks == u64::MAX {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStop,
            );
        }
        let block = {
            let failure = self.protocol_failure();
            let Some(message) = self.active_message.as_mut() else {
                return Err(failure.clone());
            };
            let Some(block) = message.active_block.take() else {
                return Err(failure);
            };
            message.next_index += 1;
            message.completed_blocks += 1;
            block
        };
        if let ActiveContentBlock::ToolUse {
            call_id,
            name,
            initial_input,
            input_json,
            input_delta_seen,
            ..
        } = block
        {
            if name.as_ref() == STRUCTURED_OUTPUT_TOOL_NAME {
                let candidate = if input_delta_seen {
                    let Ok(candidate) = serde_json::from_str::<Value>(&input_json) else {
                        return self.reject(
                            ClaudeCodeStreamJsonV1RejectionReason::ContentBlockCorrelationInvalid,
                            ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStop,
                        );
                    };
                    candidate
                } else {
                    initial_input
                };
                let Some(next_candidate_count) =
                    self.exchange_structured_output_candidates.checked_add(1)
                else {
                    return self.reject(
                        ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStop,
                    );
                };
                let failure = self.protocol_failure();
                let Some(message) = self.active_message.as_mut() else {
                    return Err(failure);
                };
                message.structured_output_candidates.push(candidate);
                message
                    .structured_output_call_ids
                    .push(Arc::clone(&call_id));
                self.exchange_structured_output_candidates = next_candidate_count;
            }
            self.observations.push(AgentObservation::ToolCall {
                call_id,
                name,
                phase: AgentToolCallPhase::Completed,
            });
        }
        Ok(())
    }

    fn stream_message_delta(
        &mut self,
        event: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let delta = required_object(event, "delta").ok_or_else(|| self.protocol_failure())?;
        let output_tokens = event
            .get("usage")
            .map(|usage| {
                usage
                    .as_object()
                    .and_then(|usage| required_u64(usage, "output_tokens"))
                    .ok_or_else(|| self.protocol_failure())
            })
            .transpose()?;
        let Some(message) = self.active_message.as_mut() else {
            return Err(self.protocol_failure());
        };
        if message.active_block.is_some()
            || message.terminal_delta_seen
            || !delta.get("stop_reason").is_some_and(Value::is_string)
        {
            return Err(self.protocol_failure());
        }
        if let Some(output_tokens) = output_tokens {
            message.output_tokens = output_tokens;
            self.observations.push(AgentObservation::Usage {
                input_tokens: message.input_tokens,
                output_tokens,
            });
        }
        message.terminal_delta_seen = true;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageUpdated));
        Ok(())
    }

    fn stream_message_stop(
        &mut self,
        _event: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let Some(message) = self.active_message.take() else {
            return Err(self.protocol_failure());
        };
        if message.active_block.is_some() || !message.terminal_delta_seen {
            self.active_message = Some(message);
            return Err(self.protocol_failure());
        }
        if message.parent_tool_use_id.is_none() {
            self.final_main_message = Some(CompletedMainMessage {
                completed_blocks: message.completed_blocks,
                text_blocks: message.text_blocks,
                structured_output_candidates: message.structured_output_candidates,
                structured_output_acknowledgements: message.structured_output_acknowledgements,
                successful_structured_output_acknowledgements: message
                    .successful_structured_output_acknowledgements,
                response: message.response,
            });
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageCompleted));
        Ok(())
    }

    fn append_assistant_text(&mut self, text: &str) -> Result<(), AgentFailureCause> {
        let Some(message) = self.active_message.as_mut() else {
            return Err(self.protocol_failure());
        };
        if message.parent_tool_use_id.is_none() && self.value_kind == AgentValueKind::Response {
            let current = u64::try_from(message.response.len()).unwrap_or(u64::MAX);
            let additional = u64::try_from(text.len()).unwrap_or(u64::MAX);
            if current
                .checked_add(additional)
                .is_none_or(|total| total > self.maximum_response_bytes.get())
            {
                return Err(AgentFailureCause::CapturedValueTooLarge);
            }
            message.response.push_str(text);
        }
        if !text.is_empty() {
            self.observations.push(AgentObservation::AssistantText {
                text: Arc::from(text),
            });
        }
        Ok(())
    }

    fn parse_assistant(
        &mut self,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.require_matching_session(object)?;
        let parent = parent_tool_use_id(object).ok_or_else(|| self.protocol_failure())?;
        let message = required_object(object, "message").ok_or_else(|| self.protocol_failure())?;
        let message_id =
            required_nonempty_string(message, "id").ok_or_else(|| self.protocol_failure())?;
        let content = required_array(message, "content").ok_or_else(|| self.protocol_failure())?;
        if required_string(message, "type") != Some("message")
            || required_string(message, "role") != Some("assistant")
        {
            return Err(self.protocol_failure());
        }
        if parent.is_none() && required_bool(object, "is_api_error_message") == Some(true) {
            let [block] = content else {
                return Err(self.protocol_failure());
            };
            let block = block.as_object().ok_or_else(|| self.protocol_failure())?;
            let diagnostic = required_nonempty_string(block, "text")
                .filter(|_| required_string(block, "type") == Some("text"))
                .ok_or_else(|| self.protocol_failure())?;
            if self.active_message.is_some()
                || required_nonempty_string(object, "error").is_none()
                || required_nonempty_string(object, "request_id").is_none()
            {
                return Err(self.protocol_failure());
            }
            self.observations.push(AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Error,
                message: Arc::from(diagnostic),
            });
            return Ok(());
        }
        if required_string(message, "model") != Some(self.expected_model.as_ref()) {
            return Err(self.protocol_failure());
        }

        if parent.is_none() {
            let failure = self.protocol_failure();
            let Some(active) = self.active_message.as_mut() else {
                return Err(failure);
            };
            let [nominal_block] = content else {
                return Err(failure);
            };
            if active.id.as_ref() != message_id
                || active.parent_tool_use_id.is_some()
                || active
                    .active_block
                    .as_mut()
                    .is_none_or(|block| !block.correlate_nominal(nominal_block))
            {
                return Err(failure);
            }
            return Ok(());
        }

        for block in content {
            let Some(block) = block.as_object() else {
                return Err(self.protocol_failure());
            };
            match required_string(block, "type") {
                Some("text") => {
                    let text =
                        required_string(block, "text").ok_or_else(|| self.protocol_failure())?;
                    if !text.is_empty() {
                        self.observations.push(AgentObservation::AssistantText {
                            text: Arc::from(text),
                        });
                    }
                }
                Some("thinking") => {
                    let thinking = required_string(block, "thinking")
                        .ok_or_else(|| self.protocol_failure())?;
                    if !thinking.is_empty() {
                        self.observations.push(AgentObservation::Reasoning {
                            text: Arc::from(thinking),
                        });
                    }
                }
                Some("tool_use") => {
                    let call_id = required_nonempty_string(block, "id")
                        .ok_or_else(|| self.protocol_failure())?;
                    let name = required_nonempty_string(block, "name")
                        .ok_or_else(|| self.protocol_failure())?;
                    self.observations.extend([
                        AgentObservation::ToolCall {
                            call_id: Arc::from(call_id),
                            name: Arc::from(name),
                            phase: AgentToolCallPhase::Started,
                        },
                        AgentObservation::ToolCall {
                            call_id: Arc::from(call_id),
                            name: Arc::from(name),
                            phase: AgentToolCallPhase::Completed,
                        },
                    ]);
                }
                _ => self.observe_unrecognized(value),
            }
        }
        Ok(())
    }

    fn parse_user(
        &mut self,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.require_matching_session(object)?;
        parent_tool_use_id(object).ok_or_else(|| self.protocol_failure())?;
        let message = required_object(object, "message").ok_or_else(|| self.protocol_failure())?;
        let content = required_array(message, "content").ok_or_else(|| self.protocol_failure())?;
        if required_string(message, "role") != Some("user") {
            return Err(self.protocol_failure());
        }
        for block in content {
            let Some(block) = block.as_object() else {
                return Err(self.protocol_failure());
            };
            if required_string(block, "type") != Some("tool_result") {
                self.observe_unrecognized(value);
                continue;
            }
            let call_id = required_nonempty_string(block, "tool_use_id")
                .ok_or_else(|| self.protocol_failure())?;
            let result = block
                .get("content")
                .and_then(normalized_tool_result_content)
                .ok_or_else(|| self.protocol_failure())?;
            let acknowledges_structured_output =
                self.active_message.as_ref().is_some_and(|message| {
                    message
                        .structured_output_call_ids
                        .iter()
                        .any(|expected| expected.as_ref() == call_id)
                });
            let omitted_structured_output_success = acknowledges_structured_output
                && result == "Structured output provided successfully"
                && required_string(object, "tool_use_result") == Some(result.as_str());
            let omitted_completed_agent_success = !acknowledges_structured_output
                && object
                    .get("tool_use_result")
                    .and_then(Value::as_object)
                    .is_some_and(|tool_result| {
                        required_string(tool_result, "status") == Some("completed")
                            && required_nonempty_string(tool_result, "agentId").is_some()
                            && required_nonempty_string(tool_result, "agentType").is_some()
                    });
            let is_error = required_bool(block, "is_error")
                .or_else(|| {
                    (omitted_structured_output_success || omitted_completed_agent_success)
                        .then_some(false)
                })
                .ok_or_else(|| self.protocol_failure())?;
            if acknowledges_structured_output {
                let message = self
                    .active_message
                    .as_mut()
                    .ok_or(AgentFailureCause::HarnessProtocolFailed)?;
                message.structured_output_acknowledgements = message
                    .structured_output_acknowledgements
                    .checked_add(1)
                    .ok_or(AgentFailureCause::HarnessProtocolFailed)?;
                if !is_error {
                    message.successful_structured_output_acknowledgements = message
                        .successful_structured_output_acknowledgements
                        .checked_add(1)
                        .ok_or(AgentFailureCause::HarnessProtocolFailed)?;
                }
            }
            self.observations.push(AgentObservation::ToolResult {
                call_id: Arc::from(call_id),
                is_error,
                content: Arc::from(result),
            });
        }
        Ok(())
    }

    fn parse_system(
        &mut self,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.require_matching_session(object)?;
        match required_string(object, "subtype").ok_or_else(|| self.protocol_failure())? {
            "status" => {
                let status = required_nonempty_string(object, "status")
                    .ok_or_else(|| self.protocol_failure())?;
                self.observations.push(AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Information,
                    message: Arc::from(status),
                });
            }
            "api_retry" => {
                if !self.exchange_active {
                    return Err(self.protocol_failure());
                }
                if !self.retry_active {
                    self.retry_active = true;
                    self.observations
                        .push(lifecycle(AgentLifecycleMilestone::RetryStarted));
                }
                let message = required_string(object, "error").unwrap_or("provider retry");
                self.observations.push(AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Warning,
                    message: Arc::from(message),
                });
            }
            "compact_boundary" => self
                .observations
                .push(lifecycle(AgentLifecycleMilestone::CompactionCompleted)),
            _ => self.observe_unrecognized(value),
        }
        Ok(())
    }

    fn parse_rate_limit_event(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_matching_session(object)?;
        self.observations.push(AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Warning,
            message: Arc::from("provider rate limit"),
        });
        Ok(())
    }

    fn parse_tool_progress(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_matching_session(object)?;
        let call_id = required_nonempty_string(object, "tool_use_id")
            .ok_or_else(|| self.protocol_failure())?;
        let name =
            required_nonempty_string(object, "tool_name").ok_or_else(|| self.protocol_failure())?;
        self.observations.push(tool_call_observation(
            call_id,
            name,
            AgentToolCallPhase::Updated,
        ));
        Ok(())
    }

    fn parse_result(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if !self.exchange_initialized
            || self.active_message.is_some()
            || self.completed_result_exchange.is_some()
            || self.result_decision_pending
            || self.accepted_result.is_some()
        {
            return Err(self.protocol_failure());
        }
        self.require_matching_session(object)?;
        let subtype =
            required_nonempty_string(object, "subtype").ok_or_else(|| self.protocol_failure())?;
        let is_error = required_bool(object, "is_error").ok_or_else(|| self.protocol_failure())?;
        let terminal_reason = required_nonempty_string(object, "terminal_reason")
            .ok_or_else(|| self.protocol_failure())?;
        let successful = !is_error;
        if successful {
            if subtype != "success"
                || terminal_reason != "completed"
                || required_string(object, "result").is_none()
            {
                return Err(self.protocol_failure());
            }
        } else {
            if terminal_reason == "completed" {
                return Err(self.protocol_failure());
            }
            self.native_failure = Some(AgentHarnessFailureDetail::ModelError);
        }

        let completes_harness = self.value_kind != AgentValueKind::Result;
        if !completes_harness {
            self.completed_result_exchange = Some(if successful {
                self.extract_result_candidate(object)
            } else {
                CompletedResultExchange::NativeFailure
            });
            self.result_decision_pending = successful
                && !matches!(
                    self.completed_result_exchange,
                    Some(CompletedResultExchange::MissingCandidate)
                );
        } else if object.contains_key("structured_output") {
            return Err(self.protocol_failure());
        }

        self.complete_retry();
        self.exchange_initialized = false;
        self.exchange_active = false;
        self.completed_exchanges = self
            .completed_exchanges
            .checked_add(1)
            .ok_or_else(|| self.protocol_failure())?;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::TurnCompleted));
        if completes_harness {
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::HarnessCompleted));
        }
        Ok(())
    }

    fn extract_result_candidate(&self, object: &Map<String, Value>) -> CompletedResultExchange {
        let Some(structured_output) = object.get("structured_output") else {
            return CompletedResultExchange::MissingCandidate;
        };
        let Some(envelope) = structured_output.as_object() else {
            return CompletedResultExchange::AmbiguousCandidate;
        };
        let Some(candidate) = envelope.get("result") else {
            return CompletedResultExchange::AmbiguousCandidate;
        };
        let Some(message) = self.final_main_message.as_ref() else {
            return CompletedResultExchange::AmbiguousCandidate;
        };
        if self.exchange_structured_output_candidates != 1
            || message.completed_blocks != 1
            || message.structured_output_candidates.len() != 1
            || message.structured_output_candidates.first() != Some(structured_output)
            || message.structured_output_acknowledgements != 1
            || message.successful_structured_output_acknowledgements != 1
        {
            return CompletedResultExchange::AmbiguousCandidate;
        }
        CompletedResultExchange::Candidate(Arc::new(candidate.clone()))
    }

    fn complete_retry(&mut self) {
        if self.retry_active {
            self.retry_active = false;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::RetryCompleted));
        }
    }

    fn require_matching_session(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if required_string(object, "session_id")
            == self.session_id.as_ref().map(AsRef::<str>::as_ref)
        {
            Ok(())
        } else {
            self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::SessionCorrelationInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::SessionCorrelation,
            )
        }
    }

    fn observe_unrecognized(&mut self, value: &Value) {
        self.observations
            .push(AgentObservation::UnrecognizedHarnessEvent {
                event: Arc::new(value.clone()),
            });
    }

    fn invalidate_values(&mut self) {
        self.active_message = None;
        self.final_main_message = None;
        self.completed_result_exchange = None;
        self.result_decision_pending = false;
        self.accepted_result = None;
    }

    // Claude initialization, rather than Pi's session-plus-agent-start sequence, owns the
    // start-to-protocol failure transition; keep that authority in this parser.
    // jscpd:ignore-start
    fn protocol_failure(&self) -> AgentFailureCause {
        if self.session_id.is_some() {
            AgentFailureCause::HarnessProtocolFailed
        } else {
            AgentFailureCause::HarnessStartFailed
        }
    }

    fn agent_failure(&self, cause: AgentFailureCause) -> AgentFailure {
        match &self.protocol_rejection {
            Some(rejection) => AgentFailure::with_protocol_rejection(cause, rejection.clone()),
            None => AgentFailure::new(cause),
        }
    }

    fn prepare_rejection(&mut self, stage: ClaudeCodeStreamJsonV1ProtocolStage) {
        self.rejection_context = ClaudeCodeStreamJsonV1RejectionContext {
            stage,
            ..ClaudeCodeStreamJsonV1RejectionContext::default()
        };
        self.rejection_state_snapshot = Some(self.rejection_state());
    }

    fn reject<T>(
        &mut self,
        reason: ClaudeCodeStreamJsonV1RejectionReason,
        stage: ClaudeCodeStreamJsonV1ProtocolStage,
    ) -> Result<T, AgentFailureCause> {
        let cause = self.protocol_failure();
        self.record_rejection(reason, stage);
        Err(cause)
    }

    fn fail_protocol<T>(
        &mut self,
        reason: ClaudeCodeStreamJsonV1RejectionReason,
        stage: ClaudeCodeStreamJsonV1ProtocolStage,
    ) -> Result<T, AgentFailureCause> {
        self.prepare_rejection(stage);
        let failure = self.protocol_failure();
        self.record_rejection(reason, stage);
        self.invalidate_values();
        self.failure = Some(failure.clone());
        Err(failure)
    }

    fn record_rejection(
        &mut self,
        reason: ClaudeCodeStreamJsonV1RejectionReason,
        stage: ClaudeCodeStreamJsonV1ProtocolStage,
    ) {
        self.rejection_context.stage = stage;
        if self.protocol_rejection.is_none() {
            self.protocol_rejection = Some(self.make_protocol_rejection(reason, stage));
        }
    }

    fn make_protocol_rejection(
        &self,
        reason: ClaudeCodeStreamJsonV1RejectionReason,
        stage: ClaudeCodeStreamJsonV1ProtocolStage,
    ) -> AgentProtocolRejectionDiagnostic {
        AgentProtocolRejectionDiagnostic::claude_code_stream_json_v1(
            ClaudeCodeStreamJsonV1ProtocolRejection {
                reason,
                stage,
                outer_event: self.rejection_context.outer_event,
                stream_event: self.rejection_context.stream_event,
                content_index: self.rejection_context.content_index,
                content_block: self.rejection_context.content_block,
                state: self
                    .rejection_state_snapshot
                    .clone()
                    .unwrap_or_else(|| self.rejection_state()),
            },
        )
    }

    fn protocol_failure_outcome(
        &self,
        cause: AgentFailureCause,
        reason: ClaudeCodeStreamJsonV1RejectionReason,
        stage: ClaudeCodeStreamJsonV1ProtocolStage,
    ) -> AgentOutcome {
        AgentOutcome::Failed(AgentFailure::with_protocol_rejection(
            cause,
            AgentProtocolRejectionDiagnostic::claude_code_stream_json_v1(
                ClaudeCodeStreamJsonV1ProtocolRejection {
                    reason,
                    stage,
                    outer_event: None,
                    stream_event: None,
                    content_index: None,
                    content_block: None,
                    state: self.rejection_state(),
                },
            ),
        ))
    }

    fn rejection_state(&self) -> ClaudeCodeStreamJsonV1RejectionState {
        let (
            active_message,
            next_content_index,
            completed_content_blocks,
            open_content_block,
            terminal_message_delta_seen,
        ) = match self.active_message.as_ref() {
            Some(message) => (
                if message.parent_tool_use_id.is_none() {
                    ClaudeCodeStreamJsonV1ActiveMessageState::Main
                } else {
                    ClaudeCodeStreamJsonV1ActiveMessageState::Subagent
                },
                Some(bounded_sequence(message.next_index)),
                Some(bounded_sequence(message.completed_blocks)),
                message.active_block.as_ref().map(|block| {
                    ClaudeCodeStreamJsonV1OpenContentBlockState {
                        kind: active_content_block_kind(block),
                        content_index: bounded_sequence(message.next_index),
                    }
                }),
                Some(message.terminal_delta_seen),
            ),
            None => (
                ClaudeCodeStreamJsonV1ActiveMessageState::None,
                None,
                None,
                None,
                None,
            ),
        };
        ClaudeCodeStreamJsonV1RejectionState {
            initialized: self.session_id.is_some(),
            exchange_initialized: self.exchange_initialized,
            exchange_active: self.exchange_active,
            completed_exchanges: bounded_sequence(self.completed_exchanges),
            active_message,
            next_content_index,
            completed_content_blocks,
            open_content_block,
            terminal_message_delta_seen,
            final_main_message: self.final_main_message.is_some(),
            exchange_structured_output_candidates: bounded_sequence(
                self.exchange_structured_output_candidates,
            ),
            completed_result_exchange: completed_result_exchange_state(
                self.completed_result_exchange.as_ref(),
            ),
            result_decision_pending: self.result_decision_pending,
            result_accepted: self.accepted_result.is_some(),
            native_failure: self.native_failure.is_some(),
            retry_active: self.retry_active,
        }
    }
    // jscpd:ignore-end
}

fn default_rejection_reason(
    stage: ClaudeCodeStreamJsonV1ProtocolStage,
) -> ClaudeCodeStreamJsonV1RejectionReason {
    match stage {
        ClaudeCodeStreamJsonV1ProtocolStage::FrameRead => {
            ClaudeCodeStreamJsonV1RejectionReason::FrameTooLarge
        }
        ClaudeCodeStreamJsonV1ProtocolStage::FrameDecode => {
            ClaudeCodeStreamJsonV1RejectionReason::FrameDecodeFailed
        }
        ClaudeCodeStreamJsonV1ProtocolStage::Initialization => {
            ClaudeCodeStreamJsonV1RejectionReason::InitializationInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ExchangeLifecycle => {
            ClaudeCodeStreamJsonV1RejectionReason::ExchangeLifecycleInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::EventEnvelope => {
            ClaudeCodeStreamJsonV1RejectionReason::EventEnvelopeInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::SessionCorrelation => {
            ClaudeCodeStreamJsonV1RejectionReason::SessionCorrelationInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::StreamEventEnvelope => {
            ClaudeCodeStreamJsonV1RejectionReason::StreamEventEnvelopeInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ActiveStreamParent => {
            ClaudeCodeStreamJsonV1RejectionReason::ActiveStreamParentMismatch
        }
        ClaudeCodeStreamJsonV1ProtocolStage::MessageStart => {
            ClaudeCodeStreamJsonV1RejectionReason::MessageStartInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::MessageTransition => {
            ClaudeCodeStreamJsonV1RejectionReason::MessageTransitionInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStart => {
            ClaudeCodeStreamJsonV1RejectionReason::ContentBlockStartInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta => {
            ClaudeCodeStreamJsonV1RejectionReason::ContentBlockDeltaInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStop => {
            ClaudeCodeStreamJsonV1RejectionReason::ContentBlockStopInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::AssistantEvent => {
            ClaudeCodeStreamJsonV1RejectionReason::AssistantEventInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::UserEvent => {
            ClaudeCodeStreamJsonV1RejectionReason::UserEventInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::SystemEvent => {
            ClaudeCodeStreamJsonV1RejectionReason::SystemEventInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ToolProgressEvent => {
            ClaudeCodeStreamJsonV1RejectionReason::ToolProgressEventInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ResultCorrelation => {
            ClaudeCodeStreamJsonV1RejectionReason::ResultCorrelationInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::TerminalDrain => {
            ClaudeCodeStreamJsonV1RejectionReason::TerminalDrainEventInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::EndOfStream => {
            ClaudeCodeStreamJsonV1RejectionReason::EndOfStreamInvariantInvalid
        }
    }
}

fn claude_event_type(event_type: &str) -> ClaudeCodeStreamJsonV1EventType {
    match event_type {
        "system" => ClaudeCodeStreamJsonV1EventType::System,
        "stream_event" => ClaudeCodeStreamJsonV1EventType::StreamEvent,
        "assistant" => ClaudeCodeStreamJsonV1EventType::Assistant,
        "user" => ClaudeCodeStreamJsonV1EventType::User,
        "rate_limit_event" => ClaudeCodeStreamJsonV1EventType::RateLimitEvent,
        "tool_progress" => ClaudeCodeStreamJsonV1EventType::ToolProgress,
        "result" => ClaudeCodeStreamJsonV1EventType::Result,
        _ => ClaudeCodeStreamJsonV1EventType::Unrecognized,
    }
}

fn claude_stream_event_type(event_type: &str) -> ClaudeCodeStreamJsonV1StreamEventType {
    match event_type {
        "message_start" => ClaudeCodeStreamJsonV1StreamEventType::MessageStart,
        "content_block_start" => ClaudeCodeStreamJsonV1StreamEventType::ContentBlockStart,
        "content_block_delta" => ClaudeCodeStreamJsonV1StreamEventType::ContentBlockDelta,
        "content_block_stop" => ClaudeCodeStreamJsonV1StreamEventType::ContentBlockStop,
        "message_delta" => ClaudeCodeStreamJsonV1StreamEventType::MessageDelta,
        "message_stop" => ClaudeCodeStreamJsonV1StreamEventType::MessageStop,
        _ => ClaudeCodeStreamJsonV1StreamEventType::Unrecognized,
    }
}

fn claude_content_block_kind(block_type: &str) -> ClaudeCodeStreamJsonV1ContentBlockKind {
    match block_type {
        "text" => ClaudeCodeStreamJsonV1ContentBlockKind::Text,
        "thinking" => ClaudeCodeStreamJsonV1ContentBlockKind::Thinking,
        "tool_use" => ClaudeCodeStreamJsonV1ContentBlockKind::ToolUse,
        _ => ClaudeCodeStreamJsonV1ContentBlockKind::Unrecognized,
    }
}

fn claude_delta_content_block_kind(delta_type: &str) -> ClaudeCodeStreamJsonV1ContentBlockKind {
    match delta_type {
        "text_delta" => ClaudeCodeStreamJsonV1ContentBlockKind::Text,
        "thinking_delta" | "signature_delta" => ClaudeCodeStreamJsonV1ContentBlockKind::Thinking,
        "input_json_delta" => ClaudeCodeStreamJsonV1ContentBlockKind::ToolUse,
        _ => ClaudeCodeStreamJsonV1ContentBlockKind::Unrecognized,
    }
}

fn active_content_block_kind(block: &ActiveContentBlock) -> ClaudeCodeStreamJsonV1ContentBlockKind {
    match block {
        ActiveContentBlock::Text { .. } => ClaudeCodeStreamJsonV1ContentBlockKind::Text,
        ActiveContentBlock::Thinking { .. } => ClaudeCodeStreamJsonV1ContentBlockKind::Thinking,
        ActiveContentBlock::ToolUse { .. } => ClaudeCodeStreamJsonV1ContentBlockKind::ToolUse,
        ActiveContentBlock::Unknown { .. } => ClaudeCodeStreamJsonV1ContentBlockKind::Unrecognized,
    }
}

fn completed_result_exchange_state(
    exchange: Option<&CompletedResultExchange>,
) -> ClaudeCodeStreamJsonV1CompletedResultExchangeState {
    match exchange {
        None => ClaudeCodeStreamJsonV1CompletedResultExchangeState::None,
        Some(CompletedResultExchange::Candidate(_)) => {
            ClaudeCodeStreamJsonV1CompletedResultExchangeState::Candidate
        }
        Some(CompletedResultExchange::AmbiguousCandidate) => {
            ClaudeCodeStreamJsonV1CompletedResultExchangeState::AmbiguousCandidate
        }
        Some(CompletedResultExchange::MissingCandidate) => {
            ClaudeCodeStreamJsonV1CompletedResultExchangeState::MissingCandidate
        }
        Some(CompletedResultExchange::NativeFailure) => {
            ClaudeCodeStreamJsonV1CompletedResultExchangeState::NativeFailure
        }
    }
}

fn bounded_sequence(value: u64) -> u64 {
    value.min(MAXIMUM_DIAGNOSTIC_SEQUENCE)
}

struct StreamedMessage {
    id: Arc<str>,
    parent_tool_use_id: Option<Arc<str>>,
    next_index: u64,
    active_block: Option<ActiveContentBlock>,
    completed_blocks: u64,
    text_blocks: u64,
    structured_output_candidates: Vec<Value>,
    structured_output_call_ids: Vec<Arc<str>>,
    structured_output_acknowledgements: u64,
    successful_structured_output_acknowledgements: u64,
    response: String,
    input_tokens: u64,
    output_tokens: u64,
    terminal_delta_seen: bool,
}

enum ActiveContentBlock {
    Text {
        text: String,
        nominal_seen: bool,
    },
    Thinking {
        thinking: String,
        nominal_seen: bool,
    },
    ToolUse {
        call_id: Arc<str>,
        name: Arc<str>,
        initial_input: Value,
        input_json: String,
        input_delta_seen: bool,
        nominal_seen: bool,
    },
    Unknown {
        initial_block: Value,
        delta_seen: bool,
        nominal_seen: bool,
    },
}

impl ActiveContentBlock {
    fn correlate_nominal(&mut self, value: &Value) -> bool {
        let Some(block) = value.as_object() else {
            return false;
        };
        match self {
            Self::Text { text, nominal_seen } => {
                let matches = !*nominal_seen
                    && required_string(block, "type") == Some("text")
                    && required_string(block, "text") == Some(text.as_str());
                *nominal_seen |= matches;
                matches
            }
            Self::Thinking {
                thinking,
                nominal_seen,
            } => {
                let matches = !*nominal_seen
                    && required_string(block, "type") == Some("thinking")
                    && required_string(block, "thinking") == Some(thinking.as_str());
                *nominal_seen |= matches;
                matches
            }
            Self::ToolUse {
                call_id,
                name,
                initial_input,
                input_json,
                input_delta_seen,
                nominal_seen,
            } => {
                let nominal_input = block.get("input");
                let input_matches = if *input_delta_seen {
                    serde_json::from_str::<Value>(input_json).ok().as_ref() == nominal_input
                } else {
                    Some(&*initial_input) == nominal_input
                };
                let matches = !*nominal_seen
                    && required_string(block, "type") == Some("tool_use")
                    && required_nonempty_string(block, "id") == Some(call_id.as_ref())
                    && required_nonempty_string(block, "name") == Some(name.as_ref())
                    && nominal_input.is_some_and(Value::is_object)
                    && input_matches;
                *nominal_seen |= matches;
                matches
            }
            Self::Unknown {
                initial_block,
                delta_seen,
                nominal_seen,
            } => {
                let matches = !*nominal_seen && !*delta_seen && initial_block == value;
                *nominal_seen |= matches;
                matches
            }
        }
    }
}

struct CompletedMainMessage {
    completed_blocks: u64,
    text_blocks: u64,
    structured_output_candidates: Vec<Value>,
    structured_output_acknowledgements: u64,
    successful_structured_output_acknowledgements: u64,
    response: String,
}

fn bounded_append(value: &mut String, addition: &str, maximum_bytes: u64) -> bool {
    let current = u64::try_from(value.len()).unwrap_or(u64::MAX);
    let additional = u64::try_from(addition.len()).unwrap_or(u64::MAX);
    if current
        .checked_add(additional)
        .is_none_or(|total| total > maximum_bytes)
    {
        return false;
    }
    value.push_str(addition);
    true
}

fn normalized_tool_result_content(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Array(blocks) => {
            let mut result = String::new();
            for block in blocks {
                let block = block.as_object()?;
                if required_string(block, "type") != Some("text") {
                    return None;
                }
                result.push_str(required_string(block, "text")?);
            }
            Some(result)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => None,
    }
}

fn parent_tool_use_id(object: &Map<String, Value>) -> Option<Option<&str>> {
    match object.get("parent_tool_use_id")? {
        Value::Null => Some(None),
        Value::String(value) if !value.is_empty() => Some(Some(value)),
        _ => None,
    }
}

fn lifecycle(milestone: AgentLifecycleMilestone) -> AgentObservation {
    AgentObservation::Lifecycle { milestone }
}

fn failed(cause: AgentFailureCause) -> AgentOutcome {
    failed_agent_outcome(cause)
}

// These accessors and native identity checks stay profile-local because their callers
// assign different lifecycle authority and failure timing to superficially similar fields.
// jscpd:ignore-start
fn required_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key)?.as_str()
}

fn required_nonempty_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    required_string(object, key).filter(|value| !value.is_empty())
}

fn required_bool(object: &Map<String, Value>, key: &str) -> Option<bool> {
    object.get(key)?.as_bool()
}

fn required_u64(object: &Map<String, Value>, key: &str) -> Option<u64> {
    object.get(key)?.as_u64()
}

fn required_object<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Option<&'a Map<String, Value>> {
    object.get(key)?.as_object()
}

fn required_array<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a [Value]> {
    object.get(key)?.as_array().map(Vec::as_slice)
}

fn valid_session_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes.get(index) == Some(&b'-'))
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
        && bytes
            .iter()
            .any(|byte| byte.is_ascii_hexdigit() && *byte != b'0')
}
// jscpd:ignore-end

#[cfg(test)]
mod adapter_tests;
#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
