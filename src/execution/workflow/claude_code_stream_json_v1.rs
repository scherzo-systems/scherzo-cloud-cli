pub(crate) mod adapter;

use std::ffi::OsString;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

use serde_json::{Map, Value, json};

use super::agent::{
    AgentDiagnosticLevel, AgentFailureCause, AgentHarnessFailureDetail, AgentLifecycleMilestone,
    AgentObservation, AgentOutcome, AgentToolCallPhase, AgentValueKind, BoundedAgentResponse,
    CompletedAgentInvocation, tool_call_observation,
};
use crate::execution::claude_code::CLAUDE_CODE_STREAM_JSON_V1_VERSION;

const MAXIMUM_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const PERMISSION_MODE: &str = "bypassPermissions";

pub(crate) const FIXED_INVOCATION_ENVIRONMENT: [(&str, &str); 5] = [
    ("DISABLE_UPDATES", "1"),
    ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
    ("CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL", "1"),
    ("CLAUDE_CODE_DISABLE_AUTO_MEMORY", "1"),
    ("CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS", "1"),
];

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
        OsString::from("--no-session-persistence"),
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

pub(crate) fn initial_user_text_frame(message: &str) -> Result<Vec<u8>, serde_json::Error> {
    let mut frame = serde_json::to_vec(&json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [{
                "type": "text",
                "text": message,
            }],
        },
    }))?;
    frame.push(b'\n');
    Ok(frame)
}

pub(crate) struct ClaudeCodeStreamJsonV1Parser {
    expected_cwd: Arc<str>,
    expected_model: Arc<str>,
    value_kind: AgentValueKind,
    maximum_response_bytes: NonZeroU64,
    limits: ClaudeCodeStreamJsonV1ProtocolLimits,
    frame: Vec<u8>,
    session_id: Option<Arc<str>>,
    exchange_initialized: bool,
    exchange_active: bool,
    completed_exchanges: u64,
    active_message: Option<StreamedMessage>,
    final_main_message: Option<CompletedMainMessage>,
    native_failure: Option<AgentHarnessFailureDetail>,
    retry_active: bool,
    observations: Vec<AgentObservation>,
    failure: Option<AgentFailureCause>,
}

impl ClaudeCodeStreamJsonV1Parser {
    pub(crate) fn profile(
        expected_cwd: Arc<str>,
        expected_model: Arc<str>,
        value_kind: AgentValueKind,
        maximum_response_bytes: NonZeroU64,
    ) -> Self {
        Self::new(
            expected_cwd,
            expected_model,
            value_kind,
            maximum_response_bytes,
            ClaudeCodeStreamJsonV1ProtocolLimits::profile(),
        )
    }

    fn new(
        expected_cwd: Arc<str>,
        expected_model: Arc<str>,
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
            session_id: None,
            exchange_initialized: false,
            exchange_active: true,
            completed_exchanges: 0,
            active_message: None,
            final_main_message: None,
            native_failure: None,
            retry_active: false,
            observations: Vec::new(),
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
        {
            return self.fail_protocol();
        }
        self.exchange_active = true;
        self.final_main_message = None;
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
                return self.fail_protocol();
            }
            self.frame.push(byte);
        }
        Ok(())
    }
    // jscpd:ignore-end

    pub(crate) fn finish(mut self, exit_success: bool) -> AgentOutcome {
        if self.failure.is_none() && !self.frame.is_empty() {
            self.failure = Some(self.protocol_failure());
            self.invalidate_values();
        }
        if let Some(failure) = self.failure {
            return failed(failure);
        }
        if self.session_id.is_none() {
            return failed(AgentFailureCause::HarnessStartFailed);
        }
        if self.exchange_active
            || self.exchange_initialized
            || self.active_message.is_some()
            || self.completed_exchanges == 0
        {
            return failed(AgentFailureCause::HarnessProtocolFailed);
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
            AgentValueKind::Result => failed(AgentFailureCause::MissingResult),
        }
    }

    fn parse_frame(&mut self, frame: &[u8]) -> Result<(), AgentFailureCause> {
        // This profile owns decoding failure classification because Claude's init boundary
        // differs from Pi's session-header and agent-start boundaries.
        // jscpd:ignore-start
        let frame_bytes = u64::try_from(frame.len()).unwrap_or(u64::MAX);
        if frame_bytes > self.limits.maximum_frame_bytes().get() {
            return Err(self.protocol_failure());
        }
        let value = serde_json::from_slice::<Value>(frame).map_err(|_| self.protocol_failure())?;
        let object = value.as_object().ok_or_else(|| self.protocol_failure())?;
        // jscpd:ignore-end

        if self.session_id.is_none() {
            return self.parse_initialization(object);
        }

        let event_type = required_string(object, "type").ok_or_else(|| self.protocol_failure())?;
        if event_type == "system" && required_string(object, "subtype") == Some("init") {
            return self.parse_initialization(object);
        }

        if !self.exchange_active {
            return self.parse_terminal_drain(event_type, object, &value);
        }
        if !self.exchange_initialized {
            return Err(self.protocol_failure());
        }

        match event_type {
            "stream_event" => self.parse_stream_event(object, &value),
            "assistant" => self.parse_assistant(object, &value),
            "user" => self.parse_user(object, &value),
            "system" => self.parse_system(object, &value),
            "rate_limit_event" => self.parse_rate_limit_event(object),
            "tool_progress" => self.parse_tool_progress(object),
            "result" => self.parse_result(object),
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
                self.parse_system(object, value)
            }
            "stream_event" | "assistant" | "user" | "tool_progress" | "result" => {
                Err(self.protocol_failure())
            }
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
            || !session_id.is_some_and(valid_session_id)
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
        self.require_matching_session(object)?;
        let parent_tool_use_id =
            parent_tool_use_id(object).ok_or_else(|| self.protocol_failure())?;
        let event = required_object(object, "event").ok_or_else(|| self.protocol_failure())?;
        let event_type = required_string(event, "type").ok_or_else(|| self.protocol_failure())?;
        if event_type != "message_start" {
            self.require_active_stream_parent(parent_tool_use_id)?;
        }
        match event_type {
            "message_start" => self.stream_message_start(event, parent_tool_use_id),
            "content_block_start" => self.stream_content_block_start(event, value),
            "content_block_delta" => self.stream_content_block_delta(event, value),
            "content_block_stop" => self.stream_content_block_stop(event),
            "message_delta" => self.stream_message_delta(event),
            "message_stop" => self.stream_message_stop(event),
            _ => {
                self.observe_unrecognized(value);
                Ok(())
            }
        }
    }

    fn require_active_stream_parent(
        &self,
        parent_tool_use_id: Option<&str>,
    ) -> Result<(), AgentFailureCause> {
        let Some(message) = self.active_message.as_ref() else {
            return Err(self.protocol_failure());
        };
        if message.parent_tool_use_id.as_deref() != parent_tool_use_id {
            return Err(self.protocol_failure());
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
        if self.retry_active {
            self.retry_active = false;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::RetryCompleted));
        }
        self.active_message = Some(StreamedMessage {
            id: Arc::from(id),
            parent_tool_use_id: parent_tool_use_id.map(Arc::from),
            next_index: 0,
            active_block: None,
            text_blocks: 0,
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
        let index = required_u64(event, "index").ok_or_else(|| self.protocol_failure())?;
        let block =
            required_object(event, "content_block").ok_or_else(|| self.protocol_failure())?;
        let block_type = required_string(block, "type").ok_or_else(|| self.protocol_failure())?;
        let Some(message) = self.active_message.as_ref() else {
            return Err(self.protocol_failure());
        };
        if message.active_block.is_some() || index != message.next_index {
            return Err(self.protocol_failure());
        }

        let maximum_correlation_bytes = self.limits.maximum_frame_bytes().get();
        let active_block = match block_type {
            "text" => {
                let text = required_string(block, "text").ok_or_else(|| self.protocol_failure())?;
                let mut streamed_text = String::new();
                if !bounded_append(&mut streamed_text, text, maximum_correlation_bytes) {
                    return Err(self.protocol_failure());
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
                    return Err(self.protocol_failure());
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
        let index = required_u64(event, "index").ok_or_else(|| self.protocol_failure())?;
        let delta = required_object(event, "delta").ok_or_else(|| self.protocol_failure())?;
        let delta_type = required_string(delta, "type").ok_or_else(|| self.protocol_failure())?;
        let Some(message) = self.active_message.as_ref() else {
            return Err(self.protocol_failure());
        };
        if index != message.next_index {
            return Err(self.protocol_failure());
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
                    return Err(failure);
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
                    return Err(failure);
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
                    return Err(failure);
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
            _ => Err(self.protocol_failure()),
        }
    }

    fn stream_content_block_stop(
        &mut self,
        event: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let index = required_u64(event, "index").ok_or_else(|| self.protocol_failure())?;
        let Some(message) = self.active_message.as_mut() else {
            return Err(self.protocol_failure());
        };
        if index != message.next_index {
            return Err(self.protocol_failure());
        }
        let Some(block) = message.active_block.take() else {
            return Err(self.protocol_failure());
        };
        let Some(next_index) = message.next_index.checked_add(1) else {
            return Err(AgentFailureCause::HarnessProtocolFailed);
        };
        message.next_index = next_index;
        if let ActiveContentBlock::ToolUse { call_id, name, .. } = block {
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
                text_blocks: message.text_blocks,
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
            || required_string(message, "model") != Some(self.expected_model.as_ref())
        {
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
            let is_error =
                required_bool(block, "is_error").ok_or_else(|| self.protocol_failure())?;
            let result = block
                .get("content")
                .and_then(normalized_tool_result_content)
                .ok_or_else(|| self.protocol_failure())?;
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
        if !self.exchange_initialized || self.active_message.is_some() {
            return Err(self.protocol_failure());
        }
        self.require_matching_session(object)?;
        let subtype =
            required_nonempty_string(object, "subtype").ok_or_else(|| self.protocol_failure())?;
        let is_error = required_bool(object, "is_error").ok_or_else(|| self.protocol_failure())?;
        let terminal_reason = required_nonempty_string(object, "terminal_reason")
            .ok_or_else(|| self.protocol_failure())?;
        if !is_error {
            if subtype != "success"
                || terminal_reason != "completed"
                || required_string(object, "result").is_none()
            {
                return Err(self.protocol_failure());
            }
        } else if subtype == "success" {
            return Err(self.protocol_failure());
        } else {
            self.native_failure = Some(AgentHarnessFailureDetail::ModelError);
        }
        if self.retry_active {
            self.retry_active = false;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::RetryCompleted));
        }
        self.exchange_initialized = false;
        self.exchange_active = false;
        self.completed_exchanges = self
            .completed_exchanges
            .checked_add(1)
            .ok_or_else(|| self.protocol_failure())?;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::TurnCompleted));
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::HarnessCompleted));
        Ok(())
    }

    fn require_matching_session(
        &self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if required_string(object, "session_id")
            == self.session_id.as_ref().map(AsRef::<str>::as_ref)
        {
            Ok(())
        } else {
            Err(self.protocol_failure())
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

    fn fail_protocol<T>(&mut self) -> Result<T, AgentFailureCause> {
        let failure = self.protocol_failure();
        self.invalidate_values();
        self.failure = Some(failure.clone());
        Err(failure)
    }
    // jscpd:ignore-end
}

struct StreamedMessage {
    id: Arc<str>,
    parent_tool_use_id: Option<Arc<str>>,
    next_index: u64,
    active_block: Option<ActiveContentBlock>,
    text_blocks: u64,
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
    text_blocks: u64,
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
    AgentOutcome::Failed { cause }
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
