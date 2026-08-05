pub(crate) mod adapter;
mod result_bridge;

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use serde_json::{Map, Number, Value};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::admission::CancellationReason;
use super::agent::{
    AgentDiagnosticLevel, AgentFailureCause, AgentHarnessFailureDetail, AgentLifecycleMilestone,
    AgentObservation, AgentToolCallPhase, AgentValueKind, BoundedAgentResponse,
    BoundedSchemaValidAgentResult, CompletedAgentInvocation,
};

const SESSION_VERSION: u64 = 3;
const MAXIMUM_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const MAXIMUM_RESPONSE_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PiJsonV1ProtocolLimits {
    maximum_frame_bytes: NonZeroU64,
}

impl PiJsonV1ProtocolLimits {
    pub(crate) const fn profile() -> Self {
        let Some(maximum_frame_bytes) = NonZeroU64::new(MAXIMUM_FRAME_BYTES) else {
            unreachable!();
        };
        Self {
            maximum_frame_bytes,
        }
    }

    pub(crate) const fn maximum_frame_bytes(self) -> NonZeroU64 {
        self.maximum_frame_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AcceptedPiJsonV1Result {
    call_id: Arc<str>,
    tool_name: Arc<str>,
    arguments: Arc<Value>,
    result: BoundedSchemaValidAgentResult,
}

impl AcceptedPiJsonV1Result {
    pub(crate) fn new(
        call_id: Arc<str>,
        tool_name: Arc<str>,
        arguments: Arc<Value>,
        result: BoundedSchemaValidAgentResult,
    ) -> Self {
        Self {
            call_id,
            tool_name,
            arguments,
            result,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PiJsonV1ProcessCompletion {
    exit_success: bool,
    cancellation: Option<CancellationReason>,
}

impl PiJsonV1ProcessCompletion {
    pub(crate) const fn exited(exit_success: bool) -> Self {
        Self {
            exit_success,
            cancellation: None,
        }
    }

    pub(crate) const fn cancelled(exit_success: bool, cancellation: CancellationReason) -> Self {
        Self {
            exit_success,
            cancellation: Some(cancellation),
        }
    }
}

pub(crate) struct PiJsonV1Parser {
    expected_cwd: Arc<str>,
    value_kind: AgentValueKind,
    maximum_response_bytes: NonZeroU64,
    limits: PiJsonV1ProtocolLimits,
    frame: Vec<u8>,
    protocol: ProtocolState,
    observations: Vec<AgentObservation>,
    expected_result_tool_name: Option<Arc<str>>,
    seen_tool_call_ids: HashMap<String, bool>,
    retained_tool_call_id_bytes: u64,
    active_validation_request: Option<(Arc<str>, Arc<str>)>,
    accepted_result: Option<AcceptedResultState>,
    failure: Option<AgentFailureCause>,
}

impl PiJsonV1Parser {
    pub(crate) fn profile(expected_cwd: Arc<str>, value_kind: AgentValueKind) -> Self {
        let Some(maximum_response_bytes) = NonZeroU64::new(MAXIMUM_RESPONSE_BYTES) else {
            unreachable!();
        };
        Self::new(
            expected_cwd,
            value_kind,
            maximum_response_bytes,
            PiJsonV1ProtocolLimits::profile(),
            None,
        )
    }

    pub(crate) fn new(
        expected_cwd: Arc<str>,
        value_kind: AgentValueKind,
        maximum_response_bytes: NonZeroU64,
        limits: PiJsonV1ProtocolLimits,
        expected_result_tool_name: Option<Arc<str>>,
    ) -> Self {
        Self {
            expected_cwd,
            value_kind,
            maximum_response_bytes,
            limits,
            frame: Vec::new(),
            protocol: ProtocolState::default(),
            observations: Vec::new(),
            expected_result_tool_name,
            seen_tool_call_ids: HashMap::new(),
            retained_tool_call_id_bytes: 0,
            active_validation_request: None,
            accepted_result: None,
            failure: None,
        }
    }

    /// Consumes arbitrary stdout chunks while retaining at most one bounded frame.
    /// The first returned failure is terminal and tells the process owner to quiesce Pi.
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
                    self.observations.clear();
                    self.failure = Some(failure.clone());
                    return Err(failure);
                }
                for observation in self.observations.drain(..) {
                    observe(observation);
                }
                continue;
            }

            let frame_bytes = u64::try_from(self.frame.len()).unwrap_or(u64::MAX);
            if frame_bytes >= self.limits.maximum_frame_bytes().get() {
                let failure = self.protocol_failure();
                self.failure = Some(failure.clone());
                return Err(failure);
            }
            self.frame.push(byte);
        }
        Ok(())
    }

    /// Records the one result already bounded and validated by the authoritative result bridge.
    /// Native completion remains provisional until the matching transcript, EOF, and exit validate.
    pub(crate) fn correlate_result_request(
        &mut self,
        tool_name: &str,
        call_id: &str,
        arguments: &Value,
    ) -> Result<(), AgentFailureCause> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        if self.value_kind != AgentValueKind::Result
            || self.accepted_result.is_some()
            || self.active_validation_request.is_some()
        {
            return self.fail_protocol();
        }

        let Some(turn) = self
            .protocol
            .active_attempt
            .as_ref()
            .and_then(|attempt| attempt.turn.as_ref())
        else {
            return self.fail_protocol();
        };
        let Some(call) = turn
            .assistant
            .as_ref()
            .and_then(AssistantMessage::only_tool_call)
        else {
            return self.fail_protocol();
        };
        let Some(call_state) = turn.calls.first() else {
            return self.fail_protocol();
        };
        if !call_state.started
            || call_state.ended.is_some()
            || call.id != call_id
            || call.name != tool_name
            || !semantically_equal_json(&call.arguments, arguments)
        {
            return self.fail_protocol();
        }
        self.active_validation_request = Some((Arc::from(call_id), Arc::from(tool_name)));
        Ok(())
    }

    pub(crate) fn accept_result(
        &mut self,
        accepted: AcceptedPiJsonV1Result,
    ) -> Result<(), AgentFailureCause> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        if self.value_kind != AgentValueKind::Result || self.accepted_result.is_some() {
            return self.fail_protocol();
        }

        let Some(attempt) = self.protocol.active_attempt.as_ref() else {
            return self.fail_protocol();
        };
        let Some(turn) = attempt.turn.as_ref() else {
            return self.fail_protocol();
        };
        let Some(assistant) = turn.assistant.as_ref() else {
            return self.fail_protocol();
        };
        let Some(call) = assistant.only_tool_call() else {
            return self.fail_protocol();
        };
        let Some(call_state) = turn.calls.first() else {
            return self.fail_protocol();
        };
        let validation_request_matches =
            self.active_validation_request
                .as_ref()
                .is_some_and(|(call_id, tool_name)| {
                    call_id == &accepted.call_id && tool_name == &accepted.tool_name
                });
        if !validation_request_matches
            || !call_state.started
            || call_state.ended.is_some()
            || call.id.as_str() != accepted.call_id.as_ref()
            || call.name.as_str() != accepted.tool_name.as_ref()
            || !semantically_equal_json(&call.arguments, accepted.arguments.as_ref())
        {
            return self.fail_protocol();
        }

        self.accepted_result = Some(AcceptedResultState {
            accepted,
            native_execution_completed: false,
        });
        Ok(())
    }

    pub(crate) fn accepted_result_ready_for_settlement(&self) -> bool {
        self.accepted_result
            .as_ref()
            .is_some_and(|accepted| accepted.native_execution_completed)
    }

    pub(crate) fn finish(
        mut self,
        completion: PiJsonV1ProcessCompletion,
    ) -> super::agent::AgentOutcome {
        if let Some(reason) = completion.cancellation {
            return super::agent::AgentOutcome::Cancelled { reason };
        }
        if self.failure.is_none() && !self.frame.is_empty() {
            self.failure = Some(self.protocol_failure());
        }

        if let Some(failure) = self.failure {
            return super::agent::AgentOutcome::Failed { cause: failure };
        }
        if let Err(failure) = self.protocol.validate_eof() {
            return super::agent::AgentOutcome::Failed { cause: failure };
        }
        if !completion.exit_success {
            return super::agent::AgentOutcome::Failed {
                cause: AgentFailureCause::HarnessFailed {
                    detail: AgentHarnessFailureDetail::UnsuccessfulExit,
                },
            };
        }
        self.classify_terminal()
    }

    fn parse_frame(&mut self, frame: &[u8]) -> Result<(), AgentFailureCause> {
        let frame_bytes = u64::try_from(frame.len()).unwrap_or(u64::MAX);
        if frame_bytes > self.limits.maximum_frame_bytes().get() {
            return Err(self.protocol_failure());
        }
        let value = serde_json::from_slice::<Value>(frame).map_err(|_| self.protocol_failure())?;
        let object = value.as_object().ok_or_else(|| self.protocol_failure())?;

        if !self.protocol.header_seen {
            self.parse_session_header(object)?;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::SessionEstablished));
            return Ok(());
        }

        let event_type = required_string(object, "type").ok_or_else(|| self.protocol_failure())?;
        if event_type == "session"
            || (self.protocol.settled && known_event_type(event_type))
            || (self.accepted_result.is_some()
                && known_event_type(event_type)
                && !matches!(
                    event_type,
                    "tool_execution_end"
                        | "message_start"
                        | "message_end"
                        | "turn_end"
                        | "agent_end"
                        | "agent_settled"
                ))
        {
            return Err(self.protocol_failure());
        }

        match event_type {
            "agent_start" => self.agent_start(object),
            "agent_end" => self.agent_end(object),
            "agent_settled" => self.agent_settled(object),
            "turn_start" => self.turn_start(object),
            "turn_end" => self.turn_end(object),
            "message_start" => self.message_start(object),
            "message_update" => self.message_update(object),
            "message_end" => self.message_end(object),
            "tool_execution_start" => self.tool_execution_start(object),
            "tool_execution_update" => self.tool_execution_update(object),
            "tool_execution_end" => self.tool_execution_end(object),
            "auto_retry_start" => self.auto_retry_start(object),
            "auto_retry_end" => self.auto_retry_end(object),
            "compaction_start" => self.compaction_start(object),
            "compaction_end" => self.compaction_end(object),
            "summarization_retry_scheduled" => self.summarization_retry_scheduled(object),
            "summarization_retry_attempt_start" => self.summarization_retry_attempt_start(object),
            "summarization_retry_finished" => self.summarization_retry_finished(object),
            "queue_update" => self.queue_update(object),
            "entry_appended" => self.entry_appended(object),
            "session_info_changed" => self.session_info_changed(object),
            "thinking_level_changed" => self.thinking_level_changed(object),
            "bash_execution_update" => self.bash_execution_update(object),
            _ => {
                self.observations
                    .push(AgentObservation::UnrecognizedHarnessEvent {
                        event: Arc::new(value),
                    });
                Ok(())
            }
        }
    }

    fn parse_session_header(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if required_string(object, "type") != Some("session")
            || required_u64(object, "version") != Some(SESSION_VERSION)
            || !required_string(object, "id").is_some_and(valid_session_id)
            || !required_string(object, "timestamp").is_some_and(valid_timestamp)
            || required_string(object, "cwd") != Some(self.expected_cwd.as_ref())
        {
            return Err(AgentFailureCause::HarnessStartFailed);
        }
        self.protocol.header_seen = true;
        Ok(())
    }

    fn agent_start(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if !has_only_required_shape(object, &["type"])
            || self.protocol.settled
            || self.protocol.active_attempt.is_some()
            || self
                .protocol
                .last_agent_end
                .as_ref()
                .is_some_and(|end| end.will_retry && !self.protocol.continuation_allowed)
        {
            return Err(self.protocol_failure());
        }
        let queued_message_required = self
            .protocol
            .last_agent_end
            .as_ref()
            .is_some_and(|end| !end.will_retry)
            && !self.protocol.continuation_allowed;
        self.protocol.active_attempt = Some(ActiveAttempt {
            queued_message_required,
            ..ActiveAttempt::default()
        });
        self.protocol.ever_started = true;
        self.protocol.continuation_allowed = false;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::HarnessStarted));
        Ok(())
    }

    fn agent_end(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let messages = required_array(object, "messages")
            .and_then(|messages| parse_messages(messages, true))
            .ok_or_else(|| self.protocol_failure())?;
        let will_retry =
            required_bool(object, "willRetry").ok_or_else(|| self.protocol_failure())?;
        let Some(attempt) = self.protocol.active_attempt.take() else {
            return Err(self.protocol_failure());
        };
        if attempt.turn.is_some() || attempt.completed_messages != messages {
            return Err(self.protocol_failure());
        }
        let Some(final_assistant) = messages
            .iter()
            .rev()
            .find_map(ParsedMessage::assistant)
            .cloned()
        else {
            return Err(self.protocol_failure());
        };
        if attempt.last_completed_assistant.as_ref() != Some(&final_assistant)
            || attempt.last_turn_assistant.as_ref() != Some(&final_assistant)
        {
            return Err(self.protocol_failure());
        }
        if self.accepted_result.is_some() && will_retry {
            return Err(self.protocol_failure());
        }

        self.protocol.last_agent_end = Some(AgentEndState {
            final_assistant,
            will_retry,
        });
        self.protocol.continuation_allowed = false;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::HarnessCompleted));
        Ok(())
    }

    fn agent_settled(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if !has_only_required_shape(object, &["type"])
            || self.protocol.settled
            || self.protocol.active_attempt.is_some()
            || self.protocol.compaction.is_some()
            || self.protocol.retry_attempt.is_some()
            || self.protocol.continuation_allowed
            || self
                .protocol
                .last_agent_end
                .as_ref()
                .is_none_or(|end| end.will_retry)
        {
            return Err(self.protocol_failure());
        }
        self.protocol.settled = true;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::HarnessQuiescent));
        Ok(())
    }

    fn turn_start(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if !has_only_required_shape(object, &["type"])
            || self.accepted_result.is_some()
            || self.protocol.settled
        {
            return Err(self.protocol_failure());
        }
        let Some(attempt) = self.protocol.active_attempt.as_mut() else {
            return Err(self.protocol_failure());
        };
        if attempt.turn.is_some() {
            return Err(self.protocol_failure());
        }
        attempt.turn = Some(ActiveTurn::default());
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::TurnStarted));
        Ok(())
    }

    fn turn_end(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let assistant = required_value(object, "message")
            .and_then(|message| parse_message(message, true))
            .and_then(|message| message.assistant().cloned())
            .ok_or_else(|| self.protocol_failure())?;
        let tool_results = required_array(object, "toolResults")
            .and_then(parse_tool_results)
            .ok_or_else(|| self.protocol_failure())?;
        let Some(attempt) = self.protocol.active_attempt.as_mut() else {
            return Err(self.protocol_failure());
        };
        let Some(turn) = attempt.turn.take() else {
            return Err(self.protocol_failure());
        };
        if turn.active_message.is_some()
            || turn.assistant.as_ref() != Some(&assistant)
            || !turn.calls.iter().all(ToolCallState::complete)
            || turn
                .calls
                .iter()
                .filter_map(|call| call.result_message.clone())
                .collect::<Vec<_>>()
                != tool_results
        {
            return Err(self.protocol_failure());
        }
        attempt.last_turn_assistant = Some(assistant);
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::TurnCompleted));
        Ok(())
    }

    fn message_start(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let value = required_value(object, "message").ok_or_else(|| self.protocol_failure())?;
        let message = parse_message(value, false).ok_or_else(|| self.protocol_failure())?;
        if let ParsedMessage::Assistant(assistant) = &message {
            if self
                .protocol
                .active_attempt
                .as_ref()
                .is_some_and(|attempt| attempt.queued_message_required)
            {
                return Err(self.protocol_failure());
            }
            self.check_response_bound(assistant)?;
        }
        let Some(turn) = self.active_turn_mut() else {
            return Err(self.protocol_failure());
        };
        if turn.active_message.is_some() {
            return Err(self.protocol_failure());
        }
        match &message {
            ParsedMessage::Assistant(assistant) => {
                if turn.assistant.is_some() || !turn.result_messages_complete() {
                    return Err(self.protocol_failure());
                }
                turn.active_message = Some(ActiveMessage::Assistant {
                    last: assistant.clone(),
                    open_block: None,
                    had_update: false,
                });
            }
            ParsedMessage::ToolResult(result) => {
                if turn.assistant.is_none() || !turn.can_start_tool_result(result) {
                    return Err(self.protocol_failure());
                }
                turn.active_message = Some(ActiveMessage::Fixed(message));
            }
            ParsedMessage::Other(_) => {
                if turn.assistant.is_some() {
                    return Err(self.protocol_failure());
                }
                turn.active_message = Some(ActiveMessage::Fixed(message));
            }
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageStarted));
        Ok(())
    }

    fn message_update(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let message = required_value(object, "message")
            .and_then(|message| parse_message(message, false))
            .and_then(|message| message.assistant().cloned())
            .ok_or_else(|| self.protocol_failure())?;
        let event = required_object(object, "assistantMessageEvent")
            .and_then(parse_assistant_event)
            .ok_or_else(|| self.protocol_failure())?;
        if event.partial != message {
            return Err(self.protocol_failure());
        }
        self.check_response_bound(&message)?;

        let Some(turn) = self.active_turn_mut() else {
            return Err(self.protocol_failure());
        };
        let Some(ActiveMessage::Assistant {
            last,
            open_block,
            had_update,
        }) = turn.active_message.as_mut()
        else {
            return Err(self.protocol_failure());
        };
        if !last.stable_with(&message) || !event.valid_transition(last, open_block) {
            return Err(self.protocol_failure());
        }

        let observations = event.normalized_observations(&message);
        *last = message;
        *had_update = true;
        self.observations.extend(observations);
        Ok(())
    }

    fn message_end(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let value = required_value(object, "message").ok_or_else(|| self.protocol_failure())?;
        let message = parse_message(value, true).ok_or_else(|| self.protocol_failure())?;
        if let ParsedMessage::Assistant(assistant) = &message {
            self.check_response_bound(assistant)?;
            self.retain_result_identity_context(assistant)?;
        }
        let retained_message_bytes = self
            .protocol
            .active_attempt
            .as_ref()
            .and_then(|attempt| {
                attempt
                    .retained_message_bytes
                    .checked_add(json_bytes(value)?)
            })
            .filter(|bytes| *bytes <= self.limits.maximum_frame_bytes().get())
            .ok_or_else(|| self.protocol_failure())?;
        let Some(attempt) = self.protocol.active_attempt.as_mut() else {
            return Err(self.protocol_failure());
        };
        let Some(turn) = attempt.turn.as_mut() else {
            return Err(self.protocol_failure());
        };
        let Some(active) = turn.active_message.take() else {
            return Err(self.protocol_failure());
        };

        let mut completed_queued_message = false;
        match (active, &message) {
            (
                ActiveMessage::Assistant {
                    last,
                    open_block,
                    had_update,
                },
                ParsedMessage::Assistant(assistant),
            ) => {
                if open_block.is_some()
                    || !last.stable_with(assistant)
                    || last.content != assistant.content
                {
                    return Err(self.protocol_failure());
                }
                if !had_update {
                    for block in &assistant.content {
                        match block {
                            ContentBlock::Text(text) if !text.is_empty() => {
                                self.observations.push(AgentObservation::AssistantText {
                                    text: Arc::from(text.as_str()),
                                });
                            }
                            ContentBlock::Thinking(text) if !text.is_empty() => {
                                self.observations.push(AgentObservation::Reasoning {
                                    text: Arc::from(text.as_str()),
                                });
                            }
                            ContentBlock::Text(_)
                            | ContentBlock::Thinking(_)
                            | ContentBlock::ToolCall(_) => {}
                        }
                    }
                }
                turn.calls = assistant
                    .tool_calls()
                    .cloned()
                    .map(ToolCallState::new)
                    .collect();
                turn.assistant = Some(assistant.clone());
                attempt.last_completed_assistant = Some(assistant.clone());
            }
            (ActiveMessage::Fixed(started), ParsedMessage::ToolResult(result)) => {
                if started != message || !turn.finish_tool_result(result) {
                    return Err(self.protocol_failure());
                }
            }
            (ActiveMessage::Fixed(started), ParsedMessage::Other(_)) => {
                if started != message {
                    return Err(self.protocol_failure());
                }
                completed_queued_message = true;
            }
            (ActiveMessage::Fixed(_), ParsedMessage::Assistant(_))
            | (ActiveMessage::Assistant { .. }, ParsedMessage::ToolResult(_))
            | (ActiveMessage::Assistant { .. }, ParsedMessage::Other(_)) => {
                return Err(self.protocol_failure());
            }
        }
        if completed_queued_message {
            attempt.queued_message_required = false;
        }
        let completed_assistant = message.assistant().cloned();
        attempt.completed_messages.push(message);
        attempt.retained_message_bytes = retained_message_bytes;
        if let Some(assistant) = completed_assistant.as_ref() {
            self.observe_assistant_completion(assistant);
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageCompleted));
        Ok(())
    }

    fn retain_result_identity_context(
        &mut self,
        assistant: &AssistantMessage,
    ) -> Result<(), AgentFailureCause> {
        let Some(expected_result_tool_name) = self.expected_result_tool_name.as_ref() else {
            return Ok(());
        };
        for call in assistant.tool_calls() {
            let is_result_tool = call.name == expected_result_tool_name.as_ref();
            if let Some(previous_was_result_tool) = self.seen_tool_call_ids.get(&call.id) {
                if *previous_was_result_tool || is_result_tool {
                    return Err(self.protocol_failure());
                }
                continue;
            }
            let retained_bytes = self
                .retained_tool_call_id_bytes
                .checked_add(u64::try_from(call.id.len()).unwrap_or(u64::MAX))
                .filter(|bytes| *bytes <= self.limits.maximum_frame_bytes().get())
                .ok_or_else(|| self.protocol_failure())?;
            self.seen_tool_call_ids
                .insert(call.id.clone(), is_result_tool);
            self.retained_tool_call_id_bytes = retained_bytes;
        }
        Ok(())
    }

    fn tool_execution_start(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if self.protocol.settled {
            return Err(self.protocol_failure());
        }
        let call_id = required_nonempty_string(object, "toolCallId")
            .ok_or_else(|| self.protocol_failure())?;
        let name =
            required_nonempty_string(object, "toolName").ok_or_else(|| self.protocol_failure())?;
        let arguments = required_value(object, "args").ok_or_else(|| self.protocol_failure())?;
        let Some(turn) = self.active_turn_mut() else {
            return Err(self.protocol_failure());
        };
        if turn.active_message.is_some() || !turn.start_call(call_id, name, arguments) {
            return Err(self.protocol_failure());
        }
        self.observations.push(AgentObservation::ToolCall {
            call_id: Arc::from(call_id),
            name: Arc::from(name),
            phase: AgentToolCallPhase::Started,
        });
        Ok(())
    }

    fn tool_execution_update(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let call_id = required_nonempty_string(object, "toolCallId")
            .ok_or_else(|| self.protocol_failure())?;
        let name =
            required_nonempty_string(object, "toolName").ok_or_else(|| self.protocol_failure())?;
        let arguments = required_value(object, "args").ok_or_else(|| self.protocol_failure())?;
        if required_value(object, "partialResult")
            .and_then(parse_tool_execution_result)
            .is_none()
            || self
                .active_turn_mut()
                .is_none_or(|turn| !turn.update_call(call_id, name, arguments))
        {
            return Err(self.protocol_failure());
        }
        self.observations.push(AgentObservation::ToolCall {
            call_id: Arc::from(call_id),
            name: Arc::from(name),
            phase: AgentToolCallPhase::Updated,
        });
        Ok(())
    }

    fn tool_execution_end(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let call_id = required_nonempty_string(object, "toolCallId")
            .ok_or_else(|| self.protocol_failure())?;
        let name =
            required_nonempty_string(object, "toolName").ok_or_else(|| self.protocol_failure())?;
        let result_value =
            required_value(object, "result").ok_or_else(|| self.protocol_failure())?;
        let result =
            parse_tool_execution_result(result_value).ok_or_else(|| self.protocol_failure())?;
        let is_error = required_bool(object, "isError").ok_or_else(|| self.protocol_failure())?;
        let is_expected_result_tool = self
            .expected_result_tool_name
            .as_ref()
            .is_some_and(|expected| expected.as_ref() == name);
        let retained_tool_bytes = self
            .protocol
            .active_attempt
            .as_ref()
            .and_then(|attempt| attempt.turn.as_ref())
            .and_then(|turn| {
                turn.retained_tool_bytes
                    .checked_add(json_bytes(result_value)?)
            })
            .filter(|bytes| *bytes <= self.limits.maximum_frame_bytes().get())
            .ok_or_else(|| self.protocol_failure())?;
        let Some(turn) = self.active_turn_mut() else {
            return Err(self.protocol_failure());
        };
        if !turn.end_call(call_id, name, result.clone(), is_error) {
            return Err(self.protocol_failure());
        }
        turn.retained_tool_bytes = retained_tool_bytes;

        let active_validation_matches = match self.active_validation_request.as_ref() {
            Some((active_id, active_name)) => {
                if active_id.as_ref() != call_id || active_name.as_ref() != name {
                    return Err(self.protocol_failure());
                }
                true
            }
            None => false,
        };
        if active_validation_matches {
            let accepted_validation_matches =
                self.accepted_result.as_ref().is_some_and(|accepted| {
                    accepted.accepted.call_id.as_ref() == call_id
                        && accepted.accepted.tool_name.as_ref() == name
                });
            if !accepted_validation_matches && (!is_error || result.terminate == Some(true)) {
                return Err(self.protocol_failure());
            }
            self.active_validation_request = None;
        } else if is_expected_result_tool && (!is_error || result.terminate == Some(true)) {
            return Err(self.protocol_failure());
        }

        if let Some(accepted) = self.accepted_result.as_mut()
            && accepted.accepted.call_id.as_ref() == call_id
            && accepted.accepted.tool_name.as_ref() == name
        {
            if is_error || result.terminate != Some(true) {
                return Err(self.protocol_failure());
            }
            accepted.native_execution_completed = true;
        }

        self.observations.push(AgentObservation::ToolCall {
            call_id: Arc::from(call_id),
            name: Arc::from(name),
            phase: AgentToolCallPhase::Completed,
        });
        self.observations.push(AgentObservation::ToolResult {
            call_id: Arc::from(call_id),
            is_error,
            content: Arc::from(result.text()),
        });
        Ok(())
    }

    fn auto_retry_start(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let (attempt, max_attempts, delay, error) =
            retry_schedule(object).ok_or_else(|| self.protocol_failure())?;
        if attempt > max_attempts
            || self.protocol.active_attempt.is_some()
            || self.protocol.settled
            || self
                .protocol
                .last_agent_end
                .as_ref()
                .is_none_or(|end| !end.will_retry)
            || self
                .protocol
                .retry_attempt
                .is_some_and(|previous| attempt != previous + 1)
        {
            return Err(self.protocol_failure());
        }
        self.protocol.retry_attempt = Some(attempt);
        self.protocol.continuation_allowed = true;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::RetryStarted));
        self.observations.push(AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Warning,
            message: Arc::from(format!(
                "retry {attempt}/{max_attempts} after {delay} ms: {error}"
            )),
        });
        Ok(())
    }

    fn auto_retry_end(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let success = required_bool(object, "success").ok_or_else(|| self.protocol_failure())?;
        let attempt =
            required_positive_u64(object, "attempt").ok_or_else(|| self.protocol_failure())?;
        let final_error =
            optional_string(object, "finalError").ok_or_else(|| self.protocol_failure())?;
        if self.protocol.retry_attempt != Some(attempt)
            || (success && self.protocol.active_attempt.is_none())
            || (!success && self.protocol.active_attempt.is_some())
        {
            return Err(self.protocol_failure());
        }
        self.protocol.retry_attempt = None;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::RetryCompleted));
        self.observe_optional_error(final_error);
        Ok(())
    }

    fn compaction_start(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let reason = required_compaction_reason(object).ok_or_else(|| self.protocol_failure())?;
        if self.protocol.active_attempt.is_some()
            || self.protocol.compaction.is_some()
            || self.protocol.last_agent_end.is_none()
            || self.protocol.settled
        {
            return Err(self.protocol_failure());
        }
        self.protocol.compaction = Some(reason);
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::CompactionStarted));
        Ok(())
    }

    fn compaction_end(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let reason = required_compaction_reason(object).ok_or_else(|| self.protocol_failure())?;
        let aborted = required_bool(object, "aborted").ok_or_else(|| self.protocol_failure())?;
        let will_retry =
            required_bool(object, "willRetry").ok_or_else(|| self.protocol_failure())?;
        let error =
            optional_string(object, "errorMessage").ok_or_else(|| self.protocol_failure())?;
        if self.protocol.compaction != Some(reason)
            || object
                .get("result")
                .is_some_and(|result| !result.is_object())
            || (aborted && will_retry)
        {
            return Err(self.protocol_failure());
        }
        self.protocol.compaction = None;
        self.protocol.continuation_allowed = will_retry;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::CompactionCompleted));
        self.observe_optional_error(error);
        Ok(())
    }

    fn summarization_retry_scheduled(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let (attempt, max_attempts, delay, error) =
            retry_schedule(object).ok_or_else(|| self.protocol_failure())?;
        if self.protocol.compaction.is_none() || attempt > max_attempts {
            return Err(self.protocol_failure());
        }
        self.protocol.summarization_retry_pending = true;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::RetryStarted));
        self.observations.push(AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Warning,
            message: Arc::from(format!(
                "summarization retry {attempt}/{max_attempts} after {delay} ms: {error}"
            )),
        });
        Ok(())
    }

    fn summarization_retry_attempt_start(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let source = required_string(object, "source").ok_or_else(|| self.protocol_failure())?;
        let valid_source = match source {
            "branchSummary" => true,
            "compaction" => required_compaction_reason(object).is_some(),
            _ => false,
        };
        if !valid_source || !self.protocol.summarization_retry_pending {
            return Err(self.protocol_failure());
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::RetryStarted));
        Ok(())
    }

    fn summarization_retry_finished(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if !has_only_required_shape(object, &["type"]) || !self.protocol.summarization_retry_pending
        {
            return Err(self.protocol_failure());
        }
        self.protocol.summarization_retry_pending = false;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::RetryCompleted));
        Ok(())
    }

    fn queue_update(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if !required_array(object, "steering").is_some_and(array_of_strings)
            || !required_array(object, "followUp").is_some_and(array_of_strings)
        {
            return Err(self.protocol_failure());
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::QueueUpdated));
        Ok(())
    }

    fn entry_appended(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if !required_value(object, "entry").is_some_and(Value::is_object) {
            return Err(self.protocol_failure());
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageCompleted));
        Ok(())
    }

    fn session_info_changed(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if object
            .get("name")
            .is_some_and(|name| !name.is_string() && !name.is_null())
        {
            return Err(self.protocol_failure());
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageUpdated));
        Ok(())
    }

    fn thinking_level_changed(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if !matches!(
            required_string(object, "level"),
            Some("off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max")
        ) {
            return Err(self.protocol_failure());
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageUpdated));
        Ok(())
    }

    fn bash_execution_update(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if required_string(object, "delta").is_none()
            || object.get("id").is_some_and(|id| !id.is_string())
        {
            return Err(self.protocol_failure());
        }
        self.observations.push(AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Information,
            message: Arc::from(required_string(object, "delta").unwrap_or_default()),
        });
        Ok(())
    }

    fn active_turn_mut(&mut self) -> Option<&mut ActiveTurn> {
        self.protocol.active_attempt.as_mut()?.turn.as_mut()
    }

    fn observe_optional_error(&mut self, error: Option<&str>) {
        if let Some(error) = error {
            self.observations.push(AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Error,
                message: Arc::from(error),
            });
        }
    }

    fn check_response_bound(&self, assistant: &AssistantMessage) -> Result<(), AgentFailureCause> {
        if self.value_kind == AgentValueKind::Response
            && assistant.text_bytes() > self.maximum_response_bytes.get()
        {
            return Err(AgentFailureCause::CapturedValueTooLarge);
        }
        Ok(())
    }

    fn observe_assistant_completion(&mut self, assistant: &AssistantMessage) {
        self.observations.push(AgentObservation::Model {
            name: Arc::from(assistant.model.as_str()),
        });
        self.observations.push(AgentObservation::Usage {
            input_tokens: assistant.usage.input,
            output_tokens: assistant.usage.output,
        });
        for diagnostic in &assistant.diagnostics {
            self.observations.push(AgentObservation::Diagnostic {
                level: if diagnostic.error_message.is_some() {
                    AgentDiagnosticLevel::Error
                } else {
                    AgentDiagnosticLevel::Information
                },
                message: Arc::from(
                    diagnostic
                        .error_message
                        .as_deref()
                        .unwrap_or(diagnostic.kind.as_str()),
                ),
            });
        }
        if let Some(error) = &assistant.error_message {
            self.observations.push(AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Error,
                message: Arc::from(error.as_str()),
            });
        }
    }

    fn classify_terminal(&self) -> super::agent::AgentOutcome {
        let Some(end) = self.protocol.last_agent_end.as_ref() else {
            return super::agent::AgentOutcome::Failed {
                cause: self.protocol_failure(),
            };
        };
        let assistant = &end.final_assistant;

        if self.accepted_result.is_some() && assistant.stop_reason != StopReason::ToolUse {
            return super::agent::AgentOutcome::Failed {
                cause: AgentFailureCause::HarnessProtocolFailed,
            };
        }

        match assistant.stop_reason {
            StopReason::Stop => match self.value_kind {
                AgentValueKind::None => {
                    super::agent::AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
                }
                AgentValueKind::Response => {
                    let text_blocks = assistant.text_blocks().collect::<Vec<_>>();
                    if text_blocks.is_empty() {
                        super::agent::AgentOutcome::Failed {
                            cause: AgentFailureCause::MissingResponse,
                        }
                    } else {
                        let response = text_blocks.concat();
                        super::agent::AgentOutcome::Completed(CompletedAgentInvocation::Response(
                            BoundedAgentResponse::from_bounded(Arc::from(response)),
                        ))
                    }
                }
                AgentValueKind::Result => super::agent::AgentOutcome::Failed {
                    cause: AgentFailureCause::MissingResult,
                },
            },
            StopReason::Length => harness_failure(AgentHarnessFailureDetail::ModelOutputTruncated),
            StopReason::ToolUse => match self.value_kind {
                AgentValueKind::None | AgentValueKind::Response => {
                    harness_failure(AgentHarnessFailureDetail::UnexpectedTerminalToolUse)
                }
                AgentValueKind::Result => {
                    let Some(accepted) = self.accepted_result.as_ref() else {
                        return super::agent::AgentOutcome::Failed {
                            cause: AgentFailureCause::MissingResult,
                        };
                    };
                    let Some(call) = assistant.only_tool_call() else {
                        return super::agent::AgentOutcome::Failed {
                            cause: AgentFailureCause::HarnessProtocolFailed,
                        };
                    };
                    if !accepted.native_execution_completed
                        || call.id.as_str() != accepted.accepted.call_id.as_ref()
                        || call.name.as_str() != accepted.accepted.tool_name.as_ref()
                        || !semantically_equal_json(
                            &call.arguments,
                            accepted.accepted.arguments.as_ref(),
                        )
                    {
                        return super::agent::AgentOutcome::Failed {
                            cause: AgentFailureCause::HarnessProtocolFailed,
                        };
                    }
                    super::agent::AgentOutcome::Completed(CompletedAgentInvocation::Result(
                        accepted.accepted.result.clone(),
                    ))
                }
            },
            StopReason::Error => harness_failure(AgentHarnessFailureDetail::ModelError),
            StopReason::Aborted => harness_failure(AgentHarnessFailureDetail::ModelAborted),
            StopReason::Pending => super::agent::AgentOutcome::Failed {
                cause: AgentFailureCause::HarnessProtocolFailed,
            },
        }
    }

    fn protocol_failure(&self) -> AgentFailureCause {
        if self.protocol.ever_started {
            AgentFailureCause::HarnessProtocolFailed
        } else {
            AgentFailureCause::HarnessStartFailed
        }
    }

    fn fail_protocol<T>(&mut self) -> Result<T, AgentFailureCause> {
        let failure = self.protocol_failure();
        self.failure = Some(failure.clone());
        Err(failure)
    }
}

#[derive(Default)]
struct ProtocolState {
    header_seen: bool,
    ever_started: bool,
    active_attempt: Option<ActiveAttempt>,
    last_agent_end: Option<AgentEndState>,
    continuation_allowed: bool,
    retry_attempt: Option<u64>,
    compaction: Option<CompactionReason>,
    summarization_retry_pending: bool,
    settled: bool,
}

impl ProtocolState {
    fn validate_eof(&self) -> Result<(), AgentFailureCause> {
        let failure = if self.ever_started {
            AgentFailureCause::HarnessProtocolFailed
        } else {
            AgentFailureCause::HarnessStartFailed
        };
        if !self.header_seen || !self.ever_started {
            return Err(AgentFailureCause::HarnessStartFailed);
        }
        if !self.settled
            || self.active_attempt.is_some()
            || self.retry_attempt.is_some()
            || self.compaction.is_some()
            || self.summarization_retry_pending
            || self.continuation_allowed
            || self
                .last_agent_end
                .as_ref()
                .is_none_or(|end| end.will_retry)
        {
            return Err(failure);
        }
        Ok(())
    }
}

#[derive(Default)]
struct ActiveAttempt {
    queued_message_required: bool,
    turn: Option<ActiveTurn>,
    completed_messages: Vec<ParsedMessage>,
    retained_message_bytes: u64,
    last_completed_assistant: Option<AssistantMessage>,
    last_turn_assistant: Option<AssistantMessage>,
}

#[derive(Default)]
struct ActiveTurn {
    active_message: Option<ActiveMessage>,
    assistant: Option<AssistantMessage>,
    calls: Vec<ToolCallState>,
    retained_tool_bytes: u64,
}

impl ActiveTurn {
    fn result_messages_complete(&self) -> bool {
        self.calls.iter().all(|call| call.result_message.is_some())
    }

    fn start_call(&mut self, id: &str, name: &str, arguments: &Value) -> bool {
        let Some(call) = self.calls.iter_mut().find(|call| !call.started) else {
            return false;
        };
        if call.call.id != id
            || call.call.name != name
            || !semantically_equal_json(&call.call.arguments, arguments)
        {
            return false;
        }
        call.started = true;
        true
    }

    fn update_call(&self, id: &str, name: &str, arguments: &Value) -> bool {
        self.calls.iter().any(|call| {
            call.started
                && call.ended.is_none()
                && call.call.id == id
                && call.call.name == name
                && semantically_equal_json(&call.call.arguments, arguments)
        })
    }

    fn end_call(
        &mut self,
        id: &str,
        name: &str,
        result: ToolExecutionResult,
        is_error: bool,
    ) -> bool {
        let Some(call) = self.calls.iter_mut().find(|call| call.call.id == id) else {
            return false;
        };
        if !call.started || call.ended.is_some() || call.call.name != name {
            return false;
        }
        call.ended = Some(CompletedToolExecution { result, is_error });
        true
    }

    fn can_start_tool_result(&self, result: &ToolResultMessage) -> bool {
        let Some(expected) = self.calls.iter().find(|call| call.result_message.is_none()) else {
            return false;
        };
        expected.matches_result_message(result)
    }

    fn finish_tool_result(&mut self, result: &ToolResultMessage) -> bool {
        let Some(expected) = self
            .calls
            .iter_mut()
            .find(|call| call.result_message.is_none())
        else {
            return false;
        };
        if !expected.matches_result_message(result) {
            return false;
        }
        expected.result_message = Some(result.clone());
        true
    }
}

enum ActiveMessage {
    Assistant {
        last: AssistantMessage,
        open_block: Option<OpenBlock>,
        had_update: bool,
    },
    Fixed(ParsedMessage),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpenBlock {
    Text(usize),
    Thinking(usize),
    ToolCall(usize),
}

#[derive(Clone)]
struct AgentEndState {
    final_assistant: AssistantMessage,
    will_retry: bool,
}

struct AcceptedResultState {
    accepted: AcceptedPiJsonV1Result,
    native_execution_completed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ParsedMessage {
    Assistant(AssistantMessage),
    ToolResult(ToolResultMessage),
    Other(Value),
}

impl ParsedMessage {
    fn assistant(&self) -> Option<&AssistantMessage> {
        match self {
            Self::Assistant(message) => Some(message),
            Self::ToolResult(_) | Self::Other(_) => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AssistantMessage {
    content: Vec<ContentBlock>,
    api: String,
    provider: String,
    model: String,
    response_model: Option<String>,
    response_id: Option<String>,
    diagnostics: Vec<AssistantDiagnostic>,
    usage: Usage,
    stop_reason: StopReason,
    error_message: Option<String>,
    timestamp: Number,
}

impl AssistantMessage {
    fn stable_with(&self, other: &Self) -> bool {
        self.api == other.api
            && self.provider == other.provider
            && self.model == other.model
            && self.timestamp == other.timestamp
    }

    fn text_bytes(&self) -> u64 {
        self.content.iter().fold(0_u64, |total, block| {
            let bytes = match block {
                ContentBlock::Text(text) => u64::try_from(text.len()).unwrap_or(u64::MAX),
                ContentBlock::Thinking(_) | ContentBlock::ToolCall(_) => 0,
            };
            total.saturating_add(bytes)
        })
    }

    fn text_blocks(&self) -> impl Iterator<Item = &str> {
        self.content.iter().filter_map(|block| match block {
            ContentBlock::Text(text) => Some(text.as_str()),
            ContentBlock::Thinking(_) | ContentBlock::ToolCall(_) => None,
        })
    }

    fn tool_calls(&self) -> impl Iterator<Item = &ToolCall> {
        self.content.iter().filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(call),
            ContentBlock::Text(_) | ContentBlock::Thinking(_) => None,
        })
    }

    fn only_tool_call(&self) -> Option<&ToolCall> {
        let mut calls = self.tool_calls();
        let call = calls.next()?;
        calls.next().is_none().then_some(call)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AssistantDiagnostic {
    kind: String,
    timestamp: Number,
    error_message: Option<String>,
    error: Option<Value>,
    details: Option<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ContentBlock {
    Text(String),
    Thinking(String),
    ToolCall(ToolCall),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ToolCall {
    id: String,
    name: String,
    arguments: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Usage {
    input: u64,
    output: u64,
    cache_read: u64,
    cache_write: u64,
    cache_write_1h: Option<u64>,
    reasoning: Option<u64>,
    total_tokens: u64,
    cost: Cost,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Cost {
    input: Number,
    output: Number,
    cache_read: Number,
    cache_write: Number,
    total: Number,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StopReason {
    Pending,
    Stop,
    Length,
    ToolUse,
    Error,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ToolResultMessage {
    call_id: String,
    name: String,
    content: Vec<MediaBlock>,
    details: Option<Value>,
    usage: Option<Usage>,
    added_tool_names: Option<Vec<String>>,
    is_error: bool,
    timestamp: Number,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MediaBlock {
    Text(String),
    Image { data: String, media_type: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ToolExecutionResult {
    content: Vec<MediaBlock>,
    details: Option<Value>,
    usage: Option<Usage>,
    added_tool_names: Option<Vec<String>>,
    terminate: Option<bool>,
}

impl ToolExecutionResult {
    fn text(&self) -> String {
        self.content
            .iter()
            .filter_map(|block| match block {
                MediaBlock::Text(text) => Some(text.as_str()),
                MediaBlock::Image { .. } => None,
            })
            .collect()
    }
}

struct ToolCallState {
    call: ToolCall,
    started: bool,
    ended: Option<CompletedToolExecution>,
    result_message: Option<ToolResultMessage>,
}

impl ToolCallState {
    fn new(call: ToolCall) -> Self {
        Self {
            call,
            started: false,
            ended: None,
            result_message: None,
        }
    }

    fn complete(&self) -> bool {
        self.started && self.ended.is_some() && self.result_message.is_some()
    }

    fn matches_result_message(&self, message: &ToolResultMessage) -> bool {
        let Some(execution) = &self.ended else {
            return false;
        };
        self.call.id == message.call_id
            && self.call.name == message.name
            && execution.is_error == message.is_error
            && execution.result.content == message.content
            && execution.result.details == message.details
            && execution.result.usage == message.usage
            && execution.result.added_tool_names == message.added_tool_names
    }
}

struct CompletedToolExecution {
    result: ToolExecutionResult,
    is_error: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompactionReason {
    Manual,
    Threshold,
    Overflow,
}

struct AssistantUpdateEvent {
    kind: AssistantUpdateKind,
    index: usize,
    partial: AssistantMessage,
}

enum AssistantUpdateKind {
    TextStart,
    TextDelta(String),
    TextEnd(String),
    ThinkingStart,
    ThinkingDelta(String),
    ThinkingEnd(String),
    ToolCallStart,
    ToolCallDelta,
    ToolCallEnd(ToolCall),
}

impl AssistantUpdateEvent {
    fn valid_transition(&self, previous: &AssistantMessage, open: &mut Option<OpenBlock>) -> bool {
        let current = &self.partial;
        match &self.kind {
            AssistantUpdateKind::TextStart => {
                start_block(previous, current, self.index, BlockKind::Text)
                    && set_open(open, OpenBlock::Text(self.index))
            }
            AssistantUpdateKind::TextDelta(delta) => {
                *open == Some(OpenBlock::Text(self.index))
                    && append_text(previous, current, self.index, delta, BlockKind::Text)
            }
            AssistantUpdateKind::TextEnd(content) => {
                let block = ContentBlock::Text(content.clone());
                *open == Some(OpenBlock::Text(self.index))
                    && previous.content.get(self.index) == Some(&block)
                    && current.content.get(self.index) == Some(&block)
                    && unchanged_except(previous, current, self.index)
                    && clear_open(open)
            }
            AssistantUpdateKind::ThinkingStart => {
                start_block(previous, current, self.index, BlockKind::Thinking)
                    && set_open(open, OpenBlock::Thinking(self.index))
            }
            AssistantUpdateKind::ThinkingDelta(delta) => {
                *open == Some(OpenBlock::Thinking(self.index))
                    && append_text(previous, current, self.index, delta, BlockKind::Thinking)
            }
            AssistantUpdateKind::ThinkingEnd(content) => {
                let block = ContentBlock::Thinking(content.clone());
                *open == Some(OpenBlock::Thinking(self.index))
                    && previous.content.get(self.index) == Some(&block)
                    && current.content.get(self.index) == Some(&block)
                    && unchanged_except(previous, current, self.index)
                    && clear_open(open)
            }
            AssistantUpdateKind::ToolCallStart => {
                start_block(previous, current, self.index, BlockKind::ToolCall)
                    && set_open(open, OpenBlock::ToolCall(self.index))
            }
            AssistantUpdateKind::ToolCallDelta => {
                *open == Some(OpenBlock::ToolCall(self.index))
                    && matches!(
                        current.content.get(self.index),
                        Some(ContentBlock::ToolCall(_))
                    )
                    && unchanged_except(previous, current, self.index)
            }
            AssistantUpdateKind::ToolCallEnd(call) => {
                *open == Some(OpenBlock::ToolCall(self.index))
                    && current.content.get(self.index)
                        == Some(&ContentBlock::ToolCall(call.clone()))
                    && unchanged_except(previous, current, self.index)
                    && clear_open(open)
            }
        }
    }

    fn normalized_observations(&self, message: &AssistantMessage) -> Vec<AgentObservation> {
        match &self.kind {
            AssistantUpdateKind::TextDelta(delta) => vec![AgentObservation::AssistantText {
                text: Arc::from(delta.as_str()),
            }],
            AssistantUpdateKind::ThinkingDelta(delta) => vec![AgentObservation::Reasoning {
                text: Arc::from(delta.as_str()),
            }],
            AssistantUpdateKind::ToolCallStart => message
                .content
                .get(self.index)
                .and_then(|block| match block {
                    ContentBlock::ToolCall(call) => Some(AgentObservation::ToolCall {
                        call_id: Arc::from(call.id.as_str()),
                        name: Arc::from(call.name.as_str()),
                        phase: AgentToolCallPhase::Started,
                    }),
                    ContentBlock::Text(_) | ContentBlock::Thinking(_) => None,
                })
                .into_iter()
                .collect(),
            AssistantUpdateKind::ToolCallDelta => message
                .content
                .get(self.index)
                .and_then(|block| match block {
                    ContentBlock::ToolCall(call) => Some(AgentObservation::ToolCall {
                        call_id: Arc::from(call.id.as_str()),
                        name: Arc::from(call.name.as_str()),
                        phase: AgentToolCallPhase::Updated,
                    }),
                    ContentBlock::Text(_) | ContentBlock::Thinking(_) => None,
                })
                .into_iter()
                .collect(),
            AssistantUpdateKind::ToolCallEnd(call) => vec![AgentObservation::ToolCall {
                call_id: Arc::from(call.id.as_str()),
                name: Arc::from(call.name.as_str()),
                phase: AgentToolCallPhase::Completed,
            }],
            AssistantUpdateKind::TextStart
            | AssistantUpdateKind::TextEnd(_)
            | AssistantUpdateKind::ThinkingStart
            | AssistantUpdateKind::ThinkingEnd(_) => {
                vec![lifecycle(AgentLifecycleMilestone::MessageUpdated)]
            }
        }
    }
}

#[derive(Clone, Copy)]
enum BlockKind {
    Text,
    Thinking,
    ToolCall,
}

fn parse_messages(values: &[Value], complete: bool) -> Option<Vec<ParsedMessage>> {
    values
        .iter()
        .map(|value| parse_message(value, complete))
        .collect()
}

fn parse_tool_results(values: &[Value]) -> Option<Vec<ToolResultMessage>> {
    values
        .iter()
        .map(|value| match parse_message(value, true)? {
            ParsedMessage::ToolResult(result) => Some(result),
            ParsedMessage::Assistant(_) | ParsedMessage::Other(_) => None,
        })
        .collect()
}

fn parse_message(value: &Value, complete: bool) -> Option<ParsedMessage> {
    let object = value.as_object()?;
    match required_string(object, "role")? {
        "assistant" => parse_assistant_message(object, complete).map(ParsedMessage::Assistant),
        "toolResult" => parse_tool_result_message(object).map(ParsedMessage::ToolResult),
        role => parse_non_assistant_message(object, role).map(ParsedMessage::Other),
    }
}

fn parse_non_assistant_message(object: &Map<String, Value>, role: &str) -> Option<Value> {
    let (required, optional): (&[&str], &[&str]) = match role {
        "user" => {
            valid_user_content(object.get("content")?)?;
            (&["role", "content", "timestamp"], &[])
        }
        "custom" => {
            required_nonempty_string(object, "customType")?;
            valid_user_content(object.get("content")?)?;
            required_bool(object, "display")?;
            (
                &["role", "customType", "content", "display", "timestamp"],
                &["details"],
            )
        }
        "bashExecution" => {
            required_string(object, "command")?;
            required_string(object, "output")?;
            required_bool(object, "cancelled")?;
            required_bool(object, "truncated")?;
            if object
                .get("exitCode")
                .is_some_and(|value| !value.is_i64() && !value.is_u64())
                || object
                    .get("fullOutputPath")
                    .is_some_and(|value| !value.is_string())
                || object
                    .get("excludeFromContext")
                    .is_some_and(|value| !value.is_boolean())
            {
                return None;
            }
            (
                &[
                    "role",
                    "command",
                    "output",
                    "cancelled",
                    "truncated",
                    "timestamp",
                ],
                &["exitCode", "fullOutputPath", "excludeFromContext"],
            )
        }
        "branchSummary" => {
            required_string(object, "summary")?;
            required_nonempty_string(object, "fromId")?;
            (&["role", "summary", "fromId", "timestamp"], &[])
        }
        "compactionSummary" => {
            required_string(object, "summary")?;
            required_u64(object, "tokensBefore")?;
            (&["role", "summary", "tokensBefore", "timestamp"], &[])
        }
        _ => return None,
    };
    object.get("timestamp")?.as_number()?;
    normalized_object(object, required, optional)
}

fn valid_user_content(value: &Value) -> Option<()> {
    match value {
        Value::String(_) => Some(()),
        Value::Array(content)
            if content
                .iter()
                .all(|block| parse_media_block(block).is_some()) =>
        {
            Some(())
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_) => {
            None
        }
    }
}

fn normalized_object(
    object: &Map<String, Value>,
    required: &[&str],
    optional: &[&str],
) -> Option<Value> {
    let mut normalized = Map::new();
    for key in required {
        normalized.insert((*key).to_owned(), object.get(*key)?.clone());
    }
    for key in optional {
        if let Some(value) = object.get(*key) {
            normalized.insert((*key).to_owned(), value.clone());
        }
    }
    Some(Value::Object(normalized))
}

fn parse_assistant_message(
    object: &Map<String, Value>,
    complete: bool,
) -> Option<AssistantMessage> {
    let content = required_array(object, "content")?
        .iter()
        .map(|block| parse_content_block(block, complete))
        .collect::<Option<Vec<_>>>()?;
    let mut call_ids = std::collections::HashSet::new();
    if !content.iter().all(|block| match block {
        ContentBlock::ToolCall(call) if complete => call_ids.insert(call.id.as_str()),
        ContentBlock::Text(_) | ContentBlock::Thinking(_) | ContentBlock::ToolCall(_) => true,
    }) {
        return None;
    }
    Some(AssistantMessage {
        content,
        api: required_nonempty_string(object, "api")?.to_owned(),
        provider: required_nonempty_string(object, "provider")?.to_owned(),
        model: required_nonempty_string(object, "model")?.to_owned(),
        response_model: optional_string(object, "responseModel")?.map(str::to_owned),
        response_id: optional_string(object, "responseId")?.map(str::to_owned),
        diagnostics: parse_diagnostics(object)?,
        usage: required_object(object, "usage").and_then(parse_usage)?,
        stop_reason: parse_stop_reason(required_string(object, "stopReason")?, complete)?,
        error_message: optional_string(object, "errorMessage")?.map(str::to_owned),
        timestamp: object.get("timestamp")?.as_number()?.clone(),
    })
}

fn parse_diagnostics(object: &Map<String, Value>) -> Option<Vec<AssistantDiagnostic>> {
    match object.get("diagnostics") {
        None => Some(Vec::new()),
        Some(Value::Array(diagnostics)) => diagnostics
            .iter()
            .map(|diagnostic| {
                let diagnostic = diagnostic.as_object()?;
                let error = match diagnostic.get("error") {
                    None => None,
                    Some(Value::Object(error)) => {
                        required_string(error, "message")?;
                        for key in ["name", "stack"] {
                            if error.get(key).is_some_and(|value| !value.is_string()) {
                                return None;
                            }
                        }
                        if error
                            .get("code")
                            .is_some_and(|value| !value.is_string() && !value.is_number())
                        {
                            return None;
                        }
                        Some(Value::Object(error.clone()))
                    }
                    Some(_) => return None,
                };
                let details = match diagnostic.get("details") {
                    None => None,
                    Some(Value::Object(details)) => Some(Value::Object(details.clone())),
                    Some(_) => return None,
                };
                Some(AssistantDiagnostic {
                    kind: required_nonempty_string(diagnostic, "type")?.to_owned(),
                    timestamp: diagnostic.get("timestamp")?.as_number()?.clone(),
                    error_message: error
                        .as_ref()
                        .and_then(Value::as_object)
                        .and_then(|error| required_string(error, "message"))
                        .map(str::to_owned),
                    error,
                    details,
                })
            })
            .collect(),
        Some(_) => None,
    }
}

fn parse_content_block(value: &Value, complete: bool) -> Option<ContentBlock> {
    let object = value.as_object()?;
    match required_string(object, "type")? {
        "text" => Some(ContentBlock::Text(
            required_string(object, "text")?.to_owned(),
        )),
        "thinking" => Some(ContentBlock::Thinking(
            required_string(object, "thinking")?.to_owned(),
        )),
        "toolCall" => parse_tool_call(object, complete).map(ContentBlock::ToolCall),
        _ => None,
    }
}

fn parse_tool_call(object: &Map<String, Value>, complete: bool) -> Option<ToolCall> {
    let id = required_string(object, "id")?;
    let name = required_string(object, "name")?;
    if complete && (id.is_empty() || name.is_empty()) {
        return None;
    }
    let arguments = required_value(object, "arguments")?;
    if !arguments.is_object() {
        return None;
    }
    Some(ToolCall {
        id: id.to_owned(),
        name: name.to_owned(),
        arguments: arguments.clone(),
    })
}

fn parse_usage(object: &Map<String, Value>) -> Option<Usage> {
    Some(Usage {
        input: required_u64(object, "input")?,
        output: required_u64(object, "output")?,
        cache_read: required_u64(object, "cacheRead")?,
        cache_write: required_u64(object, "cacheWrite")?,
        cache_write_1h: optional_u64(object, "cacheWrite1h")?,
        reasoning: optional_u64(object, "reasoning")?,
        total_tokens: required_u64(object, "totalTokens")?,
        cost: required_object(object, "cost").and_then(parse_cost)?,
    })
}

fn parse_cost(object: &Map<String, Value>) -> Option<Cost> {
    Some(Cost {
        input: nonnegative_number(object, "input")?,
        output: nonnegative_number(object, "output")?,
        cache_read: nonnegative_number(object, "cacheRead")?,
        cache_write: nonnegative_number(object, "cacheWrite")?,
        total: nonnegative_number(object, "total")?,
    })
}

fn parse_stop_reason(value: &str, complete: bool) -> Option<StopReason> {
    match value {
        "pending" if !complete => Some(StopReason::Pending),
        "stop" => Some(StopReason::Stop),
        "length" => Some(StopReason::Length),
        "toolUse" => Some(StopReason::ToolUse),
        "error" => Some(StopReason::Error),
        "aborted" => Some(StopReason::Aborted),
        _ => None,
    }
}

fn parse_tool_result_message(object: &Map<String, Value>) -> Option<ToolResultMessage> {
    Some(ToolResultMessage {
        call_id: required_nonempty_string(object, "toolCallId")?.to_owned(),
        name: required_nonempty_string(object, "toolName")?.to_owned(),
        content: required_array(object, "content")?
            .iter()
            .map(parse_media_block)
            .collect::<Option<Vec<_>>>()?,
        details: object.get("details").cloned(),
        usage: parse_optional_usage(object)?,
        added_tool_names: optional_string_array(object, "addedToolNames")?,
        is_error: required_bool(object, "isError")?,
        timestamp: object.get("timestamp")?.as_number()?.clone(),
    })
}

fn parse_media_block(value: &Value) -> Option<MediaBlock> {
    let object = value.as_object()?;
    match required_string(object, "type")? {
        "text" => Some(MediaBlock::Text(
            required_string(object, "text")?.to_owned(),
        )),
        "image" => Some(MediaBlock::Image {
            data: required_string(object, "data")?.to_owned(),
            media_type: required_nonempty_string(object, "mimeType")?.to_owned(),
        }),
        _ => None,
    }
}

fn parse_tool_execution_result(value: &Value) -> Option<ToolExecutionResult> {
    let object = value.as_object()?;
    let content = match object.get("content") {
        Some(Value::Array(content)) => content
            .iter()
            .map(parse_media_block)
            .collect::<Option<Vec<_>>>()?,
        None | Some(Value::Null) => Vec::new(),
        Some(_) => return None,
    };
    Some(ToolExecutionResult {
        content,
        details: object.get("details").cloned(),
        usage: parse_optional_usage(object)?,
        added_tool_names: optional_string_array(object, "addedToolNames")?,
        terminate: optional_bool(object, "terminate")?,
    })
}

fn parse_assistant_event(object: &Map<String, Value>) -> Option<AssistantUpdateEvent> {
    let partial = required_value(object, "partial")
        .and_then(|value| parse_message(value, false))
        .and_then(|message| message.assistant().cloned())?;
    let index = usize::try_from(required_u64(object, "contentIndex")?).ok()?;
    let kind = match required_string(object, "type")? {
        "text_start" => AssistantUpdateKind::TextStart,
        "text_delta" => {
            AssistantUpdateKind::TextDelta(required_string(object, "delta")?.to_owned())
        }
        "text_end" => AssistantUpdateKind::TextEnd(required_string(object, "content")?.to_owned()),
        "thinking_start" => AssistantUpdateKind::ThinkingStart,
        "thinking_delta" => {
            AssistantUpdateKind::ThinkingDelta(required_string(object, "delta")?.to_owned())
        }
        "thinking_end" => {
            AssistantUpdateKind::ThinkingEnd(required_string(object, "content")?.to_owned())
        }
        "toolcall_start" => AssistantUpdateKind::ToolCallStart,
        "toolcall_delta" => {
            required_string(object, "delta")?;
            AssistantUpdateKind::ToolCallDelta
        }
        "toolcall_end" => AssistantUpdateKind::ToolCallEnd(
            required_object(object, "toolCall").and_then(|call| parse_tool_call(call, true))?,
        ),
        _ => return None,
    };
    Some(AssistantUpdateEvent {
        kind,
        index,
        partial,
    })
}

fn start_block(
    previous: &AssistantMessage,
    current: &AssistantMessage,
    index: usize,
    kind: BlockKind,
) -> bool {
    if index != previous.content.len() || current.content.len() != previous.content.len() + 1 {
        return false;
    }
    let expected = match kind {
        BlockKind::Text => {
            matches!(current.content.get(index), Some(ContentBlock::Text(text)) if text.is_empty())
        }
        BlockKind::Thinking => {
            matches!(current.content.get(index), Some(ContentBlock::Thinking(text)) if text.is_empty())
        }
        BlockKind::ToolCall => {
            matches!(current.content.get(index), Some(ContentBlock::ToolCall(_)))
        }
    };
    expected && previous.content == current.content[..index]
}

fn append_text(
    previous: &AssistantMessage,
    current: &AssistantMessage,
    index: usize,
    delta: &str,
    kind: BlockKind,
) -> bool {
    if !unchanged_except(previous, current, index) {
        return false;
    }
    match (
        previous.content.get(index),
        current.content.get(index),
        kind,
    ) {
        (Some(ContentBlock::Text(before)), Some(ContentBlock::Text(after)), BlockKind::Text)
        | (
            Some(ContentBlock::Thinking(before)),
            Some(ContentBlock::Thinking(after)),
            BlockKind::Thinking,
        ) => after.strip_prefix(before) == Some(delta),
        _ => false,
    }
}

fn unchanged_except(previous: &AssistantMessage, current: &AssistantMessage, index: usize) -> bool {
    previous.content.len() == current.content.len()
        && previous
            .content
            .iter()
            .zip(&current.content)
            .enumerate()
            .all(|(candidate, (before, after))| candidate == index || before == after)
}

fn set_open(open: &mut Option<OpenBlock>, value: OpenBlock) -> bool {
    if open.is_some() {
        return false;
    }
    *open = Some(value);
    true
}

fn clear_open(open: &mut Option<OpenBlock>) -> bool {
    *open = None;
    true
}

fn lifecycle(milestone: AgentLifecycleMilestone) -> AgentObservation {
    AgentObservation::Lifecycle { milestone }
}

fn harness_failure(detail: AgentHarnessFailureDetail) -> super::agent::AgentOutcome {
    super::agent::AgentOutcome::Failed {
        cause: AgentFailureCause::HarnessFailed { detail },
    }
}

fn json_bytes(value: &Value) -> Option<u64> {
    serde_json::to_vec(value)
        .ok()
        .and_then(|bytes| u64::try_from(bytes.len()).ok())
}

fn semantically_equal_json(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(left), Value::Bool(right)) => left == right,
        (Value::Number(left), Value::Number(right)) => {
            normalized_json_number(left) == normalized_json_number(right)
        }
        (Value::String(left), Value::String(right)) => left == right,
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| semantically_equal_json(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| semantically_equal_json(left, right))
                })
        }
        (Value::Null, _)
        | (Value::Bool(_), _)
        | (Value::Number(_), _)
        | (Value::String(_), _)
        | (Value::Array(_), _)
        | (Value::Object(_), _) => false,
    }
}

fn normalized_json_number(number: &Number) -> Option<(bool, String, i64)> {
    let rendered = number.to_string();
    let (negative, unsigned) = rendered
        .strip_prefix('-')
        .map_or((false, rendered.as_str()), |unsigned| (true, unsigned));
    let (coefficient, exponent) =
        unsigned
            .split_once(['e', 'E'])
            .map_or((unsigned, 0_i64), |(coefficient, exponent)| {
                exponent
                    .parse::<i64>()
                    .map(|exponent| (coefficient, exponent))
                    .unwrap_or((coefficient, i64::MIN))
            });
    if exponent == i64::MIN {
        return None;
    }
    let (whole, fraction) = coefficient
        .split_once('.')
        .map_or((coefficient, ""), |parts| parts);
    let mut digits = String::with_capacity(whole.len().saturating_add(fraction.len()));
    digits.push_str(whole);
    digits.push_str(fraction);
    let first_nonzero = digits.find(|digit| digit != '0').unwrap_or(digits.len());
    digits.drain(..first_nonzero);
    if digits.is_empty() {
        return Some((false, "0".to_owned(), 0));
    }
    let trailing_zeroes = digits
        .len()
        .saturating_sub(digits.trim_end_matches('0').len());
    digits.truncate(digits.len().saturating_sub(trailing_zeroes));
    let fraction_digits = i64::try_from(fraction.len()).ok()?;
    let trailing_zeroes = i64::try_from(trailing_zeroes).ok()?;
    let power = exponent
        .checked_sub(fraction_digits)?
        .checked_add(trailing_zeroes)?;
    Some((negative, digits, power))
}

fn retry_schedule(object: &Map<String, Value>) -> Option<(u64, u64, u64, &str)> {
    Some((
        required_positive_u64(object, "attempt")?,
        required_positive_u64(object, "maxAttempts")?,
        required_u64(object, "delayMs")?,
        required_string(object, "errorMessage")?,
    ))
}

fn required_value<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    object.get(key)
}

fn required_object<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Option<&'a Map<String, Value>> {
    object.get(key)?.as_object()
}

fn optional_object<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Option<Option<&'a Map<String, Value>>> {
    match object.get(key) {
        None => Some(None),
        Some(value) => value.as_object().map(Some),
    }
}

fn required_array<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a [Value]> {
    object.get(key)?.as_array().map(Vec::as_slice)
}

fn required_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key)?.as_str()
}

fn required_nonempty_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    required_string(object, key).filter(|value| !value.is_empty())
}

fn optional_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<Option<&'a str>> {
    match object.get(key) {
        None => Some(None),
        Some(value) => value.as_str().map(Some),
    }
}

fn required_bool(object: &Map<String, Value>, key: &str) -> Option<bool> {
    object.get(key)?.as_bool()
}

fn optional_bool(object: &Map<String, Value>, key: &str) -> Option<Option<bool>> {
    match object.get(key) {
        None => Some(None),
        Some(value) => value.as_bool().map(Some),
    }
}

fn required_u64(object: &Map<String, Value>, key: &str) -> Option<u64> {
    object.get(key)?.as_u64()
}

fn required_positive_u64(object: &Map<String, Value>, key: &str) -> Option<u64> {
    required_u64(object, key).filter(|value| *value > 0)
}

fn optional_u64(object: &Map<String, Value>, key: &str) -> Option<Option<u64>> {
    match object.get(key) {
        None => Some(None),
        Some(value) => value.as_u64().map(Some),
    }
}

fn nonnegative_number(object: &Map<String, Value>, key: &str) -> Option<Number> {
    let number = object.get(key)?.as_number()?.clone();
    number
        .as_f64()
        .filter(|number| number.is_finite() && *number >= 0.0)
        .map(|_| number)
}

fn optional_string_array(object: &Map<String, Value>, key: &str) -> Option<Option<Vec<String>>> {
    match object.get(key) {
        None => Some(None),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| value.as_str().map(str::to_owned))
            .collect::<Option<Vec<_>>>()
            .map(|values| (!values.is_empty()).then_some(values)),
        Some(_) => None,
    }
}

fn known_event_type(event_type: &str) -> bool {
    matches!(
        event_type,
        "agent_start"
            | "agent_end"
            | "agent_settled"
            | "turn_start"
            | "turn_end"
            | "message_start"
            | "message_update"
            | "message_end"
            | "tool_execution_start"
            | "tool_execution_update"
            | "tool_execution_end"
            | "auto_retry_start"
            | "auto_retry_end"
            | "compaction_start"
            | "compaction_end"
            | "summarization_retry_scheduled"
            | "summarization_retry_attempt_start"
            | "summarization_retry_finished"
            | "queue_update"
            | "entry_appended"
            | "session_info_changed"
            | "thinking_level_changed"
            | "bash_execution_update"
    )
}

fn required_compaction_reason(object: &Map<String, Value>) -> Option<CompactionReason> {
    match required_string(object, "reason")? {
        "manual" => Some(CompactionReason::Manual),
        "threshold" => Some(CompactionReason::Threshold),
        "overflow" => Some(CompactionReason::Overflow),
        _ => None,
    }
}

fn array_of_strings(values: &[Value]) -> bool {
    values.iter().all(Value::is_string)
}

fn has_only_required_shape(object: &Map<String, Value>, required: &[&str]) -> bool {
    required.iter().all(|key| object.contains_key(*key))
}

fn valid_timestamp(value: &str) -> bool {
    OffsetDateTime::parse(value, &Rfc3339).is_ok()
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

fn parse_optional_usage(object: &Map<String, Value>) -> Option<Option<Usage>> {
    match optional_object(object, "usage")? {
        None => Some(None),
        Some(usage) => parse_usage(usage).map(Some),
    }
}

#[cfg(test)]
pub(super) struct PendingResultValidation;

#[cfg(test)]
impl crate::execution::workflow::result_validation::RunningResultValidation
    for PendingResultValidation
{
    async fn wait(
        &mut self,
    ) -> Result<crate::execution::workflow::result_validation::ValidationWorkerDecision, ()> {
        std::future::pending().await
    }

    fn request_stop(&mut self) {}

    fn quiesce(self) -> impl std::future::Future<Output = ()> + Send {
        std::future::ready(())
    }
}

#[cfg(test)]
mod adapter_tests;
#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
mod tests;
