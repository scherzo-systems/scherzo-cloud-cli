pub(crate) mod adapter;
mod input_transport;
mod result_bridge;

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};

use super::admission::CancellationReason;
use super::agent::{
    AgentDiagnosticLevel, AgentFailure, AgentFailureCause, AgentHarnessFailureDetail,
    AgentLifecycleMilestone, AgentObservation, AgentProtocolRejectionDiagnostic,
    AgentToolCallPhase, AgentValueKind, BoundedAgentResponse, CapturedJson,
    CompletedAgentInvocation, failed_agent_outcome, tool_call_observation,
};
use super::strict_json;

const SESSION_VERSION: u64 = 3;
const MAXIMUM_FRAME_BYTES: u64 = 16 * 1024 * 1024;
#[cfg(test)]
const MAXIMUM_RESPONSE_BYTES: u64 = 1024 * 1024;
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PiJsonV1ProtocolRejection {
    reason: PiJsonV1RejectionReason,
    stage: PiJsonV1ProtocolStage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outer_event: Option<PiJsonV1EventType>,
    state: PiJsonV1RejectionState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiJsonV1ProtocolStage {
    FrameRead,
    FrameDecode,
    SessionHeader,
    EventEnvelope,
    EventPayload,
    ResultCorrelation,
    TerminalValidation,
    EndOfStream,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiJsonV1RejectionReason {
    FrameTooLarge,
    FrameDecodeFailed,
    FrameNotObject,
    SessionHeaderInvalid,
    SessionEventRepeated,
    EventAfterSettlement,
    EventAfterResultAcceptance,
    EventShapeInvalid,
    EventTransitionInvalid,
    ResultCorrelationInvalid,
    TerminalInvariantInvalid,
    EndOfStreamInvariantInvalid,
    PartialFrameAtEndOfStream,
    RetainedStateLimitExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiJsonV1EventType {
    Session,
    AgentStart,
    AgentEnd,
    AgentSettled,
    TurnStart,
    TurnEnd,
    MessageStart,
    MessageUpdate,
    MessageEnd,
    ToolExecutionStart,
    ToolExecutionUpdate,
    ToolExecutionEnd,
    AutoRetryStart,
    AutoRetryEnd,
    CompactionStart,
    CompactionEnd,
    SummarizationRetryScheduled,
    SummarizationRetryAttemptStart,
    SummarizationRetryFinished,
    QueueUpdate,
    EntryAppended,
    SessionInfoChanged,
    ThinkingLevelChanged,
    BashExecutionUpdate,
    Unrecognized,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PiJsonV1RejectionState {
    session_header_seen: bool,
    agent_started: bool,
    terminal_candidate_retained: bool,
    result_accepted: bool,
    settled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PiJsonV1RejectionContext {
    stage: PiJsonV1ProtocolStage,
    outer_event: Option<PiJsonV1EventType>,
}

impl Default for PiJsonV1RejectionContext {
    fn default() -> Self {
        Self {
            stage: PiJsonV1ProtocolStage::FrameRead,
            outer_event: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PiJsonV1ProtocolLimits {
    maximum_frame_bytes: NonZeroU64,
}

impl PiJsonV1ProtocolLimits {
    pub(crate) const fn profile() -> Self {
        let maximum_frame_bytes = match NonZeroU64::new(MAXIMUM_FRAME_BYTES) {
            Some(maximum_frame_bytes) => maximum_frame_bytes,
            None => NonZeroU64::MIN,
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
    result: CapturedJson,
}

impl AcceptedPiJsonV1Result {
    pub(crate) fn new(
        call_id: Arc<str>,
        tool_name: Arc<str>,
        arguments: Arc<Value>,
        result: CapturedJson,
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
    reconstruction: Option<ActiveMessage>,
    observations: Vec<AgentObservation>,
    expected_result_tool_name: Option<Arc<str>>,
    result_calls: HashMap<String, ResultCallState>,
    retained_result_state_bytes: u64,
    active_validation_request: Option<(Arc<str>, Arc<str>)>,
    accepted_result: Option<AcceptedResultState>,
    rejection_context: PiJsonV1RejectionContext,
    rejection_state_snapshot: Option<PiJsonV1RejectionState>,
    protocol_rejection: Option<AgentProtocolRejectionDiagnostic>,
    failure: Option<AgentFailure>,
}

impl PiJsonV1Parser {
    #[cfg(test)]
    pub(crate) fn profile(expected_cwd: Arc<str>, value_kind: AgentValueKind) -> Self {
        let maximum_response_bytes =
            NonZeroU64::new(MAXIMUM_RESPONSE_BYTES).unwrap_or(NonZeroU64::MIN);
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
            reconstruction: None,
            observations: Vec::new(),
            expected_result_tool_name,
            result_calls: HashMap::new(),
            retained_result_state_bytes: 0,
            active_validation_request: None,
            accepted_result: None,
            rejection_context: PiJsonV1RejectionContext::default(),
            rejection_state_snapshot: None,
            protocol_rejection: None,
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
            return Err(failure.cause().clone());
        }

        for &byte in bytes {
            if byte == b'\n' {
                let frame = std::mem::take(&mut self.frame);
                if let Err(cause) = self.parse_frame(&frame) {
                    self.observations.clear();
                    self.failure = Some(self.agent_failure(cause.clone()));
                    return Err(cause);
                }
                for observation in self.observations.drain(..) {
                    observe(observation);
                }
                continue;
            }

            let frame_bytes = u64::try_from(self.frame.len()).unwrap_or(u64::MAX);
            if frame_bytes >= self.limits.maximum_frame_bytes().get() {
                let cause = self.protocol_failure();
                self.record_rejection(
                    PiJsonV1RejectionReason::FrameTooLarge,
                    PiJsonV1ProtocolStage::FrameRead,
                );
                self.failure = Some(self.agent_failure(cause.clone()));
                return Err(cause);
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
        if self.try_correlate_result_request(tool_name, call_id, arguments)? {
            return Ok(());
        }
        self.fail_protocol()
    }

    pub(super) fn try_correlate_result_request(
        &mut self,
        tool_name: &str,
        call_id: &str,
        arguments: &Value,
    ) -> Result<bool, AgentFailureCause> {
        if let Some(failure) = &self.failure {
            return Err(failure.cause().clone());
        }
        if self.value_kind != AgentValueKind::Result
            || self.accepted_result.is_some()
            || self.active_validation_request.is_some()
        {
            return self.fail_protocol();
        }

        if self
            .result_calls
            .iter()
            .any(|(other_id, call)| other_id != call_id && call.started && !call.ended)
        {
            return self.fail_protocol();
        }
        let Some(call) = self.result_calls.get(call_id) else {
            return Ok(false);
        };
        if !call.started {
            return Ok(false);
        }
        if call.blocked_by_sibling
            || call.ended
            || call.call.name != tool_name
            || !semantically_equal_json(&call.call.arguments, arguments)
        {
            return self.fail_protocol();
        }
        self.active_validation_request = Some((Arc::from(call_id), Arc::from(tool_name)));
        Ok(true)
    }

    pub(crate) fn accept_result(
        &mut self,
        accepted: AcceptedPiJsonV1Result,
    ) -> Result<(), AgentFailureCause> {
        if let Some(failure) = &self.failure {
            return Err(failure.cause().clone());
        }
        if self.value_kind != AgentValueKind::Result || self.accepted_result.is_some() {
            return self.fail_protocol();
        }

        let validation_request_matches =
            self.active_validation_request
                .as_ref()
                .is_some_and(|(call_id, tool_name)| {
                    call_id == &accepted.call_id && tool_name == &accepted.tool_name
                });
        let call_matches = self
            .result_calls
            .get(accepted.call_id.as_ref())
            .is_some_and(|call| {
                call.started
                    && !call.ended
                    && !call.blocked_by_sibling
                    && call.call.name.as_str() == accepted.tool_name.as_ref()
                    && semantically_equal_json(&call.call.arguments, accepted.arguments.as_ref())
            });
        if !validation_request_matches || !call_matches {
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
            let cause = self.protocol_failure();
            self.record_rejection(
                PiJsonV1RejectionReason::PartialFrameAtEndOfStream,
                PiJsonV1ProtocolStage::EndOfStream,
            );
            self.failure = Some(self.agent_failure(cause));
        }

        if let Some(failure) = self.failure {
            return super::agent::AgentOutcome::Failed(failure);
        }
        if self.has_unfinished_result_call() {
            self.record_rejection(
                PiJsonV1RejectionReason::ResultCorrelationInvalid,
                PiJsonV1ProtocolStage::ResultCorrelation,
            );
            return super::agent::AgentOutcome::Failed(
                self.agent_failure(AgentFailureCause::HarnessProtocolFailed),
            );
        }
        if self.active_validation_request.is_some() {
            self.record_rejection(
                PiJsonV1RejectionReason::EndOfStreamInvariantInvalid,
                PiJsonV1ProtocolStage::EndOfStream,
            );
            return super::agent::AgentOutcome::Failed(
                self.agent_failure(AgentFailureCause::HarnessProtocolFailed),
            );
        }
        if let Err(cause) = self.protocol.validate_eof() {
            self.record_rejection(
                PiJsonV1RejectionReason::EndOfStreamInvariantInvalid,
                PiJsonV1ProtocolStage::EndOfStream,
            );
            return super::agent::AgentOutcome::Failed(self.agent_failure(cause));
        }
        if !completion.exit_success {
            return failed_agent_outcome(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            });
        }
        self.classify_terminal()
    }

    fn parse_frame(&mut self, frame: &[u8]) -> Result<(), AgentFailureCause> {
        self.rejection_context = PiJsonV1RejectionContext::default();
        self.rejection_state_snapshot = Some(self.rejection_state());
        let result = self.parse_frame_inner(frame);
        let parser_owned_rejection = result.as_ref().is_err_and(|cause| {
            matches!(
                cause,
                AgentFailureCause::HarnessStartFailed | AgentFailureCause::HarnessProtocolFailed
            )
        });
        if parser_owned_rejection && self.protocol_rejection.is_none() {
            let reason = match self.rejection_context.stage {
                PiJsonV1ProtocolStage::FrameRead => PiJsonV1RejectionReason::FrameTooLarge,
                PiJsonV1ProtocolStage::FrameDecode => PiJsonV1RejectionReason::FrameDecodeFailed,
                PiJsonV1ProtocolStage::SessionHeader => {
                    PiJsonV1RejectionReason::SessionHeaderInvalid
                }
                PiJsonV1ProtocolStage::EventEnvelope => {
                    PiJsonV1RejectionReason::EventTransitionInvalid
                }
                PiJsonV1ProtocolStage::ResultCorrelation => {
                    PiJsonV1RejectionReason::ResultCorrelationInvalid
                }
                PiJsonV1ProtocolStage::TerminalValidation => {
                    PiJsonV1RejectionReason::TerminalInvariantInvalid
                }
                PiJsonV1ProtocolStage::EndOfStream => {
                    PiJsonV1RejectionReason::EndOfStreamInvariantInvalid
                }
                PiJsonV1ProtocolStage::EventPayload => {
                    PiJsonV1RejectionReason::EventTransitionInvalid
                }
            };
            self.record_rejection(reason, self.rejection_context.stage);
        }
        self.rejection_state_snapshot = None;
        result
    }

    fn parse_frame_inner(&mut self, frame: &[u8]) -> Result<(), AgentFailureCause> {
        let frame_bytes = u64::try_from(frame.len()).unwrap_or(u64::MAX);
        if frame_bytes > self.limits.maximum_frame_bytes().get() {
            return self.reject(
                PiJsonV1RejectionReason::FrameTooLarge,
                PiJsonV1ProtocolStage::FrameRead,
            );
        }
        self.rejection_context.stage = PiJsonV1ProtocolStage::FrameDecode;
        let value = strict_json::from_slice(frame).map_err(|_| self.protocol_failure())?;
        let Some(object) = value.as_object() else {
            return self.reject(
                PiJsonV1RejectionReason::FrameNotObject,
                PiJsonV1ProtocolStage::FrameDecode,
            );
        };

        if !self.protocol.header_seen {
            self.rejection_context.stage = PiJsonV1ProtocolStage::SessionHeader;
            self.rejection_context.outer_event =
                required_string(object, "type").map(normalized_pi_event_type);
            self.parse_session_header(object)?;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::SessionEstablished));
            return Ok(());
        }

        self.rejection_context.stage = PiJsonV1ProtocolStage::EventEnvelope;
        let Some(event_type) = required_string(object, "type") else {
            self.observe_unrecognized(value);
            return Ok(());
        };
        self.rejection_context.outer_event = Some(normalized_pi_event_type(event_type));
        if event_type == "session" {
            return self.reject(
                PiJsonV1RejectionReason::SessionEventRepeated,
                PiJsonV1ProtocolStage::EventEnvelope,
            );
        }
        if self.protocol.settled && work_bearing_after_boundary(event_type, object, true) {
            return self.reject(
                PiJsonV1RejectionReason::EventAfterSettlement,
                PiJsonV1ProtocolStage::EventEnvelope,
            );
        }
        if self.accepted_result.is_some() && work_bearing_after_boundary(event_type, object, false)
        {
            return self.reject(
                PiJsonV1RejectionReason::EventAfterResultAcceptance,
                PiJsonV1ProtocolStage::EventEnvelope,
            );
        }

        self.rejection_context.stage = PiJsonV1ProtocolStage::EventPayload;
        if self.result_tool_event_is_authoritative(event_type, object) {
            return match event_type {
                "tool_execution_start" => self.tool_execution_start(object),
                "tool_execution_end" => self.tool_execution_end(object),
                _ => self.reject(
                    PiJsonV1RejectionReason::EventTransitionInvalid,
                    PiJsonV1ProtocolStage::EventPayload,
                ),
            };
        }
        if observation_event_has_unknown_fields(event_type, object) {
            self.observe_unrecognized(value);
            return Ok(());
        }
        macro_rules! observation {
            ($handler:expr) => {{
                let before = self.observations.len();
                let result = $handler;
                self.finish_observation_event(&value, before, result)
            }};
        }
        match event_type {
            "agent_start" => self.agent_start(object),
            "agent_end" => self.agent_end(object),
            "agent_settled" => self.agent_settled(object),
            "turn_start" => observation!(self.turn_start(object)),
            "turn_end" => observation!(self.turn_end(object)),
            "message_start" => observation!(self.message_start(object)),
            "message_update" => observation!(self.message_update(object)),
            "message_end" => observation!(self.message_end(object)),
            "tool_execution_start" => observation!(self.tool_execution_start(object)),
            "tool_execution_update" => observation!(self.tool_execution_update(object)),
            "tool_execution_end" => observation!(self.tool_execution_end(object)),
            "auto_retry_start" => observation!(self.auto_retry_start(object)),
            "auto_retry_end" => observation!(self.auto_retry_end(object)),
            "compaction_start" => observation!(self.compaction_start(object)),
            "compaction_end" => observation!(self.compaction_end(object)),
            "summarization_retry_scheduled" => {
                observation!(self.summarization_retry_scheduled(object))
            }
            "summarization_retry_attempt_start" => {
                observation!(self.summarization_retry_attempt_start(object))
            }
            "summarization_retry_finished" => {
                observation!(self.summarization_retry_finished(object))
            }
            "queue_update" => observation!(self.queue_update(object)),
            "entry_appended" => observation!(self.entry_appended(object)),
            "session_info_changed" => observation!(self.session_info_changed(object)),
            "thinking_level_changed" => observation!(self.thinking_level_changed(object)),
            "bash_execution_update" => observation!(self.bash_execution_update(object)),
            _ => {
                self.observe_unrecognized(value);
                Ok(())
            }
        }
    }

    fn finish_observation_event(
        &mut self,
        raw: &Value,
        observation_count: usize,
        result: Result<(), AgentFailureCause>,
    ) -> Result<(), AgentFailureCause> {
        match result {
            Ok(()) => Ok(()),
            Err(cause)
                if cause == AgentFailureCause::CapturedValueTooLarge
                    || self.protocol_rejection.is_some() =>
            {
                Err(cause)
            }
            Err(_) => {
                self.observations.truncate(observation_count);
                self.observe_unrecognized(raw.clone());
                Ok(())
            }
        }
    }

    fn observe_unrecognized(&mut self, event: Value) {
        self.observations
            .push(AgentObservation::UnrecognizedHarnessEvent {
                event: Arc::new(event),
            });
    }

    fn parse_session_header(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if !has_only_required_shape(object, &["type", "version", "id", "timestamp", "cwd"])
            || required_string(object, "type") != Some("session")
            || required_u64(object, "version") != Some(SESSION_VERSION)
            || required_string(object, "id").is_none()
            || required_string(object, "timestamp").is_none()
            || required_string(object, "cwd") != Some(self.expected_cwd.as_ref())
        {
            return Err(AgentFailureCause::HarnessStartFailed);
        }
        self.protocol.header_seen = true;
        Ok(())
    }

    fn require_authority_event_shape(
        &mut self,
        object: &Map<String, Value>,
        required: &[&str],
    ) -> Result<(), AgentFailureCause> {
        if has_only_required_shape(object, required) {
            Ok(())
        } else {
            self.reject(
                PiJsonV1RejectionReason::EventShapeInvalid,
                PiJsonV1ProtocolStage::EventPayload,
            )
        }
    }

    fn agent_start(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        self.require_authority_event_shape(object, &["type"])?;
        if !self.protocol.can_start_agent() {
            return self.reject(
                PiJsonV1RejectionReason::EventTransitionInvalid,
                PiJsonV1ProtocolStage::EventPayload,
            );
        }
        self.protocol.agent_active = true;
        self.protocol.ever_started = true;
        self.reconstruction = None;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::HarnessStarted));
        Ok(())
    }

    fn agent_end(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        self.require_authority_event_shape(object, &["type", "messages", "willRetry"])?;
        let (Some(messages), Some(will_retry)) = (
            required_array(object, "messages"),
            required_bool(object, "willRetry"),
        ) else {
            return self.reject(
                PiJsonV1RejectionReason::EventShapeInvalid,
                PiJsonV1ProtocolStage::EventPayload,
            );
        };
        if !self.protocol.agent_active {
            return self.reject(
                PiJsonV1RejectionReason::EventTransitionInvalid,
                PiJsonV1ProtocolStage::EventPayload,
            );
        }
        if self.has_unfinished_result_call() {
            return self.reject(
                PiJsonV1RejectionReason::ResultCorrelationInvalid,
                PiJsonV1ProtocolStage::ResultCorrelation,
            );
        }
        let Some(final_assistant) = messages
            .iter()
            .rev()
            .filter_map(|message| parse_message(message, true))
            .find_map(|message| match message {
                ParsedMessage::Assistant(assistant) => Some(assistant),
                ParsedMessage::ToolResult(_) | ParsedMessage::Other(_) => None,
            })
        else {
            return self.reject(
                PiJsonV1RejectionReason::TerminalInvariantInvalid,
                PiJsonV1ProtocolStage::TerminalValidation,
            );
        };
        self.check_response_bound(&final_assistant)?;
        if self.accepted_result.is_some() {
            if will_retry {
                return self.reject(
                    PiJsonV1RejectionReason::EventTransitionInvalid,
                    PiJsonV1ProtocolStage::EventPayload,
                );
            }
            self.validate_accepted_terminal(&final_assistant)?;
        }

        self.protocol.agent_active = false;
        self.protocol.last_agent_end = Some(AgentEndState {
            final_assistant,
            will_retry,
        });
        self.reconstruction = None;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::HarnessCompleted));
        Ok(())
    }

    fn agent_settled(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        self.require_authority_event_shape(object, &["type"])?;
        if !self.protocol.can_settle() {
            return self.reject(
                PiJsonV1RejectionReason::EventTransitionInvalid,
                PiJsonV1ProtocolStage::EventPayload,
            );
        }
        self.protocol.settled = true;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::HarnessQuiescent));
        Ok(())
    }

    fn turn_start(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if !has_only_required_shape(object, &["type"]) {
            return Err(self.protocol_failure());
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::TurnStarted));
        Ok(())
    }

    fn turn_end(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        required_value(object, "message")
            .and_then(|message| parse_message(message, true))
            .and_then(|message| message.assistant().cloned())
            .ok_or_else(|| self.protocol_failure())?;
        required_array(object, "toolResults")
            .and_then(parse_tool_results)
            .ok_or_else(|| self.protocol_failure())?;
        self.reconstruction = None;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::TurnCompleted));
        Ok(())
    }

    fn message_start(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let value = required_value(object, "message").ok_or_else(|| self.protocol_failure())?;
        let message = parse_message(value, false).ok_or_else(|| self.protocol_failure())?;
        self.reconstruction = match message {
            ParsedMessage::Assistant(assistant) => {
                self.check_response_bound(&assistant)?;
                Some(ActiveMessage::new(assistant))
            }
            ParsedMessage::ToolResult(_) | ParsedMessage::Other(_) => None,
        };
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageStarted));
        Ok(())
    }

    fn message_update(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if object.contains_key("message") {
            return Err(self.protocol_failure());
        }
        let usage = required_object(object, "usage")
            .and_then(parse_usage)
            .ok_or_else(|| self.protocol_failure())?;
        let event = required_object(object, "assistantMessageEvent")
            .ok_or_else(|| self.protocol_failure())
            .and_then(|event| parse_assistant_event(event).map_err(|_| self.protocol_failure()))?;
        let maximum_response_bytes = (self.value_kind == AgentValueKind::Response)
            .then_some(self.maximum_response_bytes.get());
        let apply_result = match self.reconstruction.as_mut() {
            Some(active) => event.apply(
                active,
                self.limits.maximum_frame_bytes().get(),
                maximum_response_bytes,
            ),
            None => return Err(self.protocol_failure()),
        };
        let mut observations = match apply_result {
            Ok(observations) => observations,
            Err(ApplyAssistantUpdateError::Transition) => {
                return Err(self.protocol_failure());
            }
            Err(ApplyAssistantUpdateError::CapturedValueTooLarge) => {
                return Err(AgentFailureCause::CapturedValueTooLarge);
            }
            Err(ApplyAssistantUpdateError::RetainedStateLimitExceeded) => {
                return self.reject(
                    PiJsonV1RejectionReason::RetainedStateLimitExceeded,
                    PiJsonV1ProtocolStage::EventPayload,
                );
            }
        };
        let input_tokens = usage.input;
        let output_tokens = usage.output;
        let Some(active) = self.reconstruction.as_mut() else {
            return Err(self.protocol_failure());
        };
        active.last.usage = usage;
        active.had_update = true;
        observations.push(AgentObservation::Usage {
            input_tokens,
            output_tokens,
        });
        self.observations.extend(observations);
        Ok(())
    }

    fn message_end(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let value = required_value(object, "message").ok_or_else(|| self.protocol_failure())?;
        let message = parse_message(value, true).ok_or_else(|| self.protocol_failure())?;
        if let ParsedMessage::Assistant(assistant) = &message {
            self.check_response_bound(assistant)?;
            self.retain_result_identity_context(assistant)?;
            let streamed = self.reconstruction.take();
            self.observe_finalized_content(assistant, streamed.as_ref());
            self.observe_assistant_completion(assistant);
        } else {
            self.reconstruction = None;
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageCompleted));
        Ok(())
    }

    fn observe_finalized_content(
        &mut self,
        assistant: &AssistantMessage,
        streamed: Option<&ActiveMessage>,
    ) {
        let had_update = streamed.is_some_and(|streamed| streamed.had_update);
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
        for call in assistant.tool_calls() {
            let already_observed = streamed.is_some_and(|streamed| {
                matches!(
                    streamed.open_block.as_ref(),
                    Some(OpenBlock::ToolCall { .. })
                ) || streamed.last.tool_calls().any(|observed| {
                    observed.id == call.id
                        && observed.name == call.name
                        && semantically_equal_json(&observed.arguments, &call.arguments)
                })
            });
            if !already_observed {
                self.observations.push(tool_call_observation(
                    &call.id,
                    &call.name,
                    AgentToolCallPhase::Started,
                ));
                self.observations.push(tool_call_observation(
                    &call.id,
                    &call.name,
                    AgentToolCallPhase::Completed,
                ));
            }
        }
    }

    fn retain_result_identity_context(
        &mut self,
        assistant: &AssistantMessage,
    ) -> Result<(), AgentFailureCause> {
        let Some(expected_result_tool_name) = self.expected_result_tool_name.clone() else {
            return Ok(());
        };
        let tool_call_count = assistant.tool_calls().count();
        for call in assistant
            .tool_calls()
            .filter(|call| call.name == expected_result_tool_name.as_ref())
        {
            if self.accepted_result.is_some() || self.result_calls.contains_key(&call.id) {
                return self.reject(
                    PiJsonV1RejectionReason::ResultCorrelationInvalid,
                    PiJsonV1ProtocolStage::ResultCorrelation,
                );
            }
            let retained_bytes = u64::try_from(call.id.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(call.name.len()).unwrap_or(u64::MAX))
                .saturating_add(json_bytes(&call.arguments).unwrap_or(u64::MAX));
            let Some(total_retained_bytes) = self
                .retained_result_state_bytes
                .checked_add(retained_bytes)
                .filter(|bytes| *bytes <= self.limits.maximum_frame_bytes().get())
            else {
                return self.reject(
                    PiJsonV1RejectionReason::RetainedStateLimitExceeded,
                    PiJsonV1ProtocolStage::ResultCorrelation,
                );
            };
            self.result_calls.insert(
                call.id.clone(),
                ResultCallState {
                    call: call.clone(),
                    blocked_by_sibling: tool_call_count != 1,
                    started: false,
                    ended: false,
                },
            );
            self.retained_result_state_bytes = total_retained_bytes;
        }
        Ok(())
    }

    fn tool_execution_start(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let (authoritative, call_id, name) =
            self.result_tool_event_identity("tool_execution_start", object)?;
        let Some(arguments) = required_value(object, "args") else {
            return self.reject_result_tool_shape(authoritative);
        };
        if authoritative {
            let matches = self.result_calls.get_mut(call_id).is_some_and(|call| {
                let matches = !call.started
                    && !call.ended
                    && call.call.name == name
                    && semantically_equal_json(&call.call.arguments, arguments);
                if matches {
                    call.started = true;
                }
                matches
            });
            if !matches {
                return self.reject(
                    PiJsonV1RejectionReason::ResultCorrelationInvalid,
                    PiJsonV1ProtocolStage::ResultCorrelation,
                );
            }
        }
        self.observations.push(tool_call_observation(
            call_id,
            name,
            AgentToolCallPhase::Started,
        ));
        Ok(())
    }

    fn tool_execution_update(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        // Pi and Claude progress envelopes are independent native contracts; sharing their
        // similarly shaped field extraction would couple profiles that evolve separately.
        // jscpd:ignore-start
        let call_id = required_nonempty_string(object, "toolCallId")
            .ok_or_else(|| self.protocol_failure())?;
        let name =
            required_nonempty_string(object, "toolName").ok_or_else(|| self.protocol_failure())?;
        required_value(object, "args").ok_or_else(|| self.protocol_failure())?;
        required_value(object, "partialResult")
            .and_then(parse_tool_execution_result)
            .ok_or_else(|| self.protocol_failure())?;
        self.observations.push(tool_call_observation(
            call_id,
            name,
            AgentToolCallPhase::Updated,
        ));
        // jscpd:ignore-end
        Ok(())
    }

    fn tool_execution_end(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        let (authoritative, call_id, name) =
            self.result_tool_event_identity("tool_execution_end", object)?;
        let result = required_value(object, "result").and_then(parse_tool_execution_result);
        let is_error = required_bool(object, "isError");
        let (Some(result), Some(is_error)) = (result, is_error) else {
            return self.reject_result_tool_shape(authoritative);
        };

        if authoritative {
            let Some(call) = self.result_calls.get(call_id) else {
                return self.reject(
                    PiJsonV1RejectionReason::ResultCorrelationInvalid,
                    PiJsonV1ProtocolStage::ResultCorrelation,
                );
            };
            if !call.started || call.ended || call.call.name != name {
                return self.reject(
                    PiJsonV1RejectionReason::ResultCorrelationInvalid,
                    PiJsonV1ProtocolStage::ResultCorrelation,
                );
            }
            let blocked_by_sibling = call.blocked_by_sibling;
            let active_validation_matches =
                self.active_validation_request
                    .as_ref()
                    .is_some_and(|(active_id, active_name)| {
                        active_id.as_ref() == call_id && active_name.as_ref() == name
                    });
            let accepted_validation_matches =
                self.accepted_result.as_ref().is_some_and(|accepted| {
                    accepted.accepted.call_id.as_ref() == call_id
                        && accepted.accepted.tool_name.as_ref() == name
                });
            let successful_termination = !is_error && result.terminate == Some(true);
            let recoverable_rejection = is_error && result.terminate != Some(true);
            if blocked_by_sibling && !recoverable_rejection
                || active_validation_matches
                    && !(accepted_validation_matches && successful_termination
                        || !accepted_validation_matches && recoverable_rejection)
                || !active_validation_matches && !recoverable_rejection
            {
                return self.reject(
                    PiJsonV1RejectionReason::ResultCorrelationInvalid,
                    PiJsonV1ProtocolStage::ResultCorrelation,
                );
            }
            if active_validation_matches {
                self.active_validation_request = None;
            }
            let Some(call) = self.result_calls.get_mut(call_id) else {
                return self.reject(
                    PiJsonV1RejectionReason::ResultCorrelationInvalid,
                    PiJsonV1ProtocolStage::ResultCorrelation,
                );
            };
            call.ended = true;
            if accepted_validation_matches {
                let Some(accepted) = self.accepted_result.as_mut() else {
                    return self.reject(
                        PiJsonV1RejectionReason::ResultCorrelationInvalid,
                        PiJsonV1ProtocolStage::ResultCorrelation,
                    );
                };
                accepted.native_execution_completed = true;
            }
        }

        self.observations.push(tool_call_observation(
            call_id,
            name,
            AgentToolCallPhase::Completed,
        ));
        self.observations.push(AgentObservation::ToolResult {
            call_id: Arc::from(call_id),
            is_error,
            content: Arc::from(result.text()),
        });
        Ok(())
    }

    fn reject_result_tool_shape<T>(&mut self, authoritative: bool) -> Result<T, AgentFailureCause> {
        if authoritative {
            self.reject(
                PiJsonV1RejectionReason::ResultCorrelationInvalid,
                PiJsonV1ProtocolStage::ResultCorrelation,
            )
        } else {
            Err(self.protocol_failure())
        }
    }

    fn result_tool_event_identity<'a>(
        &mut self,
        event_type: &str,
        object: &'a Map<String, Value>,
    ) -> Result<(bool, &'a str, &'a str), AgentFailureCause> {
        let authoritative = self.result_tool_event_is_authoritative(event_type, object);
        let required = match event_type {
            "tool_execution_start" => &["type", "toolCallId", "toolName", "args"][..],
            "tool_execution_end" => &["type", "toolCallId", "toolName", "result", "isError"][..],
            _ => return self.reject_result_tool_shape(authoritative),
        };
        if authoritative && !has_only_required_shape(object, required) {
            return self.reject_result_tool_shape(true);
        }
        let Some(call_id) = required_nonempty_string(object, "toolCallId") else {
            return self.reject_result_tool_shape(authoritative);
        };
        let Some(name) = required_nonempty_string(object, "toolName") else {
            return self.reject_result_tool_shape(authoritative);
        };
        Ok((authoritative, call_id, name))
    }

    fn result_tool_event_is_authoritative(
        &self,
        event_type: &str,
        object: &Map<String, Value>,
    ) -> bool {
        if !matches!(event_type, "tool_execution_start" | "tool_execution_end") {
            return false;
        }
        required_nonempty_string(object, "toolCallId")
            .is_some_and(|call_id| self.result_calls.contains_key(call_id))
            || required_nonempty_string(object, "toolName").is_some_and(|name| {
                self.expected_result_tool_name
                    .as_ref()
                    .is_some_and(|expected| expected.as_ref() == name)
            })
    }

    fn has_unfinished_result_call(&self) -> bool {
        self.result_calls
            .values()
            .any(|call| call.started && !call.ended)
    }

    fn observe_retry_schedule(
        &mut self,
        object: &Map<String, Value>,
        label: &str,
    ) -> Result<(), AgentFailureCause> {
        let (attempt, max_attempts, delay, error) =
            retry_schedule(object).ok_or_else(|| self.protocol_failure())?;
        if attempt > max_attempts {
            return Err(self.protocol_failure());
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::RetryStarted));
        self.observations.push(AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Warning,
            message: Arc::from(format!(
                "{label}retry {attempt}/{max_attempts} after {delay} ms: {error}"
            )),
        });
        Ok(())
    }

    fn auto_retry_start(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        self.observe_retry_schedule(object, "")
    }

    fn auto_retry_end(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        required_bool(object, "success").ok_or_else(|| self.protocol_failure())?;
        required_positive_u64(object, "attempt").ok_or_else(|| self.protocol_failure())?;
        let final_error =
            optional_string(object, "finalError").ok_or_else(|| self.protocol_failure())?;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::RetryCompleted));
        self.observe_optional_error(final_error);
        Ok(())
    }

    fn compaction_start(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        required_compaction_reason(object).ok_or_else(|| self.protocol_failure())?;
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
        let _ = (reason, aborted, will_retry);
        if object
            .get("result")
            .is_some_and(|result| !result.is_null() && !result.is_object())
        {
            return Err(self.protocol_failure());
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::CompactionCompleted));
        self.observe_optional_error(error);
        Ok(())
    }

    fn summarization_retry_scheduled(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.observe_retry_schedule(object, "summarization ")
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
        if !valid_source {
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
        if !has_only_required_shape(object, &["type"]) {
            return Err(self.protocol_failure());
        }
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

    fn validate_accepted_terminal(
        &mut self,
        assistant: &AssistantMessage,
    ) -> Result<(), AgentFailureCause> {
        let matches = self.accepted_result.as_ref().is_some_and(|accepted| {
            assistant.stop_reason == StopReason::ToolUse
                && accepted.native_execution_completed
                && assistant.only_tool_call().is_some_and(|call| {
                    call.id.as_str() == accepted.accepted.call_id.as_ref()
                        && call.name.as_str() == accepted.accepted.tool_name.as_ref()
                        && semantically_equal_json(
                            &call.arguments,
                            accepted.accepted.arguments.as_ref(),
                        )
                })
        });
        if matches {
            Ok(())
        } else {
            self.reject(
                PiJsonV1RejectionReason::ResultCorrelationInvalid,
                PiJsonV1ProtocolStage::TerminalValidation,
            )
        }
    }

    fn classify_terminal(&self) -> super::agent::AgentOutcome {
        let Some(end) = self.protocol.last_agent_end.as_ref() else {
            return self.protocol_failure_outcome(
                PiJsonV1RejectionReason::TerminalInvariantInvalid,
                PiJsonV1ProtocolStage::TerminalValidation,
            );
        };
        let assistant = &end.final_assistant;

        if self.accepted_result.is_some() && assistant.stop_reason != StopReason::ToolUse {
            return self.protocol_failure_outcome(
                PiJsonV1RejectionReason::TerminalInvariantInvalid,
                PiJsonV1ProtocolStage::TerminalValidation,
            );
        }

        match assistant.stop_reason {
            StopReason::Stop => match self.value_kind {
                AgentValueKind::None => {
                    super::agent::AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
                }
                AgentValueKind::Response => {
                    let text_blocks = assistant.text_blocks().collect::<Vec<_>>();
                    if text_blocks.is_empty() {
                        failed_agent_outcome(AgentFailureCause::MissingResponse)
                    } else {
                        let response = text_blocks.concat();
                        super::agent::AgentOutcome::Completed(CompletedAgentInvocation::Response(
                            BoundedAgentResponse::from_bounded(Arc::from(response)),
                        ))
                    }
                }
                AgentValueKind::Result => failed_agent_outcome(AgentFailureCause::MissingResult),
            },
            StopReason::Length => harness_failure(AgentHarnessFailureDetail::ModelOutputTruncated),
            StopReason::ToolUse => match self.value_kind {
                AgentValueKind::None | AgentValueKind::Response => {
                    harness_failure(AgentHarnessFailureDetail::UnexpectedTerminalToolUse)
                }
                AgentValueKind::Result => {
                    let Some(accepted) = self.accepted_result.as_ref() else {
                        return failed_agent_outcome(AgentFailureCause::MissingResult);
                    };
                    let Some(call) = assistant.only_tool_call() else {
                        return self.protocol_failure_outcome(
                            PiJsonV1RejectionReason::TerminalInvariantInvalid,
                            PiJsonV1ProtocolStage::TerminalValidation,
                        );
                    };
                    if !accepted.native_execution_completed
                        || call.id.as_str() != accepted.accepted.call_id.as_ref()
                        || call.name.as_str() != accepted.accepted.tool_name.as_ref()
                        || !semantically_equal_json(
                            &call.arguments,
                            accepted.accepted.arguments.as_ref(),
                        )
                    {
                        return self.protocol_failure_outcome(
                            PiJsonV1RejectionReason::ResultCorrelationInvalid,
                            PiJsonV1ProtocolStage::TerminalValidation,
                        );
                    }
                    super::agent::AgentOutcome::Completed(CompletedAgentInvocation::Result(
                        accepted.accepted.result.clone(),
                    ))
                }
            },
            StopReason::Error => harness_failure(AgentHarnessFailureDetail::ModelError),
            StopReason::Aborted => harness_failure(AgentHarnessFailureDetail::ModelAborted),
            StopReason::Pending => self.protocol_failure_outcome(
                PiJsonV1RejectionReason::TerminalInvariantInvalid,
                PiJsonV1ProtocolStage::TerminalValidation,
            ),
        }
    }

    fn protocol_failure(&self) -> AgentFailureCause {
        if self.protocol.ever_started {
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

    fn protocol_failure_outcome(
        &self,
        reason: PiJsonV1RejectionReason,
        stage: PiJsonV1ProtocolStage,
    ) -> super::agent::AgentOutcome {
        super::agent::AgentOutcome::Failed(AgentFailure::with_protocol_rejection(
            self.protocol_failure(),
            self.make_protocol_rejection(reason, stage),
        ))
    }

    fn reject<T>(
        &mut self,
        reason: PiJsonV1RejectionReason,
        stage: PiJsonV1ProtocolStage,
    ) -> Result<T, AgentFailureCause> {
        let cause = self.protocol_failure();
        self.record_rejection(reason, stage);
        Err(cause)
    }

    fn record_rejection(&mut self, reason: PiJsonV1RejectionReason, stage: PiJsonV1ProtocolStage) {
        self.rejection_context.stage = stage;
        if self.protocol_rejection.is_none() {
            self.protocol_rejection = Some(self.make_protocol_rejection(reason, stage));
        }
    }

    fn make_protocol_rejection(
        &self,
        reason: PiJsonV1RejectionReason,
        stage: PiJsonV1ProtocolStage,
    ) -> AgentProtocolRejectionDiagnostic {
        AgentProtocolRejectionDiagnostic::pi_json_v1(PiJsonV1ProtocolRejection {
            reason,
            stage,
            outer_event: self.rejection_context.outer_event,
            state: self
                .rejection_state_snapshot
                .clone()
                .unwrap_or_else(|| self.rejection_state()),
        })
    }

    fn rejection_state(&self) -> PiJsonV1RejectionState {
        PiJsonV1RejectionState {
            session_header_seen: self.protocol.header_seen,
            agent_started: self.protocol.ever_started,
            terminal_candidate_retained: self.protocol.last_agent_end.is_some(),
            result_accepted: self.accepted_result.is_some(),
            settled: self.protocol.settled,
        }
    }

    fn fail_protocol<T>(&mut self) -> Result<T, AgentFailureCause> {
        let cause = self.protocol_failure();
        self.record_rejection(
            PiJsonV1RejectionReason::ResultCorrelationInvalid,
            PiJsonV1ProtocolStage::ResultCorrelation,
        );
        self.failure = Some(self.agent_failure(cause.clone()));
        Err(cause)
    }
}

#[derive(Default)]
struct ProtocolState {
    header_seen: bool,
    ever_started: bool,
    agent_active: bool,
    last_agent_end: Option<AgentEndState>,
    settled: bool,
}

impl ProtocolState {
    fn can_start_agent(&self) -> bool {
        !self.settled
            && !self.agent_active
            && self
                .last_agent_end
                .as_ref()
                .is_none_or(|end| end.will_retry)
    }

    fn can_settle(&self) -> bool {
        !self.settled
            && !self.agent_active
            && self
                .last_agent_end
                .as_ref()
                .is_some_and(|end| !end.will_retry)
    }

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
            || self.agent_active
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

struct ActiveMessage {
    last: AssistantMessage,
    open_block: Option<OpenBlock>,
    had_update: bool,
    retained_bytes: u64,
    text_bytes: u64,
}

impl ActiveMessage {
    fn new(last: AssistantMessage) -> Self {
        let retained_bytes = last.retained_bytes();
        let text_bytes = last.text_bytes();
        Self {
            last,
            open_block: None,
            had_update: false,
            retained_bytes,
            text_bytes,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum OpenBlock {
    Text(usize),
    Thinking(usize),
    ToolCall {
        index: usize,
        arguments: String,
        had_delta: bool,
    },
}

impl OpenBlock {
    fn index(&self) -> usize {
        match self {
            Self::Text(index) | Self::Thinking(index) => *index,
            Self::ToolCall { index, .. } => *index,
        }
    }

    fn block_kind(&self) -> BlockKind {
        match self {
            Self::Text(_) => BlockKind::Text,
            Self::Thinking(_) => BlockKind::Thinking,
            Self::ToolCall { .. } => BlockKind::ToolCall,
        }
    }
}

#[derive(Clone)]
struct AgentEndState {
    final_assistant: AssistantMessage,
    will_retry: bool,
}

struct ResultCallState {
    call: ToolCall,
    blocked_by_sibling: bool,
    started: bool,
    ended: bool,
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

    fn retained_bytes(&self) -> u64 {
        let mut bytes = [
            self.api.len(),
            self.provider.len(),
            self.model.len(),
            self.response_model.as_deref().map_or(0, str::len),
            self.response_id.as_deref().map_or(0, str::len),
            self.error_message.as_deref().map_or(0, str::len),
        ]
        .into_iter()
        .fold(0_u64, |total, bytes| {
            total.saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX))
        });
        for block in &self.content {
            bytes = bytes.saturating_add(block.retained_bytes());
        }
        for diagnostic in &self.diagnostics {
            bytes = bytes
                .saturating_add(u64::try_from(diagnostic.kind.len()).unwrap_or(u64::MAX))
                .saturating_add(diagnostic.error_message.as_deref().map_or(0, |message| {
                    u64::try_from(message.len()).unwrap_or(u64::MAX)
                }))
                .saturating_add(
                    diagnostic
                        .error
                        .as_ref()
                        .map_or(0, |value| json_bytes(value).unwrap_or(u64::MAX)),
                )
                .saturating_add(
                    diagnostic
                        .details
                        .as_ref()
                        .map_or(0, |value| json_bytes(value).unwrap_or(u64::MAX)),
                );
        }
        bytes
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

impl ContentBlock {
    fn retained_bytes(&self) -> u64 {
        content_block_structure_bytes().saturating_add(match self {
            Self::Text(text) | Self::Thinking(text) => {
                u64::try_from(text.len()).unwrap_or(u64::MAX)
            }
            Self::ToolCall(call) => call.retained_bytes(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ToolCall {
    id: String,
    name: String,
    arguments: Value,
}

impl ToolCall {
    fn retained_bytes(&self) -> u64 {
        u64::try_from(self.id.len())
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(self.name.len()).unwrap_or(u64::MAX))
            .saturating_add(json_bytes(&self.arguments).unwrap_or(u64::MAX))
    }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompactionReason {
    Manual,
    Threshold,
    Overflow,
}

struct AssistantUpdateEvent {
    kind: AssistantUpdateKind,
    index: usize,
}

enum AssistantUpdateKind {
    TextStart,
    TextDelta(String),
    TextEnd(String),
    ThinkingStart,
    ThinkingDelta(String),
    ThinkingEnd(String),
    ToolCallStart,
    ToolCallDelta(String),
    ToolCallEnd(ToolCall),
}

enum ApplyAssistantUpdateError {
    Transition,
    CapturedValueTooLarge,
    RetainedStateLimitExceeded,
}

impl AssistantUpdateEvent {
    fn apply(
        self,
        active: &mut ActiveMessage,
        maximum_retained_bytes: u64,
        maximum_response_bytes: Option<u64>,
    ) -> Result<Vec<AgentObservation>, ApplyAssistantUpdateError> {
        let Self { kind, index } = self;
        match kind {
            AssistantUpdateKind::TextStart => {
                Self::require_start(index, &active.last, &active.open_block)?;
                let retained_bytes = bounded_retained_addition(
                    active.retained_bytes,
                    content_block_structure_bytes(),
                    maximum_retained_bytes,
                )?;
                active.last.content.push(ContentBlock::Text(String::new()));
                active.open_block = Some(OpenBlock::Text(index));
                active.retained_bytes = retained_bytes;
                Ok(vec![lifecycle(AgentLifecycleMilestone::MessageUpdated)])
            }
            AssistantUpdateKind::TextDelta(delta) => {
                Self::require_open(index, &active.open_block, BlockKind::Text)?;
                let delta_bytes = u64::try_from(delta.len()).unwrap_or(u64::MAX);
                let retained_bytes = bounded_retained_addition(
                    active.retained_bytes,
                    delta_bytes,
                    maximum_retained_bytes,
                )?;
                let Some(ContentBlock::Text(text)) = active.last.content.get_mut(index) else {
                    return Err(ApplyAssistantUpdateError::Transition);
                };
                let text_bytes = active
                    .text_bytes
                    .checked_add(delta_bytes)
                    .ok_or(ApplyAssistantUpdateError::CapturedValueTooLarge)?;
                if maximum_response_bytes.is_some_and(|maximum| text_bytes > maximum) {
                    return Err(ApplyAssistantUpdateError::CapturedValueTooLarge);
                }
                let observed = Arc::from(delta.as_str());
                text.push_str(&delta);
                active.retained_bytes = retained_bytes;
                active.text_bytes = text_bytes;
                Ok(vec![AgentObservation::AssistantText { text: observed }])
            }
            AssistantUpdateKind::TextEnd(content) => {
                Self::require_open(index, &active.open_block, BlockKind::Text)?;
                if !matches!(active.last.content.get(index), Some(ContentBlock::Text(text)) if text == &content)
                {
                    return Err(ApplyAssistantUpdateError::Transition);
                }
                require_retained_bound(active.retained_bytes, maximum_retained_bytes)?;
                active.open_block = None;
                Ok(vec![lifecycle(AgentLifecycleMilestone::MessageUpdated)])
            }
            AssistantUpdateKind::ThinkingStart => {
                Self::require_start(index, &active.last, &active.open_block)?;
                let retained_bytes = bounded_retained_addition(
                    active.retained_bytes,
                    content_block_structure_bytes(),
                    maximum_retained_bytes,
                )?;
                active
                    .last
                    .content
                    .push(ContentBlock::Thinking(String::new()));
                active.open_block = Some(OpenBlock::Thinking(index));
                active.retained_bytes = retained_bytes;
                Ok(vec![lifecycle(AgentLifecycleMilestone::MessageUpdated)])
            }
            AssistantUpdateKind::ThinkingDelta(delta) => {
                Self::require_open(index, &active.open_block, BlockKind::Thinking)?;
                let delta_bytes = u64::try_from(delta.len()).unwrap_or(u64::MAX);
                let retained_bytes = bounded_retained_addition(
                    active.retained_bytes,
                    delta_bytes,
                    maximum_retained_bytes,
                )?;
                let Some(ContentBlock::Thinking(thinking)) = active.last.content.get_mut(index)
                else {
                    return Err(ApplyAssistantUpdateError::Transition);
                };
                let observed = Arc::from(delta.as_str());
                thinking.push_str(&delta);
                active.retained_bytes = retained_bytes;
                Ok(vec![AgentObservation::Reasoning { text: observed }])
            }
            AssistantUpdateKind::ThinkingEnd(content) => {
                Self::require_open(index, &active.open_block, BlockKind::Thinking)?;
                let Some(ContentBlock::Thinking(thinking)) = active.last.content.get(index) else {
                    return Err(ApplyAssistantUpdateError::Transition);
                };
                let retained_bytes = bounded_retained_replacement(
                    active.retained_bytes,
                    u64::try_from(thinking.len()).unwrap_or(u64::MAX),
                    u64::try_from(content.len()).unwrap_or(u64::MAX),
                    maximum_retained_bytes,
                )?;
                let Some(ContentBlock::Thinking(thinking)) = active.last.content.get_mut(index)
                else {
                    return Err(ApplyAssistantUpdateError::Transition);
                };
                *thinking = content;
                active.open_block = None;
                active.retained_bytes = retained_bytes;
                Ok(vec![lifecycle(AgentLifecycleMilestone::MessageUpdated)])
            }
            AssistantUpdateKind::ToolCallStart => {
                Self::require_start(index, &active.last, &active.open_block)?;
                require_retained_bound(active.retained_bytes, maximum_retained_bytes)?;
                active.open_block = Some(OpenBlock::ToolCall {
                    index,
                    arguments: String::new(),
                    had_delta: false,
                });
                Ok(Vec::new())
            }
            AssistantUpdateKind::ToolCallDelta(delta) => {
                Self::require_open(index, &active.open_block, BlockKind::ToolCall)?;
                let retained_bytes = bounded_retained_addition(
                    active.retained_bytes,
                    u64::try_from(delta.len()).unwrap_or(u64::MAX),
                    maximum_retained_bytes,
                )?;
                let Some(OpenBlock::ToolCall {
                    arguments,
                    had_delta,
                    ..
                }) = active.open_block.as_mut()
                else {
                    return Err(ApplyAssistantUpdateError::Transition);
                };
                arguments.push_str(&delta);
                *had_delta = true;
                active.retained_bytes = retained_bytes;
                Ok(Vec::new())
            }
            AssistantUpdateKind::ToolCallEnd(call) => {
                Self::require_open(index, &active.open_block, BlockKind::ToolCall)?;
                let Some(OpenBlock::ToolCall {
                    arguments,
                    had_delta,
                    ..
                }) = active.open_block.as_ref()
                else {
                    return Err(ApplyAssistantUpdateError::Transition);
                };
                if index != active.last.content.len() {
                    return Err(ApplyAssistantUpdateError::Transition);
                }
                let retained_bytes = bounded_retained_replacement(
                    active.retained_bytes,
                    u64::try_from(arguments.len()).unwrap_or(u64::MAX),
                    content_block_structure_bytes().saturating_add(call.retained_bytes()),
                    maximum_retained_bytes,
                )?;
                let had_delta = *had_delta;
                let mut observations = vec![AgentObservation::ToolCall {
                    call_id: Arc::from(call.id.as_str()),
                    name: Arc::from(call.name.as_str()),
                    phase: AgentToolCallPhase::Started,
                }];
                if had_delta {
                    observations.push(AgentObservation::ToolCall {
                        call_id: Arc::from(call.id.as_str()),
                        name: Arc::from(call.name.as_str()),
                        phase: AgentToolCallPhase::Updated,
                    });
                }
                observations.push(AgentObservation::ToolCall {
                    call_id: Arc::from(call.id.as_str()),
                    name: Arc::from(call.name.as_str()),
                    phase: AgentToolCallPhase::Completed,
                });
                active.last.content.push(ContentBlock::ToolCall(call));
                active.open_block = None;
                active.retained_bytes = retained_bytes;
                Ok(observations)
            }
        }
    }

    fn require_start(
        index: usize,
        message: &AssistantMessage,
        open: &Option<OpenBlock>,
    ) -> Result<(), ApplyAssistantUpdateError> {
        if open.is_some() || index != message.content.len() {
            return Err(ApplyAssistantUpdateError::Transition);
        }
        Ok(())
    }

    fn require_open(
        index: usize,
        open: &Option<OpenBlock>,
        expected: BlockKind,
    ) -> Result<(), ApplyAssistantUpdateError> {
        let Some(actual) = open else {
            return Err(ApplyAssistantUpdateError::Transition);
        };
        if actual.block_kind() != expected || actual.index() != index {
            return Err(ApplyAssistantUpdateError::Transition);
        }
        Ok(())
    }
}

fn require_retained_bound(
    retained_bytes: u64,
    maximum_retained_bytes: u64,
) -> Result<(), ApplyAssistantUpdateError> {
    if retained_bytes > maximum_retained_bytes {
        Err(ApplyAssistantUpdateError::RetainedStateLimitExceeded)
    } else {
        Ok(())
    }
}

fn bounded_retained_addition(
    retained_bytes: u64,
    added_bytes: u64,
    maximum_retained_bytes: u64,
) -> Result<u64, ApplyAssistantUpdateError> {
    retained_bytes
        .checked_add(added_bytes)
        .filter(|total| *total <= maximum_retained_bytes)
        .ok_or(ApplyAssistantUpdateError::RetainedStateLimitExceeded)
}

fn bounded_retained_replacement(
    retained_bytes: u64,
    removed_bytes: u64,
    added_bytes: u64,
    maximum_retained_bytes: u64,
) -> Result<u64, ApplyAssistantUpdateError> {
    let Some(retained_bytes) = retained_bytes.checked_sub(removed_bytes) else {
        return Err(ApplyAssistantUpdateError::Transition);
    };
    bounded_retained_addition(retained_bytes, added_bytes, maximum_retained_bytes)
}

fn content_block_structure_bytes() -> u64 {
    u64::try_from(std::mem::size_of::<ContentBlock>()).unwrap_or(u64::MAX)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum BlockKind {
    Text,
    Thinking,
    ToolCall,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParseAssistantEventError {
    Shape,
    Subtype,
    ContentIndex,
}

fn parse_assistant_event(
    object: &Map<String, Value>,
) -> Result<AssistantUpdateEvent, ParseAssistantEventError> {
    if object.contains_key("partial")
        || object.contains_key("message")
        || object.contains_key("scherzoCompact")
    {
        return Err(ParseAssistantEventError::Shape);
    }
    let index = required_u64(object, "contentIndex")
        .and_then(|index| usize::try_from(index).ok())
        .ok_or(ParseAssistantEventError::ContentIndex)?;
    let event_type = required_string(object, "type").ok_or(ParseAssistantEventError::Subtype)?;
    let kind = match event_type {
        "text_start" => AssistantUpdateKind::TextStart,
        "text_delta" => AssistantUpdateKind::TextDelta(
            required_string(object, "delta")
                .ok_or(ParseAssistantEventError::Shape)?
                .to_owned(),
        ),
        "text_end" => AssistantUpdateKind::TextEnd(
            required_string(object, "content")
                .ok_or(ParseAssistantEventError::Shape)?
                .to_owned(),
        ),
        "thinking_start" => AssistantUpdateKind::ThinkingStart,
        "thinking_delta" => AssistantUpdateKind::ThinkingDelta(
            required_string(object, "delta")
                .ok_or(ParseAssistantEventError::Shape)?
                .to_owned(),
        ),
        "thinking_end" => AssistantUpdateKind::ThinkingEnd(
            required_string(object, "content")
                .ok_or(ParseAssistantEventError::Shape)?
                .to_owned(),
        ),
        "toolcall_start" => AssistantUpdateKind::ToolCallStart,
        "toolcall_delta" => AssistantUpdateKind::ToolCallDelta(
            required_string(object, "delta")
                .ok_or(ParseAssistantEventError::Shape)?
                .to_owned(),
        ),
        "toolcall_end" => AssistantUpdateKind::ToolCallEnd(
            required_object(object, "toolCall")
                .and_then(|call| parse_tool_call(call, true))
                .ok_or(ParseAssistantEventError::Shape)?,
        ),
        _ => return Err(ParseAssistantEventError::Subtype),
    };
    Ok(AssistantUpdateEvent { kind, index })
}

fn lifecycle(milestone: AgentLifecycleMilestone) -> AgentObservation {
    AgentObservation::Lifecycle { milestone }
}

fn harness_failure(detail: AgentHarnessFailureDetail) -> super::agent::AgentOutcome {
    failed_agent_outcome(AgentFailureCause::HarnessFailed { detail })
}

fn json_bytes(value: &Value) -> Option<u64> {
    serde_json::to_vec(value)
        .ok()
        .and_then(|bytes| u64::try_from(bytes.len()).ok())
}

fn semantically_equal_json(left: &Value, right: &Value) -> bool {
    super::condition::json_semantically_equal(left, right)
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

fn normalized_pi_event_type(event_type: &str) -> PiJsonV1EventType {
    match event_type {
        "session" => PiJsonV1EventType::Session,
        "agent_start" => PiJsonV1EventType::AgentStart,
        "agent_end" => PiJsonV1EventType::AgentEnd,
        "agent_settled" => PiJsonV1EventType::AgentSettled,
        "turn_start" => PiJsonV1EventType::TurnStart,
        "turn_end" => PiJsonV1EventType::TurnEnd,
        "message_start" => PiJsonV1EventType::MessageStart,
        "message_update" => PiJsonV1EventType::MessageUpdate,
        "message_end" => PiJsonV1EventType::MessageEnd,
        "tool_execution_start" => PiJsonV1EventType::ToolExecutionStart,
        "tool_execution_update" => PiJsonV1EventType::ToolExecutionUpdate,
        "tool_execution_end" => PiJsonV1EventType::ToolExecutionEnd,
        "auto_retry_start" => PiJsonV1EventType::AutoRetryStart,
        "auto_retry_end" => PiJsonV1EventType::AutoRetryEnd,
        "compaction_start" => PiJsonV1EventType::CompactionStart,
        "compaction_end" => PiJsonV1EventType::CompactionEnd,
        "summarization_retry_scheduled" => PiJsonV1EventType::SummarizationRetryScheduled,
        "summarization_retry_attempt_start" => PiJsonV1EventType::SummarizationRetryAttemptStart,
        "summarization_retry_finished" => PiJsonV1EventType::SummarizationRetryFinished,
        "queue_update" => PiJsonV1EventType::QueueUpdate,
        "entry_appended" => PiJsonV1EventType::EntryAppended,
        "session_info_changed" => PiJsonV1EventType::SessionInfoChanged,
        "thinking_level_changed" => PiJsonV1EventType::ThinkingLevelChanged,
        "bash_execution_update" => PiJsonV1EventType::BashExecutionUpdate,
        _ => PiJsonV1EventType::Unrecognized,
    }
}

fn work_bearing_after_boundary(
    event_type: &str,
    object: &Map<String, Value>,
    include_agent_end: bool,
) -> bool {
    matches!(
        event_type,
        "agent_start" | "turn_start" | "tool_execution_start"
    ) || include_agent_end && event_type == "agent_end"
        || event_type == "message_start"
            && required_object(object, "message")
                .and_then(|message| required_string(message, "role"))
                == Some("assistant")
}

fn observation_event_has_unknown_fields(event_type: &str, object: &Map<String, Value>) -> bool {
    let allowed: &[&str] = match event_type {
        "turn_start" => &["type"],
        "turn_end" => &["type", "message", "toolResults"],
        "message_start" | "message_end" => &["type", "message"],
        "message_update" => &["type", "usage", "assistantMessageEvent"],
        "tool_execution_start" => &["type", "toolCallId", "toolName", "args"],
        "tool_execution_update" => &["type", "toolCallId", "toolName", "args", "partialResult"],
        "tool_execution_end" => &["type", "toolCallId", "toolName", "result", "isError"],
        "auto_retry_start" | "summarization_retry_scheduled" => {
            &["type", "attempt", "maxAttempts", "delayMs", "errorMessage"]
        }
        "auto_retry_end" => &["type", "success", "attempt", "finalError"],
        "compaction_start" => &["type", "reason"],
        "compaction_end" => &[
            "type",
            "reason",
            "result",
            "aborted",
            "willRetry",
            "errorMessage",
        ],
        "summarization_retry_attempt_start" => &["type", "source", "reason"],
        "summarization_retry_finished" => &["type"],
        "queue_update" => &["type", "steering", "followUp"],
        "entry_appended" => &["type", "entry"],
        "session_info_changed" => &["type", "name"],
        "thinking_level_changed" => &["type", "level"],
        "bash_execution_update" => &["type", "id", "delta"],
        _ => return false,
    };
    object.keys().any(|key| !allowed.contains(&key.as_str()))
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
    object.len() == required.len() && required.iter().all(|key| object.contains_key(*key))
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
