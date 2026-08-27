use std::num::NonZeroU64;
use std::sync::Arc;

use serde_json::{Value, json};

use super::*;
use crate::execution::workflow::agent::{AgentOutcome, CapturedJson};

const CWD: &str = "/execution/worktree";
const RESPONSE_SUCCESS: &[u8] = include_bytes!("fixtures/response-success.jsonl");
const NATIVE_RECOVERY: &[u8] = include_bytes!("fixtures/native-recovery.jsonl");
const SIBLING_RESULT_CORRECTION: &[u8] = include_bytes!("fixtures/sibling-result-correction.jsonl");
const TERMINAL_TOOL_USE: &[u8] = include_bytes!("fixtures/terminal-tool-use.jsonl");

fn parser(kind: AgentValueKind) -> PiJsonV1Parser {
    PiJsonV1Parser::profile(Arc::from(CWD), kind)
}

fn result_parser() -> PiJsonV1Parser {
    PiJsonV1Parser::new(
        Arc::from(CWD),
        AgentValueKind::Result,
        NonZeroU64::new(MAXIMUM_RESPONSE_BYTES).unwrap(),
        PiJsonV1ProtocolLimits::profile(),
        Some(Arc::from("scherzo_result_fixed")),
    )
}

impl PiJsonV1Parser {
    fn push_ignoring(&mut self, bytes: &[u8]) -> Result<(), AgentFailureCause> {
        self.push_stdout(bytes, drop)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct RecordedReplay {
    observations: Vec<AgentObservation>,
    outcome: AgentOutcome,
}

fn replay(bytes: &[u8], kind: AgentValueKind) -> RecordedReplay {
    let mut parser = parser(kind);
    let mut observations = Vec::new();
    let _ = parser.push_stdout(bytes, |observation| observations.push(observation));
    RecordedReplay {
        observations,
        outcome: parser.finish(PiJsonV1ProcessCompletion::exited(true)),
    }
}

fn encoded(values: &[Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in values {
        serde_json::to_writer(&mut bytes, value).unwrap();
        bytes.push(b'\n');
    }
    bytes
}

fn values(bytes: &[u8]) -> Vec<Value> {
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

fn usage() -> Value {
    json!({
        "input": 1,
        "output": 1,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 2,
        "cost": {
            "input": 0,
            "output": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "total": 0
        }
    })
}

fn assistant(content: Value, stop_reason: &str, timestamp: u64) -> Value {
    json!({
        "role": "assistant",
        "content": content,
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": usage(),
        "stopReason": stop_reason,
        "timestamp": timestamp
    })
}

fn session() -> Value {
    json!({
        "type": "session",
        "version": 3,
        "id": "00000000-0000-4000-8000-000000000004",
        "timestamp": "2026-07-30T12:00:00Z",
        "cwd": CWD
    })
}

fn terminal_transcript(message: Value) -> Vec<u8> {
    encoded(&[
        session(),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": message}),
        json!({"type": "message_end", "message": message}),
        json!({"type": "turn_end", "message": message, "toolResults": []}),
        json!({"type": "agent_end", "messages": [message], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ])
}

fn event_offset(bytes: &[u8], event_type: &str) -> usize {
    bytes
        .split_inclusive(|byte| *byte == b'\n')
        .scan(0, |offset, frame| {
            let start = *offset;
            *offset += frame.len();
            Some((start, frame))
        })
        .find_map(|(offset, frame)| {
            let event: Value = serde_json::from_slice(frame.strip_suffix(b"\n").unwrap()).unwrap();
            (event["type"] == event_type).then_some(offset)
        })
        .unwrap()
}

fn event_offset_for_call(bytes: &[u8], event_type: &str, call_id: &str) -> usize {
    bytes
        .split_inclusive(|byte| *byte == b'\n')
        .scan(0, |offset, frame| {
            let start = *offset;
            *offset += frame.len();
            Some((start, frame))
        })
        .find_map(|(offset, frame)| {
            let event: Value = serde_json::from_slice(frame.strip_suffix(b"\n").unwrap()).unwrap();
            (event["type"] == event_type && event["toolCallId"] == call_id).then_some(offset)
        })
        .unwrap()
}

fn accept_result(parser: &mut PiJsonV1Parser, call_id: &str, arguments: Arc<Value>) {
    parser
        .correlate_result_request("scherzo_result_fixed", call_id, arguments.as_ref())
        .unwrap();
    parser
        .accept_result(AcceptedPiJsonV1Result::new(
            Arc::from(call_id),
            Arc::from("scherzo_result_fixed"),
            arguments,
            CapturedJson::fixture(Arc::new(json!({"answer": 42}))),
        ))
        .unwrap();
}

fn replay_accepted_result(bytes: &[u8], call_id: &str) -> RecordedReplay {
    let end_offset = event_offset_for_call(bytes, "tool_execution_end", call_id);
    let mut parser = result_parser();
    let mut observations = Vec::new();
    parser
        .push_stdout(&bytes[..end_offset], |observation| {
            observations.push(observation);
        })
        .unwrap();
    accept_result(
        &mut parser,
        call_id,
        Arc::new(json!({"result": {"answer": 42}})),
    );
    parser
        .push_stdout(&bytes[end_offset..], |observation| {
            observations.push(observation);
        })
        .unwrap();
    RecordedReplay {
        observations,
        outcome: parser.finish(PiJsonV1ProcessCompletion::exited(true)),
    }
}

fn assert_failure(outcome: &AgentOutcome, cause: AgentFailureCause) {
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("expected failure, got {outcome:?}");
    };
    assert_eq!(failure.cause(), &cause);
}

fn protocol_rejection(outcome: &AgentOutcome) -> Value {
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("expected failure, got {outcome:?}");
    };
    serde_json::to_value(failure.protocol_rejection().unwrap()).unwrap()
}

fn completed_response(outcome: &AgentOutcome) -> &str {
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("expected completed response, got {outcome:?}");
    };
    response.as_str()
}

#[test]
fn recorded_response_is_ordered_bounded_and_repeatable_across_chunking() {
    let expected = replay(RESPONSE_SUCCESS, AgentValueKind::Response);
    let mut chunked_parser = parser(AgentValueKind::Response);
    let mut observations = Vec::new();
    for chunk in RESPONSE_SUCCESS.chunks(7) {
        chunked_parser
            .push_stdout(chunk, |observation| observations.push(observation))
            .unwrap();
    }
    let chunked = RecordedReplay {
        observations,
        outcome: chunked_parser.finish(PiJsonV1ProcessCompletion::exited(true)),
    };

    assert_eq!(chunked, expected);
    assert_eq!(completed_response(&expected.outcome), "hello world");
    let text = expected
        .observations
        .iter()
        .filter_map(|observation| match observation {
            AgentObservation::AssistantText { text } => Some(text.as_ref()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(text, "hello world");
}

#[test]
fn divergent_finalized_messages_and_failed_ordinary_tools_reach_native_terminal_outcome() {
    let failed_call = assistant(
        json!([{
            "type": "toolCall",
            "id": "call-failed",
            "name": "read",
            "arguments": {"path": "missing"}
        }]),
        "toolUse",
        2,
    );
    let failed_result = json!({
        "role": "toolResult",
        "toolCallId": "call-failed",
        "toolName": "read",
        "content": [{"type": "text", "text": "not found"}],
        "details": {},
        "isError": true,
        "timestamp": 3
    });
    let pending = assistant(json!([]), "pending", 4);
    let divergent = assistant(
        json!([{
            "type": "toolCall",
            "id": "call-search",
            "name": "web_search",
            "arguments": {"query": "ordinary work"}
        }]),
        "toolUse",
        4,
    );
    let terminal = assistant(
        json!([{"type": "text", "text": "native terminal response"}]),
        "stop",
        5,
    );
    let transcript = encoded(&[
        session(),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": failed_call}),
        json!({"type": "message_end", "message": failed_call}),
        json!({"type": "tool_execution_end", "toolCallId": "call-failed", "toolName": "read", "result": {"content": [{"type": "text", "text": "not found"}], "details": {}}, "isError": true}),
        json!({"type": "message_start", "message": failed_result}),
        json!({"type": "message_end", "message": failed_result}),
        json!({"type": "turn_end", "message": failed_call, "toolResults": [failed_result]}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": pending}),
        json!({"type": "message_update", "usage": usage(), "assistantMessageEvent": {"type": "text_start", "contentIndex": 0}}),
        json!({"type": "message_update", "usage": usage(), "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": "streamed draft"}}),
        json!({"type": "message_end", "message": divergent}),
        json!({"type": "turn_end", "message": failed_call, "toolResults": []}),
        json!({"type": "agent_end", "messages": [failed_call, failed_result, terminal], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ]);

    let replay = replay(&transcript, AgentValueKind::Response);
    assert_eq!(
        completed_response(&replay.outcome),
        "native terminal response"
    );
    assert!(replay.observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::ToolResult { call_id, is_error: true, .. }
            if call_id.as_ref() == "call-failed"
    )));
    assert!(replay.observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::ToolCall { call_id, .. } if call_id.as_ref() == "call-search"
    )));
}

#[test]
fn ordinary_tool_events_are_normalized_without_correlation_authority() {
    let terminal = assistant(json!([{"type": "text", "text": "continued"}]), "stop", 8);
    let transcript = encoded(&[
        session(),
        json!({"type": "agent_start"}),
        json!({"type": "tool_execution_update", "toolCallId": "unknown", "toolName": "inspect", "args": {"v": 2}, "partialResult": {"content": [{"type": "text", "text": "partial"}]}}),
        json!({"type": "tool_execution_end", "toolCallId": "unknown", "toolName": "inspect", "result": {"content": [{"type": "text", "text": "first end"}]}, "isError": false}),
        json!({"type": "tool_execution_start", "toolCallId": "unknown", "toolName": "renamed", "args": {"v": 3}}),
        json!({"type": "tool_execution_start", "toolCallId": "unknown", "toolName": "inspect", "args": {"v": 4}}),
        json!({"type": "tool_execution_end", "toolCallId": "unknown", "toolName": "inspect", "result": {"content": [{"type": "text", "text": "repeated end"}]}, "isError": true}),
        json!({"type": "agent_end", "messages": [terminal], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ]);

    let replay = replay(&transcript, AgentValueKind::Response);
    assert_eq!(completed_response(&replay.outcome), "continued");
    assert_eq!(
        replay
            .observations
            .iter()
            .filter(|observation| matches!(observation, AgentObservation::ToolCall { .. }))
            .count(),
        5
    );
    assert_eq!(
        replay
            .observations
            .iter()
            .filter(|observation| matches!(observation, AgentObservation::ToolResult { .. }))
            .count(),
        2
    );
}

#[test]
fn malformed_observation_shapes_and_unknown_vocabularies_are_unrecognized() {
    let terminal = assistant(
        json!([{"type": "text", "text": "still completed"}]),
        "stop",
        3,
    );
    let transcript = encoded(&[
        session(),
        json!({"type": "agent_start"}),
        json!({"type": "thinking_level_changed", "level": "ultra"}),
        json!({"type": "compaction_start", "reason": "providerNewReason"}),
        json!({"type": "message_start", "message": {"role": "futureRole", "content": [], "timestamp": 2}}),
        json!({"type": "message_end", "message": assistant(json!([{"type": "audio", "data": "opaque"}]), "stop", 2)}),
        json!({"type": "message_update", "usage": usage(), "assistantMessageEvent": {"type": "future_delta", "contentIndex": 0, "delta": "opaque"}}),
        json!({"type": "queue_update", "steering": [], "followUp": [], "futureField": true}),
        json!({"opaque": true}),
        json!({"type": "agent_end", "messages": [{"role": "futureRole", "content": []}, terminal], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ]);

    let replay = replay(&transcript, AgentValueKind::Response);
    assert_eq!(completed_response(&replay.outcome), "still completed");
    assert!(
        replay
            .observations
            .iter()
            .filter(|observation| matches!(
                observation,
                AgentObservation::UnrecognizedHarnessEvent { .. }
            ))
            .count()
            >= 7
    );
}

#[test]
fn agent_end_last_parseable_assistant_is_the_only_terminal_candidate() {
    let observed = assistant(
        json!([{"type": "text", "text": "observed transcript"}]),
        "stop",
        2,
    );
    let earlier = assistant(
        json!([{"type": "text", "text": "earlier candidate"}]),
        "stop",
        3,
    );
    let authoritative = assistant(
        json!([{"type": "text", "text": "authoritative candidate"}]),
        "stop",
        4,
    );
    let transcript = encoded(&[
        session(),
        json!({"type": "agent_start"}),
        json!({"type": "message_start", "message": observed}),
        json!({"type": "message_end", "message": observed}),
        json!({"type": "turn_end", "message": observed, "toolResults": []}),
        json!({"type": "agent_end", "messages": [earlier, authoritative, {"role": "futureRole"}], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ]);

    let replay = replay(&transcript, AgentValueKind::Response);
    assert_eq!(
        completed_response(&replay.outcome),
        "authoritative candidate"
    );
}

#[test]
fn session_header_treats_native_id_and_timestamp_formats_as_informational() {
    let mut header = session();
    header["id"] = json!("native-session-id");
    header["timestamp"] = json!("native timestamp format");
    let terminal = assistant(json!([]), "stop", 2);
    let transcript = encoded(&[
        header,
        json!({"type": "agent_start"}),
        json!({"type": "agent_end", "messages": [terminal], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ]);

    assert_eq!(
        replay(&transcript, AgentValueKind::None).outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );
}

#[test]
fn agent_start_after_non_retrying_end_is_a_protocol_failure() {
    let terminal = assistant(json!([]), "stop", 2);
    let mut parser = parser(AgentValueKind::None);
    parser
        .push_ignoring(&encoded(&[
            session(),
            json!({"type": "agent_start"}),
            json!({"type": "agent_end", "messages": [terminal], "willRetry": false}),
        ]))
        .unwrap();

    assert_eq!(
        parser.push_ignoring(b"{\"type\":\"agent_start\"}\n"),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
    assert_eq!(
        protocol_rejection(&parser.finish(PiJsonV1ProcessCompletion::exited(false)))["detail"]["reason"],
        "event_transition_invalid"
    );
}

#[test]
fn framing_handshake_lifecycle_terminal_and_eof_surfaces_remain_strict() {
    let mut malformed = parser(AgentValueKind::None);
    assert_eq!(
        malformed.push_ignoring(b"not-json\n"),
        Err(AgentFailureCause::HarnessStartFailed)
    );
    assert_eq!(
        protocol_rejection(&malformed.finish(PiJsonV1ProcessCompletion::exited(false)))["detail"]["reason"],
        "frame_decode_failed"
    );

    let cases = [
        encoded(&[session(), json!({"type": "agent_start"}), session()]),
        encoded(&[
            session(),
            json!({"type": "agent_start"}),
            json!({"type": "agent_start"}),
        ]),
        encoded(&[
            session(),
            json!({"type": "agent_start"}),
            json!({"type": "agent_settled"}),
        ]),
        encoded(&[
            session(),
            json!({"type": "agent_start"}),
            json!({"type": "agent_end", "messages": [assistant(json!([]), "futureStop", 2)], "willRetry": false}),
        ]),
    ];
    for transcript in cases {
        let replay = replay(&transcript, AgentValueKind::None);
        assert_failure(&replay.outcome, AgentFailureCause::HarnessProtocolFailed);
    }

    let terminal = assistant(json!([]), "stop", 2);
    let mut after_settlement = terminal_transcript(terminal);
    after_settlement.extend(encoded(&[json!({"type": "turn_start"})]));
    let settled_replay = replay(&after_settlement, AgentValueKind::None);
    assert_failure(
        &settled_replay.outcome,
        AgentFailureCause::HarnessProtocolFailed,
    );
    assert_eq!(
        protocol_rejection(&settled_replay.outcome)["detail"]["reason"],
        "event_after_settlement"
    );

    let mut missing_settlement = values(RESPONSE_SUCCESS);
    missing_settlement.retain(|event| event["type"] != "agent_settled");
    assert_failure(
        &replay(&encoded(&missing_settlement), AgentValueKind::Response).outcome,
        AgentFailureCause::HarnessProtocolFailed,
    );

    let mut partial = parser(AgentValueKind::None);
    partial.push_ignoring(b"{").unwrap();
    let outcome = partial.finish(PiJsonV1ProcessCompletion::exited(false));
    assert_eq!(
        protocol_rejection(&outcome)["detail"]["reason"],
        "partial_frame_at_end_of_stream"
    );
}

#[test]
fn protocol_rejection_uses_the_revised_closed_state() {
    let mut parser = parser(AgentValueKind::None);
    parser
        .push_ignoring(&encoded(&[session(), json!({"type": "agent_start"})]))
        .unwrap();
    assert_eq!(
        parser.push_ignoring(b"{\"type\":\"agent_start\"}\n"),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
    let rejection = protocol_rejection(&parser.finish(PiJsonV1ProcessCompletion::exited(false)));
    assert_eq!(
        rejection,
        json!({
            "schemaVersion": 1,
            "profile": "PiJsonV1",
            "detail": {
                "reason": "event_transition_invalid",
                "stage": "event_payload",
                "outerEvent": "agent_start",
                "state": {
                    "sessionHeaderSeen": true,
                    "agentStarted": true,
                    "terminalCandidateRetained": false,
                    "resultAccepted": false,
                    "settled": false
                }
            }
        })
    );
}

#[test]
fn native_retry_observations_do_not_override_the_final_response() {
    let replay = replay(NATIVE_RECOVERY, AgentValueKind::Response);
    assert_eq!(completed_response(&replay.outcome), "recovered");
    assert!(replay.observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::RetryStarted
        }
    )));
}

#[test]
fn terminal_stop_reason_uses_the_native_mode_table() {
    for (reason, expected) in [
        (
            "length",
            AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::ModelOutputTruncated,
            },
        ),
        (
            "error",
            AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::ModelError,
            },
        ),
        (
            "aborted",
            AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::ModelAborted,
            },
        ),
        (
            "toolUse",
            AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnexpectedTerminalToolUse,
            },
        ),
    ] {
        let message = assistant(json!([]), reason, 2);
        assert_failure(
            &replay(&terminal_transcript(message), AgentValueKind::None).outcome,
            expected,
        );
    }
}

#[test]
fn response_limit_is_enforced_incrementally_and_on_the_terminal_candidate() {
    let mut parser = PiJsonV1Parser::new(
        Arc::from(CWD),
        AgentValueKind::Response,
        NonZeroU64::new(3).unwrap(),
        PiJsonV1ProtocolLimits::profile(),
        None,
    );
    assert_eq!(
        parser.push_ignoring(RESPONSE_SUCCESS),
        Err(AgentFailureCause::CapturedValueTooLarge)
    );

    let message = assistant(json!([{"type": "text", "text": "four"}]), "stop", 2);
    let replay = {
        let mut parser = PiJsonV1Parser::new(
            Arc::from(CWD),
            AgentValueKind::Response,
            NonZeroU64::new(3).unwrap(),
            PiJsonV1ProtocolLimits::profile(),
            None,
        );
        let _ = parser.push_ignoring(&terminal_transcript(message));
        parser.finish(PiJsonV1ProcessCompletion::exited(true))
    };
    assert_failure(&replay, AgentFailureCause::CapturedValueTooLarge);
}

#[test]
fn retained_reconstruction_and_frame_bounds_remain_authoritative() {
    let limits = PiJsonV1ProtocolLimits {
        maximum_frame_bytes: NonZeroU64::new(512).unwrap(),
    };
    let mut parser = PiJsonV1Parser::new(
        Arc::from(CWD),
        AgentValueKind::None,
        NonZeroU64::new(1024).unwrap(),
        limits,
        None,
    );
    let pending = assistant(json!([]), "pending", 2);
    parser
        .push_ignoring(&encoded(&[
            session(),
            json!({"type": "agent_start"}),
            json!({"type": "message_start", "message": pending}),
        ]))
        .unwrap();
    let mut rejected = false;
    for index in 0..64 {
        let updates = encoded(&[
            json!({"type": "message_update", "usage": usage(), "assistantMessageEvent": {"type": "text_start", "contentIndex": index}}),
            json!({"type": "message_update", "usage": usage(), "assistantMessageEvent": {"type": "text_end", "contentIndex": index, "content": ""}}),
        ]);
        if parser.push_ignoring(&updates).is_err() {
            rejected = true;
            break;
        }
    }
    assert!(rejected);
    let outcome = parser.finish(PiJsonV1ProcessCompletion::exited(false));
    assert_eq!(
        protocol_rejection(&outcome)["detail"]["reason"],
        "retained_state_limit_exceeded"
    );

    let limits = PiJsonV1ProtocolLimits {
        maximum_frame_bytes: NonZeroU64::new(128).unwrap(),
    };
    let mut oversized = PiJsonV1Parser::new(
        Arc::from(CWD),
        AgentValueKind::None,
        NonZeroU64::new(1024).unwrap(),
        limits,
        None,
    );
    assert_eq!(
        oversized.push_ignoring(&[b'x'; 129]),
        Err(AgentFailureCause::HarnessStartFailed)
    );
}

#[test]
fn result_mode_accepts_only_the_correlated_singleton_lifecycle() {
    let replay = replay_accepted_result(TERMINAL_TOOL_USE, "call-result");
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = replay.outcome else {
        panic!("correlated result must complete");
    };
    assert_eq!(result.value(), &json!({"answer": 42}));

    let end_offset = event_offset(TERMINAL_TOOL_USE, "tool_execution_end");
    for (name, call_id, arguments) in [
        (
            "scherzo_result_wrong",
            "call-result",
            json!({"result": {"answer": 42}}),
        ),
        (
            "scherzo_result_fixed",
            "call-wrong",
            json!({"result": {"answer": 42}}),
        ),
        (
            "scherzo_result_fixed",
            "call-result",
            json!({"result": {"answer": 41}}),
        ),
    ] {
        let mut parser = result_parser();
        parser
            .push_ignoring(&TERMINAL_TOOL_USE[..end_offset])
            .unwrap();
        assert_eq!(
            parser.correlate_result_request(name, call_id, &arguments),
            Err(AgentFailureCause::HarnessProtocolFailed)
        );
    }
}

#[test]
fn sibling_result_rejection_is_recoverable_and_a_later_singleton_succeeds() {
    let replay = replay_accepted_result(SIBLING_RESULT_CORRECTION, "call-corrected-result");
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = replay.outcome else {
        panic!("corrected singleton result must complete");
    };
    assert_eq!(result.value(), &json!({"answer": 42}));
    assert!(replay.observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::ToolResult { call_id, is_error: true, .. }
            if call_id.as_ref() == "call-blocked-result"
    )));
}

#[test]
fn blocked_result_call_must_finish_before_later_result_correlation() {
    let mut events = values(SIBLING_RESULT_CORRECTION);
    events.retain(|event| {
        event["type"] != "tool_execution_end" || event["toolCallId"] != "call-blocked-result"
    });
    let transcript = encoded(&events);
    let corrected_end =
        event_offset_for_call(&transcript, "tool_execution_end", "call-corrected-result");
    let mut parser = result_parser();
    parser.push_ignoring(&transcript[..corrected_end]).unwrap();

    assert_eq!(
        parser.correlate_result_request(
            "scherzo_result_fixed",
            "call-corrected-result",
            &json!({"result": {"answer": 42}}),
        ),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
    assert_eq!(
        protocol_rejection(&parser.finish(PiJsonV1ProcessCompletion::exited(false)))["detail"]["reason"],
        "result_correlation_invalid"
    );
}

#[test]
fn malformed_generated_result_starts_are_protocol_failures() {
    let message = assistant(
        json!([{
            "type": "toolCall",
            "id": "call-result",
            "name": "scherzo_result_fixed",
            "arguments": {"result": {"answer": 42}}
        }]),
        "toolUse",
        2,
    );
    let starts = [
        json!({
            "type": "tool_execution_start",
            "toolName": "scherzo_result_fixed",
            "args": {"result": {"answer": 42}}
        }),
        json!({
            "type": "tool_execution_start",
            "toolCallId": "call-result",
            "toolName": "scherzo_result_fixed",
            "args": {"result": {"answer": 42}},
            "futureField": true
        }),
    ];

    for start in starts {
        let mut parser = result_parser();
        parser
            .push_ignoring(&encoded(&[
                session(),
                json!({"type": "agent_start"}),
                json!({"type": "message_end", "message": message.clone()}),
            ]))
            .unwrap();
        assert_eq!(
            parser.push_ignoring(&encoded(&[start])),
            Err(AgentFailureCause::HarnessProtocolFailed)
        );
        assert_eq!(
            protocol_rejection(&parser.finish(PiJsonV1ProcessCompletion::exited(false)))["detail"]
                ["reason"],
            "result_correlation_invalid"
        );
    }
}

#[test]
fn successful_execution_of_a_blocked_result_call_is_a_protocol_failure() {
    for additive_field in [false, true] {
        let mut events = values(SIBLING_RESULT_CORRECTION);
        let blocked_end = events
            .iter_mut()
            .find(|event| {
                event["type"] == "tool_execution_end"
                    && event["toolCallId"] == "call-blocked-result"
            })
            .unwrap();
        blocked_end["isError"] = json!(false);
        blocked_end["result"]["terminate"] = json!(true);
        if additive_field {
            blocked_end["futureField"] = json!(true);
        }
        let transcript = encoded(&events);
        let end = event_offset_for_call(&transcript, "tool_execution_end", "call-blocked-result");
        let frame_length = transcript[end..]
            .iter()
            .position(|byte| *byte == b'\n')
            .unwrap()
            + 1;
        let mut parser = result_parser();
        assert_eq!(
            parser.push_ignoring(&transcript[..end + frame_length]),
            Err(AgentFailureCause::HarnessProtocolFailed)
        );
        assert_eq!(
            protocol_rejection(&parser.finish(PiJsonV1ProcessCompletion::exited(false)))["detail"]
                ["reason"],
            "result_correlation_invalid"
        );
    }
}

#[test]
fn rejected_validation_cannot_be_followed_by_native_success() {
    let end_offset = event_offset(TERMINAL_TOOL_USE, "tool_execution_end");
    let mut parser = result_parser();
    parser
        .push_ignoring(&TERMINAL_TOOL_USE[..end_offset])
        .unwrap();
    parser
        .correlate_result_request(
            "scherzo_result_fixed",
            "call-result",
            &json!({"result": {"answer": 42}}),
        )
        .unwrap();
    let contradictory = encoded(&[json!({
        "type": "tool_execution_end",
        "toolCallId": "call-result",
        "toolName": "scherzo_result_fixed",
        "result": {"content": [{"type": "text", "text": "success"}], "terminate": true},
        "isError": false
    })]);
    assert_eq!(
        parser.push_ignoring(&contradictory),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
}

#[test]
fn accepted_result_rejects_new_work_and_terminal_candidate_mismatch() {
    let end_offset = event_offset(TERMINAL_TOOL_USE, "tool_execution_end");
    let mut parser = result_parser();
    parser
        .push_ignoring(&TERMINAL_TOOL_USE[..end_offset])
        .unwrap();
    accept_result(
        &mut parser,
        "call-result",
        Arc::new(json!({"result": {"answer": 42}})),
    );
    assert_eq!(
        parser.push_ignoring(b"{\"type\":\"turn_start\"}\n"),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );

    let mut events = values(TERMINAL_TOOL_USE);
    let agent_end = events
        .iter_mut()
        .find(|event| event["type"] == "agent_end")
        .unwrap();
    let terminal = agent_end["messages"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .rev()
        .find(|message| message["role"] == "assistant")
        .unwrap();
    terminal["content"][0]["arguments"] = json!({"result": {"answer": 41}});
    let transcript = encoded(&events);
    let end_offset = event_offset(&transcript, "tool_execution_end");
    let mut parser = result_parser();
    parser.push_ignoring(&transcript[..end_offset]).unwrap();
    accept_result(
        &mut parser,
        "call-result",
        Arc::new(json!({"result": {"answer": 42}})),
    );
    assert_eq!(
        parser.push_ignoring(&transcript[end_offset..]),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
}

#[test]
fn ordinary_call_ids_do_not_participate_in_result_correlation() {
    let mut events = values(TERMINAL_TOOL_USE);
    let ordinary = assistant(
        json!([{
            "type": "toolCall",
            "id": "call-result",
            "name": "inspect",
            "arguments": {"path": "old"}
        }]),
        "toolUse",
        1,
    );
    let insertion = events
        .iter()
        .position(|event| event["type"] == "agent_start")
        .unwrap()
        + 1;
    events.insert(
        insertion,
        json!({"type": "message_end", "message": ordinary}),
    );
    let transcript = encoded(&events);
    let replay = replay_accepted_result(&transcript, "call-result");
    assert!(matches!(
        replay.outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::Result(_))
    ));
}

#[test]
fn result_terminal_correlation_uses_semantic_numeric_equality() {
    let mut events = values(TERMINAL_TOOL_USE);
    for event in &mut events {
        if let Some(message) = event.get_mut("message")
            && message["role"] == "assistant"
            && message["content"]
                .as_array()
                .is_some_and(|content| !content.is_empty())
        {
            message["content"][0]["arguments"] = json!({"result": {"answer": 42.0}});
        }
        if event["type"] == "agent_end" {
            for message in event["messages"].as_array_mut().unwrap() {
                if message["role"] == "assistant" {
                    message["content"][0]["arguments"] = json!({"result": {"answer": 42.0}});
                }
            }
        }
        if event["type"] == "tool_execution_start" {
            event["args"] = json!({"result": {"answer": 42.0}});
        }
    }
    let transcript = encoded(&events);
    let replay = replay_accepted_result(&transcript, "call-result");
    assert!(matches!(
        replay.outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::Result(_))
    ));
}

#[test]
fn unknown_events_after_settlement_remain_observational_but_new_work_does_not() {
    let message = assistant(json!([]), "stop", 2);
    let mut transcript = terminal_transcript(message.clone());
    transcript.extend(encoded(&[
        json!({"type": "future_event", "value": 1}),
        json!({"type": "thinking_level_changed", "level": "future"}),
    ]));
    let observational_replay = replay(&transcript, AgentValueKind::None);
    assert!(matches!(
        observational_replay.outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    ));

    let mut transcript = terminal_transcript(message);
    transcript.extend(encoded(&[json!({"type": "tool_execution_start"})]));
    let work_replay = replay(&transcript, AgentValueKind::None);
    assert_failure(
        &work_replay.outcome,
        AgentFailureCause::HarnessProtocolFailed,
    );
}
