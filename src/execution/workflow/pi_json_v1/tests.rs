use std::num::NonZeroU64;
use std::sync::Arc;

use serde_json::{Value, json};

use super::*;
use crate::execution::workflow::agent::{
    AgentHarnessFailureDetail, AgentOutcome, BoundedSchemaValidAgentResult,
};

const CWD: &str = "/execution/worktree";
const RESPONSE_SUCCESS: &[u8] = include_bytes!("fixtures/response-success.jsonl");
const NATIVE_RECOVERY: &[u8] = include_bytes!("fixtures/native-recovery.jsonl");
const TERMINAL_TOOL_USE: &[u8] = include_bytes!("fixtures/terminal-tool-use.jsonl");

fn parser(kind: AgentValueKind) -> PiJsonV1Parser {
    PiJsonV1Parser::profile(Arc::from(CWD), kind)
}

#[derive(Debug, Eq, PartialEq)]
struct RecordedReplay {
    observations: Vec<AgentObservation>,
    outcome: AgentOutcome,
}

impl RecordedReplay {
    fn observations(&self) -> &[AgentObservation] {
        &self.observations
    }

    fn outcome(&self) -> &AgentOutcome {
        &self.outcome
    }
}

impl PiJsonV1Parser {
    fn push_ignoring(&mut self, bytes: &[u8]) -> Result<(), AgentFailureCause> {
        self.push_stdout(bytes, drop)
    }
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

fn simple_transcript(reason: &str, content: Value) -> Vec<u8> {
    let mut assistant = json!({
        "role": "assistant",
        "content": content,
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": {
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
        },
        "stopReason": reason,
        "timestamp": 2
    });
    if reason == "error" {
        assistant["errorMessage"] = json!("terminal provider error");
    }
    encoded(&[
        json!({
            "type": "session",
            "version": 3,
            "id": "00000000-0000-4000-8000-000000000004",
            "timestamp": "2026-07-30T12:00:00Z",
            "cwd": CWD
        }),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": assistant}),
        json!({"type": "message_end", "message": assistant}),
        json!({"type": "turn_end", "message": assistant, "toolResults": []}),
        json!({"type": "agent_end", "messages": [assistant], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ])
}

fn terminal(outcome: AgentOutcome) -> RecordedReplay {
    RecordedReplay {
        observations: Vec::new(),
        outcome,
    }
}

fn tool_error_then_success_transcript() -> Vec<u8> {
    let user = json!({"role": "user", "content": "prompt", "timestamp": 1});
    let usage = json!({
        "input": 1,
        "output": 1,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 2,
        "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}
    });
    let calling = json!({
        "role": "assistant",
        "content": [{"type": "toolCall", "id": "call-failed", "name": "inspect", "arguments": {"path": "missing"}}],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": usage,
        "stopReason": "toolUse",
        "timestamp": 2
    });
    let result = json!({
        "role": "toolResult",
        "toolCallId": "call-failed",
        "toolName": "inspect",
        "content": [{"type": "text", "text": "not found"}],
        "details": {},
        "isError": true,
        "timestamp": 3
    });
    let recovered = json!({
        "role": "assistant",
        "content": [{"type": "text", "text": "recovered"}],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": usage,
        "stopReason": "stop",
        "timestamp": 4
    });
    encoded(&[
        json!({"type": "session", "version": 3, "id": "00000000-0000-4000-8000-000000000008", "timestamp": "2026-07-30T12:00:00Z", "cwd": CWD}),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": user}),
        json!({"type": "message_end", "message": user}),
        json!({"type": "message_start", "message": calling}),
        json!({"type": "message_end", "message": calling}),
        json!({"type": "tool_execution_start", "toolCallId": "call-failed", "toolName": "inspect", "args": {"path": "missing"}}),
        json!({"type": "tool_execution_end", "toolCallId": "call-failed", "toolName": "inspect", "result": {"content": [{"type": "text", "text": "not found"}], "details": {}}, "isError": true}),
        json!({"type": "message_start", "message": result}),
        json!({"type": "message_end", "message": result}),
        json!({"type": "turn_end", "message": calling, "toolResults": [result]}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": recovered}),
        json!({"type": "message_end", "message": recovered}),
        json!({"type": "turn_end", "message": recovered, "toolResults": []}),
        json!({"type": "agent_end", "messages": [user, calling, result, recovered], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ])
}

fn assert_failure(transcript: &RecordedReplay, cause: AgentFailureCause) {
    assert_eq!(transcript.outcome(), &AgentOutcome::Failed { cause });
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
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = expected.outcome()
    else {
        panic!("recorded response must complete");
    };
    assert_eq!(response.as_str(), "hello world");
    let text = expected
        .observations()
        .iter()
        .filter_map(|observation| match observation {
            AgentObservation::AssistantText { text } => Some(text.as_ref()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(text, "hello world");
}

#[test]
fn recorded_native_retry_keeps_intermediate_error_observational() {
    let transcript = replay(NATIVE_RECOVERY, AgentValueKind::Response);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
        transcript.outcome()
    else {
        panic!("native recovery must produce one successful invocation");
    };
    assert_eq!(response.as_str(), "recovered");
    assert!(transcript.observations().iter().any(|observation| matches!(
        observation,
        AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Error,
            message,
        } if message.as_ref() == "provider unavailable"
    )));
    assert_eq!(
        transcript
            .observations()
            .iter()
            .filter(|observation| matches!(
                observation,
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::RetryStarted
                }
            ))
            .count(),
        1
    );
}

#[test]
fn recoverable_tool_error_does_not_override_the_later_terminal_response() {
    let transcript = replay(
        &tool_error_then_success_transcript(),
        AgentValueKind::Response,
    );
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
        transcript.outcome()
    else {
        panic!("tool recovery must complete from the ultimate assistant turn");
    };
    assert_eq!(response.as_str(), "recovered");
    assert!(transcript.observations().iter().any(|observation| matches!(
        observation,
        AgentObservation::ToolResult {
            call_id,
            is_error: true,
            ..
        } if call_id.as_ref() == "call-failed"
    )));
}

#[test]
fn compaction_and_provider_failures_remain_observational_during_native_recovery() {
    let mut recovering = values(NATIVE_RECOVERY);
    let first_end = recovering
        .iter()
        .position(|event| event["type"] == "agent_end")
        .unwrap();
    recovering[first_end]["willRetry"] = json!(false);
    recovering.retain(|event| {
        !matches!(
            event["type"].as_str(),
            Some("auto_retry_start" | "auto_retry_end")
        )
    });
    let first_end = recovering
        .iter()
        .position(|event| event["type"] == "agent_end")
        .unwrap();
    recovering.splice(
        first_end + 1..first_end + 1,
        [
            json!({"type": "compaction_start", "reason": "overflow"}),
            json!({"type": "summarization_retry_scheduled", "attempt": 1, "maxAttempts": 3, "delayMs": 20, "errorMessage": "summary provider unavailable"}),
            json!({"type": "summarization_retry_attempt_start", "source": "compaction", "reason": "overflow"}),
            json!({"type": "summarization_retry_finished"}),
            json!({"type": "compaction_end", "reason": "overflow", "result": {}, "aborted": false, "willRetry": true}),
        ],
    );
    let transcript = replay(&encoded(&recovering), AgentValueKind::Response);
    assert!(matches!(transcript.outcome(), AgentOutcome::Completed(_)));
    assert!(transcript.observations().iter().any(|observation| matches!(
        observation,
        AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::CompactionCompleted
        }
    )));

    let mut exhausted = values(&simple_transcript("error", json!([])));
    let agent_end = exhausted
        .iter()
        .position(|event| event["type"] == "agent_end")
        .unwrap();
    exhausted.splice(
        agent_end + 1..agent_end + 1,
        [
            json!({"type": "compaction_start", "reason": "overflow"}),
            json!({"type": "compaction_end", "reason": "overflow", "aborted": false, "willRetry": false, "errorMessage": "compaction failed"}),
        ],
    );
    assert_failure(
        &replay(&encoded(&exhausted), AgentValueKind::None),
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::ModelError,
        },
    );

    let mut diagnosed = values(&simple_transcript(
        "stop",
        json!([{"type": "text", "text": "recovered"}]),
    ));
    visit_assistant_messages(&mut diagnosed, |assistant| {
        assistant["diagnostics"] = json!([{
            "type": "provider_stream_recovered",
            "timestamp": 2,
            "error": {"name": "ProviderError", "message": "intermediate provider failure"},
            "details": {"recovered": true}
        }]);
    });
    let transcript = replay(&encoded(&diagnosed), AgentValueKind::Response);
    assert!(matches!(transcript.outcome(), AgentOutcome::Completed(_)));
    assert!(transcript.observations().iter().any(|observation| matches!(
        observation,
        AgentObservation::Diagnostic { message, .. }
            if message.as_ref() == "intermediate provider failure"
    )));
}

#[test]
fn terminal_stop_reasons_use_the_closed_mode_table() {
    let no_value = replay(&simple_transcript("stop", json!([])), AgentValueKind::None);
    assert_eq!(
        no_value.outcome(),
        &AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );
    assert_failure(
        &replay(
            &simple_transcript("stop", json!([{"type": "text", "text": "ignored"}])),
            AgentValueKind::Result,
        ),
        AgentFailureCause::MissingResult,
    );

    for (reason, detail) in [
        ("length", AgentHarnessFailureDetail::ModelOutputTruncated),
        ("error", AgentHarnessFailureDetail::ModelError),
        ("aborted", AgentHarnessFailureDetail::ModelAborted),
    ] {
        for kind in [
            AgentValueKind::None,
            AgentValueKind::Response,
            AgentValueKind::Result,
        ] {
            assert_failure(
                &replay(
                    &simple_transcript(reason, json!([{"type": "text", "text": "partial"}])),
                    kind,
                ),
                AgentFailureCause::HarnessFailed { detail },
            );
        }
    }

    for kind in [AgentValueKind::None, AgentValueKind::Response] {
        assert_failure(
            &replay(TERMINAL_TOOL_USE, kind),
            AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnexpectedTerminalToolUse,
            },
        );
    }
    assert_failure(
        &replay(TERMINAL_TOOL_USE, AgentValueKind::Result),
        AgentFailureCause::MissingResult,
    );
}

#[test]
fn accepted_singleton_result_requires_matching_native_terminal_sequence() {
    let end_offset = event_offset(TERMINAL_TOOL_USE, "tool_execution_end");
    let mut accepted_parser = parser(AgentValueKind::Result);
    accepted_parser
        .push_ignoring(&TERMINAL_TOOL_USE[..end_offset])
        .unwrap();
    let result_value = Arc::new(json!({"answer": 42}));
    accepted_parser
        .accept_result(AcceptedPiJsonV1Result::new(
            Arc::from("call-result"),
            Arc::from("scherzo_result_fixed"),
            Arc::new(json!({"result": {"answer": 42}})),
            BoundedSchemaValidAgentResult::fixture(
                result_value.clone(),
                Arc::from(br#"{"answer":42}"#.as_slice()),
            ),
        ))
        .unwrap();
    accepted_parser
        .push_ignoring(&TERMINAL_TOOL_USE[end_offset..])
        .unwrap();
    let transcript = terminal(accepted_parser.finish(PiJsonV1ProcessCompletion::exited(true)));
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = transcript.outcome()
    else {
        panic!("accepted singleton result must complete");
    };
    assert_eq!(result.value(), result_value.as_ref());

    let mut contradictory = values(TERMINAL_TOOL_USE);
    visit_assistant_messages(&mut contradictory, |assistant| {
        assistant["stopReason"] = json!("stop");
    });
    let contradictory = encoded(&contradictory);
    let end_offset = event_offset(&contradictory, "tool_execution_end");
    let mut parser = parser(AgentValueKind::Result);
    parser.push_ignoring(&contradictory[..end_offset]).unwrap();
    parser
        .accept_result(AcceptedPiJsonV1Result::new(
            Arc::from("call-result"),
            Arc::from("scherzo_result_fixed"),
            Arc::new(json!({"result": {"answer": 42}})),
            BoundedSchemaValidAgentResult::fixture(
                result_value,
                Arc::from(br#"{"answer":42}"#.as_slice()),
            ),
        ))
        .unwrap();
    parser.push_ignoring(&contradictory[end_offset..]).unwrap();
    assert_failure(
        &terminal(parser.finish(PiJsonV1ProcessCompletion::exited(true))),
        AgentFailureCause::HarnessProtocolFailed,
    );
}

#[test]
fn response_presence_distinguishes_zero_blocks_from_present_empty_blocks() {
    assert_failure(
        &replay(
            &simple_transcript("stop", json!([])),
            AgentValueKind::Response,
        ),
        AgentFailureCause::MissingResponse,
    );
    for content in [
        json!([{"type": "text", "text": ""}]),
        json!([
            {"type": "thinking", "thinking": "not captured"},
            {"type": "text", "text": ""},
            {"type": "text", "text": ""}
        ]),
    ] {
        let transcript = replay(
            &simple_transcript("stop", content),
            AgentValueKind::Response,
        );
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            transcript.outcome()
        else {
            panic!("present empty text blocks must complete");
        };
        assert_eq!(response.as_str(), "");
    }
}

#[test]
fn response_limit_accepts_below_and_exact_then_fails_on_first_excess_update() {
    for bytes in [MAXIMUM_RESPONSE_BYTES - 1, MAXIMUM_RESPONSE_BYTES] {
        let text = "x".repeat(usize::try_from(bytes).unwrap());
        let transcript = replay(
            &simple_transcript("stop", json!([{"type": "text", "text": text}])),
            AgentValueKind::Response,
        );
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            transcript.outcome()
        else {
            panic!("admitted boundary must complete");
        };
        assert_eq!(u64::try_from(response.as_str().len()).unwrap(), bytes);
    }

    let over = "x".repeat(usize::try_from(MAXIMUM_RESPONSE_BYTES + 1).unwrap());
    let assistant = json!({
        "role": "assistant",
        "content": [],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 0,
            "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}},
        "stopReason": "stop",
        "timestamp": 2
    });
    let mut started = assistant.clone();
    started["content"] = json!([{"type": "text", "text": ""}]);
    let mut updated = assistant.clone();
    updated["content"] = json!([{"type": "text", "text": over}]);
    let prefix = encoded(&[
        json!({"type": "session", "version": 3, "id": "00000000-0000-4000-8000-000000000005", "timestamp": "2026-07-30T12:00:00Z", "cwd": CWD}),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": assistant}),
        json!({"type": "message_update", "message": started, "assistantMessageEvent": {"type": "text_start", "contentIndex": 0, "partial": started}}),
    ]);
    let excess = encoded(&[json!({
        "type": "message_update",
        "message": updated,
        "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": over, "partial": updated}
    })]);
    let mut parser = parser(AgentValueKind::Response);
    parser.push_ignoring(&prefix).unwrap();
    assert_eq!(
        parser.push_ignoring(&excess),
        Err(AgentFailureCause::CapturedValueTooLarge)
    );
}

#[test]
fn frame_reader_enforces_lf_utf8_object_and_sixteen_mib_before_decode() {
    let exact_overhead = br#"{"type":"future","padding":""}"#.len();
    let padding = "x".repeat(usize::try_from(MAXIMUM_FRAME_BYTES).unwrap() - exact_overhead);
    let exact = format!("{{\"type\":\"future\",\"padding\":\"{padding}\"}}\n");
    assert_eq!(u64::try_from(exact.len() - 1).unwrap(), MAXIMUM_FRAME_BYTES);
    let header = encoded(&[json!({
        "type": "session",
        "version": 3,
        "id": "00000000-0000-4000-8000-000000000006",
        "timestamp": "2026-07-30T12:00:00Z",
        "cwd": CWD
    })]);
    let mut exact_parser = parser(AgentValueKind::None);
    exact_parser.push_ignoring(&header).unwrap();
    exact_parser.push_ignoring(exact.as_bytes()).unwrap();

    let mut oversized = vec![b' '; usize::try_from(MAXIMUM_FRAME_BYTES + 1).unwrap()];
    oversized.push(b'\n');
    let mut before_start = parser(AgentValueKind::None);
    assert_eq!(
        before_start.push_ignoring(&oversized),
        Err(AgentFailureCause::HarnessStartFailed)
    );

    let mut after_start = parser(AgentValueKind::None);
    after_start.push_ignoring(&header).unwrap();
    after_start
        .push_ignoring(b"{\"type\":\"agent_start\"}\n")
        .unwrap();
    assert_eq!(
        after_start.push_ignoring(&oversized),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
    assert_eq!(
        after_start.push_ignoring(&[0xff, b'\n']),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
}

#[test]
fn malformed_reordered_duplicate_and_contradictory_lifecycles_are_typed() {
    for bytes in [
        b"not json\n".as_slice(),
        b"[]\n".as_slice(),
        b"{\"type\":\"session\",\"version\":2}\n".as_slice(),
    ] {
        let mut parser = parser(AgentValueKind::None);
        assert_eq!(
            parser.push_ignoring(bytes),
            Err(AgentFailureCause::HarnessStartFailed)
        );
    }

    let mut fixture = values(RESPONSE_SUCCESS);
    fixture.insert(2, json!({"type": "agent_start"}));
    assert_failure(
        &replay(&encoded(&fixture), AgentValueKind::Response),
        AgentFailureCause::HarnessProtocolFailed,
    );

    let mut fixture = values(RESPONSE_SUCCESS);
    fixture.retain(|event| event["type"] != "turn_end");
    assert_failure(
        &replay(&encoded(&fixture), AgentValueKind::Response),
        AgentFailureCause::HarnessProtocolFailed,
    );

    let mut fixture = values(RESPONSE_SUCCESS);
    let agent_end = fixture
        .iter_mut()
        .find(|event| event["type"] == "agent_end")
        .unwrap();
    agent_end["messages"][1]["model"] = json!("contradictory-model");
    assert_failure(
        &replay(&encoded(&fixture), AgentValueKind::Response),
        AgentFailureCause::HarnessProtocolFailed,
    );

    let mut fixture = values(RESPONSE_SUCCESS);
    let user_end = fixture
        .iter_mut()
        .find(|event| event["type"] == "message_end" && event["message"]["role"] == "user")
        .unwrap();
    user_end["message"]
        .as_object_mut()
        .unwrap()
        .remove("content");
    assert_failure(
        &replay(&encoded(&fixture), AgentValueKind::Response),
        AgentFailureCause::HarnessProtocolFailed,
    );

    let mut parser = parser(AgentValueKind::Response);
    let missing_lf = &RESPONSE_SUCCESS[..RESPONSE_SUCCESS.len() - 1];
    parser.push_ignoring(missing_lf).unwrap();
    assert_failure(
        &terminal(parser.finish(PiJsonV1ProcessCompletion::exited(true))),
        AgentFailureCause::HarnessProtocolFailed,
    );
}

#[test]
fn tool_identity_and_turn_result_correlations_are_required() {
    let mut fixture = values(TERMINAL_TOOL_USE);
    let execution_end = fixture
        .iter_mut()
        .find(|event| event["type"] == "tool_execution_end")
        .unwrap();
    execution_end["toolCallId"] = json!("wrong-call");
    assert_failure(
        &replay(&encoded(&fixture), AgentValueKind::Result),
        AgentFailureCause::HarnessProtocolFailed,
    );

    let mut fixture = values(TERMINAL_TOOL_USE);
    let turn_end = fixture
        .iter_mut()
        .find(|event| event["type"] == "turn_end")
        .unwrap();
    turn_end["toolResults"][0]["isError"] = json!(true);
    assert_failure(
        &replay(&encoded(&fixture), AgentValueKind::Result),
        AgentFailureCause::HarnessProtocolFailed,
    );
}

#[test]
fn unknown_additive_events_are_retained_but_never_gain_terminal_authority() {
    let mut fixture = values(RESPONSE_SUCCESS);
    let agent_end = fixture
        .iter()
        .position(|event| event["type"] == "agent_end")
        .unwrap();
    fixture.insert(
        agent_end,
        json!({"type": "future_pi_activity", "newField": {"value": 1}}),
    );
    let transcript = replay(&encoded(&fixture), AgentValueKind::Response);
    assert!(transcript.observations().iter().any(|observation| matches!(
        observation,
        AgentObservation::UnrecognizedHarnessEvent { event }
            if event["type"] == "future_pi_activity"
    )));
    assert!(matches!(transcript.outcome(), AgentOutcome::Completed(_)));

    fixture.retain(|event| event["type"] != "agent_end" && event["type"] != "agent_settled");
    assert_failure(
        &replay(&encoded(&fixture), AgentValueKind::Response),
        AgentFailureCause::HarnessProtocolFailed,
    );

    let mut trailing_known = values(RESPONSE_SUCCESS);
    trailing_known.push(json!({"type": "queue_update", "steering": [], "followUp": []}));
    assert_failure(
        &replay(&encoded(&trailing_known), AgentValueKind::Response),
        AgentFailureCause::HarnessProtocolFailed,
    );
}

#[test]
fn eof_exit_and_terminal_disagreement_invalidate_provisional_values() {
    assert_failure(
        &terminal({
            let mut parser = parser(AgentValueKind::Response);
            parser.push_ignoring(RESPONSE_SUCCESS).unwrap();
            parser.finish(PiJsonV1ProcessCompletion::exited(false))
        }),
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::UnsuccessfulExit,
        },
    );

    let cancelled = terminal({
        let bytes = simple_transcript("aborted", json!([]));
        let mut parser = parser(AgentValueKind::None);
        parser.push_ignoring(&bytes).unwrap();
        parser.finish(PiJsonV1ProcessCompletion::cancelled(
            false,
            CancellationReason::UserRequest,
        ))
    });
    assert_eq!(
        cancelled.outcome(),
        &AgentOutcome::Cancelled {
            reason: CancellationReason::UserRequest
        }
    );
}

#[test]
fn additive_fields_do_not_change_profile_relevant_correlation() {
    let mut fixture = values(RESPONSE_SUCCESS);
    fixture[0]["futureHeaderField"] = json!(true);
    let message_end = fixture
        .iter_mut()
        .find(|event| event["type"] == "message_end" && event["message"]["role"] == "assistant")
        .unwrap();
    message_end["futureEventField"] = json!([1, 2, 3]);
    message_end["message"]["futureMessageField"] = json!({"nested": true});
    assert!(matches!(
        replay(&encoded(&fixture), AgentValueKind::Response).outcome(),
        AgentOutcome::Completed(_)
    ));
}

fn event_offset(bytes: &[u8], event_type: &str) -> usize {
    let mut offset = 0;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let frame = line.strip_suffix(b"\n").unwrap_or(line);
        let event = serde_json::from_slice::<Value>(frame).unwrap();
        if event["type"] == event_type {
            return offset;
        }
        offset += line.len();
    }
    panic!("fixture must contain {event_type}");
}

fn visit_assistant_messages(values: &mut [Value], mut visit: impl FnMut(&mut Value)) {
    fn descend(value: &mut Value, visit: &mut impl FnMut(&mut Value)) {
        match value {
            Value::Object(object) => {
                if object.get("role").and_then(Value::as_str) == Some("assistant") {
                    visit(value);
                    return;
                }
                for child in object.values_mut() {
                    descend(child, visit);
                }
            }
            Value::Array(values) => {
                for child in values {
                    descend(child, visit);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }

    for value in values {
        descend(value, &mut visit);
    }
}

#[test]
fn retained_correlation_state_is_bounded_by_the_protocol_frame_limit() {
    let limits = PiJsonV1ProtocolLimits {
        maximum_frame_bytes: NonZeroU64::new(512).unwrap(),
    };
    let mut parser = PiJsonV1Parser::new(
        Arc::from(CWD),
        AgentValueKind::None,
        NonZeroU64::new(1024).unwrap(),
        limits,
    );
    let mut events = vec![
        json!({"type": "session", "version": 3, "id": "00000000-0000-4000-8000-000000000007", "timestamp": "2026-07-30T12:00:00Z", "cwd": CWD}),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
    ];
    for timestamp in 1..20 {
        let message = json!({"role": "user", "content": "bounded message", "timestamp": timestamp});
        events.push(json!({"type": "message_start", "message": message}));
        events.push(json!({"type": "message_end", "message": message}));
    }
    assert_eq!(
        parser.push_ignoring(&encoded(&events)),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
}

#[test]
fn parser_accepts_an_explicit_smaller_admitted_response_limit() {
    let mut parser = PiJsonV1Parser::new(
        Arc::from(CWD),
        AgentValueKind::Response,
        NonZeroU64::new(4).unwrap(),
        PiJsonV1ProtocolLimits::profile(),
    );
    assert_eq!(
        parser.push_ignoring(&simple_transcript(
            "stop",
            json!([{"type": "text", "text": "12345"}]),
        )),
        Err(AgentFailureCause::CapturedValueTooLarge)
    );
}

#[test]
fn terminal_validation_requires_settlement_and_closed_retry_state() {
    let mut missing_settled = values(RESPONSE_SUCCESS);
    missing_settled.retain(|event| event["type"] != "agent_settled");

    let mut missing_retry_end = values(NATIVE_RECOVERY);
    missing_retry_end.retain(|event| event["type"] != "auto_retry_end");

    let actual = [
        replay(&encoded(&missing_settled), AgentValueKind::Response).outcome,
        replay(&encoded(&missing_retry_end), AgentValueKind::Response).outcome,
    ];
    let expected = [
        AgentOutcome::Failed {
            cause: AgentFailureCause::HarnessProtocolFailed,
        },
        AgentOutcome::Failed {
            cause: AgentFailureCause::HarnessProtocolFailed,
        },
    ];
    assert_eq!(actual, expected);
}

#[test]
fn queued_continuation_after_agent_end_selects_the_ultimate_response() {
    let usage = json!({
        "input": 1,
        "output": 1,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 2,
        "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}
    });
    let initial = json!({
        "role": "assistant",
        "content": [{"type": "text", "text": "initial"}],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": usage,
        "stopReason": "stop",
        "timestamp": 2
    });
    let queued = json!({
        "role": "custom",
        "customType": "continue",
        "content": "extension follow-up",
        "display": false,
        "timestamp": 3
    });
    let ultimate = json!({
        "role": "assistant",
        "content": [{"type": "text", "text": "ultimate"}],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": usage,
        "stopReason": "stop",
        "timestamp": 4
    });
    let transcript = encoded(&[
        json!({"type": "session", "version": 3, "id": "00000000-0000-4000-8000-000000000009", "timestamp": "2026-07-30T12:00:00Z", "cwd": CWD}),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": initial}),
        json!({"type": "message_end", "message": initial}),
        json!({"type": "turn_end", "message": initial, "toolResults": []}),
        json!({"type": "agent_end", "messages": [initial], "willRetry": false}),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": queued}),
        json!({"type": "message_end", "message": queued}),
        json!({"type": "message_start", "message": ultimate}),
        json!({"type": "message_end", "message": ultimate}),
        json!({"type": "turn_end", "message": ultimate, "toolResults": []}),
        json!({"type": "agent_end", "messages": [queued, ultimate], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ]);

    assert_eq!(
        replay(&transcript, AgentValueKind::Response).outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::Response(
            BoundedAgentResponse::from_bounded(Arc::from("ultimate")),
        ))
    );
}

#[test]
fn empty_added_tool_names_use_pi_tool_result_normalization() {
    let mut events = values(&tool_error_then_success_transcript());
    let execution_end = events
        .iter_mut()
        .find(|event| event["type"] == "tool_execution_end")
        .unwrap();
    execution_end["result"]["addedToolNames"] = json!([]);

    assert_eq!(
        replay(&encoded(&events), AgentValueKind::Response).outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::Response(
            BoundedAgentResponse::from_bounded(Arc::from("recovered")),
        ))
    );
}

#[test]
fn queued_continuation_requires_a_queued_message() {
    let mut events = values(&simple_transcript(
        "stop",
        json!([{"type": "text", "text": "initial"}]),
    ));
    events.pop();

    let ultimate = json!({
        "role": "assistant",
        "content": [{"type": "text", "text": "ultimate"}],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": {
            "input": 1,
            "output": 1,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": 2,
            "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}
        },
        "stopReason": "stop",
        "timestamp": 3
    });
    events.extend([
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": ultimate}),
        json!({"type": "message_end", "message": ultimate}),
        json!({"type": "turn_end", "message": ultimate, "toolResults": []}),
        json!({"type": "agent_end", "messages": [ultimate], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ]);

    assert_eq!(
        replay(&encoded(&events), AgentValueKind::Response).outcome,
        AgentOutcome::Failed {
            cause: AgentFailureCause::HarnessProtocolFailed,
        }
    );
}
