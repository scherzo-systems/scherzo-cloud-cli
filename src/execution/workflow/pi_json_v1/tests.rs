use std::num::NonZeroU64;
use std::sync::Arc;

use serde_json::{Value, json};

use super::*;
use crate::execution::workflow::admission::MAXIMUM_AGENT_RESPONSE_BYTES;
use crate::execution::workflow::agent::{
    AgentHarnessFailureDetail, AgentOutcome, BoundedSchemaValidAgentResult,
};

const CWD: &str = "/execution/worktree";
const FIRST_REASONING_SUMMARY: &str = "Inspecting the changed parser state.";
const SECOND_REASONING_SUMMARY: &str = "Checking the next transition.";
const RESPONSE_SUCCESS: &[u8] = include_bytes!("fixtures/response-success.jsonl");
const NATIVE_RECOVERY: &[u8] = include_bytes!("fixtures/native-recovery.jsonl");
const PARALLEL_WORK_BEFORE_RESULT: &[u8] =
    include_bytes!("fixtures/parallel-work-before-result.jsonl");
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

fn replay_accepted_result(bytes: &[u8], call_id: &str) -> RecordedReplay {
    let end_offset = event_offset_for_call(bytes, "tool_execution_end", call_id);
    let arguments = Arc::new(json!({"result": {"answer": 42}}));
    let mut parser = result_parser();
    let mut observations = Vec::new();
    parser
        .push_stdout(&bytes[..end_offset], |observation| {
            observations.push(observation);
        })
        .unwrap();
    parser
        .correlate_result_request("scherzo_result_fixed", call_id, arguments.as_ref())
        .unwrap();
    parser
        .accept_result(AcceptedPiJsonV1Result::new(
            Arc::from(call_id),
            Arc::from("scherzo_result_fixed"),
            arguments,
            BoundedSchemaValidAgentResult::fixture(
                Arc::new(json!({"answer": 42})),
                Arc::from(br#"{"answer":42}"#.as_slice()),
            ),
        ))
        .unwrap();
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

fn multiple_reasoning_summaries_transcript(finalized_thinking: &str) -> Vec<u8> {
    let usage = json!({
        "input": 1,
        "output": 1,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 2,
        "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}
    });
    let message = |thinking: Option<&str>, text: Option<&str>, stop_reason: &str| {
        let mut content = Vec::new();
        if let Some(thinking) = thinking {
            content.push(json!({"type": "thinking", "thinking": thinking}));
        }
        if let Some(text) = text {
            content.push(json!({"type": "text", "text": text}));
        }
        json!({
            "role": "assistant",
            "content": content,
            "api": "test-api",
            "provider": "test-provider",
            "model": "test-model",
            "usage": usage,
            "stopReason": stop_reason,
            "timestamp": 2
        })
    };
    let empty = message(None, None, "pending");
    let thinking_started = message(Some(""), None, "pending");
    let first = message(Some(FIRST_REASONING_SUMMARY), None, "pending");
    let first_separator = message(
        Some(&format!("{FIRST_REASONING_SUMMARY}\n\n")),
        None,
        "pending",
    );
    let both = message(
        Some(&format!(
            "{FIRST_REASONING_SUMMARY}\n\n{SECOND_REASONING_SUMMARY}"
        )),
        None,
        "pending",
    );
    let streamed = message(
        Some(&format!(
            "{FIRST_REASONING_SUMMARY}\n\n{SECOND_REASONING_SUMMARY}\n\n"
        )),
        None,
        "pending",
    );
    let finalized = message(Some(finalized_thinking), None, "pending");
    let text_started = message(Some(finalized_thinking), Some(""), "pending");
    let text = message(Some(finalized_thinking), Some("complete"), "pending");
    let completed = message(Some(finalized_thinking), Some("complete"), "stop");

    encoded(&[
        json!({"type": "session", "version": 3, "id": "00000000-0000-4000-8000-00000000000b", "timestamp": "2026-07-30T12:00:00Z", "cwd": CWD}),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": empty}),
        json!({"type": "message_update", "message": thinking_started, "assistantMessageEvent": {"type": "thinking_start", "contentIndex": 0, "partial": thinking_started}}),
        json!({"type": "message_update", "message": first, "assistantMessageEvent": {"type": "thinking_delta", "contentIndex": 0, "delta": FIRST_REASONING_SUMMARY, "partial": first}}),
        json!({"type": "message_update", "message": first_separator, "assistantMessageEvent": {"type": "thinking_delta", "contentIndex": 0, "delta": "\n\n", "partial": first_separator}}),
        json!({"type": "message_update", "message": both, "assistantMessageEvent": {"type": "thinking_delta", "contentIndex": 0, "delta": SECOND_REASONING_SUMMARY, "partial": both}}),
        json!({"type": "message_update", "message": streamed, "assistantMessageEvent": {"type": "thinking_delta", "contentIndex": 0, "delta": "\n\n", "partial": streamed}}),
        json!({"type": "message_update", "message": finalized, "assistantMessageEvent": {"type": "thinking_end", "contentIndex": 0, "content": finalized_thinking, "partial": finalized}}),
        json!({"type": "message_update", "message": text_started, "assistantMessageEvent": {"type": "text_start", "contentIndex": 1, "partial": text_started}}),
        json!({"type": "message_update", "message": text, "assistantMessageEvent": {"type": "text_delta", "contentIndex": 1, "delta": "complete", "partial": text}}),
        json!({"type": "message_update", "message": text, "assistantMessageEvent": {"type": "text_end", "contentIndex": 1, "content": "complete", "partial": text}}),
        json!({"type": "message_end", "message": completed}),
        json!({"type": "turn_end", "message": completed, "toolResults": []}),
        json!({"type": "agent_end", "messages": [completed], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ])
}

fn terminal(outcome: AgentOutcome) -> RecordedReplay {
    RecordedReplay {
        observations: Vec::new(),
        outcome,
    }
}

fn unclosed_streamed_tool_call(reason: &str) -> Vec<u8> {
    let usage = json!({
        "input": 1,
        "output": 1,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 2,
        "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}
    });
    let started = json!({
        "role": "assistant",
        "content": [{"type": "toolCall", "id": "call-open", "name": "inspect", "arguments": {}}],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": usage,
        "stopReason": "pending",
        "timestamp": 2
    });
    let mut updated = started.clone();
    updated["content"][0]["arguments"] = json!({"path": "partial"});
    let mut completed = updated.clone();
    completed["stopReason"] = json!(reason);
    encoded(&[
        json!({"type": "session", "version": 3, "id": "00000000-0000-4000-8000-000000000009", "timestamp": "2026-07-30T12:00:00Z", "cwd": CWD}),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({
            "type": "message_start",
            "message": {
                "role": "assistant",
                "content": [],
                "api": "test-api",
                "provider": "test-provider",
                "model": "test-model",
                "usage": usage,
                "stopReason": "pending",
                "timestamp": 2
            }
        }),
        json!({
            "type": "message_update",
            "assistantMessageEvent": {
                "type": "toolcall_start",
                "contentIndex": 0,
                "partial": started
            },
            "message": started
        }),
        json!({
            "type": "message_update",
            "assistantMessageEvent": {
                "type": "toolcall_delta",
                "contentIndex": 0,
                "delta": "{\"path\":\"partial\"}",
                "partial": updated
            },
            "message": updated
        }),
        json!({"type": "message_end", "message": completed}),
    ])
}

fn partial_tool_call_native_result_recovery_transcript() -> Vec<u8> {
    let usage = json!({
        "input": 1,
        "output": 1,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 2,
        "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}
    });
    let user = json!({"role": "user", "content": "prompt", "timestamp": 1});
    let empty = json!({
        "role": "assistant",
        "content": [],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": usage,
        "stopReason": "pending",
        "timestamp": 2
    });
    let mut thinking_started = empty.clone();
    thinking_started["content"] = json!([{"type": "thinking", "thinking": ""}]);
    let mut thinking_streamed = thinking_started.clone();
    thinking_streamed["content"][0]["thinking"] = json!("Preparing a result call.");
    let mut tool_started = thinking_streamed.clone();
    tool_started["content"].as_array_mut().unwrap().push(json!({
        "type": "toolCall",
        "id": "call-interrupted-result",
        "name": "scherzo_result_fixed",
        "arguments": {}
    }));
    let mut tool_streamed = tool_started.clone();
    tool_streamed["content"][1]["arguments"] = json!({"result": {"answer": 0}});
    let mut interrupted = tool_streamed.clone();
    interrupted["stopReason"] = json!("error");
    interrupted["errorMessage"] = json!("WebSocket closed 1006 Connection ended");
    interrupted["diagnostics"] = json!([{
        "type": "provider_transport_failure",
        "timestamp": 2,
        "error": {
            "name": "Error",
            "message": "WebSocket closed 1006 Connection ended"
        },
        "details": {
            "eventsEmitted": true,
            "phase": "after_message_stream_start"
        }
    }]);
    let recovered = json!({
        "role": "assistant",
        "content": [{
            "type": "toolCall",
            "id": "call-recovered-result",
            "name": "scherzo_result_fixed",
            "arguments": {"result": {"answer": 42}}
        }],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": usage,
        "stopReason": "toolUse",
        "timestamp": 3
    });
    let result = json!({
        "role": "toolResult",
        "toolCallId": "call-recovered-result",
        "toolName": "scherzo_result_fixed",
        "content": [{"type": "text", "text": "accepted"}],
        "details": {},
        "isError": false,
        "timestamp": 4
    });

    encoded(&[
        json!({"type": "session", "version": 3, "id": "00000000-0000-4000-8000-00000000000a", "timestamp": "2026-07-30T12:00:00Z", "cwd": CWD}),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": user}),
        json!({"type": "message_end", "message": user}),
        json!({"type": "message_start", "message": empty}),
        json!({"type": "message_update", "message": thinking_started, "assistantMessageEvent": {"type": "thinking_start", "contentIndex": 0, "partial": thinking_started}}),
        json!({"type": "message_update", "message": thinking_streamed, "assistantMessageEvent": {"type": "thinking_delta", "contentIndex": 0, "delta": "Preparing a result call.", "partial": thinking_streamed}}),
        json!({"type": "message_update", "message": thinking_streamed, "assistantMessageEvent": {"type": "thinking_end", "contentIndex": 0, "content": "Preparing a result call.", "partial": thinking_streamed}}),
        json!({"type": "message_update", "message": tool_started, "assistantMessageEvent": {"type": "toolcall_start", "contentIndex": 1, "partial": tool_started}}),
        json!({"type": "message_update", "message": tool_streamed, "assistantMessageEvent": {"type": "toolcall_delta", "contentIndex": 1, "delta": "{\"result\":{\"answer\":0}}", "partial": tool_streamed}}),
        json!({"type": "message_end", "message": interrupted}),
        json!({"type": "turn_end", "message": interrupted, "toolResults": []}),
        json!({"type": "agent_end", "messages": [user, interrupted], "willRetry": true}),
        json!({"type": "auto_retry_start", "attempt": 1, "maxAttempts": 3, "delayMs": 0, "errorMessage": "WebSocket closed 1006 Connection ended"}),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": recovered}),
        json!({"type": "message_end", "message": recovered}),
        json!({"type": "auto_retry_end", "success": true, "attempt": 1}),
        json!({"type": "tool_execution_start", "toolCallId": "call-recovered-result", "toolName": "scherzo_result_fixed", "args": {"result": {"answer": 42}}}),
        json!({"type": "tool_execution_end", "toolCallId": "call-recovered-result", "toolName": "scherzo_result_fixed", "result": {"content": [{"type": "text", "text": "accepted"}], "details": {}, "terminate": true}, "isError": false}),
        json!({"type": "message_start", "message": result}),
        json!({"type": "message_end", "message": result}),
        json!({"type": "turn_end", "message": recovered, "toolResults": [result]}),
        json!({"type": "agent_end", "messages": [recovered, result], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ])
}

fn reused_result_identity_then_correction_transcript() -> Vec<u8> {
    let usage = json!({
        "input": 1,
        "output": 1,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 2,
        "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}
    });
    let earlier = json!({
        "role": "assistant",
        "content": [{"type": "toolCall", "id": "call-blocked-result", "name": "inspect", "arguments": {"path": "old.txt"}}],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": usage,
        "stopReason": "toolUse",
        "timestamp": 1
    });
    let earlier_result = json!({
        "role": "toolResult",
        "toolCallId": "call-blocked-result",
        "toolName": "inspect",
        "content": [{"type": "text", "text": "old contents"}],
        "details": {},
        "isError": false,
        "timestamp": 2
    });
    let mut events = values(SIBLING_RESULT_CORRECTION);
    events.retain(|event| {
        event.get("toolCallId").and_then(Value::as_str) != Some("call-sibling-read")
            && event
                .get("message")
                .and_then(|message| message.get("toolCallId"))
                .and_then(Value::as_str)
                != Some("call-sibling-read")
    });
    visit_assistant_messages(&mut events, |assistant| {
        if assistant["timestamp"] == 2 {
            assistant["content"]
                .as_array_mut()
                .unwrap()
                .retain(|block| block["id"] != "call-sibling-read");
        }
    });
    for event in &mut events {
        if event["type"] == "turn_end" {
            event["toolResults"]
                .as_array_mut()
                .unwrap()
                .retain(|result| result["toolCallId"] != "call-sibling-read");
        } else if event["type"] == "agent_end" {
            event["messages"]
                .as_array_mut()
                .unwrap()
                .retain(|message| message["toolCallId"] != "call-sibling-read");
        }
    }
    events.splice(
        2..2,
        [
            json!({"type": "turn_start"}),
            json!({"type": "message_start", "message": earlier}),
            json!({"type": "message_end", "message": earlier}),
            json!({"type": "tool_execution_start", "toolCallId": "call-blocked-result", "toolName": "inspect", "args": {"path": "old.txt"}}),
            json!({"type": "tool_execution_end", "toolCallId": "call-blocked-result", "toolName": "inspect", "result": {"content": [{"type": "text", "text": "old contents"}], "details": {}}, "isError": false}),
            json!({"type": "message_start", "message": earlier_result}),
            json!({"type": "message_end", "message": earlier_result}),
            json!({"type": "turn_end", "message": earlier, "toolResults": [earlier_result]}),
        ],
    );
    let messages = events
        .iter_mut()
        .find(|event| event["type"] == "agent_end")
        .unwrap()["messages"]
        .as_array_mut()
        .unwrap();
    messages.splice(0..0, [earlier, earlier_result]);
    String::from_utf8(encoded(&events))
        .unwrap()
        .replace(
            "No result was accepted. Call the workflow result tool by itself, without sibling tool calls.",
            "No result was accepted. The workflow result call could not be correlated.",
        )
        .into_bytes()
}

fn parallel_work_then_stop_transcript() -> Vec<u8> {
    let mut events = values(PARALLEL_WORK_BEFORE_RESULT);
    let second_turn = events
        .iter()
        .enumerate()
        .filter(|(_, event)| event["type"] == "turn_start")
        .nth(1)
        .map(|(index, _)| index)
        .unwrap();
    let mut completed_messages = events[..second_turn]
        .iter()
        .filter(|event| event["type"] == "message_end")
        .map(|event| event["message"].clone())
        .collect::<Vec<_>>();
    events.truncate(second_turn);
    let assistant = json!({
        "role": "assistant",
        "content": [{"type": "text", "text": "parallel work completed"}],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": {
            "input": 2,
            "output": 1,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": 3,
            "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}
        },
        "stopReason": "stop",
        "timestamp": 5
    });
    completed_messages.push(assistant.clone());
    events.extend([
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": assistant}),
        json!({"type": "message_end", "message": assistant}),
        json!({"type": "turn_end", "message": assistant, "toolResults": []}),
        json!({"type": "agent_end", "messages": completed_messages, "willRetry": false}),
        json!({"type": "agent_settled"}),
    ]);
    encoded(&events)
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
    assert_outcome_failure(transcript.outcome(), cause);
}

fn assert_outcome_failure(outcome: &AgentOutcome, cause: AgentFailureCause) {
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("expected agent failure, got {outcome:?}");
    };
    assert_eq!(failure.cause(), &cause);
}

fn protocol_rejection_value(outcome: &AgentOutcome) -> Value {
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("expected agent failure, got {outcome:?}");
    };
    serde_json::to_value(
        failure
            .protocol_rejection()
            .expect("parser-owned failure must carry a rejection diagnostic"),
    )
    .unwrap()
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
fn parser_rejections_report_stable_frame_startup_and_reasoning_transition_context() {
    let mut malformed_frame = parser(AgentValueKind::None);
    assert_eq!(
        malformed_frame.push_ignoring(b"not-json\n"),
        Err(AgentFailureCause::HarnessStartFailed)
    );
    let malformed_frame = malformed_frame.finish(PiJsonV1ProcessCompletion::exited(false));
    assert_eq!(
        protocol_rejection_value(&malformed_frame),
        json!({
            "schemaVersion": 1,
            "profile": "PiJsonV1",
            "detail": {
                "reason": "frame_decode_failed",
                "stage": "frame_decode",
                "state": {
                    "sessionHeaderSeen": false,
                    "agentStarted": false,
                    "activeAttempt": false,
                    "activeTurn": false,
                    "activeMessage": "none",
                    "assistantUpdateSeen": false,
                    "resultAccepted": false,
                    "settled": false
                }
            }
        })
    );

    let events = values(RESPONSE_SUCCESS);
    let rejected_index = events
        .iter()
        .position(|event| {
            event["type"] == "message_update"
                && event["assistantMessageEvent"]["type"] == "text_delta"
        })
        .unwrap();
    let prefix = encoded(&events[..rejected_index]);
    let mut original = events[rejected_index].clone();
    for (field, sentinel) in [
        ("prompt", "SENTINEL_PROMPT_CONTENT"),
        ("reasoning", "SENTINEL_REASONING_CONTENT"),
        ("toolArguments", "SENTINEL_TOOL_ARGUMENTS"),
        ("signature", "SENTINEL_PROVIDER_SIGNATURE"),
        ("nativeSessionId", "SENTINEL_NATIVE_SESSION_ID"),
    ] {
        original[field] = json!(sentinel);
    }
    let cases = [
        (
            "assistant_update_invalid",
            "assistant_update",
            "text_delta",
            0,
            {
                let mut event = original.clone();
                event["assistantMessageEvent"]
                    .as_object_mut()
                    .unwrap()
                    .remove("delta");
                event
            },
        ),
        (
            "assistant_update_content_index_mismatch",
            "assistant_transition",
            "text_delta",
            1,
            {
                let mut event = original.clone();
                event["assistantMessageEvent"]["contentIndex"] = json!(1);
                event
            },
        ),
        (
            "assistant_update_open_block_mismatch",
            "assistant_transition",
            "thinking_delta",
            0,
            {
                let mut event = original;
                event["assistantMessageEvent"]["type"] = json!("thinking_delta");
                event
            },
        ),
    ];
    for (reason, stage, update, content_index, rejected) in cases {
        let mut parser = parser(AgentValueKind::Response);
        parser.push_ignoring(&prefix).unwrap();
        assert_eq!(
            parser.push_ignoring(&encoded(&[rejected])),
            Err(AgentFailureCause::HarnessProtocolFailed)
        );
        let outcome = parser.finish(PiJsonV1ProcessCompletion::exited(false));
        let diagnostic = protocol_rejection_value(&outcome);
        assert_eq!(diagnostic["schemaVersion"], 1);
        assert_eq!(diagnostic["profile"], "PiJsonV1");
        assert_eq!(diagnostic["detail"]["reason"], reason);
        assert_eq!(diagnostic["detail"]["stage"], stage);
        assert_eq!(diagnostic["detail"]["outerEvent"], "message_update");
        assert_eq!(diagnostic["detail"]["assistantUpdate"], update);
        assert_eq!(diagnostic["detail"]["contentIndex"], content_index);
        assert_eq!(
            diagnostic["detail"]["state"],
            json!({
                "sessionHeaderSeen": true,
                "agentStarted": true,
                "activeAttempt": true,
                "activeTurn": true,
                "activeMessage": "assistant",
                "activeAssistantContentBlocks": 1,
                "assistantUpdateSeen": true,
                "openBlock": {"kind": "text", "contentIndex": 0},
                "resultAccepted": false,
                "settled": false
            })
        );
        let serialized = diagnostic.to_string();
        for sentinel in [
            "SENTINEL_PROMPT_CONTENT",
            "SENTINEL_REASONING_CONTENT",
            "SENTINEL_TOOL_ARGUMENTS",
            "SENTINEL_PROVIDER_SIGNATURE",
            "SENTINEL_NATIVE_SESSION_ID",
        ] {
            assert!(!serialized.contains(sentinel));
        }
        assert!(serialized.len() < 16 * 1024);
    }

    let limits = PiJsonV1ProtocolLimits {
        maximum_frame_bytes: NonZeroU64::new(512).unwrap(),
    };
    let mut oversized = PiJsonV1Parser::new(
        Arc::from(CWD),
        AgentValueKind::None,
        NonZeroU64::new(1024).unwrap(),
        limits,
        None,
    );
    oversized
        .push_ignoring(&encoded(&[
            json!({"type": "session", "version": 3, "id": "00000000-0000-4000-8000-00000000000e", "timestamp": "2026-07-30T12:00:00Z", "cwd": CWD}),
            json!({"type": "agent_start"}),
        ]))
        .unwrap();
    let oversized_frame = encoded(&[json!({
        "type": "message_update",
        "reasoning": "SENTINEL_OVERSIZED_FRAME",
        "padding": "x".repeat(1024)
    })]);
    assert_eq!(
        oversized.push_ignoring(&oversized_frame),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
    let diagnostic =
        protocol_rejection_value(&oversized.finish(PiJsonV1ProcessCompletion::exited(false)));
    let serialized = diagnostic.to_string();
    assert_eq!(diagnostic["detail"]["reason"], "frame_too_large");
    assert!(!serialized.contains("SENTINEL_OVERSIZED_FRAME"));
    assert!(serialized.len() < 16 * 1024);
}

#[test]
fn rejected_turn_end_preserves_pre_transition_state_in_diagnostic() {
    let events = values(RESPONSE_SUCCESS);
    let rejected_boundary = events
        .iter()
        .position(|event| {
            event["type"] == "message_update"
                && event["assistantMessageEvent"]["type"] == "text_delta"
        })
        .unwrap()
        + 1;
    let premature_turn_end = events
        .iter()
        .find(|event| event["type"] == "turn_end")
        .unwrap()
        .clone();
    let mut rejected = events[..rejected_boundary].to_vec();
    rejected.push(premature_turn_end);

    let outcome = replay(&encoded(&rejected), AgentValueKind::Response).outcome;
    let diagnostic = protocol_rejection_value(&outcome);

    assert_eq!(diagnostic["detail"]["outerEvent"], "turn_end");
    assert_eq!(diagnostic["detail"]["state"]["activeTurn"], true);
    assert_eq!(diagnostic["detail"]["state"]["activeMessage"], "assistant");
    assert_eq!(
        diagnostic["detail"]["state"]["openBlock"],
        json!({"kind": "text", "contentIndex": 0})
    );
}

#[test]
fn provider_finalized_thinking_reaches_the_next_action_and_terminal_snapshot() {
    const FINALIZED_THINKING: &str = "Provider-finalized reasoning snapshot.";
    let conforming = replay(
        &multiple_reasoning_summaries_transcript(FINALIZED_THINKING),
        AgentValueKind::Response,
    );
    assert_eq!(
        conforming.outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::Response(
            BoundedAgentResponse::from_bounded(Arc::from("complete")),
        ))
    );
    let reasoning = conforming
        .observations
        .iter()
        .filter_map(|observation| match observation {
            AgentObservation::Reasoning { text } => Some(text.as_ref()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        reasoning,
        [
            FIRST_REASONING_SUMMARY,
            "\n\n",
            SECOND_REASONING_SUMMARY,
            "\n\n"
        ]
    );
    assert!(!reasoning.contains(&FINALIZED_THINKING));
}

#[test]
fn compact_thinking_end_cannot_erase_event_snapshot_disagreement() {
    const FINALIZED_THINKING: &str = "Provider-finalized reasoning snapshot.";
    let mut events = values(&multiple_reasoning_summaries_transcript(FINALIZED_THINKING));
    let thinking_end = events
        .iter_mut()
        .find(|event| {
            event["type"] == "message_update"
                && event["assistantMessageEvent"]["type"] == "thinking_end"
        })
        .unwrap();
    thinking_end["assistantMessageEvent"]["content"] =
        json!("A provider event snapshot that disagrees with its partial.");

    // Reproduce the superseded compaction shape to keep the parser fail closed
    // if a compact thinking-end frame reaches it from any source.
    let assistant_event = thinking_end["assistantMessageEvent"]
        .as_object_mut()
        .unwrap();
    assistant_event.insert("scherzoCompact".to_owned(), json!(true));
    assistant_event.remove("partial");
    assistant_event.remove("content");

    let rejected = replay(&encoded(&events), AgentValueKind::Response);
    assert_outcome_failure(&rejected.outcome, AgentFailureCause::HarnessProtocolFailed);
    let diagnostic = protocol_rejection_value(&rejected.outcome);
    assert_eq!(diagnostic["detail"]["reason"], "assistant_update_invalid");
    assert_eq!(diagnostic["detail"]["stage"], "assistant_update");
    assert_eq!(diagnostic["detail"]["assistantUpdate"], "thinking_end");
}

#[test]
fn thinking_end_rejects_structural_and_snapshot_disagreement() {
    const FINALIZED_THINKING: &str = "Provider-finalized reasoning snapshot.";
    let baseline = values(&multiple_reasoning_summaries_transcript(FINALIZED_THINKING));
    let thinking_end_index = baseline
        .iter()
        .position(|event| {
            event["type"] == "message_update"
                && event["assistantMessageEvent"]["type"] == "thinking_end"
        })
        .unwrap();
    let assert_rejected = |events: Vec<Value>, expected_reason: &str, expected_index: u64| {
        let rejected = replay(&encoded(&events), AgentValueKind::Response);
        assert_outcome_failure(&rejected.outcome, AgentFailureCause::HarnessProtocolFailed);
        let diagnostic = protocol_rejection_value(&rejected.outcome);
        assert_eq!(diagnostic["detail"]["reason"], expected_reason);
        assert_eq!(diagnostic["detail"]["stage"], "assistant_transition");
        assert_eq!(diagnostic["detail"]["outerEvent"], "message_update");
        assert_eq!(diagnostic["detail"]["assistantUpdate"], "thinking_end");
        assert_eq!(diagnostic["detail"]["contentIndex"], expected_index);
        assert_eq!(diagnostic["detail"]["state"]["activeMessage"], "assistant");
        let serialized = diagnostic.to_string();
        assert!(!serialized.contains(FIRST_REASONING_SUMMARY));
        assert!(!serialized.contains(FINALIZED_THINKING));
        assert!(serialized.len() < 16 * 1024);
    };

    let mut no_open_block = baseline.clone();
    no_open_block.retain(|event| {
        event["type"] != "message_update"
            || !matches!(
                event["assistantMessageEvent"]["type"].as_str(),
                Some("thinking_start" | "thinking_delta")
            )
    });
    assert_rejected(no_open_block, "assistant_update_open_block_mismatch", 0);

    let mut wrong_index = baseline.clone();
    wrong_index[thinking_end_index]["assistantMessageEvent"]["contentIndex"] = json!(1);
    assert_rejected(wrong_index, "assistant_update_content_index_mismatch", 1);

    let mut wrong_kind = baseline.clone();
    let text_block = json!({"type": "text", "text": FINALIZED_THINKING});
    wrong_kind[thinking_end_index]["message"]["content"][0] = text_block.clone();
    wrong_kind[thinking_end_index]["assistantMessageEvent"]["partial"]["content"][0] = text_block;
    assert_rejected(wrong_kind, "thinking_finalization_rewrite", 0);

    let mut event_partial_disagreement = baseline.clone();
    event_partial_disagreement[thinking_end_index]["assistantMessageEvent"]["content"] =
        json!("A different event snapshot.");
    assert_rejected(
        event_partial_disagreement,
        "thinking_finalization_rewrite",
        0,
    );

    let mut stable_fields_changed = baseline.clone();
    stable_fields_changed[thinking_end_index]["message"]["model"] = json!("changed-model");
    stable_fields_changed[thinking_end_index]["assistantMessageEvent"]["partial"]["model"] =
        json!("changed-model");
    assert_rejected(stable_fields_changed, "assistant_stable_fields_changed", 0);

    let mut sibling_added = baseline;
    let sibling = json!({"type": "text", "text": "changed sibling"});
    sibling_added[thinking_end_index]["message"]["content"]
        .as_array_mut()
        .unwrap()
        .push(sibling.clone());
    sibling_added[thinking_end_index]["assistantMessageEvent"]["partial"]["content"]
        .as_array_mut()
        .unwrap()
        .push(sibling);
    assert_rejected(sibling_added, "thinking_finalization_rewrite", 0);
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
fn partial_tool_call_error_recovers_without_execution_or_result_correlation() {
    let bytes = partial_tool_call_native_result_recovery_transcript();
    let events = values(&bytes);
    let first_turn_end = events
        .iter()
        .position(|event| event["type"] == "turn_end")
        .unwrap();
    let mut interrupted = result_parser();
    interrupted
        .push_ignoring(&encoded(&events[..first_turn_end]))
        .unwrap();
    assert_eq!(
        interrupted.try_correlate_result_request(
            "scherzo_result_fixed",
            "call-interrupted-result",
            &json!({"result": {"answer": 0}}),
        ),
        Ok(false),
        "the interrupted result call must not become correlatable"
    );

    let transcript = replay_accepted_result(&bytes, "call-recovered-result");
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = transcript.outcome()
    else {
        panic!("native retry must complete from the recovered result call");
    };
    assert_eq!(result.value(), &json!({"answer": 42}));
    assert!(transcript.observations().iter().any(|observation| matches!(
        observation,
        AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Error,
            message,
        } if message.as_ref() == "WebSocket closed 1006 Connection ended"
    )));
    let retry_milestones = transcript
        .observations()
        .iter()
        .filter_map(|observation| match observation {
            AgentObservation::Lifecycle { milestone }
                if matches!(
                    milestone,
                    AgentLifecycleMilestone::RetryStarted | AgentLifecycleMilestone::RetryCompleted
                ) =>
            {
                Some(*milestone)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        retry_milestones,
        [
            AgentLifecycleMilestone::RetryStarted,
            AgentLifecycleMilestone::RetryCompleted
        ]
    );
    assert!(
        !transcript.observations().iter().any(|observation| matches!(
            observation,
            AgentObservation::ToolCall {
                call_id,
                phase: AgentToolCallPhase::Completed,
                ..
            } if call_id.as_ref() == "call-interrupted-result"
        ))
    );
    assert!(
        !transcript.observations().iter().any(|observation| matches!(
            observation,
            AgentObservation::ToolResult { call_id, .. }
                if call_id.as_ref() == "call-interrupted-result"
        ))
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
fn pending_is_partial_only_and_recorded_streams_reach_terminal_reasons() {
    for (fixture, terminal_reason) in [(RESPONSE_SUCCESS, "stop"), (TERMINAL_TOOL_USE, "toolUse")] {
        let events = values(fixture);
        let partial_reasons = events
            .iter()
            .filter(|event| {
                matches!(
                    event["type"].as_str(),
                    Some("message_start" | "message_update")
                )
            })
            .filter_map(|event| event["message"]["stopReason"].as_str())
            .collect::<Vec<_>>();
        assert!(!partial_reasons.is_empty());
        assert!(partial_reasons.iter().all(|reason| *reason == "pending"));
        assert_eq!(
            events
                .iter()
                .find(|event| {
                    event["type"] == "message_end" && event["message"]["role"] == "assistant"
                })
                .unwrap()["message"]["stopReason"],
            terminal_reason
        );
    }

    assert!(matches!(
        replay(RESPONSE_SUCCESS, AgentValueKind::Response).outcome(),
        AgentOutcome::Completed(CompletedAgentInvocation::Response(_))
    ));
    assert_failure(
        &replay(TERMINAL_TOOL_USE, AgentValueKind::Result),
        AgentFailureCause::MissingResult,
    );
    assert_failure(
        &replay(
            &simple_transcript("pending", json!([])),
            AgentValueKind::None,
        ),
        AgentFailureCause::HarnessProtocolFailed,
    );
}

#[test]
fn non_retry_terminal_reasons_cannot_close_a_streamed_tool_call() {
    for reason in ["stop", "toolUse", "aborted"] {
        let mut parser = parser(AgentValueKind::None);
        assert_eq!(
            parser.push_ignoring(&unclosed_streamed_tool_call(reason)),
            Err(AgentFailureCause::HarnessProtocolFailed),
            "unexpected implicit close for {reason}"
        );
    }

    let bytes = partial_tool_call_native_result_recovery_transcript();
    let mut events = values(&bytes);
    let first_agent_end = events
        .iter()
        .position(|event| event["type"] == "agent_end")
        .unwrap();
    events[first_agent_end]["willRetry"] = json!(false);
    events.truncate(first_agent_end + 1);
    let mut parser = result_parser();
    assert_eq!(
        parser.push_ignoring(&encoded(&events)),
        Err(AgentFailureCause::HarnessProtocolFailed),
        "an interrupted tool call requires Pi's promised native retry"
    );
}

#[test]
fn partial_tool_call_requires_native_retry_start_before_continuation() {
    let mut events = values(&partial_tool_call_native_result_recovery_transcript());
    events.retain(|event| {
        !matches!(
            event["type"].as_str(),
            Some("auto_retry_start" | "auto_retry_end")
        )
    });
    let first_agent_end = events
        .iter()
        .position(|event| event["type"] == "agent_end")
        .unwrap();
    events.splice(
        first_agent_end + 1..first_agent_end + 1,
        [
            json!({"type": "compaction_start", "reason": "overflow"}),
            json!({"type": "compaction_end", "reason": "overflow", "result": {}, "aborted": false, "willRetry": true}),
        ],
    );

    let mut parser = result_parser();
    assert_eq!(
        parser.push_ignoring(&encoded(&events)),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
}

#[test]
fn interrupted_tool_call_cannot_emit_native_execution_events() {
    let mut events = values(&partial_tool_call_native_result_recovery_transcript());
    let first_turn_end = events
        .iter()
        .position(|event| event["type"] == "turn_end")
        .unwrap();
    events.insert(
        first_turn_end,
        json!({
            "type": "tool_execution_start",
            "toolCallId": "call-interrupted-result",
            "toolName": "scherzo_result_fixed",
            "args": {"result": {"answer": 0}}
        }),
    );
    let mut parser = result_parser();
    assert_eq!(
        parser.push_ignoring(&encoded(&events)),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
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
fn ordinary_parallel_work_remains_valid_in_every_output_mode() {
    let stop_transcript = parallel_work_then_stop_transcript();
    assert_eq!(
        replay(&stop_transcript, AgentValueKind::None).outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );
    assert_eq!(
        replay(&stop_transcript, AgentValueKind::Response).outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::Response(
            BoundedAgentResponse::from_bounded(Arc::from("parallel work completed")),
        ))
    );
    assert_failure(
        &replay(&stop_transcript, AgentValueKind::Result),
        AgentFailureCause::MissingResult,
    );

    let first = replay_accepted_result(PARALLEL_WORK_BEFORE_RESULT, "call-final-result");
    let second = replay_accepted_result(PARALLEL_WORK_BEFORE_RESULT, "call-final-result");
    assert_eq!(first, second);
    assert!(matches!(
        first.outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::Result(_))
    ));
    assert_eq!(
        first
            .observations
            .iter()
            .filter(|observation| matches!(
                observation,
                AgentObservation::ToolCall {
                    phase: AgentToolCallPhase::Started,
                    ..
                }
            ))
            .count(),
        3
    );
}

#[test]
fn sibling_result_rejection_and_singleton_correction_are_repeatable() {
    let first = replay_accepted_result(SIBLING_RESULT_CORRECTION, "call-corrected-result");
    let second = replay_accepted_result(SIBLING_RESULT_CORRECTION, "call-corrected-result");
    assert_eq!(first, second);
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = &first.outcome else {
        panic!("the corrected singleton result must complete");
    };
    assert_eq!(result.value(), &json!({"answer": 42}));
    assert!(first.observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::ToolResult {
            call_id,
            is_error: true,
            ..
        } if call_id.as_ref() == "call-blocked-result"
    )));
    assert!(first.observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::ToolResult {
            call_id,
            is_error: false,
            ..
        } if call_id.as_ref() == "call-sibling-read"
    )));
}

#[test]
fn reused_result_identity_cannot_be_recovered_into_a_committed_result() {
    let transcript = reused_result_identity_then_correction_transcript();
    let ambiguous_call = event_offset_for_call_occurrence(
        &transcript,
        "tool_execution_start",
        "call-blocked-result",
        1,
    );
    let mut parser = result_parser();
    assert_eq!(
        parser.push_ignoring(&transcript[..ambiguous_call]),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
    assert_failure(
        &terminal(parser.finish(PiJsonV1ProcessCompletion::exited(true))),
        AgentFailureCause::HarnessProtocolFailed,
    );
}

#[test]
fn result_identity_ambiguity_and_post_acceptance_work_are_protocol_failures() {
    let singleton_end = event_offset(TERMINAL_TOOL_USE, "tool_execution_end");
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
            .push_ignoring(&TERMINAL_TOOL_USE[..singleton_end])
            .unwrap();
        assert_eq!(
            parser.correlate_result_request(name, call_id, &arguments),
            Err(AgentFailureCause::HarnessProtocolFailed)
        );
    }

    let sibling_end = event_offset_for_call(
        SIBLING_RESULT_CORRECTION,
        "tool_execution_end",
        "call-blocked-result",
    );
    let mut ambiguous = result_parser();
    ambiguous
        .push_ignoring(&SIBLING_RESULT_CORRECTION[..sibling_end])
        .unwrap();
    assert_eq!(
        ambiguous.correlate_result_request(
            "scherzo_result_fixed",
            "call-blocked-result",
            &json!({"result": {"answer": 0}}),
        ),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );

    let mut accepted = result_parser();
    accepted
        .push_ignoring(&TERMINAL_TOOL_USE[..singleton_end])
        .unwrap();
    let arguments = Arc::new(json!({"result": {"answer": 42}}));
    accepted
        .correlate_result_request("scherzo_result_fixed", "call-result", arguments.as_ref())
        .unwrap();
    accepted
        .accept_result(AcceptedPiJsonV1Result::new(
            Arc::from("call-result"),
            Arc::from("scherzo_result_fixed"),
            arguments,
            BoundedSchemaValidAgentResult::fixture(
                Arc::new(json!({"answer": 42})),
                Arc::from(br#"{"answer":42}"#.as_slice()),
            ),
        ))
        .unwrap();
    assert_eq!(
        accepted.push_ignoring(b"{\"type\":\"turn_start\"}\n"),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );

    let mut unvalidated_success = result_parser();
    assert_eq!(
        unvalidated_success.push_ignoring(TERMINAL_TOOL_USE),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
}

#[test]
fn accepted_result_allows_exact_cancelled_threshold_compaction_before_settlement() {
    let tool_end = event_offset(TERMINAL_TOOL_USE, "tool_execution_end");
    let settled = event_offset(TERMINAL_TOOL_USE, "agent_settled");
    let parser_after_agent_end = || {
        let mut parser = result_parser();
        parser
            .push_ignoring(&TERMINAL_TOOL_USE[..tool_end])
            .unwrap();
        let arguments = Arc::new(json!({"result": {"answer": 42}}));
        parser
            .correlate_result_request("scherzo_result_fixed", "call-result", arguments.as_ref())
            .unwrap();
        parser
            .accept_result(AcceptedPiJsonV1Result::new(
                Arc::from("call-result"),
                Arc::from("scherzo_result_fixed"),
                arguments,
                BoundedSchemaValidAgentResult::fixture(
                    Arc::new(json!({"answer": 42})),
                    Arc::from(br#"{"answer":42}"#.as_slice()),
                ),
            ))
            .unwrap();
        parser
            .push_ignoring(&TERMINAL_TOOL_USE[tool_end..settled])
            .unwrap();
        parser
    };

    let mut cancelled = parser_after_agent_end();
    cancelled
        .push_ignoring(&encoded(&[
            json!({"type": "compaction_start", "reason": "threshold"}),
            json!({"type": "compaction_end", "reason": "threshold", "aborted": true, "willRetry": false}),
        ]))
        .unwrap();
    cancelled
        .push_ignoring(&TERMINAL_TOOL_USE[settled..])
        .unwrap();
    assert!(matches!(
        cancelled.finish(PiJsonV1ProcessCompletion::exited(true)),
        AgentOutcome::Completed(CompletedAgentInvocation::Result(_))
    ));

    let mut overflow = parser_after_agent_end();
    assert_eq!(
        overflow.push_ignoring(b"{\"type\":\"compaction_start\",\"reason\":\"overflow\"}\n"),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );

    let mut completed = parser_after_agent_end();
    completed
        .push_ignoring(b"{\"type\":\"compaction_start\",\"reason\":\"threshold\"}\n")
        .unwrap();
    assert_eq!(
        completed.push_ignoring(
            b"{\"type\":\"compaction_end\",\"reason\":\"threshold\",\"result\":{},\"aborted\":false,\"willRetry\":false}\n"
        ),
        Err(AgentFailureCause::HarnessProtocolFailed)
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
        .correlate_result_request(
            "scherzo_result_fixed",
            "call-result",
            &json!({"result": {"answer": 42}}),
        )
        .unwrap();
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
        .correlate_result_request(
            "scherzo_result_fixed",
            "call-result",
            &json!({"result": {"answer": 42}}),
        )
        .unwrap();
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
fn rejected_validation_cannot_be_followed_by_native_tool_success() {
    let end_offset = event_offset(TERMINAL_TOOL_USE, "tool_execution_end");
    let mut parser = parser(AgentValueKind::Result);
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

    // A native tool-result handler must not contradict the authoritative rejection.
    let contradictory_end = encoded(&[json!({
        "type": "tool_execution_end",
        "toolCallId": "call-result",
        "toolName": "scherzo_result_fixed",
        "result": {
            "content": [{"type": "text", "text": "rewritten as success"}],
            "details": {}
        },
        "isError": false
    })]);
    assert_eq!(
        parser.push_ignoring(&contradictory_end),
        Err(AgentFailureCause::HarnessProtocolFailed),
    );
}

#[test]
fn terminal_result_settlement_uses_semantic_numeric_equality() {
    let mut events = values(TERMINAL_TOOL_USE);
    let transcript_arguments = json!({"result": {"count": 1.0}});
    visit_assistant_messages(&mut events, |assistant| {
        assistant["content"][0]["arguments"] = transcript_arguments.clone();
    });
    events
        .iter_mut()
        .find(|event| event["type"] == "tool_execution_start")
        .unwrap()["args"] = transcript_arguments;
    let transcript = encoded(&events);
    let end_offset = event_offset(&transcript, "tool_execution_end");
    let mut parser = parser(AgentValueKind::Result);
    parser.push_ignoring(&transcript[..end_offset]).unwrap();

    let socket_arguments = Arc::new(json!({"result": {"count": 1}}));
    parser
        .correlate_result_request(
            "scherzo_result_fixed",
            "call-result",
            socket_arguments.as_ref(),
        )
        .unwrap();
    parser
        .accept_result(AcceptedPiJsonV1Result::new(
            Arc::from("call-result"),
            Arc::from("scherzo_result_fixed"),
            socket_arguments,
            BoundedSchemaValidAgentResult::fixture(
                Arc::new(json!({"count": 1})),
                Arc::from(br#"{"count":1}"#.as_slice()),
            ),
        ))
        .unwrap();
    parser.push_ignoring(&transcript[end_offset..]).unwrap();

    assert!(matches!(
        parser.finish(PiJsonV1ProcessCompletion::exited(true)),
        AgentOutcome::Completed(CompletedAgentInvocation::Result(_))
    ));
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
fn admitted_response_limit_accepts_exact_and_rejects_one_excess_byte() {
    let parser = PiJsonV1Parser::new(
        Arc::from(CWD),
        AgentValueKind::Response,
        NonZeroU64::new(MAXIMUM_AGENT_RESPONSE_BYTES).unwrap(),
        PiJsonV1ProtocolLimits::profile(),
        None,
    );
    let response = |bytes: u64| {
        let value = json!({
            "role": "assistant",
            "content": [{
                "type": "text",
                "text": "x".repeat(usize::try_from(bytes).unwrap())
            }],
            "api": "test-api",
            "provider": "test-provider",
            "model": "test-model",
            "usage": {
                "input": 0,
                "output": 0,
                "cacheRead": 0,
                "cacheWrite": 0,
                "totalTokens": 0,
                "cost": {
                    "input": 0,
                    "output": 0,
                    "cacheRead": 0,
                    "cacheWrite": 0,
                    "total": 0
                }
            },
            "stopReason": "stop",
            "timestamp": 2
        });
        let ParsedMessage::Assistant(message) = parse_message(&value, true).unwrap() else {
            panic!("the response fixture must parse as an assistant message");
        };
        message
    };

    assert_eq!(
        parser.check_response_bound(&response(MAXIMUM_AGENT_RESPONSE_BYTES)),
        Ok(())
    );
    assert_eq!(
        parser.check_response_bound(&response(MAXIMUM_AGENT_RESPONSE_BYTES + 1)),
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

    for exit_success in [false, true] {
        let cancelled = terminal({
            let mut parser = parser(AgentValueKind::Response);
            parser.push_ignoring(RESPONSE_SUCCESS).unwrap();
            parser.finish(PiJsonV1ProcessCompletion::cancelled(
                exit_success,
                CancellationReason::UserRequest,
            ))
        });
        assert_eq!(
            cancelled.outcome(),
            &AgentOutcome::Cancelled {
                reason: CancellationReason::UserRequest
            },
            "cancellation must discard the provisional response for every exit status"
        );
    }

    let cancelled_after_protocol_failure = terminal({
        let mut parser = parser(AgentValueKind::None);
        assert!(parser.push_ignoring(b"not-json\n").is_err());
        parser.finish(PiJsonV1ProcessCompletion::cancelled(
            false,
            CancellationReason::UserRequest,
        ))
    });
    assert_eq!(
        cancelled_after_protocol_failure.outcome(),
        &AgentOutcome::Cancelled {
            reason: CancellationReason::UserRequest
        },
        "late native protocol failures cannot replace accepted cancellation"
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

fn event_offset_for_call(bytes: &[u8], event_type: &str, call_id: &str) -> usize {
    event_offset_for_call_occurrence(bytes, event_type, call_id, 0)
}

fn event_offset_for_call_occurrence(
    bytes: &[u8],
    event_type: &str,
    call_id: &str,
    expected_occurrence: usize,
) -> usize {
    let mut offset = 0;
    let mut occurrence = 0;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let frame = line.strip_suffix(b"\n").unwrap_or(line);
        let event = serde_json::from_slice::<Value>(frame).unwrap();
        if event["type"] == event_type && event["toolCallId"] == call_id {
            if occurrence == expected_occurrence {
                return offset;
            }
            occurrence += 1;
        }
        offset += line.len();
    }
    panic!("fixture must contain occurrence {expected_occurrence} of {event_type} for {call_id}");
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
fn argument_correlation_uses_exact_json_semantics_including_numeric_equivalence() {
    assert!(semantically_equal_json(
        &json!({"result": {"count": 1, "items": [true, null]}}),
        &json!({"result": {"items": [true, null], "count": 1.0}}),
    ));
    assert!(!semantically_equal_json(
        &json!({"result": 9_007_199_254_740_993_u64}),
        &json!({"result": 9_007_199_254_740_992.0_f64}),
    ));
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
        None,
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
        None,
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
    for outcome in &actual {
        assert_outcome_failure(outcome, AgentFailureCause::HarnessProtocolFailed);
    }
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

    assert_outcome_failure(
        &replay(&encoded(&events), AgentValueKind::Response).outcome,
        AgentFailureCause::HarnessProtocolFailed,
    );
}
