use std::ffi::OsString;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

use serde_json::{Value, json};

use super::*;

const SESSION_ID: &str = "00000000-0000-4000-8000-000000000001";
const CWD: &str = "/synthetic/project";
const MODEL: &str = "scherzo-loopback";
const RESPONSE_FIXTURE: &[u8] = include_bytes!("fixtures/response-duplicates-and-subagent.jsonl");
const CONTRADICTORY_INIT: &[u8] = include_bytes!("fixtures/contradictory-init.jsonl");
const MISSING_INIT: &[u8] = include_bytes!("fixtures/missing-init.jsonl");
const MALFORMED: &[u8] = include_bytes!("fixtures/malformed.jsonl");

fn init(version: &str, session_id: &str) -> Value {
    json!({
        "type": "system",
        "subtype": "init",
        "cwd": CWD,
        "session_id": session_id,
        "model": MODEL,
        "permissionMode": "bypassPermissions",
        "claude_code_version": version,
    })
}

fn status(session_id: &str) -> Value {
    json!({
        "type": "system",
        "subtype": "status",
        "status": "requesting",
        "session_id": session_id,
    })
}

fn result(session_id: &str) -> Value {
    json!({
        "type": "result",
        "subtype": "success",
        "is_error": false,
        "terminal_reason": "completed",
        "result": "observational convenience text",
        "session_id": session_id,
    })
}

fn framed(values: &[Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in values {
        serde_json::to_writer(&mut bytes, value).unwrap();
        bytes.push(b'\n');
    }
    bytes
}

fn parser(kind: AgentValueKind, maximum_response_bytes: u64) -> ClaudeCodeStreamJsonV1Parser {
    ClaudeCodeStreamJsonV1Parser::profile(
        Arc::from(CWD),
        Arc::from(MODEL),
        kind,
        NonZeroU64::new(maximum_response_bytes).unwrap(),
    )
}

fn replay(
    bytes: &[u8],
    kind: AgentValueKind,
    maximum_response_bytes: u64,
) -> (Vec<AgentObservation>, AgentOutcome) {
    let mut parser = parser(kind, maximum_response_bytes);
    let mut observations = Vec::new();
    let _ = parser.push_stdout(bytes, |observation| observations.push(observation));
    (observations, parser.finish(true))
}

fn stream_event(event: Value) -> Value {
    json!({
        "type": "stream_event",
        "event": event,
        "session_id": SESSION_ID,
        "parent_tool_use_id": null,
    })
}

fn message_start(id: &str) -> Value {
    stream_event(json!({
        "type": "message_start",
        "message": {
            "id": id,
            "type": "message",
            "role": "assistant",
            "content": [],
            "model": MODEL,
            "usage": {"input_tokens": 1, "output_tokens": 0},
        },
    }))
}

fn text_message(id: &str, deltas: &[&str], present: bool) -> Vec<Value> {
    let mut values = vec![message_start(id)];
    if present {
        values.push(stream_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""},
        })));
        values.extend(deltas.iter().map(|text| {
            stream_event(json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": text},
            }))
        }));
        values.push(stream_event(json!({
            "type": "content_block_stop",
            "index": 0,
        })));
    }
    values.extend([
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 1},
        })),
        stream_event(json!({"type": "message_stop"})),
    ]);
    values
}

fn completed_text_transcript(deltas: &[&str], present: bool) -> Vec<u8> {
    let mut values = vec![init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID)];
    values.extend(text_message("msg-final", deltas, present));
    values.push(result(SESSION_ID));
    framed(&values)
}

fn assert_failed(outcome: AgentOutcome, cause: AgentFailureCause) {
    assert_eq!(outcome, AgentOutcome::Failed { cause });
}

#[test]
fn normal_mode_arguments_and_input_frame_are_exact() {
    let arguments = normal_mode_arguments(
        "claude-profile-model",
        "xhigh",
        Path::new("/synthetic/private/system-prompt"),
    );
    assert_eq!(
        arguments,
        [
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--forward-subagent-text",
            "--no-session-persistence",
            "--permission-mode",
            "bypassPermissions",
            "--setting-sources",
            "user,project,local",
            "--model",
            "claude-profile-model",
            "--effort",
            "xhigh",
            "--append-system-prompt-file",
            "/synthetic/private/system-prompt",
        ]
        .map(OsString::from)
    );

    let frame = initial_user_text_frame("exact\nuser text").unwrap();
    assert_eq!(frame.last(), Some(&b'\n'));
    assert_eq!(frame.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert_eq!(
        serde_json::from_slice::<Value>(&frame[..frame.len() - 1]).unwrap(),
        json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": "exact\nuser text"}],
            },
        })
    );
}

#[test]
fn parser_requires_matching_initialization_and_serialized_exchange_results() {
    let mut parser = parser(AgentValueKind::None, 1024);
    let first = framed(&[
        init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
        status(SESSION_ID),
        result(SESSION_ID),
    ]);
    for byte in first {
        parser.push_stdout(&[byte], drop).unwrap();
    }

    parser.begin_exchange().unwrap();
    parser
        .push_stdout(
            &framed(&[
                init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
                result(SESSION_ID),
            ]),
            drop,
        )
        .unwrap();
    assert_eq!(parser.session_id(), Some(SESSION_ID));
    assert_eq!(parser.completed_exchanges(), 2);
    assert_eq!(
        parser.finish(true),
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );
}

#[test]
fn recorded_response_uses_only_final_main_thread_deltas_once() {
    let (observations, outcome) = replay(RESPONSE_FIXTURE, AgentValueKind::Response, 1024);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("recorded response must complete");
    };
    assert_eq!(response.as_str(), "final answer");
    let observed_text = observations
        .iter()
        .filter_map(|observation| match observation {
            AgentObservation::AssistantText { text } => Some(text.as_ref()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(observed_text, "earliersubagentfinal answer");
}

#[test]
fn contradictory_nominal_assistant_text_rejects_the_response() {
    let mut values = vec![init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID)];
    values.extend([
        message_start("msg-contradictory"),
        stream_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""},
        })),
        stream_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "stream answer"},
        })),
        json!({
            "type": "assistant",
            "message": {
                "id": "msg-contradictory",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text": "different nominal answer"}],
                "model": MODEL,
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
        }),
        stream_event(json!({"type": "content_block_stop", "index": 0})),
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 1},
        })),
        stream_event(json!({"type": "message_stop"})),
        result(SESSION_ID),
    ]);

    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);
}

#[test]
fn stream_continuations_from_another_thread_reject_the_response() {
    let values = [
        init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
        message_start("msg-main"),
        json!({
            "type": "stream_event",
            "event": {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": "subagent"},
            },
            "session_id": SESSION_ID,
            "parent_tool_use_id": "tool-spawn-subagent",
        }),
        json!({
            "type": "stream_event",
            "event": {"type": "content_block_stop", "index": 0},
            "session_id": SESSION_ID,
            "parent_tool_use_id": "tool-spawn-subagent",
        }),
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 1},
        })),
        stream_event(json!({"type": "message_stop"})),
        result(SESSION_ID),
    ];

    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);
}

#[test]
fn malformed_and_contradictory_recorded_streams_never_accept_a_response() {
    for (bytes, expected) in [
        (MALFORMED, AgentFailureCause::HarnessStartFailed),
        (MISSING_INIT, AgentFailureCause::HarnessStartFailed),
        (CONTRADICTORY_INIT, AgentFailureCause::HarnessProtocolFailed),
    ] {
        let (_, outcome) = replay(bytes, AgentValueKind::Response, 1024);
        assert_failed(outcome, expected);
    }

    let (_, invalid_utf8) = replay(&[0xff, b'\n'], AgentValueKind::Response, 1024);
    assert_failed(invalid_utf8, AgentFailureCause::HarnessStartFailed);

    let (_, non_object) = replay(b"[]\n", AgentValueKind::Response, 1024);
    assert_failed(non_object, AgentFailureCause::HarnessStartFailed);
}

#[test]
fn initialization_identity_and_exchange_boundaries_are_unambiguous() {
    let contradictory = [
        ("claude_code_version", json!("2.1.223")),
        ("cwd", json!("/other")),
        ("model", json!("other-model")),
        ("permissionMode", json!("default")),
        ("session_id", json!("00000000-0000-0000-0000-000000000000")),
    ];
    for (field, replacement) in contradictory {
        let mut initialization = init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID);
        initialization[field] = replacement;
        let (_, outcome) = replay(&framed(&[initialization]), AgentValueKind::Response, 1024);
        assert_failed(outcome, AgentFailureCause::HarnessStartFailed);
    }

    let mut duplicate_result = parser(AgentValueKind::Response, 1024);
    duplicate_result
        .push_stdout(
            &framed(&[
                init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
                result(SESSION_ID),
            ]),
            drop,
        )
        .unwrap();
    assert_eq!(
        duplicate_result
            .push_stdout(&framed(&[result(SESSION_ID)]), drop)
            .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );
    assert_failed(
        duplicate_result.finish(true),
        AgentFailureCause::HarnessProtocolFailed,
    );

    let mut missing_exchange_init = parser(AgentValueKind::Response, 1024);
    missing_exchange_init
        .push_stdout(
            &framed(&[
                init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
                result(SESSION_ID),
            ]),
            drop,
        )
        .unwrap();
    missing_exchange_init.begin_exchange().unwrap();
    assert_eq!(
        missing_exchange_init
            .push_stdout(&framed(&[status(SESSION_ID)]), drop)
            .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );
    assert_failed(
        missing_exchange_init.finish(true),
        AgentFailureCause::HarnessProtocolFailed,
    );
}

#[test]
fn truncation_and_frame_overflow_fail_in_the_current_protocol_phase() {
    let mut transcript = framed(&[
        init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
        result(SESSION_ID),
    ]);
    transcript.pop();
    let (_, outcome) = replay(&transcript, AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);

    let limits =
        ClaudeCodeStreamJsonV1ProtocolLimits::with_maximum_frame_bytes(NonZeroU64::new(8).unwrap());
    let mut oversized = ClaudeCodeStreamJsonV1Parser::new(
        Arc::from(CWD),
        Arc::from(MODEL),
        AgentValueKind::Response,
        NonZeroU64::new(1024).unwrap(),
        limits,
    );
    assert_eq!(
        oversized.push_stdout(b"123456789", drop).unwrap_err(),
        AgentFailureCause::HarnessStartFailed
    );
    assert_failed(
        oversized.finish(true),
        AgentFailureCause::HarnessStartFailed,
    );
}

#[test]
fn response_distinguishes_missing_text_from_a_present_empty_block() {
    let (_, missing) = replay(
        &completed_text_transcript(&[], false),
        AgentValueKind::Response,
        1024,
    );
    assert_failed(missing, AgentFailureCause::MissingResponse);

    let (_, empty) = replay(
        &completed_text_transcript(&[], true),
        AgentValueKind::Response,
        1024,
    );
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(empty)) = empty else {
        panic!("present empty block must complete");
    };
    assert_eq!(empty.as_str(), "");
}

#[test]
fn response_utf8_limit_succeeds_exactly_and_fails_before_first_excess_byte() {
    let exact = completed_text_transcript(&["A", "é", "🔥"], true);
    let (_, exact_outcome) = replay(&exact, AgentValueKind::Response, 7);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = exact_outcome
    else {
        panic!("exact-limit response must complete");
    };
    assert_eq!(response.as_str(), "Aé🔥");

    let over = completed_text_transcript(&["A", "é", "🔥", "B"], true);
    let (_, over_outcome) = replay(&over, AgentValueKind::Response, 7);
    assert_failed(over_outcome, AgentFailureCause::CapturedValueTooLarge);
}

#[test]
fn reasoning_tools_results_metadata_and_unknown_events_are_normalized() {
    let mut values = vec![init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID)];
    values.extend([
        message_start("msg-tools"),
        stream_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "plan"},
        })),
        stream_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": " more"},
        })),
        stream_event(json!({"type": "content_block_stop", "index": 0})),
        stream_event(json!({
            "type": "content_block_start",
            "index": 1,
            "content_block": {"type": "tool_use", "id": "tool-1", "name": "Read", "input": {}},
        })),
        stream_event(json!({
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": "{}"},
        })),
        stream_event(json!({"type": "content_block_stop", "index": 1})),
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 2},
        })),
        stream_event(json!({"type": "message_stop"})),
        json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "tool-1",
                    "is_error": false,
                    "content": [{"type": "text", "text": "contents"}],
                }],
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
        }),
        json!({"type": "future_additive_event", "session_id": SESSION_ID}),
    ]);
    values.extend(text_message("msg-final", &["done"], true));
    values.push(result(SESSION_ID));
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("normalized activity transcript must complete");
    };
    assert_eq!(response.as_str(), "done");
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Reasoning { text } if text.as_ref() == "plan"
    )));
    for phase in [
        AgentToolCallPhase::Started,
        AgentToolCallPhase::Updated,
        AgentToolCallPhase::Completed,
    ] {
        assert!(observations.iter().any(|observation| matches!(
            observation,
            AgentObservation::ToolCall { call_id, name, phase: observed }
                if call_id.as_ref() == "tool-1" && name.as_ref() == "Read" && *observed == phase
        )));
    }
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::ToolResult { call_id, is_error: false, content }
            if call_id.as_ref() == "tool-1" && content.as_ref() == "contents"
    )));
    assert!(observations
        .iter()
        .any(|observation| matches!(observation, AgentObservation::Model { name } if name.as_ref() == MODEL)));
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Usage {
            input_tokens: 1,
            output_tokens: 2
        }
    )));
    assert_eq!(
        observations
            .iter()
            .filter(|observation| matches!(
                observation,
                AgentObservation::UnrecognizedHarnessEvent { .. }
            ))
            .count(),
        1
    );
}

#[test]
fn unsuccessful_native_result_and_exit_use_existing_typed_failures() {
    let native_error = framed(&[
        init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
        json!({
            "type": "result",
            "subtype": "error_during_execution",
            "is_error": true,
            "terminal_reason": "error_during_execution",
            "session_id": SESSION_ID,
        }),
    ]);
    let (_, outcome) = replay(&native_error, AgentValueKind::None, 1024);
    assert_failed(
        outcome,
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::ModelError,
        },
    );

    let mut parser = parser(AgentValueKind::None, 1024);
    parser
        .push_stdout(
            &framed(&[
                init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
                result(SESSION_ID),
            ]),
            drop,
        )
        .unwrap();
    assert_failed(
        parser.finish(false),
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::UnsuccessfulExit,
        },
    );
}
