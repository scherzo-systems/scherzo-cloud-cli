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
        Arc::from(SESSION_ID),
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
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("expected agent failure");
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
fn normal_mode_arguments_and_input_frame_are_exact() {
    let arguments = normal_mode_arguments(
        "claude-profile-model",
        "xhigh",
        SESSION_ID,
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
            "--session-id",
            SESSION_ID,
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
fn parser_rejections_report_stable_profile_owned_structural_context() {
    let mut malformed = parser(AgentValueKind::None, 1024);
    assert_eq!(
        malformed.push_stdout(b"not-json\n", drop),
        Err(AgentFailureCause::HarnessStartFailed)
    );
    let malformed = malformed.finish(false);
    assert_eq!(
        protocol_rejection_value(&malformed),
        json!({
            "schemaVersion": 1,
            "profile": "ClaudeCodeStreamJsonV1",
            "detail": {
                "reason": "frame_decode_failed",
                "stage": "frame_decode",
                "state": {
                    "initialized": false,
                    "exchangeInitialized": false,
                    "exchangeActive": true,
                    "completedExchanges": 0,
                    "activeMessage": "none",
                    "finalMainMessage": false,
                    "exchangeStructuredOutputCandidates": 0,
                    "completedResultExchange": "none",
                    "resultDecisionPending": false,
                    "resultAccepted": false,
                    "nativeFailure": false,
                    "retryActive": false
                }
            }
        })
    );

    let mut initialization = init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID);
    initialization["cwd"] = json!("/contradictory");
    let (_, invalid_initialization) =
        replay(&framed(&[initialization]), AgentValueKind::None, 1024);
    let initialization = protocol_rejection_value(&invalid_initialization);
    assert_eq!(initialization["detail"]["reason"], "initialization_invalid");
    assert_eq!(initialization["detail"]["stage"], "initialization");
    assert_eq!(initialization["detail"]["outerEvent"], "system");

    let mut missing_exchange_init = parser(AgentValueKind::None, 1024);
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
    assert!(
        missing_exchange_init
            .push_stdout(&framed(&[status(SESSION_ID)]), drop)
            .is_err()
    );
    let missing_exchange_init = protocol_rejection_value(&missing_exchange_init.finish(false));
    assert_eq!(
        missing_exchange_init["detail"]["reason"],
        "exchange_initialization_missing"
    );
    assert_eq!(
        missing_exchange_init["detail"]["stage"],
        "exchange_lifecycle"
    );

    let wrong_parent = json!({
        "type": "stream_event",
        "event": {
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": "SENTINEL_ASSISTANT_TEXT"},
        },
        "session_id": SESSION_ID,
        "parent_tool_use_id": "SENTINEL_PARENT_TOOL_ID",
    });
    let (_, wrong_parent) = replay(
        &framed(&[
            init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
            message_start("msg-main"),
            wrong_parent,
        ]),
        AgentValueKind::Response,
        1024,
    );
    let wrong_parent = protocol_rejection_value(&wrong_parent);
    assert_eq!(
        wrong_parent["detail"]["reason"],
        "active_stream_parent_mismatch"
    );
    assert_eq!(wrong_parent["detail"]["outerEvent"], "stream_event");
    assert_eq!(wrong_parent["detail"]["streamEvent"], "content_block_start");
    assert_eq!(wrong_parent["detail"]["contentIndex"], 0);
    assert_eq!(wrong_parent["detail"]["contentBlock"], "text");
    assert_eq!(wrong_parent["detail"]["state"]["activeMessage"], "main");

    let (_, message_transition) = replay(
        &framed(&[
            init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
            message_start("msg-incomplete"),
            stream_event(json!({"type": "message_stop"})),
        ]),
        AgentValueKind::Response,
        1024,
    );
    let message_transition = protocol_rejection_value(&message_transition);
    assert_eq!(
        message_transition["detail"]["reason"],
        "message_transition_invalid"
    );
    assert_eq!(message_transition["detail"]["streamEvent"], "message_stop");
    assert_eq!(
        message_transition["detail"]["state"]["activeMessage"],
        "main"
    );

    let mut rejected_delta = stream_event(json!({
        "type": "content_block_delta",
        "index": 1,
        "delta": {
            "type": "text_delta",
            "text": "SENTINEL_ASSISTANT_TEXT",
            "reasoning": "SENTINEL_REASONING",
            "tool_input": {"secret": "SENTINEL_TOOL_INPUT"},
        },
    }));
    rejected_delta["prompt"] = json!("SENTINEL_PROMPT");
    rejected_delta["tool_result"] = json!("SENTINEL_TOOL_RESULT");
    rejected_delta["structured_output"] = json!({"value": "SENTINEL_STRUCTURED_OUTPUT"});
    rejected_delta["provider_payload"] = json!("SENTINEL_PROVIDER_PAYLOAD");
    rejected_delta["request_id"] = json!("SENTINEL_REQUEST_ID");
    rejected_delta["native_session_id"] = json!("SENTINEL_NATIVE_SESSION_ID");
    rejected_delta["credential"] = json!("SENTINEL_CREDENTIAL");
    rejected_delta["oversized"] = json!("x".repeat(100_000));
    let (_, rejected_delta) = replay(
        &framed(&[
            init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
            message_start("msg-content"),
            stream_event(json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            })),
            rejected_delta,
        ]),
        AgentValueKind::Response,
        1024,
    );
    let rejected_delta = protocol_rejection_value(&rejected_delta);
    assert_eq!(
        rejected_delta["detail"]["reason"],
        "content_block_index_mismatch"
    );
    assert_eq!(rejected_delta["detail"]["stage"], "content_block_delta");
    assert_eq!(rejected_delta["detail"]["outerEvent"], "stream_event");
    assert_eq!(
        rejected_delta["detail"]["streamEvent"],
        "content_block_delta"
    );
    assert_eq!(rejected_delta["detail"]["contentIndex"], 1);
    assert_eq!(rejected_delta["detail"]["contentBlock"], "text");
    assert_eq!(
        rejected_delta["detail"]["state"]["openContentBlock"],
        json!({"kind": "text", "contentIndex": 0})
    );
    let serialized = rejected_delta.to_string();
    for sentinel in [
        "SENTINEL_ASSISTANT_TEXT",
        "SENTINEL_REASONING",
        "SENTINEL_TOOL_INPUT",
        "SENTINEL_PROMPT",
        "SENTINEL_TOOL_RESULT",
        "SENTINEL_STRUCTURED_OUTPUT",
        "SENTINEL_PROVIDER_PAYLOAD",
        "SENTINEL_REQUEST_ID",
        "SENTINEL_NATIVE_SESSION_ID",
        "SENTINEL_CREDENTIAL",
        SESSION_ID,
    ] {
        assert!(!serialized.contains(sentinel));
    }
    assert!(serialized.len() < 16 * 1024);

    let (_, result_correlation) = replay(
        &framed(&[
            init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
            message_start("msg-active"),
            result(SESSION_ID),
        ]),
        AgentValueKind::None,
        1024,
    );
    let result_correlation = protocol_rejection_value(&result_correlation);
    assert_eq!(
        result_correlation["detail"]["reason"],
        "result_correlation_invalid"
    );
    assert_eq!(result_correlation["detail"]["outerEvent"], "result");
    assert_eq!(
        result_correlation["detail"]["state"]["activeMessage"],
        "main"
    );

    let (_, terminal_drain) = replay(
        &framed(&[
            init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
            result(SESSION_ID),
            result(SESSION_ID),
        ]),
        AgentValueKind::None,
        1024,
    );
    let terminal_drain = protocol_rejection_value(&terminal_drain);
    assert_eq!(
        terminal_drain["detail"]["reason"],
        "terminal_drain_event_invalid"
    );
    assert_eq!(terminal_drain["detail"]["stage"], "terminal_drain");
    assert_eq!(terminal_drain["detail"]["outerEvent"], "result");

    let (_, eof) = replay(
        &framed(&[init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID)]),
        AgentValueKind::None,
        1024,
    );
    let eof = protocol_rejection_value(&eof);
    assert_eq!(eof["detail"]["reason"], "end_of_stream_invariant_invalid");
    assert_eq!(eof["detail"]["stage"], "end_of_stream");
    assert!(eof["detail"].get("outerEvent").is_none());
}

#[test]
fn initialization_identity_and_exchange_boundaries_are_unambiguous() {
    let contradictory = [
        ("claude_code_version", json!("2.1.223")),
        ("cwd", json!("/other")),
        ("model", json!("other-model")),
        ("permissionMode", json!("default")),
        ("session_id", json!("00000000-0000-4000-8000-000000000002")),
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
        Arc::from(SESSION_ID),
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
            "content_block": {"type": "tool_use", "id": "tool-1", "name": "Agent", "input": {}},
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
                    "content": [{"type": "text", "text": "contents"}],
                }],
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
            "tool_use_result": {
                "status": "completed",
                "agentId": "agent-fixture",
                "agentType": "general-purpose",
            },
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
                if call_id.as_ref() == "tool-1" && name.as_ref() == "Agent" && *observed == phase
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
fn result_candidate_requires_correlated_success_acknowledgement() {
    let envelope = json!({"result": 7});
    let call_id = "tool-structured-output";
    let mut terminal_result = result(SESSION_ID);
    terminal_result["structured_output"] = envelope.clone();
    let values = [
        init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
        message_start("msg-result"),
        stream_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {
                "type": "tool_use",
                "id": call_id,
                "name": "StructuredOutput",
                "input": envelope,
            },
        })),
        json!({
            "type": "assistant",
            "message": {
                "id": "msg-result",
                "type": "message",
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": call_id,
                    "name": "StructuredOutput",
                    "input": {"result": 7},
                }],
                "model": MODEL,
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
        }),
        stream_event(json!({"type": "content_block_stop", "index": 0})),
        json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "is_error": true,
                    "content": "Structured output failed",
                }],
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
        }),
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 1},
        })),
        stream_event(json!({"type": "message_stop"})),
        terminal_result,
    ];

    let mut parser = parser(AgentValueKind::Result, 1024);
    parser.push_stdout(&framed(&values), drop).unwrap();
    assert_eq!(
        parser.take_completed_result_exchange(),
        Some(CompletedResultExchange::AmbiguousCandidate),
        "a failed StructuredOutput acknowledgement must disqualify its candidate",
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

    let api_error = framed(&[
        init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
        json!({
            "type": "assistant",
            "message": {
                "id": "msg-api-error",
                "model": "<synthetic>",
                "role": "assistant",
                "type": "message",
                "content": [{"type": "text", "text": "API Error: controlled rejection"}],
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
            "error": "unknown",
            "request_id": "req-controlled",
            "is_api_error_message": true,
        }),
        json!({
            "type": "result",
            "subtype": "success",
            "is_error": true,
            "terminal_reason": "api_error",
            "result": "API Error: controlled rejection",
            "session_id": SESSION_ID,
        }),
    ]);
    let (observations, outcome) = replay(&api_error, AgentValueKind::None, 1024);
    assert_failed(
        outcome,
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::ModelError,
        },
    );
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Error,
            message,
        } if message.as_ref() == "API Error: controlled rejection"
    )));

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
