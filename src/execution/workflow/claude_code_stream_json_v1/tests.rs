use std::ffi::OsString;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

use serde_json::{Value, json};

use super::*;

const SESSION_ID: &str = "00000000-0000-4000-8000-000000000001";
const CWD: &str = "/synthetic/project";
const MODEL: &str = "scherzo-loopback";

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
    let mut parser = ClaudeCodeStreamJsonV1Parser::profile(Arc::from(CWD), Arc::from(MODEL));
    let first = framed(&[
        init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
        status(SESSION_ID),
        result(SESSION_ID),
    ]);
    for byte in first {
        parser.push_stdout(&[byte]).unwrap();
    }

    parser.begin_exchange().unwrap();
    parser
        .push_stdout(&framed(&[
            init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
            result(SESSION_ID),
        ]))
        .unwrap();
    let completion = parser.finish(true).unwrap();
    assert_eq!(completion.session_id(), SESSION_ID);
    assert_eq!(completion.completed_exchanges(), 2);
}

#[test]
fn parser_rejects_wrong_version_truncation_and_frame_overflow() {
    let mut wrong_version = ClaudeCodeStreamJsonV1Parser::profile(Arc::from(CWD), Arc::from(MODEL));
    assert_eq!(
        wrong_version
            .push_stdout(&framed(&[init("2.1.223", SESSION_ID)]))
            .unwrap_err(),
        AgentFailureCause::HarnessStartFailed
    );

    let mut truncated = ClaudeCodeStreamJsonV1Parser::profile(Arc::from(CWD), Arc::from(MODEL));
    let mut transcript = framed(&[
        init(CLAUDE_CODE_STREAM_JSON_V1_VERSION, SESSION_ID),
        result(SESSION_ID),
    ]);
    transcript.pop();
    truncated.push_stdout(&transcript).unwrap();
    assert_eq!(
        truncated.finish(true).unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );

    let limits =
        ClaudeCodeStreamJsonV1ProtocolLimits::with_maximum_frame_bytes(NonZeroU64::new(8).unwrap());
    let mut oversized = ClaudeCodeStreamJsonV1Parser::new(Arc::from(CWD), Arc::from(MODEL), limits);
    assert_eq!(
        oversized.push_stdout(b"123456789").unwrap_err(),
        AgentFailureCause::HarnessStartFailed
    );
}
