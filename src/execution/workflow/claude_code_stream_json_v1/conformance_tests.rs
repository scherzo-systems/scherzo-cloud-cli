use std::fs;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

use super::test_support::{LoopbackProvider, SyntheticClaudeCodeRoot, version_probe_environment};
use super::*;

const MODEL: &str = "scherzo-loopback";
const EFFORT: &str = "xhigh";
const RESPONSE: &str = "loopback complete";
const WATCHDOG: Duration = Duration::from_secs(20);

fn conformance_executable() -> Option<PathBuf> {
    option_env!("SCHERZO_CLAUDE_CODE_CONFORMANCE_EXECUTABLE").map(PathBuf::from)
}

#[test]
fn pinned_claude_code_00_qualification_anchor_is_exact() {
    let Some(executable) = conformance_executable() else {
        return;
    };
    let temporary = tempfile::tempdir().unwrap();
    for (_, value) in version_probe_environment(temporary.path()) {
        fs::create_dir_all(value).unwrap();
    }
    let output = std::process::Command::new(executable)
        .arg("--version")
        .env_clear()
        .envs(version_probe_environment(temporary.path()))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        output.stdout,
        format!("{CLAUDE_CODE_STREAM_JSON_V1_VERSION} (Claude Code)\n").as_bytes()
    );
    println!(
        "qualified Claude Code version={} profile=ClaudeCodeStreamJsonV1 host={}-{}",
        CLAUDE_CODE_STREAM_JSON_V1_VERSION,
        std::env::consts::OS,
        std::env::consts::ARCH,
    );
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
async fn pinned_claude_code_01_normal_mode_loopback_conforms_from_a_synthetic_root() {
    let Some(executable) = conformance_executable() else {
        return;
    };
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let expected_cwd = fs::canonicalize(root.project()).unwrap();
        let message = "Complete the deterministic synthetic exchange.";

        let mut command = Command::new(executable);
        command
            .args(normal_mode_arguments(MODEL, EFFORT, root.system_prompt()))
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        root.configure_command(&mut command, &provider);
        let mut child = command.spawn().unwrap();
        let mut stdin = child.stdin.take().unwrap();
        stdin
            .write_all(&initial_user_text_frame(message).unwrap())
            .await
            .unwrap();
        stdin.shutdown().await.unwrap();
        drop(stdin);

        let request = provider.next_request().await;
        assert_eq!(request.path(), "/v1/messages?beta=true");
        assert!(request.used_placeholder_key());
        assert_eq!(request.body()["model"], MODEL);
        assert_eq!(request.body()["stream"], true);
        assert!(contains_exact_string(request.body(), message));
        request.release_text(RESPONSE);

        let output = child.wait_with_output().await.unwrap();
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        assert!(!provider.has_pending_request());

        let mut parser = ClaudeCodeStreamJsonV1Parser::profile(
            Arc::from(expected_cwd.to_str().unwrap()),
            Arc::from(MODEL),
        );
        for chunk in output.stdout.chunks(7) {
            parser.push_stdout(chunk).unwrap();
        }
        let completion = parser.finish(true).unwrap();
        assert!(!completion.session_id().is_empty());
        assert_eq!(completion.completed_exchanges(), 1);
        provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code conformance watchdog expired");
}

fn contains_exact_string(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values
            .iter()
            .any(|value| contains_exact_string(value, expected)),
        Value::Object(object) => object
            .values()
            .any(|value| contains_exact_string(value, expected)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}
