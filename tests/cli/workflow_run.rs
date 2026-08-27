use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::OwnedFd;
#[cfg(not(target_os = "macos"))]
use std::os::unix::ffi::OsStringExt as _;
use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use nix::pty::{Winsize, openpty};
use nix::sys::stat::Mode;
use nix::unistd::mkfifo;
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use rustix::process::{Pid, Signal, kill_process, test_kill_process};
use tempfile::TempDir;

use super::claude_code_installation::{
    COMPLETE_HELP as CLAUDE_CODE_COMPLETE_HELP, ClaudeCodeFixture,
};
use super::codex_installation::CodexFixture;
use super::pi_installation::{COMPLETE_HELP, PiFixture, quote};
use super::{
    CREDENTIALS_FILE_VARIABLE, DEPLOYMENT_VARIABLES, RUNNER_TELEMETRY_VARIABLES, poll_until,
};

const WORKFLOW_PATH: &str = "workflow.yaml";
const OVERSIZED_AGENT_MESSAGE_BYTES: usize = 512 * 1024;
const OVERSIZED_AGENT_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const OVERSIZED_AGENT_SYSTEM_PROMPT_BYTES: usize = 256 * 1024;
const SIGNAL_FIXTURE_TEST: &str = "workflow_run::signal_command_fixture";
const TUI_HANDSHAKE_VARIABLE: &str = "SCHERZO_INTERNAL_WORKFLOW_RUN_TUI_HANDSHAKE";
const CODEX_THREAD_ID: &str = "018f7f1e-7b5a-7d13-8f19-2b6a4c8d0e12";
const CODEX_TURN_ID: &str = "turn-fixture";
const CODEX_CORRECTION_TURN_ID: &str = "turn-correction";
const CODEX_PROVIDER: &str = "fixture-provider";

#[cfg(target_os = "linux")]
#[expect(
    clippy::disallowed_methods,
    reason = "this fixed external-process pre-delay is not a condition poll"
)]
pub(super) fn wait_for_process_poll() {
    let (_sender, receiver) = std::sync::mpsc::channel::<()>();
    assert_eq!(
        receiver.recv_timeout(std::time::Duration::from_millis(10)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    );
}

#[cfg(target_os = "linux")]
fn process_state(process: Pid) -> Option<u8> {
    fs::read(
        Path::new("/proc")
            .join(process.as_raw_pid().to_string())
            .join("stat"),
    )
    .ok()
    .and_then(|stat| {
        stat.windows(2)
            .rposition(|bytes| bytes == b") ")
            .and_then(|end| stat.get(end + 2).copied())
    })
}

#[cfg(target_os = "linux")]
pub(super) fn open_tui_pty() -> (OwnedFd, OwnedFd) {
    let master = rustix::pty::openpt(
        rustix::pty::OpenptFlags::RDWR
            | rustix::pty::OpenptFlags::NOCTTY
            | rustix::pty::OpenptFlags::CLOEXEC,
    )
    .unwrap();
    rustix::pty::grantpt(&master).unwrap();
    rustix::pty::unlockpt(&master).unwrap();
    let slave = rustix::pty::ioctl_tiocgptpeer(
        &master,
        rustix::pty::OpenptFlags::RDWR
            | rustix::pty::OpenptFlags::NOCTTY
            | rustix::pty::OpenptFlags::CLOEXEC,
    )
    .unwrap();
    rustix::termios::tcsetwinsize(
        &slave,
        rustix::termios::Winsize {
            ws_row: 30,
            ws_col: 100,
            ws_xpixel: 0,
            ws_ypixel: 0,
        },
    )
    .unwrap();
    (master, slave)
}

#[cfg(target_os = "linux")]
pub(super) fn spawn_tui_run(
    args: &[String],
    master: OwnedFd,
    slave: &OwnedFd,
) -> (
    std::process::Child,
    std::fs::File,
    std::thread::JoinHandle<Vec<u8>>,
) {
    let master_reader = rustix::io::dup(&master).unwrap();
    let reader = std::thread::spawn(move || {
        let mut master_reader = std::fs::File::from(master_reader);
        let mut transcript = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            match master_reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => transcript.extend_from_slice(&buffer[..count]),
                Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
                Err(error) => panic!("read TUI pseudoterminal: {error}"),
            }
        }
        transcript
    });

    let child_stdin = rustix::io::dup(slave).unwrap();
    let child_stdout = rustix::io::dup(slave).unwrap();
    let child_stderr = rustix::io::dup(slave).unwrap();
    let child = isolated_command(args)
        .env("TERM", "xterm")
        .env("NO_COLOR", "1")
        .stdin(Stdio::from(child_stdin))
        .stdout(Stdio::from(child_stdout))
        .stderr(Stdio::from(child_stderr))
        .spawn()
        .unwrap();
    (child, std::fs::File::from(master), reader)
}

pub(super) struct RunBundle {
    _temporary: TempDir,
    source_root: PathBuf,
    execution_root: PathBuf,
    result_parent: PathBuf,
    claude_config: PathBuf,
}

impl RunBundle {
    pub(super) fn new(source: &str) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        let result_parent = temporary.path().join("results");
        let claude_config = temporary.path().join("claude-config");
        for directory in [
            &source_root,
            &execution_root,
            &result_parent,
            &claude_config,
        ] {
            fs::create_dir(directory).unwrap();
        }
        fs::write(source_root.join(WORKFLOW_PATH), source).unwrap();
        Self {
            _temporary: temporary,
            source_root,
            execution_root,
            result_parent,
            claude_config,
        }
    }

    pub(super) fn result(&self, name: &str) -> PathBuf {
        self.result_parent.join(name)
    }

    fn write_source(&self, path: &str, bytes: impl AsRef<[u8]>) {
        let destination = self.source_root.join(path);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(destination, bytes).unwrap();
    }

    pub(super) fn source_root(&self) -> &Path {
        &self.source_root
    }

    pub(super) fn execution_root(&self) -> &Path {
        &self.execution_root
    }

    pub(super) fn initial_cwd(&self) -> &Path {
        self._temporary.path()
    }

    fn claude_config(&self) -> &Path {
        &self.claude_config
    }

    pub(super) fn args(&self, result: &Path) -> Vec<String> {
        vec![
            "workflow".to_owned(),
            "run".to_owned(),
            "--source-root".to_owned(),
            self.source_root.to_string_lossy().into_owned(),
            "--execution-root".to_owned(),
            self.execution_root.to_string_lossy().into_owned(),
            "--run-dir".to_owned(),
            result.to_string_lossy().into_owned(),
            self.source_root
                .join(WORKFLOW_PATH)
                .to_string_lossy()
                .into_owned(),
        ]
    }
}

pub(super) fn isolated_command(args: &[String]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args(args)
        .env_remove(CREDENTIALS_FILE_VARIABLE)
        .env(
            CREDENTIALS_FILE_VARIABLE,
            "/dev/null/workflow-run-credentials.json",
        );
    for variable in DEPLOYMENT_VARIABLES
        .into_iter()
        .chain(RUNNER_TELEMETRY_VARIABLES)
    {
        command.env_remove(variable);
    }
    command
}

pub(super) fn run(args: &[String]) -> Output {
    isolated_command(args).output().unwrap()
}

fn fixture_path_with_host_tools(fixtures: &[&Path]) -> OsString {
    let inherited = std::env::var_os("PATH").unwrap_or_default();
    std::env::join_paths(
        fixtures
            .iter()
            .map(|path| (*path).to_owned())
            .chain(std::env::split_paths(&inherited)),
    )
    .unwrap()
}

fn serve_fake_provider(
    socket_path: &Path,
    expected_requests: usize,
) -> std::thread::JoinHandle<Vec<serde_json::Value>> {
    let listener = UnixListener::bind(socket_path).unwrap();
    std::thread::spawn(move || {
        let mut observed = Vec::new();
        for _ in 0..expected_requests {
            let (mut stream, _) = listener.accept().unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).unwrap();
            let mut payload = vec![0_u8; usize::try_from(u32::from_be_bytes(length)).unwrap()];
            stream.read_exact(&mut payload).unwrap();
            let request: serde_json::Value = serde_json::from_slice(&payload).unwrap();
            let response = fake_provider_response(&request);
            let payload = serde_json::to_vec(&response).unwrap();
            stream
                .write_all(&u32::try_from(payload.len()).unwrap().to_be_bytes())
                .unwrap();
            stream.write_all(&payload).unwrap();
            observed.push(request);
        }
        observed
    })
}

fn fake_provider_response(request: &serde_json::Value) -> serde_json::Value {
    match request["kind"].as_str() {
        Some("before_agent_start") => serde_json::json!({"kind": "release"}),
        Some("model") => {
            let system = request["systemPrompt"].as_str().unwrap();
            if system.contains("MODE_NONE") {
                let has_tool_result = request["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|message| message["role"] == "toolResult");
                if has_tool_result {
                    serde_json::json!({
                        "kind": "text",
                        "blocks": ["no requested value"],
                        "stopReason": "stop"
                    })
                } else {
                    serde_json::json!({
                        "kind": "toolCalls",
                        "calls": [{
                            "id": "write-agent-file",
                            "name": "fixture_write",
                            "arguments": {}
                        }]
                    })
                }
            } else if system.contains("MODE_OVERSIZED_RESPONSE") {
                serde_json::json!({
                    "kind": "text",
                    "blocks": ["r".repeat(OVERSIZED_AGENT_RESPONSE_BYTES)],
                    "stopReason": "stop"
                })
            } else if system.contains("MODE_RESPONSE") {
                serde_json::json!({
                    "kind": "text",
                    "blocks": ["agent ", "response"],
                    "stopReason": "stop"
                })
            } else if system.contains("MODE_OVERSIZED_RESULT") {
                let tool_name = request["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(|tool| tool["name"].as_str())
                    .find(|name| name.starts_with("scherzo_result_"))
                    .unwrap();
                serde_json::json!({
                    "kind": "toolCalls",
                    "calls": [{
                        "id": "submit-oversized-result",
                        "name": tool_name,
                        "arguments": {"result": {"payload": "x".repeat(4 * 1024 * 1024 - 14)}}
                    }]
                })
            } else if system.contains("MODE_RESULT") {
                let tool_name = request["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(|tool| tool["name"].as_str())
                    .find(|name| name.starts_with("scherzo_result_"))
                    .unwrap();
                serde_json::json!({
                    "kind": "toolCalls",
                    "calls": [{
                        "id": "submit-result",
                        "name": tool_name,
                        "arguments": {"result": {"answer": 42, "source": "agent response"}}
                    }]
                })
            } else {
                panic!("unexpected model system prompt")
            }
        }
        kind => panic!("unexpected fake-provider request: {kind:?}"),
    }
}

fn attempt_result(path: &Path) -> PathBuf {
    path.join("attempts/000001/result")
}

fn result_json(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&fs::read(attempt_result(path).join("result.json")).unwrap()).unwrap()
}

fn normalized_run_directory(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap()
}

fn git(repository: &Path, arguments: &[&str]) -> String {
    let path = std::env::var_os("PATH");
    let mut command = Command::new("git");
    command
        .args([
            "--no-pager",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgSign=false",
            "-c",
            "tag.gpgSign=false",
            "-c",
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
        ])
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .env_clear()
        .env("HOME", "/nonexistent")
        .env("LC_ALL", "C")
        .env("TZ", "UTC")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_PAGER", "cat")
        .env("GIT_EDITOR", "true")
        .env("GIT_SEQUENCE_EDITOR", "true")
        .env("PAGER", "cat")
        .env("EDITOR", "true")
        .stdin(Stdio::null());
    if let Some(path) = path {
        command.env("PATH", path);
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn initialize_git_repository(repository: &Path) {
    git(repository, &["init", "--quiet"]);
    git(repository, &["config", "user.name", "Scherzo Test"]);
    git(
        repository,
        &["config", "user.email", "test@example.invalid"],
    );
    fs::write(repository.join("tracked.txt"), b"baseline\n").unwrap();
    git(repository, &["add", "tracked.txt"]);
    git(repository, &["commit", "--quiet", "-m", "baseline"]);
}

fn producer_consumer_source() -> &'static str {
    r#"schemaVersion: 1
steps:
  produce:
    kind: cmd
    inputs:
      prompt:
        ref: imports.prompt
      attachments:
        ref: imports.attachments
    command:
      argv: ["sh", "-c", "set -eu; if IFS= read -r unexpected; then exit 91; fi; test -z \"${SCHERZO_PRIVATE_SENTINEL+x}\"; { cat \"$SCHERZO_STEP_INPUTS/values/prompt\"; printf '|'; cat \"$SCHERZO_STEP_INPUTS/collections/attachments/000000\"; printf '|'; cat \"$SCHERZO_STEP_INPUTS/collections/attachments/000001\"; } > produced.txt; printf producer-live"]
    outputs:
      artifact:
        kind: file
        from: path
        path: produced.txt
        mediaType: text/plain
  consume:
    kind: cmd
    inputs:
      source:
        ref: outputs.produce.artifact
    command:
      argv: ["sh", "-c", "set -eu; if IFS= read -r unexpected; then exit 92; fi; cat \"$SCHERZO_STEP_INPUTS/values/source\" > exported.txt; printf consumer-live"]
    outputs:
      result:
        kind: file
        from: path
        path: exported.txt
        mediaType: text/plain
exports:
  result:
    ref: outputs.consume.result
"#
}

fn response_agent_source() -> &'static str {
    r#"schemaVersion: 1
agentProfiles:
  local:
    harness:
      kind: pi
      config:
        model: fixture/model
        thinking: off
steps:
  answer:
    kind: agent
    agent:
      profile: local
      systemPrompt: system.md
      message:
        text:
          - file: message.md
    outputs:
      response:
        kind: text
        from: agent_response
exports:
  response:
    ref: outputs.answer.response
"#
}

fn git_branch_agent_source() -> &'static str {
    r#"schemaVersion: 1
agentProfiles:
  local:
    harness:
      kind: pi
      config:
        model: fixture/model
        thinking: off
steps:
  implement:
    kind: agent
    agent:
      profile: local
      systemPrompt: system.md
      message:
        text:
          - file: message.md
    outputs:
      changes:
        kind: git_branch
        from: workspace
exports:
  changes:
    ref: outputs.implement.changes
"#
}

fn response_pi_execution() -> String {
    let frames = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/execution/workflow/pi_json_v1/fixtures/response-success.jsonl"
    ));
    let remaining = frames
        .lines()
        .skip(1)
        .map(quote)
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "session_dir=; while [ \"$#\" -gt 0 ]; do if [ \"$1\" = --session-dir ]; then shift; session_dir=$1; fi; shift; done; case \"$session_dir\" in /*) ;; *) exit 74 ;; esac; printf '{{partial' > \"$session_dir/retained-partial.jsonl\"; cwd=$(pwd); printf '{{\"type\":\"session\",\"version\":3,\"id\":\"00000000-0000-4000-8000-000000000001\",\"timestamp\":\"2026-07-30T12:00:00Z\",\"cwd\":\"%s\"}}\\n' \"$cwd\"; printf '%s\\n' {remaining}"
    )
}

fn response_claude_code_execution() -> &'static str {
    r#"set -eu
model=
session=
previous=
for argument in "$@"; do
  if [ "$previous" = --model ]; then model=$argument; fi
  if [ "$previous" = --session-id ]; then session=$argument; fi
  previous=$argument
done
config=${CLAUDE_CONFIG_DIR:-$HOME/.claude}
set -- "$config"/projects/*
[ "$#" -eq 1 ] && [ -d "$1" ]
native_project=$1
printf '{malformed retained transcript' > "$native_project/$session.jsonl"
while IFS= read -r _; do :; done
printf '{"type":"system","subtype":"init","cwd":"%s","session_id":"%s","model":"%s","permissionMode":"bypassPermissions","claude_code_version":"2.1.241"}\n' "$PWD" "$session" "$model"
printf '{"type":"stream_event","event":{"type":"message_start","message":{"id":"msg-local","type":"message","role":"assistant","content":[],"model":"%s","usage":{"input_tokens":1,"output_tokens":0}}},"session_id":"%s","parent_tool_use_id":null}\n' "$model" "$session"
printf '{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"claude response"}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"assistant","message":{"id":"msg-local","type":"message","role":"assistant","content":[{"type":"text","text":"claude response"}],"model":"%s"},"parent_tool_use_id":null,"session_id":"%s"}\n' "$model" "$session"
printf '{"type":"stream_event","event":{"type":"content_block_stop","index":0},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_stop"},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"result","subtype":"success","is_error":false,"terminal_reason":"completed","result":"convenience duplicate","session_id":"%s"}\n' "$session""#
}

fn response_claude_code_execution_for_version(version: &str) -> String {
    response_claude_code_execution().replace(
        "\"claude_code_version\":\"2.1.241\"",
        &format!("\"claude_code_version\":\"{version}\""),
    )
}

fn corrected_claude_code_result_execution() -> &'static str {
    r#"set -eu
model=
session=
previous=
for argument in "$@"; do
  if [ "$previous" = --model ]; then model=$argument; fi
  if [ "$previous" = --session-id ]; then session=$argument; fi
  previous=$argument
done
emit_exchange() {
  exchange=$1
  value=$2
  call=tool-result-$exchange
  message=msg-result-$exchange
  printf '{"type":"system","subtype":"init","cwd":"%s","session_id":"%s","model":"%s","permissionMode":"bypassPermissions","claude_code_version":"2.1.241"}\n' "$PWD" "$session" "$model"
  printf '{"type":"stream_event","event":{"type":"message_start","message":{"id":"%s","type":"message","role":"assistant","content":[],"model":"%s","usage":{"input_tokens":1,"output_tokens":0}}},"session_id":"%s","parent_tool_use_id":null}\n' "$message" "$model" "$session"
  printf '{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"%s","name":"StructuredOutput","input":{"result":%s}}},"session_id":"%s","parent_tool_use_id":null}\n' "$call" "$value" "$session"
  printf '{"type":"assistant","message":{"id":"%s","type":"message","role":"assistant","content":[{"type":"tool_use","id":"%s","name":"StructuredOutput","input":{"result":%s}}],"model":"%s"},"parent_tool_use_id":null,"session_id":"%s"}\n' "$message" "$call" "$value" "$model" "$session"
  printf '{"type":"stream_event","event":{"type":"content_block_stop","index":0},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
  printf '{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"%s","content":"Structured output provided successfully"}]},"parent_tool_use_id":null,"session_id":"%s","tool_use_result":"Structured output provided successfully"}\n' "$call" "$session"
  printf '{"type":"stream_event","event":{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
  printf '{"type":"stream_event","event":{"type":"message_stop"},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
  printf '{"type":"result","subtype":"success","is_error":false,"terminal_reason":"completed","result":"structured convenience","session_id":"%s","structured_output":{"result":%s}}\n' "$session" "$value"
}
IFS= read -r _
emit_exchange 1 -1
IFS= read -r _
emit_exchange 2 7
"#
}

fn result_claude_code_agent_source() -> &'static str {
    r#"schemaVersion: 1
agentProfiles:
  local:
    harness:
      kind: claude_code
      config:
        model: fixture/claude
        effort: high
steps:
  answer:
    kind: agent
    agent:
      profile: local
      systemPrompt: system.md
      message:
        text:
          - file: message.md
    outputs:
      result:
        kind: json
        from: agent_result
        schema: result.schema.json
exports:
  result:
    ref: outputs.answer.result
"#
}

fn blocked_claude_code_execution() -> &'static str {
    r#"set -eu
model=
session=
previous=
for argument in "$@"; do
  if [ "$previous" = --model ]; then model=$argument; fi
  if [ "$previous" = --session-id ]; then session=$argument; fi
  previous=$argument
done
IFS= read -r _
printf '%s\n' "$$" > "$CLAUDE_FIXTURE_PID"
trap 'exit 130' INT TERM
printf '{"type":"system","subtype":"init","cwd":"%s","session_id":"%s","model":"%s","permissionMode":"bypassPermissions","claude_code_version":"2.1.241"}\n' "$PWD" "$session" "$model"
printf '\001' > "$WORKFLOW_READY_FIFO"
IFS= read -r _ < "$WORKFLOW_RELEASE_FIFO"
exit 23"#
}

fn response_claude_code_agent_source() -> &'static str {
    r#"schemaVersion: 1
agentProfiles:
  local:
    harness:
      kind: claude_code
      config:
        model: fixture/claude
        effort: high
steps:
  answer:
    kind: agent
    agent:
      profile: local
      systemPrompt: system.md
      message:
        text:
          - file: message.md
    outputs:
      response:
        kind: text
        from: agent_response
exports:
  response:
    ref: outputs.answer.response
"#
}

fn response_codex_agent_source() -> &'static str {
    r#"schemaVersion: 1
agentProfiles:
  local:
    harness:
      kind: codex
      config:
        model: fixture/codex
        effort: xhigh
steps:
  answer:
    kind: agent
    agent:
      profile: local
      systemPrompt: system.md
      message:
        text:
          - file: message.md
    outputs:
      response:
        kind: text
        from: agent_response
exports:
  response:
    ref: outputs.answer.response
"#
}

fn no_value_codex_agent_source() -> &'static str {
    r#"schemaVersion: 1
agentProfiles:
  local:
    harness:
      kind: codex
      config:
        model: fixture/codex
        effort: high
steps:
  act:
    kind: agent
    agent:
      profile: local
      systemPrompt: system.md
      message:
        text:
          - file: message.md
"#
}

fn result_codex_agent_source() -> &'static str {
    r#"schemaVersion: 1
agentProfiles:
  local:
    harness:
      kind: codex
      config:
        model: fixture/codex
        effort: high
steps:
  answer:
    kind: agent
    agent:
      profile: local
      systemPrompt: system.md
      message:
        text:
          - file: message.md
    outputs:
      result:
        kind: json
        from: agent_result
        schema: result.schema.json
exports:
  result:
    ref: outputs.answer.result
"#
}

fn mixed_harness_source() -> &'static str {
    r#"schemaVersion: 1
agentProfiles:
  piLocal:
    harness:
      kind: pi
      config:
        model: fixture/pi
        thinking: high
  claudeLocal:
    harness:
      kind: claude_code
      config:
        model: fixture/claude
        effort: xhigh
steps:
  piAnswer:
    kind: agent
    agent:
      profile: piLocal
      systemPrompt: pi-system.md
      message:
        text:
          - file: message.md
    outputs:
      response:
        kind: text
        from: agent_response
  claudeAnswer:
    kind: agent
    dependsOn: [piAnswer]
    agent:
      profile: claudeLocal
      systemPrompt: claude-system.md
      message:
        text:
          - file: message.md
    outputs:
      response:
        kind: text
        from: agent_response
exports:
  claudeResponse:
    ref: outputs.claudeAnswer.response
  piResponse:
    ref: outputs.piAnswer.response
"#
}

fn mixed_codex_harness_source() -> &'static str {
    r#"schemaVersion: 1
agentProfiles:
  piLocal:
    harness:
      kind: pi
      config:
        model: fixture/pi
        thinking: high
  claudeLocal:
    harness:
      kind: claude_code
      config:
        model: fixture/claude
        effort: xhigh
  codexLocal:
    harness:
      kind: codex
      config:
        model: fixture/codex
        effort: high
steps:
  piAnswer:
    kind: agent
    agent:
      profile: piLocal
      systemPrompt: pi-system.md
      message:
        text:
          - file: message.md
    outputs:
      response:
        kind: text
        from: agent_response
  claudeAnswer:
    kind: agent
    dependsOn: [piAnswer]
    agent:
      profile: claudeLocal
      systemPrompt: claude-system.md
      message:
        text:
          - file: message.md
    outputs:
      response:
        kind: text
        from: agent_response
  codexAnswer:
    kind: agent
    dependsOn: [claudeAnswer]
    agent:
      profile: codexLocal
      systemPrompt: codex-system.md
      message:
        text:
          - file: message.md
    outputs:
      response:
        kind: text
        from: agent_response
exports:
  claudeResponse:
    ref: outputs.claudeAnswer.response
  codexResponse:
    ref: outputs.codexAnswer.response
  piResponse:
    ref: outputs.piAnswer.response
"#
}

#[test]
fn command_only_run_remains_harness_independent() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"/bin/sh\", \"-c\", \"printf command-only\"]\n",
    );
    let empty_path = tempfile::tempdir().unwrap();
    let destination = bundle.result("without-agent-harness");
    let output = isolated_command(&bundle.args(&destination))
        .env("PATH", empty_path.path())
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("command-only"));
    let result_path = attempt_result(&destination).join("result.json");
    let result: serde_json::Value =
        serde_json::from_slice(&fs::read(result_path).unwrap()).unwrap();
    assert!(result.get("finalization").is_none());
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("state.json")).unwrap()).unwrap();
    assert!(state["attempts"][0].get("finalization").is_none());
}

#[test]
fn advisory_failure_keeps_truthful_state_and_returns_success() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  analyze:
    kind: cmd
    failurePolicy: advisory
    command:
      argv: ["/bin/sh", "-c", "exit 9"]
    outputs:
      report:
        kind: file
        from: path
        path: report.json
        mediaType: application/json
  package:
    kind: cmd
    dependsOn: [analyze]
    command:
      argv: ["/bin/sh", "-c", "printf packaged > package.txt"]
  summarize:
    kind: cmd
    failurePolicy: advisory
    inputs:
      report:
        ref: outputs.analyze.report
    command:
      argv: ["/bin/true"]
"#,
    );
    let destination = bundle.result("advisory-failure");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let output = run(&args);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["outcome"], "succeeded");
    assert_eq!(terminal["exitStatus"], 0);
    assert!(terminal["result"].get("primaryFailure").is_none());
    assert_eq!(terminal["result"]["steps"][0]["id"], "analyze");
    assert_eq!(terminal["result"]["steps"][0]["failurePolicy"], "advisory");
    assert_eq!(terminal["result"]["steps"][0]["state"], "failed");
    assert_eq!(terminal["result"]["steps"][1]["id"], "package");
    assert_eq!(terminal["result"]["steps"][1]["failurePolicy"], "required");
    assert_eq!(terminal["result"]["steps"][1]["state"], "succeeded");
    assert_eq!(terminal["result"]["steps"][2]["id"], "summarize");
    assert_eq!(terminal["result"]["steps"][2]["failurePolicy"], "advisory");
    assert_eq!(terminal["result"]["steps"][2]["state"], "blocked");
    assert_eq!(terminal["result"]["steps"][2]["dependency"], "analyze");
    assert_eq!(
        fs::read(bundle.execution_root().join("package.txt")).unwrap(),
        b"packaged"
    );
    let progress = String::from_utf8(output.stderr).unwrap();
    assert!(progress.contains("failed"));
    assert!(progress.contains("advisory"));
    assert!(progress.contains("2 advisory issues"));

    let status = isolated_command(&[
        "workflow".to_owned(),
        "status".to_owned(),
        destination.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["state"]["attempts"][0]["state"], "succeeded");
    assert_eq!(
        status["state"]["attempts"][0]["progress"]["steps"][0]["failurePolicy"],
        "advisory"
    );
    assert_eq!(
        status["state"]["attempts"][0]["progress"]["steps"][0]["state"],
        "failed"
    );
    assert_eq!(status["retry"]["eligible"], false);
    assert_eq!(status["retry"]["reason"], "latest_attempt_succeeded");
}

#[test]
fn finalization_runs_after_ordinary_failure_and_is_durable_before_publication() {
    let bundle = RunBundle::new(include_str!(
        "../fixtures/workflow-run/finalization-cleanup.yaml"
    ));
    let destination = bundle.result("failed-with-cleanup");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let output = run(&args);

    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = &terminal["result"];
    assert_eq!(result["outcome"], "failed");
    assert_eq!(result["primaryFailure"]["node"]["id"], "fail");
    assert_eq!(result["primaryFailure"]["node"]["role"], "step");
    assert_eq!(result["steps"][0]["role"], "step");
    assert_eq!(result["finalization"]["trigger"], "failed");
    assert_eq!(result["finalization"]["finalizers"][0]["id"], "cleanup");
    assert_eq!(result["finalization"]["finalizers"][0]["role"], "finalizer");
    assert_eq!(
        result["finalization"]["finalizers"][0]["state"],
        "succeeded"
    );
    assert_eq!(result["finalization"]["finalizers"][1]["state"], "not_run");
    assert_eq!(
        result["finalization"]["finalizers"][1]["reason"],
        "finalizer_trigger_not_selected"
    );
    assert_eq!(result["finalization"]["finalizers"][2]["state"], "failed");
    assert_eq!(
        result["finalization"]["issues"],
        serde_json::json!([{
            "node": { "id": "notify", "role": "finalizer" },
            "impact": "advisory"
        }])
    );
    assert_eq!(
        result["exports"]["cleanupReceipt"],
        serde_json::json!({
            "state": "unavailable",
            "reason": "source_trigger_not_selected"
        })
    );
    assert_eq!(
        fs::read(bundle.execution_root().join("cleanup.txt")).unwrap(),
        b"cleanup-complete"
    );

    let progress = String::from_utf8(output.stderr).unwrap();
    let ordinary = progress.find("ordinary phase").unwrap();
    let finalization = progress
        .find("finalization phase · trigger failed")
        .unwrap();
    assert!(ordinary < finalization);
    assert!(progress.contains("finalization: cleanup complete"));

    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["attempts"][0]["finalization"]["complete"], true);
    assert_eq!(state["attempts"][0]["finalization"]["trigger"], "failed");
    assert_eq!(
        state["attempts"][0]["finalization"]["finalizers"][0]["state"],
        "succeeded"
    );
    let status = isolated_command(&[
        "workflow".to_owned(),
        "status".to_owned(),
        destination.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(
        status["state"]["attempts"][0]["finalization"]["complete"],
        true
    );
    assert_eq!(
        status["state"]["attempts"][0]["finalization"]["trigger"],
        "failed"
    );
    assert_eq!(status["retry"]["eligible"], true);
}

#[test]
fn finalizer_input_unavailability_is_preserved_in_summary_and_exports() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  complete:
    kind: cmd
    command:
      argv: ["true"]
finalizers:
  produce:
    kind: cmd
    when: [succeeded]
    command:
      argv: ["/bin/sh", "-c", "exit 19"]
    outputs:
      payload:
        kind: file
        from: path
        path: payload.json
        mediaType: application/json
  consume:
    kind: cmd
    when: [succeeded]
    inputs:
      payload:
        ref: outputs.produce.payload
    command:
      argv: ["true"]
    outputs:
      receipt:
        kind: file
        from: path
        path: receipt.json
        mediaType: application/json
exports:
  receipt:
    ref: outputs.consume.receipt
"#,
    );
    let destination = bundle.result("finalizer-input-unavailable");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let output = run(&args);

    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = &terminal["result"];
    assert_eq!(result["outcome"], "failed");
    assert_eq!(result["primaryFailure"]["node"]["id"], "produce");
    assert_eq!(result["primaryFailure"]["node"]["role"], "finalizer");
    assert_eq!(result["finalization"]["trigger"], "succeeded");
    assert_eq!(result["finalization"]["finalizers"][0]["state"], "failed");
    assert_eq!(result["finalization"]["finalizers"][1]["state"], "blocked");
    assert_eq!(
        result["finalization"]["finalizers"][1]["reason"],
        "input_unavailable"
    );
    assert_eq!(
        result["finalization"]["finalizers"][1]["unavailableReferences"],
        serde_json::json!(["outputs.produce.payload"])
    );
    assert_eq!(
        result["finalization"]["issues"].as_array().unwrap().len(),
        2
    );
    assert_eq!(
        result["exports"]["receipt"],
        serde_json::json!({
            "state": "unavailable",
            "reason": "source_input_unavailable"
        })
    );
    let progress = String::from_utf8(output.stderr).unwrap();
    assert!(progress.contains("inputs unavailable: outputs.produce.payload"));
}

#[test]
fn workflow_file_and_run_directory_resolve_from_the_initial_working_directory() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    );
    let args = [
        "workflow",
        "run",
        "--source-root",
        "source",
        "--execution-root",
        "execution",
        "--run-dir",
        "results/completable",
        "source/workflow.yaml",
        "--json",
    ];
    let output = isolated_command(&args.map(str::to_owned))
        .current_dir(bundle.initial_cwd())
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let run_directory = fs::canonicalize(bundle.result("completable")).unwrap();
    assert_eq!(terminal["runDirectory"], run_directory.to_str().unwrap());
    assert_eq!(terminal["result"]["workflow"]["path"], WORKFLOW_PATH);
}

#[test]
fn export_aliases_share_carriers_without_collapsing_equal_captures() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  produce:
    kind: cmd
    command:
      argv: ["/bin/sh", "-c", "printf same > first.bin; printf same > second.bin"]
    outputs:
      first:
        kind: file
        from: path
        path: first.bin
        mediaType: application/octet-stream
      second:
        kind: file
        from: path
        path: second.bin
        mediaType: application/octet-stream
exports:
  firstCopy:
    ref: outputs.produce.first
  firstPrimary:
    ref: outputs.produce.first
  second:
    ref: outputs.produce.second
"#,
    );
    let destination = bundle.result("aliases");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let output = run(&args);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = &terminal["result"];
    assert_eq!(
        result["exports"]["firstCopy"],
        result["exports"]["firstPrimary"]
    );
    assert_eq!(result["exports"]["firstCopy"]["path"], "exports/0001");
    assert_eq!(result["exports"]["second"]["path"], "exports/0003");
    assert_eq!(
        result["exports"]["firstCopy"]["digest"],
        result["exports"]["second"]["digest"]
    );
    assert!(result["steps"][0].get("committedOutputCount").is_none());
    let result_root = attempt_result(&destination);
    for name in ["firstCopy", "firstPrimary", "second"] {
        let relative = result["exports"][name]["path"].as_str().unwrap();
        assert_eq!(fs::read(result_root.join(relative)).unwrap(), b"same");
    }
    let files = fs::read_dir(result_root.join("exports"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        files,
        std::collections::BTreeSet::from(["0001".into(), "0003".into()])
    );
}

#[test]
fn git_context_rejection_occurs_before_any_step_starts() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  mutate:
    kind: cmd
    command:
      argv: ["/bin/sh", "-c", "printf started > marker.txt"]
    outputs:
      changes:
        kind: git_branch
        from: workspace
exports:
  changes:
    ref: outputs.mutate.changes
"#,
    );
    initialize_git_repository(bundle.initial_cwd());
    let destination = bundle.result("git-context-rejected");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let output = run(&args);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let rejection: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rejection["outcome"], "rejected");
    assert_eq!(rejection["phase"], "admission");
    assert_eq!(
        rejection["diagnostics"][0]["code"],
        "git_context_execution_root_mismatch"
    );
    assert!(!bundle.execution_root().join("marker.txt").exists());
    assert!(!destination.exists());
}

#[test]
fn semantic_outputs_workspace_command_mixed_success() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  produce:
    kind: cmd
    command:
      argv: ["/bin/sh", "-c", "set -eu; printf 'changed\\n' > tracked.txt; printf report > report.txt; git add tracked.txt report.txt; git commit --quiet -m change"]
    outputs:
      changes:
        kind: git_branch
        from: workspace
      report:
        kind: file
        from: path
        path: report.txt
        mediaType: text/plain
exports:
  changesAlias:
    ref: outputs.produce.changes
  changesPrimary:
    ref: outputs.produce.changes
  report:
    ref: outputs.produce.report
"#,
    );
    initialize_git_repository(bundle.execution_root());
    let destination = bundle.result("git-changed");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let output = run(&args);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = &terminal["result"];
    let alias = &result["exports"]["changesAlias"];
    assert_eq!(alias, &result["exports"]["changesPrimary"]);
    assert_eq!(alias["kind"], "git_branch");
    assert_eq!(alias["artifactVersion"], 1);
    assert_eq!(alias["objectFormat"], "sha1");
    assert_ne!(alias["baseOid"], alias["headOid"]);
    assert_eq!(alias["carrier"]["path"], "exports/0001");
    assert_eq!(alias["carrier"]["mediaType"], "application/vnd.git.bundle");
    assert_eq!(result["exports"]["report"]["path"], "exports/0003");
    let artifact = attempt_result(&destination);
    let files = fs::read_dir(artifact.join("exports"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        files,
        std::collections::BTreeSet::from(["0001".into(), "0003".into()])
    );
    let mut complete_set = result.clone();
    complete_set["exports"]["unavailable"] = serde_json::json!({
        "state": "unavailable",
        "reason": "source_blocked"
    });
    let mut result_bytes = serde_json::to_vec_pretty(&complete_set).unwrap();
    result_bytes.push(b'\n');
    fs::write(artifact.join("result.json"), result_bytes).unwrap();

    let validation = run(&[
        "artifact".to_owned(),
        "validate".to_owned(),
        "--json".to_owned(),
        artifact.to_string_lossy().into_owned(),
    ]);
    assert!(
        validation.status.success(),
        "artifact validation failed: {}",
        String::from_utf8_lossy(&validation.stdout)
    );

    let carrier_path = artifact.join("exports/0001");
    let original = fs::read(&carrier_path).unwrap();
    fs::set_permissions(&carrier_path, fs::Permissions::from_mode(0o600)).unwrap();
    let advertised_ref = b"refs/scherzo/head";
    let ref_offset = original
        .windows(advertised_ref.len())
        .position(|window| window == advertised_ref)
        .unwrap();
    let mut wrong_profile = original.clone();
    wrong_profile[ref_offset..ref_offset + advertised_ref.len()]
        .copy_from_slice(b"refs/scherzo/heap");
    fs::write(&carrier_path, wrong_profile).unwrap();
    let invalid_profile = run(&[
        "artifact".to_owned(),
        "validate".to_owned(),
        "--json".to_owned(),
        artifact.to_string_lossy().into_owned(),
    ]);
    let profile_report: serde_json::Value =
        serde_json::from_slice(&invalid_profile.stdout).unwrap();
    let profile_codes = profile_report["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|diagnostic| diagnostic["code"].as_str())
        .collect::<Vec<_>>();
    assert!(profile_codes.contains(&"carrier_digest_mismatch"));
    assert!(profile_codes.contains(&"git_bundle_profile_invalid"));

    let mut wrong_checksum = original.clone();
    *wrong_checksum.last_mut().unwrap() ^= 1;
    fs::write(&carrier_path, wrong_checksum).unwrap();
    let invalid_checksum = run(&[
        "artifact".to_owned(),
        "validate".to_owned(),
        "--json".to_owned(),
        artifact.to_string_lossy().into_owned(),
    ]);
    let checksum_report: serde_json::Value =
        serde_json::from_slice(&invalid_checksum.stdout).unwrap();
    let checksum_codes = checksum_report["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|diagnostic| diagnostic["code"].as_str())
        .collect::<Vec<_>>();
    assert!(checksum_codes.contains(&"carrier_digest_mismatch"));
    assert!(checksum_codes.contains(&"git_pack_checksum_mismatch"));
}

#[test]
fn zero_delta_git_branch_has_no_carrier_file_or_reservation_artifact() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  inspect:
    kind: cmd
    command:
      argv: ["true"]
    outputs:
      changes:
        kind: git_branch
        from: workspace
exports:
  changes:
    ref: outputs.inspect.changes
"#,
    );
    initialize_git_repository(bundle.execution_root());
    let destination = bundle.result("git-zero-delta");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let output = run(&args);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result = result_json(&destination);
    let branch = &result["exports"]["changes"];
    assert_eq!(branch["kind"], "git_branch");
    assert_eq!(branch["baseOid"], branch["headOid"]);
    assert!(branch.get("carrier").is_none());
    let artifact = attempt_result(&destination);
    assert_eq!(fs::read_dir(artifact.join("exports")).unwrap().count(), 0);
    let validation = run(&[
        "artifact".to_owned(),
        "validate".to_owned(),
        "--json".to_owned(),
        artifact.to_string_lossy().into_owned(),
    ]);
    assert!(
        validation.status.success(),
        "zero-delta artifact validation failed: {}",
        String::from_utf8_lossy(&validation.stdout)
    );
}

#[test]
fn semantic_outputs_workspace_finalizer_success() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  ordinary:
    kind: cmd
    command:
      argv: ["true"]
finalizers:
  publish:
    kind: cmd
    when: [succeeded]
    command:
      argv: ["/bin/sh", "-c", "set -eu; printf 'finalizer change\\n' > tracked.txt; git add tracked.txt; git commit --quiet -m finalizer-change"]
    outputs:
      changes:
        kind: git_branch
        from: workspace
exports:
  changes:
    ref: outputs.publish.changes
"#,
    );
    initialize_git_repository(bundle.execution_root());
    let destination = bundle.result("finalizer-git-branch");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let output = run(&args);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result = result_json(&destination);
    let branch = &result["exports"]["changes"];
    assert_eq!(branch["kind"], "git_branch");
    assert_ne!(branch["baseOid"], branch["headOid"]);
    assert!(branch.get("from").is_none());
    assert_eq!(branch["carrier"]["path"], "exports/0001");
}

#[test]
fn semantic_outputs_workspace_agent_success() {
    let execution = format!(
        "printf 'agent change\\n' > tracked.txt; git add tracked.txt; git commit --quiet -m agent-change; {}",
        response_pi_execution()
    );
    let pi = PiFixture::with_execution("0.84.2", COMPLETE_HELP, true, &execution);
    let inherited_path = std::env::var_os("PATH").unwrap_or_default();
    let path = std::env::join_paths(
        std::iter::once(pi.path_directory().to_path_buf())
            .chain(std::env::split_paths(&inherited_path)),
    )
    .unwrap();
    let bundle = RunBundle::new(git_branch_agent_source());
    bundle.write_source("system.md", "system");
    bundle.write_source("message.md", "prompt");
    initialize_git_repository(bundle.execution_root());
    let destination = bundle.result("agent-git-branch");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let output = isolated_command(&args).env("PATH", path).output().unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result = result_json(&destination);
    let branch = &result["exports"]["changes"];
    assert_eq!(branch["kind"], "git_branch");
    assert_ne!(branch["baseOid"], branch["headOid"]);
    assert_eq!(branch["carrier"]["path"], "exports/0001");
}

#[test]
fn agent_installation_rejections_use_inherited_path_order_without_publication() {
    let missing_bundle = RunBundle::new(response_agent_source());
    missing_bundle.write_source("system.md", "system");
    missing_bundle.write_source("message.md", "prompt");
    let missing_path = tempfile::tempdir().unwrap();
    let missing_destination = missing_bundle.result("missing");
    let mut missing_args = missing_bundle.args(&missing_destination);
    missing_args.insert(missing_args.len() - 1, "--json".to_owned());
    let missing = isolated_command(&missing_args)
        .env("PATH", missing_path.path())
        .output()
        .unwrap();

    assert_eq!(missing.status.code(), Some(1));
    assert!(missing.stderr.is_empty());
    let terminal: serde_json::Value = serde_json::from_slice(&missing.stdout).unwrap();
    assert_eq!(terminal["outcome"], "rejected");
    assert_eq!(terminal["phase"], "installation");
    assert_eq!(
        terminal["diagnostics"][0]["code"],
        "missing_pi_installation"
    );
    assert_eq!(
        terminal["diagnostics"][0]["location"],
        serde_json::json!({"kind": "agent_harness", "profile": "PiJsonV1"})
    );
    assert!(!missing_destination.exists());

    let incompatible = PiFixture::new("0.84.0", COMPLETE_HELP, true);
    let fallback = PiFixture::new("0.84.2", COMPLETE_HELP, true);
    let ordered_path =
        std::env::join_paths([incompatible.path_directory(), fallback.path_directory()]).unwrap();
    let incompatible_bundle = RunBundle::new(response_agent_source());
    incompatible_bundle.write_source("system.md", "system");
    incompatible_bundle.write_source("message.md", "prompt");
    let incompatible_destination = incompatible_bundle.result("incompatible");
    let output = isolated_command(&incompatible_bundle.args(&incompatible_destination))
        .env("PATH", ordered_path)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unsupported_pi_version"));
    assert_eq!(incompatible.recorded_probes(), b"--version\n");
    assert!(fallback.recorded_probes().is_empty());
    assert!(!incompatible_destination.exists());
}

#[test]
fn claude_code_only_run_pins_the_validated_executable_without_pi_or_path_fallback() {
    let replacement =
        ClaudeCodeFixture::new("2.1.241 (Claude Code)", CLAUDE_CODE_COMPLETE_HELP, false);
    let mut probe_barrier = AgentBarrierFixture::new();
    let capability_hook = format!(
        "printf '\\001' > {}; IFS= read -r _ < {}",
        quote(probe_barrier.ready_path.to_str().unwrap()),
        quote(probe_barrier.release_path.to_str().unwrap()),
    );
    let execution = response_claude_code_execution_for_version("2.1.235");
    let claude = ClaudeCodeFixture::with_execution_and_capability_hook(
        "2.1.235 (Claude Code)",
        CLAUDE_CODE_COMPLETE_HELP,
        true,
        &execution,
        &capability_hook,
    );
    let ordered_path =
        fixture_path_with_host_tools(&[replacement.path_directory(), claude.path_directory()]);
    let bundle = RunBundle::new(response_claude_code_agent_source());
    bundle.write_source("system.md", "system");
    bundle.write_source("message.md", "prompt");
    let runner_resources = [
        (
            bundle.claude_config().join("settings.json"),
            b"runner settings sentinel".as_slice(),
        ),
        (
            bundle.claude_config().join("skills/diagnostic/SKILL.md"),
            b"runner skill sentinel".as_slice(),
        ),
        (
            bundle.execution_root().join("CLAUDE.md"),
            b"project instruction sentinel".as_slice(),
        ),
        (
            bundle.execution_root().join(".mcp.json"),
            b"project MCP sentinel".as_slice(),
        ),
        (
            bundle.execution_root().join(".claude/settings.json"),
            b"project hook and plugin sentinel".as_slice(),
        ),
    ];
    for (path, bytes) in &runner_resources {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
    let destination = bundle.result("claude-response");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let child = isolated_command(&args)
        .env("PATH", ordered_path)
        .env("CLAUDE_CONFIG_DIR", bundle.claude_config())
        .env("ANTHROPIC_API_KEY", "synthetic-credential-sentinel")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    probe_barrier.wait_until_started();
    fs::set_permissions(replacement.executable(), fs::Permissions::from_mode(0o755)).unwrap();
    probe_barrier.release_observation();
    let output = child.wait_with_output().unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(replacement.recorded_probes().is_empty());
    let probes = String::from_utf8(claude.recorded_probes()).unwrap();
    let lines = probes.lines().collect::<Vec<_>>();
    assert_eq!(lines[0], "--version");
    assert_eq!(lines[1], "--help");
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.starts_with("-p --input-format stream-json"))
            .count(),
        1
    );
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0001")).unwrap(),
        b"claude response"
    );
    let metadata_root = destination.join("attempts/000001/diagnostics/claude-code-stream-json-v1");
    let diagnostic = fs::read_dir(metadata_root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let metadata_bytes = fs::read(diagnostic.join("metadata.json")).unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&metadata_bytes).unwrap();
    assert_eq!(metadata["profile"], "ClaudeCodeStreamJsonV1");
    assert_eq!(metadata["claudeCodeVersion"], "2.1.235");
    assert_eq!(
        metadata["nativeSession"],
        serde_json::json!({"relativeDirectory": "session", "formatVersion": 1})
    );
    assert!(diagnostic.join("session/resources").is_dir());
    assert_eq!(
        fs::read(diagnostic.join("session/transcript.jsonl")).unwrap(),
        b"{malformed retained transcript"
    );
    assert!(metadata.get("environment").is_none());
    assert!(!String::from_utf8_lossy(&metadata_bytes).contains("synthetic-credential-sentinel"));
    assert!(
        !String::from_utf8_lossy(&metadata_bytes)
            .contains(bundle.claude_config().to_str().unwrap())
    );
    fs::remove_file(diagnostic.join("session/transcript.jsonl")).unwrap();
    let status = isolated_command(&[
        "workflow".to_owned(),
        "status".to_owned(),
        destination.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["state"]["attempts"][0]["state"], "succeeded");
    assert_eq!(status["recovery"]["status"], "settled");
    assert_eq!(status["retry"]["eligible"], false);
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0001")).unwrap(),
        b"claude response"
    );
    for (path, expected) in runner_resources {
        assert_eq!(fs::read(path).unwrap(), expected);
    }
    let ambient_projects = bundle.claude_config().join("projects");
    for project in fs::read_dir(ambient_projects).unwrap() {
        assert_eq!(fs::read_dir(project.unwrap().path()).unwrap().count(), 0);
    }
}

#[test]
fn local_claude_execution_rejects_a_version_that_contradicts_the_validated_snapshot() {
    let claude = ClaudeCodeFixture::with_execution(
        "2.1.235 (Claude Code)",
        CLAUDE_CODE_COMPLETE_HELP,
        true,
        response_claude_code_execution(),
    );
    let bundle = RunBundle::new(response_claude_code_agent_source());
    bundle.write_source("system.md", "system");
    bundle.write_source("message.md", "prompt");
    let destination = bundle.result("claude-version-mismatch");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let output = isolated_command(&args)
        .env(
            "PATH",
            fixture_path_with_host_tools(&[claude.path_directory()]),
        )
        .env("CLAUDE_CONFIG_DIR", bundle.claude_config())
        .output()
        .unwrap();

    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["result"]["outcome"], "failed");
    assert_eq!(
        terminal["result"]["primaryFailure"]["cause"]["code"],
        "harness_start_failed"
    );
    assert_eq!(
        terminal["result"]["primaryFailure"]["cause"]["protocolRejection"]["profile"],
        "ClaudeCodeStreamJsonV1"
    );
    assert_eq!(
        terminal["result"]["primaryFailure"]["cause"]["protocolRejection"]["detail"]["reason"],
        "initialization_invalid"
    );
    assert_eq!(
        terminal["result"]["exports"]["response"]["state"],
        "unavailable"
    );
    assert!(!attempt_result(&destination).join("exports/0001").exists());
}

#[test]
fn local_admission_probes_only_the_harness_selected_by_the_workflow() {
    let pi = PiFixture::new("0.84.2", COMPLETE_HELP, true);
    let claude = ClaudeCodeFixture::new("2.1.241 (Claude Code)", CLAUDE_CODE_COMPLETE_HELP, true);
    let codex = CodexFixture::with_execution("0.147.0");

    let claude_bundle = RunBundle::new(response_claude_code_agent_source());
    claude_bundle.write_source("system.md", "system");
    claude_bundle.write_source("message.md", "prompt");
    let claude_destination = claude_bundle.result("missing-claude");
    let mut claude_args = claude_bundle.args(&claude_destination);
    claude_args.insert(claude_args.len() - 1, "--json".to_owned());
    let missing_claude = isolated_command(&claude_args)
        .env("PATH", pi.path_directory())
        .output()
        .unwrap();
    assert_eq!(missing_claude.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&missing_claude.stdout).unwrap()["diagnostics"]
            [0]["code"],
        "missing_claude_code_installation"
    );
    assert!(pi.recorded_probes().is_empty());
    assert!(!claude_destination.exists());

    let pi_bundle = RunBundle::new(response_agent_source());
    pi_bundle.write_source("system.md", "system");
    pi_bundle.write_source("message.md", "prompt");
    let pi_destination = pi_bundle.result("missing-pi");
    let mut pi_args = pi_bundle.args(&pi_destination);
    pi_args.insert(pi_args.len() - 1, "--json".to_owned());
    let missing_pi = isolated_command(&pi_args)
        .env("PATH", claude.path_directory())
        .output()
        .unwrap();
    assert_eq!(missing_pi.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&missing_pi.stdout).unwrap()["diagnostics"][0]
            ["code"],
        "missing_pi_installation"
    );
    assert!(claude.recorded_probes().is_empty());
    assert!(!pi_destination.exists());

    let codex_bundle = RunBundle::new(response_codex_agent_source());
    codex_bundle.write_source("system.md", "system");
    codex_bundle.write_source("message.md", "prompt");
    let codex_destination = codex_bundle.result("missing-codex");
    let mut codex_args = codex_bundle.args(&codex_destination);
    codex_args.insert(codex_args.len() - 1, "--json".to_owned());
    let pi_and_claude =
        std::env::join_paths([pi.path_directory(), claude.path_directory()]).unwrap();
    let missing_codex = isolated_command(&codex_args)
        .env("PATH", pi_and_claude)
        .output()
        .unwrap();
    assert_eq!(missing_codex.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&missing_codex.stdout).unwrap()["diagnostics"]
            [0]["code"],
        "missing_codex_installation"
    );
    assert!(pi.recorded_probes().is_empty());
    assert!(claude.recorded_probes().is_empty());
    assert!(!codex_destination.exists());

    let codex_only_path = codex.path_directory();
    let unrelated_pi_bundle = RunBundle::new(response_agent_source());
    unrelated_pi_bundle.write_source("system.md", "system");
    unrelated_pi_bundle.write_source("message.md", "prompt");
    let unrelated_pi_destination = unrelated_pi_bundle.result("codex-must-not-substitute-pi");
    let mut unrelated_pi_args = unrelated_pi_bundle.args(&unrelated_pi_destination);
    unrelated_pi_args.insert(unrelated_pi_args.len() - 1, "--json".to_owned());
    let missing_pi_with_codex_available = isolated_command(&unrelated_pi_args)
        .env("PATH", codex_only_path)
        .output()
        .unwrap();
    assert_eq!(missing_pi_with_codex_available.status.code(), Some(1));
    assert!(codex.recorded_probes().is_empty());
    assert!(!unrelated_pi_destination.exists());

    let incompatible_claude =
        ClaudeCodeFixture::new("2.1.221 (Claude Code)", CLAUDE_CODE_COMPLETE_HELP, true);
    let available_pi =
        PiFixture::with_execution("0.84.2", COMPLETE_HELP, true, &response_pi_execution());
    let path = std::env::join_paths([
        incompatible_claude.path_directory(),
        available_pi.path_directory(),
    ])
    .unwrap();
    let incompatible_bundle = RunBundle::new(response_claude_code_agent_source());
    incompatible_bundle.write_source("system.md", "system");
    incompatible_bundle.write_source("message.md", "prompt");
    let incompatible_destination = incompatible_bundle.result("incompatible-claude");
    let mut incompatible_args = incompatible_bundle.args(&incompatible_destination);
    incompatible_args.insert(incompatible_args.len() - 1, "--json".to_owned());
    let incompatible = isolated_command(&incompatible_args)
        .env("PATH", path)
        .output()
        .unwrap();
    assert_eq!(incompatible.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&incompatible.stdout).unwrap()["diagnostics"]
            [0]["code"],
        "unsupported_claude_code_version"
    );
    assert_eq!(incompatible_claude.recorded_probes(), b"--version\n");
    assert!(available_pi.recorded_probes().is_empty());
    assert!(!incompatible_destination.exists());
}

fn run_codex_scenario(
    bundle: &RunBundle,
    fixture: &CodexFixture,
    destination: &Path,
    scenario: &str,
) -> Output {
    let mut args = bundle.args(destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let codex_home = bundle.initial_cwd().join(format!("codex-home-{scenario}"));
    fs::create_dir(&codex_home).unwrap();
    let codex_home = fs::canonicalize(codex_home).unwrap();
    isolated_command(&args)
        .env(
            "PATH",
            fixture_path_with_host_tools(&[fixture.path_directory()]),
        )
        .env("CODEX_HOME", codex_home)
        .env("CODEX_FIXTURE_HELPER", std::env::current_exe().unwrap())
        .env("CODEX_LOCAL_SCENARIO", scenario)
        .output()
        .unwrap()
}

#[test]
fn codex_only_response_and_no_value_use_the_selected_production_dispatcher() {
    for (scenario, source) in [
        ("response", response_codex_agent_source()),
        ("no-value", no_value_codex_agent_source()),
    ] {
        let fixture = CodexFixture::with_execution("0.147.0");
        let bundle = RunBundle::new(source);
        bundle.write_source("system.md", "system");
        bundle.write_source("message.md", "prompt");
        let destination = bundle.result(scenario);

        let output = run_codex_scenario(&bundle, &fixture, &destination, scenario);

        assert!(
            output.status.success(),
            "{scenario}\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let result = result_json(&destination);
        assert_eq!(result["outcome"], "succeeded");
        if scenario == "response" {
            let relative = result["exports"]["response"]["path"].as_str().unwrap();
            assert_eq!(
                fs::read(attempt_result(&destination).join(relative)).unwrap(),
                b"codex response"
            );
        }
        let diagnostic_root = destination.join("attempts/000001/diagnostics/codex-app-server-v1");
        let invocation = fs::read_dir(diagnostic_root)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(invocation.join("metadata.json")).unwrap()).unwrap();
        assert_eq!(metadata["profile"], "CodexAppServerV1");
        assert_eq!(metadata["codexVersion"], "0.147.0");
        assert!(!invocation.join("session").exists());
        if scenario == "response" {
            let status = isolated_command(&[
                "workflow".to_owned(),
                "status".to_owned(),
                destination.to_string_lossy().into_owned(),
                "--json".to_owned(),
            ])
            .output()
            .unwrap();
            assert!(status.status.success());
            let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
            assert_eq!(status["state"]["attempts"][0]["state"], "succeeded");
            assert_eq!(status["recovery"]["status"], "settled");
        }
        assert!(String::from_utf8_lossy(&output.stderr).contains("codex fixture diagnostic"));
        let probes = String::from_utf8(fixture.recorded_probes()).unwrap();
        assert_eq!(probes.matches("--version").count(), 1);
        assert_eq!(probes.matches("generate-json-schema").count(), 1);
        assert_eq!(probes.matches("app-server --strict-config").count(), 1);
    }
}

#[test]
fn codex_result_correction_publishes_only_the_schema_valid_value() {
    let fixture = CodexFixture::with_execution("0.147.0");
    let bundle = RunBundle::new(result_codex_agent_source());
    bundle.write_source("system.md", "system");
    bundle.write_source("message.md", "prompt");
    bundle.write_source(
        "result.schema.json",
        br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"integer","minimum":1}"#,
    );
    let destination = bundle.result("codex-correction");

    let output = run_codex_scenario(&bundle, &fixture, &destination, "result-correction");

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result = result_json(&destination);
    let relative = result["exports"]["result"]["path"].as_str().unwrap();
    assert_eq!(
        fs::read(attempt_result(&destination).join(relative)).unwrap(),
        b"7"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("value_rejected"));
}

#[test]
fn codex_native_failure_is_attributed_without_value_publication_or_fallback() {
    let fixture = CodexFixture::with_execution("0.147.0");
    let bundle = RunBundle::new(response_codex_agent_source());
    bundle.write_source("system.md", "system");
    bundle.write_source("message.md", "prompt");
    let destination = bundle.result("codex-native-failure");

    let output = run_codex_scenario(&bundle, &fixture, &destination, "native-failure");

    assert_eq!(output.status.code(), Some(1));
    let result = result_json(&destination);
    assert_eq!(result["outcome"], "failed");
    assert_eq!(
        result["steps"][0]["failure"]["cause"]["code"],
        "harness_failed"
    );
    assert_eq!(
        result["exports"]["response"],
        serde_json::json!({"state": "unavailable", "reason": "source_failed"})
    );
    assert!(!attempt_result(&destination).join("exports/0001").exists());
    assert_eq!(
        String::from_utf8(fixture.recorded_probes())
            .unwrap()
            .matches("app-server --strict-config")
            .count(),
        1
    );
}

#[test]
fn codex_cancellation_quiesces_before_atomic_publication() {
    let fixture = CodexFixture::with_execution("0.147.0");
    let bundle = RunBundle::new(response_codex_agent_source());
    bundle.write_source("system.md", "system");
    bundle.write_source("message.md", "prompt");
    let destination = bundle.result("codex-cancelled");
    let ready = bundle.initial_cwd().join("codex-ready");
    let codex_home = bundle.initial_cwd().join("codex-home-cancellation");
    fs::create_dir(&codex_home).unwrap();
    let codex_home = fs::canonicalize(codex_home).unwrap();
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let child = isolated_command(&args)
        .env(
            "PATH",
            fixture_path_with_host_tools(&[fixture.path_directory()]),
        )
        .env("CODEX_HOME", codex_home)
        .env("CODEX_FIXTURE_HELPER", std::env::current_exe().unwrap())
        .env("CODEX_LOCAL_SCENARIO", "cancellation")
        .env("CODEX_LOCAL_READY", &ready)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    poll_until(
        "Codex cancellation readiness",
        || ready.is_file(),
        |ready| *ready,
    );
    assert!(!attempt_result(&destination).exists());
    kill_process(
        Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        Signal::INT,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();

    assert_eq!(output.status.code(), Some(130));
    let result = result_json(&destination);
    assert_eq!(result["outcome"], "cancelled");
    assert_eq!(
        result["exports"]["response"],
        serde_json::json!({"state": "unavailable", "reason": "source_cancelled"})
    );
    assert!(
        fs::read_dir(destination.join(".private"))
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn mixed_local_run_invokes_each_harness_once_and_publishes_both_exports() {
    let pi = PiFixture::with_execution("0.84.2", COMPLETE_HELP, true, &response_pi_execution());
    let claude = ClaudeCodeFixture::with_execution(
        "2.1.241 (Claude Code)",
        CLAUDE_CODE_COMPLETE_HELP,
        true,
        response_claude_code_execution(),
    );
    let path = fixture_path_with_host_tools(&[pi.path_directory(), claude.path_directory()]);
    let bundle = RunBundle::new(mixed_harness_source());
    for path in ["pi-system.md", "claude-system.md", "message.md"] {
        bundle.write_source(path, "fixture");
    }
    let destination = bundle.result("mixed-harnesses");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let output = isolated_command(&args)
        .env("PATH", path)
        .env("CLAUDE_CONFIG_DIR", bundle.claude_config())
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let pi_probes = String::from_utf8(pi.recorded_probes()).unwrap();
    assert_eq!(pi_probes.matches("--mode json").count(), 1);
    let claude_probes = String::from_utf8(claude.recorded_probes()).unwrap();
    assert_eq!(
        claude_probes
            .lines()
            .filter(|line| line.starts_with("-p --input-format stream-json"))
            .count(),
        1
    );
    let result = result_json(&destination);
    assert_eq!(result["outcome"], "succeeded");
    let steps = result["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 2);
    assert!(steps.iter().all(|step| step["state"] == "succeeded"));
    let transcript = String::from_utf8(output.stderr).unwrap();
    assert!(transcript.contains("event       assistant"));
    assert!(!transcript.contains("stream_event"));
    assert!(!transcript.contains("00000000-0000-4000-8000-00000000000"));
    let result_root = attempt_result(&destination);
    for (name, expected) in [
        ("claudeResponse", b"claude response".as_slice()),
        ("piResponse", b"hello world".as_slice()),
    ] {
        assert_eq!(result["exports"][name]["state"], "available");
        let path = result["exports"][name]["path"].as_str().unwrap();
        assert_eq!(fs::read(result_root.join(path)).unwrap(), expected);
    }
    assert!(
        fs::read_dir(destination.join(".private"))
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn mixed_pi_claude_and_codex_run_preserves_independent_dispatch_and_atomic_exports() {
    let pi = PiFixture::with_execution("0.84.2", COMPLETE_HELP, true, &response_pi_execution());
    let claude = ClaudeCodeFixture::with_execution(
        "2.1.241 (Claude Code)",
        CLAUDE_CODE_COMPLETE_HELP,
        true,
        response_claude_code_execution(),
    );
    let codex = CodexFixture::with_execution("0.147.0");
    let path = fixture_path_with_host_tools(&[
        pi.path_directory(),
        claude.path_directory(),
        codex.path_directory(),
    ]);
    let bundle = RunBundle::new(mixed_codex_harness_source());
    for source in [
        "pi-system.md",
        "claude-system.md",
        "codex-system.md",
        "message.md",
    ] {
        bundle.write_source(source, "fixture");
    }
    let codex_home = bundle.initial_cwd().join("codex-home-mixed");
    fs::create_dir(&codex_home).unwrap();
    let codex_home = fs::canonicalize(codex_home).unwrap();
    let destination = bundle.result("mixed-with-codex");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let output = isolated_command(&args)
        .env("PATH", path)
        .env("CLAUDE_CONFIG_DIR", bundle.claude_config())
        .env("CODEX_HOME", codex_home)
        .env("CODEX_FIXTURE_HELPER", std::env::current_exe().unwrap())
        .env("CODEX_LOCAL_SCENARIO", "response")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(pi.recorded_probes())
            .unwrap()
            .matches("--mode json")
            .count(),
        1
    );
    assert_eq!(
        String::from_utf8(claude.recorded_probes())
            .unwrap()
            .lines()
            .filter(|line| line.starts_with("-p --input-format stream-json"))
            .count(),
        1
    );
    assert_eq!(
        String::from_utf8(codex.recorded_probes())
            .unwrap()
            .matches("app-server --strict-config")
            .count(),
        1
    );
    let transcript = String::from_utf8_lossy(&output.stderr);
    assert!(transcript.contains("codexLocal · codex · fixture/codex · effort=high"));
    let result = result_json(&destination);
    assert_eq!(result["outcome"], "succeeded");
    let result_root = attempt_result(&destination);
    for (name, expected) in [
        ("claudeResponse", b"claude response".as_slice()),
        ("codexResponse", b"codex response".as_slice()),
        ("piResponse", b"hello world".as_slice()),
    ] {
        let relative = result["exports"][name]["path"].as_str().unwrap();
        assert_eq!(fs::read(result_root.join(relative)).unwrap(), expected);
    }
}

#[test]
fn local_claude_result_correction_publishes_only_the_authoritatively_valid_value() {
    let claude = ClaudeCodeFixture::with_execution(
        "2.1.241 (Claude Code)",
        CLAUDE_CODE_COMPLETE_HELP,
        true,
        corrected_claude_code_result_execution(),
    );
    let bundle = RunBundle::new(result_claude_code_agent_source());
    bundle.write_source("system.md", "system");
    bundle.write_source("message.md", "prompt");
    bundle.write_source(
        "result.schema.json",
        br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"integer","minimum":1}"#,
    );
    let destination = bundle.result("corrected-result");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());

    let path = fixture_path_with_host_tools(&[claude.path_directory()]);
    let output = isolated_command(&args)
        .env("PATH", path)
        .env("CLAUDE_CONFIG_DIR", bundle.claude_config())
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result = result_json(&destination);
    assert_eq!(result["outcome"], "succeeded");
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0001")).unwrap(),
        b"7"
    );
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("value_rejected")
    );
}

#[test]
fn mixed_local_failure_and_cancellation_publish_only_after_quiescence() {
    for (mode, expected_status, expected_outcome, unavailable_reason) in [
        ("failure", 1, "failed", "source_failed"),
        ("cancellation", 130, "cancelled", "source_cancelled"),
    ] {
        let pi = PiFixture::with_execution("0.84.2", COMPLETE_HELP, true, &response_pi_execution());
        let claude = ClaudeCodeFixture::with_execution(
            "2.1.241 (Claude Code)",
            CLAUDE_CODE_COMPLETE_HELP,
            true,
            blocked_claude_code_execution(),
        );
        let path = fixture_path_with_host_tools(&[pi.path_directory(), claude.path_directory()]);
        let bundle = RunBundle::new(mixed_harness_source());
        for path in ["pi-system.md", "claude-system.md", "message.md"] {
            bundle.write_source(path, "fixture");
        }
        let destination = bundle.result(mode);
        let mut args = bundle.args(&destination);
        args.insert(args.len() - 1, "--json".to_owned());
        let mut barrier = AgentBarrierFixture::new();
        let process_pid = bundle.initial_cwd().join(format!("{mode}-claude.pid"));
        let child = isolated_command(&args)
            .env("PATH", path)
            .env("CLAUDE_CONFIG_DIR", bundle.claude_config())
            .env("WORKFLOW_READY_FIFO", &barrier.ready_path)
            .env("WORKFLOW_RELEASE_FIFO", &barrier.release_path)
            .env("CLAUDE_FIXTURE_PID", &process_pid)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        barrier.wait_until_started();
        assert_eq!(
            String::from_utf8(pi.recorded_probes())
                .unwrap()
                .matches("--mode json")
                .count(),
            1
        );
        assert_eq!(
            String::from_utf8(claude.recorded_probes())
                .unwrap()
                .lines()
                .filter(|line| line.starts_with("-p --input-format stream-json"))
                .count(),
            1
        );
        assert!(
            !attempt_result(&destination).exists(),
            "a provisional mixed result became visible before {mode} settled"
        );

        if mode == "failure" {
            barrier.release_observation();
        } else {
            let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
            kill_process(pid, Signal::INT).unwrap();
        }
        let output = child.wait_with_output().unwrap();

        assert_eq!(output.status.code(), Some(expected_status));
        let result = result_json(&destination);
        assert_eq!(result["outcome"], expected_outcome);
        assert_eq!(result["exports"]["piResponse"]["state"], "available");
        assert_eq!(
            result["exports"]["claudeResponse"],
            serde_json::json!({"state": "unavailable", "reason": unavailable_reason})
        );
        let claude_pid = fs::read_to_string(&process_pid)
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap();
        assert_eq!(
            test_kill_process(Pid::from_raw(claude_pid).unwrap()),
            Err(rustix::io::Errno::SRCH),
            "the contained Claude process survived {mode} publication"
        );
        assert!(
            fs::read_dir(destination.join(".private"))
                .unwrap()
                .next()
                .is_none()
        );
    }
}

#[test]
fn compatible_path_pi_is_validated_once_and_pinned_after_path_order_changes() {
    let replacement = PiFixture::new("0.84.2", COMPLETE_HELP, false);
    let mut probe_barrier = AgentBarrierFixture::new();
    let capability_hook = format!(
        "printf '\\001' > {}; IFS= read -r _ < {}",
        quote(probe_barrier.ready_path.to_str().unwrap()),
        quote(probe_barrier.release_path.to_str().unwrap()),
    );
    let pi = PiFixture::with_execution_and_capability_hook(
        "0.84.2",
        COMPLETE_HELP,
        true,
        &response_pi_execution(),
        &capability_hook,
    );
    let ordered_path =
        std::env::join_paths([replacement.path_directory(), pi.path_directory()]).unwrap();
    let bundle = RunBundle::new(response_agent_source());
    bundle.write_source("system.md", "system");
    bundle.write_source("message.md", "prompt");
    let destination = bundle.result("agent-response");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let child = isolated_command(&args)
        .env("PATH", ordered_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    probe_barrier.wait_until_started();
    fs::set_permissions(
        Path::new(replacement.executable()),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    probe_barrier.release_observation();
    let output = child.wait_with_output().unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let probes = pi.recorded_probes();
    let probes = String::from_utf8(probes).unwrap();
    let lines = probes.lines().collect::<Vec<_>>();
    assert_eq!(probes.matches("--mode json").count(), 1);
    assert!(lines.len() >= 3);
    assert_eq!(lines[0], "--version");
    assert_eq!(
        lines[1],
        "--no-approve --no-extensions --no-skills --no-prompt-templates --no-themes --no-context-files --help"
    );
    assert!(lines[2].starts_with("--mode json --approve --session-dir /"));
    for forbidden in [
        "--continue",
        "--resume",
        "--session",
        "--session-id",
        "--fork",
        "--no-session",
    ] {
        assert!(
            !lines[2]
                .split_whitespace()
                .any(|argument| argument == forbidden)
        );
    }
    assert!(replacement.recorded_probes().is_empty());

    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["outcome"], "succeeded");
    assert_eq!(terminal["result"]["steps"][0]["kind"], "agent");
    assert_eq!(terminal["result"]["exports"]["response"]["kind"], "text");
    assert_eq!(
        terminal["result"]["exports"]["response"]["mediaType"],
        "text/plain; charset=utf-8"
    );
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0001")).unwrap(),
        b"hello world"
    );
    assert!(
        fs::read_dir(destination.join(".private"))
            .unwrap()
            .next()
            .is_none()
    );
    let diagnostic_root = destination.join("attempts/000001/diagnostics/pi-json-v1");
    let invocation_directories = fs::read_dir(&diagnostic_root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(invocation_directories.len(), 1);
    let diagnostic = &invocation_directories[0];
    let metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(diagnostic.join("metadata.json")).unwrap()).unwrap();
    let run: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("run.json")).unwrap()).unwrap();
    assert_eq!(metadata["localRunId"], run["localRunId"]);
    assert_eq!(metadata["attemptNumber"], 1);
    assert_eq!(metadata["stepId"], "answer");
    assert!(
        metadata["invocationId"]
            .as_u64()
            .is_some_and(|value| value > 0)
    );
    assert_eq!(metadata["profile"], "PiJsonV1");
    assert_eq!(metadata["piVersion"], "0.84.2");
    assert_eq!(
        metadata["nativeSession"],
        serde_json::json!({"relativeDirectory": "session", "formatVersion": 3})
    );
    assert!(metadata.get("environment").is_none());
    assert_eq!(
        fs::read(diagnostic.join("session/retained-partial.jsonl")).unwrap(),
        b"{partial"
    );
    assert_eq!(result_json(&destination)["outcome"], "succeeded");
    let live = String::from_utf8(output.stderr).unwrap();
    assert!(live.contains("event       assistant · hello"));
    assert!(live.contains("event       usage · input 1 · output 2"));
}

struct AgentBarrierFixture {
    _directory: TempDir,
    ready: fs::File,
    release: fs::File,
    hold: fs::File,
    ready_path: PathBuf,
    release_path: PathBuf,
    hold_path: PathBuf,
}

impl AgentBarrierFixture {
    fn new() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("scherzo-agent-barrier-")
            .tempdir_in("/tmp")
            .unwrap();
        let ready_path = directory.path().join("ready");
        let release_path = directory.path().join("release");
        let hold_path = directory.path().join("hold");
        for path in [&ready_path, &release_path, &hold_path] {
            mkfifo(path, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        }
        let open_control = |path: &Path| {
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap()
        };
        Self {
            ready: open_control(&ready_path),
            release: open_control(&release_path),
            hold: open_control(&hold_path),
            ready_path,
            release_path,
            hold_path,
            _directory: directory,
        }
    }

    fn command(&self, args: &[String], pi: &PiFixture) -> Command {
        let mut command = isolated_command(args);
        command
            .env("PATH", pi.path_directory())
            .env("WORKFLOW_READY_FIFO", &self.ready_path)
            .env("WORKFLOW_RELEASE_FIFO", &self.release_path)
            .env("WORKFLOW_HOLD_FIFO", &self.hold_path);
        command
    }

    fn wait_until_started(&mut self) {
        let mut ready = [0_u8; 1];
        self.ready.read_exact(&mut ready).unwrap();
        assert_eq!(ready, [1]);
    }

    fn release_observation(&mut self) {
        self.release.write_all(b"go\n").unwrap();
    }
}

fn barrier_pi_execution() -> &'static str {
    r#"trap 'exit 130' INT TERM
cwd=$(pwd)
printf '{"type":"session","version":3,"id":"00000000-0000-4000-8000-000000000001","timestamp":"2026-07-30T12:00:00Z","cwd":"%s"}\n' "$cwd"
printf '%s\n' '{"type":"agent_start"}'
printf '\001' > "$WORKFLOW_READY_FIFO"
IFS= read -r _ < "$WORKFLOW_RELEASE_FIFO"
printf '%s\n' '{"type":"fixture_observation","payload":"typed"}'
IFS= read -r _ < "$WORKFLOW_HOLD_FIFO""#
}

#[test]
fn running_agent_process_group_is_durably_guarded() {
    let pi = PiFixture::with_execution("0.84.2", COMPLETE_HELP, true, barrier_pi_execution());
    let bundle = RunBundle::new(response_agent_source());
    bundle.write_source("system.md", "system");
    bundle.write_source("message.md", "prompt");
    let destination = bundle.result("agent-process-guard");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let mut barrier = AgentBarrierFixture::new();
    let child = barrier
        .command(&args, &pi)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    barrier.wait_until_started();

    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("state.json")).unwrap()).unwrap();
    let guards = state["attempts"][0]["processGuards"]
        .as_array()
        .unwrap()
        .clone();

    let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(pid, Signal::INT).unwrap();
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(130));

    assert_eq!(guards.len(), 1, "a running Pi group must be recoverable");
    assert_eq!(guards[0]["stepId"], "answer");
    assert!(matches!(
        guards[0]["state"].as_str(),
        Some("prepared" | "released")
    ));
}

#[test]
fn agent_signal_and_output_failure_cancel_and_quiesce_the_pi_group() {
    for (mode, expected_reason, expected_status) in [
        ("signal", "user_request", 130),
        ("output", "caller_output_failure", 1),
    ] {
        let pi = PiFixture::with_execution("0.84.2", COMPLETE_HELP, true, barrier_pi_execution());
        let bundle = RunBundle::new(response_agent_source());
        bundle.write_source("system.md", "system");
        bundle.write_source("message.md", "prompt");
        let destination = bundle.result(mode);
        let mut args = bundle.args(&destination);
        args.insert(
            args.len() - 1,
            if mode == "signal" {
                "--json"
            } else {
                "--plain"
            }
            .to_owned(),
        );
        let mut barrier = AgentBarrierFixture::new();
        let mut child = barrier
            .command(&args, &pi)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        barrier.wait_until_started();

        if mode == "signal" {
            let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
            kill_process(pid, Signal::INT).unwrap();
        } else {
            drop(child.stdout.take());
            barrier.release_observation();
        }
        let output = child.wait_with_output().unwrap();

        assert_eq!(output.status.code(), Some(expected_status));
        let retained = result_json(&destination);
        assert_eq!(retained["outcome"], "cancelled");
        assert_eq!(retained["cancellation"]["reason"], expected_reason);
        let state: serde_json::Value =
            serde_json::from_slice(&fs::read(destination.join("state.json")).unwrap()).unwrap();
        assert_eq!(state["attempts"][0]["state"], "cancelled");
        assert_eq!(
            state["attempts"][0]["progress"]["outstandingActions"],
            serde_json::json!([])
        );
        assert_eq!(state["attempts"][0]["result"]["status"], "published");
        assert!(barrier.hold.metadata().unwrap().file_type().is_fifo());
        if mode == "signal" {
            let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(terminal["exitStatus"], 130);
        }
    }
}

fn mixed_agent_source() -> &'static str {
    r#"schemaVersion: 1
agentProfiles:
  local:
    harness:
      kind: pi
      config:
        model: scherzo-fake/conformance
        thinking: xhigh
steps:
  seed:
    kind: cmd
    command:
      argv: ["/bin/sh", "-c", "printf seed > seed.txt"]
    outputs:
      seed:
        kind: file
        from: path
        path: seed.txt
        mediaType: text/plain
  noValue:
    kind: agent
    dependsOn: [seed]
    agent:
      profile: local
      systemPrompt: none-system.md
      message:
        text:
          - file: message.md
        attachments:
          - ref: outputs.seed.seed
    outputs:
      file:
        kind: file
        from: path
        path: agent-file.txt
        mediaType: text/plain
  response:
    kind: agent
    dependsOn: [noValue]
    agent:
      profile: local
      systemPrompt: response-system.md
      message:
        text:
          - file: message.md
        attachments:
          - ref: outputs.noValue.file
    outputs:
      response:
        kind: text
        from: agent_response
  oversizedResponse:
    kind: agent
    dependsOn: [response]
    agent:
      profile: local
      systemPrompt: oversized-response-system.md
      message:
        text:
          - file: message.md
    outputs:
      response:
        kind: text
        from: agent_response
  result:
    kind: agent
    dependsOn: [oversizedResponse]
    agent:
      profile: local
      systemPrompt: result-system.md
      message:
        text:
          - ref: outputs.response.response
    outputs:
      result:
        kind: json
        from: agent_result
        schema: result.schema.json
  oversized:
    kind: agent
    dependsOn: [result]
    agent:
      profile: local
      systemPrompt: oversized-system.md
      message:
        text:
          - file: oversized-message.md
    outputs:
      result:
        kind: json
        from: agent_result
        schema: oversized-result.schema.json
exports:
  file:
    ref: outputs.noValue.file
  response:
    ref: outputs.oversizedResponse.response
  result:
    ref: outputs.result.result
  oversized:
    ref: outputs.oversized.result
  seed:
    ref: outputs.seed.seed
"#
}

const FIXTURE_WRITE_EXTENSION: &str = r#"import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { writeFile } from "node:fs/promises";
import { Type } from "typebox";

export default function fixtureWrite(pi: ExtensionAPI): void {
  pi.registerTool({
    name: "fixture_write",
    label: "Fixture write",
    description: "Write the deterministic agent fixture file",
    parameters: Type.Object({}),
    async execute() {
      await writeFile("agent-file.txt", "agent file\n", "utf8");
      return { content: [{ type: "text" as const, text: "written" }], details: {} };
    },
  });
}
"#;

#[test]
fn pinned_real_pi_runs_the_complete_mixed_value_and_export_dag() {
    let Some(pinned_pi) = std::env::var_os("SCHERZO_PI_CONFORMANCE_EXECUTABLE")
        .map(PathBuf::from)
        .filter(|path| path.to_string_lossy().ends_with("-pi-0.84.2/bin/pi"))
    else {
        return;
    };
    let bundle = RunBundle::new(mixed_agent_source());
    for (path, text) in [
        ("none-system.md", "MODE_NONE"),
        ("response-system.md", "MODE_RESPONSE"),
        ("oversized-response-system.md", "MODE_OVERSIZED_RESPONSE"),
        ("result-system.md", "MODE_RESULT"),
        ("message.md", "fixture message"),
    ] {
        bundle.write_source(path, text);
    }
    let oversized_system_prefix = "MODE_OVERSIZED_RESULT\n";
    let oversized_system_prompt = format!(
        "{oversized_system_prefix}{}",
        "s".repeat(OVERSIZED_AGENT_SYSTEM_PROMPT_BYTES - oversized_system_prefix.len())
    );
    bundle.write_source("oversized-system.md", &oversized_system_prompt);
    bundle.write_source(
        "oversized-message.md",
        "m".repeat(OVERSIZED_AGENT_MESSAGE_BYTES),
    );
    bundle.write_source(
        "result.schema.json",
        br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["answer","source"],"properties":{"answer":{"const":42},"source":{"const":"agent response"}},"additionalProperties":false}"#,
    );
    bundle.write_source(
        "oversized-result.schema.json",
        br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["payload"],"properties":{"payload":{"type":"string"}},"additionalProperties":false}"#,
    );
    bundle.write_source(
        "../execution/.pi/extensions/fake-provider.ts",
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/execution/workflow/pi-json-v1-extension/src/conformance/fake-provider.ts"
        )),
    );
    bundle.write_source(
        "../execution/.pi/extensions/fixture-write.ts",
        FIXTURE_WRITE_EXTENSION,
    );

    let isolated_path = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(pinned_pi, isolated_path.path().join("pi")).unwrap();
    let developer = tempfile::Builder::new()
        .prefix("scherzo-cli-agent-")
        .tempdir_in("/tmp")
        .unwrap();
    let agent_directory = developer.path().join("agent");
    let home = developer.path().join("home");
    let config = developer.path().join("config");
    let cache = developer.path().join("cache");
    let data = developer.path().join("data");
    let state = developer.path().join("state");
    for directory in [&agent_directory, &home, &config, &cache, &data, &state] {
        fs::create_dir(directory).unwrap();
    }
    let socket_path = developer.path().join("provider.sock");
    let provider = serve_fake_provider(&socket_path, 11);
    let destination = bundle.result("mixed-agent");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let output = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args(args)
        .env_clear()
        .env("PATH", isolated_path.path())
        .env("HOME", &home)
        .env("PI_CODING_AGENT_DIR", &agent_directory)
        .env("XDG_CONFIG_HOME", &config)
        .env("XDG_CACHE_HOME", &cache)
        .env("XDG_DATA_HOME", &data)
        .env("XDG_STATE_HOME", &state)
        .env("PI_OFFLINE", "1")
        .env("PI_SKIP_VERSION_CHECK", "1")
        .env("PI_TELEMETRY", "0")
        .env("WORKFLOW_RUN_FIXTURE_SOCKET", &socket_path)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = provider.join().unwrap();
    assert_eq!(requests.len(), 11);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["kind"] == "model")
            .count(),
        6
    );
    let oversized_start = requests
        .iter()
        .find(|request| {
            request["kind"] == "before_agent_start"
                && request["systemPrompt"]
                    .as_str()
                    .is_some_and(|system| system.contains("MODE_OVERSIZED_RESULT"))
        })
        .unwrap();
    assert!(
        oversized_start["systemPrompt"]
            .as_str()
            .unwrap()
            .contains(oversized_system_prompt.as_str()),
        "Pi must receive the staged workflow system prompt without changing its text"
    );
    let oversized_prompt = oversized_start["prompt"].as_str().unwrap();
    assert_eq!(
        oversized_prompt.len(),
        OVERSIZED_AGENT_MESSAGE_BYTES,
        "Pi must receive the workflow message without transport wrapper text"
    );
    assert!(oversized_prompt.bytes().all(|byte| byte == b'm'));
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["outcome"], "succeeded");
    assert_eq!(terminal["result"]["steps"][0]["kind"], "cmd");
    for index in 1..=5 {
        assert_eq!(terminal["result"]["steps"][index]["kind"], "agent");
    }
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0001")).unwrap(),
        b"agent file\n"
    );
    assert_eq!(
        fs::metadata(attempt_result(&destination).join("exports/0002"))
            .unwrap()
            .len(),
        4 * 1024 * 1024
    );
    let response = fs::read(attempt_result(&destination).join("exports/0003")).unwrap();
    assert_eq!(response.len(), OVERSIZED_AGENT_RESPONSE_BYTES);
    assert!(response.iter().all(|byte| *byte == b'r'));
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0004")).unwrap(),
        br#"{"answer":42,"source":"agent response"}"#
    );
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0005")).unwrap(),
        b"seed"
    );
    assert_eq!(terminal["result"]["exports"]["file"]["kind"], "file");
    assert_eq!(terminal["result"]["exports"]["response"]["kind"], "text");
    assert_eq!(terminal["result"]["exports"]["result"]["kind"], "json");
    assert_eq!(
        terminal["result"]["exports"]["result"]["mediaType"],
        "application/json"
    );
    let live = String::from_utf8(output.stderr).unwrap();
    assert!(live.contains("event       tool_call · started"));
    assert!(live.contains("event       assistant · agent "));
    assert!(live.contains("event       tool_result"));
}

#[test]
fn local_run_preserves_caller_github_environment_and_reserves_engine_prefix() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  authenticate:
    kind: cmd
    command:
      argv:
        - sh
        - -c
        - 'test "$GH_TOKEN" = local-gh-token && test "$GITHUB_TOKEN" = local-github-token && test -z "${SCHERZO_PRIVATE_SENTINEL+x}"'
"#,
    );
    let output = isolated_command(&bundle.args(&bundle.result("environment")))
        .env("GH_TOKEN", "local-gh-token")
        .env("GITHUB_TOKEN", "local-github-token")
        .env("SCHERZO_PRIVATE_SENTINEL", "must-not-reach-command")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn json_run_executes_imports_closed_stdin_publication_and_offline_boundaries() {
    let bundle = RunBundle::new(producer_consumer_source());
    let first = bundle._temporary.path().join("first.txt");
    let second = bundle._temporary.path().join("second.txt");
    fs::write(&first, b"first").unwrap();
    fs::write(&second, b"second").unwrap();
    let destination = bundle.result("complete");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut args = bundle.args(&destination);
    args.splice(
        args.len() - 1..args.len() - 1,
        [
            "--prompt-file".to_owned(),
            "-".to_owned(),
            "--attachment".to_owned(),
            "text/plain".to_owned(),
            first.to_string_lossy().into_owned(),
            "--attachment".to_owned(),
            "application/octet-stream".to_owned(),
            second.to_string_lossy().into_owned(),
            "--max-parallel".to_owned(),
            "2".to_owned(),
            "--json".to_owned(),
            "--color".to_owned(),
            "always".to_owned(),
        ],
    );
    let mut child = isolated_command(&args)
        .env("SCHERZO_PRIVATE_SENTINEL", "must-not-reach-command")
        .env(
            "SCHERZO_CLOUD_API_URL",
            format!("http://{}", listener.local_addr().unwrap()),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(b"prompt\n").unwrap();
    let output = child.wait_with_output().unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["schemaVersion"], 1);
    assert_eq!(terminal["command"], "scherzo-cloud workflow run");
    assert_eq!(terminal["outcome"], "succeeded");
    assert_eq!(terminal["exitStatus"], 0);
    let normalized_run = normalized_run_directory(&destination);
    assert_eq!(terminal["runDirectory"], normalized_run.to_str().unwrap());
    assert_eq!(terminal["attemptNumber"], 1);
    assert_eq!(terminal["result"]["attemptNumber"], 1);
    assert!(terminal["result"].get("command").is_none());
    assert_eq!(
        terminal["resultDirectory"],
        attempt_result(&normalized_run).to_str().unwrap()
    );
    assert_eq!(terminal["result"], result_json(&destination));
    assert_eq!(terminal["result"]["execution"]["maximumParallelSteps"], 2);
    let run_document: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("run.json")).unwrap()).unwrap();
    assert_eq!(
        run_document
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([
            "createdAt",
            "localRunId",
            "schemaVersion",
            "workflowDigest",
            "workflowManifestDigest",
        ])
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["schemaVersion"], 1);
    assert_eq!(state["localRunId"], run_document["localRunId"]);
    assert_eq!(state["currentAttemptNumber"], 1);
    assert_eq!(state["attempts"][0]["state"], "succeeded");
    assert_eq!(
        state["attempts"][0]["progress"]["outstandingActions"],
        serde_json::json!([])
    );
    let process_guards = state["attempts"][0]["processGuards"].as_array().unwrap();
    assert_eq!(process_guards.len(), 2);
    for guard in process_guards {
        assert_eq!(guard["state"], "quiesced");
        assert_eq!(
            guard["executionHost"],
            state["attempts"][0]["owner"]["executionHost"]
        );
        assert!(guard["processGroupId"].as_i64().is_some_and(|id| id > 0));
        assert_eq!(guard["liveness"]["kind"], "leader_start_identity");
        assert!(
            guard["liveness"]["value"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
    }
    assert_eq!(state["attempts"][0]["result"]["status"], "published");
    assert_eq!(
        state["attempts"][0]["result"]["relativeDirectory"],
        "attempts/000001/result"
    );
    assert_eq!(terminal["result"]["steps"][0]["id"], "produce");
    assert_eq!(terminal["result"]["steps"][1]["id"], "consume");
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0001")).unwrap(),
        b"prompt\n|first|second"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("producer-live"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("consumer-live"));
    assert!(!output.stdout.contains(&0x1b));
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "workflow run adapter must not contact configured Cloud endpoints"
    );
    assert!(bundle.execution_root.exists());
}

#[cfg(target_os = "linux")]
#[test]
fn tui_releases_ownership_and_restores_before_summary_handoff() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    );
    let destination = bundle.result("tui-handoff");
    let (master, slave) = open_tui_pty();
    let original_input_mode = rustix::termios::tcgetattr(&slave).unwrap();
    let (mut child, mut master_writer, reader) =
        spawn_tui_run(&bundle.args(&destination), master, &slave);

    let status_args = vec![
        "workflow".to_owned(),
        "status".to_owned(),
        destination.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ];
    let mut last_status_error = String::new();
    let status = (0..500).find_map(|_| {
        let output = isolated_command(&status_args).output().unwrap();
        if output.status.success() {
            let status: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            if status["recovery"] == serde_json::json!({"status": "settled"}) {
                return Some(status);
            }
            last_status_error = format!("latest recovery status: {}", status["recovery"]);
        } else {
            last_status_error = String::from_utf8_lossy(&output.stderr).into_owned();
        }
        wait_for_process_poll();
        None
    });
    let Some(status) = status else {
        let _ = child.kill();
        let _ = child.wait();
        drop(slave);
        drop(master_writer);
        let transcript = reader.join().unwrap();
        panic!(
            "status did not observe released ownership: {last_status_error}; transcript: {:?}",
            String::from_utf8_lossy(&transcript)
        );
    };
    assert_eq!(status["recovery"], serde_json::json!({"status": "settled"}));
    assert_eq!(
        status["retry"],
        serde_json::json!({
            "eligible": false,
            "reason": "latest_attempt_succeeded"
        })
    );

    master_writer.write_all(b"q").unwrap();
    master_writer.flush().unwrap();
    let process_status = (0..200)
        .find_map(|_| {
            let status = child.try_wait().unwrap();
            if status.is_none() {
                wait_for_process_poll();
            }
            status
        })
        .unwrap_or_else(|| {
            let _ = child.kill();
            panic!("TUI did not exit after eligible q")
        });
    assert!(process_status.success());
    let restored_input_mode = rustix::termios::tcgetattr(&slave).unwrap();
    assert_eq!(
        restored_input_mode.input_modes,
        original_input_mode.input_modes
    );
    assert_eq!(
        restored_input_mode.output_modes,
        original_input_mode.output_modes
    );
    assert_eq!(
        restored_input_mode.control_modes,
        original_input_mode.control_modes
    );
    assert_eq!(
        restored_input_mode.local_modes,
        original_input_mode.local_modes
    );
    assert_eq!(
        restored_input_mode.special_codes[rustix::termios::SpecialCodeIndex::VMIN],
        original_input_mode.special_codes[rustix::termios::SpecialCodeIndex::VMIN]
    );
    assert_eq!(
        restored_input_mode.special_codes[rustix::termios::SpecialCodeIndex::VTIME],
        original_input_mode.special_codes[rustix::termios::SpecialCodeIndex::VTIME]
    );

    drop(slave);
    drop(master_writer);
    let transcript = reader.join().unwrap();
    let transcript = String::from_utf8_lossy(&transcript);
    let restored = transcript
        .rfind("\u{1b}[?1049l")
        .expect("TUI must leave the alternate screen");
    let summary = transcript[restored..]
        .find("── summary")
        .map(|offset| restored + offset)
        .expect("standard summary must follow TUI restoration");
    assert!(restored < summary);
    assert!(!transcript[restored + "\u{1b}[?1049l".len()..].contains("\u{1b}[?1049h"));
    assert!(transcript[summary..].contains("result succeeded · exit 0"));
    assert!(transcript[summary..].contains("attempts/000001/result"));
}

#[cfg(target_os = "linux")]
#[test]
fn tui_releases_outer_private_staging_before_run_lock() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    );
    let destination = bundle.result("tui-private-cleanup-order");
    let (master, slave) = open_tui_pty();
    let (mut child, master_writer, reader) =
        spawn_tui_run(&bundle.args(&destination), master, &slave);
    drop(master_writer);

    let lock_released = (0..500).any(|_| {
        let available = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(destination.join("run.lock"))
            .ok()
            .is_some_and(|lock| {
                rustix::fs::fcntl_lock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive)
                    .is_ok()
            });
        if !available {
            wait_for_process_poll();
        }
        available
    });
    let private_entries = lock_released.then(|| {
        fs::read_dir(destination.join(".private"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>()
    });

    child.kill().unwrap();
    child.wait().unwrap();
    drop(slave);
    reader.join().unwrap();

    assert!(lock_released, "TUI never released run.lock");
    assert!(
        private_entries.as_ref().unwrap().is_empty(),
        "run.lock was released before private staging cleanup: {private_entries:?}"
    );
}

#[test]
fn execution_root_rebinding_fails_default_and_nested_cwds_before_command_launch() {
    for cwd in [None, Some("nested")] {
        let cwd_field = cwd.map_or_else(String::new, |cwd| format!("    cwd: {cwd}\n"));
        let output_path = cwd.map_or("command-ran", |_| "nested/command-ran");
        let source = format!(
            "schemaVersion: 1\nsteps:\n  rebind:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"set -eu; mv \\\"$ROOT_PATH\\\" \\\"$MOVED_ROOT\\\"; mkdir \\\"$ROOT_PATH\\\"; mkdir \\\"$ROOT_PATH/nested\\\"; mkdir \\\"$MOVED_ROOT/nested\\\"\"]\n  affected:\n    kind: cmd\n    dependsOn: [rebind]\n{cwd_field}    command:\n      argv: [\"sh\", \"-c\", \"printf ran > command-ran\"]\n    outputs:\n      marker:\n        kind: file\n        from: path\n        path: {output_path}\n        mediaType: text/plain\n"
        );
        let bundle = RunBundle::new(&source);
        let normalized_execution_root = fs::canonicalize(&bundle.execution_root).unwrap();
        let moved_root = bundle._temporary.path().join("moved-execution");
        let destination = bundle.result(cwd.unwrap_or("default"));
        let mut args = bundle.args(&destination);
        args.insert(args.len() - 1, "--json".to_owned());
        let output = isolated_command(&args)
            .env("ROOT_PATH", &bundle.execution_root)
            .env("MOVED_ROOT", &moved_root)
            .output()
            .unwrap();

        assert_eq!(output.status.code(), Some(1));
        let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(terminal["outcome"], "failed");
        assert_eq!(terminal["result"]["outcome"], "failed");
        assert_eq!(
            terminal["result"]["primaryFailure"]["node"]["id"],
            "affected",
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(terminal["result"]["primaryFailure"]["phase"], "start");
        assert_eq!(
            terminal["result"]["primaryFailure"]["cause"]["code"],
            "execution_root_rebound"
        );
        assert_eq!(
            terminal["result"]["execution"]["executionRoot"],
            normalized_execution_root.to_str().unwrap()
        );
        assert!(!terminal.to_string().contains("output_missing"));
        assert!(attempt_result(&destination).join("result.json").is_file());
        assert!(!bundle.execution_root.join(output_path).exists());
        assert!(!moved_root.join(output_path).exists());
    }
}

#[test]
fn failure_rejection_usage_and_result_preconditions_keep_their_outcome_precedence() {
    let failed = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  fail:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"printf failed-live; exit 23\"]\n",
    );
    let failed_destination = failed.result("failed");
    let mut args = failed.args(&failed_destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let output = run(&args);
    assert_eq!(output.status.code(), Some(1));
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["outcome"], "failed");
    assert_eq!(terminal["exitStatus"], 1);
    assert_eq!(terminal["result"]["primaryFailure"]["node"]["id"], "fail");
    assert_eq!(
        terminal["result"]["primaryFailure"]["cause"]["exitCode"],
        23
    );
    assert!(failed_destination.exists());

    let rejected = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  needsPrompt:\n    kind: cmd\n    inputs:\n      prompt:\n        ref: imports.prompt\n    command:\n      argv: [\"true\"]\n",
    );
    let rejected_destination = rejected.result("rejected");
    let mut args = rejected.args(&rejected_destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let output = run(&args);
    assert_eq!(output.status.code(), Some(1));
    let rejection: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rejection["outcome"], "rejected");
    assert_eq!(rejection["phase"], "admission");
    assert_eq!(
        rejection["diagnostics"][0]["code"],
        "missing_required_prompt"
    );
    assert!(output.stderr.is_empty());
    assert!(!rejected_destination.exists());

    let finalizers = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: { argv: [\"true\"] }\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: { argv: [\"sh\", \"-c\", \"touch finalizer-ran\"] }\n",
    );
    let finalizer_destination = finalizers.result("finalizers-supported");
    let mut args = finalizers.args(&finalizer_destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let output = run(&args);
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["outcome"], "succeeded");
    assert_eq!(terminal["result"]["finalization"]["trigger"], "succeeded");
    assert_eq!(
        terminal["result"]["finalization"]["finalizers"][0]["state"],
        "succeeded"
    );
    assert!(finalizers.execution_root.join("finalizer-ran").is_file());

    let malformed = RunBundle::new("schemaVersion: [\n");
    let malformed_destination = malformed.result("malformed");
    let mut args = malformed.args(&malformed_destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let output = run(&args);
    assert_eq!(output.status.code(), Some(1));
    let rejection: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rejection["outcome"], "rejected");
    assert_eq!(rejection["phase"], "resolution");
    assert_eq!(rejection["diagnostics"][0]["code"], "malformed_yaml");
    assert!(output.stderr.is_empty());
    assert!(!malformed_destination.exists());

    let occupied = rejected.result("occupied");
    fs::write(&occupied, b"unchanged").unwrap();
    let mut args = rejected.args(&occupied);
    args.insert(args.len() - 1, "--prompt-file".to_owned());
    args.insert(
        args.len() - 1,
        rejected
            .source_root
            .join(WORKFLOW_PATH)
            .to_string_lossy()
            .into_owned(),
    );
    let output = run(&args);
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(fs::read(&occupied).unwrap(), b"unchanged");

    let usage = run(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "--plain".to_owned(),
        "--json".to_owned(),
    ]);
    assert_eq!(usage.status.code(), Some(2));
    assert!(usage.stdout.is_empty());

    let mut obsolete_result_option = rejected.args(&rejected.result("obsolete"));
    let option = obsolete_result_option
        .iter_mut()
        .find(|argument| argument.as_str() == "--run-dir")
        .unwrap();
    *option = "--result-dir".to_owned();
    let usage = run(&obsolete_result_option);
    assert_eq!(usage.status.code(), Some(2));
    assert!(usage.stdout.is_empty());
    assert!(String::from_utf8_lossy(&usage.stderr).contains("--result-dir"));
}

#[test]
fn prompt_stdin_accepts_a_redirected_regular_file() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  consume:
    kind: cmd
    inputs:
      prompt:
        ref: imports.prompt
    command:
      argv: ["sh", "-c", "cat \"$SCHERZO_STEP_INPUTS/values/prompt\" > prompt.txt"]
    outputs:
      prompt:
        kind: file
        from: path
        path: prompt.txt
        mediaType: text/plain
exports:
  prompt:
    ref: outputs.consume.prompt
"#,
    );
    let prompt = bundle._temporary.path().join("prompt.txt");
    fs::write(&prompt, b"prompt from a regular file\n").unwrap();
    let destination = bundle.result("regular-file-stdin");
    let mut args = bundle.args(&destination);
    args.splice(
        args.len() - 1..args.len() - 1,
        ["--prompt-file".to_owned(), "-".to_owned()],
    );

    let output = isolated_command(&args)
        .stdin(fs::File::open(&prompt).unwrap())
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0001")).unwrap(),
        b"prompt from a regular file\n"
    );
}

// Darwin filesystems reject non-UTF-8 names before the CLI can inspect them.
#[cfg(not(target_os = "macos"))]
#[test]
fn attachment_paths_accept_non_utf8_host_names() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  consume:
    kind: cmd
    inputs:
      attachments:
        ref: imports.attachments
    command:
      argv: ["sh", "-c", "cat \"$SCHERZO_STEP_INPUTS/collections/attachments/000000\" > attachment.bin"]
    outputs:
      attachment:
        kind: file
        from: path
        path: attachment.bin
        mediaType: application/octet-stream
exports:
  attachment:
    ref: outputs.consume.attachment
"#,
    );
    let attachment = bundle
        ._temporary
        .path()
        .join(OsString::from_vec(b"attachment-\xff".to_vec()));
    fs::write(&attachment, b"non-UTF-8 attachment path").unwrap();
    let destination = bundle.result("non-utf8-attachment");
    let mut args = bundle.args(&destination);
    let workflow_path = args.pop().unwrap();

    let output = isolated_command(&args)
        .args(["--attachment", "application/octet-stream"])
        .arg(&attachment)
        .arg(workflow_path)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0001")).unwrap(),
        b"non-UTF-8 attachment path"
    );
}

#[test]
fn initially_unwritable_result_parent_prevents_execution() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  mutate:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"touch \\\"$EXECUTION_MARKER\\\"\"]\n",
    );
    let destination = bundle.result("unwritable-parent");
    let marker = bundle.execution_root.join("command-ran");
    let original_permissions = fs::metadata(&bundle.result_parent).unwrap().permissions();
    let mut unwritable_permissions = original_permissions.clone();
    unwritable_permissions.set_mode(0o500);
    fs::set_permissions(&bundle.result_parent, unwritable_permissions).unwrap();

    let output = isolated_command(&bundle.args(&destination))
        .env("EXECUTION_MARKER", &marker)
        .output()
        .unwrap();

    fs::set_permissions(&bundle.result_parent, original_permissions).unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(
        !marker.exists(),
        "an initially invalid result target must fail before a command starts"
    );
    assert!(!destination.exists());
}

#[test]
fn invalid_local_import_fails_before_resolution_presentation_or_publication() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    );
    let prompt = bundle._temporary.path().join("invalid-prompt");
    fs::write(&prompt, [0xff]).unwrap();
    let destination = bundle.result("invalid-import");
    let mut args = bundle.args(&destination);
    args.splice(
        args.len() - 1..args.len() - 1,
        [
            "--prompt-file".to_owned(),
            prompt.to_string_lossy().into_owned(),
            "--json".to_owned(),
        ],
    );
    let output = run(&args);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("InvalidUtf8"));
    assert!(!destination.exists());
}

#[test]
fn corrupt_authoritative_state_stops_before_a_later_action_is_released() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  corrupt:
    kind: cmd
    command:
      argv:
        - sh
        - -c
        - |
          while ! grep -q '\"state\": \"running\"' "$RUN_STATE"; do sleep 0.01; done
          printf partial > "$RUN_STATE.corrupt"
          mv "$RUN_STATE.corrupt" "$RUN_STATE"
  forbidden:
    kind: cmd
    dependsOn: [corrupt]
    command:
      argv: ["sh", "-c", "touch \"$LATER_ACTION\""]
"#,
    );
    let destination = bundle.result("corrupt-state");
    let later_action = bundle.execution_root.join("later-action");

    let output = isolated_command(&bundle.args(&destination))
        .env("RUN_STATE", destination.join("state.json"))
        .env("LATER_ACTION", &later_action)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        fs::read(destination.join("state.json")).unwrap(),
        b"partial"
    );
    assert!(!later_action.exists());
    assert!(!attempt_result(&destination).exists());
}

#[test]
fn state_persistence_failure_quiesces_already_running_steps() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  corrupt:
    kind: cmd
    command:
      argv:
        - sh
        - -c
        - |
          while [ "$(grep -c '\"state\": \"running\"' "$RUN_STATE")" -lt 3 ]; do sleep 0.01; done
          printf partial > "$RUN_STATE.corrupt"
          mv "$RUN_STATE.corrupt" "$RUN_STATE"
  survivor:
    kind: cmd
    command:
      argv: ["sh", "-c", "sleep 2; touch \"$LATE_SIDE_EFFECT\""]
"#,
    );
    let destination = bundle.result("persistence-failure-quiescence");
    let late_side_effect = bundle.execution_root.join("late-side-effect");
    let mut args = bundle.args(&destination);
    args.splice(
        args.len() - 1..args.len() - 1,
        ["--max-parallel".to_owned(), "2".to_owned()],
    );

    let output = isolated_command(&args)
        .env("RUN_STATE", destination.join("state.json"))
        .env("LATE_SIDE_EFFECT", &late_side_effect)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(
        !late_side_effect.exists(),
        "work already owned when persistence fails must be quiesced"
    );
}

#[test]
fn private_staging_cleanup_failure_is_recorded_in_durable_state() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  moveInputStore:
    kind: cmd
    inputs:
      prompt:
        ref: imports.prompt
    command:
      argv: ["sh", "-c", "store=$(dirname \"$SCHERZO_STEP_INPUTS\"); mv \"$store\" \"$store.moved\""]
"#,
    );
    let destination = bundle.result("cleanup-diagnostic");
    let prompt = bundle._temporary.path().join("prompt.txt");
    fs::write(&prompt, b"prompt").unwrap();
    let mut args = bundle.args(&destination);
    args.splice(
        args.len() - 1..args.len() - 1,
        [
            "--prompt-file".to_owned(),
            prompt.to_string_lossy().into_owned(),
        ],
    );

    let output = isolated_command(&args).output().unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(attempt_result(&destination).join("result.json").is_file());
    let summary = String::from_utf8_lossy(&output.stdout);
    assert!(summary.contains("attempts/000001/result"));
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["diagnostics"][0]["code"], "private_cleanup_failure");
}

#[test]
fn run_directory_may_have_a_nonexistent_parent_suffix() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    );
    let destination = bundle.result("nested").join("specific-run");

    let output = isolated_command(&bundle.args(&destination))
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(destination.join("run.json").is_file());
}

#[test]
fn tui_setup_failure_prevents_execution_and_releases_the_attempt() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  forbidden:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"printf started > \\\"$WORKFLOW_MARKER\\\"\"]\n",
    );
    let destination = bundle.result("tui-setup-failure");
    let marker = bundle.execution_root().join("started");

    let output = run_with_unusable_tui(&bundle.args(&destination), |command| {
        command.env("WORKFLOW_MARKER", &marker);
    });

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!marker.exists());
    assert_executor_fault_before_execution(&destination, 0);
    assert!(!attempt_result(&destination).exists());
    assert_attempt_resources_released(&destination);

    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr.lines().count(), 1);
    assert!(stderr.contains("TerminalSetup"));
    assert!(!stderr.contains("summary"));
}

#[test]
fn presentation_setup_failure_settles_the_published_attempt() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  forbidden:\n    kind: cmd\n    command:\n      argv: [\"false\"]\n",
    );
    let destination = bundle.result("presentation-setup-failure");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--plain".to_owned());
    let (reader, writer) = UnixStream::pair().unwrap();
    drop(reader);

    let output = isolated_command(&args)
        .stdout(Stdio::from(OwnedFd::from(writer)))
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["attempts"][0]["state"], "interrupted");
    assert_eq!(
        state["attempts"][0]["interruption"],
        serde_json::json!({
            "cause": "executor_fault",
            "executionMayHaveStarted": false,
            "cancellationRequested": false
        })
    );
    assert!(!attempt_result(&destination).exists());
}

#[test]
fn real_pty_boundary_restores_input_mode_before_the_standard_summary_handoff() {
    let (finished, completion) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = finished.send(run_real_pty_boundary_smoke());
    });

    let result = completion
        .recv()
        .expect("PTY workflow boundary worker should report completion");
    result.expect("PTY workflow boundary failed");
    worker
        .join()
        .expect("PTY workflow boundary worker panicked");
}

fn run_real_pty_boundary_smoke() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  handshake:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"printf ready > \\\"$READY_FIFO\\\"; IFS= read -r release < \\\"$RELEASE_FIFO\\\"; test \\\"$release\\\" = release\"]\n",
    );
    let destination = bundle.result("pty-boundary");
    let ready_fifo = bundle._temporary.path().join("ready.fifo");
    let release_fifo = bundle._temporary.path().join("release.fifo");
    let fifo_mode = Mode::S_IRUSR | Mode::S_IWUSR;
    mkfifo(&ready_fifo, fifo_mode)?;
    mkfifo(&release_fifo, fifo_mode)?;
    let handshake_directory = tempfile::tempdir_in("/tmp")?;
    let handshake_path = handshake_directory.path().join("tui.socket");
    let handshake_listener = UnixListener::bind(&handshake_path)?;

    let size = Winsize {
        ws_row: 24,
        ws_col: 80,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let pty = openpty(Some(&size), None::<&nix::sys::termios::Termios>)?;
    let original_mode = rustix::termios::tcgetattr(&pty.slave)?;
    let child_input = rustix::io::dup(&pty.slave)?;
    let child_output = rustix::io::dup(&pty.slave)?;
    let master_reader = rustix::io::dup(&pty.master)?;
    let mut master_writer = File::from(pty.master);
    let mut master_reader = File::from(master_reader);
    let terminal_reader = std::thread::spawn(move || {
        let mut output = Vec::new();
        read_pty_to_end(&mut master_reader, &mut output)?;
        Ok::<_, std::io::Error>(output)
    });

    let mut child = isolated_command(&bundle.args(&destination))
        .env("TERM", "xterm-256color")
        .env("NO_COLOR", "1")
        .env("READY_FIFO", &ready_fifo)
        .env("RELEASE_FIFO", &release_fifo)
        .env(TUI_HANDSHAKE_VARIABLE, &handshake_path)
        .stdin(Stdio::from(child_input))
        .stdout(Stdio::from(child_output))
        .stderr(Stdio::piped())
        .spawn()?;

    let (handshake, _) = handshake_listener.accept()?;
    let mut handshake = BufReader::new(handshake);
    let mut ready = OpenOptions::new().read(true).open(&ready_fifo)?;
    let mut ready_bytes = [0_u8; 5];
    ready.read_exact(&mut ready_bytes)?;
    if ready_bytes != *b"ready" {
        return Err("workflow command emitted an invalid readiness handshake".into());
    }

    master_writer.write_all(b"q?")?;
    master_writer.flush()?;
    read_terminal_handshake(&mut handshake, "help-open")?;

    let mut release = OpenOptions::new().write(true).open(&release_fifo)?;
    release.write_all(b"release\n")?;
    release.flush()?;
    drop(release);
    read_terminal_handshake(&mut handshake, "quit-eligible")?;
    master_writer.write_all(b"q")?;
    master_writer.flush()?;
    let status = child.wait()?;
    let restored_mode = rustix::termios::tcgetattr(&pty.slave)?;
    assert_eq!(restored_mode.input_modes, original_mode.input_modes);
    assert_eq!(restored_mode.output_modes, original_mode.output_modes);
    assert_eq!(restored_mode.control_modes, original_mode.control_modes);
    assert_eq!(restored_mode.local_modes, original_mode.local_modes);
    assert_eq!(
        restored_mode.special_codes[rustix::termios::SpecialCodeIndex::VMIN],
        original_mode.special_codes[rustix::termios::SpecialCodeIndex::VMIN]
    );
    assert_eq!(
        restored_mode.special_codes[rustix::termios::SpecialCodeIndex::VTIME],
        original_mode.special_codes[rustix::termios::SpecialCodeIndex::VTIME]
    );
    drop(pty.slave);
    let terminal_output = terminal_reader
        .join()
        .map_err(|_| std::io::Error::other("PTY reader panicked"))??;

    let mut stderr = String::new();
    if let Some(mut child_stderr) = child.stderr.take() {
        child_stderr.read_to_string(&mut stderr)?;
    }
    if !status.success() {
        return Err(format!("PTY workflow exited with {status}: {stderr}").into());
    }
    let rendered = String::from_utf8_lossy(&terminal_output);
    assert!(rendered.contains("summary"));
    assert!(rendered.contains("succeeded · exit 0"));
    assert!(stderr.is_empty(), "unexpected PTY stderr: {stderr}");
    Ok(())
}

pub(super) fn run_with_unusable_tui(
    args: &[String],
    configure: impl FnOnce(&mut Command),
) -> Output {
    let size = Winsize {
        ws_row: 24,
        ws_col: 80,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let pty = openpty(Some(&size), None::<&nix::sys::termios::Termios>).unwrap();
    let original_mode = rustix::termios::tcgetattr(&pty.slave).unwrap();
    let child_input = rustix::io::dup(&pty.slave).unwrap();
    let child_output = rustix::io::dup(&pty.slave).unwrap();
    let mut master_reader = File::from(pty.master);
    let handshake_directory = tempfile::tempdir().unwrap();
    let unavailable_handshake = handshake_directory.path().join("missing.socket");
    let mut command = isolated_command(args);
    configure(&mut command);
    let mut child = command
        .env("TERM", "xterm-256color")
        .env("NO_COLOR", "1")
        .env(TUI_HANDSHAKE_VARIABLE, unavailable_handshake)
        .stdin(Stdio::from(child_input))
        .stdout(Stdio::from(child_output))
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let status = child.wait().unwrap();
    let restored_mode = rustix::termios::tcgetattr(&pty.slave).unwrap();
    assert_eq!(restored_mode.input_modes, original_mode.input_modes);
    assert_eq!(restored_mode.output_modes, original_mode.output_modes);
    assert_eq!(restored_mode.control_modes, original_mode.control_modes);
    assert_eq!(restored_mode.local_modes, original_mode.local_modes);
    assert_eq!(
        restored_mode.special_codes[rustix::termios::SpecialCodeIndex::VMIN],
        original_mode.special_codes[rustix::termios::SpecialCodeIndex::VMIN]
    );
    assert_eq!(
        restored_mode.special_codes[rustix::termios::SpecialCodeIndex::VTIME],
        original_mode.special_codes[rustix::termios::SpecialCodeIndex::VTIME]
    );
    drop(pty.slave);

    let flags = fcntl_getfl(&master_reader).unwrap();
    fcntl_setfl(&master_reader, flags | OFlags::NONBLOCK).unwrap();
    let mut stdout = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        match master_reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => stdout.extend_from_slice(&buffer[..read]),
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.raw_os_error() == Some(libc::EIO) =>
            {
                break;
            }
            Err(error) => panic!("read failed TUI output: {error}"),
        }
    }
    let mut stderr = Vec::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_end(&mut stderr)
        .unwrap();
    Output {
        status,
        stdout,
        stderr,
    }
}

pub(super) fn assert_attempt_resources_released(run_directory: &Path) {
    assert!(
        fs::read_dir(run_directory.join(".private"))
            .unwrap()
            .next()
            .is_none()
    );
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(run_directory.join("run.lock"))
        .unwrap();
    lock.try_lock().unwrap();
    fs4::FileExt::unlock(&lock).unwrap();
}

pub(super) fn assert_executor_fault_before_execution(run_directory: &Path, attempt_index: usize) {
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run_directory.join("state.json")).unwrap()).unwrap();
    let attempt = &state["attempts"][attempt_index];
    assert_eq!(attempt["state"], "interrupted");
    assert!(attempt.get("startedAt").is_none());
    assert!(attempt.get("cancellation").is_none());
    assert_eq!(
        attempt["interruption"],
        serde_json::json!({
            "cause": "executor_fault",
            "executionMayHaveStarted": false,
            "cancellationRequested": false
        })
    );
    assert!(
        attempt["progress"]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .all(|step| step["state"] == "pending")
    );
    assert_eq!(
        attempt["result"],
        serde_json::json!({
            "status": "not_published",
            "reason": "interrupted"
        })
    );
}

fn read_terminal_handshake(
    handshake: &mut BufReader<UnixStream>,
    expected: &str,
) -> std::io::Result<()> {
    let mut event = String::new();
    if handshake.read_line(&mut event)? == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "terminal lifecycle handshake closed",
        ));
    }
    if event.trim_end() != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("expected terminal lifecycle event {expected}, received {event:?}"),
        ));
    }
    Ok(())
}

fn read_pty_to_end(reader: &mut File, output: &mut Vec<u8>) -> std::io::Result<()> {
    let mut buffer = [0_u8; 4096];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(read) => output.extend_from_slice(&buffer[..read]),
            Err(error) if error.raw_os_error() == Some(libc::EIO) => return Ok(()),
            Err(error) => return Err(error),
        }
    }
}

#[test]
fn plain_mode_reports_publication_failure_without_overwriting_the_racing_target() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  race:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"mkdir \\\"$RESULT_TARGET\\\"; printf command-complete\"]\nfinalizers:\n  cleanup:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    );
    let destination = bundle.result("racing");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--plain".to_owned());
    let racing_target = attempt_result(&destination);
    let output = isolated_command(&args)
        .env("RESULT_TARGET", &racing_target)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("command-complete"));
    assert!(!stdout.contains(&format!("result: {}", destination.display())));
    assert!(String::from_utf8_lossy(&output.stderr).contains("ResultConflict"));
    assert!(racing_target.is_dir());
    assert!(fs::read_dir(racing_target).unwrap().next().is_none());
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("state.json")).unwrap()).unwrap();
    assert_eq!(
        state["attempts"][0]["result"]["status"],
        "publication_failed"
    );
    assert_eq!(state["attempts"][0]["finalization"]["complete"], true);
    assert_eq!(state["attempts"][0]["finalization"]["trigger"], "succeeded");
    assert_eq!(
        state["attempts"][0]["finalization"]["finalizers"][0]["state"],
        "succeeded"
    );
}

#[test]
fn signal_cancellation_uses_authoritative_reason_status_and_published_result() {
    for (signal, expected_status, expected_reason) in [
        (Signal::INT, 130, "user_request"),
        (Signal::TERM, 143, "termination_request"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let bundle = signal_bundle();
        let destination = bundle.result(expected_reason);
        let mut args = bundle.args(&destination);
        args.insert(args.len() - 1, "--json".to_owned());
        let child = isolated_command(&args)
            .env(
                "WORKFLOW_RUN_FIXTURE_SOCKET",
                listener.local_addr().unwrap().to_string(),
            )
            .env("WORKFLOW_RUN_FIXTURE_MODE", "wait")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let (mut control, _) = listener.accept().unwrap();
        let mut ready = [0_u8; 1];
        control.read_exact(&mut ready).unwrap();
        assert_eq!(ready, [1]);
        let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
        kill_process(pid, signal).unwrap();
        let output = child.wait_with_output().unwrap();

        assert_eq!(output.status.code(), Some(expected_status));
        let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(terminal["outcome"], "cancelled");
        assert_eq!(terminal["exitStatus"], expected_status);
        assert_eq!(
            terminal["result"]["cancellation"]["reason"],
            expected_reason
        );
        assert_eq!(terminal["result"], result_json(&destination));
    }
}

#[test]
fn signals_during_finalization_cancel_that_phase_with_signal_status() {
    for (signal, mode, expected_status, expected_reason) in [
        (Signal::INT, "signal-exit", 130, "user_request"),
        (Signal::TERM, "wait", 143, "termination_request"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let bundle = finalizer_signal_bundle();
        let destination = bundle.result(expected_reason);
        let mut args = bundle.args(&destination);
        args.insert(args.len() - 1, "--json".to_owned());
        let child = isolated_command(&args)
            .env(
                "WORKFLOW_RUN_FIXTURE_SOCKET",
                listener.local_addr().unwrap().to_string(),
            )
            .env("WORKFLOW_RUN_FIXTURE_MODE", mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let (mut control, _) = listener.accept().unwrap();
        let mut ready = [0_u8; 1];
        control.read_exact(&mut ready).unwrap();
        assert_eq!(ready, [1]);
        let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
        kill_process(pid, signal).unwrap();
        let output = child.wait_with_output().unwrap();

        assert_eq!(
            output.status.code(),
            Some(expected_status),
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(terminal["outcome"], "cancelled");
        assert_eq!(terminal["exitStatus"], expected_status);
        assert!(terminal["result"].get("cancellation").is_none());
        assert_eq!(terminal["result"]["finalization"]["trigger"], "succeeded");
        assert_eq!(
            terminal["result"]["finalization"]["cancellation"]["reason"],
            expected_reason
        );
        assert_eq!(terminal["result"]["finalization"]["forceAbort"], false);
        assert_eq!(terminal["result"], result_json(&destination));
    }
}

#[test]
fn second_interrupt_during_finalization_forces_abort_after_graceful_cancellation() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let bundle = finalizer_signal_bundle();
    let destination = bundle.result("force-abort");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let child = isolated_command(&args)
        .env(
            "WORKFLOW_RUN_FIXTURE_SOCKET",
            listener.local_addr().unwrap().to_string(),
        )
        .env("WORKFLOW_RUN_FIXTURE_MODE", "signal-hold")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (mut control, _) = listener.accept().unwrap();
    let mut report = [0_u8; 5];
    control.read_exact(&mut report).unwrap();
    assert_eq!(report[4], 1);
    let mut event = [0_u8; 1];
    let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();

    kill_process(pid, Signal::INT).unwrap();
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [2]);
    kill_process(pid, Signal::INT).unwrap();
    let output = child.wait_with_output().unwrap();

    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let finalization = &terminal["result"]["finalization"];
    assert_eq!(finalization["trigger"], "succeeded");
    assert_eq!(finalization["cancellation"]["reason"], "user_request");
    assert_eq!(finalization["forceAbort"], true);
    assert_eq!(finalization["finalizers"][0]["reason"], "user_request");
}

#[cfg(target_os = "linux")]
#[test]
fn abrupt_owner_loss_terminates_reaps_and_exposes_an_abandoned_attempt() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let bundle = signal_bundle();
    let destination = bundle.result("owner-loss");
    let child = isolated_command(&bundle.args(&destination))
        .env(
            "WORKFLOW_RUN_FIXTURE_SOCKET",
            listener.local_addr().unwrap().to_string(),
        )
        .env("WORKFLOW_RUN_FIXTURE_MODE", "signal-hold")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let (mut control, _) = listener.accept().unwrap();
    let mut report = [0_u8; 5];
    control.read_exact(&mut report).unwrap();
    assert_eq!(report[4], 1);
    let guarded_pid = Pid::from_raw(i32::from_be_bytes(report[..4].try_into().unwrap())).unwrap();
    let owner_pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();

    kill_process(owner_pid, Signal::KILL).unwrap();
    assert!(child.wait_with_output().unwrap().status.code().is_none());
    control
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let mut remaining = Vec::new();
    control.read_to_end(&mut remaining).unwrap();
    assert!(remaining.is_empty());

    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("state.json")).unwrap()).unwrap();
    assert_eq!(
        state["attempts"][0]["processGuards"][0]["processGroupId"],
        guarded_pid.as_raw_pid()
    );
    assert!(matches!(
        state["attempts"][0]["processGuards"][0]["state"].as_str(),
        Some("prepared" | "released")
    ));
    let status = isolated_command(&[
        "workflow".to_owned(),
        "status".to_owned(),
        destination.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["recovery"]["status"], "abandoned");
    assert_eq!(status["retry"]["eligible"], true);
}

#[cfg(target_os = "linux")]
#[test]
fn owner_death_before_registration_or_continuation_never_executes_user_code() {
    let staging = tempfile::Builder::new()
        .prefix("scherzo-child-guard-boundary-")
        .tempdir_in("/tmp")
        .unwrap();
    let marker_root = tempfile::tempdir().unwrap();
    let marker = marker_root.path().join("user-code-ran");
    let manifest = serde_json::json!({
        "program": b"/bin/sh".to_vec(),
        "arguments": [
            b"-c".to_vec(),
            format!("touch {}", marker.display()).into_bytes(),
        ],
        "environment": Vec::<(Vec<u8>, Vec<u8>)>::new(),
        "streamingStandardInput": false,
    });
    fs::write(
        staging.path().join("launch.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let control = staging.path().join("owner-control");
    let guard_pid = staging.path().join("guard.pid");
    let mut owner = Command::new("/bin/sh")
        .arg("-c")
        .arg(
            "set -eu; mkfifo \"$CONTROL\"; exec 3<>\"$CONTROL\"; \
             \"$GUARD_BIN\" <\"$CONTROL\" 3>&- & guard=$!; \
             printf '%s\\n' \"$guard\" > \"$GUARD_PID\"; wait \"$guard\"",
        )
        .env("CONTROL", &control)
        .env("GUARD_BIN", env!("CARGO_BIN_EXE_scherzo-cloud"))
        .env("GUARD_PID", &guard_pid)
        .env("SCHERZO_INTERNAL_CHILD_GUARD_WORKER", "guard-v1")
        .env("SCHERZO_INTERNAL_CHILD_GUARD_ROOT", staging.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let owner_pid = Pid::from_raw(i32::try_from(owner.id()).unwrap()).unwrap();
    let ready_path = staging.path().join("ready.json");
    for _ in 0..500 {
        if ready_path.is_file() && guard_pid.is_file() {
            break;
        }
        wait_for_process_poll();
    }
    assert!(ready_path.is_file());
    assert!(guard_pid.is_file());
    let ready: serde_json::Value = serde_json::from_slice(&fs::read(&ready_path).unwrap()).unwrap();
    let leader = ready["processGroupId"].as_i64().unwrap().to_string();

    kill_process(owner_pid, Signal::KILL).unwrap();
    assert!(owner.wait().unwrap().code().is_none());
    for _ in 0..500 {
        if !Path::new("/proc").join(&leader).exists() {
            break;
        }
        wait_for_process_poll();
    }
    assert!(
        !marker.is_file(),
        "stopped user code crossed the release boundary"
    );
    assert!(!Path::new("/proc").join(leader).exists());
}

#[cfg(target_os = "linux")]
#[test]
fn guardian_loss_cannot_leave_a_descendant_running() {
    let argv = serde_json::to_string(&[
        "sh",
        "-c",
        "set -eu; printf '%s\\n' \"$$\" > \"$LEADER_FILE\"; printf '%s\\n' \"$PPID\" > \"$GUARDIAN_FILE\"; sleep 300 </dev/null >/dev/null 2>&1 & descendant=$!; printf '%s\\n' \"$descendant\" > \"$DESCENDANT_FILE\"; wait \"$descendant\"",
    ])
    .unwrap();
    let bundle = RunBundle::new(&format!(
        "schemaVersion: 1\nsteps:\n  active:\n    kind: cmd\n    command:\n      argv: {argv}\n"
    ));
    let leader_file = bundle._temporary.path().join("leader.pid");
    let guardian_file = bundle._temporary.path().join("guardian.pid");
    let descendant_file = bundle._temporary.path().join("descendant.pid");
    let destination = bundle.result("guardian-loss");
    let child = isolated_command(&bundle.args(&destination))
        .env("LEADER_FILE", &leader_file)
        .env("GUARDIAN_FILE", &guardian_file)
        .env("DESCENDANT_FILE", &descendant_file)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let owner = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();

    for _ in 0..500 {
        if [&leader_file, &guardian_file, &descendant_file]
            .into_iter()
            .all(|path| path.is_file())
        {
            break;
        }
        wait_for_process_poll();
    }
    let read_pid = |path: &Path| {
        Pid::from_raw(
            fs::read_to_string(path)
                .unwrap()
                .trim()
                .parse::<i32>()
                .unwrap(),
        )
        .unwrap()
    };
    let leader = read_pid(&leader_file);
    let guardian = read_pid(&guardian_file);
    let descendant = read_pid(&descendant_file);

    kill_process(owner, Signal::STOP).unwrap();
    for _ in 0..500 {
        if matches!(process_state(owner), Some(b'T' | b't')) {
            break;
        }
        wait_for_process_poll();
    }
    assert!(
        matches!(process_state(owner), Some(b'T' | b't')),
        "the execution owner must stop before the guard is killed"
    );

    kill_process(guardian, Signal::KILL).unwrap();
    let leader_is_zombie = || process_state(leader) == Some(b'Z');
    for _ in 0..500 {
        if leader_is_zombie() {
            break;
        }
        wait_for_process_poll();
    }
    assert!(
        leader_is_zombie(),
        "the subreaper must retain the leader identity until cleanup"
    );
    assert!(
        Path::new("/proc")
            .join(descendant.as_raw_pid().to_string())
            .exists(),
        "the descendant must still be live before the execution owner handles guard loss"
    );

    kill_process(owner, Signal::CONT).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    let descendant_survived = Path::new("/proc")
        .join(descendant.as_raw_pid().to_string())
        .exists();
    if descendant_survived {
        let _ = kill_process(descendant, Signal::KILL);
    }
    assert!(
        !descendant_survived,
        "guard failure left an executing descendant in the authenticated process group"
    );
}

#[test]
fn output_failure_after_a_signal_keeps_the_first_reason_but_forces_status_one() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let bundle = signal_bundle();
    let destination = bundle.result("signal-then-output-failure");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--plain".to_owned());
    let mut child = isolated_command(&args)
        .env(
            "WORKFLOW_RUN_FIXTURE_SOCKET",
            listener.local_addr().unwrap().to_string(),
        )
        .env("WORKFLOW_RUN_FIXTURE_MODE", "signal-emit")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (mut control, _) = listener.accept().unwrap();
    let mut event = [0_u8; 1];
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [1]);
    let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(pid, Signal::INT).unwrap();
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [2]);
    drop(child.stdout.take());
    control.write_all(&[1]).unwrap();
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [3]);
    let output = child.wait_with_output().unwrap();

    assert_eq!(output.status.code(), Some(1));
    let retained = result_json(&destination);
    assert_eq!(retained["outcome"], "cancelled");
    assert_eq!(retained["cancellation"]["reason"], "user_request");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let resolved_destination = fs::canonicalize(&destination).unwrap();
    assert!(stderr.contains(resolved_destination.to_str().unwrap()));
}

#[test]
fn failure_committed_before_a_signal_remains_the_primary_outcome() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let bundle = failure_then_signal_bundle();
    let destination = bundle.result("failure-then-signal");
    let mut args = bundle.args(&destination);
    args.splice(
        args.len() - 1..args.len() - 1,
        [
            "--plain".to_owned(),
            "--max-parallel".to_owned(),
            "2".to_owned(),
        ],
    );
    let mut child = isolated_command(&args)
        .env(
            "WORKFLOW_RUN_FIXTURE_SOCKET",
            listener.local_addr().unwrap().to_string(),
        )
        .env("WORKFLOW_RUN_FIXTURE_MODE", "signal-exit")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (mut control, _) = listener.accept().unwrap();
    let mut event = [0_u8; 1];
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [1]);

    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        assert_ne!(stdout.read_line(&mut line).unwrap(), 0);
        let mut fields = line.split_whitespace();
        if fields.nth(1) == Some("fail") && fields.next() == Some("failed") {
            break;
        }
    }
    let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(pid, Signal::INT).unwrap();
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [2]);
    let mut remainder = Vec::new();
    stdout.read_to_end(&mut remainder).unwrap();
    let output = child.wait_with_output().unwrap();

    assert_eq!(output.status.code(), Some(1));
    let retained = result_json(&destination);
    assert_eq!(retained["outcome"], "failed");
    assert_eq!(retained["primaryFailure"]["node"]["id"], "fail");
    assert_eq!(retained["cancellation"]["reason"], "user_request");
}

#[test]
fn live_presentation_failure_cancels_quiesces_and_reports_the_published_path() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let bundle = signal_bundle();
    let destination = bundle.result("output-failure");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--plain".to_owned());
    let mut child = isolated_command(&args)
        .env(
            "WORKFLOW_RUN_FIXTURE_SOCKET",
            listener.local_addr().unwrap().to_string(),
        )
        .env("WORKFLOW_RUN_FIXTURE_MODE", "emit")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (mut control, _) = listener.accept().unwrap();
    let mut ready = [0_u8; 1];
    control.read_exact(&mut ready).unwrap();
    assert_eq!(ready, [1]);
    drop(child.stdout.take());
    control.write_all(&[1]).unwrap();
    let output = child.wait_with_output().unwrap();

    assert_eq!(output.status.code(), Some(1));
    let retained = result_json(&destination);
    assert_eq!(retained["outcome"], "cancelled");
    assert_eq!(retained["cancellation"]["reason"], "caller_output_failure");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let resolved_destination = fs::canonicalize(&destination).unwrap();
    assert!(stderr.contains(resolved_destination.to_str().unwrap()));
}

#[test]
fn terminal_json_failure_occurs_after_publication_and_forces_status_one() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    );
    let destination = bundle.result("json-closed");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let mut child = isolated_command(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stdout.take());
    let output = child.wait_with_output().unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(result_json(&destination)["outcome"], "succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("TerminalJsonWriter"));
    let resolved_destination = fs::canonicalize(&destination).unwrap();
    assert!(stderr.contains(resolved_destination.to_str().unwrap()));
}

pub(super) fn signal_bundle() -> RunBundle {
    let argv = fixture_argv();
    RunBundle::new(&format!(
        "schemaVersion: 1\nsteps:\n  active:\n    kind: cmd\n    command:\n      argv: {argv}\n"
    ))
}

pub(super) fn finalizer_signal_bundle() -> RunBundle {
    let argv = fixture_argv();
    RunBundle::new(&format!(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\nfinalizers:\n  active:\n    kind: cmd\n    command:\n      argv: {argv}\n"
    ))
}

fn failure_then_signal_bundle() -> RunBundle {
    let argv = fixture_argv();
    RunBundle::new(&format!(
        "schemaVersion: 1\nsteps:\n  active:\n    kind: cmd\n    command:\n      argv: {argv}\n  fail:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"exit 23\"]\n"
    ))
}

fn write_codex_server_frame(output: &mut impl Write, value: serde_json::Value) {
    serde_json::to_writer(&mut *output, &value).unwrap();
    output.write_all(b"\n").unwrap();
    output.flush().unwrap();
}

fn read_codex_client_frame(input: &mut impl BufRead) -> serde_json::Value {
    let mut line = String::new();
    assert!(input.read_line(&mut line).unwrap() > 0);
    serde_json::from_str(line.trim_end()).unwrap()
}

fn codex_thread_document(cwd: &str) -> serde_json::Value {
    serde_json::json!({
        "id": CODEX_THREAD_ID,
        "sessionId": CODEX_THREAD_ID,
        "forkedFromId": null,
        "parentThreadId": null,
        "ephemeral": true,
        "path": null,
        "cliVersion": "0.147.0",
        "turns": [],
        "cwd": cwd,
        "modelProvider": CODEX_PROVIDER,
    })
}

fn send_codex_item(
    output: &mut impl Write,
    turn_id: &str,
    item_id: &str,
    text: &str,
) -> serde_json::Value {
    write_codex_server_frame(
        output,
        serde_json::json!({
            "method": "item/started",
            "params": {
                "threadId": CODEX_THREAD_ID,
                "turnId": turn_id,
                "item": {"id": item_id, "type": "agentMessage", "text": ""},
            }
        }),
    );
    let item = serde_json::json!({
        "id": item_id,
        "type": "agentMessage",
        "text": text,
        "phase": "final_answer",
    });
    write_codex_server_frame(
        output,
        serde_json::json!({
            "method": "item/completed",
            "params": {
                "threadId": CODEX_THREAD_ID,
                "turnId": turn_id,
                "item": item,
            }
        }),
    );
    item
}

fn send_codex_turn_completed(
    output: &mut impl Write,
    turn_id: &str,
    status: &str,
    items: Vec<serde_json::Value>,
    error: Option<serde_json::Value>,
) {
    let mut turn = serde_json::json!({"id": turn_id, "items": items, "status": status});
    if let Some(error) = error {
        turn["error"] = error;
    }
    write_codex_server_frame(
        output,
        serde_json::json!({
            "method": "turn/completed",
            "params": {"threadId": CODEX_THREAD_ID, "turn": turn}
        }),
    );
}

#[test]
#[ignore = "launched as the local production Codex App Server fixture"]
fn codex_app_server_fixture() {
    let scenario = std::env::var("CODEX_LOCAL_SCENARIO").unwrap();
    eprintln!("codex fixture diagnostic: {scenario}");
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let mut output = OpenOptions::new().write(true).open("/dev/fd/3").unwrap();

    let initialize = read_codex_client_frame(&mut input);
    assert_eq!(initialize["id"], 1);
    write_codex_server_frame(
        &mut output,
        serde_json::json!({
            "id": 1,
            "result": {
                "userAgent": "codex/0.147.0",
                "codexHome": std::env::var("CODEX_HOME").unwrap(),
            }
        }),
    );
    assert_eq!(read_codex_client_frame(&mut input)["method"], "initialized");
    let config_read = read_codex_client_frame(&mut input);
    assert_eq!(config_read["id"], 2);
    write_codex_server_frame(
        &mut output,
        serde_json::json!({
            "method": "configWarning",
            "params": {"summary": "local fixture configuration warning"}
        }),
    );
    write_codex_server_frame(
        &mut output,
        serde_json::json!({
            "id": 2,
            "result": {
                "config": {
                    "developer_instructions": "native fixture instructions",
                    "sqlite_home": std::env::var("CODEX_FIXTURE_SQLITE_HOME").unwrap(),
                    "model_provider": CODEX_PROVIDER,
                },
                "origins": {},
            }
        }),
    );

    let thread = read_codex_client_frame(&mut input);
    assert_eq!(thread["id"], 3);
    assert_eq!(thread["params"]["model"], "fixture/codex");
    assert_eq!(thread["params"]["ephemeral"], true);
    let cwd = thread["params"]["cwd"].as_str().unwrap();
    write_codex_server_frame(
        &mut output,
        serde_json::json!({
            "id": 3,
            "result": {
                "thread": codex_thread_document(cwd),
                "model": "fixture/codex",
                "modelProvider": CODEX_PROVIDER,
                "cwd": cwd,
                "approvalPolicy": "never",
                "sandbox": {"type": "dangerFullAccess"},
            }
        }),
    );

    let turn = read_codex_client_frame(&mut input);
    assert_eq!(turn["id"], 4);
    assert!(matches!(
        turn["params"]["effort"].as_str(),
        Some("high" | "xhigh")
    ));
    write_codex_server_frame(
        &mut output,
        serde_json::json!({
            "method": "thread/started",
            "params": {"thread": codex_thread_document(cwd)}
        }),
    );
    write_codex_server_frame(
        &mut output,
        serde_json::json!({
            "id": 4,
            "result": {
                "turn": {"id": CODEX_TURN_ID, "items": [], "status": "inProgress"}
            }
        }),
    );
    write_codex_server_frame(
        &mut output,
        serde_json::json!({
            "method": "turn/started",
            "params": {
                "threadId": CODEX_THREAD_ID,
                "turn": {"id": CODEX_TURN_ID, "items": [], "status": "inProgress"}
            }
        }),
    );

    match scenario.as_str() {
        "response" => {
            let item = send_codex_item(
                &mut output,
                CODEX_TURN_ID,
                "response-message",
                "codex response",
            );
            send_codex_turn_completed(&mut output, CODEX_TURN_ID, "completed", vec![item], None);
        }
        "no-value" => {
            send_codex_turn_completed(&mut output, CODEX_TURN_ID, "completed", Vec::new(), None);
        }
        "native-failure" => {
            write_codex_server_frame(
                &mut output,
                serde_json::json!({
                    "method": "error",
                    "params": {
                        "threadId": CODEX_THREAD_ID,
                        "turnId": CODEX_TURN_ID,
                        "error": {
                            "message": "native authentication failed",
                            "codexErrorInfo": "unauthorized",
                        },
                        "willRetry": false,
                    }
                }),
            );
            send_codex_turn_completed(
                &mut output,
                CODEX_TURN_ID,
                "failed",
                Vec::new(),
                Some(serde_json::json!({
                    "message": "native authentication failed",
                    "codexErrorInfo": "unauthorized",
                })),
            );
        }
        "result-correction" => {
            let invalid = send_codex_item(
                &mut output,
                CODEX_TURN_ID,
                "invalid-result",
                r#"{"result":"-1"}"#,
            );
            send_codex_turn_completed(&mut output, CODEX_TURN_ID, "completed", vec![invalid], None);
            let correction = read_codex_client_frame(&mut input);
            assert_eq!(correction["id"], 6);
            assert_eq!(correction["method"], "turn/start");
            write_codex_server_frame(
                &mut output,
                serde_json::json!({
                    "id": 6,
                    "result": {
                        "turn": {
                            "id": CODEX_CORRECTION_TURN_ID,
                            "items": [],
                            "status": "inProgress",
                        }
                    }
                }),
            );
            write_codex_server_frame(
                &mut output,
                serde_json::json!({
                    "method": "turn/started",
                    "params": {
                        "threadId": CODEX_THREAD_ID,
                        "turn": {
                            "id": CODEX_CORRECTION_TURN_ID,
                            "items": [],
                            "status": "inProgress",
                        }
                    }
                }),
            );
            let valid = send_codex_item(
                &mut output,
                CODEX_CORRECTION_TURN_ID,
                "valid-result",
                r#"{"result":"7"}"#,
            );
            send_codex_turn_completed(
                &mut output,
                CODEX_CORRECTION_TURN_ID,
                "completed",
                vec![valid],
                None,
            );
        }
        "cancellation" => {
            fs::write(std::env::var_os("CODEX_LOCAL_READY").unwrap(), b"ready\n").unwrap();
            let interrupt = read_codex_client_frame(&mut input);
            assert_eq!(interrupt["id"], 5);
            assert_eq!(interrupt["method"], "turn/interrupt");
            write_codex_server_frame(&mut output, serde_json::json!({"id": 5, "result": {}}));
            send_codex_turn_completed(&mut output, CODEX_TURN_ID, "interrupted", Vec::new(), None);
        }
        other => panic!("unknown local Codex fixture scenario: {other}"),
    }

    let mut trailing = Vec::new();
    input.read_to_end(&mut trailing).unwrap();
    assert!(trailing.is_empty());
}

fn fixture_argv() -> String {
    let executable = std::env::current_exe().unwrap();
    serde_json::to_string(&[
        executable.to_string_lossy().into_owned(),
        "--ignored".to_owned(),
        "--exact".to_owned(),
        SIGNAL_FIXTURE_TEST.to_owned(),
        "--nocapture".to_owned(),
    ])
    .unwrap()
}

#[test]
#[ignore = "launched as a synchronized workflow command fixture"]
fn signal_command_fixture() {
    let address = std::env::var("WORKFLOW_RUN_FIXTURE_SOCKET").unwrap();
    let mode = std::env::var("WORKFLOW_RUN_FIXTURE_MODE").unwrap();
    let mut control = TcpStream::connect(address).unwrap();
    if matches!(mode.as_str(), "signal-emit" | "signal-exit" | "signal-hold") {
        let mut interrupted = control.try_clone().unwrap();
        let exit_on_signal = mode == "signal-exit";
        ctrlc::set_handler(move || {
            interrupted.write_all(&[2]).unwrap();
            if exit_on_signal {
                std::process::exit(0);
            }
        })
        .unwrap();
    }
    if mode == "signal-hold" {
        control
            .write_all(&std::process::id().to_be_bytes())
            .unwrap();
    }
    control.write_all(&[1]).unwrap();
    if matches!(mode.as_str(), "emit" | "signal-emit") {
        let mut release = [0_u8; 1];
        control.read_exact(&mut release).unwrap();
        println!("presentation-failure-trigger");
        std::io::stdout().flush().unwrap();
        control.write_all(&[3]).unwrap();
        if mode == "signal-emit" {
            return;
        }
    }
    let mut blocked = [0_u8; 1];
    let _ = control.read(&mut blocked);
}
