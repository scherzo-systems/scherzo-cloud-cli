#[cfg(not(target_os = "macos"))]
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
use rustix::process::{Pid, Signal, kill_process};
use tempfile::TempDir;

use super::pi_installation::{COMPLETE_HELP, PiFixture, quote};
use super::{CREDENTIALS_FILE_VARIABLE, DEPLOYMENT_VARIABLES, RUNNER_TELEMETRY_VARIABLES};

const WORKFLOW_PATH: &str = "workflow.yaml";
const SIGNAL_FIXTURE_TEST: &str = "workflow_run::signal_command_fixture";
const TUI_HANDSHAKE_VARIABLE: &str = "SCHERZO_INTERNAL_WORKFLOW_RUN_TUI_HANDSHAKE";

#[cfg(target_os = "linux")]
fn wait_for_process_poll() {
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
fn open_tui_pty() -> (OwnedFd, OwnedFd) {
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
fn spawn_tui_run(
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
}

impl RunBundle {
    pub(super) fn new(source: &str) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        let result_parent = temporary.path().join("results");
        for directory in [&source_root, &execution_root, &result_parent] {
            fs::create_dir(directory).unwrap();
        }
        fs::write(source_root.join(WORKFLOW_PATH), source).unwrap();
        Self {
            _temporary: temporary,
            source_root,
            execution_root,
            result_parent,
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
            WORKFLOW_PATH.to_owned(),
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
            } else if system.contains("MODE_RESPONSE") {
                serde_json::json!({
                    "kind": "text",
                    "blocks": ["agent ", "response"],
                    "stopReason": "stop"
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
        kind: agent_response
exports:
  response:
    ref: outputs.answer.response
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
        "cwd=$(pwd); printf '{{\"type\":\"session\",\"version\":3,\"id\":\"00000000-0000-4000-8000-000000000001\",\"timestamp\":\"2026-07-30T12:00:00Z\",\"cwd\":\"%s\"}}\\n' \"$cwd\"; printf '%s\\n' {remaining}"
    )
}

#[test]
fn command_only_run_and_help_remain_pi_independent() {
    let help = run(&["workflow".to_owned(), "run".to_owned(), "--help".to_owned()]);
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    assert!(!help.contains("--pi"));
    assert!(!help.contains("pi-executable"));

    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"/bin/sh\", \"-c\", \"printf command-only\"]\n",
    );
    let empty_path = tempfile::tempdir().unwrap();
    let destination = bundle.result("without-pi");
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
    assert!(attempt_result(&destination).join("result.json").is_file());
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
    let fallback = PiFixture::new("0.83.0", COMPLETE_HELP, true);
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
fn compatible_path_pi_is_validated_once_pinned_and_executes_an_agent() {
    let pi = PiFixture::with_execution("0.83.0", COMPLETE_HELP, true, &response_pi_execution());
    let bundle = RunBundle::new(response_agent_source());
    bundle.write_source("system.md", "system");
    bundle.write_source("message.md", "prompt");
    let destination = bundle.result("agent-response");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let output = isolated_command(&args)
        .env("PATH", pi.path_directory())
        .output()
        .unwrap();

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
    assert!(lines[2].starts_with("--mode json --approve --no-session"));

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
    let pi = PiFixture::with_execution("0.83.0", COMPLETE_HELP, true, barrier_pi_execution());
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
        let pi = PiFixture::with_execution("0.83.0", COMPLETE_HELP, true, barrier_pi_execution());
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
        } else {
            assert!(
                String::from_utf8_lossy(&output.stderr).contains("workflow run output failure")
            );
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
        kind: agent_response
  result:
    kind: agent
    dependsOn: [response]
    agent:
      profile: local
      systemPrompt: result-system.md
      message:
        text:
          - ref: outputs.response.response
    outputs:
      result:
        kind: agent_result
        schema: result.schema.json
exports:
  file:
    ref: outputs.noValue.file
  response:
    ref: outputs.response.response
  result:
    ref: outputs.result.result
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
    let Some(pinned_pi) = option_env!("SCHERZO_PI_CONFORMANCE_EXECUTABLE") else {
        return;
    };
    let bundle = RunBundle::new(mixed_agent_source());
    for (path, text) in [
        ("none-system.md", "MODE_NONE"),
        ("response-system.md", "MODE_RESPONSE"),
        ("result-system.md", "MODE_RESULT"),
        ("message.md", "fixture message"),
    ] {
        bundle.write_source(path, text);
    }
    bundle.write_source(
        "result.schema.json",
        br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["answer","source"],"properties":{"answer":{"const":42},"source":{"const":"agent response"}},"additionalProperties":false}"#,
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
    let provider = serve_fake_provider(&socket_path, 7);
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
    assert_eq!(requests.len(), 7);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["kind"] == "model")
            .count(),
        4
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["outcome"], "succeeded");
    assert_eq!(terminal["result"]["steps"][0]["kind"], "cmd");
    for index in 1..=3 {
        assert_eq!(terminal["result"]["steps"][index]["kind"], "agent");
    }
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0001")).unwrap(),
        b"agent file\n"
    );
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0002")).unwrap(),
        b"agent response"
    );
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0003")).unwrap(),
        br#"{"answer":42,"source":"agent response"}"#
    );
    assert_eq!(
        fs::read(attempt_result(&destination).join("exports/0004")).unwrap(),
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
        "--run-dir".to_owned(),
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
            "schemaVersion: 1\nsteps:\n  rebind:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"set -eu; mv \\\"$ROOT_PATH\\\" \\\"$MOVED_ROOT\\\"; mkdir \\\"$ROOT_PATH\\\"; mkdir \\\"$ROOT_PATH/nested\\\"; mkdir \\\"$MOVED_ROOT/nested\\\"\"]\n  affected:\n    kind: cmd\n    dependsOn: [rebind]\n{cwd_field}    command:\n      argv: [\"sh\", \"-c\", \"printf ran > command-ran\"]\n    outputs:\n      marker:\n        kind: file\n        path: {output_path}\n        mediaType: text/plain\n"
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
            terminal["result"]["primaryFailure"]["step"],
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
    assert_eq!(terminal["result"]["primaryFailure"]["step"], "fail");
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
    assert!(summary.contains("result succeeded · exit 0"));
    assert!(summary.contains("attempts/000001/result"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("release private workflow staging"));
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
    let progress = std::sync::Arc::new(std::sync::Mutex::new("starting"));
    let worker_progress = std::sync::Arc::clone(&progress);
    let worker = std::thread::spawn(move || {
        let _ = finished.send(run_real_pty_boundary_smoke(&worker_progress));
    });

    // Success is driven only by the readiness strings and process exit below. This
    // watchdog bounds failure reporting if the real OS terminal boundary stops making
    // progress; it is not a success condition or an interaction delay.
    let result = match completion.recv_timeout(std::time::Duration::from_secs(15)) {
        Ok(result) => result,
        Err(error) => {
            let progress = *progress
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::panic::panic_any(format!(
                "PTY workflow boundary watchdog expired at {progress}: {error}"
            ));
        }
    };
    result.expect("PTY workflow boundary failed");
    worker
        .join()
        .expect("PTY workflow boundary worker panicked");
}

fn run_real_pty_boundary_smoke(
    progress: &std::sync::Mutex<&'static str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    *progress
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = "terminal handshake connection";
    let (handshake, _) = handshake_listener.accept()?;
    let mut handshake = BufReader::new(handshake);

    *progress
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = "command readiness";
    let mut ready = OpenOptions::new().read(true).open(&ready_fifo)?;
    let mut ready_bytes = [0_u8; 5];
    ready.read_exact(&mut ready_bytes)?;
    if ready_bytes != *b"ready" {
        return Err("workflow command emitted an invalid readiness handshake".into());
    }

    *progress
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = "active q acknowledgement";
    master_writer.write_all(b"q?")?;
    master_writer.flush()?;
    read_terminal_handshake(&mut handshake, "help-open")?;

    *progress
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = "command release";
    let mut release = OpenOptions::new().write(true).open(&release_fifo)?;
    release.write_all(b"release\n")?;
    release.flush()?;
    drop(release);
    *progress
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = "quit readiness";
    read_terminal_handshake(&mut handshake, "quit-eligible")?;

    *progress
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = "process exit";
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
        "schemaVersion: 1\nsteps:\n  race:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"mkdir \\\"$RESULT_TARGET\\\"; printf command-complete\"]\n",
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
    assert!(stdout.contains("workflow succeeded"));
    assert!(stdout.contains("result publication failed"));
    assert!(!stdout.contains(&format!("result: {}", destination.display())));
    assert!(String::from_utf8_lossy(&output.stderr).contains("DestinationExists"));
    assert!(racing_target.is_dir());
    assert!(fs::read_dir(racing_target).unwrap().next().is_none());
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("state.json")).unwrap()).unwrap();
    assert_eq!(
        state["attempts"][0]["result"]["status"],
        "publication_failed"
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
        "--run-dir".to_owned(),
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
    assert!(stderr.contains("workflow run output failure"));
    assert!(stderr.contains(destination.to_str().unwrap()));
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
    assert_eq!(retained["primaryFailure"]["step"], "fail");
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
    assert!(stderr.contains("workflow run output failure"));
    assert!(stderr.contains(destination.to_str().unwrap()));
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
    assert!(stderr.contains(destination.to_str().unwrap()));
}

pub(super) fn signal_bundle() -> RunBundle {
    let argv = fixture_argv();
    RunBundle::new(&format!(
        "schemaVersion: 1\nsteps:\n  active:\n    kind: cmd\n    command:\n      argv: {argv}\n"
    ))
}

fn failure_then_signal_bundle() -> RunBundle {
    let argv = fixture_argv();
    RunBundle::new(&format!(
        "schemaVersion: 1\nsteps:\n  active:\n    kind: cmd\n    command:\n      argv: {argv}\n  fail:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"exit 23\"]\n"
    ))
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
