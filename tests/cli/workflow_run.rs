#[cfg(not(target_os = "macos"))]
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
#[cfg(not(target_os = "macos"))]
use std::os::unix::ffi::OsStringExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use rustix::process::{Pid, Signal, kill_process};
use tempfile::TempDir;

use super::{CREDENTIALS_FILE_VARIABLE, DEPLOYMENT_VARIABLES, RUNNER_TELEMETRY_VARIABLES};

const WORKFLOW_PATH: &str = "workflow.yaml";
const SIGNAL_FIXTURE_TEST: &str = "workflow_run::signal_command_fixture";

struct RunBundle {
    _temporary: TempDir,
    source_root: PathBuf,
    execution_root: PathBuf,
    result_parent: PathBuf,
}

impl RunBundle {
    fn new(source: &str) -> Self {
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

    fn result(&self, name: &str) -> PathBuf {
        self.result_parent.join(name)
    }

    fn args(&self, result: &Path) -> Vec<String> {
        vec![
            "workflow".to_owned(),
            "run".to_owned(),
            "--source-root".to_owned(),
            self.source_root.to_string_lossy().into_owned(),
            "--execution-root".to_owned(),
            self.execution_root.to_string_lossy().into_owned(),
            "--result-dir".to_owned(),
            result.to_string_lossy().into_owned(),
            WORKFLOW_PATH.to_owned(),
        ]
    }
}

fn isolated_command(args: &[String]) -> Command {
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

fn run(args: &[String]) -> Output {
    isolated_command(args).output().unwrap()
}

fn result_json(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&fs::read(path.join("result.json")).unwrap()).unwrap()
}

fn normalized_result_destination(path: &Path) -> PathBuf {
    fs::canonicalize(path.parent().unwrap())
        .unwrap()
        .join(path.file_name().unwrap())
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
    let normalized_destination = normalized_result_destination(&destination);
    assert_eq!(
        terminal["resultDirectory"],
        normalized_destination.to_str().unwrap()
    );
    assert_eq!(terminal["result"], result_json(&destination));
    assert_eq!(terminal["result"]["execution"]["maximumParallelSteps"], 2);
    assert_eq!(terminal["result"]["steps"][0]["id"], "produce");
    assert_eq!(terminal["result"]["steps"][1]["id"], "consume");
    assert_eq!(
        fs::read(destination.join("exports/0001")).unwrap(),
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
        assert_eq!(terminal["result"]["primaryFailure"]["step"], "affected");
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
        assert!(destination.join("result.json").is_file());
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
        fs::read(destination.join("exports/0001")).unwrap(),
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
        fs::read(destination.join("exports/0001")).unwrap(),
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
fn plain_mode_reports_publication_failure_without_overwriting_the_racing_target() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  race:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"mkdir \\\"$RESULT_TARGET\\\"; printf command-complete\"]\n",
    );
    let destination = bundle.result("racing");
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--plain".to_owned());
    let output = isolated_command(&args)
        .env("RESULT_TARGET", &destination)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("command-complete"));
    assert!(stdout.contains("workflow succeeded"));
    assert!(stdout.contains("result publication failed"));
    assert!(!stdout.contains(&format!("result: {}", destination.display())));
    assert!(String::from_utf8_lossy(&output.stderr).contains("DestinationExists"));
    assert!(destination.is_dir());
    assert!(fs::read_dir(destination).unwrap().next().is_none());
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

fn signal_bundle() -> RunBundle {
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
    if matches!(mode.as_str(), "signal-emit" | "signal-exit") {
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
