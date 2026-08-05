#[cfg(not(target_os = "macos"))]
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::OwnedFd;
#[cfg(not(target_os = "macos"))]
use std::os::unix::ffi::OsStringExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use rustix::process::{Pid, Signal, kill_process};
use tempfile::TempDir;

use super::{CREDENTIALS_FILE_VARIABLE, DEPLOYMENT_VARIABLES, RUNNER_TELEMETRY_VARIABLES};

const WORKFLOW_PATH: &str = "workflow.yaml";
const SIGNAL_FIXTURE_TEST: &str = "workflow_run::signal_command_fixture";

#[cfg(target_os = "linux")]
fn wait_for_process_poll() {
    let (_sender, receiver) = std::sync::mpsc::channel::<()>();
    assert_eq!(
        receiver.recv_timeout(std::time::Duration::from_millis(10)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    );
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
          printf partial > "$RUN_STATE"
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
          printf partial > "$RUN_STATE"
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
    kill_process(guardian, Signal::KILL).unwrap();
    let leader_stat = Path::new("/proc")
        .join(leader.as_raw_pid().to_string())
        .join("stat");
    let leader_is_zombie = || {
        fs::read_to_string(&leader_stat)
            .ok()
            .and_then(|stat| stat.rsplit_once(") ").map(|(_, fields)| fields.to_owned()))
            .and_then(|fields| fields.split_ascii_whitespace().next().map(str::to_owned))
            .as_deref()
            == Some("Z")
    };
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
