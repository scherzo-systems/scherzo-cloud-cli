use std::fs;
use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use rustix::process::{Pid, Signal, kill_process};

use super::workflow_run::{
    RunBundle, assert_attempt_resources_released, assert_executor_fault_before_execution,
    isolated_command, run, run_with_unusable_tui, signal_bundle,
};

fn retry_args(run_directory: &Path, execution_root: &Path, options: &[&str]) -> Vec<String> {
    let mut args = vec![
        "workflow".to_owned(),
        "retry".to_owned(),
        run_directory.to_string_lossy().into_owned(),
        "--execution-root".to_owned(),
        execution_root.to_string_lossy().into_owned(),
    ];
    args.extend(options.iter().map(|option| (*option).to_owned()));
    args
}

fn read_state(run_directory: &Path) -> serde_json::Value {
    serde_json::from_slice(&fs::read(run_directory.join("state.json")).unwrap()).unwrap()
}

fn write_state(run_directory: &Path, state: &serde_json::Value) {
    let mut bytes = serde_json::to_vec_pretty(state).unwrap();
    bytes.push(b'\n');
    fs::write(run_directory.join("state.json"), bytes).unwrap();
}

fn environment_retry_bundle() -> RunBundle {
    let argv = serde_json::to_string(&[
        "sh",
        "-c",
        "set -eu; printf '%s\\n' \"$RETRY_PHASE\" >> phases; test \"$RETRY_PHASE\" = retry",
    ])
    .unwrap();
    RunBundle::new(&format!(
        "schemaVersion: 1\nsteps:\n  execute:\n    kind: cmd\n    command:\n      argv: {argv}\n"
    ))
}

fn initial_failed_run(bundle: &RunBundle, name: &str) -> PathBuf {
    let destination = bundle.result(name);
    let mut args = bundle.args(&destination);
    args.insert(args.len() - 1, "--json".to_owned());
    let output = isolated_command(&args)
        .env("RETRY_PHASE", "initial")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "failed");
    destination
}

#[test]
fn retry_uses_the_immutable_bundle_current_environment_and_fresh_attempt() {
    let bundle = environment_retry_bundle();
    let run_directory = initial_failed_run(&bundle, "fresh");
    fs::write(
        bundle.source_root().join("workflow.yaml"),
        b"not: the retained workflow\n",
    )
    .unwrap();
    let unavailable_root = bundle.result("unavailable-execution");
    let rejected = isolated_command(&retry_args(&run_directory, &unavailable_root, &["--json"]))
        .env("RETRY_PHASE", "retry")
        .output()
        .unwrap();
    assert_eq!(rejected.status.code(), Some(1));
    assert!(rejected.stderr.is_empty());
    let rejected: serde_json::Value = serde_json::from_slice(&rejected.stdout).unwrap();
    assert_eq!(rejected["command"], "scherzo-cloud workflow retry");
    assert_eq!(rejected["phase"], "admission");
    assert_eq!(
        rejected["diagnostics"][0]["code"],
        "execution_root_unavailable"
    );
    assert_eq!(
        rejected["runDirectory"],
        fs::canonicalize(&run_directory).unwrap().to_str().unwrap()
    );
    assert!(!run_directory.join("attempts/000002").exists());

    let output = isolated_command(&retry_args(
        &run_directory,
        bundle.execution_root(),
        &["--json"],
    ))
    .env("RETRY_PHASE", "retry")
    .output()
    .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["command"], "scherzo-cloud workflow retry");
    assert_eq!(terminal["outcome"], "succeeded");
    assert_eq!(terminal["attemptNumber"], 2);
    assert_eq!(terminal["result"]["attemptNumber"], 2);
    assert_eq!(terminal["result"]["execution"]["maximumParallelSteps"], 1);
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("phases")).unwrap(),
        "initial\nretry\n"
    );
    let warning = String::from_utf8_lossy(&output.stderr);
    assert!(warning.contains("was used by earlier attempt(s) 1"));
    assert!(warning.contains("does not check cleanliness or mutations"));
    assert!(
        run_directory
            .join("attempts/000001/result/result.json")
            .is_file()
    );
    assert!(
        run_directory
            .join("attempts/000002/result/result.json")
            .is_file()
    );

    let state = read_state(&run_directory);
    assert_eq!(state["currentAttemptNumber"], 2);
    assert_eq!(state["attempts"][0]["state"], "workflow_failed");
    assert_eq!(state["attempts"][1]["trigger"], "explicit_retry");
    assert_eq!(state["attempts"][1]["priorAttemptNumber"], 1);
    assert_eq!(state["attempts"][1]["state"], "succeeded");

    let state_before = fs::read(run_directory.join("state.json")).unwrap();
    let rejected = isolated_command(&retry_args(
        &run_directory,
        bundle.execution_root(),
        &["--json"],
    ))
    .output()
    .unwrap();
    assert_eq!(rejected.status.code(), Some(1));
    assert!(rejected.stderr.is_empty());
    let rejected: serde_json::Value = serde_json::from_slice(&rejected.stdout).unwrap();
    assert_eq!(rejected["command"], "scherzo-cloud workflow retry");
    assert_eq!(rejected["phase"], "retry");
    assert_eq!(rejected["attemptNumber"], 2);
    assert_eq!(
        rejected["diagnostics"][0]["code"],
        "latest_attempt_succeeded"
    );
    assert_eq!(
        fs::read(run_directory.join("state.json")).unwrap(),
        state_before
    );
    assert!(!run_directory.join("attempts/000003").exists());
}

#[test]
fn tui_setup_failure_prevents_retry_execution_and_releases_the_attempt() {
    let bundle = environment_retry_bundle();
    let run_directory = initial_failed_run(&bundle, "tui-setup-failure");
    let alternate_root = bundle.result("tui-setup-execution");
    fs::create_dir(&alternate_root).unwrap();

    let output = run_with_unusable_tui(
        &retry_args(&run_directory, &alternate_root, &[]),
        |command| {
            command.env("RETRY_PHASE", "retry");
        },
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("phases")).unwrap(),
        "initial\n"
    );
    assert!(!alternate_root.join("phases").exists());
    assert_executor_fault_before_execution(&run_directory, 1);
    assert!(!run_directory.join("attempts/000002/result").exists());
    assert_attempt_resources_released(&run_directory);
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr.lines().count(), 1);
    assert!(stderr.contains("TerminalSetup"));
    assert!(!stderr.contains("summary"));
}

#[test]
fn plain_retry_streams_a_human_run_and_rejection() {
    let bundle = environment_retry_bundle();
    let run_directory = initial_failed_run(&bundle, "plain");
    let alternate_root = bundle.result("plain-execution");
    fs::create_dir(&alternate_root).unwrap();

    let output = isolated_command(&retry_args(
        &run_directory,
        &alternate_root,
        &["--plain", "--color", "never"],
    ))
    .env("RETRY_PHASE", "retry")
    .output()
    .unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let presentation = String::from_utf8(output.stdout).unwrap();
    assert!(presentation.contains("result succeeded · exit 0"));
    assert!(presentation.contains("attempts/000002/result"));

    let rejection = run(&retry_args(
        &run_directory,
        &alternate_root,
        &["--plain", "--color", "never"],
    ));
    assert_eq!(rejection.status.code(), Some(1));
    assert!(rejection.stdout.is_empty());
    assert!(String::from_utf8_lossy(&rejection.stderr).contains("latest_attempt_succeeded"));
}

#[test]
fn concurrent_retry_is_a_nonblocking_typed_rejection() {
    let bundle = signal_bundle();
    let run_directory = bundle.result("locked");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let child = isolated_command(&bundle.args(&run_directory))
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

    let output = run(&retry_args(
        &run_directory,
        bundle.execution_root(),
        &["--json"],
    ));
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let rejection: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rejection["phase"], "retry");
    assert_eq!(rejection["attemptNumber"], 1);
    assert_eq!(rejection["diagnostics"][0]["code"], "run_locked");
    assert_eq!(
        read_state(&run_directory)["attempts"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let owner = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(owner, Signal::INT).unwrap();
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [2]);
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(130));
}

#[test]
fn ownership_unproven_rejection_preserves_the_predecessor() {
    let bundle = environment_retry_bundle();
    let run_directory = initial_failed_run(&bundle, "ownership-unproven");
    let mut state = read_state(&run_directory);
    let guard = &mut state["attempts"][0]["processGuards"][0];
    guard["state"] = serde_json::json!("released");
    guard["liveness"]["kind"] = serde_json::json!("guard_handle_identity");
    write_state(&run_directory, &state);
    let state_before = fs::read(run_directory.join("state.json")).unwrap();
    let alternate_root = bundle.result("alternate-execution");
    fs::create_dir(&alternate_root).unwrap();

    let output = isolated_command(&retry_args(&run_directory, &alternate_root, &["--json"]))
        .env("RETRY_PHASE", "retry")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let rejection: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rejection["phase"], "retry");
    assert_eq!(rejection["diagnostics"][0]["code"], "ownership_unproven");
    assert_eq!(
        rejection["diagnostics"][0]["location"]["ownershipReason"],
        "process_identity_inspection_unavailable"
    );
    assert_eq!(
        rejection["diagnostics"][0]["location"]["guardIds"][0],
        state["attempts"][0]["processGuards"][0]["guardId"]
    );
    assert_eq!(
        fs::read(run_directory.join("state.json")).unwrap(),
        state_before
    );
    assert!(!run_directory.join("attempts/000002").exists());
}

#[test]
fn retry_terminal_json_output_failure_preserves_the_published_attempt() {
    let bundle = environment_retry_bundle();
    let run_directory = initial_failed_run(&bundle, "output-failure");
    let mut child = isolated_command(&retry_args(
        &run_directory,
        bundle.execution_root(),
        &["--json"],
    ))
    .env("RETRY_PHASE", "retry")
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    drop(child.stdout.take());
    let output = child.wait_with_output().unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        read_state(&run_directory)["attempts"][1]["state"],
        "succeeded"
    );
    assert!(
        run_directory
            .join("attempts/000002/result/result.json")
            .is_file()
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("TerminalJsonWriter"));
    assert!(stderr.contains("attempts/000002/result"));
}
