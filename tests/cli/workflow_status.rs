use std::fs;
use std::io::{ErrorKind, Read};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

#[cfg(target_os = "linux")]
use nix::errno::Errno;
#[cfg(target_os = "linux")]
use nix::sys::signal::killpg;
#[cfg(target_os = "linux")]
use nix::unistd::Pid as NixPid;
use rustix::process::{Pid, Signal, kill_process};

#[cfg(target_os = "linux")]
use super::poll_until;
use super::workflow_run::{
    RunBundle, finalizer_signal_bundle, isolated_command, run, signal_bundle,
};

fn successful_bundle() -> RunBundle {
    RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    )
}

fn completed_run(name: &str) -> (RunBundle, PathBuf) {
    let bundle = successful_bundle();
    let destination = bundle.result(name);
    let output = run(&bundle.args(&destination));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    (bundle, destination)
}

fn status_args(run_directory: &Path, options: &[&str]) -> Vec<String> {
    let mut args = vec![
        "workflow".to_owned(),
        "status".to_owned(),
        run_directory.to_string_lossy().into_owned(),
    ];
    args.extend(options.iter().map(|option| (*option).to_owned()));
    args
}

fn status(run_directory: &Path, options: &[&str]) -> std::process::Output {
    isolated_command(&status_args(run_directory, options))
        .output()
        .unwrap()
}

fn status_json(run_directory: &Path) -> (std::process::Output, serde_json::Value) {
    let output = status(run_directory, &["--json"]);
    let value = serde_json::from_slice(&output.stdout).unwrap();
    (output, value)
}

#[cfg(target_os = "linux")]
fn wait_for_process_group_quiescence(process_group: i32) {
    let process_group_path = Path::new("/proc").join(process_group.to_string());
    let process_group = NixPid::from_raw(process_group);
    let _final_observation = poll_until(
        "guarded process group quiescence",
        || {
            (
                killpg(process_group, None::<nix::sys::signal::Signal>),
                process_group_path.exists(),
            )
        },
        |(observation, leader_exists)| matches!(observation, Err(Errno::ESRCH)) && !leader_exists,
    );
}

fn read_state(run_directory: &Path) -> serde_json::Value {
    serde_json::from_slice(&fs::read(run_directory.join("state.json")).unwrap()).unwrap()
}

fn write_state(run_directory: &Path, state: &serde_json::Value) {
    let mut bytes = serde_json::to_vec_pretty(state).unwrap();
    bytes.push(b'\n');
    fs::write(run_directory.join("state.json"), bytes).unwrap();
}

fn status_schema() -> jsonschema::Validator {
    let schema: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/schemas/workflow-status-result-v1.schema.json"
    )))
    .unwrap();
    jsonschema::draft202012::new(&schema).unwrap()
}

#[test]
fn live_finalization_status_is_incomplete_role_aware_and_retry_ineligible() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let bundle = finalizer_signal_bundle();
    let run_directory = bundle.result("live-finalization");
    let mut args = bundle.args(&run_directory);
    args.insert(args.len() - 1, "--json".to_owned());
    let child = isolated_command(&args)
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
    let mut ready = [0_u8; 1];
    control.read_exact(&mut ready).unwrap();
    assert_eq!(ready, [1]);

    let (output, result) = status_json(&run_directory);
    assert!(output.status.success());
    assert!(status_schema().is_valid(&result));
    assert_eq!(result["state"]["attempts"][0]["state"], "running");
    assert_eq!(
        result["state"]["attempts"][0]["finalization"]["complete"],
        false
    );
    assert_eq!(
        result["state"]["attempts"][0]["finalization"]["trigger"],
        "succeeded"
    );
    assert_eq!(
        result["state"]["attempts"][0]["finalization"]["finalizers"][0]["role"],
        "finalizer"
    );
    assert!(matches!(
        result["state"]["attempts"][0]["finalization"]["finalizers"][0]["state"].as_str(),
        Some("starting" | "running")
    ));
    assert_eq!(result["recovery"]["status"], "active");
    assert_eq!(
        result["retry"],
        serde_json::json!({"eligible": false, "reason": "run_locked"})
    );

    let owner = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(owner, Signal::INT).unwrap();
    assert_eq!(child.wait_with_output().unwrap().status.code(), Some(130));

    let (output, settled) = status_json(&run_directory);
    assert!(output.status.success());
    assert!(status_schema().is_valid(&settled));
    assert_eq!(settled["state"]["attempts"][0]["state"], "cancelled");
    assert_eq!(
        settled["state"]["attempts"][0]["finalization"]["complete"],
        true
    );
    assert_eq!(
        settled["state"]["attempts"][0]["finalization"]["cancellation"]["reason"],
        "user_request"
    );
    assert_eq!(settled["retry"]["eligible"], true);
}

#[test]
fn status_schema_rejects_active_ownership_with_eligible_retry() {
    let mut value: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/workflow-status/v1/valid/settled-interrupted.json"
    )))
    .unwrap();
    value["recovery"] = serde_json::json!({"status": "active"});

    assert!(
        !status_schema().is_valid(&value),
        "active ownership requires retry reason run_locked"
    );
}

#[test]
fn closed_status_schema_accepts_positive_and_rejects_negative_fixtures() {
    let validator = status_schema();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/workflow-status/v1");
    for (category, valid) in [("valid", true), ("invalid", false)] {
        let mut fixtures = fs::read_dir(root.join(category))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        fixtures.sort_unstable_by_key(std::fs::DirEntry::file_name);
        assert!(!fixtures.is_empty());
        for fixture in fixtures {
            let value: serde_json::Value =
                serde_json::from_slice(&fs::read(fixture.path()).unwrap()).unwrap();
            assert_eq!(
                validator.is_valid(&value),
                valid,
                "unexpected schema result for {}",
                fixture.path().display()
            );
        }
    }
}

#[test]
fn status_json_and_plain_are_closed_read_only_snapshots() {
    let (_bundle, run_directory) = completed_run("settled");
    let run_before = fs::read(run_directory.join("run.json")).unwrap();
    let state_before = fs::read(run_directory.join("state.json")).unwrap();
    let lock_before = fs::metadata(run_directory.join("run.lock")).unwrap();

    let output = status(&run_directory, &["--json", "--color", "always"]);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert!(!output.stdout.contains(&0x1b));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(status_schema().is_valid(&result));
    assert_eq!(
        result
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        [
            "command",
            "exitStatus",
            "outcome",
            "recovery",
            "retry",
            "run",
            "runDirectory",
            "schemaVersion",
            "state",
        ]
    );
    assert_eq!(result["schemaVersion"], 1);
    assert_eq!(result["command"], "scherzo-cloud workflow status");
    assert_eq!(result["outcome"], "status");
    assert_eq!(result["exitStatus"], 0);
    assert_eq!(result["recovery"], serde_json::json!({"status": "settled"}));
    assert_eq!(
        result["retry"],
        serde_json::json!({
            "eligible": false,
            "reason": "latest_attempt_succeeded"
        })
    );
    let expected_run: serde_json::Value = serde_json::from_slice(&run_before).unwrap();
    let expected_state: serde_json::Value = serde_json::from_slice(&state_before).unwrap();
    assert_eq!(result["run"], expected_run);
    assert_eq!(result["state"], expected_state);
    assert!(result.get("private").is_none());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(".private"));

    let plain = status(&run_directory, &[]);
    assert!(plain.status.success());
    assert!(plain.stderr.is_empty());
    assert!(!plain.stdout.contains(&0x1b));
    assert!(!plain.stdout.is_empty());

    let colored = status(&run_directory, &["--plain", "--color", "always"]);
    assert!(colored.status.success());
    assert!(colored.stdout.contains(&0x1b));
    assert!(
        String::from_utf8_lossy(&colored.stdout).contains("\u{1b}[0m (attempts/000001/result)")
    );

    assert_eq!(
        fs::read(run_directory.join("run.json")).unwrap(),
        run_before
    );
    assert_eq!(
        fs::read(run_directory.join("state.json")).unwrap(),
        state_before
    );
    let lock_after = fs::metadata(run_directory.join("run.lock")).unwrap();
    assert_eq!(lock_after.len(), lock_before.len());
}

#[test]
fn active_owner_has_retry_precedence_and_status_does_not_signal_work() {
    let bundle = signal_bundle();
    let run_directory = bundle.result("active");
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

    let (output, result) = status_json(&run_directory);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(result["recovery"], serde_json::json!({"status": "active"}));
    assert_eq!(
        result["retry"],
        serde_json::json!({"eligible": false, "reason": "run_locked"})
    );
    control
        .set_read_timeout(Some(Duration::from_millis(20)))
        .unwrap();
    let error = control.read_exact(&mut event).unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::WouldBlock | ErrorKind::TimedOut
    ));
    control.set_read_timeout(None).unwrap();

    let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(pid, Signal::INT).unwrap();
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [2]);
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(130));
}

#[test]
fn orphaned_guarded_command_is_reaped_and_retry_eligible() {
    let bundle = signal_bundle();
    let run_directory = bundle.result("guarded-owner-loss");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut child = isolated_command(&bundle.args(&run_directory))
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

    let state = read_state(&run_directory);
    assert_eq!(
        state["attempts"][0]["processGuards"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        state["attempts"][0]["progress"]["outstandingActions"][0]["kind"],
        "start_step"
    );

    let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(pid, Signal::KILL).unwrap();
    assert!(!child.wait().unwrap().success());

    control
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut remaining = Vec::new();
    control.read_to_end(&mut remaining).unwrap();
    let (output, result) = status_json(&run_directory);

    assert!(output.status.success());
    assert_eq!(result["recovery"]["status"], "abandoned");
    assert_eq!(result["retry"], serde_json::json!({"eligible": true}));
}

#[cfg(target_os = "linux")]
#[test]
fn orphaned_guarded_cancellation_is_reaped_and_retry_eligible() {
    let bundle = signal_bundle();
    let run_directory = bundle.result("guarded-cancellation-owner-loss");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut child = isolated_command(&bundle.args(&run_directory))
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
    let mut process_group = [0_u8; std::mem::size_of::<u32>()];
    control.read_exact(&mut process_group).unwrap();
    let process_group_raw = i32::try_from(u32::from_be_bytes(process_group)).unwrap();
    let mut event = [0_u8; 1];
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [1]);
    let owner = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(owner, Signal::INT).unwrap();
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [2]);

    let state = read_state(&run_directory);
    assert_eq!(
        state["attempts"][0]["progress"]["outstandingActions"][0]["kind"],
        "cancel_step"
    );
    assert_eq!(
        state["attempts"][0]["processGuards"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    kill_process(owner, Signal::KILL).unwrap();
    assert!(!child.wait().unwrap().success());
    control
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut remaining = Vec::new();
    control.read_to_end(&mut remaining).unwrap();

    // Fixture EOF precedes the independent guard's reap boundary.
    wait_for_process_group_quiescence(process_group_raw);
    let (output, result) = status_json(&run_directory);
    assert!(output.status.success());
    assert_eq!(result["recovery"]["status"], "abandoned");
    assert_eq!(result["retry"], serde_json::json!({"eligible": true}));
}

#[test]
fn status_distinguishes_interrupted_abandoned_and_unproven_ownership() {
    let (_bundle, run_directory) = completed_run("recoverable");
    let mut state = read_state(&run_directory);
    let attempt = &mut state["attempts"][0];
    attempt["state"] = serde_json::json!("interrupted");
    attempt["interruption"] = serde_json::json!({
        "cause": "execution_owner_lost",
        "executionMayHaveStarted": true,
        "cancellationRequested": false
    });
    attempt["result"] = serde_json::json!({"status": "not_published", "reason": "interrupted"});
    write_state(&run_directory, &state);

    let (output, interrupted) = status_json(&run_directory);
    assert!(output.status.success());
    assert_eq!(interrupted["state"]["attempts"][0]["state"], "interrupted");
    assert_eq!(interrupted["recovery"]["status"], "settled");
    assert_eq!(interrupted["retry"], serde_json::json!({"eligible": true}));

    let mut state = read_state(&run_directory);
    let attempt = &mut state["attempts"][0];
    attempt["state"] = serde_json::json!("running");
    attempt.as_object_mut().unwrap().remove("settledAt");
    attempt.as_object_mut().unwrap().remove("interruption");
    attempt["result"] =
        serde_json::json!({"status": "not_published", "reason": "attempt_nonterminal"});
    attempt["processGuards"] = serde_json::json!([]);
    write_state(&run_directory, &state);

    let (output, abandoned) = status_json(&run_directory);
    assert!(output.status.success());
    assert_eq!(abandoned["recovery"]["status"], "abandoned");
    assert_eq!(abandoned["retry"], serde_json::json!({"eligible": true}));

    let mut state = read_state(&run_directory);
    let attempt = &mut state["attempts"][0];
    attempt["processGuards"] = serde_json::json!([{
        "guardId": "11111111-1111-4111-8111-111111111111",
        "actionId": 1,
        "stepId": "complete",
        "nodeRole": "step",
        "state": "released",
        "executionHost": attempt["owner"]["executionHost"].clone(),
        "processGroupId": 1,
        "liveness": {
            "kind": "guard_handle_identity",
            "value": "retained-handle"
        }
    }]);
    write_state(&run_directory, &state);

    let (output, unproven) = status_json(&run_directory);
    assert!(output.status.success());
    assert!(status_schema().is_valid(&unproven));
    assert_eq!(unproven["recovery"]["status"], "ownership_unproven");
    assert_eq!(
        unproven["recovery"]["reason"],
        "process_identity_inspection_unavailable"
    );
    assert_eq!(
        unproven["recovery"]["guardIds"],
        serde_json::json!(["11111111-1111-4111-8111-111111111111"])
    );
    assert_eq!(
        unproven["retry"],
        serde_json::json!({"eligible": false, "reason": "ownership_unproven"})
    );
    let plain = status(&run_directory, &[]);
    assert!(plain.status.success());
    assert!(!plain.stdout.is_empty());
}

#[test]
fn signals_interrupt_blocked_status_output_without_a_valid_object() {
    let (_bundle, run_directory) = completed_run("signal");
    let mut state = read_state(&run_directory);
    state["attempts"][0]["processGuards"] = serde_json::json!([]);
    state["attempts"][0]["progress"]["steps"] = serde_json::Value::Array(
        (0..20_000)
            .map(|index| {
                serde_json::json!({
                    "id": format!("step-{index:05}"),
                    "role": "step",
                    "failurePolicy": "required",
                    "state": "succeeded"
                })
            })
            .collect(),
    );
    write_state(&run_directory, &state);
    let state_size = fs::metadata(run_directory.join("state.json"))
        .unwrap()
        .len();
    assert!((1_048_576..4_194_304).contains(&state_size));

    for (signal, expected_status) in [(Signal::INT, 130), (Signal::TERM, 143)] {
        let mut child = isolated_command(&status_args(&run_directory, &["--json"]))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let mut bytes = vec![0_u8; 1];
        stdout.read_exact(&mut bytes).unwrap();

        let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
        kill_process(pid, signal).unwrap();
        let exit = child.wait().unwrap();
        stdout.read_to_end(&mut bytes).unwrap();
        let mut diagnostic = Vec::new();
        stderr.read_to_end(&mut diagnostic).unwrap();

        assert_eq!(exit.code(), Some(expected_status));
        assert!(serde_json::from_slice::<serde_json::Value>(&bytes).is_err());
        assert!(diagnostic.is_empty());
    }
}

#[test]
fn status_usage_errors_return_two_without_a_status_object() {
    for args in [
        vec!["workflow", "status", "--json"],
        vec!["workflow", "status", "/tmp/run", "--plain", "--json"],
        vec![
            "workflow",
            "status",
            "/tmp/run",
            "--execution-root",
            "/tmp/execution",
        ],
        vec!["workflow", "status", "--run-dir", "/tmp/run"],
    ] {
        let args = args.into_iter().map(str::to_owned).collect::<Vec<_>>();
        let output = isolated_command(&args).output().unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}

#[test]
fn invalid_run_directory_is_structured_and_missing_lock_is_not_created() {
    let (_bundle, run_directory) = completed_run("missing-lock");
    fs::remove_file(run_directory.join("run.lock")).unwrap();

    let (output, result) = status_json(&run_directory);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    assert!(status_schema().is_valid(&result));
    assert_eq!(result["outcome"], "error");
    assert_eq!(result["exitStatus"], 1);
    assert_eq!(result["error"]["code"], "run_directory_invalid");
    assert!(!run_directory.join("run.lock").exists());

    let plain = status(&run_directory, &[]);
    assert_eq!(plain.status.code(), Some(1));
    assert!(plain.stdout.is_empty());
    assert!(!plain.stderr.is_empty());
    assert!(!run_directory.join("run.lock").exists());

    let unavailable = status(&run_directory.join("absent"), &["--json"]);
    assert_eq!(unavailable.status.code(), Some(1));
    assert!(unavailable.stderr.is_empty());
    let unavailable: serde_json::Value = serde_json::from_slice(&unavailable.stdout).unwrap();
    assert_eq!(unavailable["error"]["code"], "run_directory_unavailable");
    assert!(unavailable.get("runDirectory").is_none());
}
