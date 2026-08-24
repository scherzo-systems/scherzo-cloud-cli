use std::fs;
use std::path::Path;
use std::process::Stdio;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use rustix::process::{Pid, Signal, kill_process};

use super::poll_until;
use super::workflow_run::{RunBundle, isolated_command, run};

fn json_run_args(bundle: &RunBundle, run_directory: &Path) -> Vec<String> {
    let mut args = bundle.args(run_directory);
    args.insert(args.len() - 1, "--json".to_owned());
    args
}

fn read_json(path: impl AsRef<Path>) -> serde_json::Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn fixture_source(name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/workflow-run")
            .join(name),
    )
    .unwrap()
}

fn schema(path: &str) -> jsonschema::Validator {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    let schema: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    jsonschema::draft202012::new(&schema).unwrap()
}

fn retry_args(run_directory: &Path, execution_root: &Path) -> Vec<String> {
    vec![
        "workflow".to_owned(),
        "retry".to_owned(),
        "--json".to_owned(),
        "--execution-root".to_owned(),
        execution_root.to_string_lossy().into_owned(),
        run_directory.to_string_lossy().into_owned(),
    ]
}

fn assert_closed_recovery_step(
    step: &serde_json::Value,
    expected_handler: Option<&str>,
    expected_invocations: usize,
) {
    let recovery = step["recovery"].as_object().unwrap();
    let expected_recovery_keys = if expected_handler.is_some() {
        vec![
            "configuredRetries",
            "handlerKind",
            "rounds",
            "schemaVersion",
            "termination",
        ]
    } else {
        vec![
            "configuredRetries",
            "rounds",
            "schemaVersion",
            "termination",
        ]
    };
    assert_eq!(
        recovery.keys().cloned().collect::<Vec<_>>(),
        expected_recovery_keys
    );
    assert_eq!(recovery["schemaVersion"], 1);
    assert_eq!(
        recovery
            .get("handlerKind")
            .and_then(serde_json::Value::as_str),
        expected_handler
    );
    let invocations = step["invocations"].as_array().unwrap();
    assert_eq!(invocations.len(), expected_invocations);
    for invocation in invocations {
        assert_eq!(
            invocation["usage"],
            serde_json::json!({
                "inputTokens": 0,
                "outputTokens": 0
            })
        );
        assert_eq!(invocation["diagnostics"].as_array().unwrap().len(), 2);
    }
}

#[test]
fn local_handlerless_and_command_repair_publish_exact_recovery_evidence() {
    for (name, source, handler, invocations) in [
        (
            "handlerless",
            fixture_source("recovery-handlerless.yaml"),
            None,
            2,
        ),
        (
            "command",
            fixture_source("recovery-command-handler.yaml"),
            Some("cmd"),
            3,
        ),
    ] {
        let bundle = RunBundle::new(&source);
        let run_directory = bundle.result(name);
        let output = run(&json_run_args(&bundle, &run_directory));
        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let live = String::from_utf8(output.stderr).unwrap();
        assert!(live.contains("target execution 2 · round 1/1"));
        if handler.is_some() {
            assert!(live.contains("recovery_handler cmd running · round 1/1"));
            assert!(live.contains("decision recheck"));
        }
        assert!(!live.contains("advisory issue"));
        assert!(live.contains("latest target failure execution · command_exit · exit 75"));
        let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(terminal["outcome"], "succeeded");
        let step = &terminal["result"]["steps"][0];
        assert_eq!(step["state"], "succeeded");
        assert_eq!(
            step["recovery"]["termination"],
            serde_json::json!({
                "kind": "recovered",
                "executionNumber": 2
            })
        );
        assert_closed_recovery_step(step, handler, invocations);
        assert_eq!(
            fs::read(bundle.execution_root().join("target-runs")).unwrap(),
            b"xx"
        );
        assert!(
            step["commandOutput"]["stderr"]["data"]
                .as_str()
                .unwrap()
                .is_empty()
        );
        assert!(!bundle.execution_root().join("decision.json").exists());

        let state = read_json(run_directory.join("state.json"));
        assert_eq!(
            state["attempts"][0]["progress"]["steps"][0]["recovery"],
            step["recovery"]
        );
        assert_eq!(
            state["attempts"][0]["progress"]["accounting"]["observedInvocations"],
            u64::try_from(invocations).unwrap()
        );

        let status = run(&[
            "workflow".to_owned(),
            "status".to_owned(),
            "--json".to_owned(),
            run_directory.to_string_lossy().into_owned(),
        ]);
        assert!(status.status.success());
        let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
        assert!(schema("schemas/workflow-status-result-v1.schema.json").is_valid(&status));
        assert_eq!(
            status["state"]["attempts"][0]["progress"]["steps"][0]["recovery"],
            step["recovery"]
        );
        let view = run(&[
            "workflow".to_owned(),
            "view".to_owned(),
            "--json".to_owned(),
            run_directory.to_string_lossy().into_owned(),
        ]);
        assert!(view.status.success());
        let view: serde_json::Value = serde_json::from_slice(&view.stdout).unwrap();
        assert!(schema("schemas/workflow-view-result-v1.schema.json").is_valid(&view));
        assert_eq!(view["result"]["steps"][0], *step);
        let plain_status = run(&[
            "workflow".to_owned(),
            "status".to_owned(),
            "--plain".to_owned(),
            "--color".to_owned(),
            "never".to_owned(),
            run_directory.to_string_lossy().into_owned(),
        ]);
        let plain_status = String::from_utf8(plain_status.stdout).unwrap();
        assert!(plain_status.contains("step recovery: verify · recovered"));
        assert!(plain_status.contains(&format!("invocations: {invocations} observed")));
        let plain_view = run(&[
            "workflow".to_owned(),
            "view".to_owned(),
            "--plain".to_owned(),
            "--color".to_owned(),
            "never".to_owned(),
            run_directory.to_string_lossy().into_owned(),
        ]);
        let plain_view = String::from_utf8(plain_view.stdout).unwrap();
        assert!(plain_view.contains("recovery recovered at target execution 2"));
        assert!(plain_view.contains("latest target failure execution · command_exit · exit 75"));
        assert!(plain_view.contains(&format!("{invocations} invocations")));

        let result_directory = run_directory.join("attempts/000001/result");
        let artifact = run(&[
            "artifact".to_owned(),
            "validate".to_owned(),
            "--json".to_owned(),
            result_directory.to_string_lossy().into_owned(),
        ]);
        assert!(artifact.status.success());
    }
}

#[test]
fn durable_recovery_is_authoritative_before_handler_and_recheck_launch() {
    let source = "schemaVersion: 1\nsteps:\n  verify:\n    kind: cmd\n    recovery:\n      retries: 1\n      handler:\n        kind: cmd\n        command:\n          argv:\n            - /bin/sh\n            - -c\n            - |\n              set -eu\n              role=false\n              active=false\n              while IFS= read -r line; do\n                case \"$line\" in *'\"role\": \"recovery_handler\"'*) role=true ;; esac\n                case \"$line\" in *'\"state\": \"active\"'*) active=true ;; esac\n              done < \"$RECOVERY_STATE\"\n              $role\n              $active\n              : > handler-observed-durable-state\n              : > repaired\n              printf '%s' '{\"schemaVersion\":1,\"decision\":\"recheck\",\"summary\":\"Durable handler state observed.\",\"reason\":\"The recheck can verify its own durable authorization.\"}' > \"$SCHERZO_RECOVERY_RESULT\"\n    command:\n      argv:\n        - /bin/sh\n        - -c\n        - |\n          set -eu\n          if [ ! -f repaired ]; then exit 75; fi\n          decision=false\n          execution=false\n          while IFS= read -r line; do\n            case \"$line\" in *'\"decision\": \"recheck\"'*) decision=true ;; esac\n            case \"$line\" in *'\"targetExecution\": 2'*) execution=true ;; esac\n          done < \"$RECOVERY_STATE\"\n          $decision\n          $execution\n          : > recheck-observed-durable-state\n";
    let bundle = RunBundle::new(source);
    let run_directory = bundle.result("durability-boundaries");
    let output = isolated_command(&json_run_args(&bundle, &run_directory))
        .env("RECOVERY_STATE", run_directory.join("state.json"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        bundle
            .execution_root()
            .join("handler-observed-durable-state")
            .is_file()
    );
    assert!(
        bundle
            .execution_root()
            .join("recheck-observed-durable-state")
            .is_file()
    );
    let state = read_json(run_directory.join("state.json"));
    let progress = &state["attempts"][0]["progress"];
    assert_eq!(progress["accounting"]["observedInvocations"], 3);
    assert_eq!(progress["accounting"]["settledInvocations"], 3);
    assert!(
        progress["invocations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|invocation| {
                invocation["startedAt"].is_string()
                    && invocation["finishedAt"].is_string()
                    && invocation["usage"].is_object()
                    && invocation["diagnostics"].is_array()
            })
    );
}

#[test]
fn advisory_exhaustion_is_one_terminal_issue_after_all_target_executions() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  lint:\n    kind: cmd\n    failurePolicy: advisory\n    recovery:\n      retries: 1\n    command:\n      argv: [/bin/sh, -c, 'printf x >> lint-runs; exit 75']\n  package:\n    kind: cmd\n    dependsOn: [lint]\n    command:\n      argv: [/bin/sh, -c, 'printf package > package-ran']\n",
    );
    let run_directory = bundle.result("advisory");
    let output = run(&json_run_args(&bundle, &run_directory));
    assert!(output.status.success());
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["outcome"], "succeeded");
    assert_eq!(terminal["result"]["steps"][0]["state"], "failed");
    assert_eq!(
        terminal["result"]["steps"][0]["recovery"]["termination"]["kind"],
        "exhausted"
    );
    assert_eq!(terminal["result"]["steps"][1]["state"], "succeeded");
    assert_eq!(
        fs::read(bundle.execution_root().join("lint-runs")).unwrap(),
        b"xx"
    );
    assert_eq!(
        fs::read(bundle.execution_root().join("package-ran")).unwrap(),
        b"package"
    );
    let plain = String::from_utf8(output.stderr).unwrap();
    assert!(plain.contains("1 advisory issues"));
}

#[test]
fn gave_up_and_handler_failure_preserve_the_raw_target_failure() {
    let scenarios = [
        (
            "gave-up",
            "printf '%s' '{\"schemaVersion\":1,\"decision\":\"gave_up\",\"summary\":\"Inspected the failure.\",\"reason\":\"Automatic repair is unsafe.\"}' > \"$SCHERZO_RECOVERY_RESULT\"",
            "gave_up",
            "gave_up",
        ),
        ("handler-failed", "exit 9", "handler_failed", "failed"),
    ];
    for (name, handler_command, termination, handler_outcome) in scenarios {
        let bundle = RunBundle::new(&format!(
            "schemaVersion: 1\nsteps:\n  verify:\n    kind: cmd\n    recovery:\n      retries: 1\n      handler:\n        kind: cmd\n        command:\n          argv: [/bin/sh, -c, {}]\n    command:\n      argv: [/bin/sh, -c, 'exit 75']\n",
            serde_json::to_string(handler_command).unwrap(),
        ));
        let run_directory = bundle.result(name);
        let output = run(&json_run_args(&bundle, &run_directory));
        assert_eq!(output.status.code(), Some(1));
        let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let step = &terminal["result"]["steps"][0];
        assert_eq!(step["state"], "failed");
        assert_eq!(step["failure"]["cause"]["code"], "command_exit");
        assert_eq!(step["failure"]["cause"]["exitCode"], 75);
        assert_eq!(step["recovery"]["termination"]["kind"], termination);
        assert_eq!(
            step["recovery"]["rounds"][0]["handler"]["outcome"],
            handler_outcome
        );
        assert_eq!(step["invocations"].as_array().unwrap().len(), 2);
    }
}

#[test]
fn exhaustion_remains_terminal_until_retry_starts_fresh_execution_one() {
    let bundle = RunBundle::new(&fixture_source("recovery-exhausted.yaml"));
    let run_directory = bundle.result("exhausted");
    let initial = run(&json_run_args(&bundle, &run_directory));
    assert_eq!(initial.status.code(), Some(1));
    let initial: serde_json::Value = serde_json::from_slice(&initial.stdout).unwrap();
    let failed = &initial["result"]["steps"][0];
    assert_eq!(failed["state"], "failed");
    assert_eq!(failed["failure"]["cause"]["exitCode"], 75);
    assert_eq!(
        failed["recovery"]["termination"],
        serde_json::json!({
            "kind": "exhausted",
            "executionNumber": 3
        })
    );
    assert_closed_recovery_step(failed, None, 3);
    assert!(!bundle.execution_root().join("recovered.txt").exists());

    let retry_root = bundle.result("retry-execution");
    fs::create_dir(&retry_root).unwrap();
    fs::write(retry_root.join("retry-success"), b"").unwrap();
    let retried = run(&retry_args(&run_directory, &retry_root));
    assert!(retried.status.success());
    let retried: serde_json::Value = serde_json::from_slice(&retried.stdout).unwrap();
    let retried_step = &retried["result"]["steps"][0];
    assert_eq!(retried["attemptNumber"], 2);
    assert_eq!(retried_step["state"], "succeeded");
    assert!(retried_step.get("recovery").is_none());
    assert!(retried_step.get("invocations").is_none());
    assert_eq!(fs::read(retry_root.join("target-runs")).unwrap(), b"x");

    let state = read_json(run_directory.join("state.json"));
    assert!(
        state["attempts"][1]["progress"]
            .get("invocations")
            .is_none()
    );
    assert!(
        state["attempts"][1]["progress"]["steps"][0]
            .get("recovery")
            .is_none()
    );
}

#[test]
fn active_status_reports_handler_kind_and_recheck_decision() {
    let source = "schemaVersion: 1\nsteps:\n  verify:\n    kind: cmd\n    recovery:\n      retries: 1\n      handler:\n        kind: cmd\n        command:\n          argv:\n            - /bin/sh\n            - -c\n            - |\n              set -eu\n              mkfifo handler-release\n              : > handler-ready\n              read -r ignored < handler-release\n              printf '%s' '{\"schemaVersion\":1,\"decision\":\"recheck\",\"summary\":\"The handler authorized a recheck.\",\"reason\":\"The status projection must retain this decision.\"}' > \"$SCHERZO_RECOVERY_RESULT\"\n    command:\n      argv:\n        - /bin/sh\n        - -c\n        - |\n          set -eu\n          if [ ! -f first-target-failed ]; then\n            : > first-target-failed\n            exit 75\n          fi\n          mkfifo target-release\n          : > target-ready\n          read -r ignored < target-release\n";
    let bundle = RunBundle::new(source);
    let run_directory = bundle.result("active-status");
    let child = isolated_command(&json_run_args(&bundle, &run_directory))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    poll_until(
        "recovery handler status readiness",
        || {
            let handler_running = fs::read(run_directory.join("state.json"))
                .ok()
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                .and_then(|state| {
                    state
                        .pointer("/attempts/0/progress/steps/0/recovery/active/handlerState")
                        .and_then(serde_json::Value::as_str)
                        .map(|state| state == "running")
                })
                .unwrap_or(false);
            (
                bundle.execution_root().join("handler-ready").exists(),
                handler_running,
            )
        },
        |(ready, running)| *ready && *running,
    );
    let handler_status = run(&[
        "workflow".to_owned(),
        "status".to_owned(),
        "--plain".to_owned(),
        "--color".to_owned(),
        "never".to_owned(),
        run_directory.to_string_lossy().into_owned(),
    ]);
    assert!(handler_status.status.success());
    assert!(
        String::from_utf8(handler_status.stdout)
            .unwrap()
            .contains("recovery_handler cmd running · round 1")
    );
    fs::write(
        bundle.execution_root().join("handler-release"),
        b"continue\n",
    )
    .unwrap();

    poll_until(
        "target recheck status readiness",
        || bundle.execution_root().join("target-ready").exists(),
        |ready| *ready,
    );
    let target_status = run(&[
        "workflow".to_owned(),
        "status".to_owned(),
        "--plain".to_owned(),
        "--color".to_owned(),
        "never".to_owned(),
        run_directory.to_string_lossy().into_owned(),
    ]);
    assert!(target_status.status.success());
    let target_status = String::from_utf8(target_status.stdout).unwrap();
    assert!(target_status.contains("target execution 2"));
    assert!(target_status.contains("decision recheck"));
    fs::write(
        bundle.execution_root().join("target-release"),
        b"continue\n",
    )
    .unwrap();

    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cancellation_quiesces_the_ready_handler_and_publishes_no_recheck() {
    let bundle = RunBundle::new(&fixture_source("recovery-cancellation.yaml"));
    let run_directory = bundle.result("cancelled");
    let child = isolated_command(&json_run_args(&bundle, &run_directory))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let ready = bundle.execution_root().join("recovery-handler-ready");
    poll_until(
        "recovery handler readiness",
        || ready.exists(),
        |ready| *ready,
    );

    let owner = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(owner, Signal::INT).unwrap();
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(130));
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let step = &terminal["result"]["steps"][0];
    assert_eq!(terminal["outcome"], "cancelled");
    assert_eq!(step["state"], "cancelled");
    assert_eq!(step["recovery"]["termination"]["kind"], "cancelled");
    assert_eq!(
        step["recovery"]["termination"]["activeRole"],
        "recovery_handler"
    );
    assert_eq!(step["invocations"][1]["state"], "cancelled");
    assert!(!bundle.execution_root().join("unexpected-recheck").exists());

    let state = read_json(run_directory.join("state.json"));
    assert!(
        state["attempts"][0]["processGuards"]
            .as_array()
            .unwrap()
            .iter()
            .all(|guard| guard["state"] == "quiesced")
    );
}

#[test]
fn status_and_view_dispatch_unsupported_recovery_versions_without_rewriting() {
    let bundle = RunBundle::new(&fixture_source("recovery-handlerless.yaml"));
    let run_directory = bundle.result("unsupported");
    assert!(
        run(&json_run_args(&bundle, &run_directory))
            .status
            .success()
    );

    let mut state = read_json(run_directory.join("state.json"));
    state["attempts"][0]["progress"]["steps"][0]["recovery"]["schemaVersion"] =
        serde_json::json!(2);
    write_json(run_directory.join("state.json"), &state);
    let unsupported_state_bytes = fs::read(run_directory.join("state.json")).unwrap();
    let status = run(&[
        "workflow".to_owned(),
        "status".to_owned(),
        "--json".to_owned(),
        run_directory.to_string_lossy().into_owned(),
    ]);
    assert_eq!(status.status.code(), Some(1));
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["error"]["code"], "recovery_schema_unsupported");
    assert_eq!(
        fs::read(run_directory.join("state.json")).unwrap(),
        unsupported_state_bytes
    );

    state["attempts"][0]["progress"]["steps"][0]["recovery"]["schemaVersion"] =
        serde_json::json!(1);
    write_json(run_directory.join("state.json"), &state);
    let result_path = run_directory.join("attempts/000001/result/result.json");
    let mut published = read_json(&result_path);
    published["steps"][0]["recovery"]["schemaVersion"] = serde_json::json!(2);
    write_json(&result_path, &published);
    let bytes_before = fs::read(&result_path).unwrap();
    let artifact = run(&[
        "artifact".to_owned(),
        "validate".to_owned(),
        "--json".to_owned(),
        result_path.parent().unwrap().to_string_lossy().into_owned(),
    ]);
    assert_eq!(artifact.status.code(), Some(1));
    let artifact: serde_json::Value = serde_json::from_slice(&artifact.stdout).unwrap();
    assert!(
        artifact["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "recovery_schema_unsupported")
    );

    let view = run(&[
        "workflow".to_owned(),
        "view".to_owned(),
        "--json".to_owned(),
        run_directory.to_string_lossy().into_owned(),
    ]);
    assert_eq!(view.status.code(), Some(1));
    let view: serde_json::Value = serde_json::from_slice(&view.stdout).unwrap();
    assert_eq!(view["error"]["code"], "recovery_schema_unsupported");
    assert_eq!(fs::read(result_path).unwrap(), bytes_before);
}

#[test]
fn terminal_pending_publication_reconciles_the_committed_result() {
    let bundle = RunBundle::new(&fixture_source("recovery-handlerless.yaml"));
    let run_directory = bundle.result("publication-reconciliation");
    assert!(
        run(&json_run_args(&bundle, &run_directory))
            .status
            .success()
    );

    let result_path = run_directory.join("attempts/000001/result/result.json");
    let result_bytes = fs::read(&result_path).unwrap();
    let mut state = read_json(run_directory.join("state.json"));
    state["attempts"][0]["result"] = serde_json::json!({
        "status": "not_published",
        "reason": "publication_pending"
    });
    write_json(run_directory.join("state.json"), &state);

    let status = run(&[
        "workflow".to_owned(),
        "status".to_owned(),
        "--json".to_owned(),
        run_directory.to_string_lossy().into_owned(),
    ]);
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(
        status["state"]["attempts"][0]["result"],
        serde_json::json!({
            "status": "published",
            "relativeDirectory": "attempts/000001/result"
        })
    );

    let view = run(&[
        "workflow".to_owned(),
        "view".to_owned(),
        "--json".to_owned(),
        run_directory.to_string_lossy().into_owned(),
    ]);
    assert!(view.status.success());
    assert_eq!(fs::read(result_path).unwrap(), result_bytes);
}

#[test]
fn archived_view_rejects_invocation_bytes_that_disagree_with_durable_evidence() {
    let bundle = RunBundle::new(&fixture_source("recovery-handlerless.yaml"));
    let run_directory = bundle.result("diagnostic-parity");
    assert!(
        run(&json_run_args(&bundle, &run_directory))
            .status
            .success()
    );

    let result_path = run_directory.join("attempts/000001/result/result.json");
    let mut published = read_json(&result_path);
    let stream = &mut published["steps"][0]["invocations"][0]["diagnostics"][1]["stream"];
    let mut bytes = BASE64_STANDARD
        .decode(stream["data"].as_str().unwrap())
        .unwrap();
    assert!(!bytes.is_empty());
    bytes[0] ^= 1;
    stream["data"] = serde_json::Value::String(BASE64_STANDARD.encode(bytes));
    write_json(&result_path, &published);

    let view = run(&[
        "workflow".to_owned(),
        "view".to_owned(),
        "--json".to_owned(),
        run_directory.to_string_lossy().into_owned(),
    ]);
    assert_eq!(
        view.status.code(),
        Some(1),
        "archived view accepted invocation bytes that differ from the immutable durable stream"
    );
    let view: serde_json::Value = serde_json::from_slice(&view.stdout).unwrap();
    assert_eq!(view["error"]["code"], "published_result_invalid");
}

fn write_json(path: impl AsRef<Path>, value: &serde_json::Value) {
    let mut bytes = serde_json::to_vec_pretty(value).unwrap();
    bytes.push(b'\n');
    fs::write(path, bytes).unwrap();
}
