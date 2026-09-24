use std::fs;
use std::path::Path;

use super::workflow_run::{RunBundle, git, initialize_git_repository, isolated_command};

const GIT_CONTINUATION_SOURCE: &str = r#"schemaVersion: 1
steps:
  capture:
    kind: cmd
    command: {argv: ["true"]}
    outputs:
      branch: {kind: git_branch, from: workspace}
  check:
    kind: cmd
    dependsOn: [capture]
    command: {argv: ["sh", "-c", "test \"$PHASE\" = continuation"]}
exports:
  branch: {ref: outputs.capture.branch}
"#;

fn continue_args(run: &Path, from: &[&str]) -> Vec<String> {
    let mut args = vec![
        "workflow".to_owned(),
        "continue".to_owned(),
        run.to_string_lossy().into_owned(),
    ];
    for id in from {
        args.extend(["--from".to_owned(), (*id).to_owned()]);
    }
    args.push("--json".to_owned());
    args
}

#[test]
fn continuation_collects_all_missing_candidate_harness_profiles() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"false\"]}\n",
    );
    fs::write(
        bundle.source_root().join("system.md"),
        "You are a test agent.",
    )
    .unwrap();
    fs::write(bundle.source_root().join("message.md"), "Act.").unwrap();
    let replacement = bundle.source_root().join("candidate.yaml");
    fs::write(
        &replacement,
        r#"schemaVersion: 1
agentProfiles:
  pi:
    harness:
      kind: pi
      config:
        model: fixture/model
        thinking: off
  codex:
    harness:
      kind: codex
      config:
        model: fixture/codex
        effort: high
steps:
  first:
    kind: cmd
    command: {argv: ["false"]}
    outputs:
      branch: {kind: git_branch, from: workspace}
  pi:
    kind: agent
    agent:
      profile: pi
      systemPrompt: system.md
      message:
        text:
          - file: message.md
  codex:
    kind: agent
    agent:
      profile: codex
      systemPrompt: system.md
      message:
        text:
          - file: message.md
"#,
    )
    .unwrap();
    let run = bundle.result("all-harnesses");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let mut request = continue_args(&run, &["first"]);
    request.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    let output = isolated_command(&request)
        .env("PATH", "/nonexistent")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let profiles = result["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["location"]["kind"] == "agent_harness")
        .map(|d| d["location"]["profile"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(profiles.len(), 2, "{result}");
    assert!(profiles.contains(&"PiJsonV1"));
    assert!(profiles.contains(&"CodexAppServerV1"));
    assert!(
        result["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"]
                .as_str()
                .is_some_and(|code| code.starts_with("git_"))),
        "{result}"
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["attempts"].as_array().unwrap().len(), 1);
}

#[test]
fn continuation_reports_every_immutable_input_constraint_failure() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
inputs:
  first: {kind: json, schema: first.schema.json}
  second: {kind: json, schema: second.schema.json}
steps:
  fail:
    kind: cmd
    command: {argv: ["false"]}
"#,
    );
    for name in ["first.schema.json", "second.schema.json"] {
        fs::write(
            bundle.source_root().join(name),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
        )
        .unwrap();
    }
    let run = bundle.result("input-constraint-aggregation");
    let mut initial = bundle.args(&run);
    let position = initial.len() - 1;
    initial.splice(
        position..position,
        [
            "--input-json".to_owned(),
            "first".to_owned(),
            r#"{"value":1}"#.to_owned(),
            "--input-json".to_owned(),
            "second".to_owned(),
            r#"{"value":2}"#.to_owned(),
            "--json".to_owned(),
        ],
    );
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    for name in ["first.schema.json", "second.schema.json"] {
        fs::write(
            bundle.source_root().join(name),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}"#,
        )
        .unwrap();
    }
    fs::write(
        bundle.source_root().join("workflow.yaml"),
        r#"schemaVersion: 1
inputs:
  first: {kind: json, schema: first.schema.json}
  second: {kind: json, schema: second.schema.json}
  third: {kind: text}
steps:
  fail:
    kind: cmd
    command: {argv: ["false"]}
"#,
    )
    .unwrap();
    let mut request = continue_args(&run, &["fail"]);
    request.extend([
        "--workflow".to_owned(),
        bundle
            .source_root()
            .join("workflow.yaml")
            .to_string_lossy()
            .into_owned(),
    ]);
    let output = isolated_command(&request).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let diagnostics = result["diagnostics"].as_array().unwrap();
    assert_eq!(diagnostics.len(), 3, "{result}");
    assert!(
        diagnostics
            .iter()
            .all(|diagnostic| { diagnostic["code"] == "continuation_inputs_unsatisfied" })
    );
    assert_eq!(
        diagnostics
            .iter()
            .map(|diagnostic| diagnostic["location"]["input"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["first", "second", "third"]
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["attempts"].as_array().unwrap().len(), 1);
}

#[test]
fn replacement_resolution_failure_uses_the_continuation_diagnostic_vocabulary() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  fail:\n    kind: cmd\n    command: {argv: [\"false\"]}\n",
    );
    let run = bundle.result("invalid-replacement-definition");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let replacement = bundle.source_root().join("invalid.yaml");
    fs::write(&replacement, "schemaVersion: 2\nsteps: {}\n").unwrap();
    let mut request = continue_args(&run, &["fail"]);
    request.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    let output = isolated_command(&request).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["command"], "scherzo-cloud workflow continue");
    assert_eq!(result["phase"], "continuation");
    assert_eq!(
        result["diagnostics"][0]["code"],
        "continuation_definition_invalid"
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["attempts"].as_array().unwrap().len(), 1);
}

#[test]
fn invalid_selection_keeps_independent_immutable_input_diagnostics() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  fail:\n    kind: cmd\n    command: {argv: [\"false\"]}\n",
    );
    let run = bundle.result("invalid-selection-and-input");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let replacement = bundle.source_root().join("replacement.yaml");
    fs::write(
        &replacement,
        "schemaVersion: 1\ninputs:\n  required: {kind: text}\nsteps:\n  fail:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
    )
    .unwrap();
    let mut request = continue_args(&run, &["missing"]);
    request.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    let output = isolated_command(&request).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["diagnostics"].as_array().unwrap().len(), 2);
    assert_eq!(
        result["diagnostics"][0]["code"],
        "continuation_definition_invalid"
    );
    assert_eq!(
        result["diagnostics"][1]["code"],
        "continuation_inputs_unsatisfied"
    );
    assert_eq!(result["diagnostics"][1]["location"]["input"], "required");
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["attempts"].as_array().unwrap().len(), 1);
}

#[test]
fn changed_inherited_json_schema_does_not_revalidate_producer_evidence() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  producer:
    kind: cmd
    command: {argv: ["sh", "-c", "printf '{\"value\":1}' > value.json"]}
    outputs:
      value: {kind: json, from: path, path: value.json, schema: value.schema.json}
  consumer:
    kind: cmd
    inputs:
      value: {ref: outputs.producer.value}
    command: {argv: ["false"]}
"#,
    );
    fs::write(
        bundle.source_root().join("value.schema.json"),
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
    )
    .unwrap();
    let run = bundle.result("changed-json-schema");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    fs::write(
        bundle.source_root().join("value.schema.json"),
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}"#,
    )
    .unwrap();
    fs::write(
        bundle.source_root().join("workflow.yaml"),
        r#"schemaVersion: 1
steps:
  producer:
    kind: cmd
    command: {argv: ["sh", "-c", "printf '{\"value\":1}' > value.json"]}
    outputs:
      value: {kind: json, from: path, path: value.json, schema: value.schema.json}
  consumer:
    kind: cmd
    inputs:
      value: {ref: outputs.producer.value}
    command: {argv: ["true"]}
"#,
    )
    .unwrap();
    let mut request = continue_args(&run, &["consumer"]);
    request.extend([
        "--workflow".to_owned(),
        bundle
            .source_root()
            .join("workflow.yaml")
            .to_string_lossy()
            .into_owned(),
    ]);
    let output = isolated_command(&request).output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    let attempt = &state["attempts"][1];
    assert_eq!(attempt["progress"]["steps"][0]["state"], "inherited");
    assert_eq!(
        attempt["continuation"]["inheritedSteps"][0]["definitionChanged"],
        true
    );
}

#[test]
fn chained_inheritance_rejects_a_referenced_output_without_retained_evidence() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command: {argv: [\"true\"]}\n  middle:\n    kind: cmd\n    dependsOn: [producer]\n    command: {argv: [\"false\"]}\n",
    );
    let workflow_path = bundle.source_root().join("workflow.yaml");
    let run = bundle.result("missing-chained-output");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    fs::write(
        &workflow_path,
        "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      value: {kind: text, from: path, path: value.txt}\n  middle:\n    kind: cmd\n    dependsOn: [producer]\n    command: {argv: [\"false\"]}\n",
    )
    .unwrap();
    let mut second = continue_args(&run, &["middle"]);
    second.extend([
        "--workflow".to_owned(),
        workflow_path.to_string_lossy().into_owned(),
    ]);
    assert_eq!(
        isolated_command(&second).output().unwrap().status.code(),
        Some(1)
    );
    fs::write(
        &workflow_path,
        "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      value: {kind: text, from: path, path: value.txt}\n  consumer:\n    kind: cmd\n    inputs:\n      value: {ref: outputs.producer.value}\n    command: {argv: [\"true\"]}\n",
    )
    .unwrap();
    let before = fs::read(run.join("state.json")).unwrap();
    let mut third = continue_args(&run, &["consumer"]);
    third.extend([
        "--workflow".to_owned(),
        workflow_path.to_string_lossy().into_owned(),
    ]);
    let output = isolated_command(&third).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        result["diagnostics"][0]["code"],
        "continuation_references_unsatisfied"
    );
    assert_eq!(
        result["diagnostics"][0]["location"]["reference"],
        "outputs.producer.value"
    );
    assert_eq!(fs::read(run.join("state.json")).unwrap(), before);
    assert!(!run.join("attempts/000003").exists());
}

#[test]
fn chained_continuation_authenticates_the_original_producer_closure_before_claim() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  bootstrap:\n    kind: cmd\n    command: {argv: [\"false\"]}\n",
    );
    let run = bundle.result("historical-producer-closure");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let replacement = bundle.source_root().join("replacement.yaml");
    fs::write(
        bundle.source_root().join("value.schema.json"),
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
    )
    .unwrap();
    fs::write(
        &replacement,
        r#"schemaVersion: 1
steps:
  producer:
    kind: cmd
    command: {argv: ["sh", "-c", "printf '{\"value\":1}' > value.json"]}
    outputs:
      value: {kind: json, from: path, path: value.json, schema: value.schema.json}
  consumer:
    kind: cmd
    inputs:
      value: {ref: outputs.producer.value}
    command: {argv: ["false"]}
"#,
    )
    .unwrap();
    let mut second = continue_args(&run, &["producer"]);
    second.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    assert_eq!(
        isolated_command(&second).output().unwrap().status.code(),
        Some(1)
    );
    let mut third = continue_args(&run, &["consumer"]);
    third.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    assert_eq!(
        isolated_command(&third).output().unwrap().status.code(),
        Some(1)
    );

    fs::remove_file(run.join("attempts/000002/workflow/manifest.json")).unwrap();
    let before = fs::read(run.join("state.json")).unwrap();
    let output = isolated_command(&third).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(fs::read(run.join("state.json")).unwrap(), before);
    assert!(!run.join("attempts/000004").exists());
}

#[test]
fn continuation_accumulates_inheritance_and_executed_git_admission_failures() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  producer:
    kind: cmd
    command: {argv: ["sh", "-c", "echo value > value.txt"]}
    outputs:
      value: {kind: text, from: path, path: value.txt}
  consumer:
    kind: cmd
    inputs:
      value: {ref: outputs.producer.value}
    command: {argv: ["false"]}
"#,
    );
    fs::write(
        bundle.source_root().join("alternate.yaml"),
        r#"schemaVersion: 1
steps:
  producer:
    kind: cmd
    command: {argv: ["sh", "-c", "echo value > value.txt"]}
    outputs:
      value: {kind: file, from: path, path: value.txt, mediaType: text/plain}
  consumer:
    kind: cmd
    inputs:
      value: {ref: outputs.producer.value}
    command: {argv: ["true"]}
    outputs:
      branch: {kind: git_branch, from: workspace}
"#,
    )
    .unwrap();
    let run = bundle.result("admission-aggregation");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let mut request = continue_args(&run, &["consumer"]);
    request.extend([
        "--workflow".to_owned(),
        bundle
            .source_root()
            .join("alternate.yaml")
            .to_string_lossy()
            .into_owned(),
    ]);
    let output = isolated_command(&request).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let codes = result["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["code"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(
        codes.contains(&"continuation_references_unsatisfied"),
        "{codes:?}"
    );
    assert!(
        codes.iter().any(|code| code.starts_with("git_")),
        "{codes:?}"
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["attempts"].as_array().unwrap().len(), 1);
}

#[test]
fn invalid_selection_does_not_claim_an_attempt() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: {argv: [\"false\"]}\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
    );
    let run = bundle.result("invalid-selection");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let before = fs::read(run.join("state.json")).unwrap();
    for selected in [
        &["missing"][..],
        &["cleanup"][..],
        &["work", "work"][..],
        &["zzz", "aaa"][..],
    ] {
        let output = isolated_command(&continue_args(&run, selected))
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        let diagnostic: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(
            diagnostic["diagnostics"][0]["code"],
            "continuation_definition_invalid"
        );
        if selected.len() == 2 && selected[0] == "zzz" {
            assert_eq!(diagnostic["diagnostics"][0]["location"]["id"], "aaa");
            assert_eq!(diagnostic["diagnostics"][1]["location"]["id"], "zzz");
        }
        assert_eq!(fs::read(run.join("state.json")).unwrap(), before);
        assert!(!run.join("attempts/000002").exists());
    }
}

#[test]
fn inherited_zero_delta_git_keeps_original_producer_without_carrier() {
    let bundle = RunBundle::new(GIT_CONTINUATION_SOURCE);
    initialize_git_repository(bundle.execution_root());
    let run = bundle.result("inherited-zero-delta");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial)
            .env("PHASE", "initial")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    let continued = isolated_command(&continue_args(&run, &["check"]))
        .env("PHASE", "continuation")
        .output()
        .unwrap();
    assert!(
        continued.status.success(),
        "{} {}",
        String::from_utf8_lossy(&continued.stdout),
        String::from_utf8_lossy(&continued.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("attempts/000002/result/result.json")).unwrap())
            .unwrap();
    assert_eq!(result["exports"]["branch"]["provenance"], "inherited");
    assert_eq!(result["exports"]["branch"]["producer"]["attemptNumber"], 1);
    assert!(result["exports"]["branch"]["carrier"].is_null());
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    assert!(state["attempts"][1]["progress"]["steps"][0]["outputs"][0]["carrier"].is_null());
    let view = isolated_command(&[
        "workflow".to_owned(),
        "view".to_owned(),
        run.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(view.status.success());
    let view: serde_json::Value = serde_json::from_slice(&view.stdout).unwrap();
    let schema: serde_json::Value = serde_json::from_slice(
        &fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/workflow-view-result-v1.schema.json"
        ))
        .unwrap(),
    )
    .unwrap();
    assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&view));
}

#[test]
fn continuation_git_capture_uses_initial_baseline_after_head_advances() {
    let bundle = RunBundle::new(GIT_CONTINUATION_SOURCE);
    initialize_git_repository(bundle.execution_root());
    let baseline = git(bundle.execution_root(), &["rev-parse", "HEAD"])
        .trim()
        .to_owned();
    let run = bundle.result("git-baseline");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    let first = isolated_command(&initial)
        .env("PHASE", "initial")
        .output()
        .unwrap();
    assert_eq!(
        first.status.code(),
        Some(1),
        "{} {}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );
    fs::write(bundle.execution_root().join("tracked.txt"), "advanced\n").unwrap();
    git(bundle.execution_root(), &["add", "tracked.txt"]);
    git(
        bundle.execution_root(),
        &["commit", "--quiet", "-m", "advance"],
    );
    let head = git(bundle.execution_root(), &["rev-parse", "HEAD"])
        .trim()
        .to_owned();
    assert_ne!(baseline, head);
    let resumed = isolated_command(&continue_args(&run, &["capture"]))
        .env("PHASE", "continuation")
        .output()
        .unwrap();
    assert!(
        resumed.status.success(),
        "{} {}",
        String::from_utf8_lossy(&resumed.stdout),
        String::from_utf8_lossy(&resumed.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("attempts/000002/result/result.json")).unwrap())
            .unwrap();
    assert_eq!(result["exports"]["branch"]["baseOid"], baseline);
    assert_eq!(result["exports"]["branch"]["headOid"], head);
    assert!(result["exports"]["branch"]["carrier"].is_object());
}

#[test]
fn retry_after_failed_continuation_reruns_the_initial_definition_without_context() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  first:
    kind: cmd
    command: {argv: ["sh", "-c", "test -z \"${SCHERZO_CONTINUATION_CONTEXT:-}\"; echo first >> calls"]}
  second:
    kind: cmd
    dependsOn: [first]
    command: {argv: ["sh", "-c", "if test \"$PHASE\" = continued; then test -r \"$SCHERZO_CONTINUATION_CONTEXT\"; exit 75; fi; test -z \"${SCHERZO_CONTINUATION_CONTEXT:-}\"; test \"$PHASE\" = retry"]}
finalizers:
  cleanup:
    kind: cmd
    command: {argv: ["sh", "-c", "echo cleanup >> calls"]}
"#,
    );
    let run = bundle.result("retry-after-continuation");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial)
            .env("PHASE", "initial")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    let replacement = bundle.source_root().join("replacement.yaml");
    fs::write(
        &replacement,
        r#"schemaVersion: 1
steps:
  first:
    kind: cmd
    command: {argv: ["sh", "-c", "echo replacement >> calls"]}
  second:
    kind: cmd
    dependsOn: [first]
    command: {argv: ["sh", "-c", "test -r \"$SCHERZO_CONTINUATION_CONTEXT\"; exit 75"]}
finalizers:
  cleanup:
    kind: cmd
    command: {argv: ["sh", "-c", "echo replacement-cleanup >> calls"]}
"#,
    )
    .unwrap();
    let mut continuation = continue_args(&run, &["second"]);
    continuation.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    assert_eq!(
        isolated_command(&continuation)
            .env("PHASE", "continued")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    let retry = isolated_command(&[
        "workflow".to_owned(),
        "retry".to_owned(),
        run.to_string_lossy().into_owned(),
        "--execution-root".to_owned(),
        bundle.execution_root().to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .env("PHASE", "retry")
    .output()
    .unwrap();
    assert!(
        retry.status.success(),
        "{} {}",
        String::from_utf8_lossy(&retry.stdout),
        String::from_utf8_lossy(&retry.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    let last = &state["attempts"][2];
    assert_eq!(last["trigger"], "explicit_retry");
    assert!(last["continuation"].is_null());
    assert_eq!(last["progress"]["steps"][0]["state"], "succeeded");
    assert_eq!(last["progress"]["steps"][1]["state"], "succeeded");
    assert_eq!(last["finalization"]["finalizers"][0]["state"], "succeeded");
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("calls")).unwrap(),
        "first\ncleanup\nreplacement-cleanup\nfirst\ncleanup\n"
    );
}

#[test]
fn inherited_json_and_file_values_stage_from_original_producer() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  producer:
    kind: cmd
    command: {argv: ["sh", "-c", "echo '{\"ok\":true}' > data.json; echo artifact > file.txt"]}
    outputs:
      data: {kind: json, from: path, path: data.json, schema: data.schema.json}
      document: {kind: file, from: path, path: file.txt, mediaType: text/plain}
  consume:
    kind: cmd
    inputs:
      data: {ref: outputs.producer.data}
      document: {ref: outputs.producer.document}
    command:
      argv: ["sh", "-c", "test -r $SCHERZO_STEP_INPUTS/values/data; test -r $SCHERZO_STEP_INPUTS/values/document; test $PHASE = continuation"]
exports:
  retainedDocument: {ref: outputs.producer.document}
"#,
    );
    fs::write(bundle.source_root().join("data.schema.json"), r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["ok"],"properties":{"ok":{"const":true}}}"#).unwrap();
    let run = bundle.result("json-file");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial)
            .env("PHASE", "initial")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    let output = isolated_command(&continue_args(&run, &["consume"]))
        .env("PHASE", "continuation")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    for descriptor in state["attempts"][1]["progress"]["steps"][0]["outputs"]
        .as_array()
        .unwrap()
    {
        assert_eq!(descriptor["producer"]["attemptNumber"], 1);
    }
    let result: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("attempts/000002/result/result.json")).unwrap())
            .unwrap();
    assert_eq!(
        result["exports"]["retainedDocument"]["provenance"],
        "inherited"
    );
    assert_eq!(
        result["exports"]["retainedDocument"]["producer"]["attemptNumber"],
        1
    );
}

#[test]
fn continuation_recovery_handler_reads_bound_context_and_starts_fresh() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  retained:
    kind: cmd
    command: {argv: ["true"]}
  repairable:
    kind: cmd
    dependsOn: [retained]
    recovery:
      retries: 1
      handler:
        kind: cmd
        command:
          argv:
            - /bin/sh
            - -c
            - |
              set -eu
              if test "$PHASE" = resumed; then
                test -r "$SCHERZO_CONTINUATION_CONTEXT"
                : > repaired
              else
                test -z "${SCHERZO_CONTINUATION_CONTEXT:-}"
              fi
              printf '%s' '{"schemaVersion":1,"decision":"recheck","summary":"Retried after preparation.","reason":"Recheck current state."}' > "$SCHERZO_RECOVERY_RESULT"
    command:
      argv:
        - /bin/sh
        - -c
        - |
          set -eu
          if test "$PHASE" = resumed; then
            test -r "$SCHERZO_CONTINUATION_CONTEXT"
            test -f repaired
          else
            test -z "${SCHERZO_CONTINUATION_CONTEXT:-}"
            exit 75
          fi
"#,
    );
    let run = bundle.result("recovery-context");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial)
            .env("PHASE", "initial")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    let output = isolated_command(&continue_args(&run, &["repairable"]))
        .env("PHASE", "resumed")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    let attempt = &state["attempts"][1];
    assert_eq!(attempt["progress"]["steps"][0]["state"], "inherited");
    assert_eq!(attempt["progress"]["steps"][1]["state"], "succeeded");
    assert_eq!(
        attempt["progress"]["steps"][1]["recovery"]["configuredRetries"],
        1
    );
}

#[test]
fn replacement_definition_and_root_are_selected_independently() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  prior:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"echo original >> calls\"]}\n  target:\n    kind: cmd\n    command: {argv: [\"false\"]}\n",
    );
    let run = bundle.result("replacement");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let replacement = bundle.source_root().join("replacement.yaml");
    fs::write(&replacement, "schemaVersion: 1\nsteps:\n  prior:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"echo changed >> calls\"]}\n  target:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"echo target >> calls\"]}\n").unwrap();
    let normalized_replacement = fs::canonicalize(&replacement).unwrap();
    let new_root = bundle.result("other-root");
    fs::create_dir(&new_root).unwrap();
    let normalized_new_root = fs::canonicalize(&new_root).unwrap();
    let mut args = continue_args(&run, &["target"]);
    args.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
        "--execution-root".to_owned(),
        new_root.to_string_lossy().into_owned(),
    ]);
    let output = isolated_command(&args).output().unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    let attempt = &state["attempts"][1];
    assert_eq!(
        attempt["continuation"]["definitionSource"]["kind"],
        "replaced"
    );
    assert_eq!(
        attempt["continuation"]["inheritedSteps"][0]["definitionChanged"],
        true
    );
    assert_eq!(attempt["continuation"]["workspace"]["modified"], "unknown");
    assert_eq!(
        attempt["continuation"]["workspace"]["executionRoot"],
        normalized_new_root.to_str().unwrap()
    );
    assert_eq!(
        attempt["continuation"]["request"]["definition"]["replaced"]["path"],
        normalized_replacement.to_str().unwrap()
    );
    assert!(run.join("attempts/000002/workflow/manifest.json").exists());
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("calls")).unwrap(),
        "original\n"
    );
    assert_eq!(
        fs::read_to_string(new_root.join("calls")).unwrap(),
        "target\n"
    );
    let status = isolated_command(&[
        "workflow".to_owned(),
        "status".to_owned(),
        run.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    let schema: serde_json::Value = serde_json::from_slice(
        &fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/workflow-status-result-v1.schema.json"
        ))
        .unwrap(),
    )
    .unwrap();
    assert!(
        jsonschema::validator_for(&schema)
            .unwrap()
            .is_valid(&status)
    );
}

#[test]
fn chained_continuation_keeps_original_producer_and_reruns_finalizers() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"echo producer >> calls; echo inherited > output.txt\"]}\n    outputs:\n      value: {kind: text, from: path, path: output.txt}\n  consumer:\n    kind: cmd\n    inputs:\n      value: {ref: outputs.producer.value}\n    command: {argv: [\"sh\", \"-c\", \"echo consumer >> calls; test \\\"$PHASE\\\" = final\"]}\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"if test \\\"$PHASE\\\" != initial; then test -r \\\"$SCHERZO_CONTINUATION_CONTEXT\\\"; else test -z ${SCHERZO_CONTINUATION_CONTEXT:-}; fi; echo cleanup >> calls\"]}\nexports:\n  retained:\n    ref: outputs.producer.value\n",
    );
    let run = bundle.result("chain");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial)
            .env("PHASE", "initial")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    assert_eq!(
        isolated_command(&continue_args(&run, &["consumer"]))
            .env("PHASE", "again")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    let last = isolated_command(&continue_args(&run, &["consumer"]))
        .env("PHASE", "final")
        .output()
        .unwrap();
    assert!(
        last.status.success(),
        "{} {}",
        String::from_utf8_lossy(&last.stdout),
        String::from_utf8_lossy(&last.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    let attempts = state["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 3);
    assert_eq!(
        attempts[2]["continuation"]["inheritedSteps"][0]["priorState"],
        "inherited"
    );
    assert_eq!(
        attempts[2]["progress"]["steps"][0]["detail"]["priorAttemptNumber"],
        2
    );
    assert_eq!(
        attempts[2]["progress"]["steps"][0]["outputs"][0]["producer"]["attemptNumber"],
        1
    );
    assert_eq!(
        attempts[2]["finalization"]["finalizers"][0]["state"],
        "succeeded"
    );
    let portable: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("attempts/000003/result/result.json")).unwrap())
            .unwrap();
    assert_eq!(portable["exports"]["retained"]["provenance"], "inherited");
    assert_eq!(
        portable["exports"]["retained"]["producer"]["attemptNumber"],
        1
    );
    assert!(run.join("attempts/000003/result/exports/0001").is_file());
    let view = isolated_command(&[
        "workflow".to_owned(),
        "view".to_owned(),
        run.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(view.status.success());
    let view: serde_json::Value = serde_json::from_slice(&view.stdout).unwrap();
    let schema: serde_json::Value = serde_json::from_slice(
        &fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/workflow-view-result-v1.schema.json"
        ))
        .unwrap(),
    )
    .unwrap();
    assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&view));
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("calls")).unwrap(),
        "producer\nconsumer\ncleanup\nconsumer\ncleanup\nconsumer\ncleanup\n"
    );
    assert!(!run.join("attempts/000002/values/step/producer").exists());
    assert!(!run.join("attempts/000003/values/step/producer").exists());
}

#[test]
fn workflow_continue_inherits_prior_output_and_executes_only_downstream() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"printf first > first.txt; echo first >> calls; test -z ${SCHERZO_CONTINUATION_CONTEXT:-}\"]}\n    outputs:\n      value: {kind: text, from: path, path: first.txt}\n  second:\n    kind: cmd\n    inputs:\n      value: {ref: outputs.first.value}\n    command: {argv: [\"sh\", \"-c\", \"echo second >> calls; if test \\\"$PHASE\\\" = continuation; then test -r \\\"$SCHERZO_CONTINUATION_CONTEXT\\\"; grep -q inheritedOutputs \\\"$SCHERZO_CONTINUATION_CONTEXT\\\"; else test -z ${SCHERZO_CONTINUATION_CONTEXT:-}; false; fi\"]}\n",
    );
    let run = bundle.result("continuation");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    let first = isolated_command(&initial)
        .env("PHASE", "initial")
        .output()
        .unwrap();
    assert_eq!(
        first.status.code(),
        Some(1),
        "{} {}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );
    let output = isolated_command(&continue_args(&run, &["second"]))
        .env("PHASE", "continuation")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    let continuation = &state["attempts"][1];
    assert_eq!(continuation["trigger"], "continuation");
    assert_eq!(
        continuation["continuation"]["reexecutedSteps"],
        serde_json::json!(["second"])
    );
    assert_eq!(continuation["progress"]["steps"][0]["state"], "inherited");
    assert_eq!(continuation["progress"]["steps"][1]["state"], "succeeded");
    let event: serde_json::Value =
        serde_json::from_slice(output.stderr.split(|byte| *byte == b'\n').next().unwrap()).unwrap();
    assert_eq!(event["event"], "continuation_partition");
    assert_eq!(event["continuation"], continuation["continuation"]);
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["command"], "scherzo-cloud workflow continue");
    assert_eq!(
        terminal["result"]["continuation"],
        continuation["continuation"]
    );
    let result: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("attempts/000002/result/result.json")).unwrap())
            .unwrap();
    assert_eq!(result["continuation"], continuation["continuation"]);
    assert_eq!(result["steps"][0]["state"], "inherited");
    assert!(result["steps"][0]["startedAt"].is_null());
    assert!(result["steps"][0]["durationMilliseconds"].is_null());
    assert!(result["steps"][0]["commandOutput"].is_null());
    assert!(result["steps"][0]["invocations"].is_null());
    let context: serde_json::Value = serde_json::from_slice(
        &fs::read(run.join("attempts/000002/continuation-context.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(context["continuation"], continuation["continuation"]);
    assert_eq!(
        context["inheritedOutputs"]["first"][0]["producer"]["attemptNumber"],
        1
    );
    assert_eq!(
        continuation["progress"]["steps"][0]["outputs"][0]["producer"]["attemptNumber"],
        1
    );
    let status = isolated_command(&[
        "workflow".to_owned(),
        "status".to_owned(),
        run.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(
        status["state"]["attempts"][1]["continuation"],
        continuation["continuation"]
    );
    assert_eq!(
        status["workspaceModified"],
        continuation["continuation"]["workspace"]["modified"]
    );
    let schema: serde_json::Value = serde_json::from_slice(
        &fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/workflow-status-result-v1.schema.json"
        ))
        .unwrap(),
    )
    .unwrap();
    assert!(
        jsonschema::validator_for(&schema)
            .unwrap()
            .is_valid(&status)
    );
    let view = isolated_command(&[
        "workflow".to_owned(),
        "view".to_owned(),
        run.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(
        view.status.success(),
        "{} {}",
        String::from_utf8_lossy(&view.stdout),
        String::from_utf8_lossy(&view.stderr)
    );
    let view: serde_json::Value = serde_json::from_slice(&view.stdout).unwrap();
    assert_eq!(view["result"]["continuation"], continuation["continuation"]);
    let plain = isolated_command(&[
        "workflow".to_owned(),
        "view".to_owned(),
        run.to_string_lossy().into_owned(),
        "--plain".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(plain.status.success());
    let plain = String::from_utf8(plain.stdout).unwrap();
    assert!(plain.contains("inherited"));
    assert!(plain.contains("workspace modified"));
    let view_schema: serde_json::Value = serde_json::from_slice(
        &fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/workflow-view-result-v1.schema.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let validator = jsonschema::validator_for(&view_schema).unwrap();
    assert!(
        validator.is_valid(&view),
        "{:?}",
        validator
            .iter_errors(&view)
            .map(|error| error.to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("calls")).unwrap(),
        "first\nsecond\nsecond\n"
    );
    let before = fs::read(run.join("state.json")).unwrap();
    let rejected = isolated_command(&continue_args(&run, &["second"]))
        .output()
        .unwrap();
    assert_eq!(rejected.status.code(), Some(1));
    let diagnostic: serde_json::Value = serde_json::from_slice(&rejected.stdout).unwrap();
    assert_eq!(
        diagnostic["diagnostics"][0]["code"],
        "continuation_disposition_ineligible"
    );
    assert_eq!(fs::read(run.join("state.json")).unwrap(), before);
}
