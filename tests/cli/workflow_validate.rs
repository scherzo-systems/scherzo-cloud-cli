use std::fs;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

use super::{CREDENTIALS_FILE_VARIABLE, run, run_with_env};

const WORKFLOW_PATH: &str = "workflows/complete.yaml";
const WORKFLOW_SENTINEL: &str = "unique-workflow-static-content-sentinel";
const SYSTEM_SENTINEL: &str = "unique-system-prompt-content-sentinel";
const MESSAGE_SENTINEL: &str = "unique-message-content-sentinel";
const ATTACHMENT_SENTINEL: &str = "unique-attachment-content-sentinel";
const SCHEMA_SENTINEL: &str = "unique-result-schema-content-sentinel";

struct WorkflowBundle {
    _temporary: TempDir,
    root: PathBuf,
    marker: PathBuf,
}

impl WorkflowBundle {
    fn valid() -> Self {
        let temporary = tempfile::tempdir().expect("temporary workflow directory should exist");
        let root = temporary.path().join("source");
        for directory in ["workflows", "prompts", "attachments", "schemas", "scripts"] {
            fs::create_dir_all(root.join(directory)).expect("workflow directory should exist");
        }
        let marker = temporary.path().join("executed");
        let command = root.join("scripts/should-not-run");
        fs::write(
            &command,
            format!("#!/bin/sh\nprintf invoked > '{}'\n", marker.display()),
        )
        .expect("command sentinel should be written");
        let mut permissions = fs::metadata(&command)
            .expect("command sentinel metadata should be available")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&command, permissions).expect("command sentinel should be executable");

        let workflow = format!(
            r#"schemaVersion: 1
description: {WORKFLOW_SENTINEL}
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  prepare:
    kind: cmd
    command:
      argv: ["{}"]
  agent:
    kind: agent
    dependsOn: [prepare]
    agent:
      profile: coding
      systemPrompt: ../prompts/system.md
      message:
        text:
          - file: ../prompts/message.md
          - ref: imports.prompt
        attachments:
          - file: ../attachments/data.txt
    outputs:
      result:
        kind: agent_result
        schema: ../schemas/result.schema.json
exports:
  result:
    ref: outputs.agent.result
"#,
            command.display()
        );
        fs::write(root.join(WORKFLOW_PATH), workflow).expect("workflow should be written");
        fs::write(root.join("prompts/system.md"), SYSTEM_SENTINEL)
            .expect("system prompt should be written");
        fs::write(root.join("prompts/message.md"), MESSAGE_SENTINEL)
            .expect("message should be written");
        fs::write(root.join("attachments/data.txt"), ATTACHMENT_SENTINEL)
            .expect("attachment should be written");
        fs::write(
            root.join("schemas/result.schema.json"),
            format!(
                r#"{{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","description":"{SCHEMA_SENTINEL}"}}"#
            ),
        )
        .expect("result schema should be written");

        Self {
            _temporary: temporary,
            root,
            marker,
        }
    }

    fn workflow_path(&self) -> PathBuf {
        self.root.join(WORKFLOW_PATH)
    }

    fn replace_workflow(&self, workflow: &str) {
        fs::write(self.workflow_path(), workflow).expect("workflow should be replaced");
    }

    fn root_argument(&self) -> &str {
        self.root
            .to_str()
            .expect("temporary source root should be UTF-8")
    }
}

fn validate(bundle: &WorkflowBundle, json: bool) -> std::process::Output {
    let listener = TcpListener::bind("127.0.0.1:0").expect("network sentinel should bind");
    listener
        .set_nonblocking(true)
        .expect("network sentinel should be nonblocking");
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let mut args = vec![
        "workflow",
        "validate",
        "--source-root",
        bundle.root_argument(),
        WORKFLOW_PATH,
    ];
    if json {
        args.push("--json");
    }
    let output = run_with_env(
        &args,
        &[
            (
                CREDENTIALS_FILE_VARIABLE,
                "/dev/null/workflow-validation-credentials.json",
            ),
            ("SCHERZO_CLOUD_API_URL", &api_url),
            (
                "SCHERZO_CLOUD_AUTH_ISSUER",
                "http://auth.workflow-validation.invalid/",
            ),
            (
                "SCHERZO_CLOUD_AUTH_AUDIENCE",
                "https://api.workflow-validation.invalid",
            ),
            ("SCHERZO_CLOUD_AUTH_CLIENT_ID", "workflow-validation-client"),
        ],
    );

    assert!(
        !bundle.marker.exists(),
        "validation must not execute a step"
    );
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "validation must not open a network connection"
    );
    output
}

fn assert_static_contents_absent(output: &std::process::Output) {
    for sentinel in [
        WORKFLOW_SENTINEL,
        SYSTEM_SENTINEL,
        MESSAGE_SENTINEL,
        ATTACHMENT_SENTINEL,
        SCHEMA_SENTINEL,
    ] {
        assert!(!String::from_utf8_lossy(&output.stdout).contains(sentinel));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(sentinel));
    }
}

#[test]
fn valid_bundle_reports_provenance_without_executing_or_exposing_static_sources() {
    let bundle = WorkflowBundle::valid();

    let human = validate(&bundle, false);
    assert!(human.status.success());
    let stdout = String::from_utf8(human.stdout.clone()).expect("human output should be UTF-8");
    assert!(stdout.contains("✓ Workflow V1 definition is valid."));
    assert!(stdout.contains("Workflow: workflows/complete.yaml"));
    let digest = stdout
        .lines()
        .find_map(|line| line.strip_prefix("Digest: sha256:"))
        .expect("human output should contain a SHA-256 digest");
    assert_eq!(digest.len(), 64);
    assert!(
        digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    );
    assert!(stdout.contains("Steps: 2"));
    assert!(stdout.contains("Required optional imports: prompt"));
    assert!(stdout.contains("No workflow steps were executed."));
    assert!(human.stderr.is_empty());
    assert_static_contents_absent(&human);

    let json = validate(&bundle, true);
    assert!(json.status.success());
    let report: serde_json::Value =
        serde_json::from_slice(&json.stdout).expect("validation output should be JSON");
    assert_eq!(report["schemaVersion"], 1);
    assert_eq!(report["command"], "scherzo-cloud workflow validate");
    assert_eq!(report["outcome"], "valid");
    assert_eq!(report["workflow"]["path"], WORKFLOW_PATH);
    assert_eq!(report["digest"]["algorithm"], "sha256");
    let digest = report["digest"]["value"]
        .as_str()
        .expect("digest should be a string");
    assert_eq!(digest.len(), 64);
    assert!(
        digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    );
    assert_eq!(report["stepCount"], 2);
    assert_eq!(report["requiredImports"], serde_json::json!(["prompt"]));
    assert!(report.get("diagnostics").is_none());
    assert!(json.stdout.ends_with(b"\n"));
    assert!(json.stderr.is_empty());
    assert_static_contents_absent(&json);
}

#[test]
fn malformed_semantic_missing_escaping_and_schema_failures_are_bounded_results() {
    let malformed = WorkflowBundle::valid();
    malformed.replace_workflow("schemaVersion: [\n");

    let semantic = WorkflowBundle::valid();
    semantic.replace_workflow(
        "schemaVersion: 1\nsteps:\n  consumer:\n    kind: cmd\n    dependsOn: [missing]\n    command:\n      argv: [\"true\"]\n",
    );

    let missing = WorkflowBundle::valid();
    fs::remove_file(missing.root.join("prompts/system.md"))
        .expect("system prompt should be removed");

    let escaping = WorkflowBundle::valid();
    let source = fs::read_to_string(escaping.workflow_path()).expect("workflow should be readable");
    escaping.replace_workflow(&source.replace("../prompts/system.md", "../../outside.md"));

    let invalid_schema = WorkflowBundle::valid();
    fs::write(
        invalid_schema.root.join("schemas/result.schema.json"),
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":42}"#,
    )
    .expect("invalid result schema should be written");

    let missing_message_output = WorkflowBundle::valid();
    let source = fs::read_to_string(missing_message_output.workflow_path())
        .expect("workflow should be readable");
    missing_message_output
        .replace_workflow(&source.replace("ref: imports.prompt", "ref: outputs.missing.response"));

    for (bundle, expected_code, expected_location) in [
        (malformed, "malformed_yaml", "workflow"),
        (semantic, "missing_dependency", "step_dependency"),
        (missing, "source_unavailable", "system_prompt"),
        (escaping, "source_path_escape", "system_prompt"),
        (invalid_schema, "invalid_result_schema", "result_schema"),
        (
            missing_message_output,
            "unknown_output_step",
            "message_text",
        ),
    ] {
        let human = validate(&bundle, false);
        assert_eq!(human.status.code(), Some(1));
        let stdout = String::from_utf8_lossy(&human.stdout);
        assert!(stdout.contains("✗ Workflow V1 definition is invalid."));
        assert!(stdout.contains("Workflow: workflows/complete.yaml"));
        assert!(stdout.contains(&format!("Code: {expected_code}")));
        assert!(stdout.contains("No workflow steps were executed."));
        assert!(human.stdout.len() < 1024);
        assert!(human.stderr.is_empty());
        assert_static_contents_absent(&human);

        let json = validate(&bundle, true);
        assert_eq!(json.status.code(), Some(1));
        let report: serde_json::Value =
            serde_json::from_slice(&json.stdout).expect("invalid result should be JSON");
        assert_eq!(report["schemaVersion"], 1);
        assert_eq!(report["outcome"], "invalid");
        assert_eq!(report["workflow"]["path"], WORKFLOW_PATH);
        let diagnostics = report["diagnostics"]
            .as_array()
            .expect("invalid result should contain diagnostics");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0]["code"], expected_code);
        assert!(diagnostics[0]["message"].as_str().unwrap().len() <= 128);
        assert_eq!(diagnostics[0]["location"]["kind"], expected_location);
        assert!(report.get("digest").is_none());
        assert!(report.get("stepCount").is_none());
        assert!(report.get("requiredImports").is_none());
        assert!(json.stdout.len() < 2048);
        assert!(json.stdout.ends_with(b"\n"));
        assert!(json.stderr.is_empty());
        assert_static_contents_absent(&json);
    }
}

#[test]
fn source_root_is_required_even_for_an_absolute_workflow_path() {
    let bundle = WorkflowBundle::valid();
    let selected_path = bundle.workflow_path();
    let selected = selected_path
        .to_str()
        .expect("temporary workflow path should be UTF-8");

    let output = run(&["workflow", "validate", selected]);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--source-root <ROOT>"));
    assert!(!bundle.marker.exists());
}

#[test]
fn selected_workflow_must_remain_within_the_explicit_source_root() {
    let bundle = WorkflowBundle::valid();
    let outside = bundle.root.parent().unwrap().join("outside.yaml");
    fs::write(&outside, "schemaVersion: 1\nsteps: {}\n")
        .expect("outside workflow should be written");

    let output = run(&[
        "workflow",
        "validate",
        "--source-root",
        bundle.root_argument(),
        Path::new("../outside.yaml")
            .to_str()
            .expect("fixture path should be UTF-8"),
        "--json",
    ]);

    assert_eq!(output.status.code(), Some(1));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["outcome"], "invalid");
    assert!(report["workflow"].is_null());
    assert_eq!(report["diagnostics"][0]["code"], "source_path_escape");
    assert!(output.stderr.is_empty());
    assert!(!bundle.marker.exists());
}
