use std::collections::BTreeMap;
#[cfg(all(unix, not(target_os = "macos")))]
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use super::*;
use crate::execution::workflow::pi::{PiConfig, Thinking};
use crate::execution::workflow::validated::{
    ValidatedHarness, ValidatedMessageSource, ValidatedRecoveryHandler, ValidatedStep,
    WorkflowValueType,
};

const WORKFLOW_PATH: &str = "workflows/complete.yaml";
const JSON_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";
const RESULT_SCHEMA: &[u8] = br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false}
"#;
const WORKFLOW: &str = r#"schemaVersion: 1
description: Complete resolution fixture.
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: xhigh
steps:
  agent:
    kind: agent
    cwd: runtime/does-not-exist
    agent:
      profile: coding
      systemPrompt: ../prompts/system.md
      message:
        text:
          - file: ../prompts/message.md
          - ref: imports.prompt
          - file: ../prompts/message.md
        attachments:
          - file: ../attachments/data.bin
    outputs:
      result:
        kind: json
        from: agent_result
        schema: ../schemas/result.schema.json
      artifact:
        kind: file
        from: path
        path: runtime/does-not-exist/artifact.bin
        mediaType: application/octet-stream
exports:
  result:
    ref: outputs.agent.result
"#;

struct FixtureBundle {
    _temporary: TempDir,
    root: PathBuf,
}

impl FixtureBundle {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("source");
        for directory in ["workflows", "prompts", "attachments", "schemas"] {
            fs::create_dir_all(root.join(directory)).unwrap();
        }
        fs::write(root.join(WORKFLOW_PATH), WORKFLOW).unwrap();
        fs::write(root.join("prompts/system.md"), b"System prompt.\n").unwrap();
        fs::write(root.join("prompts/message.md"), "Message: caf\u{e9}\n").unwrap();
        fs::write(root.join("attachments/data.bin"), [0, 0xff, 0x80, b'\n']).unwrap();
        fs::write(root.join("schemas/result.schema.json"), RESULT_SCHEMA).unwrap();
        Self {
            _temporary: temporary,
            root,
        }
    }

    fn workflow_path(&self) -> PathBuf {
        self.root.join(WORKFLOW_PATH)
    }

    fn resolve(&self) -> Result<ResolvedWorkflow, ResolutionFailure> {
        resolve_workflow_file(&self.root, &self.workflow_path())
    }
}

#[test]
fn semantic_outputs_definition_accepts_exact_six_rows_and_retains_json_schemas() {
    let bundle = FixtureBundle::new();
    let source = r#"schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: xhigh
steps:
  paths:
    kind: cmd
    command:
      argv: ["true"]
    outputs:
      summary:
        kind: text
        from: path
        path: summary.txt
      data:
        kind: json
        from: path
        path: data.json
        schema: ../schemas/result.schema.json
      report:
        kind: file
        from: path
        path: report.bin
        mediaType: application/octet-stream
      changes:
        kind: git_branch
        from: workspace
  response:
    kind: agent
    agent:
      profile: coding
      systemPrompt: ../prompts/system.md
      message:
        text: [{ file: ../prompts/message.md }]
    outputs:
      response:
        kind: text
        from: agent_response
  result:
    kind: agent
    agent:
      profile: coding
      systemPrompt: ../prompts/system.md
      message:
        text: [{ file: ../prompts/message.md }]
    outputs:
      result:
        kind: json
        from: agent_result
        schema: ../schemas/result.schema.json
"#;
    fs::write(bundle.workflow_path(), source).unwrap();

    let resolved = bundle.resolve().unwrap();
    let ValidatedStep::Command(paths) = &resolved.definition.steps["paths"] else {
        panic!("paths must remain a command step");
    };
    assert_eq!(
        paths
            .common
            .outputs
            .values()
            .map(|output| output.value_type)
            .collect::<Vec<_>>(),
        [
            WorkflowValueType::GitBranch,
            WorkflowValueType::Json,
            WorkflowValueType::File,
            WorkflowValueType::Text,
        ]
    );
    assert!(matches!(
        paths.common.outputs["summary"].definition,
        Output::TextPath { .. }
    ));
    assert!(matches!(
        paths.common.outputs["data"].definition,
        Output::JsonPath { .. }
    ));
    assert!(matches!(
        paths.common.outputs["report"].definition,
        Output::FilePath { .. }
    ));
    assert!(matches!(
        paths.common.outputs["changes"].definition,
        Output::GitBranchWorkspace
    ));
    let ValidatedStep::Agent(response) = &resolved.definition.steps["response"] else {
        panic!("response must remain an agent step");
    };
    let ValidatedStep::Agent(result) = &resolved.definition.steps["result"] else {
        panic!("result must remain an agent step");
    };
    assert!(matches!(
        response.common.outputs["response"].definition,
        Output::TextAgentResponse
    ));
    assert!(matches!(
        result.common.outputs["result"].definition,
        Output::JsonAgentResult { .. }
    ));
    assert!(resolved.json_schema("paths", "data").is_some());
    assert!(resolved.json_schema("result", "result").is_some());
}

#[test]
fn complete_bundle_resolves_canonical_sources_and_retains_an_immutable_snapshot() {
    let bundle = FixtureBundle::new();
    let resolved = bundle.resolve().unwrap();

    assert_eq!(resolved.source.workflow_path, WORKFLOW_PATH);
    assert!(resolved.required_imports().prompt);
    assert_eq!(resolved.content_digest.algorithm.as_str(), "sha256");
    assert_eq!(resolved.content_digest.value.len(), 64);
    assert!(
        resolved
            .content_digest
            .value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_eq!(
        resolved
            .source_closure
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        [
            "attachments/data.bin",
            "prompts/message.md",
            "prompts/system.md",
            "schemas/result.schema.json",
            WORKFLOW_PATH,
        ]
    );
    assert_eq!(
        resolved.source_bytes("attachments/data.bin"),
        Some([0, 0xff, 0x80, b'\n'].as_slice())
    );
    assert_eq!(
        resolved.source_bytes("prompts/message.md"),
        Some("Message: caf\u{e9}\n".as_bytes())
    );
    assert_eq!(
        resolved.source_bytes(WORKFLOW_PATH),
        Some(WORKFLOW.as_bytes())
    );

    let ValidatedStep::Agent(agent) = &resolved.definition.steps["agent"] else {
        panic!("agent fixture step must remain an agent step");
    };
    assert_eq!(agent.agent.profile, "coding");
    assert_eq!(
        agent.agent.harness,
        ValidatedHarness::Pi(PiConfig {
            model: "openai/gpt-5".to_owned(),
            thinking: Thinking::XHigh,
        })
    );
    assert_eq!(agent.agent.system_prompt, "prompts/system.md");
    assert_eq!(
        agent.agent.message.text[0],
        ValidatedMessageSource::File {
            path: "prompts/message.md".to_owned(),
        }
    );
    assert_eq!(
        agent.agent.message.attachments[0],
        ValidatedMessageSource::File {
            path: "attachments/data.bin".to_owned(),
        }
    );
    assert_eq!(
        agent.common.outputs["result"].definition,
        Output::JsonAgentResult {
            schema: "schemas/result.schema.json".to_owned(),
        }
    );
    assert_eq!(agent.common.cwd.as_deref(), Some("runtime/does-not-exist"));
    let retained_schema = resolved.json_schema("agent", "result").unwrap();
    assert_eq!(retained_schema.bytes(), RESULT_SCHEMA);
    assert_eq!(retained_schema.document()["type"], "object");

    let retained_digest = resolved.content_digest.clone();
    fs::write(
        bundle.root.join(WORKFLOW_PATH),
        format!("# changed\n{WORKFLOW}"),
    )
    .unwrap();
    let replacement = bundle.root.join("prompts/replacement.md");
    fs::write(&replacement, b"Replacement message.\n").unwrap();
    fs::remove_file(bundle.root.join("prompts/message.md")).unwrap();
    fs::rename(replacement, bundle.root.join("prompts/message.md")).unwrap();
    fs::write(
        bundle.root.join("schemas/result.schema.json"),
        br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"array"}
"#,
    )
    .unwrap();

    assert_eq!(resolved.content_digest, retained_digest);
    assert_eq!(
        resolved.source_bytes("prompts/message.md"),
        Some("Message: caf\u{e9}\n".as_bytes())
    );
    assert_eq!(
        resolved.source_bytes("schemas/result.schema.json"),
        Some(RESULT_SCHEMA)
    );
    assert_eq!(
        resolved.source_bytes(WORKFLOW_PATH),
        Some(WORKFLOW.as_bytes())
    );
}

#[test]
fn recovery_resolution_pins_profiles_paths_prompt_bytes_and_explicit_absence() {
    let bundle = FixtureBundle::new();
    fs::write(
        bundle.root.join("prompts/recovery.md"),
        b"Repair the failed target.\n",
    )
    .unwrap();
    let workflow = WORKFLOW
        .replace(
            "  agent:\n    kind: agent\n    cwd: runtime/does-not-exist\n",
            "  agent:\n    kind: agent\n    cwd: runtime/does-not-exist\n    recovery:\n      retries: 3\n      handler:\n        kind: agent\n        profile: coding\n        prompt: ../prompts/recovery.md\n        cwd: repairs\n",
        )
        .replace(
            "exports:\n",
            "  commandRepair:\n    kind: cmd\n    recovery:\n      retries: 2\n      handler:\n        kind: cmd\n        cwd: repairs\n        command:\n          argv: [\"./repair\", \"--generated\"]\n    command:\n      argv: [\"true\"]\nexports:\n",
        );
    fs::write(bundle.workflow_path(), workflow).unwrap();

    let first = bundle.resolve().unwrap();
    let recovery = first.definition.recoveries["agent"].as_ref().unwrap();
    assert_eq!(recovery.retries, 3);
    let Some(ValidatedRecoveryHandler::Agent {
        profile,
        prompt,
        cwd,
        harness,
    }) = &recovery.handler
    else {
        panic!("fixture recovery must use an agent handler");
    };
    assert_eq!(profile, "coding");
    assert_eq!(prompt, "prompts/recovery.md");
    assert_eq!(cwd.as_deref(), Some("repairs"));
    assert_eq!(
        harness,
        &ValidatedHarness::Pi(PiConfig {
            model: "openai/gpt-5".to_owned(),
            thinking: Thinking::XHigh,
        })
    );
    assert!(first.source_bytes("prompts/recovery.md").is_some());
    let command_recovery = first.definition.recoveries["commandRepair"]
        .as_ref()
        .unwrap();
    assert_eq!(command_recovery.retries, 2);
    assert_eq!(
        command_recovery.handler,
        Some(ValidatedRecoveryHandler::Command {
            argv: vec!["./repair".to_owned(), "--generated".to_owned()],
            cwd: Some("repairs".to_owned()),
        })
    );
    assert!(first.capacity.is_bound_to(&first.content_digest));

    fs::write(
        bundle.root.join("prompts/recovery.md"),
        b"Changed recovery prompt bytes.\n",
    )
    .unwrap();
    let second = bundle.resolve().unwrap();
    assert_ne!(first.content_digest, second.content_digest);
    assert_ne!(
        first.capacity.source_closure_digest,
        second.capacity.source_closure_digest
    );

    let omitted = FixtureBundle::new().resolve().unwrap();
    assert_eq!(omitted.definition.recoveries["agent"], None);
}

#[test]
fn recovery_resolution_rejects_unknown_profiles_and_invalid_static_paths() {
    for replacement in [
        "        profile: missing\n        prompt: ../prompts/recovery.md",
        "        profile: coding\n        prompt: ../../outside.md",
    ] {
        let bundle = FixtureBundle::new();
        fs::write(
            bundle.root.join("prompts/recovery.md"),
            b"Repair the failed target.\n",
        )
        .unwrap();
        let workflow = WORKFLOW.replace(
            "    agent:\n      profile: coding\n",
            &format!(
                "    recovery:\n      retries: 1\n      handler:\n        kind: agent\n{replacement}\n    agent:\n      profile: coding\n"
            ),
        );
        fs::write(bundle.workflow_path(), workflow).unwrap();
        assert!(bundle.resolve().is_err());
    }
}

#[test]
fn finalizer_static_sources_join_the_same_immutable_closure() {
    let bundle = FixtureBundle::new();
    fs::write(
        bundle.root.join("prompts/finalizer-system.md"),
        b"Finalizer system.\n",
    )
    .unwrap();
    fs::write(
        bundle.root.join("prompts/finalizer-message.md"),
        b"Finalizer message.\n",
    )
    .unwrap();
    fs::write(bundle.root.join("attachments/finalizer.bin"), [0x01, 0xff]).unwrap();
    fs::write(
        bundle.root.join("schemas/finalizer.schema.json"),
        RESULT_SCHEMA,
    )
    .unwrap();
    let workflow = WORKFLOW.replace(
        "exports:\n",
        "finalizers:\n  report:\n    kind: agent\n    agent:\n      profile: coding\n      systemPrompt: ../prompts/finalizer-system.md\n      message:\n        text:\n          - file: ../prompts/finalizer-message.md\n        attachments:\n          - file: ../attachments/finalizer.bin\n          - ref: finalization.context\n    outputs:\n      result:\n        kind: json\n        from: agent_result\n        schema: ../schemas/finalizer.schema.json\n      changes:\n        kind: git_branch\n        from: workspace\nexports:\n",
    );
    fs::write(bundle.workflow_path(), workflow).unwrap();

    let resolved = bundle.resolve().unwrap();
    assert!(resolved.requires_git_capture());
    for path in [
        "prompts/finalizer-system.md",
        "prompts/finalizer-message.md",
        "attachments/finalizer.bin",
        "schemas/finalizer.schema.json",
    ] {
        assert!(resolved.source_bytes(path).is_some(), "missing {path}");
    }
    let finalizer = &resolved.definition.finalizers["report"];
    let ValidatedStep::Agent(finalizer) = &finalizer.body else {
        panic!("report must remain an agent finalizer");
    };
    assert_eq!(finalizer.agent.system_prompt, "prompts/finalizer-system.md");
    assert_eq!(
        finalizer.agent.message.text[0],
        ValidatedMessageSource::File {
            path: "prompts/finalizer-message.md".to_owned(),
        }
    );
    assert_eq!(
        finalizer.agent.message.attachments[0],
        ValidatedMessageSource::File {
            path: "attachments/finalizer.bin".to_owned(),
        }
    );
    assert_eq!(
        finalizer.common.outputs["result"].definition,
        Output::JsonAgentResult {
            schema: "schemas/finalizer.schema.json".to_owned(),
        }
    );
    assert_eq!(
        resolved.json_schema("report", "result").unwrap().bytes(),
        RESULT_SCHEMA
    );
}

#[test]
fn oversized_retained_source_is_rejected_before_it_can_exhaust_the_closure_budget() {
    let bundle = FixtureBundle::new();
    let attachment_path = bundle.root.join("attachments/data.bin");
    let attachment = fs::File::create(attachment_path).unwrap();
    attachment.set_len(MAX_SOURCE_CLOSURE_BYTES + 1).unwrap();
    drop(attachment);

    assert_failure(
        bundle.resolve(),
        ResolutionFailureKind::DigestInputTooLarge,
        ResolutionLocation::MessageAttachment {
            step: "agent".to_owned(),
            index: 0,
        },
    );
}

#[test]
fn shared_cloud_and_runner_rejection_fixtures_fail_resolution() {
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/workflow/v1/resolution-invalid");

    for fixture in ["unknown-dependency", "nested-result-schema-resource"] {
        let root = fixtures.join(fixture);
        assert!(
            resolve_workflow_file(&root, &root.join("workflow.yaml")).is_err(),
            "shared rejection fixture {fixture} unexpectedly resolved"
        );
    }
}

#[test]
fn shared_cloud_and_runner_closure_fixture_has_the_normative_digest() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/workflow/v1/closure/complete");
    let expected: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("expected.json")).unwrap()).unwrap();
    let workflow_path = expected["workflowPath"].as_str().unwrap();
    let resolved = resolve_workflow_file(&root, &root.join(workflow_path)).unwrap();
    let expected_paths = expected["paths"]
        .as_array()
        .unwrap()
        .iter()
        .map(|path| path.as_str().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        resolved
            .source_closure
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        expected_paths
    );
    assert_eq!(
        resolved.content_digest.algorithm.as_str(),
        expected["algorithm"].as_str().unwrap()
    );
    assert_eq!(
        resolved.content_digest.value,
        expected["value"].as_str().unwrap()
    );
}

#[test]
fn digest_has_a_normative_known_answer_and_is_sensitive_to_paths() {
    let mut closure = BTreeMap::new();
    closure.insert(
        "a/\u{e9}.bin".to_owned(),
        Arc::<[u8]>::from([0, 0xff, b'\n']),
    );
    closure.insert(
        "workflow.yaml".to_owned(),
        Arc::<[u8]>::from(&b"schemaVersion: 1\n"[..]),
    );

    let digest = digest_source_closure(&closure).unwrap();
    assert_eq!(digest.algorithm, ContentDigestAlgorithm::Sha256);
    assert_eq!(
        digest.value,
        "a7c4c26a7d260dae5198e6c8b7c0942e5ecf3ad51a2b75a0242f23da3a107a00"
    );

    let content = closure.remove("a/\u{e9}.bin").unwrap();
    closure.insert("a/renamed.bin".to_owned(), content);
    assert_ne!(digest_source_closure(&closure).unwrap(), digest);
}

#[test]
fn profile_declarations_and_references_are_pinned_by_resolution() {
    let renamed = FixtureBundle::new();
    fs::write(
        renamed.workflow_path(),
        WORKFLOW
            .replace("  coding:\n", "  renamed:\n")
            .replace("profile: coding", "profile: renamed"),
    )
    .unwrap();
    let renamed = renamed.resolve().unwrap();

    let changed_config = FixtureBundle::new();
    fs::write(
        changed_config.workflow_path(),
        WORKFLOW.replace("model: openai/gpt-5", "model: openai/gpt-4.1"),
    )
    .unwrap();
    let changed_config = changed_config.resolve().unwrap();

    let original = FixtureBundle::new().resolve().unwrap();
    assert_ne!(renamed.content_digest, original.content_digest);
    assert_ne!(changed_config.content_digest, original.content_digest);

    let ValidatedStep::Agent(agent) = &changed_config.definition.steps["agent"] else {
        panic!("agent fixture step must remain an agent step");
    };
    assert_eq!(
        agent.agent.harness,
        ValidatedHarness::Pi(PiConfig {
            model: "openai/gpt-4.1".to_owned(),
            thinking: Thinking::XHigh,
        })
    );
}

#[test]
fn digest_is_portable_across_source_roots_and_changes_with_retained_bytes() {
    let first = FixtureBundle::new();
    let second = FixtureBundle::new();
    let first_resolved = first.resolve().unwrap();
    let second_resolved = second.resolve().unwrap();

    assert_ne!(
        fs::canonicalize(&first.root).unwrap(),
        fs::canonicalize(&second.root).unwrap()
    );
    assert_eq!(
        first_resolved.content_digest,
        second_resolved.content_digest
    );

    fs::write(
        second.root.join("prompts/system.md"),
        b"Changed system prompt.\n",
    )
    .unwrap();
    let changed = second.resolve().unwrap();
    assert_ne!(changed.content_digest, first_resolved.content_digest);
}

#[test]
fn missing_non_regular_and_lexically_escaping_sources_are_rejected() {
    let missing = FixtureBundle::new();
    fs::remove_file(missing.root.join("prompts/system.md")).unwrap();
    assert_failure(
        missing.resolve(),
        ResolutionFailureKind::SourceUnavailable,
        ResolutionLocation::SystemPrompt {
            step: "agent".to_owned(),
        },
    );

    let non_regular = FixtureBundle::new();
    fs::remove_file(non_regular.root.join("prompts/system.md")).unwrap();
    fs::create_dir(non_regular.root.join("prompts/system.md")).unwrap();
    assert_failure(
        non_regular.resolve(),
        ResolutionFailureKind::SourceNotRegularFile,
        ResolutionLocation::SystemPrompt {
            step: "agent".to_owned(),
        },
    );

    let lexical = FixtureBundle::new();
    fs::write(
        lexical.workflow_path(),
        WORKFLOW.replace("../prompts/system.md", "../../outside-source-root.md"),
    )
    .unwrap();
    assert_failure(
        lexical.resolve(),
        ResolutionFailureKind::LexicalSourceEscape,
        ResolutionLocation::SystemPrompt {
            step: "agent".to_owned(),
        },
    );

    let selected_directory = FixtureBundle::new();
    assert_failure(
        resolve(&selected_directory.root, Path::new("workflows")),
        ResolutionFailureKind::SourceNotRegularFile,
        ResolutionLocation::Workflow,
    );

    let selected_missing = FixtureBundle::new();
    assert_failure(
        resolve(&selected_missing.root, Path::new("workflows/missing.yaml")),
        ResolutionFailureKind::SourceUnavailable,
        ResolutionLocation::Workflow,
    );

    let unavailable_root = FixtureBundle::new();
    assert_failure(
        resolve(
            &unavailable_root.root.join("missing"),
            Path::new(WORKFLOW_PATH),
        ),
        ResolutionFailureKind::SourceRootUnavailable,
        ResolutionLocation::SourceRoot,
    );

    let file_root = FixtureBundle::new();
    assert_failure(
        resolve(&file_root.workflow_path(), Path::new("complete.yaml")),
        ResolutionFailureKind::SourceRootNotDirectory,
        ResolutionLocation::SourceRoot,
    );

    let selected_escape = FixtureBundle::new();
    assert_failure(
        resolve(&selected_escape.root, Path::new("../outside.yaml")),
        ResolutionFailureKind::LexicalSourceEscape,
        ResolutionLocation::Workflow,
    );
}

#[cfg(unix)]
#[test]
fn a_symbolic_link_source_root_normalizes_an_absolute_selected_path() {
    use std::os::unix::fs::symlink;

    let bundle = FixtureBundle::new();
    let root_alias = bundle.root.parent().unwrap().join("source-alias");
    symlink(&bundle.root, &root_alias).unwrap();

    let resolved = resolve(&root_alias, &root_alias.join(WORKFLOW_PATH)).unwrap();
    assert_eq!(resolved.source.workflow_path, WORKFLOW_PATH);
    assert_eq!(
        resolved.content_digest,
        bundle.resolve().unwrap().content_digest
    );
}

#[cfg(unix)]
#[test]
fn an_absolute_selected_path_cannot_hide_a_symbolic_link_escape_with_parent_traversal() {
    use std::os::unix::fs::symlink;

    let bundle = FixtureBundle::new();
    let outside = bundle.root.parent().unwrap().join("outside");
    fs::create_dir_all(outside.join("child")).unwrap();
    fs::create_dir_all(outside.join("workflows")).unwrap();
    fs::write(outside.join(WORKFLOW_PATH), WORKFLOW).unwrap();
    symlink(outside.join("child"), bundle.root.join("detour")).unwrap();
    let selected = bundle.root.join("detour/../workflows/complete.yaml");
    assert_eq!(
        fs::canonicalize(&selected).unwrap(),
        fs::canonicalize(outside.join(WORKFLOW_PATH)).unwrap()
    );

    assert!(matches!(
        resolve(&bundle.root, &selected),
        Err(failure)
            if failure.kind() == ResolutionFailureKind::SymbolicLinkEscape
                && failure.location() == &ResolutionLocation::Workflow
    ));
}

#[cfg(unix)]
#[test]
fn symbolic_link_escapes_are_rejected() {
    use std::os::unix::fs::symlink;

    let static_escape = FixtureBundle::new();
    let outside = static_escape.root.parent().unwrap().join("outside.md");
    fs::write(&outside, b"Outside.\n").unwrap();
    fs::remove_file(static_escape.root.join("prompts/system.md")).unwrap();
    symlink(&outside, static_escape.root.join("prompts/system.md")).unwrap();
    assert_failure(
        static_escape.resolve(),
        ResolutionFailureKind::SymbolicLinkEscape,
        ResolutionLocation::SystemPrompt {
            step: "agent".to_owned(),
        },
    );

    let selected_escape = FixtureBundle::new();
    let outside_workflow = selected_escape.root.parent().unwrap().join("outside.yaml");
    fs::write(&outside_workflow, WORKFLOW).unwrap();
    fs::remove_file(selected_escape.workflow_path()).unwrap();
    symlink(&outside_workflow, selected_escape.workflow_path()).unwrap();
    assert_failure(
        selected_escape.resolve(),
        ResolutionFailureKind::SymbolicLinkEscape,
        ResolutionLocation::Workflow,
    );
}

// Darwin filesystems reject non-UTF-8 names before the resolver can inspect them.
#[cfg(all(unix, not(target_os = "macos")))]
#[test]
fn non_utf8_canonical_components_are_rejected() {
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::symlink;

    let invalid_utf8 = FixtureBundle::new();
    let invalid_component = OsString::from_vec(vec![b'n', b'o', b'n', 0xff, b'u', b't', b'f']);
    let invalid_directory = invalid_utf8.root.join(&invalid_component);
    fs::create_dir(&invalid_directory).unwrap();
    fs::write(invalid_directory.join("system.md"), b"System prompt.\n").unwrap();
    fs::remove_file(invalid_utf8.root.join("prompts/system.md")).unwrap();
    symlink(
        PathBuf::from("..")
            .join(&invalid_component)
            .join("system.md"),
        invalid_utf8.root.join("prompts/system.md"),
    )
    .unwrap();
    assert_failure(
        invalid_utf8.resolve(),
        ResolutionFailureKind::InvalidCanonicalPath,
        ResolutionLocation::SystemPrompt {
            step: "agent".to_owned(),
        },
    );
}

#[test]
fn required_text_and_json_schema_contracts_are_validated() {
    let system_encoding = FixtureBundle::new();
    fs::write(system_encoding.root.join("prompts/system.md"), [0xff]).unwrap();
    assert_failure_kind(
        system_encoding.resolve(),
        ResolutionFailureKind::InvalidTextEncoding,
    );

    let message_encoding = FixtureBundle::new();
    fs::write(message_encoding.root.join("prompts/message.md"), [0xff]).unwrap();
    assert_failure_kind(
        message_encoding.resolve(),
        ResolutionFailureKind::InvalidTextEncoding,
    );

    for (bytes, expected) in [
        (
            vec![0xff],
            ResolutionFailureKind::InvalidResultSchemaEncoding,
        ),
        (
            b"{".to_vec(),
            ResolutionFailureKind::InvalidResultSchemaJson,
        ),
        (
            br#"{"type":"object"}"#.to_vec(),
            ResolutionFailureKind::InvalidResultSchemaDialect,
        ),
        (
            br#"{"$schema":"https://json-schema.org/draft/2019-09/schema","type":"object"}"#
                .to_vec(),
            ResolutionFailureKind::InvalidResultSchemaDialect,
        ),
        (
            br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":42}"#.to_vec(),
            ResolutionFailureKind::InvalidResultSchema,
        ),
    ] {
        let bundle = FixtureBundle::new();
        fs::write(bundle.root.join("schemas/result.schema.json"), bytes).unwrap();
        assert_failure_kind(bundle.resolve(), expected);
    }
}

#[test]
fn self_contained_schema_resources_and_fragment_references_resolve() {
    let accepted = [
        serde_json::json!({
            "$schema": JSON_SCHEMA_DIALECT,
            "$id": "https://schemas.example.invalid/authored-root",
            "$defs": {
                "slash/name": {"type": "object", "$anchor": "plain"},
                "til~de": {"type": "object"},
                "dynamic": {
                    "$dynamicAnchor": "node",
                    "type": "object",
                    "properties": {"next": {"$dynamicRef": "#node"}}
                }
            },
            "allOf": [
                {"$ref": "#/$defs/slash~1name"},
                {"$ref": "#/$defs/til~0de"},
                {"$ref": "#plain"},
                {"$dynamicRef": "#node"}
            ]
        }),
        serde_json::json!({
            "$schema": JSON_SCHEMA_DIALECT,
            "$ref": "#"
        }),
        serde_json::json!({
            "$schema": JSON_SCHEMA_DIALECT,
            "properties": {
                "$ref": {"type": "string"},
                "$id": {"type": "string"}
            },
            "const": {
                "$ref": "other.json",
                "$dynamicRef": "other.json",
                "$id": "literal",
                "$schema": "literal",
                "$vocabulary": "literal"
            },
            "enum": [{"$ref": "other.json"}],
            "default": {"$id": "literal"},
            "examples": [{"$schema": "literal"}],
            "pattern": "^(a+)+$"
        }),
    ];

    for schema in accepted {
        let bundle = FixtureBundle::new();
        write_json_schema(&bundle, &schema);
        bundle.resolve().unwrap();
    }
}

#[test]
fn unsupported_schema_resources_and_references_fail_at_the_json_schema_location() {
    let invalid_references = [
        serde_json::json!({"$schema": JSON_SCHEMA_DIALECT, "$ref": "other.json"}),
        serde_json::json!({"$schema": JSON_SCHEMA_DIALECT, "$dynamicRef": "https://example.invalid/schema#node"}),
        serde_json::json!({"$schema": JSON_SCHEMA_DIALECT, "$ref": "#/$defs/missing"}),
        serde_json::json!({"$schema": JSON_SCHEMA_DIALECT, "$ref": "#/const/value", "const": {"value": {"type": "string"}}}),
        serde_json::json!({"$schema": JSON_SCHEMA_DIALECT, "$ref": "#missing"}),
        serde_json::json!({
            "$schema": JSON_SCHEMA_DIALECT,
            "$defs": {
                "first": {"$anchor": "duplicate"},
                "second": {"$anchor": "duplicate"}
            }
        }),
        serde_json::json!({"$schema": JSON_SCHEMA_DIALECT, "$defs": {"nested": {"$id": "nested"}}}),
        serde_json::json!({"$schema": JSON_SCHEMA_DIALECT, "properties": {"nested": {"$id": "nested"}}}),
        serde_json::json!({"$schema": JSON_SCHEMA_DIALECT, "items": {"$id": "nested"}}),
        serde_json::json!({"$schema": JSON_SCHEMA_DIALECT, "if": {"$id": "nested"}}),
    ];

    for schema in invalid_references {
        assert_json_schema_failure(schema, ResolutionFailureKind::InvalidResultSchemaReference);
    }
}

#[test]
fn nested_dialects_vocabularies_and_unsupported_patterns_are_rejected() {
    let invalid_dialects = [
        serde_json::json!({
            "$schema": JSON_SCHEMA_DIALECT,
            "$defs": {"nested": {"$schema": JSON_SCHEMA_DIALECT}}
        }),
        serde_json::json!({
            "$schema": JSON_SCHEMA_DIALECT,
            "$vocabulary": {"https://example.invalid/vocabulary": true}
        }),
        serde_json::json!({
            "$schema": JSON_SCHEMA_DIALECT,
            "properties": {"nested": {"$vocabulary": {}}}
        }),
    ];
    for schema in invalid_dialects {
        assert_json_schema_failure(schema, ResolutionFailureKind::InvalidResultSchemaDialect);
    }

    for schema in [
        serde_json::json!({"$schema": JSON_SCHEMA_DIALECT, "pattern": "^(a+)\\1$"}),
        serde_json::json!({"$schema": JSON_SCHEMA_DIALECT, "pattern": "(?=a)a"}),
        serde_json::json!({
            "$schema": JSON_SCHEMA_DIALECT,
            "patternProperties": {"(?<=a)b": {"type": "string"}}
        }),
    ] {
        assert_json_schema_failure(schema, ResolutionFailureKind::InvalidResultSchema);
    }

    let literal_pattern = FixtureBundle::new();
    write_json_schema(
        &literal_pattern,
        &serde_json::json!({
            "$schema": JSON_SCHEMA_DIALECT,
            "const": {"pattern": "(?=unsupported literal)"},
            "examples": [{"patternProperties": {"(?=literal)": true}}]
        }),
    );
    literal_pattern.resolve().unwrap();
}

fn assert_json_schema_failure(schema: Value, kind: ResolutionFailureKind) {
    let bundle = FixtureBundle::new();
    write_json_schema(&bundle, &schema);
    assert_failure(
        bundle.resolve(),
        kind,
        ResolutionLocation::ResultSchema {
            step: "agent".to_owned(),
            output: "result".to_owned(),
        },
    );
}

fn write_json_schema(bundle: &FixtureBundle, schema: &Value) {
    fs::write(
        bundle.root.join("schemas/result.schema.json"),
        serde_json::to_vec(schema).unwrap(),
    )
    .unwrap();
}

#[test]
fn structurally_invalid_source_and_runtime_paths_fail_during_definition_resolution() {
    for invalid_workflow in [
        WORKFLOW.replace("../prompts/system.md", "/outside-source-root.md"),
        WORKFLOW.replace("cwd: runtime/does-not-exist", "cwd: ../outside-runtime"),
        WORKFLOW.replace(
            "path: runtime/does-not-exist/artifact.bin",
            "path: runtime/../artifact.bin",
        ),
    ] {
        let bundle = FixtureBundle::new();
        fs::write(bundle.workflow_path(), invalid_workflow).unwrap();
        assert_failure_kind(
            bundle.resolve(),
            ResolutionFailureKind::InvalidWorkflowDocument(DecodeFailureKind::StructuralContract),
        );
    }
}

#[cfg(unix)]
#[test]
fn runtime_paths_are_not_resolved_against_a_source_or_execution_root() {
    use std::os::unix::fs::symlink;

    let bundle = FixtureBundle::new();
    let outside_runtime = bundle.root.parent().unwrap().join("outside-runtime");
    fs::create_dir(&outside_runtime).unwrap();
    symlink(&outside_runtime, bundle.root.join("runtime")).unwrap();

    bundle.resolve().unwrap();
}

fn assert_failure(
    result: Result<ResolvedWorkflow, ResolutionFailure>,
    kind: ResolutionFailureKind,
    location: ResolutionLocation,
) {
    let failure = result.unwrap_err();
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.location(), &location);
}

fn assert_failure_kind(
    result: Result<ResolvedWorkflow, ResolutionFailure>,
    kind: ResolutionFailureKind,
) {
    assert_eq!(result.unwrap_err().kind(), kind);
}
