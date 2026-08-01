use std::collections::BTreeMap;
#[cfg(unix)]
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use super::*;
use crate::execution::workflow::pi::{PiConfig, Thinking};
use crate::execution::workflow::validated::{
    ValidatedHarness, ValidatedMessageSource, ValidatedStep,
};

const WORKFLOW_PATH: &str = "workflows/complete.yaml";
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
        kind: agent_result
        schema: ../schemas/result.schema.json
      artifact:
        kind: file
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
        resolve(&self.root, Path::new(WORKFLOW_PATH))
    }
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
        Output::AgentResult {
            schema: "schemas/result.schema.json".to_owned(),
        }
    );
    assert_eq!(agent.common.cwd.as_deref(), Some("runtime/does-not-exist"));

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
fn symbolic_link_escapes_and_non_utf8_canonical_components_are_rejected() {
    use std::os::unix::ffi::OsStringExt;
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
fn required_text_and_result_schema_contracts_are_validated() {
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
