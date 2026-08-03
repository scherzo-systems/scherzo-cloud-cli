use std::fs;
use std::path::{Path, PathBuf};

use super::document::{
    HarnessDefinition, MessageSource, Output, OutputReference, Step, ValueReference,
};
use super::*;

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/workflow/v1")
}

fn canonical_valid_fixture() -> Vec<u8> {
    fs::read(fixture_root().join("valid/plan-implement-test.yaml")).unwrap()
}

#[test]
fn canonical_workflow_decodes_into_the_complete_execution_document() {
    let workflow = decode(&canonical_valid_fixture()).unwrap();

    assert_eq!(workflow.schema_version, 1);
    assert_eq!(
        workflow.description.as_deref(),
        Some("Plan, implement, test, and export a requested change.")
    );
    assert_eq!(workflow.agent_profiles.len(), 1);
    assert_eq!(workflow.steps.len(), 4);
    assert_eq!(
        workflow.step_order,
        ["prepare", "plan", "implement", "test"]
    );
    assert_eq!(workflow.exports.len(), 2);
    assert_eq!(
        workflow.agent_profiles["coding"].harness,
        HarnessDefinition::Pi {
            config: serde_json::json!({
                "model": "openai/gpt-5",
                "thinking": "high",
            }),
        }
    );

    let Step::Command(prepare) = &workflow.steps["prepare"] else {
        panic!("prepare must be a command step");
    };
    assert_eq!(prepare.argv, ["./scripts/prepare-workspace.sh"]);
    assert!(prepare.common.control_dependencies.is_empty());
    assert!(prepare.inputs.is_empty());
    assert!(prepare.common.outputs.is_empty());

    let Step::Agent(plan) = &workflow.steps["plan"] else {
        panic!("plan must be an agent step");
    };
    assert_eq!(plan.common.control_dependencies, ["prepare"]);
    assert_eq!(plan.agent.profile, "coding");
    assert_eq!(plan.agent.system_prompt, "prompts/plan-system.md");
    assert_eq!(
        plan.agent.message.text,
        [MessageSource::Reference(ValueReference::Import {
            name: "prompt".to_owned()
        })]
    );
    assert_eq!(
        plan.agent.message.attachments,
        [MessageSource::Reference(ValueReference::Import {
            name: "attachments".to_owned()
        })]
    );
    assert_eq!(
        plan.common.outputs["plan"],
        Output::AgentResult {
            schema: "schemas/change-plan.schema.json".to_owned()
        }
    );
    assert_eq!(
        plan.common.outputs["artifact"],
        Output::File {
            path: "artifacts/plan.txt".to_owned(),
            media_type: "text/plain".to_owned(),
        }
    );

    let Step::Agent(implement) = &workflow.steps["implement"] else {
        panic!("implement must be an agent step");
    };
    assert!(implement.common.control_dependencies.is_empty());
    assert_eq!(implement.agent.profile, "coding");
    assert_eq!(implement.agent.system_prompt, "prompts/implement-system.md");
    assert_eq!(
        implement.agent.message.text,
        [
            MessageSource::File {
                path: "prompts/implement-message.md".to_owned()
            },
            MessageSource::Reference(ValueReference::Import {
                name: "prompt".to_owned()
            })
        ]
    );
    assert_eq!(
        implement.agent.message.attachments,
        [
            MessageSource::Reference(ValueReference::Import {
                name: "attachments".to_owned()
            }),
            MessageSource::Reference(ValueReference::Output(OutputReference {
                step: "plan".to_owned(),
                output: "plan".to_owned(),
            })),
            MessageSource::Reference(ValueReference::Output(OutputReference {
                step: "plan".to_owned(),
                output: "artifact".to_owned(),
            }))
        ]
    );
    assert_eq!(implement.common.outputs["response"], Output::AgentResponse);

    let Step::Command(test) = &workflow.steps["test"] else {
        panic!("test must be a command step");
    };
    assert!(test.common.control_dependencies.is_empty());
    assert_eq!(test.common.cwd.as_deref(), Some("packages/api"));
    assert_eq!(test.argv, ["./scripts/test.sh"]);
    assert_eq!(
        test.inputs["prompt"],
        ValueReference::Import {
            name: "prompt".to_owned(),
        }
    );
    assert_eq!(
        test.inputs["changeSummary"],
        ValueReference::Output(OutputReference {
            step: "implement".to_owned(),
            output: "response".to_owned(),
        })
    );
    assert_eq!(
        test.common.outputs["report"],
        Output::File {
            path: "packages/api/artifacts/test-report.xml".to_owned(),
            media_type: "application/junit+xml".to_owned(),
        }
    );

    assert_eq!(
        workflow.exports["response"],
        OutputReference {
            step: "implement".to_owned(),
            output: "response".to_owned(),
        }
    );
    assert_eq!(
        workflow.exports["testReport"],
        OutputReference {
            step: "test".to_owned(),
            output: "report".to_owned(),
        }
    );
}

#[test]
fn every_canonical_invalid_fixture_is_a_structural_failure() {
    let invalid_root = fixture_root().join("invalid");
    let fixtures = fs::read_dir(&invalid_root)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(!fixtures.is_empty());

    for fixture in fixtures {
        let path = fixture.path();
        let error = decode(&fs::read(&path).unwrap()).unwrap_err();
        assert_eq!(
            error.kind(),
            DecodeFailureKind::StructuralContract,
            "unexpected classification for {}",
            path.display()
        );
    }
}

#[test]
fn unused_agent_profiles_still_use_the_pi_config_contract() {
    let source = b"schemaVersion: 1
agentProfiles:
  unused:
    harness:
      kind: pi
      config:
        model: ''
        thinking: high
steps:
  command:
    kind: cmd
    command:
      argv: [\"true\"]
";

    assert_eq!(
        decode(source).unwrap_err().kind(),
        DecodeFailureKind::StructuralContract
    );
}

#[test]
fn forbidden_yaml_features_are_not_normalized() {
    let cases = [
        (
            "duplicate mapping key",
            "schemaVersion: 1\nschemaVersion: 1\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "non-string mapping key",
            "schemaVersion: 1\nsteps: {1: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "anchor",
            "schemaVersion: 1\ndescription: &description text\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "alias",
            "schemaVersion: 1\ndescription: *description\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "merge key",
            "schemaVersion: 1\nsteps:\n  prepare:\n    <<: {cwd: work}\n    kind: cmd\n    command: {argv: [true]}\n",
        ),
        (
            "custom tag",
            "schemaVersion: 1\ndescription: !text hello\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "multiple documents",
            "schemaVersion: 1\nsteps: {one: {kind: cmd, command: {argv: [true]}}}\n---\nschemaVersion: 1\nsteps: {two: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "YAML 1.1 directive",
            "%YAML 1.1\n---\nschemaVersion: 1\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "non-JSON core float",
            "schemaVersion: 1\ndescription: .nan\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
    ];

    for (name, source) in cases {
        let error = decode(source.as_bytes()).unwrap_err();
        assert_eq!(
            error.kind(),
            DecodeFailureKind::ForbiddenYaml,
            "unexpected classification for {name}"
        );
    }
}

#[test]
fn yaml_1_2_core_does_not_apply_legacy_scalar_coercions() {
    let workflow = decode(
        b"%YAML 1.2\n---\nschemaVersion: 01\ndescription: yes\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n",
    )
    .unwrap();

    assert_eq!(workflow.schema_version, 1);
    assert_eq!(workflow.description.as_deref(), Some("yes"));
    let Step::Command(prepare) = &workflow.steps["prepare"] else {
        panic!("prepare must be a command step");
    };
    assert_eq!(prepare.argv, ["true"]);

    let workflow = decode(
        b"schemaVersion: 0x1\ndescription: +.nan\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n",
    )
    .unwrap();
    assert_eq!(workflow.description.as_deref(), Some("+.nan"));

    let workflow = decode(
        b"schemaVersion: 1.\ndescription: +0x1\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n",
    )
    .unwrap();
    assert_eq!(workflow.schema_version, 1);
    assert_eq!(workflow.description.as_deref(), Some("+0x1"));

    assert_eq!(
        decode(
            b"schemaVersion: 1\ndescription: 1.\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n",
        )
        .unwrap_err()
        .kind(),
        DecodeFailureKind::StructuralContract
    );
}

#[test]
fn schema_version_does_not_round_to_one() {
    let error = decode(
        b"schemaVersion: 1.0000000000000001\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n",
    )
    .expect_err("a distinct Core float must not satisfy the schemaVersion const");

    assert_eq!(error.kind(), DecodeFailureKind::StructuralContract);
}

#[test]
fn explicit_yaml_core_tags_are_accepted() {
    let workflow = decode(
        b"schemaVersion: !!int 1\ndescription: !!str true\nsteps: !!map {prepare: !!map {kind: cmd, command: !!map {argv: !!seq [\"true\"]}}}\n",
    )
    .expect("standard YAML Core tags are not custom tags");

    assert_eq!(workflow.schema_version, 1);
    assert_eq!(workflow.description.as_deref(), Some("true"));
    let Step::Command(prepare) = &workflow.steps["prepare"] else {
        panic!("prepare must be a command step");
    };
    assert_eq!(prepare.argv, ["true"]);
}

#[test]
fn failures_are_classified_without_unbounded_dependency_diagnostics() {
    let failures = [
        decode(b"schemaVersion: [\n").unwrap_err(),
        decode(
            b"schemaVersion: 1\nsteps: {prepare: &step {kind: cmd, command: {argv: [true]}}}\n",
        )
        .unwrap_err(),
        decode(
            b"schemaVersion: 1\nsteps: {prepare: {kind: cmd, unknown: true, command: {argv: [true]}}}\n",
        )
        .unwrap_err(),
    ];

    assert_eq!(failures[0].kind(), DecodeFailureKind::MalformedYaml);
    assert_eq!(failures[1].kind(), DecodeFailureKind::ForbiddenYaml);
    assert_eq!(failures[2].kind(), DecodeFailureKind::StructuralContract);
    for failure in failures {
        assert!(failure.diagnostic().len() <= MAX_DECODE_DIAGNOSTIC_BYTES);
        assert!(failure.to_string().len() <= MAX_DECODE_DIAGNOSTIC_BYTES);
    }

    assert_eq!(
        decode(b"schemaVersion: \xff").unwrap_err().kind(),
        DecodeFailureKind::MalformedYaml
    );
}
