use std::fs;
use std::path::{Path, PathBuf};

use super::document::{
    FailurePolicy, FinalizationTrigger, HarnessDefinition, MessageSource, NodeBody, Output,
    OutputReference, RecoveryHandler, ValueReference,
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
    assert!(workflow.finalizers.is_empty());
    assert!(workflow.finalizer_order.is_empty());
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

    let prepare = &workflow.steps["prepare"];
    let NodeBody::Command(prepare_body) = &prepare.body else {
        panic!("prepare must be a command step");
    };
    assert_eq!(prepare_body.argv, ["./scripts/prepare-workspace.sh"]);
    assert!(prepare.control_dependencies.is_empty());
    assert!(prepare_body.inputs.is_empty());
    assert!(prepare_body.common.outputs.is_empty());

    let plan = &workflow.steps["plan"];
    let NodeBody::Agent(plan_body) = &plan.body else {
        panic!("plan must be an agent step");
    };
    assert_eq!(plan.control_dependencies, ["prepare"]);
    assert_eq!(plan_body.agent.profile, "coding");
    assert_eq!(plan_body.agent.system_prompt, "prompts/plan-system.md");
    assert_eq!(
        plan_body.agent.message.text,
        [MessageSource::Reference(ValueReference::Import {
            name: "prompt".to_owned()
        })]
    );
    assert_eq!(
        plan_body.agent.message.attachments,
        [MessageSource::Reference(ValueReference::Import {
            name: "attachments".to_owned()
        })]
    );
    assert_eq!(
        plan_body.common.outputs["plan"],
        Output::AgentResult {
            schema: "schemas/change-plan.schema.json".to_owned()
        }
    );
    assert_eq!(
        plan_body.common.outputs["artifact"],
        Output::File {
            path: "artifacts/plan.txt".to_owned(),
            media_type: "text/plain".to_owned(),
        }
    );

    let implement = &workflow.steps["implement"];
    let NodeBody::Agent(implement_body) = &implement.body else {
        panic!("implement must be an agent step");
    };
    assert!(implement.control_dependencies.is_empty());
    assert_eq!(implement_body.agent.profile, "coding");
    assert_eq!(
        implement_body.agent.system_prompt,
        "prompts/implement-system.md"
    );
    assert_eq!(
        implement_body.agent.message.text,
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
        implement_body.agent.message.attachments,
        [
            MessageSource::Reference(ValueReference::Import {
                name: "attachments".to_owned()
            }),
            MessageSource::Reference(ValueReference::Output(OutputReference {
                node: "plan".to_owned(),
                output: "plan".to_owned(),
            })),
            MessageSource::Reference(ValueReference::Output(OutputReference {
                node: "plan".to_owned(),
                output: "artifact".to_owned(),
            }))
        ]
    );
    assert_eq!(
        implement_body.common.outputs["response"],
        Output::AgentResponse
    );

    let test = &workflow.steps["test"];
    let NodeBody::Command(test_body) = &test.body else {
        panic!("test must be a command step");
    };
    assert!(test.control_dependencies.is_empty());
    assert_eq!(test_body.common.cwd.as_deref(), Some("packages/api"));
    assert_eq!(test_body.argv, ["./scripts/test.sh"]);
    assert_eq!(
        test_body.inputs["prompt"],
        ValueReference::Import {
            name: "prompt".to_owned(),
        }
    );
    assert_eq!(
        test_body.inputs["changeSummary"],
        ValueReference::Output(OutputReference {
            node: "implement".to_owned(),
            output: "response".to_owned(),
        })
    );
    assert_eq!(
        test_body.common.outputs["report"],
        Output::File {
            path: "packages/api/artifacts/test-report.xml".to_owned(),
            media_type: "application/junit+xml".to_owned(),
        }
    );

    assert_eq!(
        workflow.exports["response"],
        OutputReference {
            node: "implement".to_owned(),
            output: "response".to_owned(),
        }
    );
    assert_eq!(
        workflow.exports["testReport"],
        OutputReference {
            node: "test".to_owned(),
            output: "report".to_owned(),
        }
    );
}

#[test]
fn workflow_v1_decodes_the_three_closed_recovery_forms_only_on_ordinary_steps() {
    let fixture = fs::read(fixture_root().join("valid/recovery-forms.yaml")).unwrap();
    let workflow = decode(&fixture).unwrap();

    let handlerless = workflow.steps["handlerless"].recovery.as_ref().unwrap();
    assert_eq!(handlerless.retries, 1);
    assert_eq!(handlerless.handler, None);
    let command = workflow.steps["commandRepair"].recovery.as_ref().unwrap();
    assert_eq!(command.retries, 2);
    assert_eq!(
        command.handler,
        Some(RecoveryHandler::Command {
            argv: vec!["./repair".to_owned(), "--generated".to_owned()],
            cwd: Some("repair".to_owned()),
        })
    );
    let agent = workflow.steps["agentRepair"].recovery.as_ref().unwrap();
    assert_eq!(agent.retries, 10);
    assert_eq!(
        agent.handler,
        Some(RecoveryHandler::Agent {
            profile: "repair".to_owned(),
            prompt: "prompts/recovery.md".to_owned(),
            cwd: Some("repair".to_owned()),
        })
    );
    assert!(workflow.finalizers.is_empty());
}

#[test]
fn workflow_v1_decodes_all_three_closed_agent_profiles() {
    let fixture = fs::read(fixture_root().join("valid/mixed-agent-harnesses.yaml")).unwrap();
    let workflow = decode(&fixture).unwrap();

    assert_eq!(workflow.agent_profiles.len(), 3);
    assert_eq!(
        workflow.agent_profiles["claudeCoding"].harness,
        HarnessDefinition::ClaudeCode {
            config: serde_json::json!({
                "model": "claude-opus-4-1",
                "effort": "xhigh",
            }),
        }
    );
    assert_eq!(
        workflow.agent_profiles["piCoding"].harness,
        HarnessDefinition::Pi {
            config: serde_json::json!({
                "model": "openai/gpt-5",
                "thinking": "high",
            }),
        }
    );
    assert_eq!(
        workflow.agent_profiles["codexCoding"].harness,
        HarnessDefinition::Codex {
            config: serde_json::json!({
                "model": "gpt-5.4",
                "effort": "xhigh",
            }),
        }
    );
}

#[test]
fn workflow_v1_codex_shape_is_exact() {
    for config in [
        "effort: high",
        "model: gpt-5.4",
        "model: \"\"\n        effort: high",
        "model: gpt-5.4\n        effort: \"\"",
        "model: gpt-5.4\n        effort: high\n        modelProvider: openai",
        "model: gpt-5.4\n        effort: high\n        apiKey: forbidden",
    ] {
        let source = format!(
            "schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: codex\n      config:\n        {config}\nsteps:\n  command:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
        );

        assert_eq!(
            decode(source.as_bytes()).unwrap_err().kind(),
            DecodeFailureKind::StructuralContract
        );
    }
}

#[test]
fn finalizers_decode_with_independent_order_defaults_and_engine_context() {
    let source = br#"schemaVersion: 1
agentProfiles:
  reporting:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  zStep:
    kind: cmd
    command:
      argv: ["true"]
  aStep:
    kind: cmd
    command:
      argv: ["true"]
finalizers:
  zCleanup:
    kind: cmd
    inputs:
      context:
        ref: finalization.context
    command:
      argv: ["true"]
  aReport:
    kind: agent
    failurePolicy: advisory
    after: [zCleanup]
    when: [failed]
    agent:
      profile: reporting
      systemPrompt: system.md
      message:
        text:
          - file: message.md
        attachments:
          - ref: finalization.context
"#;

    let workflow = decode(source).unwrap();
    assert_eq!(workflow.step_order, ["zStep", "aStep"]);
    assert_eq!(workflow.finalizer_order, ["zCleanup", "aReport"]);
    assert_eq!(
        workflow.finalizers["zCleanup"].when,
        FinalizationTrigger::all()
    );
    assert_eq!(workflow.finalizers["aReport"].after, ["zCleanup"]);
    assert_eq!(
        workflow.finalizers["aReport"].when,
        [FinalizationTrigger::Failed].into_iter().collect()
    );

    let NodeBody::Command(cleanup) = &workflow.finalizers["zCleanup"].body else {
        panic!("cleanup must be a command finalizer");
    };
    assert_eq!(
        cleanup.inputs["context"],
        ValueReference::FinalizationContext
    );
    assert_eq!(cleanup.common.failure_policy, FailurePolicy::Required);

    let NodeBody::Agent(report) = &workflow.finalizers["aReport"].body else {
        panic!("report must be an agent finalizer");
    };
    assert_eq!(report.common.failure_policy, FailurePolicy::Advisory);
    assert_eq!(
        report.agent.message.attachments,
        [MessageSource::Reference(
            ValueReference::FinalizationContext
        )]
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
fn finalization_context_is_structurally_restricted_to_finalizer_inputs_and_attachments() {
    let agent_prefix = "schemaVersion: 1
agentProfiles:
  reporting:
    harness:
      kind: pi
      config: { model: openai/gpt-5, thinking: high }
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
";
    let invalid = [
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    inputs: { context: { ref: finalization.context } }\n    command: { argv: [\"true\"] }\n".to_owned(),
        format!(
            "{agent_prefix}  observer:\n    kind: agent\n    agent:\n      profile: reporting\n      systemPrompt: system.md\n      message:\n        text: [{{ file: message.md }}]\n        attachments: [{{ ref: finalization.context }}]\n"
        ),
        format!(
            "{agent_prefix}finalizers:\n  report:\n    kind: agent\n    agent:\n      profile: reporting\n      systemPrompt: system.md\n      message:\n        text: [{{ ref: finalization.context }}]\n"
        ),
        format!(
            "{agent_prefix}exports:\n  context: {{ ref: finalization.context }}\n"
        ),
    ];

    for source in invalid {
        assert_eq!(
            decode(source.as_bytes()).unwrap_err().kind(),
            DecodeFailureKind::StructuralContract
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
            "duplicate step ID",
            "schemaVersion: 1\nsteps:\n  work: {kind: cmd, command: {argv: [true]}}\n  work: {kind: cmd, command: {argv: [true]}}\n",
        ),
        (
            "duplicate finalizer ID",
            "schemaVersion: 1\nsteps: {work: {kind: cmd, command: {argv: [true]}}}\nfinalizers:\n  cleanup: {kind: cmd, command: {argv: [true]}}\n  cleanup: {kind: cmd, command: {argv: [true]}}\n",
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
    let NodeBody::Command(prepare) = &workflow.steps["prepare"].body else {
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
    let NodeBody::Command(prepare) = &workflow.steps["prepare"].body else {
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
