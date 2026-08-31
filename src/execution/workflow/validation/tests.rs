use super::super::claude_code::{ClaudeCodeConfig, ClaudeCodeEffort};
use super::super::decode;
use super::super::document::{
    FailurePolicy, FinalizationTrigger, HarnessDefinition, NodeBody, Output, ValueReference,
};
use super::super::pi::{PiConfig, Thinking};
use super::super::validated::{
    ResolvedDirectPrerequisite, ResolvedOutputSource, ResolvedValueSource, ValidatedHarness,
    ValidatedMessageSource, ValidatedStep, ValidatedWorkflow, WorkflowImport, WorkflowNode,
    WorkflowNodeRole, WorkflowValueType,
};
use super::{ValidationFailureKind, ValidationLocation};

fn validate_yaml(source: &str) -> Result<ValidatedWorkflow, super::ValidationFailure> {
    super::validate(decode(source.as_bytes()).unwrap())
}

fn assert_failure(source: &str, kind: ValidationFailureKind, location: ValidationLocation) {
    let failure = validate_yaml(source).unwrap_err();
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.location(), &location);
}

fn command_step(name: &str, dependencies: &str) -> String {
    format!("  {name}:\n    kind: cmd\n{dependencies}    command:\n      argv: [\"true\"]\n")
}

fn prerequisite_shape(prerequisites: &[ResolvedDirectPrerequisite]) -> Vec<(&str, bool, bool)> {
    prerequisites
        .iter()
        .map(|prerequisite| {
            (
                prerequisite.producer.as_str(),
                prerequisite.control,
                prerequisite.data,
            )
        })
        .collect()
}

#[test]
fn failure_policy_defaults_and_typed_data_compatibility_are_resolved() {
    let source = "schemaVersion: 1
steps:
  analyze:
    kind: cmd
    failurePolicy: advisory
    command:
      argv: [\"true\"]
    outputs:
      report:
        kind: file
        from: path
        path: report.json
        mediaType: application/json
  summarize:
    kind: cmd
    failurePolicy: advisory
    dependsOn: [analyze]
    inputs:
      report:
        ref: outputs.analyze.report
    command:
      argv: [\"true\"]
  required:
    kind: cmd
    dependsOn: [analyze]
    command:
      argv: [\"true\"]
";
    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Command(analyze) = &workflow.steps["analyze"] else {
        panic!("analyze must be a command step");
    };
    let ValidatedStep::Command(summarize) = &workflow.steps["summarize"] else {
        panic!("summarize must be a command step");
    };
    let ValidatedStep::Command(required) = &workflow.steps["required"] else {
        panic!("required must be a command step");
    };
    assert_eq!(analyze.common.failure_policy, FailurePolicy::Advisory);
    assert_eq!(summarize.common.failure_policy, FailurePolicy::Advisory);
    assert_eq!(required.common.failure_policy, FailurePolicy::Required);
    assert_eq!(
        prerequisite_shape(&summarize.common.prerequisites),
        [("analyze", true, true)]
    );
    assert_eq!(
        prerequisite_shape(&required.common.prerequisites),
        [("analyze", true, false)]
    );

    let required_consumer = source.replace(
        "  summarize:\n    kind: cmd\n    failurePolicy: advisory\n",
        "  summarize:\n    kind: cmd\n",
    );
    assert_failure(
        &required_consumer,
        ValidationFailureKind::AdvisoryDataDependency,
        ValidationLocation::StepInput {
            step: "summarize".to_owned(),
            input: "report".to_owned(),
        },
    );
}

#[test]
fn advisory_outputs_cannot_be_exported() {
    let source = "schemaVersion: 1
steps:
  analyze:
    kind: cmd
    failurePolicy: advisory
    command:
      argv: [\"true\"]
    outputs:
      report:
        kind: file
        from: path
        path: report.json
        mediaType: application/json
exports:
  report:
    ref: outputs.analyze.report
";
    assert_failure(
        source,
        ValidationFailureKind::AdvisoryExportTarget,
        ValidationLocation::Export {
            name: "report".to_owned(),
        },
    );
}

#[test]
fn dependency_graph_failures_are_classified_at_owned_locations() {
    let missing = format!(
        "schemaVersion: 1\nsteps:\n{}",
        command_step("consumer", "    dependsOn: [missing]\n")
    );
    assert_failure(
        &missing,
        ValidationFailureKind::MissingDependency,
        ValidationLocation::StepDependency {
            step: "consumer".to_owned(),
            index: 0,
        },
    );

    let self_dependency = format!(
        "schemaVersion: 1\nsteps:\n{}",
        command_step("consumer", "    dependsOn: [consumer]\n")
    );
    assert_failure(
        &self_dependency,
        ValidationFailureKind::SelfDependency,
        ValidationLocation::StepDependency {
            step: "consumer".to_owned(),
            index: 0,
        },
    );

    let mut duplicate = decode(
        format!(
            "schemaVersion: 1\nsteps:\n{}{}",
            command_step("producer", ""),
            command_step("consumer", "    dependsOn: [producer]\n")
        )
        .as_bytes(),
    )
    .unwrap();
    duplicate
        .steps
        .get_mut("consumer")
        .unwrap()
        .control_dependencies
        .push("producer".to_owned());
    let duplicate_failure = super::validate(duplicate).unwrap_err();
    assert_eq!(
        duplicate_failure.kind(),
        ValidationFailureKind::DuplicateDependency
    );
    assert_eq!(
        duplicate_failure.location(),
        &ValidationLocation::StepDependency {
            step: "consumer".to_owned(),
            index: 1,
        }
    );

    let cycle = format!(
        "schemaVersion: 1\nsteps:\n{}{}{}",
        command_step("one", "    dependsOn: [three]\n"),
        command_step("two", "    dependsOn: [one]\n"),
        command_step("three", "    dependsOn: [two]\n")
    );
    assert_failure(
        &cycle,
        ValidationFailureKind::DependencyCycle,
        ValidationLocation::WorkflowGraph,
    );

    let data_cycle = "schemaVersion: 1
steps:
  one:
    kind: cmd
    inputs:
      value:
        ref: outputs.two.value
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: one.txt
        mediaType: text/plain
  two:
    kind: cmd
    inputs:
      value:
        ref: outputs.one.value
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: two.txt
        mediaType: text/plain
";
    assert_failure(
        data_cycle,
        ValidationFailureKind::DependencyCycle,
        ValidationLocation::WorkflowGraph,
    );

    let mixed_cycle = data_cycle.replace(
        "    inputs:\n      value:\n        ref: outputs.two.value\n",
        "    dependsOn: [two]\n",
    );
    assert_failure(
        &mixed_cycle,
        ValidationFailureKind::DependencyCycle,
        ValidationLocation::WorkflowGraph,
    );
}

#[test]
fn branching_and_disconnected_dag_retains_every_edge_and_step() {
    let source = format!(
        "schemaVersion: 1\nsteps:\n{}{}{}{}{}",
        command_step("root", ""),
        command_step("left", "    dependsOn: [root]\n"),
        command_step("right", "    dependsOn: [root]\n"),
        command_step("join", "    dependsOn: [left, right]\n"),
        command_step("isolated", ""),
    );
    let workflow = validate_yaml(&source).unwrap();

    assert_eq!(workflow.steps.len(), 5);
    let ValidatedStep::Command(join) = &workflow.steps["join"] else {
        panic!("join must be a command step");
    };
    assert_eq!(
        prerequisite_shape(&join.common.prerequisites),
        [("left", true, false), ("right", true, false)]
    );
    for (dependency, dependent) in [
        ("root", "left"),
        ("root", "right"),
        ("left", "join"),
        ("right", "join"),
    ] {
        let dependency_position = workflow
            .presentation_order
            .iter()
            .position(|step| step == dependency)
            .unwrap();
        let dependent_position = workflow
            .presentation_order
            .iter()
            .position(|step| step == dependent)
            .unwrap();
        assert!(dependency_position < dependent_position);
    }
    assert!(workflow.presentation_order.contains(&"isolated".to_owned()));
}

#[test]
fn presentation_order_uses_source_index_for_each_kahn_ready_set() {
    let source = "schemaVersion: 1
steps:
  z:
    kind: cmd
    command:
      argv: [\"true\"]
  a:
    kind: cmd
    dependsOn: [z]
    command:
      argv: [\"true\"]
  m:
    kind: cmd
    command:
      argv: [\"true\"]
";

    let workflow = validate_yaml(source).unwrap();

    assert_eq!(workflow.source_order, ["z", "a", "m"]);
    assert_eq!(workflow.presentation_order, ["z", "a", "m"]);
}

#[test]
fn agent_profiles_resolve_to_pinned_configs_and_missing_references_fail() {
    let source = "schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: xhigh
  unused:
    harness:
      kind: pi
      config:
        model: openai/gpt-4.1
        thinking: low
steps:
  agent:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
";
    let workflow = validate_yaml(source).unwrap();
    assert!(!workflow.required_imports.prompt);
    let ValidatedStep::Agent(agent) = &workflow.steps["agent"] else {
        panic!("agent must be an agent step");
    };
    assert_eq!(agent.agent.profile, "coding");
    assert_eq!(
        agent.agent.harness,
        ValidatedHarness::Pi(PiConfig {
            model: "openai/gpt-5".to_owned(),
            thinking: Thinking::XHigh,
        })
    );

    assert_failure(
        &source.replace("profile: coding", "profile: missing"),
        ValidationFailureKind::UnknownAgentProfile,
        ValidationLocation::AgentProfileReference {
            step: "agent".to_owned(),
        },
    );

    let mut invalid_unused = decode(source.as_bytes()).unwrap();
    let unused = invalid_unused.agent_profiles.get_mut("unused").unwrap();
    unused.harness = HarnessDefinition::Pi {
        config: serde_json::json!({
            "model": "",
            "thinking": "low",
        }),
    };
    let failure = super::validate(invalid_unused).unwrap_err();
    assert_eq!(
        failure.kind(),
        ValidationFailureKind::InvalidAgentProfileConfig
    );
    assert_eq!(
        failure.location(),
        &ValidationLocation::AgentProfile {
            profile: "unused".to_owned(),
        }
    );
}

#[test]
fn claude_code_profiles_resolve_without_installation_or_model_lookup() {
    let source = "schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: claude_code
      config:
        model: future-claude-model
        effort: xhigh
steps:
  agent:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
";

    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Agent(agent) = &workflow.steps["agent"] else {
        panic!("agent must be an agent step");
    };
    assert_eq!(
        agent.agent.harness,
        ValidatedHarness::ClaudeCode(ClaudeCodeConfig {
            model: "future-claude-model".to_owned(),
            effort: ClaudeCodeEffort::XHigh,
        })
    );
}

#[test]
fn codex_profiles_resolve_exact_nonempty_native_strings_without_lookup() {
    let source = "schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: codex
      config:
        model: future-codex-model
        effort: future-native-effort
steps:
  agent:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
";

    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Agent(agent) = &workflow.steps["agent"] else {
        panic!("agent must be an agent step");
    };
    assert_eq!(
        agent.agent.harness,
        ValidatedHarness::Codex(crate::execution::workflow::codex::CodexConfig {
            model: "future-codex-model".to_owned(),
            effort: "future-native-effort".to_owned(),
        })
    );
}

#[test]
fn output_bindings_require_declared_non_self_outputs() {
    let missing_step = "schemaVersion: 1
steps:
  consumer:
    kind: cmd
    inputs:
      value:
        ref: outputs.missing.value
    command:
      argv: [\"true\"]
";
    assert_failure(
        missing_step,
        ValidationFailureKind::UnknownOutputStep,
        ValidationLocation::StepInput {
            step: "consumer".to_owned(),
            input: "value".to_owned(),
        },
    );

    let missing_output = "schemaVersion: 1
steps:
  producer:
    kind: cmd
    command:
      argv: [\"true\"]
  consumer:
    kind: cmd
    inputs:
      value:
        ref: outputs.producer.missing
    command:
      argv: [\"true\"]
";
    assert_failure(
        missing_output,
        ValidationFailureKind::UnknownOutput,
        ValidationLocation::StepInput {
            step: "consumer".to_owned(),
            input: "value".to_owned(),
        },
    );

    let self_reference = "schemaVersion: 1
steps:
  consumer:
    kind: cmd
    inputs:
      value:
        ref: outputs.consumer.value
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: value.txt
        mediaType: text/plain
";
    assert_failure(
        self_reference,
        ValidationFailureKind::SelfDependency,
        ValidationLocation::StepInput {
            step: "consumer".to_owned(),
            input: "value".to_owned(),
        },
    );
}

#[test]
fn command_output_references_form_normalized_direct_prerequisites() {
    let source = "schemaVersion: 1
steps:
  alpha:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: alpha.txt
        mediaType: text/plain
  beta:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: beta.txt
        mediaType: text/plain
  middle:
    kind: cmd
    dependsOn: [alpha]
    command:
      argv: [\"true\"]
  zeta:
    kind: cmd
    command:
      argv: [\"true\"]
  consumer:
    kind: cmd
    dependsOn: [zeta, middle, alpha]
    inputs:
      second:
        ref: outputs.beta.value
      first:
        ref: outputs.alpha.value
    command:
      argv: [\"true\"]
";

    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Command(consumer) = &workflow.steps["consumer"] else {
        panic!("consumer must be a command step");
    };
    assert_eq!(
        prerequisite_shape(&consumer.common.prerequisites),
        [
            ("alpha", true, true),
            ("beta", false, true),
            ("middle", true, false),
            ("zeta", true, false),
        ]
    );
    assert_eq!(consumer.inputs.len(), 2);
    assert_eq!(consumer.inputs["first"].value_type, WorkflowValueType::File);
    assert_eq!(
        workflow.presentation_order,
        ["alpha", "beta", "middle", "zeta", "consumer"]
    );
}

#[test]
fn transitive_output_reference_remains_a_direct_prerequisite() {
    let source = "schemaVersion: 1
steps:
  producer:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: value.txt
        mediaType: text/plain
  middle:
    kind: cmd
    dependsOn: [producer]
    command:
      argv: [\"true\"]
  consumer:
    kind: cmd
    dependsOn: [middle]
    inputs:
      value:
        ref: outputs.producer.value
    command:
      argv: [\"true\"]
";

    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Command(consumer) = &workflow.steps["consumer"] else {
        panic!("consumer must be a command step");
    };
    assert_eq!(
        prerequisite_shape(&consumer.common.prerequisites),
        [("middle", true, false), ("producer", false, true)]
    );
    assert_eq!(
        workflow.presentation_order,
        ["producer", "middle", "consumer"]
    );
}

#[test]
fn imports_and_exports_do_not_create_step_prerequisites() {
    let source = "schemaVersion: 1
steps:
  consumer:
    kind: cmd
    inputs:
      prompt:
        ref: imports.prompt
    command:
      argv: [\"true\"]
  producer:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: value.txt
        mediaType: text/plain
exports:
  value:
    ref: outputs.producer.value
";

    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Command(consumer) = &workflow.steps["consumer"] else {
        panic!("consumer must be a command step");
    };
    assert!(consumer.common.prerequisites.is_empty());
    assert!(workflow.required_imports.prompt);
    assert_eq!(workflow.presentation_order, ["consumer", "producer"]);
}

#[test]
fn closed_import_namespace_rejects_unknown_imports() {
    let source = "schemaVersion: 1
steps:
  consumer:
    kind: cmd
    inputs:
      value:
        ref: imports.unknown
    command:
      argv: [\"true\"]
";
    assert_failure(
        source,
        ValidationFailureKind::UnknownImport,
        ValidationLocation::StepInput {
            step: "consumer".to_owned(),
            input: "value".to_owned(),
        },
    );
}

fn typed_message_workflow(reference: &str, destination: &str) -> String {
    let message = match destination {
        "text" => format!("        text:\n          - ref: {reference}\n"),
        "attachment" => format!(
            "        text:\n          - file: prompts/message.md\n        attachments:\n          - ref: {reference}\n"
        ),
        _ => panic!("unknown test destination"),
    };
    format!(
        "schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  responseProducer:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
    outputs:
      response:
        kind: text
        from: agent_response
      file:
        kind: file
        from: path
        path: artifact.bin
        mediaType: application/octet-stream
  resultProducer:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
    outputs:
      result:
        kind: json
        from: agent_result
        schema: schemas/result.json
  consumer:
    kind: agent
    dependsOn: [responseProducer, resultProducer]
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
{message}"
    )
}

#[test]
fn message_type_table_rejects_every_inverse_destination_without_conversion() {
    let cases = [
        ("imports.prompt", "attachment"),
        ("imports.attachments", "text"),
        ("outputs.responseProducer.response", "attachment"),
        ("outputs.resultProducer.result", "text"),
        ("outputs.responseProducer.file", "text"),
    ];

    for (reference, destination) in cases {
        let failure = validate_yaml(&typed_message_workflow(reference, destination)).unwrap_err();
        assert_eq!(
            failure.kind(),
            ValidationFailureKind::MessageTypeMismatch,
            "unexpected classification for {reference} in {destination}"
        );
        let expected_location = match destination {
            "text" => ValidationLocation::MessageText {
                step: "consumer".to_owned(),
                index: 0,
            },
            "attachment" => ValidationLocation::MessageAttachment {
                step: "consumer".to_owned(),
                index: 0,
            },
            _ => panic!("unknown test destination"),
        };
        assert_eq!(failure.location(), &expected_location);
    }
}

#[test]
fn validated_definition_preserves_explicit_consumption_and_effective_prerequisites() {
    let source = "schemaVersion: 1
description: Typed workflow.
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  responseProducer:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
    outputs:
      response:
        kind: text
        from: agent_response
      file:
        kind: file
        from: path
        path: artifact.bin
        mediaType: application/octet-stream
      ignored:
        kind: file
        from: path
        path: ignored.bin
        mediaType: application/octet-stream
  resultProducer:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
    outputs:
      result:
        kind: json
        from: agent_result
        schema: schemas/result.json
  consumer:
    kind: agent
    dependsOn: [responseProducer, resultProducer]
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - ref: imports.prompt
          - ref: outputs.responseProducer.response
        attachments:
          - ref: imports.attachments
          - ref: outputs.resultProducer.result
          - ref: outputs.responseProducer.file
exports:
  response:
    ref: outputs.responseProducer.response
  result:
    ref: outputs.resultProducer.result
  file:
    ref: outputs.responseProducer.file
";
    let workflow = validate_yaml(source).unwrap();

    assert!(workflow.required_imports.prompt);
    let consumer_position = workflow
        .presentation_order
        .iter()
        .position(|step| step == "consumer")
        .unwrap();
    for producer in ["responseProducer", "resultProducer"] {
        let producer_position = workflow
            .presentation_order
            .iter()
            .position(|step| step == producer)
            .unwrap();
        assert!(producer_position < consumer_position);
    }
    let ValidatedStep::Agent(consumer) = &workflow.steps["consumer"] else {
        panic!("consumer must be an agent step");
    };
    assert_eq!(
        prerequisite_shape(&consumer.common.prerequisites),
        [
            ("responseProducer", true, true),
            ("resultProducer", true, true),
        ]
    );
    assert_eq!(
        consumer.agent.message.text,
        [
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Import(WorkflowImport::Prompt),
                value_type: WorkflowValueType::Text,
            },
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Output(ResolvedOutputSource {
                    node: WorkflowNode {
                        id: "responseProducer".to_owned(),
                        role: WorkflowNodeRole::Step,
                    },
                    output: "response".to_owned(),
                    value_type: WorkflowValueType::Text,
                }),
                value_type: WorkflowValueType::Text,
            },
        ]
    );
    assert_eq!(
        consumer.agent.message.attachments,
        [
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Import(WorkflowImport::Attachments),
                value_type: WorkflowValueType::AttachmentCollection,
            },
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Output(ResolvedOutputSource {
                    node: WorkflowNode {
                        id: "resultProducer".to_owned(),
                        role: WorkflowNodeRole::Step,
                    },
                    output: "result".to_owned(),
                    value_type: WorkflowValueType::Json,
                }),
                value_type: WorkflowValueType::Json,
            },
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Output(ResolvedOutputSource {
                    node: WorkflowNode {
                        id: "responseProducer".to_owned(),
                        role: WorkflowNodeRole::Step,
                    },
                    output: "file".to_owned(),
                    value_type: WorkflowValueType::File,
                }),
                value_type: WorkflowValueType::File,
            },
        ]
    );

    let ValidatedStep::Agent(response_producer) = &workflow.steps["responseProducer"] else {
        panic!("response producer must be an agent step");
    };
    assert_eq!(
        response_producer.common.outputs["response"].value_type,
        WorkflowValueType::Text
    );
    assert_eq!(
        response_producer.common.outputs["file"].value_type,
        WorkflowValueType::File
    );
    let ValidatedStep::Agent(result_producer) = &workflow.steps["resultProducer"] else {
        panic!("result producer must be an agent step");
    };
    assert_eq!(
        result_producer.common.outputs["result"].value_type,
        WorkflowValueType::Json
    );
    assert_eq!(
        response_producer.common.outputs["ignored"].value_type,
        WorkflowValueType::File
    );
    for (name, step, expected_type) in [
        ("response", "responseProducer", WorkflowValueType::Text),
        ("result", "resultProducer", WorkflowValueType::Json),
        ("file", "responseProducer", WorkflowValueType::File),
    ] {
        assert_eq!(workflow.exports[name].node.id, step);
        assert_eq!(workflow.exports[name].node.role, WorkflowNodeRole::Step);
        assert_eq!(workflow.exports[name].output, name);
        assert_eq!(workflow.exports[name].value_type, expected_type);
    }
}

#[test]
fn direct_message_reference_failures_are_reported_at_the_message_location() {
    for (reference, kind) in [
        ("imports.unknown", ValidationFailureKind::UnknownImport),
        (
            "outputs.missing.value",
            ValidationFailureKind::UnknownOutputStep,
        ),
        (
            "outputs.responseProducer.missing",
            ValidationFailureKind::UnknownOutput,
        ),
    ] {
        let failure = validate_yaml(&typed_message_workflow(reference, "text")).unwrap_err();
        assert_eq!(failure.kind(), kind);
        assert_eq!(
            failure.location(),
            &ValidationLocation::MessageText {
                step: "consumer".to_owned(),
                index: 0,
            }
        );
    }

    let data_only = typed_message_workflow("outputs.responseProducer.response", "text")
        .replace("    dependsOn: [responseProducer, resultProducer]\n", "");
    let workflow = validate_yaml(&data_only).unwrap();
    let ValidatedStep::Agent(consumer) = &workflow.steps["consumer"] else {
        panic!("consumer must be an agent step");
    };
    assert_eq!(
        prerequisite_shape(&consumer.common.prerequisites),
        [("responseProducer", false, true)]
    );
}

#[test]
fn git_branch_is_exportable_but_rejected_from_every_downstream_binding() {
    let producer = "schemaVersion: 1
steps:
  produce:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      changes:
        kind: git_branch
        from: workspace
";
    let exported = format!("{producer}exports:\n  changes:\n    ref: outputs.produce.changes\n");
    let workflow = validate_yaml(&exported).unwrap();
    assert_eq!(
        workflow.exports["changes"].value_type,
        WorkflowValueType::GitBranch
    );

    let command = format!(
        "{producer}  consume:\n    kind: cmd\n    inputs:\n      changes:\n        ref: outputs.produce.changes\n    command:\n      argv: [\"true\"]\n"
    );
    assert_failure(
        &command,
        ValidationFailureKind::TerminalOutputReference,
        ValidationLocation::StepInput {
            step: "consume".to_owned(),
            input: "changes".to_owned(),
        },
    );

    for (message, location) in [
        (
            "        text: [{ ref: outputs.produce.changes }]",
            ValidationLocation::MessageText {
                step: "consume".to_owned(),
                index: 0,
            },
        ),
        (
            "        text: [{ file: system.md }]\n        attachments: [{ ref: outputs.produce.changes }]",
            ValidationLocation::MessageAttachment {
                step: "consume".to_owned(),
                index: 0,
            },
        ),
    ] {
        let agent = format!(
            "schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: pi\n      config:\n        model: openai/gpt-5\n        thinking: high\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      changes:\n        kind: git_branch\n        from: workspace\n  consume:\n    kind: agent\n    agent:\n      profile: coding\n      systemPrompt: system.md\n      message:\n{message}\n"
        );
        assert_failure(
            &agent,
            ValidationFailureKind::TerminalOutputReference,
            location,
        );
    }
}

#[test]
fn semantic_outputs_definition_rejects_duplicate_paths_and_excess_workspace_sources() {
    let duplicate_path = r#"schemaVersion: 1
steps:
  produce:
    kind: cmd
    command:
      argv: ["true"]
    outputs:
      first:
        kind: text
        from: path
        path: value
      second:
        kind: file
        from: path
        path: value
        mediaType: application/octet-stream
"#;
    assert_failure(
        duplicate_path,
        ValidationFailureKind::DuplicateOutputPath,
        ValidationLocation::StepOutput {
            step: "produce".to_owned(),
            output: "second".to_owned(),
        },
    );

    let excess_workspace = r#"schemaVersion: 1
steps:
  produce:
    kind: cmd
    command:
      argv: ["true"]
    outputs:
      first:
        kind: git_branch
        from: workspace
      second:
        kind: git_branch
        from: workspace
"#;
    assert_failure(
        excess_workspace,
        ValidationFailureKind::ExcessWorkspaceOutput,
        ValidationLocation::StepOutput {
            step: "produce".to_owned(),
            output: "second".to_owned(),
        },
    );

    let equal_paths_in_different_nodes = duplicate_path.replace(
        "      second:\n        kind: file\n        from: path\n        path: value\n        mediaType: application/octet-stream\n",
        "  consume:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      second:\n        kind: file\n        from: path\n        path: value\n        mediaType: application/octet-stream\n",
    );
    assert!(validate_yaml(&equal_paths_in_different_nodes).is_ok());
}

#[test]
fn output_kind_and_agent_cardinality_rules_are_enforced() {
    let mut command =
        decode(b"schemaVersion: 1\nsteps: {command: {kind: cmd, command: {argv: [\"true\"]}}}\n")
            .unwrap();
    let NodeBody::Command(command_step) = &mut command.steps.get_mut("command").unwrap().body
    else {
        panic!("command must be a command step");
    };
    command_step
        .common
        .outputs
        .insert("response".to_owned(), Output::TextAgentResponse);
    let failure = super::validate(command).unwrap_err();
    assert_eq!(failure.kind(), ValidationFailureKind::IllegalCommandOutput);
    assert_eq!(
        failure.location(),
        &ValidationLocation::StepOutput {
            step: "command".to_owned(),
            output: "response".to_owned(),
        }
    );

    let two_responses = "schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  agent:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
    outputs:
      first:
        kind: text
        from: agent_response
      second:
        kind: text
        from: agent_response
";
    assert_failure(
        two_responses,
        ValidationFailureKind::ExcessAgentResponseOutput,
        ValidationLocation::StepOutput {
            step: "agent".to_owned(),
            output: "second".to_owned(),
        },
    );

    let two_results = two_responses.replace(
        "kind: text\n        from: agent_response",
        "kind: json\n        from: agent_result\n        schema: schemas/result.json",
    );
    assert_failure(
        &two_results,
        ValidationFailureKind::ExcessAgentResultOutput,
        ValidationLocation::StepOutput {
            step: "agent".to_owned(),
            output: "second".to_owned(),
        },
    );

    let conflicting_values = two_responses.replacen(
        "kind: text\n        from: agent_response",
        "kind: json\n        from: agent_result\n        schema: schemas/result.json",
        1,
    );
    assert_failure(
        &conflicting_values,
        ValidationFailureKind::ConflictingAgentValueOutputs,
        ValidationLocation::StepOutput {
            step: "agent".to_owned(),
            output: "second".to_owned(),
        },
    );
}

#[test]
fn exports_must_target_declared_outputs() {
    let base = "schemaVersion: 1
steps:
  command:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      report:
        kind: file
        from: path
        path: report.txt
        mediaType: text/plain
exports:
  public:
    ref: TARGET
";
    for target in ["outputs.missing.report", "outputs.command.missing"] {
        assert_failure(
            &base.replace("TARGET", target),
            ValidationFailureKind::InvalidExportTarget,
            ValidationLocation::Export {
                name: "public".to_owned(),
            },
        );
    }
}

#[test]
fn structurally_decoded_references_are_not_reparsed_during_validation() {
    let mut document =
        decode(b"schemaVersion: 1\nsteps: {command: {kind: cmd, command: {argv: [\"true\"]}}}\n")
            .unwrap();
    let NodeBody::Command(command) = &mut document.steps.get_mut("command").unwrap().body else {
        panic!("command must be a command step");
    };
    command.inputs.insert(
        "prompt".to_owned(),
        ValueReference::Import {
            name: "prompt".to_owned(),
        },
    );

    let validated = super::validate(document).unwrap();
    let ValidatedStep::Command(command) = &validated.steps["command"] else {
        panic!("command must be a command step");
    };
    assert_eq!(
        command.inputs["prompt"].source,
        ResolvedValueSource::Import(WorkflowImport::Prompt)
    );
}

#[test]
fn combined_node_limit_and_shared_namespace_are_enforced_before_validation() {
    fn workflow_with_counts(step_count: usize, finalizer_count: usize) -> String {
        let mut source = String::from("schemaVersion: 1\nsteps:\n");
        for index in 0..step_count {
            source.push_str(&command_step(&format!("s{index}"), ""));
        }
        source.push_str("finalizers:\n");
        for index in 0..finalizer_count {
            source.push_str(&format!(
                "  f{index}:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
            ));
        }
        source
    }

    let accepted = validate_yaml(&workflow_with_counts(128, 128)).unwrap();
    assert_eq!(accepted.steps.len() + accepted.finalizers.len(), 256);

    let too_many = validate_yaml(&workflow_with_counts(128, 129)).unwrap_err();
    assert_eq!(too_many.kind(), ValidationFailureKind::TooManyNodes);
    assert_eq!(too_many.location(), &ValidationLocation::WorkflowNamespace);

    assert_failure(
        "schemaVersion: 1
steps:
  shared:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  shared:
    kind: cmd
    command: { argv: [\"true\"] }
",
        ValidationFailureKind::DuplicateNodeId,
        ValidationLocation::WorkflowNamespace,
    );

    let mut duplicate_step_order = decode(workflow_with_counts(2, 1).as_bytes()).unwrap();
    duplicate_step_order.step_order[1] = duplicate_step_order.step_order[0].clone();
    assert_eq!(
        super::validate(duplicate_step_order).unwrap_err().kind(),
        ValidationFailureKind::InvalidSourceOrder
    );

    let mut duplicate_finalizer_order = decode(workflow_with_counts(1, 2).as_bytes()).unwrap();
    duplicate_finalizer_order.finalizer_order[1] =
        duplicate_finalizer_order.finalizer_order[0].clone();
    assert_eq!(
        super::validate(duplicate_finalizer_order)
            .unwrap_err()
            .kind(),
        ValidationFailureKind::InvalidSourceOrder
    );
}

#[test]
fn semantic_outputs_ordinary_finalizer_parity() {
    let source = "schemaVersion: 1
steps:
  later:
    kind: cmd
    dependsOn: [producer]
    command: { argv: [\"true\"] }
  producer:
    kind: cmd
    command: { argv: [\"true\"] }
    outputs:
      value:
        kind: file
        from: path
        path: value.json
        mediaType: application/json
finalizers:
  report:
    kind: cmd
    after: [cleanup]
    when: [succeeded]
    inputs:
      result:
        ref: outputs.cleanup.result
      context:
        ref: finalization.context
    command: { argv: [\"true\"] }
  cleanup:
    kind: cmd
    inputs:
      value:
        ref: outputs.producer.value
    command: { argv: [\"true\"] }
    outputs:
      result:
        kind: file
        from: path
        path: result.json
        mediaType: application/json
exports:
  report:
    ref: outputs.report.result
";
    let source = source.replace(
        "    command: { argv: [\"true\"] }\n  cleanup:",
        "    command: { argv: [\"true\"] }\n    outputs:\n      result:\n        kind: file\n        from: path\n        path: report.json\n        mediaType: application/json\n  cleanup:",
    );
    let workflow = validate_yaml(&source).unwrap();

    assert_eq!(workflow.source_order, ["later", "producer"]);
    assert_eq!(workflow.presentation_order, ["producer", "later"]);
    assert_eq!(workflow.finalizer_source_order, ["report", "cleanup"]);
    assert_eq!(workflow.finalizer_presentation_order, ["cleanup", "report"]);
    let report = &workflow.finalizers["report"];
    assert_eq!(
        report.when,
        [FinalizationTrigger::Succeeded].into_iter().collect()
    );
    let ValidatedStep::Command(report) = &report.body else {
        panic!("report must be a command finalizer");
    };
    assert_eq!(
        prerequisite_shape(&report.common.prerequisites),
        [("cleanup", true, true)]
    );
    assert_eq!(
        report.inputs["context"],
        super::super::validated::ResolvedValueReference {
            source: ResolvedValueSource::FinalizationContext,
            value_type: WorkflowValueType::Json,
        }
    );
    let ValidatedStep::Command(cleanup) = &workflow.finalizers["cleanup"].body else {
        panic!("cleanup must be a command finalizer");
    };
    assert!(cleanup.common.prerequisites.is_empty());
    assert_eq!(
        workflow.exports["report"].node.role,
        WorkflowNodeRole::Finalizer
    );
    assert_eq!(workflow.exports["report"].node.id, "report");
}

#[test]
fn finalizer_graph_rejects_cross_phase_edges_self_edges_duplicates_and_cycles() {
    let after_step = "schemaVersion: 1
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  cleanup:
    kind: cmd
    after: [work]
    command: { argv: [\"true\"] }
";
    assert_failure(
        after_step,
        ValidationFailureKind::InvalidFinalizerAfterTarget,
        ValidationLocation::FinalizerAfter {
            finalizer: "cleanup".to_owned(),
            index: 0,
        },
    );

    let self_edge = after_step.replace("after: [work]", "after: [cleanup]");
    assert_failure(
        &self_edge,
        ValidationFailureKind::SelfDependency,
        ValidationLocation::FinalizerAfter {
            finalizer: "cleanup".to_owned(),
            index: 0,
        },
    );

    let mut duplicate = decode(
        after_step
            .replace("after: [work]", "")
            .replace(
                "  cleanup:\n",
                "  before:\n    kind: cmd\n    command: { argv: [\"true\"] }\n  cleanup:\n",
            )
            .as_bytes(),
    )
    .unwrap();
    duplicate.finalizers.get_mut("cleanup").unwrap().after =
        vec!["before".to_owned(), "before".to_owned()];
    assert_eq!(
        super::validate(duplicate).unwrap_err().kind(),
        ValidationFailureKind::DuplicateDependency
    );

    let control_cycle = "schemaVersion: 1
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  one:
    kind: cmd
    after: [two]
    command: { argv: [\"true\"] }
  two:
    kind: cmd
    after: [one]
    command: { argv: [\"true\"] }
";
    assert_failure(
        control_cycle,
        ValidationFailureKind::DependencyCycle,
        ValidationLocation::WorkflowGraph,
    );

    let data_cycle = "schemaVersion: 1
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  one:
    kind: cmd
    inputs: { value: { ref: outputs.two.value } }
    command: { argv: [\"true\"] }
    outputs:
      value: { kind: file, from: path, path: one.txt, mediaType: text/plain }
  two:
    kind: cmd
    inputs: { value: { ref: outputs.one.value } }
    command: { argv: [\"true\"] }
    outputs:
      value: { kind: file, from: path, path: two.txt, mediaType: text/plain }
";
    assert_failure(
        data_cycle,
        ValidationFailureKind::DependencyCycle,
        ValidationLocation::WorkflowGraph,
    );
}

#[test]
fn output_references_obey_phase_trigger_and_failure_impact_rules() {
    let ordinary_reads_finalizer = "schemaVersion: 1
steps:
  work:
    kind: cmd
    inputs: { value: { ref: outputs.cleanup.value } }
    command: { argv: [\"true\"] }
finalizers:
  cleanup:
    kind: cmd
    command: { argv: [\"true\"] }
    outputs:
      value: { kind: file, from: path, path: value.txt, mediaType: text/plain }
";
    assert_failure(
        ordinary_reads_finalizer,
        ValidationFailureKind::CrossPhaseOutputReference,
        ValidationLocation::StepInput {
            step: "work".to_owned(),
            input: "value".to_owned(),
        },
    );

    let incompatible_trigger = "schemaVersion: 1
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  produce:
    kind: cmd
    when: [succeeded]
    command: { argv: [\"true\"] }
    outputs:
      value: { kind: file, from: path, path: value.txt, mediaType: text/plain }
  consume:
    kind: cmd
    inputs: { value: { ref: outputs.produce.value } }
    command: { argv: [\"true\"] }
";
    assert_failure(
        incompatible_trigger,
        ValidationFailureKind::IncompatibleFinalizerTriggers,
        ValidationLocation::FinalizerInput {
            finalizer: "consume".to_owned(),
            input: "value".to_owned(),
        },
    );

    let advisory_ordinary = "schemaVersion: 1
steps:
  produce:
    kind: cmd
    failurePolicy: advisory
    command: { argv: [\"true\"] }
    outputs:
      value: { kind: file, from: path, path: value.txt, mediaType: text/plain }
finalizers:
  consume:
    kind: cmd
    inputs: { value: { ref: outputs.produce.value } }
    command: { argv: [\"true\"] }
";
    assert_failure(
        advisory_ordinary,
        ValidationFailureKind::AdvisoryDataDependency,
        ValidationLocation::FinalizerInput {
            finalizer: "consume".to_owned(),
            input: "value".to_owned(),
        },
    );

    let advisory_finalizer = incompatible_trigger
        .replace("    when: [succeeded]\n", "    failurePolicy: advisory\n")
        .replace(
            "  consume:\n    kind: cmd\n",
            "  consume:\n    kind: cmd\n    when: [succeeded]\n",
        );
    assert_failure(
        &advisory_finalizer,
        ValidationFailureKind::AdvisoryDataDependency,
        ValidationLocation::FinalizerInput {
            finalizer: "consume".to_owned(),
            input: "value".to_owned(),
        },
    );

    let control_only = advisory_finalizer.replace(
        "    inputs: { value: { ref: outputs.produce.value } }\n",
        "    after: [produce]\n",
    );
    validate_yaml(&control_only).unwrap();
}

#[test]
fn finalizer_exports_require_required_success_eligibility() {
    let base = "schemaVersion: 1
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  report:
    kind: cmd
    POLICY
    when: [TRIGGER]
    command: { argv: [\"true\"] }
    outputs:
      value: { kind: file, from: path, path: value.txt, mediaType: text/plain }
exports:
  value: { ref: outputs.report.value }
";
    assert_failure(
        &base
            .replace("POLICY", "failurePolicy: advisory")
            .replace("TRIGGER", "succeeded"),
        ValidationFailureKind::AdvisoryExportTarget,
        ValidationLocation::Export {
            name: "value".to_owned(),
        },
    );
    assert_failure(
        &base
            .replace("POLICY", "failurePolicy: required")
            .replace("TRIGGER", "failed"),
        ValidationFailureKind::FinalizerExportTrigger,
        ValidationLocation::Export {
            name: "value".to_owned(),
        },
    );
    let accepted = base
        .replace("POLICY", "failurePolicy: required")
        .replace("TRIGGER", "succeeded");
    assert_eq!(
        validate_yaml(&accepted).unwrap().exports["value"].node.role,
        WorkflowNodeRole::Finalizer
    );
}

#[test]
fn finalization_context_is_json_and_only_valid_in_declared_finalizer_positions() {
    let source = "schemaVersion: 1
agentProfiles:
  reporting:
    harness:
      kind: pi
      config: { model: openai/gpt-5, thinking: high }
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  command:
    kind: cmd
    inputs: { context: { ref: finalization.context } }
    command: { argv: [\"true\"] }
  agent:
    kind: agent
    agent:
      profile: reporting
      systemPrompt: system.md
      message:
        text: [{ file: message.md }]
        attachments: [{ ref: finalization.context }]
";
    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Command(command) = &workflow.finalizers["command"].body else {
        panic!("command must remain a command finalizer");
    };
    assert_eq!(
        command.inputs["context"].value_type,
        WorkflowValueType::Json
    );
    assert_eq!(
        command.inputs["context"].source,
        ResolvedValueSource::FinalizationContext
    );
    let ValidatedStep::Agent(agent) = &workflow.finalizers["agent"].body else {
        panic!("agent must remain an agent finalizer");
    };
    assert_eq!(
        agent.agent.message.attachments[0],
        ValidatedMessageSource::Reference {
            source: ResolvedValueSource::FinalizationContext,
            value_type: WorkflowValueType::Json,
        }
    );

    let mut ordinary = decode(
        b"schemaVersion: 1\nsteps: { work: { kind: cmd, command: { argv: [\"true\"] } } }\n",
    )
    .unwrap();
    let NodeBody::Command(work) = &mut ordinary.steps.get_mut("work").unwrap().body else {
        panic!("work must be a command step");
    };
    work.inputs
        .insert("context".to_owned(), ValueReference::FinalizationContext);
    assert_eq!(
        super::validate(ordinary).unwrap_err().kind(),
        ValidationFailureKind::InvalidFinalizationContext
    );

    let mut finalizer_text = decode(source.as_bytes()).unwrap();
    let NodeBody::Agent(agent) = &mut finalizer_text.finalizers.get_mut("agent").unwrap().body
    else {
        panic!("agent must be an agent finalizer");
    };
    agent.agent.message.text[0] =
        super::super::document::MessageSource::Reference(ValueReference::FinalizationContext);
    assert_eq!(
        super::validate(finalizer_text).unwrap_err().kind(),
        ValidationFailureKind::InvalidFinalizationContext
    );
}

#[test]
fn workflow_node_evidence_definition_limit_applies_to_steps_and_finalizers() {
    for finalizer in [false, true] {
        for count in [1_024, 1_025] {
            let mut outputs = String::new();
            let mut inputs = String::new();
            for index in 0..count {
                let name = format!("value{index:04}");
                outputs.push_str(&format!(
                    "      {name}:\n        kind: text\n        from: path\n        path: {name}.txt\n"
                ));
                inputs.push_str(&format!(
                    "      {name}:\n        ref: outputs.producer.{name}\n"
                ));
            }
            let consumer = format!(
                "    kind: cmd\n    inputs:\n{inputs}    command:\n      argv: [\"true\"]\n"
            );
            let source = if finalizer {
                format!(
                    "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n{outputs}finalizers:\n  consume:\n    when: [succeeded]\n{consumer}"
                )
            } else {
                format!(
                    "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n{outputs}  consume:\n{consumer}"
                )
            };
            let resolved = validate_yaml(&source);
            if count == 1_024 {
                let workflow = resolved.unwrap();
                let step = if finalizer {
                    &workflow.finalizers["consume"].body
                } else {
                    &workflow.steps["consume"]
                };
                let common = match step {
                    ValidatedStep::Command(command) => &command.common,
                    ValidatedStep::Agent(agent) => &agent.common,
                };
                assert_eq!(common.evidence_prerequisites.len(), 1_024);
                assert!(common.evidence_prerequisites.iter().all(|descriptor| {
                    descriptor.kind() == super::super::evidence::PrerequisiteKind::Body
                }));
            } else {
                assert_eq!(
                    resolved.unwrap_err().kind(),
                    ValidationFailureKind::TooManyPrerequisites
                );
            }
        }
    }
}
