use super::super::decode;
use super::super::document::{HarnessDefinition, Output, Step, ValueReference};
use super::super::pi::{PiConfig, Thinking};
use super::super::validated::{
    ResolvedOutputSource, ResolvedValueSource, ValidatedHarness, ValidatedMessageSource,
    ValidatedStep, ValidatedWorkflow, WorkflowImport, WorkflowValueType,
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
    let Step::Command(consumer) = duplicate.steps.get_mut("consumer").unwrap() else {
        panic!("consumer must be a command step");
    };
    consumer
        .common
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
    assert_eq!(join.common.prerequisites, ["left", "right"]);
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
        path: alpha.txt
        mediaType: text/plain
  beta:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
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
        consumer.common.prerequisites,
        ["alpha", "beta", "middle", "zeta"]
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
    assert_eq!(consumer.common.prerequisites, ["middle", "producer"]);
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
        kind: agent_response
      file:
        kind: file
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
        kind: agent_result
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
        kind: agent_response
      file:
        kind: file
        path: artifact.bin
        mediaType: application/octet-stream
      ignored:
        kind: file
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
        kind: agent_result
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
        consumer.common.prerequisites,
        ["responseProducer", "resultProducer"]
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
                    step: "responseProducer".to_owned(),
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
                    step: "resultProducer".to_owned(),
                    output: "result".to_owned(),
                    value_type: WorkflowValueType::Json,
                }),
                value_type: WorkflowValueType::Json,
            },
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Output(ResolvedOutputSource {
                    step: "responseProducer".to_owned(),
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
        assert_eq!(workflow.exports[name].step, step);
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
    assert_eq!(consumer.common.prerequisites, ["responseProducer"]);
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
            "schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: pi\n      config:\n        model: openai/gpt-5\n        thinking: high\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      changes:\n        kind: git_branch\n  consume:\n    kind: agent\n    agent:\n      profile: coding\n      systemPrompt: system.md\n      message:\n{message}\n"
        );
        assert_failure(
            &agent,
            ValidationFailureKind::TerminalOutputReference,
            location,
        );
    }
}

#[test]
fn output_kind_and_agent_cardinality_rules_are_enforced() {
    let mut command =
        decode(b"schemaVersion: 1\nsteps: {command: {kind: cmd, command: {argv: [\"true\"]}}}\n")
            .unwrap();
    let Step::Command(command_step) = command.steps.get_mut("command").unwrap() else {
        panic!("command must be a command step");
    };
    command_step
        .common
        .outputs
        .insert("response".to_owned(), Output::AgentResponse);
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
        kind: agent_response
      second:
        kind: agent_response
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
        "kind: agent_response",
        "kind: agent_result\n        schema: schemas/result.json",
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
        "kind: agent_response",
        "kind: agent_result\n        schema: schemas/result.json",
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
    let Step::Command(command) = document.steps.get_mut("command").unwrap() else {
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
