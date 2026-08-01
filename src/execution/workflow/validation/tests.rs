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
    consumer.common.dependencies.push("producer".to_owned());
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
    assert_eq!(join.common.dependencies, ["left", "right"]);
    for (dependency, dependent) in [
        ("root", "left"),
        ("root", "right"),
        ("left", "join"),
        ("right", "join"),
    ] {
        let dependency_position = workflow
            .topological_order
            .iter()
            .position(|step| step == dependency)
            .unwrap();
        let dependent_position = workflow
            .topological_order
            .iter()
            .position(|step| step == dependent)
            .unwrap();
        assert!(dependency_position < dependent_position);
    }
    assert!(workflow.topological_order.contains(&"isolated".to_owned()));
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
fn output_bindings_require_existing_reachable_producers() {
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
    dependsOn: [producer]
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

    let unreachable = "schemaVersion: 1
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
  other:
    kind: cmd
    dependsOn: [producer]
    command:
      argv: [\"true\"]
  consumer:
    kind: cmd
    inputs:
      value:
        ref: outputs.producer.value
    command:
      argv: [\"true\"]
";
    assert_failure(
        unreachable,
        ValidationFailureKind::OutputProducerNotDependency,
        ValidationLocation::StepInput {
            step: "consumer".to_owned(),
            input: "value".to_owned(),
        },
    );

    let transitive = unreachable.replace(
        "  consumer:\n    kind: cmd\n    inputs:",
        "  consumer:\n    kind: cmd\n    dependsOn: [other]\n    inputs:",
    );
    let workflow = validate_yaml(&transitive).unwrap();
    let ValidatedStep::Command(consumer) = &workflow.steps["consumer"] else {
        panic!("consumer must be a command step");
    };
    assert_eq!(consumer.inputs["value"].value_type, WorkflowValueType::File);
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
fn validated_definition_preserves_only_explicit_direct_message_references() {
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
        .topological_order
        .iter()
        .position(|step| step == "consumer")
        .unwrap();
    for producer in ["responseProducer", "resultProducer"] {
        let producer_position = workflow
            .topological_order
            .iter()
            .position(|step| step == producer)
            .unwrap();
        assert!(producer_position < consumer_position);
    }
    let ValidatedStep::Agent(consumer) = &workflow.steps["consumer"] else {
        panic!("consumer must be an agent step");
    };
    assert_eq!(
        consumer.common.dependencies,
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

    let unreachable = typed_message_workflow("outputs.responseProducer.response", "text")
        .replace("    dependsOn: [responseProducer, resultProducer]\n", "");
    assert_failure(
        &unreachable,
        ValidationFailureKind::OutputProducerNotDependency,
        ValidationLocation::MessageText {
            step: "consumer".to_owned(),
            index: 0,
        },
    );
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
