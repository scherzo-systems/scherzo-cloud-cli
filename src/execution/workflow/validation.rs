use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use super::document::{
    Agent, CommonStep, HarnessDefinition, MessageSource, Output, OutputReference, Step,
    ValueReference, WorkflowDocument,
};
use super::pi;
use super::validated::{
    RequiredImports, ResolvedOutputSource, ResolvedValueReference, ResolvedValueSource,
    ValidatedAgent, ValidatedAgentMessage, ValidatedAgentStep, ValidatedCommandStep,
    ValidatedCommonStep, ValidatedHarness, ValidatedMessageSource, ValidatedOutput, ValidatedStep,
    ValidatedWorkflow, WorkflowImport, WorkflowValueType,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ValidationFailureKind {
    MissingDependency,
    SelfDependency,
    DuplicateDependency,
    DependencyCycle,
    InvalidAgentProfileConfig,
    UnknownAgentProfile,
    UnknownImport,
    UnknownOutputStep,
    UnknownOutput,
    OutputProducerNotDependency,
    MessageTypeMismatch,
    IllegalCommandOutput,
    ExcessAgentResponseOutput,
    ExcessAgentResultOutput,
    ConflictingAgentValueOutputs,
    InvalidExportTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidationLocation {
    WorkflowGraph,
    AgentProfile { profile: String },
    AgentProfileReference { step: String },
    StepDependency { step: String, index: usize },
    StepInput { step: String, input: String },
    MessageText { step: String, index: usize },
    MessageAttachment { step: String, index: usize },
    StepOutput { step: String, output: String },
    Export { name: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidationFailure {
    kind: ValidationFailureKind,
    location: ValidationLocation,
}

impl ValidationFailure {
    pub(crate) fn kind(&self) -> ValidationFailureKind {
        self.kind
    }

    pub(crate) fn location(&self) -> &ValidationLocation {
        &self.location
    }

    fn new(kind: ValidationFailureKind, location: ValidationLocation) -> Self {
        Self { kind, location }
    }
}

impl fmt::Display for ValidationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "workflow semantic failure: {:?}", self.kind)
    }
}

impl std::error::Error for ValidationFailure {}

struct ValidatedGraph {
    topological_order: Vec<String>,
    ancestors: BTreeMap<String, BTreeSet<String>>,
}

pub(crate) fn validate(document: WorkflowDocument) -> Result<ValidatedWorkflow, ValidationFailure> {
    let agent_profiles = validate_agent_profiles(&document)?;
    let graph = validate_graph(&document)?;
    validate_output_rules(&document)?;

    let mut required_imports = RequiredImports::default();
    let mut steps = BTreeMap::new();
    for (step_name, step) in &document.steps {
        let validated = match step {
            Step::Command(command) => ValidatedStep::Command(ValidatedCommandStep {
                common: validate_common(&command.common),
                inputs: validate_command_inputs(
                    step_name,
                    &command.inputs,
                    &document,
                    &graph.ancestors,
                    &mut required_imports,
                )?,
                argv: command.argv.clone(),
            }),
            Step::Agent(agent) => {
                let common = validate_common(&agent.common);
                let harness = resolve_agent_profile(step_name, &agent.agent, &agent_profiles)?;
                let validated_agent = validate_agent(
                    step_name,
                    &agent.agent,
                    &document,
                    &graph.ancestors,
                    &mut required_imports,
                    harness,
                )?;
                ValidatedStep::Agent(ValidatedAgentStep {
                    common,
                    agent: validated_agent,
                })
            }
        };
        steps.insert(step_name.clone(), validated);
    }

    let exports = document
        .exports
        .iter()
        .map(|(name, reference)| {
            resolve_export(name, reference, &document).map(|source| (name.clone(), source))
        })
        .collect::<Result<_, _>>()?;

    Ok(ValidatedWorkflow {
        schema_version: document.schema_version,
        description: document.description,
        steps,
        exports,
        required_imports,
        topological_order: graph.topological_order,
    })
}

fn validate_graph(document: &WorkflowDocument) -> Result<ValidatedGraph, ValidationFailure> {
    let mut dependents = BTreeMap::<String, Vec<String>>::new();
    let mut remaining_dependencies = BTreeMap::<String, usize>::new();

    for (step_name, step) in &document.steps {
        let common = common_step(step);
        let mut direct_dependencies = BTreeSet::new();
        for (index, dependency) in common.dependencies.iter().enumerate() {
            let location = || ValidationLocation::StepDependency {
                step: step_name.clone(),
                index,
            };
            if dependency == step_name {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::SelfDependency,
                    location(),
                ));
            }
            if !direct_dependencies.insert(dependency) {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::DuplicateDependency,
                    location(),
                ));
            }
            if !document.steps.contains_key(dependency) {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::MissingDependency,
                    location(),
                ));
            }
            dependents
                .entry(dependency.clone())
                .or_default()
                .push(step_name.clone());
        }
        remaining_dependencies.insert(step_name.clone(), common.dependencies.len());
    }

    let mut ready = remaining_dependencies
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(name, _)| name.clone())
        .collect::<BTreeSet<_>>();
    let mut topological_order = Vec::with_capacity(document.steps.len());

    while let Some(step_name) = ready.pop_first() {
        topological_order.push(step_name.clone());
        if let Some(step_dependents) = dependents.get(&step_name) {
            for dependent in step_dependents {
                if let Some(count) = remaining_dependencies.get_mut(dependent) {
                    *count -= 1;
                    if *count == 0 {
                        ready.insert(dependent.clone());
                    }
                }
            }
        }
    }

    if topological_order.len() != document.steps.len() {
        return Err(ValidationFailure::new(
            ValidationFailureKind::DependencyCycle,
            ValidationLocation::WorkflowGraph,
        ));
    }

    let mut ancestors = BTreeMap::<String, BTreeSet<String>>::new();
    for step_name in &topological_order {
        let mut step_ancestors = BTreeSet::new();
        if let Some(step) = document.steps.get(step_name) {
            for dependency in &common_step(step).dependencies {
                step_ancestors.insert(dependency.clone());
                if let Some(dependency_ancestors) = ancestors.get(dependency) {
                    step_ancestors.extend(dependency_ancestors.iter().cloned());
                }
            }
        }
        ancestors.insert(step_name.clone(), step_ancestors);
    }

    Ok(ValidatedGraph {
        topological_order,
        ancestors,
    })
}

fn validate_output_rules(document: &WorkflowDocument) -> Result<(), ValidationFailure> {
    for (step_name, step) in &document.steps {
        match step {
            Step::Command(command) => {
                for (output_name, output) in &command.common.outputs {
                    if !matches!(output, Output::File { .. }) {
                        return Err(output_failure(
                            ValidationFailureKind::IllegalCommandOutput,
                            step_name,
                            output_name,
                        ));
                    }
                }
            }
            Step::Agent(agent) => {
                let mut response_count = 0;
                let mut result_count = 0;
                for (output_name, output) in &agent.common.outputs {
                    let failure_kind = match output {
                        Output::AgentResponse => {
                            response_count += 1;
                            if response_count > 1 {
                                Some(ValidationFailureKind::ExcessAgentResponseOutput)
                            } else {
                                (result_count > 0)
                                    .then_some(ValidationFailureKind::ConflictingAgentValueOutputs)
                            }
                        }
                        Output::AgentResult { .. } => {
                            result_count += 1;
                            if result_count > 1 {
                                Some(ValidationFailureKind::ExcessAgentResultOutput)
                            } else {
                                (response_count > 0)
                                    .then_some(ValidationFailureKind::ConflictingAgentValueOutputs)
                            }
                        }
                        Output::File { .. } => None,
                    };
                    if let Some(kind) = failure_kind {
                        return Err(output_failure(kind, step_name, output_name));
                    }
                }
            }
        }
    }
    Ok(())
}

fn output_failure(kind: ValidationFailureKind, step: &str, output: &str) -> ValidationFailure {
    ValidationFailure::new(
        kind,
        ValidationLocation::StepOutput {
            step: step.to_owned(),
            output: output.to_owned(),
        },
    )
}

fn validate_common(common: &CommonStep) -> ValidatedCommonStep {
    let outputs = common
        .outputs
        .iter()
        .map(|(name, output)| {
            (
                name.clone(),
                ValidatedOutput {
                    definition: output.clone(),
                    value_type: output_value_type(output),
                },
            )
        })
        .collect();

    ValidatedCommonStep {
        dependencies: common.dependencies.clone(),
        cwd: common.cwd.clone(),
        outputs,
    }
}

fn validate_command_inputs(
    step_name: &str,
    inputs: &BTreeMap<String, ValueReference>,
    document: &WorkflowDocument,
    ancestors: &BTreeMap<String, BTreeSet<String>>,
    required_imports: &mut RequiredImports,
) -> Result<BTreeMap<String, ResolvedValueReference>, ValidationFailure> {
    inputs
        .iter()
        .map(|(input_name, reference)| {
            resolve_value_reference(
                step_name,
                reference,
                document,
                ancestors,
                required_imports,
                ValidationLocation::StepInput {
                    step: step_name.to_owned(),
                    input: input_name.clone(),
                },
            )
            .map(|input| (input_name.clone(), input))
        })
        .collect()
}

fn resolve_value_reference(
    step_name: &str,
    reference: &ValueReference,
    document: &WorkflowDocument,
    ancestors: &BTreeMap<String, BTreeSet<String>>,
    required_imports: &mut RequiredImports,
    location: ValidationLocation,
) -> Result<ResolvedValueReference, ValidationFailure> {
    match reference {
        ValueReference::Import { name } => {
            let (source, value_type) = match name.as_str() {
                "prompt" => {
                    required_imports.prompt = true;
                    (WorkflowImport::Prompt, WorkflowValueType::Text)
                }
                "attachments" => (
                    WorkflowImport::Attachments,
                    WorkflowValueType::AttachmentCollection,
                ),
                _ => {
                    return Err(ValidationFailure::new(
                        ValidationFailureKind::UnknownImport,
                        location,
                    ));
                }
            };
            Ok(ResolvedValueReference {
                source: ResolvedValueSource::Import(source),
                value_type,
            })
        }
        ValueReference::Output(reference) => {
            let Some(producer) = document.steps.get(&reference.step) else {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::UnknownOutputStep,
                    location,
                ));
            };
            let Some(output) = common_step(producer).outputs.get(&reference.output) else {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::UnknownOutput,
                    location,
                ));
            };
            let producer_is_ancestor = ancestors
                .get(step_name)
                .is_some_and(|step_ancestors| step_ancestors.contains(&reference.step));
            if !producer_is_ancestor {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::OutputProducerNotDependency,
                    location,
                ));
            }

            let value_type = output_value_type(output);
            Ok(ResolvedValueReference {
                source: ResolvedValueSource::Output(ResolvedOutputSource {
                    step: reference.step.clone(),
                    output: reference.output.clone(),
                    value_type,
                }),
                value_type,
            })
        }
    }
}

fn validate_agent_profiles(
    document: &WorkflowDocument,
) -> Result<BTreeMap<String, ValidatedHarness>, ValidationFailure> {
    document
        .agent_profiles
        .iter()
        .map(|(name, profile)| {
            let harness = match &profile.harness {
                HarnessDefinition::Pi { config } => pi::resolve_config(config)
                    .map(ValidatedHarness::Pi)
                    .ok_or_else(|| {
                        ValidationFailure::new(
                            ValidationFailureKind::InvalidAgentProfileConfig,
                            ValidationLocation::AgentProfile {
                                profile: name.clone(),
                            },
                        )
                    })?,
            };
            Ok((name.clone(), harness))
        })
        .collect()
}

fn resolve_agent_profile(
    step_name: &str,
    agent: &Agent,
    profiles: &BTreeMap<String, ValidatedHarness>,
) -> Result<ValidatedHarness, ValidationFailure> {
    profiles.get(&agent.profile).cloned().ok_or_else(|| {
        ValidationFailure::new(
            ValidationFailureKind::UnknownAgentProfile,
            ValidationLocation::AgentProfileReference {
                step: step_name.to_owned(),
            },
        )
    })
}

fn validate_agent(
    step_name: &str,
    agent: &Agent,
    document: &WorkflowDocument,
    ancestors: &BTreeMap<String, BTreeSet<String>>,
    required_imports: &mut RequiredImports,
    harness: ValidatedHarness,
) -> Result<ValidatedAgent, ValidationFailure> {
    let text = agent
        .message
        .text
        .iter()
        .enumerate()
        .map(|(index, source)| {
            validate_message_source(
                step_name,
                index,
                source,
                document,
                ancestors,
                required_imports,
                MessageDestination::Text,
            )
        })
        .collect::<Result<_, _>>()?;
    let attachments = agent
        .message
        .attachments
        .iter()
        .enumerate()
        .map(|(index, source)| {
            validate_message_source(
                step_name,
                index,
                source,
                document,
                ancestors,
                required_imports,
                MessageDestination::Attachment,
            )
        })
        .collect::<Result<_, _>>()?;

    Ok(ValidatedAgent {
        profile: agent.profile.clone(),
        system_prompt: agent.system_prompt.clone(),
        message: ValidatedAgentMessage { text, attachments },
        harness,
    })
}

#[derive(Clone, Copy)]
enum MessageDestination {
    Text,
    Attachment,
}

fn validate_message_source(
    step_name: &str,
    index: usize,
    source: &MessageSource,
    document: &WorkflowDocument,
    ancestors: &BTreeMap<String, BTreeSet<String>>,
    required_imports: &mut RequiredImports,
    destination: MessageDestination,
) -> Result<ValidatedMessageSource, ValidationFailure> {
    let location = match destination {
        MessageDestination::Text => ValidationLocation::MessageText {
            step: step_name.to_owned(),
            index,
        },
        MessageDestination::Attachment => ValidationLocation::MessageAttachment {
            step: step_name.to_owned(),
            index,
        },
    };

    let reference = match source {
        MessageSource::File { path } => {
            return Ok(ValidatedMessageSource::File { path: path.clone() });
        }
        MessageSource::Reference(reference) => reference,
    };
    let value = resolve_value_reference(
        step_name,
        reference,
        document,
        ancestors,
        required_imports,
        location.clone(),
    )?;
    let compatible = match destination {
        MessageDestination::Text => value.value_type == WorkflowValueType::Text,
        MessageDestination::Attachment => matches!(
            value.value_type,
            WorkflowValueType::AttachmentCollection
                | WorkflowValueType::Json
                | WorkflowValueType::File
        ),
    };
    if !compatible {
        return Err(ValidationFailure::new(
            ValidationFailureKind::MessageTypeMismatch,
            location,
        ));
    }

    Ok(ValidatedMessageSource::Reference {
        source: value.source,
        value_type: value.value_type,
    })
}

fn resolve_export(
    name: &str,
    reference: &OutputReference,
    document: &WorkflowDocument,
) -> Result<ResolvedOutputSource, ValidationFailure> {
    let Some(step) = document.steps.get(&reference.step) else {
        return Err(invalid_export(name));
    };
    let Some(output) = common_step(step).outputs.get(&reference.output) else {
        return Err(invalid_export(name));
    };
    Ok(ResolvedOutputSource {
        step: reference.step.clone(),
        output: reference.output.clone(),
        value_type: output_value_type(output),
    })
}

fn invalid_export(name: &str) -> ValidationFailure {
    ValidationFailure::new(
        ValidationFailureKind::InvalidExportTarget,
        ValidationLocation::Export {
            name: name.to_owned(),
        },
    )
}

fn common_step(step: &Step) -> &CommonStep {
    match step {
        Step::Command(command) => &command.common,
        Step::Agent(agent) => &agent.common,
    }
}

fn output_value_type(output: &Output) -> WorkflowValueType {
    match output {
        Output::AgentResponse => WorkflowValueType::Text,
        Output::AgentResult { .. } => WorkflowValueType::Json,
        Output::File { .. } => WorkflowValueType::File,
    }
}

#[cfg(test)]
mod tests;
