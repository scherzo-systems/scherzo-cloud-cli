use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use serde_json::Value;

use super::condition::{
    ConditionValueKind, JsonPointer, ResolvedOperand, ResolvedPredicate, ResolvedSelector,
};
use super::document::{
    Agent, CommonNode, ConditionOperand, ConditionPredicate, FailurePolicy, FinalizerDefinition,
    HarnessDefinition, MessageSource, NodeBody, Output, OutputReference, RecoveryHandler,
    StepDefinition, StepRecovery, ValueReference, WorkflowDocument,
};
use super::evidence::{MAXIMUM_PREREQUISITES, Prerequisite};
use super::validated::{
    RequiredImports, ResolvedDirectPrerequisite, ResolvedOutputSource, ResolvedValueReference,
    ResolvedValueSource, ValidatedAgent, ValidatedAgentMessage, ValidatedAgentStep,
    ValidatedCommandStep, ValidatedCommonStep, ValidatedFinalizer, ValidatedHarness,
    ValidatedMessageSource, ValidatedOutput, ValidatedRecoveryHandler, ValidatedStep,
    ValidatedStepRecovery, ValidatedWorkflow, WorkflowImport, WorkflowNode, WorkflowNodeRole,
    WorkflowValueType,
};
use super::{claude_code, codex, pi};

const MAXIMUM_WORKFLOW_NODES: usize = 256;
const MAXIMUM_CONDITION_DEPTH: usize = 16;
const MAXIMUM_CONDITION_NODES: usize = 256;
const MAXIMUM_CONDITION_CHILDREN: usize = 64;
const MAXIMUM_WORKFLOW_CONDITION_NODES: usize = 4_096;

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
    MessageTypeMismatch,
    TerminalOutputReference,
    IllegalCommandOutput,
    ExcessAgentResponseOutput,
    ExcessAgentResultOutput,
    ConflictingAgentValueOutputs,
    ExcessWorkspaceOutput,
    DuplicateOutputPath,
    AdvisoryDataDependency,
    InvalidExportTarget,
    AdvisoryExportTarget,
    TooManyNodes,
    DuplicateNodeId,
    InvalidSourceOrder,
    CrossPhaseOutputReference,
    InvalidFinalizerAfterTarget,
    InvalidFinalizerTrigger,
    IncompatibleFinalizerTriggers,
    InvalidFinalizationContext,
    FinalizerExportTrigger,
    TooManyPrerequisites,
    InvalidCondition,
    InvalidConditionReference,
    InvalidConditionType,
    InvalidJsonPointer,
    ConditionDepthExceeded,
    ConditionNodeLimitExceeded,
    ConditionChildLimitExceeded,
    WorkflowConditionNodeLimitExceeded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidationLocation {
    WorkflowGraph,
    WorkflowNamespace,
    AgentProfile { profile: String },
    AgentProfileReference { step: String },
    RecoveryAgentProfileReference { step: String },
    FinalizerAgentProfileReference { finalizer: String },
    StepDependency { step: String, index: usize },
    StepInput { step: String, input: String },
    MessageText { step: String, index: usize },
    MessageAttachment { step: String, index: usize },
    StepOutput { step: String, output: String },
    StepCondition { step: String },
    FinalizerAfter { finalizer: String, index: usize },
    FinalizerInput { finalizer: String, input: String },
    FinalizerMessageText { finalizer: String, index: usize },
    FinalizerMessageAttachment { finalizer: String, index: usize },
    FinalizerOutput { finalizer: String, output: String },
    FinalizerCondition { finalizer: String },
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
    direct_prerequisites: BTreeMap<String, Vec<ResolvedDirectPrerequisite>>,
    presentation_order: Vec<String>,
}

pub(crate) fn validate(document: WorkflowDocument) -> Result<ValidatedWorkflow, ValidationFailure> {
    validate_node_namespace(&document)?;
    validate_workflow_condition_bounds(&document)?;
    let agent_profiles = validate_agent_profiles(&document)?;
    let step_graph = validate_step_graph(&document)?;
    let finalizer_graph = validate_finalizer_graph(&document)?;
    validate_output_rules(&document)?;

    let mut required_imports = RequiredImports::default();
    let mut steps = BTreeMap::new();
    let mut recoveries = BTreeMap::new();
    for (step_name, step) in &document.steps {
        let body = validate_body(
            step_name,
            WorkflowNodeRole::Step,
            &step.body,
            &step_graph.direct_prerequisites[step_name],
            &document,
            &agent_profiles,
            &mut required_imports,
        )?;
        let recovery = validate_recovery(step_name, step.recovery.as_ref(), &agent_profiles)?;
        steps.insert(step_name.clone(), body);
        recoveries.insert(step_name.clone(), recovery);
    }

    let mut finalizers = BTreeMap::new();
    for (finalizer_name, finalizer) in &document.finalizers {
        let body = validate_body(
            finalizer_name,
            WorkflowNodeRole::Finalizer,
            &finalizer.body,
            &finalizer_graph.direct_prerequisites[finalizer_name],
            &document,
            &agent_profiles,
            &mut required_imports,
        )?;
        finalizers.insert(
            finalizer_name.clone(),
            ValidatedFinalizer {
                body,
                when: finalizer.when.clone(),
            },
        );
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
        recoveries,
        source_order: document.step_order,
        presentation_order: step_graph.presentation_order,
        finalizers,
        finalizer_source_order: document.finalizer_order,
        finalizer_presentation_order: finalizer_graph.presentation_order,
        exports,
        required_imports,
    })
}

fn validate_node_namespace(document: &WorkflowDocument) -> Result<(), ValidationFailure> {
    let total = document
        .steps
        .len()
        .checked_add(document.finalizers.len())
        .ok_or_else(|| namespace_failure(ValidationFailureKind::TooManyNodes))?;
    if total > MAXIMUM_WORKFLOW_NODES {
        return Err(namespace_failure(ValidationFailureKind::TooManyNodes));
    }
    if document
        .steps
        .keys()
        .any(|id| document.finalizers.contains_key(id))
    {
        return Err(namespace_failure(ValidationFailureKind::DuplicateNodeId));
    }
    validate_source_order(&document.steps, &document.step_order)?;
    validate_source_order(&document.finalizers, &document.finalizer_order)?;
    Ok(())
}

fn validate_source_order<T>(
    nodes: &BTreeMap<String, T>,
    source_order: &[String],
) -> Result<(), ValidationFailure> {
    let unique = source_order.iter().collect::<BTreeSet<_>>();
    if source_order.len() != nodes.len()
        || unique.len() != source_order.len()
        || source_order.iter().any(|id| !nodes.contains_key(id))
    {
        return Err(namespace_failure(ValidationFailureKind::InvalidSourceOrder));
    }
    Ok(())
}

fn namespace_failure(kind: ValidationFailureKind) -> ValidationFailure {
    ValidationFailure::new(kind, ValidationLocation::WorkflowNamespace)
}

fn validate_step_graph(document: &WorkflowDocument) -> Result<ValidatedGraph, ValidationFailure> {
    let direct_prerequisites = document
        .steps
        .iter()
        .map(|(step_name, step)| {
            resolve_step_prerequisites(step_name, step, document)
                .map(|prerequisites| (step_name.clone(), prerequisites))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    validate_graph(
        direct_prerequisites,
        &document.step_order,
        document.steps.len(),
    )
}

fn validate_finalizer_graph(
    document: &WorkflowDocument,
) -> Result<ValidatedGraph, ValidationFailure> {
    let direct_prerequisites = document
        .finalizers
        .iter()
        .map(|(finalizer_name, finalizer)| {
            resolve_finalizer_prerequisites(finalizer_name, finalizer, document)
                .map(|prerequisites| (finalizer_name.clone(), prerequisites))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    validate_graph(
        direct_prerequisites,
        &document.finalizer_order,
        document.finalizers.len(),
    )
}

fn validate_graph(
    direct_prerequisites: BTreeMap<String, Vec<ResolvedDirectPrerequisite>>,
    source_order: &[String],
    node_count: usize,
) -> Result<ValidatedGraph, ValidationFailure> {
    let source_indices = source_order
        .iter()
        .enumerate()
        .map(|(index, node)| (node.clone(), index))
        .collect::<BTreeMap<_, _>>();
    let mut dependents = BTreeMap::<String, Vec<String>>::new();
    let mut remaining_prerequisites = BTreeMap::<String, usize>::new();

    for (node_name, prerequisites) in &direct_prerequisites {
        for prerequisite in prerequisites {
            dependents
                .entry(prerequisite.producer.clone())
                .or_default()
                .push(node_name.clone());
        }
        remaining_prerequisites.insert(node_name.clone(), prerequisites.len());
    }

    let mut ready = remaining_prerequisites
        .iter()
        .filter(|(_, count)| **count == 0)
        .filter_map(|(name, _)| source_indices.get(name).map(|index| (*index, name.clone())))
        .collect::<BTreeSet<_>>();
    let mut presentation_order = Vec::with_capacity(node_count);

    while let Some((_, node_name)) = ready.pop_first() {
        presentation_order.push(node_name.clone());
        if let Some(node_dependents) = dependents.get(&node_name) {
            for dependent in node_dependents {
                if let Some(count) = remaining_prerequisites.get_mut(dependent) {
                    *count -= 1;
                    if *count == 0
                        && let Some(index) = source_indices.get(dependent)
                    {
                        ready.insert((*index, dependent.clone()));
                    }
                }
            }
        }
    }

    if presentation_order.len() != node_count {
        return Err(ValidationFailure::new(
            ValidationFailureKind::DependencyCycle,
            ValidationLocation::WorkflowGraph,
        ));
    }

    Ok(ValidatedGraph {
        direct_prerequisites,
        presentation_order,
    })
}

fn resolve_step_prerequisites(
    step_name: &str,
    step: &StepDefinition,
    document: &WorkflowDocument,
) -> Result<Vec<ResolvedDirectPrerequisite>, ValidationFailure> {
    let mut control_dependencies = BTreeSet::new();
    let mut prerequisites = BTreeMap::<String, ResolvedDirectPrerequisite>::new();
    for (index, dependency) in step.control_dependencies.iter().enumerate() {
        insert_control_prerequisite(
            step_name,
            dependency,
            &mut control_dependencies,
            &mut prerequisites,
            document.steps.contains_key(dependency),
            ValidationFailureKind::MissingDependency,
            ValidationLocation::StepDependency {
                step: step_name.to_owned(),
                index,
            },
        )?;
    }
    infer_condition_prerequisites(
        step_name,
        WorkflowNodeRole::Step,
        common_node(&step.body).condition.as_ref(),
        document,
        &mut prerequisites,
    )?;
    infer_body_prerequisites(
        step_name,
        WorkflowNodeRole::Step,
        &step.body,
        document,
        &mut prerequisites,
    )?;
    Ok(prerequisites.into_values().collect())
}

fn resolve_finalizer_prerequisites(
    finalizer_name: &str,
    finalizer: &FinalizerDefinition,
    document: &WorkflowDocument,
) -> Result<Vec<ResolvedDirectPrerequisite>, ValidationFailure> {
    if finalizer.when.is_empty() {
        return Err(ValidationFailure::new(
            ValidationFailureKind::InvalidFinalizerTrigger,
            ValidationLocation::WorkflowGraph,
        ));
    }

    let mut after = BTreeSet::new();
    let mut prerequisites = BTreeMap::<String, ResolvedDirectPrerequisite>::new();
    for (index, dependency) in finalizer.after.iter().enumerate() {
        insert_control_prerequisite(
            finalizer_name,
            dependency,
            &mut after,
            &mut prerequisites,
            document.finalizers.contains_key(dependency),
            ValidationFailureKind::InvalidFinalizerAfterTarget,
            ValidationLocation::FinalizerAfter {
                finalizer: finalizer_name.to_owned(),
                index,
            },
        )?;
    }
    infer_condition_prerequisites(
        finalizer_name,
        WorkflowNodeRole::Finalizer,
        common_node(&finalizer.body).condition.as_ref(),
        document,
        &mut prerequisites,
    )?;
    infer_body_prerequisites(
        finalizer_name,
        WorkflowNodeRole::Finalizer,
        &finalizer.body,
        document,
        &mut prerequisites,
    )?;
    Ok(prerequisites.into_values().collect())
}

fn insert_control_prerequisite(
    consumer: &str,
    dependency: &String,
    seen: &mut BTreeSet<String>,
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
    target_exists: bool,
    missing_kind: ValidationFailureKind,
    location: ValidationLocation,
) -> Result<(), ValidationFailure> {
    let failure = |kind| ValidationFailure::new(kind, location.clone());
    if dependency == consumer {
        return Err(failure(ValidationFailureKind::SelfDependency));
    }
    if !seen.insert(dependency.clone()) {
        return Err(failure(ValidationFailureKind::DuplicateDependency));
    }
    if !target_exists {
        return Err(failure(missing_kind));
    }
    prerequisites.insert(
        dependency.clone(),
        ResolvedDirectPrerequisite {
            producer: dependency.clone(),
            control: true,
            disposition_control: false,
            data: false,
            condition_data: false,
        },
    );
    Ok(())
}

fn validate_workflow_condition_bounds(
    document: &WorkflowDocument,
) -> Result<(), ValidationFailure> {
    let mut total = 0_usize;
    for (name, role, condition) in document
        .steps
        .iter()
        .map(|(name, step)| {
            (
                name,
                WorkflowNodeRole::Step,
                common_node(&step.body).condition.as_ref(),
            )
        })
        .chain(document.finalizers.iter().map(|(name, finalizer)| {
            (
                name,
                WorkflowNodeRole::Finalizer,
                common_node(&finalizer.body).condition.as_ref(),
            )
        }))
    {
        let Some(condition) = condition else { continue };
        let location = condition_location(name, role);
        let count = condition_node_count(condition, 1, &location)?;
        total = total.checked_add(count).ok_or_else(|| {
            ValidationFailure::new(
                ValidationFailureKind::WorkflowConditionNodeLimitExceeded,
                ValidationLocation::WorkflowGraph,
            )
        })?;
        if total > MAXIMUM_WORKFLOW_CONDITION_NODES {
            return Err(ValidationFailure::new(
                ValidationFailureKind::WorkflowConditionNodeLimitExceeded,
                ValidationLocation::WorkflowGraph,
            ));
        }
    }
    Ok(())
}

fn condition_node_count(
    predicate: &ConditionPredicate,
    depth: usize,
    location: &ValidationLocation,
) -> Result<usize, ValidationFailure> {
    if depth > MAXIMUM_CONDITION_DEPTH {
        return Err(ValidationFailure::new(
            ValidationFailureKind::ConditionDepthExceeded,
            location.clone(),
        ));
    }
    let children: &[ConditionPredicate] = match predicate {
        ConditionPredicate::All(children) | ConditionPredicate::Any(children) => {
            if children.is_empty() || children.len() > MAXIMUM_CONDITION_CHILDREN {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::ConditionChildLimitExceeded,
                    location.clone(),
                ));
            }
            children
        }
        ConditionPredicate::Not(child) => std::slice::from_ref(child),
        ConditionPredicate::Equals(_)
        | ConditionPredicate::Exists(_)
        | ConditionPredicate::Disposition { .. } => &[],
    };
    let mut count = 1_usize;
    for child in children {
        count = count
            .checked_add(condition_node_count(child, depth + 1, location)?)
            .ok_or_else(|| {
                ValidationFailure::new(
                    ValidationFailureKind::ConditionNodeLimitExceeded,
                    location.clone(),
                )
            })?;
        if count > MAXIMUM_CONDITION_NODES {
            return Err(ValidationFailure::new(
                ValidationFailureKind::ConditionNodeLimitExceeded,
                location.clone(),
            ));
        }
    }
    Ok(count)
}

fn infer_condition_prerequisites(
    consumer_name: &str,
    consumer_role: WorkflowNodeRole,
    condition: Option<&ConditionPredicate>,
    document: &WorkflowDocument,
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
) -> Result<(), ValidationFailure> {
    let Some(condition) = condition else {
        return Ok(());
    };
    match condition {
        ConditionPredicate::All(children) | ConditionPredicate::Any(children) => {
            for child in children {
                infer_condition_prerequisites(
                    consumer_name,
                    consumer_role,
                    Some(child),
                    document,
                    prerequisites,
                )?;
            }
        }
        ConditionPredicate::Not(child) => infer_condition_prerequisites(
            consumer_name,
            consumer_role,
            Some(child),
            document,
            prerequisites,
        )?,
        ConditionPredicate::Equals(operands) => {
            for operand in operands {
                if let ConditionOperand::Reference { reference, .. } = operand {
                    infer_condition_value_reference(
                        consumer_name,
                        consumer_role,
                        reference,
                        document,
                        prerequisites,
                    )?;
                }
            }
        }
        ConditionPredicate::Exists(selector) => infer_condition_value_reference(
            consumer_name,
            consumer_role,
            &selector.reference,
            document,
            prerequisites,
        )?,
        ConditionPredicate::Disposition { node, .. } => {
            let target_role = condition_target_role(
                document,
                node,
                condition_location(consumer_name, consumer_role),
            )?;
            if node == consumer_name && target_role == consumer_role
                || consumer_role == WorkflowNodeRole::Step && target_role != WorkflowNodeRole::Step
            {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::InvalidConditionReference,
                    condition_location(consumer_name, consumer_role),
                ));
            }
            if consumer_role == target_role {
                prerequisites
                    .entry(node.clone())
                    .and_modify(|prerequisite| prerequisite.disposition_control = true)
                    .or_insert_with(|| ResolvedDirectPrerequisite {
                        producer: node.clone(),
                        control: false,
                        disposition_control: true,
                        data: false,
                        condition_data: false,
                    });
            }
        }
    }
    Ok(())
}

fn infer_condition_value_reference(
    consumer_name: &str,
    consumer_role: WorkflowNodeRole,
    reference: &ValueReference,
    document: &WorkflowDocument,
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
) -> Result<(), ValidationFailure> {
    let location = condition_location(consumer_name, consumer_role);
    match reference {
        ValueReference::Import { name } if name == "prompt" => Ok(()),
        ValueReference::Import { .. } => Err(ValidationFailure::new(
            ValidationFailureKind::InvalidConditionReference,
            location,
        )),
        ValueReference::FinalizationContext if consumer_role == WorkflowNodeRole::Finalizer => {
            Ok(())
        }
        ValueReference::FinalizationContext => Err(ValidationFailure::new(
            ValidationFailureKind::InvalidConditionReference,
            location,
        )),
        ValueReference::Output(reference) => {
            let (producer_role, producer_body, output) =
                declared_output(document, reference, location.clone())?;
            if !matches!(
                output,
                Output::TextPath { .. }
                    | Output::TextAgentResponse
                    | Output::JsonPath { .. }
                    | Output::JsonAgentResult { .. }
            ) || reference.node == consumer_name && producer_role == consumer_role
                || consumer_role == WorkflowNodeRole::Step
                    && producer_role == WorkflowNodeRole::Finalizer
            {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::InvalidConditionReference,
                    location,
                ));
            }
            // jscpd:ignore-start -- Condition and body traversals retain distinct failure locations.
            validate_data_dependency_authority(
                document,
                consumer_name,
                &reference.node,
                consumer_role,
                producer_role,
                common_node(producer_body).failure_policy,
                &location,
            )?;
            if consumer_role == producer_role {
                mark_data_prerequisite(prerequisites, &reference.node, true);
            }
            // jscpd:ignore-end
            Ok(())
        }
    }
}

fn condition_location(name: &str, role: WorkflowNodeRole) -> ValidationLocation {
    match role {
        WorkflowNodeRole::Step => ValidationLocation::StepCondition {
            step: name.to_owned(),
        },
        WorkflowNodeRole::Finalizer => ValidationLocation::FinalizerCondition {
            finalizer: name.to_owned(),
        },
    }
}

fn condition_target_role(
    document: &WorkflowDocument,
    node: &str,
    location: ValidationLocation,
) -> Result<WorkflowNodeRole, ValidationFailure> {
    if document.steps.contains_key(node) {
        Ok(WorkflowNodeRole::Step)
    } else if document.finalizers.contains_key(node) {
        Ok(WorkflowNodeRole::Finalizer)
    } else {
        Err(ValidationFailure::new(
            ValidationFailureKind::InvalidConditionReference,
            location,
        ))
    }
}

fn validate_data_dependency_authority(
    document: &WorkflowDocument,
    consumer_name: &str,
    producer_name: &str,
    consumer_role: WorkflowNodeRole,
    producer_role: WorkflowNodeRole,
    producer_policy: FailurePolicy,
    location: &ValidationLocation,
) -> Result<(), ValidationFailure> {
    if producer_policy == FailurePolicy::Advisory
        && node_common(document, consumer_name, consumer_role).failure_policy
            == FailurePolicy::Required
    {
        return Err(ValidationFailure::new(
            ValidationFailureKind::AdvisoryDataDependency,
            location.clone(),
        ));
    }
    validate_finalizer_trigger_compatibility(
        document,
        consumer_name,
        producer_name,
        consumer_role,
        producer_role,
        location,
    )
}

fn validate_finalizer_trigger_compatibility(
    document: &WorkflowDocument,
    consumer_name: &str,
    producer_name: &str,
    consumer_role: WorkflowNodeRole,
    producer_role: WorkflowNodeRole,
    location: &ValidationLocation,
) -> Result<(), ValidationFailure> {
    if consumer_role == WorkflowNodeRole::Finalizer
        && producer_role == WorkflowNodeRole::Finalizer
        && !document.finalizers[consumer_name]
            .when
            .is_subset(&document.finalizers[producer_name].when)
    {
        return Err(ValidationFailure::new(
            ValidationFailureKind::IncompatibleFinalizerTriggers,
            location.clone(),
        ));
    }
    Ok(())
}

fn infer_body_prerequisites(
    consumer_name: &str,
    consumer_role: WorkflowNodeRole,
    body: &NodeBody,
    document: &WorkflowDocument,
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
) -> Result<(), ValidationFailure> {
    match body {
        NodeBody::Command(command) => {
            for (input, reference) in &command.inputs {
                infer_reference_prerequisite(
                    consumer_name,
                    consumer_role,
                    reference,
                    document,
                    prerequisites,
                    node_input_location(consumer_name, consumer_role, input),
                    consumer_role == WorkflowNodeRole::Finalizer,
                )?;
            }
        }
        NodeBody::Agent(agent) => {
            for (index, source) in agent.agent.message.text.iter().enumerate() {
                infer_message_prerequisite(
                    consumer_name,
                    consumer_role,
                    source,
                    document,
                    prerequisites,
                    node_message_location(consumer_name, consumer_role, index, false),
                    false,
                )?;
            }
            for (index, source) in agent.agent.message.attachments.iter().enumerate() {
                infer_message_prerequisite(
                    consumer_name,
                    consumer_role,
                    source,
                    document,
                    prerequisites,
                    node_message_location(consumer_name, consumer_role, index, true),
                    consumer_role == WorkflowNodeRole::Finalizer,
                )?;
            }
        }
    }
    Ok(())
}

fn infer_message_prerequisite(
    consumer_name: &str,
    consumer_role: WorkflowNodeRole,
    source: &MessageSource,
    document: &WorkflowDocument,
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
    location: ValidationLocation,
    context_allowed: bool,
) -> Result<(), ValidationFailure> {
    if let MessageSource::Reference(reference) = source {
        infer_reference_prerequisite(
            consumer_name,
            consumer_role,
            reference,
            document,
            prerequisites,
            location,
            context_allowed,
        )?;
    }
    Ok(())
}

fn infer_reference_prerequisite(
    consumer_name: &str,
    consumer_role: WorkflowNodeRole,
    reference: &ValueReference,
    document: &WorkflowDocument,
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
    location: ValidationLocation,
    context_allowed: bool,
) -> Result<(), ValidationFailure> {
    match reference {
        ValueReference::Import { .. } => return Ok(()),
        ValueReference::FinalizationContext => {
            return if context_allowed {
                Ok(())
            } else {
                Err(ValidationFailure::new(
                    ValidationFailureKind::InvalidFinalizationContext,
                    location,
                ))
            };
        }
        ValueReference::Output(reference) => {
            let (producer_role, producer_body, output) =
                declared_output(document, reference, location.clone())?;
            if matches!(output, Output::GitBranchWorkspace) {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::TerminalOutputReference,
                    location,
                ));
            }
            if reference.node == consumer_name && producer_role == consumer_role {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::SelfDependency,
                    location,
                ));
            }
            if consumer_role == WorkflowNodeRole::Step
                && producer_role == WorkflowNodeRole::Finalizer
            {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::CrossPhaseOutputReference,
                    location,
                ));
            }
            validate_data_dependency_authority(
                document,
                consumer_name,
                &reference.node,
                consumer_role,
                producer_role,
                common_node(producer_body).failure_policy,
                &location,
            )?;
            if consumer_role == producer_role {
                mark_data_prerequisite(prerequisites, &reference.node, false);
            }
        }
    }
    Ok(())
}

fn mark_data_prerequisite(
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
    producer: &str,
    condition_data: bool,
) {
    prerequisites
        .entry(producer.to_owned())
        .and_modify(|prerequisite| {
            prerequisite.data = true;
            prerequisite.condition_data |= condition_data;
        })
        .or_insert_with(|| ResolvedDirectPrerequisite {
            producer: producer.to_owned(),
            control: false,
            disposition_control: false,
            data: true,
            condition_data,
        });
}

fn validate_output_rules(document: &WorkflowDocument) -> Result<(), ValidationFailure> {
    for (name, step) in &document.steps {
        validate_node_outputs(name, WorkflowNodeRole::Step, &step.body)?;
    }
    for (name, finalizer) in &document.finalizers {
        validate_node_outputs(name, WorkflowNodeRole::Finalizer, &finalizer.body)?;
    }
    Ok(())
}

fn validate_node_outputs(
    name: &str,
    role: WorkflowNodeRole,
    body: &NodeBody,
) -> Result<(), ValidationFailure> {
    let common = common_node(body);
    let mut paths = BTreeSet::new();
    let mut workspace_count = 0;
    for (output_name, output) in &common.outputs {
        if let Some(path) = output_path(output)
            && !paths.insert(path)
        {
            return Err(output_failure(
                ValidationFailureKind::DuplicateOutputPath,
                name,
                role,
                output_name,
            ));
        }
        if matches!(output, Output::GitBranchWorkspace) {
            workspace_count += 1;
            if workspace_count > 1 {
                return Err(output_failure(
                    ValidationFailureKind::ExcessWorkspaceOutput,
                    name,
                    role,
                    output_name,
                ));
            }
        }
    }

    match body {
        NodeBody::Command(command) => {
            for (output_name, output) in &command.common.outputs {
                if matches!(
                    output,
                    Output::TextAgentResponse | Output::JsonAgentResult { .. }
                ) {
                    return Err(output_failure(
                        ValidationFailureKind::IllegalCommandOutput,
                        name,
                        role,
                        output_name,
                    ));
                }
            }
        }
        NodeBody::Agent(agent) => {
            let mut response_count = 0;
            let mut result_count = 0;
            for (output_name, output) in &agent.common.outputs {
                let failure_kind = match output {
                    Output::TextAgentResponse => {
                        response_count += 1;
                        if response_count > 1 {
                            Some(ValidationFailureKind::ExcessAgentResponseOutput)
                        } else {
                            (result_count > 0)
                                .then_some(ValidationFailureKind::ConflictingAgentValueOutputs)
                        }
                    }
                    Output::JsonAgentResult { .. } => {
                        result_count += 1;
                        if result_count > 1 {
                            Some(ValidationFailureKind::ExcessAgentResultOutput)
                        } else {
                            (response_count > 0)
                                .then_some(ValidationFailureKind::ConflictingAgentValueOutputs)
                        }
                    }
                    Output::TextPath { .. }
                    | Output::JsonPath { .. }
                    | Output::FilePath { .. }
                    | Output::GitBranchWorkspace => None,
                };
                if let Some(kind) = failure_kind {
                    return Err(output_failure(kind, name, role, output_name));
                }
            }
        }
    }
    Ok(())
}

fn output_path(output: &Output) -> Option<&str> {
    match output {
        Output::TextPath { path }
        | Output::JsonPath { path, .. }
        | Output::FilePath { path, .. } => Some(path),
        Output::TextAgentResponse | Output::JsonAgentResult { .. } | Output::GitBranchWorkspace => {
            None
        }
    }
}

fn output_failure(
    kind: ValidationFailureKind,
    name: &str,
    role: WorkflowNodeRole,
    output: &str,
) -> ValidationFailure {
    ValidationFailure::new(kind, node_output_location(name, role, output))
}

fn validate_recovery(
    step_name: &str,
    recovery: Option<&StepRecovery>,
    agent_profiles: &BTreeMap<String, ValidatedHarness>,
) -> Result<Option<ValidatedStepRecovery>, ValidationFailure> {
    recovery
        .map(|recovery| {
            let handler = recovery
                .handler
                .as_ref()
                .map(|handler| match handler {
                    RecoveryHandler::Command { argv, cwd } => {
                        Ok(ValidatedRecoveryHandler::Command {
                            argv: argv.clone(),
                            cwd: cwd.clone(),
                        })
                    }
                    RecoveryHandler::Agent {
                        profile,
                        prompt,
                        cwd,
                    } => {
                        let harness = agent_profiles.get(profile).cloned().ok_or_else(|| {
                            ValidationFailure::new(
                                ValidationFailureKind::UnknownAgentProfile,
                                ValidationLocation::RecoveryAgentProfileReference {
                                    step: step_name.to_owned(),
                                },
                            )
                        })?;
                        Ok(ValidatedRecoveryHandler::Agent {
                            profile: profile.clone(),
                            prompt: prompt.clone(),
                            cwd: cwd.clone(),
                            harness,
                        })
                    }
                })
                .transpose()?;
            Ok(ValidatedStepRecovery {
                retries: recovery.retries,
                handler,
            })
        })
        .transpose()
}

fn resolve_condition_predicate(
    predicate: &ConditionPredicate,
    node_name: &str,
    role: WorkflowNodeRole,
    document: &WorkflowDocument,
    required_imports: &mut RequiredImports,
    condition_values: &mut BTreeMap<String, ResolvedValueSource>,
) -> Result<ResolvedPredicate, ValidationFailure> {
    let resolve_child =
        |child: &ConditionPredicate,
         required_imports: &mut RequiredImports,
         condition_values: &mut BTreeMap<String, ResolvedValueSource>| {
            resolve_condition_predicate(
                child,
                node_name,
                role,
                document,
                required_imports,
                condition_values,
            )
        };
    match predicate {
        ConditionPredicate::All(children) => Ok(ResolvedPredicate::All(
            children
                .iter()
                .map(|child| resolve_child(child, required_imports, condition_values))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        )),
        ConditionPredicate::Any(children) => Ok(ResolvedPredicate::Any(
            children
                .iter()
                .map(|child| resolve_child(child, required_imports, condition_values))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        )),
        ConditionPredicate::Not(child) => Ok(ResolvedPredicate::Not(Box::new(resolve_child(
            child,
            required_imports,
            condition_values,
        )?))),
        ConditionPredicate::Equals(operands) => resolve_condition_equals(
            operands,
            node_name,
            role,
            document,
            required_imports,
            condition_values,
        ),
        ConditionPredicate::Exists(selector) => {
            let (canonical, source, kind) = resolve_condition_reference(
                &selector.reference,
                node_name,
                role,
                document,
                required_imports,
            )?;
            if kind != ConditionValueKind::Json {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::InvalidConditionType,
                    condition_location(node_name, role),
                ));
            }
            let pointer =
                JsonPointer::parse(Arc::<str>::from(selector.pointer.as_str())).map_err(|_| {
                    ValidationFailure::new(
                        ValidationFailureKind::InvalidJsonPointer,
                        condition_location(node_name, role),
                    )
                })?;
            condition_values.insert(canonical.clone(), source);
            Ok(ResolvedPredicate::Exists(ResolvedSelector::new(
                canonical, pointer,
            )))
        }
        ConditionPredicate::Disposition { node, is } => {
            let target_role =
                condition_target_role(document, node, condition_location(node_name, role))?;
            Ok(ResolvedPredicate::Disposition {
                node: WorkflowNode {
                    id: node.clone(),
                    role: target_role,
                },
                is: *is,
            })
        }
    }
}

fn resolve_condition_equals(
    operands: &[ConditionOperand; 2],
    node_name: &str,
    role: WorkflowNodeRole,
    document: &WorkflowDocument,
    required_imports: &mut RequiredImports,
    condition_values: &mut BTreeMap<String, ResolvedValueSource>,
) -> Result<ResolvedPredicate, ValidationFailure> {
    struct ReferenceResolution {
        canonical: String,
        source: ResolvedValueSource,
        kind: ConditionValueKind,
        pointer: Option<JsonPointer>,
    }
    let mut references: [Option<ReferenceResolution>; 2] = [None, None];
    for (index, operand) in operands.iter().enumerate() {
        let ConditionOperand::Reference { reference, pointer } = operand else {
            continue;
        };
        let (canonical, source, kind) =
            resolve_condition_reference(reference, node_name, role, document, required_imports)?;
        if kind == ConditionValueKind::Text && pointer.is_some() {
            return Err(ValidationFailure::new(
                ValidationFailureKind::InvalidConditionType,
                condition_location(node_name, role),
            ));
        }
        let pointer = pointer
            .as_ref()
            .map(|pointer| JsonPointer::parse(Arc::<str>::from(pointer.as_str())))
            .transpose()
            .map_err(|_| {
                ValidationFailure::new(
                    ValidationFailureKind::InvalidJsonPointer,
                    condition_location(node_name, role),
                )
            })?;
        references[index] = Some(ReferenceResolution {
            canonical,
            source,
            kind,
            pointer,
        });
    }
    let kind = match (&references[0], &references[1]) {
        (None, None) => {
            return Err(ValidationFailure::new(
                ValidationFailureKind::InvalidCondition,
                condition_location(node_name, role),
            ));
        }
        (Some(left), Some(right)) if left.kind != right.kind => {
            return Err(ValidationFailure::new(
                ValidationFailureKind::InvalidConditionType,
                condition_location(node_name, role),
            ));
        }
        (Some(reference), _) | (_, Some(reference)) => reference.kind,
    };
    let mut resolved = Vec::with_capacity(2);
    for (index, operand) in operands.iter().enumerate() {
        match operand {
            ConditionOperand::Reference { .. } => {
                let reference = references[index].take().ok_or_else(|| {
                    ValidationFailure::new(
                        ValidationFailureKind::InvalidConditionReference,
                        condition_location(node_name, role),
                    )
                })?;
                condition_values.insert(reference.canonical.clone(), reference.source);
                resolved.push(match reference.kind {
                    ConditionValueKind::Text => {
                        ResolvedOperand::text_reference(reference.canonical)
                    }
                    ConditionValueKind::Json => {
                        ResolvedOperand::json_reference(reference.canonical, reference.pointer)
                    }
                });
            }
            ConditionOperand::Literal(value) => match kind {
                ConditionValueKind::Text => {
                    let Value::String(value) = value else {
                        return Err(ValidationFailure::new(
                            ValidationFailureKind::InvalidConditionType,
                            condition_location(node_name, role),
                        ));
                    };
                    resolved.push(ResolvedOperand::text_literal(Arc::<str>::from(
                        value.as_str(),
                    )));
                }
                ConditionValueKind::Json => {
                    resolved.push(ResolvedOperand::json_literal(Arc::new(value.clone())));
                }
            },
        }
    }
    let operands: [ResolvedOperand; 2] = resolved.try_into().map_err(|_| {
        ValidationFailure::new(
            ValidationFailureKind::InvalidCondition,
            condition_location(node_name, role),
        )
    })?;
    Ok(ResolvedPredicate::Equals(operands))
}

fn resolve_condition_reference(
    reference: &ValueReference,
    node_name: &str,
    role: WorkflowNodeRole,
    document: &WorkflowDocument,
    required_imports: &mut RequiredImports,
) -> Result<(String, ResolvedValueSource, ConditionValueKind), ValidationFailure> {
    let resolved = resolve_value_reference(
        reference,
        document,
        required_imports,
        condition_location(node_name, role),
        role == WorkflowNodeRole::Finalizer,
    )?;
    let kind = match resolved.value_type {
        WorkflowValueType::Text => ConditionValueKind::Text,
        WorkflowValueType::Json => ConditionValueKind::Json,
        WorkflowValueType::AttachmentCollection
        | WorkflowValueType::File
        | WorkflowValueType::GitBranch => {
            return Err(ValidationFailure::new(
                ValidationFailureKind::InvalidConditionType,
                condition_location(node_name, role),
            ));
        }
    };
    let canonical = match &resolved.source {
        ResolvedValueSource::Import(WorkflowImport::Prompt) => "imports.prompt".to_owned(),
        ResolvedValueSource::Import(WorkflowImport::Attachments) => {
            return Err(ValidationFailure::new(
                ValidationFailureKind::InvalidConditionReference,
                condition_location(node_name, role),
            ));
        }
        ResolvedValueSource::Output(output) => output.reference(),
        ResolvedValueSource::FinalizationContext => "finalization.context".to_owned(),
    };
    Ok((canonical, resolved.source, kind))
}

fn validate_body(
    node_name: &str,
    role: WorkflowNodeRole,
    body: &NodeBody,
    prerequisites: &[ResolvedDirectPrerequisite],
    document: &WorkflowDocument,
    agent_profiles: &BTreeMap<String, ValidatedHarness>,
    required_imports: &mut RequiredImports,
) -> Result<ValidatedStep, ValidationFailure> {
    let mut condition_values = BTreeMap::new();
    let condition = common_node(body)
        .condition
        .as_ref()
        .map(|condition| {
            resolve_condition_predicate(
                condition,
                node_name,
                role,
                document,
                required_imports,
                &mut condition_values,
            )
        })
        .transpose()?;
    let evidence_prerequisites = evidence_prerequisites(body, prerequisites)?;
    match body {
        NodeBody::Command(command) => Ok(ValidatedStep::Command(ValidatedCommandStep {
            common: validate_common(
                &command.common,
                condition,
                condition_values,
                prerequisites,
                evidence_prerequisites,
            ),
            inputs: validate_command_inputs(
                node_name,
                role,
                &command.inputs,
                document,
                required_imports,
            )?,
            argv: command.argv.clone(),
        })),
        NodeBody::Agent(agent) => {
            let common = validate_common(
                &agent.common,
                condition,
                condition_values,
                prerequisites,
                evidence_prerequisites,
            );
            let harness = resolve_agent_profile(node_name, role, &agent.agent, agent_profiles)?;
            let validated_agent = validate_agent(
                node_name,
                role,
                &agent.agent,
                document,
                required_imports,
                harness,
            )?;
            Ok(ValidatedStep::Agent(ValidatedAgentStep {
                common,
                agent: validated_agent,
            }))
        }
    }
}

fn evidence_prerequisites(
    body: &NodeBody,
    direct: &[ResolvedDirectPrerequisite],
) -> Result<Vec<Prerequisite>, ValidationFailure> {
    let mut descriptors = direct
        .iter()
        .filter(|prerequisite| prerequisite.control)
        .filter_map(|prerequisite| Prerequisite::control(prerequisite.producer.clone()).ok())
        .collect::<Vec<_>>();
    retain_condition_descriptors(common_node(body).condition.as_ref(), &mut descriptors);
    let mut retain_reference = |reference: &ValueReference| {
        if let ValueReference::Output(output) = reference
            && let Ok(descriptor) =
                Prerequisite::body(format!("outputs.{}.{}", output.node, output.output))
        {
            descriptors.push(descriptor);
        }
    };
    match body {
        NodeBody::Command(command) => {
            for reference in command.inputs.values() {
                retain_reference(reference);
            }
        }
        NodeBody::Agent(agent) => {
            for source in agent
                .agent
                .message
                .text
                .iter()
                .chain(&agent.agent.message.attachments)
            {
                if let MessageSource::Reference(reference) = source {
                    retain_reference(reference);
                }
            }
        }
    }
    descriptors.sort();
    descriptors.dedup();
    if descriptors.len() > MAXIMUM_PREREQUISITES {
        return Err(ValidationFailure::new(
            ValidationFailureKind::TooManyPrerequisites,
            ValidationLocation::WorkflowGraph,
        ));
    }
    Ok(descriptors)
}

fn retain_condition_descriptors(
    condition: Option<&ConditionPredicate>,
    descriptors: &mut Vec<Prerequisite>,
) {
    let Some(condition) = condition else { return };
    match condition {
        ConditionPredicate::All(children) | ConditionPredicate::Any(children) => {
            for child in children {
                retain_condition_descriptors(Some(child), descriptors);
            }
        }
        ConditionPredicate::Not(child) => {
            retain_condition_descriptors(Some(child), descriptors);
        }
        ConditionPredicate::Equals(operands) => {
            for operand in operands {
                if let ConditionOperand::Reference {
                    reference: ValueReference::Output(output),
                    ..
                } = operand
                    && let Ok(descriptor) = Prerequisite::condition(format!(
                        "outputs.{}.{}",
                        output.node, output.output
                    ))
                {
                    descriptors.push(descriptor);
                }
            }
        }
        ConditionPredicate::Exists(selector) => {
            if let ValueReference::Output(output) = &selector.reference
                && let Ok(descriptor) =
                    Prerequisite::condition(format!("outputs.{}.{}", output.node, output.output))
            {
                descriptors.push(descriptor);
            }
        }
        ConditionPredicate::Disposition { node, .. } => {
            if let Ok(descriptor) = Prerequisite::control(node.clone()) {
                descriptors.push(descriptor);
            }
        }
    }
}

fn validate_common(
    common: &CommonNode,
    condition: Option<ResolvedPredicate>,
    condition_values: BTreeMap<String, ResolvedValueSource>,
    prerequisites: &[ResolvedDirectPrerequisite],
    evidence_prerequisites: Vec<Prerequisite>,
) -> ValidatedCommonStep {
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
        failure_policy: common.failure_policy,
        condition,
        condition_values,
        prerequisites: prerequisites.to_vec(),
        evidence_prerequisites,
        cwd: common.cwd.clone(),
        outputs,
    }
}

fn validate_command_inputs(
    node_name: &str,
    role: WorkflowNodeRole,
    inputs: &BTreeMap<String, ValueReference>,
    document: &WorkflowDocument,
    required_imports: &mut RequiredImports,
) -> Result<BTreeMap<String, ResolvedValueReference>, ValidationFailure> {
    inputs
        .iter()
        .map(|(input_name, reference)| {
            resolve_value_reference(
                reference,
                document,
                required_imports,
                node_input_location(node_name, role, input_name),
                role == WorkflowNodeRole::Finalizer,
            )
            .map(|input| (input_name.clone(), input))
        })
        .collect()
}

fn resolve_value_reference(
    reference: &ValueReference,
    document: &WorkflowDocument,
    required_imports: &mut RequiredImports,
    location: ValidationLocation,
    context_allowed: bool,
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
            let (role, _, output) = declared_output(document, reference, location)?;
            let value_type = output_value_type(output);
            Ok(ResolvedValueReference {
                source: ResolvedValueSource::Output(ResolvedOutputSource {
                    node: WorkflowNode {
                        id: reference.node.clone(),
                        role,
                    },
                    output: reference.output.clone(),
                    value_type,
                }),
                value_type,
            })
        }
        ValueReference::FinalizationContext if context_allowed => Ok(ResolvedValueReference {
            source: ResolvedValueSource::FinalizationContext,
            value_type: WorkflowValueType::Json,
        }),
        ValueReference::FinalizationContext => Err(ValidationFailure::new(
            ValidationFailureKind::InvalidFinalizationContext,
            location,
        )),
    }
}

fn declared_output<'a>(
    document: &'a WorkflowDocument,
    reference: &OutputReference,
    location: ValidationLocation,
) -> Result<(WorkflowNodeRole, &'a NodeBody, &'a Output), ValidationFailure> {
    let (role, body) = node_body(document, &reference.node).ok_or_else(|| {
        ValidationFailure::new(ValidationFailureKind::UnknownOutputStep, location.clone())
    })?;
    let output = common_node(body)
        .outputs
        .get(&reference.output)
        .ok_or_else(|| ValidationFailure::new(ValidationFailureKind::UnknownOutput, location))?;
    Ok((role, body, output))
}

fn validate_agent_profiles(
    document: &WorkflowDocument,
) -> Result<BTreeMap<String, ValidatedHarness>, ValidationFailure> {
    document
        .agent_profiles
        .iter()
        .map(|(name, profile)| {
            let invalid_config = || {
                ValidationFailure::new(
                    ValidationFailureKind::InvalidAgentProfileConfig,
                    ValidationLocation::AgentProfile {
                        profile: name.clone(),
                    },
                )
            };
            let harness = match &profile.harness {
                HarnessDefinition::Pi { config } => pi::resolve_config(config)
                    .map(ValidatedHarness::Pi)
                    .ok_or_else(invalid_config)?,
                HarnessDefinition::ClaudeCode { config } => claude_code::resolve_config(config)
                    .map(ValidatedHarness::ClaudeCode)
                    .ok_or_else(invalid_config)?,
                HarnessDefinition::Codex { config } => codex::resolve_config(config)
                    .map(ValidatedHarness::Codex)
                    .ok_or_else(invalid_config)?,
            };
            Ok((name.clone(), harness))
        })
        .collect()
}

fn resolve_agent_profile(
    node_name: &str,
    role: WorkflowNodeRole,
    agent: &Agent,
    profiles: &BTreeMap<String, ValidatedHarness>,
) -> Result<ValidatedHarness, ValidationFailure> {
    profiles.get(&agent.profile).cloned().ok_or_else(|| {
        let location = match role {
            WorkflowNodeRole::Step => ValidationLocation::AgentProfileReference {
                step: node_name.to_owned(),
            },
            WorkflowNodeRole::Finalizer => ValidationLocation::FinalizerAgentProfileReference {
                finalizer: node_name.to_owned(),
            },
        };
        ValidationFailure::new(ValidationFailureKind::UnknownAgentProfile, location)
    })
}

fn validate_agent(
    node_name: &str,
    role: WorkflowNodeRole,
    agent: &Agent,
    document: &WorkflowDocument,
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
                node_name,
                role,
                index,
                source,
                document,
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
                node_name,
                role,
                index,
                source,
                document,
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
    node_name: &str,
    role: WorkflowNodeRole,
    index: usize,
    source: &MessageSource,
    document: &WorkflowDocument,
    required_imports: &mut RequiredImports,
    destination: MessageDestination,
) -> Result<ValidatedMessageSource, ValidationFailure> {
    let attachment = matches!(destination, MessageDestination::Attachment);
    let location = node_message_location(node_name, role, index, attachment);

    let reference = match source {
        MessageSource::File { path } => {
            return Ok(ValidatedMessageSource::File { path: path.clone() });
        }
        MessageSource::Reference(reference) => reference,
    };
    let value = resolve_value_reference(
        reference,
        document,
        required_imports,
        location.clone(),
        role == WorkflowNodeRole::Finalizer && attachment,
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
    let Some((role, body)) = node_body(document, &reference.node) else {
        return Err(invalid_export(name));
    };
    let Some(output) = common_node(body).outputs.get(&reference.output) else {
        return Err(invalid_export(name));
    };
    if common_node(body).failure_policy == FailurePolicy::Advisory {
        return Err(ValidationFailure::new(
            ValidationFailureKind::AdvisoryExportTarget,
            ValidationLocation::Export {
                name: name.to_owned(),
            },
        ));
    }
    if role == WorkflowNodeRole::Finalizer
        && !document.finalizers[&reference.node]
            .when
            .contains(&super::document::FinalizationTrigger::Succeeded)
    {
        return Err(ValidationFailure::new(
            ValidationFailureKind::FinalizerExportTrigger,
            ValidationLocation::Export {
                name: name.to_owned(),
            },
        ));
    }
    Ok(ResolvedOutputSource {
        node: WorkflowNode {
            id: reference.node.clone(),
            role,
        },
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

fn node_body<'a>(
    document: &'a WorkflowDocument,
    name: &str,
) -> Option<(WorkflowNodeRole, &'a NodeBody)> {
    document
        .steps
        .get(name)
        .map(|step| (WorkflowNodeRole::Step, &step.body))
        .or_else(|| {
            document
                .finalizers
                .get(name)
                .map(|finalizer| (WorkflowNodeRole::Finalizer, &finalizer.body))
        })
}

fn node_common<'a>(
    document: &'a WorkflowDocument,
    name: &str,
    role: WorkflowNodeRole,
) -> &'a CommonNode {
    match role {
        WorkflowNodeRole::Step => common_node(&document.steps[name].body),
        WorkflowNodeRole::Finalizer => common_node(&document.finalizers[name].body),
    }
}

fn common_node(body: &NodeBody) -> &CommonNode {
    match body {
        NodeBody::Command(command) => &command.common,
        NodeBody::Agent(agent) => &agent.common,
    }
}

fn node_input_location(name: &str, role: WorkflowNodeRole, input: &str) -> ValidationLocation {
    match role {
        WorkflowNodeRole::Step => ValidationLocation::StepInput {
            step: name.to_owned(),
            input: input.to_owned(),
        },
        WorkflowNodeRole::Finalizer => ValidationLocation::FinalizerInput {
            finalizer: name.to_owned(),
            input: input.to_owned(),
        },
    }
}

fn node_message_location(
    name: &str,
    role: WorkflowNodeRole,
    index: usize,
    attachment: bool,
) -> ValidationLocation {
    match (role, attachment) {
        (WorkflowNodeRole::Step, false) => ValidationLocation::MessageText {
            step: name.to_owned(),
            index,
        },
        (WorkflowNodeRole::Step, true) => ValidationLocation::MessageAttachment {
            step: name.to_owned(),
            index,
        },
        (WorkflowNodeRole::Finalizer, false) => ValidationLocation::FinalizerMessageText {
            finalizer: name.to_owned(),
            index,
        },
        (WorkflowNodeRole::Finalizer, true) => ValidationLocation::FinalizerMessageAttachment {
            finalizer: name.to_owned(),
            index,
        },
    }
}

fn node_output_location(name: &str, role: WorkflowNodeRole, output: &str) -> ValidationLocation {
    match role {
        WorkflowNodeRole::Step => ValidationLocation::StepOutput {
            step: name.to_owned(),
            output: output.to_owned(),
        },
        WorkflowNodeRole::Finalizer => ValidationLocation::FinalizerOutput {
            finalizer: name.to_owned(),
            output: output.to_owned(),
        },
    }
}

fn output_value_type(output: &Output) -> WorkflowValueType {
    match output {
        Output::TextPath { .. } | Output::TextAgentResponse => WorkflowValueType::Text,
        Output::JsonPath { .. } | Output::JsonAgentResult { .. } => WorkflowValueType::Json,
        Output::FilePath { .. } => WorkflowValueType::File,
        Output::GitBranchWorkspace => WorkflowValueType::GitBranch,
    }
}

#[cfg(test)]
mod tests;
