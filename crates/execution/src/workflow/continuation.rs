use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::admission::{ResolvedInput, ResolvedInputs};
use super::document::{FailurePolicy, Output};
use super::resolution::ResolvedWorkflow;
use super::validated::{
    ResolvedOutputSource, ResolvedValueSource, ValidatedCommonStep, ValidatedMessageSource,
    ValidatedStep, ValidatedWorkflow, WorkflowNodeRole, WorkflowValueType,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Partition {
    pub(crate) reexecuted: Vec<String>,
    pub(crate) inherited: Vec<String>,
}

/// Resolve selections against the new definition, never against the prior attempt.
/// The validated presentation order is Kahn order with source-index tie breaking.
pub(crate) fn partition(
    workflow: &ValidatedWorkflow,
    from: &[String],
) -> Result<Partition, Vec<String>> {
    let mut seen = BTreeSet::new();
    let invalid = from
        .iter()
        .filter(|id| !seen.insert((*id).clone()) || !workflow.steps.contains_key(*id))
        .cloned()
        .collect::<Vec<_>>();
    if from.is_empty() || !invalid.is_empty() {
        return Err(invalid);
    }

    let mut dependents: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for id in &workflow.presentation_order {
        let Some(step) = workflow.steps.get(id) else {
            return Err(vec![id.clone()]);
        };
        let common = match step {
            ValidatedStep::Command(command) => &command.common,
            ValidatedStep::Agent(agent) => &agent.common,
        };
        for prerequisite in &common.prerequisites {
            dependents
                .entry(&prerequisite.producer)
                .or_default()
                .push(id);
        }
    }
    let mut reexecuted = from.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let mut queue = from.iter().map(String::as_str).collect::<VecDeque<_>>();
    while let Some(id) = queue.pop_front() {
        for dependent in dependents.get(id).into_iter().flatten() {
            if reexecuted.insert(dependent) {
                queue.push_back(dependent);
            }
        }
    }
    let (reexecuted, inherited) = workflow
        .presentation_order
        .iter()
        .cloned()
        .partition(|id| reexecuted.contains(id.as_str()));
    Ok(Partition {
        reexecuted,
        inherited,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PriorState {
    Succeeded,
    Skipped,
    Inherited,
    Unsatisfied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionViolation {
    InvalidFrom {
        id: String,
    },
    Node {
        id: String,
        prior: Option<PriorState>,
    },
    RequiredSkipped {
        consumer: String,
        producer: String,
    },
    Reference {
        reference: String,
    },
}

/// Project immutable run values by the replacement declaration, without borrowing
/// undeclared values into the attempt. The caller must still admit the projected
/// values against every constraint of the resolved replacement definition.
pub(crate) fn project_inputs(
    workflow: &ValidatedWorkflow,
    prior: &ValidatedWorkflow,
    values: &ResolvedInputs,
) -> Result<ResolvedInputs, Vec<String>> {
    let (projected, unsatisfied) = project_input_candidates(workflow, prior, values);
    if unsatisfied.is_empty() {
        Ok(projected)
    } else {
        Err(unsatisfied)
    }
}

pub(crate) fn project_input_candidates(
    workflow: &ValidatedWorkflow,
    prior: &ValidatedWorkflow,
    values: &ResolvedInputs,
) -> (ResolvedInputs, Vec<String>) {
    let mut projected = BTreeMap::new();
    let mut unsatisfied = Vec::new();
    for (name, kind) in &workflow.required_inputs {
        let retained = values.get(name);
        if prior.required_inputs.get(name) != Some(kind)
            || !matches!(
                (kind, retained),
                (WorkflowValueType::Text, Some(ResolvedInput::Text(_)))
                    | (WorkflowValueType::Json, Some(ResolvedInput::Json(_)))
                    | (WorkflowValueType::File, Some(ResolvedInput::File(_)))
                    | (
                        WorkflowValueType::AttachmentCollection,
                        Some(ResolvedInput::Attachments(_))
                    )
            )
        {
            unsatisfied.push(name.clone());
        } else if let Some(value) = retained {
            projected.insert(name.clone(), value.clone());
        }
    }
    (ResolvedInputs::new(projected), unsatisfied)
}

/// Collect independent node and ordinary-output definition failures. Inputs and
/// finalizer-to-finalizer references belong to their own admission domains.
pub(crate) fn check_inheritance(
    workflow: &ValidatedWorkflow,
    previous: &ValidatedWorkflow,
    partition: &Partition,
    states: &BTreeMap<String, PriorState>,
    effectively_skipped: &BTreeSet<String>,
) -> Vec<AdmissionViolation> {
    let inherited = partition
        .inherited
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut violations = Vec::new();
    for id in &partition.inherited {
        let prior = states.get(id).copied();
        if !matches!(
            prior,
            Some(PriorState::Succeeded | PriorState::Skipped | PriorState::Inherited)
        ) {
            violations.push(AdmissionViolation::Node {
                id: id.clone(),
                prior,
            });
        }
    }
    let references = ordered_references(
        workflow,
        referenced_outputs(workflow, &partition.reexecuted),
    );
    for id in &partition.reexecuted {
        if let Some(step) = workflow.steps.get(id)
            && step_common(step).failure_policy == FailurePolicy::Required
        {
            let mut body_references = BTreeSet::new();
            check_body_references(step, &mut body_references);
            for producer in body_references
                .iter()
                .map(|source| &source.node.id)
                .collect::<BTreeSet<_>>()
            {
                if inherited.contains(producer.as_str()) && effectively_skipped.contains(producer) {
                    violations.push(AdmissionViolation::RequiredSkipped {
                        consumer: id.clone(),
                        producer: producer.clone(),
                    });
                }
            }
        }
    }
    for source in references {
        if source.node.role != WorkflowNodeRole::Step
            || !inherited.contains(source.node.id.as_str())
        {
            continue;
        }
        let expected = workflow
            .steps
            .get(&source.node.id)
            .and_then(|step| step_common(step).outputs.get(&source.output));
        let prior = previous
            .steps
            .get(&source.node.id)
            .and_then(|step| step_common(step).outputs.get(&source.output));
        if !matches!((expected, prior), (Some(expected), Some(prior)) if same_output_contract(&expected.definition, &prior.definition))
        {
            violations.push(AdmissionViolation::Reference {
                reference: source.reference(),
            });
        }
    }
    violations
}

fn ordered_references(
    workflow: &ValidatedWorkflow,
    references: BTreeSet<ResolvedOutputSource>,
) -> Vec<ResolvedOutputSource> {
    let indices = workflow
        .presentation_order
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let mut references = references.into_iter().collect::<Vec<_>>();
    references.sort_by_key(|source| {
        (
            indices
                .get(source.node.id.as_str())
                .copied()
                .unwrap_or(usize::MAX),
            source.reference(),
        )
    });
    references
}

pub(crate) fn sort_violations(violations: &mut [AdmissionViolation], workflow: &ValidatedWorkflow) {
    let indices = workflow
        .presentation_order
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    violations.sort_by_key(|violation| {
        let (reason, node, reference) = match violation {
            AdmissionViolation::InvalidFrom { id } => (0, id.as_str(), id.as_str()),
            AdmissionViolation::Node { id, .. } => (1, id.as_str(), ""),
            AdmissionViolation::RequiredSkipped { consumer, producer } => {
                (1, consumer.as_str(), producer.as_str())
            }
            AdmissionViolation::Reference { reference } => (
                2,
                reference.split('.').nth(1).unwrap_or(""),
                reference.as_str(),
            ),
        };
        (
            reason,
            indices.get(node).copied().unwrap_or(usize::MAX),
            reference.to_owned(),
        )
    });
}

pub(crate) fn referenced_outputs(
    workflow: &ValidatedWorkflow,
    reexecuted: &[String],
) -> BTreeSet<ResolvedOutputSource> {
    let mut references = BTreeSet::new();
    for id in reexecuted {
        if let Some(step) = workflow.steps.get(id) {
            check_step_references(step, &mut references);
        }
    }
    for finalizer in workflow.finalizers.values() {
        check_step_references(&finalizer.body, &mut references);
    }
    references.extend(workflow.exports.values().cloned());
    references
}

fn step_common(step: &ValidatedStep) -> &ValidatedCommonStep {
    match step {
        ValidatedStep::Command(command) => &command.common,
        ValidatedStep::Agent(agent) => &agent.common,
    }
}

fn check_step_references(step: &ValidatedStep, references: &mut BTreeSet<ResolvedOutputSource>) {
    let common = step_common(step);
    for source in common.condition_values.values() {
        add_reference(source, references);
    }
    check_body_references(step, references);
}

fn check_body_references(step: &ValidatedStep, references: &mut BTreeSet<ResolvedOutputSource>) {
    match step {
        ValidatedStep::Command(command) => {
            for reference in command.inputs.values() {
                add_reference(&reference.source, references);
            }
        }
        ValidatedStep::Agent(agent) => {
            // Document validation and resolved-reference admission traverse distinct types.
            // Keeping these traversals separate makes the admission boundary explicit.
            // jscpd:ignore-start
            for source in agent
                .agent
                .message
                .text
                .iter()
                .chain(&agent.agent.message.attachments)
            {
                if let ValidatedMessageSource::Reference { source, .. } = source {
                    add_reference(source, references);
                }
            }
            // jscpd:ignore-end
        }
    }
}

fn add_reference(source: &ResolvedValueSource, references: &mut BTreeSet<ResolvedOutputSource>) {
    if let ResolvedValueSource::Output(output) = source {
        references.insert(output.clone());
    }
}

/// Compare resolved node semantics and only the retained static bytes reachable
/// from this node. A change elsewhere in the closure does not change this node.
pub(crate) fn definition_changed(
    current: &ResolvedWorkflow,
    previous: &ResolvedWorkflow,
    id: &str,
) -> bool {
    let current_step = current.definition.steps.get(id);
    let previous_step = previous.definition.steps.get(id);
    if current_step != previous_step
        || current.definition.recoveries.get(id) != previous.definition.recoveries.get(id)
    {
        return true;
    }
    let Some(step) = current_step else {
        return true;
    };
    let mut paths = BTreeSet::new();
    let common = step_common(step);
    for output in common.outputs.values() {
        match &output.definition {
            Output::JsonPath { schema, .. } | Output::JsonAgentResult { schema } => {
                paths.insert(schema.as_str());
            }
            _ => {}
        }
    }
    if let Some(Some(super::validated::ValidatedStepRecovery {
        handler: Some(super::validated::ValidatedRecoveryHandler::Agent { prompt, .. }),
        ..
    })) = current.definition.recoveries.get(id)
    {
        paths.insert(prompt.as_str());
    }
    if let ValidatedStep::Agent(agent) = step {
        paths.insert(agent.agent.system_prompt.as_str());
        for source in agent
            .agent
            .message
            .text
            .iter()
            .chain(&agent.agent.message.attachments)
        {
            if let ValidatedMessageSource::File { path } = source {
                paths.insert(path);
            }
        }
    }
    paths
        .into_iter()
        .any(|path| current.source_bytes(path) != previous.source_bytes(path))
}

fn same_output_contract(current: &Output, previous: &Output) -> bool {
    match (current, previous) {
        (
            Output::TextPath { .. } | Output::TextAgentResponse,
            Output::TextPath { .. } | Output::TextAgentResponse,
        )
        | (
            Output::JsonPath { .. } | Output::JsonAgentResult { .. },
            Output::JsonPath { .. } | Output::JsonAgentResult { .. },
        )
        | (Output::GitBranchWorkspace, Output::GitBranchWorkspace) => true,
        (Output::FilePath { media_type: a, .. }, Output::FilePath { media_type: b, .. }) => a == b,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_of_control_and_data_closures_uses_kahn_order_not_selection_order() {
        let document = super::super::decode(
            b"schemaVersion: 1\nsteps:\n  west:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      text: {kind: text, from: path, path: west.txt}\n  east:\n    kind: cmd\n    command: {argv: [\"true\"]}\n  join:\n    kind: cmd\n    dependsOn: [east]\n    inputs:\n      value: {ref: outputs.west.text}\n    command: {argv: [\"true\"]}\n  tail:\n    kind: cmd\n    dependsOn: [join]\n    command: {argv: [\"true\"]}\n  alone:\n    kind: cmd\n    command: {argv: [\"true\"]}\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
        )
        .unwrap();
        let workflow = super::super::validation::validate(document).unwrap();
        let from = ["east".to_owned(), "west".to_owned()];
        let selected = partition(&workflow, &from).unwrap();
        assert_eq!(selected.reexecuted, ["west", "east", "join", "tail"]);
        assert_eq!(selected.inherited, ["alone"]);
        assert_eq!(
            partition(&workflow, &["cleanup".to_owned()]),
            Err(vec!["cleanup".to_owned()])
        );
        assert_eq!(
            partition(&workflow, &["east".to_owned(), "east".to_owned()]),
            Err(vec!["east".to_owned()])
        );
    }

    #[test]
    fn reference_failures_follow_presentation_order_not_lexical_ids() {
        let document = super::super::decode(b"schemaVersion: 1\nsteps:\n  zeta:\n    kind: cmd\n    outputs:\n      value: {kind: text, from: path, path: z.txt}\n    command: {argv: [\"true\"]}\n  alpha:\n    kind: cmd\n    outputs:\n      value: {kind: text, from: path, path: a.txt}\n    command: {argv: [\"true\"]}\n  consume:\n    kind: cmd\n    inputs:\n      z: {ref: outputs.zeta.value}\n      a: {ref: outputs.alpha.value}\n    command: {argv: [\"true\"]}\n").unwrap();
        let workflow = super::super::validation::validate(document).unwrap();
        let selected = partition(&workflow, &["consume".to_owned()]).unwrap();
        let mut previous = workflow.clone();
        for id in ["zeta", "alpha"] {
            if let Some(ValidatedStep::Command(command)) = previous.steps.get_mut(id) {
                command.common.outputs.clear();
            }
        }
        let states = BTreeMap::from([
            ("zeta".to_owned(), PriorState::Succeeded),
            ("alpha".to_owned(), PriorState::Succeeded),
        ]);
        assert_eq!(
            check_inheritance(&workflow, &previous, &selected, &states, &BTreeSet::new()),
            vec![
                AdmissionViolation::Reference {
                    reference: "outputs.zeta.value".to_owned()
                },
                AdmissionViolation::Reference {
                    reference: "outputs.alpha.value".to_owned()
                },
            ]
        );
    }

    #[test]
    fn definition_comparison_tracks_only_node_reached_static_bytes() {
        use std::fs;
        use std::path::Path;
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        fs::write(root.join("flow.yaml"), "schemaVersion: 1\nsteps:\n  cached:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      result: {kind: json, from: path, path: result.json, schema: result.schema.json}\n  independent:\n    kind: cmd\n    command: {argv: [\"true\"]}\n").unwrap();
        fs::write(
            root.join("result.schema.json"),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
        )
        .unwrap();
        let previous = super::super::resolution::resolve(root, Path::new("flow.yaml")).unwrap();
        fs::write(
            root.join("result.schema.json"),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}"#,
        )
        .unwrap();
        let current = super::super::resolution::resolve(root, Path::new("flow.yaml")).unwrap();
        assert!(definition_changed(&current, &previous, "cached"));
        assert!(!definition_changed(&current, &previous, "independent"));
    }

    #[test]
    fn definition_comparison_includes_agent_and_recovery_prompt_bytes() {
        use std::fs;
        use std::path::Path;
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        fs::write(root.join("system.md"), "initial system").unwrap();
        fs::write(root.join("message.md"), "initial message").unwrap();
        fs::write(root.join("repair.md"), "initial repair").unwrap();
        fs::write(root.join("flow.yaml"), "schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: pi\n      config: {model: fixture/model, thinking: off}\nsteps:\n  cached:\n    kind: agent\n    recovery:\n      retries: 1\n      handler:\n        kind: agent\n        profile: coding\n        prompt: repair.md\n    agent:\n      profile: coding\n      systemPrompt: system.md\n      message:\n        text: [{file: message.md}]\n").unwrap();
        let previous = super::super::resolution::resolve(root, Path::new("flow.yaml")).unwrap();
        for (file, bytes) in [("system.md", "new system"), ("repair.md", "new repair")] {
            fs::write(root.join(file), bytes).unwrap();
            let current = super::super::resolution::resolve(root, Path::new("flow.yaml")).unwrap();
            assert!(definition_changed(&current, &previous, "cached"), "{file}");
            fs::write(
                root.join(file),
                format!("initial {name}", name = file.trim_end_matches(".md")),
            )
            .unwrap();
        }
    }

    #[test]
    fn changed_retained_json_schema_is_recorded_without_rejecting_inheritance() {
        use std::{fs, path::Path};
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        fs::write(root.join("flow.yaml"), "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      value: {kind: json, from: path, path: value.json, schema: value.schema.json}\n  consumer:\n    kind: cmd\n    inputs:\n      value: {ref: outputs.producer.value}\n    command: {argv: [\"true\"]}\n").unwrap();
        fs::write(
            root.join("value.schema.json"),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
        )
        .unwrap();
        let previous = super::super::resolution::resolve(root, Path::new("flow.yaml")).unwrap();
        fs::write(
            root.join("value.schema.json"),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}"#,
        )
        .unwrap();
        let current = super::super::resolution::resolve(root, Path::new("flow.yaml")).unwrap();
        let selected = partition(&current.definition, &["consumer".to_owned()]).unwrap();
        let states = BTreeMap::from([("producer".to_owned(), PriorState::Succeeded)]);
        assert!(
            check_inheritance(
                &current.definition,
                &previous.definition,
                &selected,
                &states,
                &BTreeSet::new(),
            )
            .is_empty()
        );
        assert!(definition_changed(&current, &previous, "producer"));
    }

    #[test]
    fn condition_only_skipped_source_remains_admissible_across_inherited_chain() {
        let document = super::super::decode(b"schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      value: {kind: text, from: path, path: value.txt}\n  consumer:\n    kind: cmd\n    condition:\n      equals:\n        - ref: outputs.producer.value\n        - value: expected\n    command: {argv: [\"true\"]}\n").unwrap();
        let workflow = super::super::validation::validate(document).unwrap();
        let selected = partition(&workflow, &["consumer".to_owned()]).unwrap();
        let states = BTreeMap::from([("producer".to_owned(), PriorState::Inherited)]);
        let skipped = BTreeSet::from(["producer".to_owned()]);
        assert!(check_inheritance(&workflow, &workflow, &selected, &states, &skipped).is_empty());
    }

    #[test]
    fn admission_reports_inherited_node_and_reference_failures_independently() {
        let document = super::super::decode(
            b"schemaVersion: 1\nsteps:\n  cached:\n    kind: cmd\n    outputs:\n      value: {kind: text, from: path, path: value.txt}\n    command: {argv: [\"true\"]}\n  run:\n    kind: cmd\n    inputs:\n      value: {ref: outputs.cached.value}\n    command: {argv: [\"true\"]}\n  stranded:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
        ).unwrap();
        let workflow = super::super::validation::validate(document).unwrap();
        let selected = partition(&workflow, &["run".to_owned()]).unwrap();
        let mut previous = workflow.clone();
        if let Some(ValidatedStep::Command(command)) = previous.steps.get_mut("cached") {
            command.common.outputs.clear();
        }
        let states = BTreeMap::from([
            ("cached".to_owned(), PriorState::Inherited),
            ("stranded".to_owned(), PriorState::Unsatisfied),
        ]);
        assert_eq!(
            check_inheritance(&workflow, &previous, &selected, &states, &BTreeSet::new()),
            vec![
                AdmissionViolation::Node {
                    id: "stranded".to_owned(),
                    prior: Some(PriorState::Unsatisfied)
                },
                AdmissionViolation::Reference {
                    reference: "outputs.cached.value".to_owned()
                },
            ]
        );
        let states = BTreeMap::from([
            ("cached".to_owned(), PriorState::Skipped),
            ("stranded".to_owned(), PriorState::Succeeded),
        ]);
        assert!(
            check_inheritance(
                &workflow,
                &workflow,
                &selected,
                &states,
                &BTreeSet::from(["cached".to_owned()]),
            )
            .contains(&AdmissionViolation::RequiredSkipped {
                consumer: "run".to_owned(),
                producer: "cached".to_owned()
            })
        );
    }
}
