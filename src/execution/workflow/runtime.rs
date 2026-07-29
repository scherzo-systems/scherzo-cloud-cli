use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::admission::AdmittedCommandWorkflow;
use super::validated::{ValidatedCommonStep, ValidatedStep, ValidatedWorkflow};

pub(crate) type OutputSet<Output> = BTreeMap<String, Output>;
pub(crate) type ExportSet<Output> = BTreeMap<String, Output>;

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct TransitionSequence(u64);

impl TransitionSequence {
    pub(crate) const fn get(self) -> u64 {
        self.0
    }

    fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ActionId {
    pub(crate) transition_sequence: TransitionSequence,
}

impl ActionId {
    fn for_transition(transition_sequence: TransitionSequence) -> Self {
        Self {
            transition_sequence,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SchedulingGate {
    Open,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowState {
    Executing { gate: SchedulingGate },
    Succeeded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StepState<Output> {
    Pending,
    Starting,
    Running,
    CapturingOutputs,
    Succeeded { outputs: OutputSet<Output> },
}

impl<Output> StepState<Output> {
    fn kind(&self) -> StepStateKind {
        match self {
            Self::Pending => StepStateKind::Pending,
            Self::Starting => StepStateKind::Starting,
            Self::Running => StepStateKind::Running,
            Self::CapturingOutputs => StepStateKind::CapturingOutputs,
            Self::Succeeded { .. } => StepStateKind::Succeeded,
        }
    }

    fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Starting | Self::Running | Self::CapturingOutputs
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StepStateKind {
    Pending,
    Starting,
    Running,
    CapturingOutputs,
    Succeeded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StepRuntimeState<Output> {
    pub(crate) state: StepState<Output>,
    pub(crate) current_action: Option<ActionId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeStep {
    dependencies: Arc<[String]>,
    declared_outputs: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeExport {
    step: String,
    output: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeDefinition {
    steps: BTreeMap<String, RuntimeStep>,
    exports: BTreeMap<String, RuntimeExport>,
    maximum_parallel_steps: NonZeroUsize,
}

impl RuntimeDefinition {
    fn from_admitted(admitted: &AdmittedCommandWorkflow) -> Self {
        Self::from_workflow(
            &admitted.workflow().definition,
            admitted.execution().limits().maximum_parallel_steps(),
        )
    }

    fn from_workflow(workflow: &ValidatedWorkflow, maximum_parallel_steps: NonZeroUsize) -> Self {
        let steps = workflow
            .steps
            .iter()
            .map(|(step_id, step)| {
                let common = common_step(step);
                (
                    step_id.clone(),
                    RuntimeStep {
                        dependencies: Arc::from(common.dependencies.clone()),
                        declared_outputs: common.outputs.keys().cloned().collect(),
                    },
                )
            })
            .collect();
        let exports = workflow
            .exports
            .iter()
            .map(|(name, source)| {
                (
                    name.clone(),
                    RuntimeExport {
                        step: source.step.clone(),
                        output: source.output.clone(),
                    },
                )
            })
            .collect();
        Self {
            steps,
            exports,
            maximum_parallel_steps,
        }
    }
}

fn common_step(step: &ValidatedStep) -> &ValidatedCommonStep {
    match step {
        ValidatedStep::Command(command) => &command.common,
        ValidatedStep::Agent(agent) => &agent.common,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeState<Output> {
    definition: Arc<RuntimeDefinition>,
    pub(crate) workflow: WorkflowState,
    pub(crate) steps: BTreeMap<String, StepRuntimeState<Output>>,
    pub(crate) exports: Option<ExportSet<Output>>,
    pub(crate) last_transition_sequence: TransitionSequence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Occurrence<Provisional, Output> {
    StepStarted {
        step: String,
        action: ActionId,
    },
    StepExecutionCompleted {
        step: String,
        action: ActionId,
        provisional: Provisional,
    },
    OutputsCaptured {
        step: String,
        action: ActionId,
        outputs: OutputSet<Output>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RunOutcome {
    Succeeded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Action<Provisional, Output> {
    StartStep {
        step: String,
    },
    CaptureOutputs {
        step: String,
        provisional: Provisional,
    },
    FinishRun {
        outcome: RunOutcome,
        exports: ExportSet<Output>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestedAction<Provisional, Output> {
    pub(crate) id: ActionId,
    pub(crate) action: Action<Provisional, Output>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TransitionEvent {
    Step {
        sequence: TransitionSequence,
        step: String,
        from: StepStateKind,
        to: StepStateKind,
    },
    Workflow {
        sequence: TransitionSequence,
        from: WorkflowState,
        to: WorkflowState,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Reduction<Provisional, Output> {
    pub(crate) state: RuntimeState<Output>,
    pub(crate) events: Vec<TransitionEvent>,
    pub(crate) actions: Vec<RequestedAction<Provisional, Output>>,
}

pub(crate) fn initialize<Provisional, Output>(
    admitted: &AdmittedCommandWorkflow,
) -> Reduction<Provisional, Output>
where
    Output: Clone,
{
    initialize_definition(RuntimeDefinition::from_admitted(admitted))
}

fn initialize_definition<Provisional, Output>(
    definition: RuntimeDefinition,
) -> Reduction<Provisional, Output>
where
    Output: Clone,
{
    let steps = definition
        .steps
        .keys()
        .map(|step| {
            (
                step.clone(),
                StepRuntimeState {
                    state: StepState::Pending,
                    current_action: None,
                },
            )
        })
        .collect();
    let mut reduction = Reduction {
        state: RuntimeState {
            definition: Arc::new(definition),
            workflow: WorkflowState::Executing {
                gate: SchedulingGate::Open,
            },
            steps,
            exports: None,
            last_transition_sequence: TransitionSequence::default(),
        },
        events: Vec::new(),
        actions: Vec::new(),
    };
    stabilize(&mut reduction);
    reduction
}

pub(crate) fn reduce<Provisional, Output>(
    current: &RuntimeState<Output>,
    occurrence: Occurrence<Provisional, Output>,
) -> Reduction<Provisional, Output>
where
    Output: Clone,
{
    let mut reduction = Reduction {
        state: current.clone(),
        events: Vec::new(),
        actions: Vec::new(),
    };
    if !matches!(reduction.state.workflow, WorkflowState::Executing { .. })
        || !apply_occurrence(&mut reduction, occurrence)
    {
        return reduction;
    }

    stabilize(&mut reduction);
    reduction
}

fn apply_occurrence<Provisional, Output>(
    reduction: &mut Reduction<Provisional, Output>,
    occurrence: Occurrence<Provisional, Output>,
) -> bool
where
    Output: Clone,
{
    match occurrence {
        Occurrence::StepStarted { step, action } => {
            if !step_accepts(&reduction.state, &step, StepStateKind::Starting, action) {
                return false;
            }
            transition_step(
                &mut reduction.state,
                &mut reduction.events,
                &step,
                StepState::Running,
                Some(action),
            );
        }
        Occurrence::StepExecutionCompleted {
            step,
            action,
            provisional,
        } => {
            if !step_accepts(&reduction.state, &step, StepStateKind::Running, action) {
                return false;
            }
            let sequence = transition_step(
                &mut reduction.state,
                &mut reduction.events,
                &step,
                StepState::CapturingOutputs,
                None,
            );
            if step_declares_outputs(&reduction.state, &step) {
                let capture_action = ActionId::for_transition(sequence);
                set_current_action(&mut reduction.state, &step, capture_action);
                reduction.actions.push(RequestedAction {
                    id: capture_action,
                    action: Action::CaptureOutputs { step, provisional },
                });
            } else {
                transition_step(
                    &mut reduction.state,
                    &mut reduction.events,
                    &step,
                    StepState::Succeeded {
                        outputs: BTreeMap::new(),
                    },
                    None,
                );
            }
        }
        Occurrence::OutputsCaptured {
            step,
            action,
            outputs,
        } => {
            if !step_accepts(
                &reduction.state,
                &step,
                StepStateKind::CapturingOutputs,
                action,
            ) || !outputs_match_declaration(&reduction.state, &step, &outputs)
            {
                return false;
            }
            transition_step(
                &mut reduction.state,
                &mut reduction.events,
                &step,
                StepState::Succeeded { outputs },
                None,
            );
        }
    }
    true
}

fn stabilize<Provisional, Output>(reduction: &mut Reduction<Provisional, Output>)
where
    Output: Clone,
{
    select_ready_steps(reduction);
    finish_if_terminal(reduction);
}

fn select_ready_steps<Provisional, Output>(reduction: &mut Reduction<Provisional, Output>) {
    if !matches!(
        reduction.state.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::Open
        }
    ) {
        return;
    }

    let active_steps = reduction
        .state
        .steps
        .values()
        .filter(|step| step.state.is_active())
        .count();
    let available_slots = reduction
        .state
        .definition
        .maximum_parallel_steps
        .get()
        .saturating_sub(active_steps);
    let selected = reduction
        .state
        .definition
        .steps
        .iter()
        .filter(|(step_id, definition)| step_is_ready(&reduction.state, step_id, definition))
        .take(available_slots)
        .map(|(step_id, _)| step_id.clone())
        .collect::<Vec<_>>();

    for step in selected {
        let sequence = transition_step(
            &mut reduction.state,
            &mut reduction.events,
            &step,
            StepState::Starting,
            None,
        );
        let action_id = ActionId::for_transition(sequence);
        set_current_action(&mut reduction.state, &step, action_id);
        reduction.actions.push(RequestedAction {
            id: action_id,
            action: Action::StartStep { step },
        });
    }
}

fn step_is_ready<Output>(
    state: &RuntimeState<Output>,
    step_id: &str,
    definition: &RuntimeStep,
) -> bool {
    state
        .steps
        .get(step_id)
        .is_some_and(|step| matches!(step.state, StepState::Pending))
        && definition.dependencies.iter().all(|dependency| {
            state
                .steps
                .get(dependency)
                .is_some_and(|step| matches!(step.state, StepState::Succeeded { .. }))
        })
}

fn finish_if_terminal<Provisional, Output>(reduction: &mut Reduction<Provisional, Output>)
where
    Output: Clone,
{
    if !matches!(reduction.state.workflow, WorkflowState::Executing { .. })
        || !reduction
            .state
            .steps
            .values()
            .all(|step| matches!(step.state, StepState::Succeeded { .. }))
    {
        return;
    }

    let Some(exports) = derive_exports(&reduction.state) else {
        return;
    };
    let from = reduction.state.workflow;
    let to = WorkflowState::Succeeded;
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to;
    reduction.state.exports = Some(exports.clone());
    reduction
        .events
        .push(TransitionEvent::Workflow { sequence, from, to });
    reduction.actions.push(RequestedAction {
        id: ActionId::for_transition(sequence),
        action: Action::FinishRun {
            outcome: RunOutcome::Succeeded,
            exports,
        },
    });
}

fn derive_exports<Output>(state: &RuntimeState<Output>) -> Option<ExportSet<Output>>
where
    Output: Clone,
{
    state
        .definition
        .exports
        .iter()
        .map(|(name, source)| {
            let step = state.steps.get(&source.step)?;
            let StepState::Succeeded { outputs } = &step.state else {
                return None;
            };
            Some((name.clone(), outputs.get(&source.output)?.clone()))
        })
        .collect()
}

fn step_accepts<Output>(
    state: &RuntimeState<Output>,
    step: &str,
    expected_state: StepStateKind,
    action: ActionId,
) -> bool {
    state.steps.get(step).is_some_and(|runtime| {
        runtime.state.kind() == expected_state && runtime.current_action == Some(action)
    })
}

fn step_declares_outputs<Output>(state: &RuntimeState<Output>, step: &str) -> bool {
    state
        .definition
        .steps
        .get(step)
        .is_some_and(|definition| !definition.declared_outputs.is_empty())
}

fn outputs_match_declaration<Output>(
    state: &RuntimeState<Output>,
    step: &str,
    outputs: &OutputSet<Output>,
) -> bool {
    state
        .definition
        .steps
        .get(step)
        .is_some_and(|definition| outputs.keys().eq(definition.declared_outputs.iter()))
}

fn set_current_action<Output>(state: &mut RuntimeState<Output>, step: &str, action: ActionId) {
    if let Some(runtime) = state.steps.get_mut(step) {
        runtime.current_action = Some(action);
    }
}

fn transition_step<Output>(
    state: &mut RuntimeState<Output>,
    events: &mut Vec<TransitionEvent>,
    step: &str,
    to: StepState<Output>,
    current_action: Option<ActionId>,
) -> TransitionSequence {
    let sequence = next_sequence(state);
    if let Some(runtime) = state.steps.get_mut(step) {
        let from = runtime.state.kind();
        let to_kind = to.kind();
        runtime.state = to;
        runtime.current_action = current_action;
        events.push(TransitionEvent::Step {
            sequence,
            step: step.to_owned(),
            from,
            to: to_kind,
        });
    }
    sequence
}

fn next_sequence<Output>(state: &mut RuntimeState<Output>) -> TransitionSequence {
    let sequence = state.last_transition_sequence.next();
    state.last_transition_sequence = sequence;
    sequence
}

#[cfg(test)]
mod tests;
