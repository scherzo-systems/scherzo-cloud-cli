use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::admission::{AdmittedWorkflow, CancellationReason};
use super::document::FailurePolicy;
use super::validated::{
    ResolvedDirectPrerequisite, ResolvedValueSource, ValidatedCommonStep, ValidatedMessageSource,
    ValidatedStep, ValidatedWorkflow,
};

pub(crate) type OutputSet<Output> = BTreeMap<String, Output>;
pub(crate) type ExportSet<Output> = BTreeMap<String, ExportValue<Output>>;

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
pub(crate) enum FailurePhase {
    Start,
    Execution,
    OutputCapture,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StepFailure<Cause> {
    pub(crate) step: String,
    pub(crate) phase: FailurePhase,
    pub(crate) cause: Cause,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SchedulingGate<Cause> {
    Open,
    FailureStopped {
        primary_failure: StepFailure<Cause>,
    },
    Cancelling {
        reason: CancellationReason,
        prior_failure: Option<StepFailure<Cause>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowState<Cause> {
    Executing {
        gate: SchedulingGate<Cause>,
    },
    Succeeded,
    Failed {
        primary_failure: StepFailure<Cause>,
        later_cancellation: Option<CancellationReason>,
    },
    Cancelled {
        reason: CancellationReason,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotRunReason {
    FailureStop,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StepState<Cause, Output> {
    Pending,
    Starting,
    Running,
    CapturingOutputs,
    Cancelling { reason: CancellationReason },
    Succeeded { outputs: OutputSet<Output> },
    Failed { phase: FailurePhase, cause: Cause },
    Blocked { dependency: String },
    NotRun { reason: NotRunReason },
    Cancelled { reason: CancellationReason },
}

impl<Cause, Output> StepState<Cause, Output> {
    fn kind(&self) -> StepStateKind {
        match self {
            Self::Pending => StepStateKind::Pending,
            Self::Starting => StepStateKind::Starting,
            Self::Running => StepStateKind::Running,
            Self::CapturingOutputs => StepStateKind::CapturingOutputs,
            Self::Cancelling { .. } => StepStateKind::Cancelling,
            Self::Succeeded { .. } => StepStateKind::Succeeded,
            Self::Failed { .. } => StepStateKind::Failed,
            Self::Blocked { .. } => StepStateKind::Blocked,
            Self::NotRun { .. } => StepStateKind::NotRun,
            Self::Cancelled { .. } => StepStateKind::Cancelled,
        }
    }

    fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Starting | Self::Running | Self::CapturingOutputs | Self::Cancelling { .. }
        )
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded { .. }
                | Self::Failed { .. }
                | Self::Blocked { .. }
                | Self::NotRun { .. }
                | Self::Cancelled { .. }
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StepStateKind {
    Pending,
    Starting,
    Running,
    CapturingOutputs,
    Cancelling,
    Succeeded,
    Failed,
    Blocked,
    NotRun,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StepRuntimeState<Cause, Output> {
    pub(crate) state: StepState<Cause, Output>,
    pub(crate) current_action: Option<ActionId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeStep {
    failure_policy: FailurePolicy,
    prerequisites: Arc<[ResolvedDirectPrerequisite]>,
    inputs: BTreeMap<String, ResolvedValueSource>,
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
    fn from_admitted(admitted: &AdmittedWorkflow) -> Self {
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
                        failure_policy: common.failure_policy,
                        prerequisites: Arc::from(common.prerequisites.clone()),
                        inputs: match step {
                            ValidatedStep::Command(command) => command
                                .inputs
                                .iter()
                                .map(|(name, reference)| (name.clone(), reference.source.clone()))
                                .collect(),
                            ValidatedStep::Agent(agent) => agent
                                .agent
                                .message
                                .text
                                .iter()
                                .chain(&agent.agent.message.attachments)
                                .filter_map(|source| match source {
                                    ValidatedMessageSource::Reference {
                                        source: ResolvedValueSource::Output(source),
                                        ..
                                    } => Some((
                                        source.reference(),
                                        ResolvedValueSource::Output(source.clone()),
                                    )),
                                    ValidatedMessageSource::File { .. }
                                    | ValidatedMessageSource::Reference {
                                        source: ResolvedValueSource::Import(_),
                                        ..
                                    } => None,
                                })
                                .collect(),
                        },
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExportUnavailableReason {
    Failed,
    Blocked,
    NotRun,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExportValue<Output> {
    Available { output: Output },
    Unavailable { reason: ExportUnavailableReason },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeState<Cause, Output> {
    definition: Arc<RuntimeDefinition>,
    pub(crate) workflow: WorkflowState<Cause>,
    pub(crate) steps: BTreeMap<String, StepRuntimeState<Cause, Output>>,
    pub(crate) exports: Option<ExportSet<Output>>,
    pub(crate) last_transition_sequence: TransitionSequence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CancellationRequest<Deadline> {
    pub(crate) reason: CancellationReason,
    pub(crate) deadline: Deadline,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Occurrence<Provisional, Cause, Output, Deadline> {
    StepStarted {
        step: String,
        action: ActionId,
    },
    StepStartFailed {
        step: String,
        action: ActionId,
        cause: Cause,
    },
    StepExecutionCompleted {
        step: String,
        action: ActionId,
        provisional: Provisional,
    },
    StepExecutionFailed {
        step: String,
        action: ActionId,
        cause: Cause,
    },
    OutputsCaptured {
        step: String,
        action: ActionId,
        outputs: OutputSet<Output>,
    },
    OutputCaptureFailed {
        step: String,
        action: ActionId,
        cause: Cause,
    },
    CancellationRequested {
        reason: CancellationReason,
        deadline: Deadline,
    },
    StepQuiesced {
        step: String,
        action: ActionId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RunOutcome<Cause> {
    Succeeded,
    Failed {
        primary_failure: StepFailure<Cause>,
        later_cancellation: Option<CancellationReason>,
    },
    Cancelled {
        reason: CancellationReason,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ActionInput<Output> {
    Import,
    Output(Output),
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Action<Provisional, Cause, Output, Deadline> {
    StartStep {
        step: String,
        inputs: BTreeMap<String, ActionInput<Output>>,
    },
    CaptureOutputs {
        step: String,
        provisional: Provisional,
    },
    CancelStep {
        step: String,
        reason: CancellationReason,
        deadline: Deadline,
    },
    FinishRun {
        outcome: RunOutcome<Cause>,
        exports: ExportSet<Output>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestedAction<Provisional, Cause, Output, Deadline> {
    pub(crate) id: ActionId,
    pub(crate) action: Action<Provisional, Cause, Output, Deadline>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TransitionEvent<Cause, Deadline> {
    Step {
        sequence: TransitionSequence,
        step: String,
        failure_policy: FailurePolicy,
        from: StepStateKind,
        to: StepStateKind,
    },
    Workflow {
        sequence: TransitionSequence,
        from: WorkflowState<Cause>,
        to: WorkflowState<Cause>,
    },
    CancellationAccepted {
        sequence: TransitionSequence,
        reason: CancellationReason,
        deadline: Deadline,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Reduction<Provisional, Cause, Output, Deadline> {
    pub(crate) state: RuntimeState<Cause, Output>,
    pub(crate) events: Vec<TransitionEvent<Cause, Deadline>>,
    pub(crate) actions: Vec<RequestedAction<Provisional, Cause, Output, Deadline>>,
    // Initialization is accepted by construction; reductions expose stale rejection to
    // occurrence adapters that retain resources until the coordinator commits a decision.
    pub(crate) occurrence_accepted: bool,
}

pub(crate) fn initialize<Provisional, Cause, Output, Deadline>(
    admitted: &AdmittedWorkflow,
    initial_cancellation: Option<CancellationRequest<Deadline>>,
) -> Reduction<Provisional, Cause, Output, Deadline>
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    initialize_definition(ExecutionStart {
        definition: RuntimeDefinition::from_admitted(admitted),
        initial_cancellation,
    })
}

struct ExecutionStart<Deadline> {
    definition: RuntimeDefinition,
    initial_cancellation: Option<CancellationRequest<Deadline>>,
}

fn initialize_definition<Provisional, Cause, Output, Deadline>(
    start: ExecutionStart<Deadline>,
) -> Reduction<Provisional, Cause, Output, Deadline>
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let ExecutionStart {
        definition,
        initial_cancellation,
    } = start;
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
        occurrence_accepted: true,
    };
    if let Some(cancellation) = initial_cancellation {
        apply_cancellation(&mut reduction, cancellation);
    }
    stabilize(&mut reduction);
    reduction
}

pub(crate) fn reduce<Provisional, Cause, Output, Deadline>(
    current: &RuntimeState<Cause, Output>,
    occurrence: Occurrence<Provisional, Cause, Output, Deadline>,
) -> Reduction<Provisional, Cause, Output, Deadline>
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let mut reduction = Reduction {
        state: current.clone(),
        events: Vec::new(),
        actions: Vec::new(),
        occurrence_accepted: false,
    };
    if !matches!(&reduction.state.workflow, WorkflowState::Executing { .. })
        || !apply_occurrence(&mut reduction, occurrence)
    {
        return reduction;
    }

    reduction.occurrence_accepted = true;
    stabilize(&mut reduction);
    reduction
}

fn apply_occurrence<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    occurrence: Occurrence<Provisional, Cause, Output, Deadline>,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
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
        Occurrence::StepStartFailed {
            step,
            action,
            cause,
        } => {
            return apply_step_failure(
                reduction,
                step,
                action,
                StepStateKind::Starting,
                FailurePhase::Start,
                cause,
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
        Occurrence::StepExecutionFailed {
            step,
            action,
            cause,
        } => {
            return apply_step_failure(
                reduction,
                step,
                action,
                StepStateKind::Running,
                FailurePhase::Execution,
                cause,
            );
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
        Occurrence::OutputCaptureFailed {
            step,
            action,
            cause,
        } => {
            return apply_step_failure(
                reduction,
                step,
                action,
                StepStateKind::CapturingOutputs,
                FailurePhase::OutputCapture,
                cause,
            );
        }
        Occurrence::CancellationRequested { reason, deadline } => {
            return apply_cancellation(reduction, CancellationRequest { reason, deadline });
        }
        Occurrence::StepQuiesced { step, action } => {
            let Some(reason) = cancelling_step_reason(&reduction.state, &step, action) else {
                return false;
            };
            transition_step(
                &mut reduction.state,
                &mut reduction.events,
                &step,
                StepState::Cancelled { reason },
                None,
            );
        }
    }
    true
}

fn apply_cancellation<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    cancellation: CancellationRequest<Deadline>,
) -> bool
where
    Cause: Clone,
    Deadline: Clone,
{
    let prior_failure = match &reduction.state.workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => None,
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { primary_failure },
        } => Some(primary_failure.clone()),
        WorkflowState::Executing {
            gate: SchedulingGate::Cancelling { .. },
        }
        | WorkflowState::Succeeded
        | WorkflowState::Failed { .. }
        | WorkflowState::Cancelled { .. } => return false,
    };

    let to = WorkflowState::Executing {
        gate: SchedulingGate::Cancelling {
            reason: cancellation.reason,
            prior_failure,
        },
    };
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to;
    reduction
        .events
        .push(TransitionEvent::CancellationAccepted {
            sequence,
            reason: cancellation.reason,
            deadline: cancellation.deadline.clone(),
        });

    let steps = reduction
        .state
        .steps
        .iter()
        .map(|(step, runtime)| (step.clone(), runtime.state.kind()))
        .collect::<Vec<_>>();
    for (step, state) in steps {
        match state {
            StepStateKind::Pending => {
                transition_step(
                    &mut reduction.state,
                    &mut reduction.events,
                    &step,
                    StepState::Cancelled {
                        reason: cancellation.reason,
                    },
                    None,
                );
            }
            StepStateKind::Starting | StepStateKind::Running | StepStateKind::CapturingOutputs => {
                let sequence = transition_step(
                    &mut reduction.state,
                    &mut reduction.events,
                    &step,
                    StepState::Cancelling {
                        reason: cancellation.reason,
                    },
                    None,
                );
                let action = ActionId::for_transition(sequence);
                set_current_action(&mut reduction.state, &step, action);
                reduction.actions.push(RequestedAction {
                    id: action,
                    action: Action::CancelStep {
                        step,
                        reason: cancellation.reason,
                        deadline: cancellation.deadline.clone(),
                    },
                });
            }
            StepStateKind::Cancelling
            | StepStateKind::Succeeded
            | StepStateKind::Failed
            | StepStateKind::Blocked
            | StepStateKind::NotRun
            | StepStateKind::Cancelled => {}
        }
    }
    true
}

fn cancelling_step_reason<Cause, Output>(
    state: &RuntimeState<Cause, Output>,
    step: &str,
    action: ActionId,
) -> Option<CancellationReason> {
    let runtime = state.steps.get(step)?;
    match &runtime.state {
        StepState::Cancelling { reason } if runtime.current_action == Some(action) => Some(*reason),
        _ => None,
    }
}

fn apply_step_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
    action: ActionId,
    expected_state: StepStateKind,
    phase: FailurePhase,
    cause: Cause,
) -> bool
where
    Cause: Clone,
{
    if !step_accepts(&reduction.state, &step, expected_state, action) {
        return false;
    }

    let failure_policy = reduction
        .state
        .definition
        .steps
        .get(&step)
        .map(|definition| definition.failure_policy)
        .unwrap_or_default();
    let primary_failure = StepFailure {
        step: step.clone(),
        phase,
        cause: cause.clone(),
    };
    transition_step(
        &mut reduction.state,
        &mut reduction.events,
        &step,
        StepState::Failed { phase, cause },
        None,
    );
    if failure_policy == FailurePolicy::Required {
        close_gate_for_failure(&mut reduction.state, &mut reduction.events, primary_failure);
    }
    true
}

fn close_gate_for_failure<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output>,
    events: &mut Vec<TransitionEvent<Cause, Deadline>>,
    primary_failure: StepFailure<Cause>,
) where
    Cause: Clone,
{
    if !matches!(
        &state.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::Open
        }
    ) {
        return;
    }

    let from = state.workflow.clone();
    let to = WorkflowState::Executing {
        gate: SchedulingGate::FailureStopped { primary_failure },
    };
    let sequence = next_sequence(state);
    state.workflow = to.clone();
    events.push(TransitionEvent::Workflow { sequence, from, to });
}

fn stabilize<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
{
    propagate_pending_dispositions(reduction);
    select_ready_steps(reduction);
    finish_if_terminal(reduction);
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PendingDisposition {
    Blocked { dependency: String },
    NotRun,
}

fn propagate_pending_dispositions<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) {
    while let Some((step, disposition)) = next_pending_disposition(&reduction.state) {
        let state = match disposition {
            PendingDisposition::Blocked { dependency } => StepState::Blocked { dependency },
            PendingDisposition::NotRun => StepState::NotRun {
                reason: NotRunReason::FailureStop,
            },
        };
        transition_step(
            &mut reduction.state,
            &mut reduction.events,
            &step,
            state,
            None,
        );
    }
}

fn next_pending_disposition<Cause, Output>(
    state: &RuntimeState<Cause, Output>,
) -> Option<(String, PendingDisposition)> {
    let failure_stopped = matches!(
        &state.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { .. }
        }
    );
    state
        .definition
        .steps
        .iter()
        .find_map(|(step_id, definition)| {
            let runtime = state.steps.get(step_id)?;
            if !matches!(runtime.state, StepState::Pending) {
                return None;
            }

            if let Some(prerequisite) = definition
                .prerequisites
                .iter()
                .filter(|prerequisite| {
                    state
                        .steps
                        .get(&prerequisite.producer)
                        .is_some_and(|step| step.state.is_terminal())
                        && !prerequisite_satisfied(state, prerequisite)
                })
                .min_by(|left, right| left.producer.cmp(&right.producer))
            {
                return Some((
                    step_id.clone(),
                    PendingDisposition::Blocked {
                        dependency: prerequisite.producer.clone(),
                    },
                ));
            }

            (failure_stopped
                && definition
                    .prerequisites
                    .iter()
                    .all(|prerequisite| prerequisite_satisfied(state, prerequisite)))
            .then(|| (step_id.clone(), PendingDisposition::NotRun))
        })
}

fn prerequisite_satisfied<Cause, Output>(
    state: &RuntimeState<Cause, Output>,
    prerequisite: &ResolvedDirectPrerequisite,
) -> bool {
    let Some(producer) = state.steps.get(&prerequisite.producer) else {
        return false;
    };
    let succeeded = matches!(producer.state, StepState::Succeeded { .. });
    let control_satisfied = succeeded
        || (state
            .definition
            .steps
            .get(&prerequisite.producer)
            .is_some_and(|definition| definition.failure_policy == FailurePolicy::Advisory)
            && matches!(
                producer.state,
                StepState::Failed { .. } | StepState::Blocked { .. }
            ));
    (!prerequisite.control || control_satisfied) && (!prerequisite.data || succeeded)
}

fn select_ready_steps<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Output: Clone,
{
    if !matches!(
        &reduction.state.workflow,
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
        .map(|(step_id, definition)| {
            (
                step_id.clone(),
                resolved_action_inputs(&reduction.state, definition),
            )
        })
        .take(available_slots)
        .collect::<Vec<_>>();

    for (step, inputs) in selected {
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
            action: Action::StartStep { step, inputs },
        });
    }
}

fn resolved_action_inputs<Cause, Output>(
    state: &RuntimeState<Cause, Output>,
    definition: &RuntimeStep,
) -> BTreeMap<String, ActionInput<Output>>
where
    Output: Clone,
{
    definition
        .inputs
        .iter()
        .map(|(input, source)| {
            let value = match source {
                ResolvedValueSource::Import(_) => ActionInput::Import,
                ResolvedValueSource::Output(source) => state
                    .steps
                    .get(&source.step)
                    .and_then(|producer| match &producer.state {
                        StepState::Succeeded { outputs } => outputs.get(&source.output),
                        _ => None,
                    })
                    .cloned()
                    .map_or(ActionInput::Unavailable, ActionInput::Output),
            };
            (input.clone(), value)
        })
        .collect()
}

fn step_is_ready<Cause, Output>(
    state: &RuntimeState<Cause, Output>,
    step_id: &str,
    definition: &RuntimeStep,
) -> bool {
    state
        .steps
        .get(step_id)
        .is_some_and(|step| matches!(step.state, StepState::Pending))
        && definition
            .prerequisites
            .iter()
            .all(|prerequisite| prerequisite_satisfied(state, prerequisite))
}

fn finish_if_terminal<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
{
    if !matches!(&reduction.state.workflow, WorkflowState::Executing { .. })
        || !reduction
            .state
            .steps
            .values()
            .all(|step| step.state.is_terminal())
    {
        return;
    }

    let Some(exports) = derive_exports(&reduction.state) else {
        return;
    };
    let (to, outcome) = match &reduction.state.workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => (WorkflowState::Succeeded, RunOutcome::Succeeded),
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { primary_failure },
        } => (
            WorkflowState::Failed {
                primary_failure: primary_failure.clone(),
                later_cancellation: None,
            },
            RunOutcome::Failed {
                primary_failure: primary_failure.clone(),
                later_cancellation: None,
            },
        ),
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_failure: Some(primary_failure),
                },
        } => (
            WorkflowState::Failed {
                primary_failure: primary_failure.clone(),
                later_cancellation: Some(*reason),
            },
            RunOutcome::Failed {
                primary_failure: primary_failure.clone(),
                later_cancellation: Some(*reason),
            },
        ),
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_failure: None,
                },
        } => (
            WorkflowState::Cancelled { reason: *reason },
            RunOutcome::Cancelled { reason: *reason },
        ),
        WorkflowState::Succeeded
        | WorkflowState::Failed { .. }
        | WorkflowState::Cancelled { .. } => return,
    };
    let from = reduction.state.workflow.clone();
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to.clone();
    reduction.state.exports = Some(exports.clone());
    reduction
        .events
        .push(TransitionEvent::Workflow { sequence, from, to });
    reduction.actions.push(RequestedAction {
        id: ActionId::for_transition(sequence),
        action: Action::FinishRun { outcome, exports },
    });
}

fn derive_exports<Cause, Output>(state: &RuntimeState<Cause, Output>) -> Option<ExportSet<Output>>
where
    Output: Clone,
{
    state
        .definition
        .exports
        .iter()
        .map(|(name, source)| {
            let step = state.steps.get(&source.step)?;
            let value = match &step.state {
                StepState::Succeeded { outputs } => ExportValue::Available {
                    output: outputs.get(&source.output)?.clone(),
                },
                StepState::Failed { .. } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::Failed,
                },
                StepState::Blocked { .. } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::Blocked,
                },
                StepState::NotRun { .. } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::NotRun,
                },
                StepState::Cancelled { .. } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::Cancelled,
                },
                StepState::Pending
                | StepState::Starting
                | StepState::Running
                | StepState::CapturingOutputs
                | StepState::Cancelling { .. } => return None,
            };
            Some((name.clone(), value))
        })
        .collect()
}

fn step_accepts<Cause, Output>(
    state: &RuntimeState<Cause, Output>,
    step: &str,
    expected_state: StepStateKind,
    action: ActionId,
) -> bool {
    state.steps.get(step).is_some_and(|runtime| {
        runtime.state.kind() == expected_state && runtime.current_action == Some(action)
    })
}

fn step_declares_outputs<Cause, Output>(state: &RuntimeState<Cause, Output>, step: &str) -> bool {
    state
        .definition
        .steps
        .get(step)
        .is_some_and(|definition| !definition.declared_outputs.is_empty())
}

fn outputs_match_declaration<Cause, Output>(
    state: &RuntimeState<Cause, Output>,
    step: &str,
    outputs: &OutputSet<Output>,
) -> bool {
    state
        .definition
        .steps
        .get(step)
        .is_some_and(|definition| outputs.keys().eq(definition.declared_outputs.iter()))
}

fn set_current_action<Cause, Output>(
    state: &mut RuntimeState<Cause, Output>,
    step: &str,
    action: ActionId,
) {
    if let Some(runtime) = state.steps.get_mut(step) {
        runtime.current_action = Some(action);
    }
}

fn transition_step<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output>,
    events: &mut Vec<TransitionEvent<Cause, Deadline>>,
    step: &str,
    to: StepState<Cause, Output>,
    current_action: Option<ActionId>,
) -> TransitionSequence {
    let sequence = next_sequence(state);
    let failure_policy = state
        .definition
        .steps
        .get(step)
        .map(|definition| definition.failure_policy)
        .unwrap_or_default();
    if let Some(runtime) = state.steps.get_mut(step) {
        let from = runtime.state.kind();
        let to_kind = to.kind();
        runtime.state = to;
        runtime.current_action = current_action;
        events.push(TransitionEvent::Step {
            sequence,
            step: step.to_owned(),
            failure_policy,
            from,
            to: to_kind,
        });
    }
    sequence
}

fn next_sequence<Cause, Output>(state: &mut RuntimeState<Cause, Output>) -> TransitionSequence {
    let sequence = state.last_transition_sequence.next();
    state.last_transition_sequence = sequence;
    sequence
}

#[cfg(test)]
mod tests;
