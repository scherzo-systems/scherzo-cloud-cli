use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::admission::{AdmittedWorkflow, CancellationOperationId, CancellationReason};
use super::document::{FailurePolicy, FinalizationTrigger};
use super::finalization_context::{
    self, FinalizationContext, OrdinaryIssue, OrdinaryIssueDisposition,
};
use super::validated::{
    ResolvedDirectPrerequisite, ResolvedValueSource, ValidatedCommonStep, ValidatedFinalizer,
    ValidatedMessageSource, ValidatedStep, ValidatedWorkflow, WorkflowNodeRole,
};

pub(crate) type OutputSet<Output> = BTreeMap<String, Output>;
pub(crate) type ExportSet<Output> = BTreeMap<String, ExportValue<Output>>;

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct TransitionSequence(pub(crate) u64);

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
    // Kept as the serialized scalar while allocation is no longer restricted to
    // transition identities. Force-containment actions use the disjoint high range.
    pub(crate) transition_sequence: TransitionSequence,
}

impl ActionId {
    fn for_transition(transition_sequence: TransitionSequence) -> Self {
        Self {
            transition_sequence,
        }
    }

    fn for_force_abort(operation: CancellationOperationId, index: usize) -> Self {
        let operation = operation.get() & 0x7fff_ffff;
        let index = u64::try_from(index).unwrap_or(u64::MAX) & 0xffff_ffff;
        Self {
            transition_sequence: TransitionSequence((1_u64 << 63) | (operation << 32) | index),
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
    pub(crate) role: WorkflowNodeRole,
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
pub(crate) enum FinalizationGate<Deadline = ()> {
    Open,
    Cancelling {
        reason: CancellationReason,
        deadline: Option<Deadline>,
        force_abort: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowState<Cause, Deadline = ()> {
    Executing {
        gate: SchedulingGate<Cause>,
    },
    Finalizing {
        trigger: FinalizationTrigger,
        gate: FinalizationGate<Deadline>,
        primary_failure: Option<StepFailure<Cause>>,
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

impl<Cause, Deadline> WorkflowState<Cause, Deadline> {
    pub(crate) fn map_deadline<Mapped>(
        self,
        map: impl FnOnce(Deadline) -> Mapped,
    ) -> WorkflowState<Cause, Mapped> {
        match self {
            Self::Executing { gate } => WorkflowState::Executing { gate },
            Self::Finalizing {
                trigger,
                gate,
                primary_failure,
            } => WorkflowState::Finalizing {
                trigger,
                gate: match gate {
                    FinalizationGate::Open => FinalizationGate::Open,
                    FinalizationGate::Cancelling {
                        reason,
                        deadline,
                        force_abort,
                    } => FinalizationGate::Cancelling {
                        reason,
                        deadline: deadline.map(map),
                        force_abort,
                    },
                },
                primary_failure,
            },
            Self::Succeeded => WorkflowState::Succeeded,
            Self::Failed {
                primary_failure,
                later_cancellation,
            } => WorkflowState::Failed {
                primary_failure,
                later_cancellation,
            },
            Self::Cancelled { reason } => WorkflowState::Cancelled { reason },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotRunReason {
    FailureStop,
    FinalizerTriggerNotSelected,
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
    InputUnavailable { references: Vec<String> },
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
            Self::Blocked { .. } | Self::InputUnavailable { .. } => StepStateKind::Blocked,
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
                | Self::InputUnavailable { .. }
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
    role: WorkflowNodeRole,
    failure_policy: FailurePolicy,
    prerequisites: Arc<[ResolvedDirectPrerequisite]>,
    inputs: BTreeMap<String, ResolvedValueSource>,
    declared_outputs: BTreeSet<String>,
    when: BTreeSet<FinalizationTrigger>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeExport {
    step: String,
    output: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeDefinition {
    steps: BTreeMap<String, RuntimeStep>,
    ordinary_ids: BTreeSet<String>,
    finalizer_ids: BTreeSet<String>,
    finalizer_presentation_order: Vec<String>,
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
        let ordinary_ids = workflow.steps.keys().cloned().collect();
        let finalizer_ids = workflow.finalizers.keys().cloned().collect();
        let steps = workflow
            .steps
            .iter()
            .map(|(id, step)| {
                (
                    id.clone(),
                    runtime_step(WorkflowNodeRole::Step, step, BTreeSet::new()),
                )
            })
            .chain(
                workflow
                    .finalizers
                    .iter()
                    .map(|(id, finalizer)| (id.clone(), runtime_finalizer(finalizer))),
            )
            .collect();
        let exports = workflow
            .exports
            .iter()
            .map(|(name, source)| {
                (
                    name.clone(),
                    RuntimeExport {
                        step: source.node.id.clone(),
                        output: source.output.clone(),
                    },
                )
            })
            .collect();
        Self {
            steps,
            ordinary_ids,
            finalizer_ids,
            finalizer_presentation_order: workflow.finalizer_presentation_order.clone(),
            exports,
            maximum_parallel_steps,
        }
    }
}

fn runtime_finalizer(finalizer: &ValidatedFinalizer) -> RuntimeStep {
    runtime_step(
        WorkflowNodeRole::Finalizer,
        &finalizer.body,
        finalizer.when.clone(),
    )
}

fn runtime_step(
    role: WorkflowNodeRole,
    step: &ValidatedStep,
    when: BTreeSet<FinalizationTrigger>,
) -> RuntimeStep {
    let common = common_step(step);
    RuntimeStep {
        role,
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
                    ValidatedMessageSource::Reference { source, .. } => match source {
                        ResolvedValueSource::Output(output) => {
                            Some((output.reference(), source.clone()))
                        }
                        ResolvedValueSource::FinalizationContext => Some((
                            "finalization.context".to_owned(),
                            ResolvedValueSource::FinalizationContext,
                        )),
                        ResolvedValueSource::Import(_) => None,
                    },
                    ValidatedMessageSource::File { .. } => None,
                })
                .collect(),
        },
        declared_outputs: common.outputs.keys().cloned().collect(),
        when,
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
    InputUnavailable,
    NotRun,
    TriggerNotSelected,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExportValue<Output> {
    Available { output: Output },
    Unavailable { reason: ExportUnavailableReason },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OrdinaryOutcome<Cause> {
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
pub(crate) struct FinalizationCancellation<Deadline = ()> {
    pub(crate) reason: CancellationReason,
    pub(crate) deadline: Option<Deadline>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FinalizerResult<Cause> {
    pub(crate) finalizer: String,
    pub(crate) failure_policy: FailurePolicy,
    pub(crate) disposition: StepState<Cause, ()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FinalizationSummary<Cause, Deadline = ()> {
    pub(crate) trigger: FinalizationTrigger,
    pub(crate) finalizers: Vec<FinalizerResult<Cause>>,
    pub(crate) cancellation: Option<FinalizationCancellation<Deadline>>,
    pub(crate) force_abort: bool,
}

// Mapping deadlines mirrors WorkflowState because summaries outlive transitions and are
// projected independently; a shared trait would obscure these two closed contracts.
// jscpd:ignore-start
impl<Cause, Deadline> FinalizationSummary<Cause, Deadline> {
    pub(crate) fn map_deadline<Mapped>(
        self,
        map: impl FnOnce(Deadline) -> Mapped,
    ) -> FinalizationSummary<Cause, Mapped> {
        FinalizationSummary {
            trigger: self.trigger,
            finalizers: self.finalizers,
            cancellation: self
                .cancellation
                .map(|cancellation| FinalizationCancellation {
                    reason: cancellation.reason,
                    deadline: cancellation.deadline.map(map),
                }),
            force_abort: self.force_abort,
        }
    }
}
// jscpd:ignore-end

#[derive(Clone, Debug, Eq, PartialEq)]
struct FinalizationRuntime<Cause, Deadline> {
    trigger: FinalizationTrigger,
    context: Arc<[u8]>,
    ordinary_outcome: OrdinaryOutcome<Cause>,
    cancellation: Option<FinalizationCancellation<Deadline>>,
    force_abort: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeState<Cause, Output, Deadline = ()> {
    definition: Arc<RuntimeDefinition>,
    pub(crate) workflow: WorkflowState<Cause, Deadline>,
    // One namespace and one reducer map; role is retained in the definition and events.
    pub(crate) steps: BTreeMap<String, StepRuntimeState<Cause, Output>>,
    pub(crate) exports: Option<ExportSet<Output>>,
    pub(crate) finalization_summary: Option<FinalizationSummary<Cause, Deadline>>,
    finalization: Option<FinalizationRuntime<Cause, Deadline>>,
    pub(crate) last_cancellation_operation: Option<CancellationOperationId>,
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
    CancellationOperationRequested {
        operation: CancellationOperationId,
        reason: CancellationReason,
        deadline: Deadline,
    },
    ForceAbortRequested {
        operation: CancellationOperationId,
        deadline: Deadline,
    },
    StepQuiesced {
        step: String,
        action: ActionId,
    },
}

// Runtime and terminal outcomes intentionally remain distinct: the former exists only while
// crossing the phase boundary, while the latter is the adapter-facing finish contract.
// jscpd:ignore-start
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
// jscpd:ignore-end

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ActionInput<Output> {
    Import,
    Output(Output),
    FinalizationContext(Arc<[u8]>),
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
    ForceAbortStep {
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
        role: WorkflowNodeRole,
        failure_policy: FailurePolicy,
        from: StepStateKind,
        to: StepStateKind,
    },
    Workflow {
        sequence: TransitionSequence,
        from: WorkflowState<Cause, Deadline>,
        to: WorkflowState<Cause, Deadline>,
    },
    CancellationAccepted {
        sequence: TransitionSequence,
        reason: CancellationReason,
        deadline: Deadline,
    },
    FinalizationCancellationAccepted {
        sequence: TransitionSequence,
        reason: CancellationReason,
        deadline: Deadline,
    },
    ForceAbortAccepted {
        sequence: TransitionSequence,
        reason: CancellationReason,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Reduction<Provisional, Cause, Output, Deadline> {
    pub(crate) state: RuntimeState<Cause, Output, Deadline>,
    pub(crate) events: Vec<TransitionEvent<Cause, Deadline>>,
    pub(crate) actions: Vec<RequestedAction<Provisional, Cause, Output, Deadline>>,
    pub(crate) occurrence_accepted: bool,
}

pub(crate) const fn maximum_transition_count(step_count: usize, finalizer_count: usize) -> usize {
    if finalizer_count == 0 {
        5 * step_count + 3
    } else {
        5 * (step_count + finalizer_count) + 6
    }
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
    initialize_with_operation(admitted, initial_cancellation, None)
}

pub(super) fn initialize_with_operation<Provisional, Cause, Output, Deadline>(
    admitted: &AdmittedWorkflow,
    initial_cancellation: Option<CancellationRequest<Deadline>>,
    initial_cancellation_operation: Option<CancellationOperationId>,
) -> Reduction<Provisional, Cause, Output, Deadline>
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    initialize_definition(ExecutionStart {
        definition: RuntimeDefinition::from_admitted(admitted),
        initial_cancellation,
        initial_cancellation_operation,
    })
}

struct ExecutionStart<Deadline> {
    definition: RuntimeDefinition,
    initial_cancellation: Option<CancellationRequest<Deadline>>,
    initial_cancellation_operation: Option<CancellationOperationId>,
}

fn initialize_definition<Provisional, Cause, Output, Deadline>(
    start: ExecutionStart<Deadline>,
) -> Reduction<Provisional, Cause, Output, Deadline>
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let steps = start
        .definition
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
            definition: Arc::new(start.definition),
            workflow: WorkflowState::Executing {
                gate: SchedulingGate::Open,
            },
            steps,
            exports: None,
            finalization_summary: None,
            finalization: None,
            last_cancellation_operation: None,
            last_transition_sequence: TransitionSequence::default(),
        },
        events: Vec::new(),
        actions: Vec::new(),
        occurrence_accepted: true,
    };
    if let Some(cancellation) = start.initial_cancellation {
        apply_cancellation(
            &mut reduction,
            cancellation,
            start.initial_cancellation_operation,
        );
    }
    stabilize(&mut reduction);
    reduction
}

pub(crate) fn reduce<Provisional, Cause, Output, Deadline>(
    current: &RuntimeState<Cause, Output, Deadline>,
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
    if matches!(
        &reduction.state.workflow,
        WorkflowState::Succeeded | WorkflowState::Failed { .. } | WorkflowState::Cancelled { .. }
    ) || !apply_occurrence(&mut reduction, occurrence)
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
            transition_step(reduction, &step, StepState::Running, Some(action));
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
            let sequence = transition_step(reduction, &step, StepState::CapturingOutputs, None);
            if step_declares_outputs(&reduction.state, &step) {
                let capture_action = ActionId::for_transition(sequence);
                set_current_action(&mut reduction.state, &step, capture_action);
                reduction.actions.push(RequestedAction {
                    id: capture_action,
                    action: Action::CaptureOutputs { step, provisional },
                });
            } else {
                transition_step(
                    reduction,
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
            transition_step(reduction, &step, StepState::Succeeded { outputs }, None);
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
            if reason == CancellationReason::FinalizationForceAbort {
                return false;
            }
            return apply_cancellation(reduction, CancellationRequest { reason, deadline }, None);
        }
        Occurrence::CancellationOperationRequested {
            operation,
            reason,
            deadline,
        } => {
            if reason == CancellationReason::FinalizationForceAbort
                || stale_operation(&reduction.state, operation)
            {
                return false;
            }
            return apply_cancellation(
                reduction,
                CancellationRequest { reason, deadline },
                Some(operation),
            );
        }
        Occurrence::ForceAbortRequested {
            operation,
            deadline,
        } => {
            if stale_operation(&reduction.state, operation) {
                return false;
            }
            return apply_force_abort(reduction, operation, deadline);
        }
        Occurrence::StepQuiesced { step, action } => {
            let Some(reason) = cancelling_step_reason(&reduction.state, &step, action) else {
                return false;
            };
            transition_step(reduction, &step, StepState::Cancelled { reason }, None);
        }
    }
    true
}

fn stale_operation<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    operation: CancellationOperationId,
) -> bool {
    state
        .last_cancellation_operation
        .is_some_and(|last| operation <= last)
}

// Ordinary and finalization cancellation deliberately retain separate gates and events;
// their matching dispatch shapes make the phase boundary explicit rather than generic.
// jscpd:ignore-start
fn apply_cancellation<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    cancellation: CancellationRequest<Deadline>,
    operation: Option<CancellationOperationId>,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    match &reduction.state.workflow {
        WorkflowState::Executing { .. } => {
            apply_ordinary_cancellation(reduction, cancellation, operation)
        }
        WorkflowState::Finalizing {
            gate: FinalizationGate::Open,
            ..
        } => apply_finalization_cancellation(reduction, cancellation, operation),
        WorkflowState::Finalizing {
            gate: FinalizationGate::Cancelling { .. },
            ..
        }
        | WorkflowState::Succeeded
        | WorkflowState::Failed { .. }
        | WorkflowState::Cancelled { .. } => false,
    }
}

fn apply_ordinary_cancellation<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    cancellation: CancellationRequest<Deadline>,
    operation: Option<CancellationOperationId>,
) -> bool
where
    Cause: Clone,
    Output: Clone,
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
        } => return false,
        _ => return false,
    };
    if let Some(operation) = operation {
        reduction.state.last_cancellation_operation = Some(operation);
    }
    reduction.state.workflow = WorkflowState::Executing {
        gate: SchedulingGate::Cancelling {
            reason: cancellation.reason,
            prior_failure,
        },
    };
    let sequence = next_sequence(&mut reduction.state);
    reduction
        .events
        .push(TransitionEvent::CancellationAccepted {
            sequence,
            reason: cancellation.reason,
            deadline: cancellation.deadline.clone(),
        });
    cancel_nodes(
        reduction,
        WorkflowNodeRole::Step,
        cancellation.reason,
        Some(cancellation.deadline),
        false,
        operation,
    );
    true
}

fn apply_finalization_cancellation<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    cancellation: CancellationRequest<Deadline>,
    operation: Option<CancellationOperationId>,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if let Some(operation) = operation {
        reduction.state.last_cancellation_operation = Some(operation);
    }
    let from = reduction.state.workflow.clone();
    let WorkflowState::Finalizing {
        trigger,
        primary_failure,
        ..
    } = &from
    else {
        return false;
    };
    let to = WorkflowState::Finalizing {
        trigger: *trigger,
        gate: FinalizationGate::Cancelling {
            reason: cancellation.reason,
            deadline: Some(cancellation.deadline.clone()),
            force_abort: false,
        },
        primary_failure: primary_failure.clone(),
    };
    reduction.state.workflow = to;
    if let Some(finalization) = reduction.state.finalization.as_mut() {
        finalization.cancellation = Some(FinalizationCancellation {
            reason: cancellation.reason,
            deadline: Some(cancellation.deadline.clone()),
        });
    }
    let sequence = next_sequence(&mut reduction.state);
    reduction
        .events
        .push(TransitionEvent::FinalizationCancellationAccepted {
            sequence,
            reason: cancellation.reason,
            deadline: cancellation.deadline.clone(),
        });
    cancel_nodes(
        reduction,
        WorkflowNodeRole::Finalizer,
        cancellation.reason,
        Some(cancellation.deadline),
        false,
        operation,
    );
    true
}

// jscpd:ignore-end
fn apply_force_abort<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    operation: CancellationOperationId,
    deadline: Deadline,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let WorkflowState::Finalizing {
        trigger,
        gate,
        primary_failure,
    } = &reduction.state.workflow
    else {
        return false;
    };
    if matches!(
        gate,
        FinalizationGate::Cancelling {
            force_abort: true,
            ..
        }
    ) {
        return false;
    }
    let trigger = *trigger;
    let primary_failure = primary_failure.clone();
    let (reason, phase_deadline) = match gate {
        FinalizationGate::Open => (CancellationReason::FinalizationForceAbort, None),
        FinalizationGate::Cancelling {
            reason, deadline, ..
        } => (*reason, deadline.clone()),
    };
    reduction.state.last_cancellation_operation = Some(operation);
    reduction.state.workflow = WorkflowState::Finalizing {
        trigger,
        gate: FinalizationGate::Cancelling {
            reason,
            deadline: phase_deadline,
            force_abort: true,
        },
        primary_failure,
    };
    if let Some(finalization) = reduction.state.finalization.as_mut() {
        finalization.force_abort = true;
        if finalization.cancellation.is_none() {
            finalization.cancellation = Some(FinalizationCancellation {
                reason,
                deadline: None,
            });
        }
    }
    let sequence = next_sequence(&mut reduction.state);
    reduction
        .events
        .push(TransitionEvent::ForceAbortAccepted { sequence, reason });
    cancel_nodes(
        reduction,
        WorkflowNodeRole::Finalizer,
        reason,
        Some(deadline),
        true,
        Some(operation),
    );
    true
}

fn cancel_nodes<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    role: WorkflowNodeRole,
    reason: CancellationReason,
    deadline: Option<Deadline>,
    force: bool,
    operation: Option<CancellationOperationId>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let force_context = match (force, operation, deadline.clone()) {
        (true, Some(operation), Some(deadline)) => Some((operation, deadline)),
        (true, _, _) => return,
        (false, _, _) => None,
    };
    let graceful_deadline = if force { None } else { deadline };
    let nodes = reduction
        .state
        .definition
        .steps
        .iter()
        .filter(|(_, definition)| definition.role == role)
        .filter_map(|(id, _)| {
            reduction
                .state
                .steps
                .get(id)
                .map(|runtime| (id.clone(), runtime.state.kind()))
        })
        .collect::<Vec<_>>();
    let mut containment_index = 1;
    for (step, state) in nodes {
        match state {
            StepStateKind::Pending => {
                transition_step(reduction, &step, StepState::Cancelled { reason }, None);
            }
            StepStateKind::Starting | StepStateKind::Running | StepStateKind::CapturingOutputs => {
                let sequence =
                    transition_step(reduction, &step, StepState::Cancelling { reason }, None);
                let action = force_context.as_ref().map_or_else(
                    || ActionId::for_transition(sequence),
                    |(operation, _)| ActionId::for_force_abort(*operation, containment_index),
                );
                containment_index += 1;
                set_current_action(&mut reduction.state, &step, action);
                let requested = match &force_context {
                    Some((_, deadline)) => Action::ForceAbortStep {
                        step,
                        reason,
                        deadline: deadline.clone(),
                    },
                    None => {
                        let Some(deadline) = graceful_deadline.clone() else {
                            continue;
                        };
                        Action::CancelStep {
                            step,
                            reason,
                            deadline,
                        }
                    }
                };
                reduction.actions.push(RequestedAction {
                    id: action,
                    action: requested,
                });
            }
            StepStateKind::Cancelling if force => {
                let Some((operation, deadline)) = &force_context else {
                    continue;
                };
                let action = ActionId::for_force_abort(*operation, containment_index);
                containment_index += 1;
                set_current_action(&mut reduction.state, &step, action);
                reduction.actions.push(RequestedAction {
                    id: action,
                    action: Action::ForceAbortStep {
                        step,
                        reason,
                        deadline: deadline.clone(),
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
}

fn cancelling_step_reason<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
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
    Output: Clone,
    Deadline: Clone,
{
    if !step_accepts(&reduction.state, &step, expected_state, action) {
        return false;
    }
    let Some(definition) = reduction.state.definition.steps.get(&step).cloned() else {
        return false;
    };
    let primary_failure = StepFailure {
        step: step.clone(),
        role: definition.role,
        phase,
        cause: cause.clone(),
    };
    transition_step(reduction, &step, StepState::Failed { phase, cause }, None);
    if definition.failure_policy == FailurePolicy::Required {
        match definition.role {
            WorkflowNodeRole::Step => close_ordinary_gate_for_failure(reduction, primary_failure),
            WorkflowNodeRole::Finalizer => {
                select_finalizer_primary_failure(reduction, primary_failure)
            }
        }
    }
    true
}

// Primary failure selection is phase-specific even though both transitions carry the same
// surrounding workflow fields; keeping them separate makes ordinary precedence explicit.
// jscpd:ignore-start
fn close_ordinary_gate_for_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    primary_failure: StepFailure<Cause>,
) where
    Cause: Clone,
    Deadline: Clone,
{
    if !matches!(
        &reduction.state.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::Open
        }
    ) {
        return;
    }
    let from = reduction.state.workflow.clone();
    let to = WorkflowState::Executing {
        gate: SchedulingGate::FailureStopped { primary_failure },
    };
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to.clone();
    reduction
        .events
        .push(TransitionEvent::Workflow { sequence, from, to });
}

fn select_finalizer_primary_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    primary_failure: StepFailure<Cause>,
) where
    Cause: Clone,
    Deadline: Clone,
{
    let WorkflowState::Finalizing {
        trigger,
        gate: FinalizationGate::Open,
        primary_failure: None,
    } = &reduction.state.workflow
    else {
        return;
    };
    if *trigger != FinalizationTrigger::Succeeded {
        return;
    }
    let from = reduction.state.workflow.clone();
    let to = WorkflowState::Finalizing {
        trigger: *trigger,
        gate: FinalizationGate::Open,
        primary_failure: Some(primary_failure),
    };
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to.clone();
    reduction
        .events
        .push(TransitionEvent::Workflow { sequence, from, to });
}

// jscpd:ignore-end
fn stabilize<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    loop {
        let before = reduction.state.last_transition_sequence;
        match &reduction.state.workflow {
            WorkflowState::Executing { .. } => {
                propagate_ordinary_pending_dispositions(reduction);
                select_ready_nodes(reduction, WorkflowNodeRole::Step);
                enter_finalization_or_finish(reduction);
            }
            WorkflowState::Finalizing { .. } => {
                propagate_finalizer_dispositions(reduction);
                select_ready_nodes(reduction, WorkflowNodeRole::Finalizer);
                finish_finalization_if_terminal(reduction);
            }
            WorkflowState::Succeeded
            | WorkflowState::Failed { .. }
            | WorkflowState::Cancelled { .. } => {}
        }
        if reduction.state.last_transition_sequence == before {
            break;
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PendingDisposition {
    Blocked { dependency: String },
    NotRun,
}

fn propagate_ordinary_pending_dispositions<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    while let Some((step, disposition)) = next_ordinary_pending_disposition(&reduction.state) {
        let state = match disposition {
            PendingDisposition::Blocked { dependency } => StepState::Blocked { dependency },
            PendingDisposition::NotRun => StepState::NotRun {
                reason: NotRunReason::FailureStop,
            },
        };
        transition_step(reduction, &step, state, None);
    }
}

fn next_ordinary_pending_disposition<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Option<(String, PendingDisposition)> {
    let failure_stopped = matches!(
        &state.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { .. }
        }
    );
    state.definition.ordinary_ids.iter().find_map(|step_id| {
        let definition = state.definition.steps.get(step_id)?;
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
                    && !ordinary_prerequisite_satisfied(state, prerequisite)
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
                .all(|prerequisite| ordinary_prerequisite_satisfied(state, prerequisite)))
        .then(|| (step_id.clone(), PendingDisposition::NotRun))
    })
}

fn ordinary_prerequisite_satisfied<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
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
                StepState::Failed { .. }
                    | StepState::Blocked { .. }
                    | StepState::InputUnavailable { .. }
            ));
    (!prerequisite.control || control_satisfied) && (!prerequisite.data || succeeded)
}

fn propagate_finalizer_dispositions<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let Some(trigger) = reduction
        .state
        .finalization
        .as_ref()
        .map(|finalization| finalization.trigger)
    else {
        return;
    };
    let not_selected = reduction
        .state
        .definition
        .finalizer_ids
        .iter()
        .filter(|id| {
            reduction
                .state
                .steps
                .get(*id)
                .is_some_and(|runtime| matches!(runtime.state, StepState::Pending))
                && reduction
                    .state
                    .definition
                    .steps
                    .get(*id)
                    .is_some_and(|definition| !definition.when.contains(&trigger))
        })
        .cloned()
        .collect::<Vec<_>>();
    for finalizer in not_selected {
        transition_step(
            reduction,
            &finalizer,
            StepState::NotRun {
                reason: NotRunReason::FinalizerTriggerNotSelected,
            },
            None,
        );
    }

    while let Some((finalizer, references)) = next_input_unavailable(&reduction.state) {
        transition_step(
            reduction,
            &finalizer,
            StepState::InputUnavailable { references },
            None,
        );
    }
}

fn next_input_unavailable<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Option<(String, Vec<String>)> {
    state.definition.finalizer_ids.iter().find_map(|id| {
        let definition = state.definition.steps.get(id)?;
        if !finalizer_ready(state, id, definition) {
            return None;
        }
        let references = unavailable_references(state, definition);
        (!references.is_empty()).then(|| (id.clone(), references))
    })
}

fn unavailable_references<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    definition: &RuntimeStep,
) -> Vec<String> {
    definition
        .inputs
        .values()
        .filter_map(|source| match source {
            ResolvedValueSource::Output(source)
                if state
                    .steps
                    .get(&source.node.id)
                    .and_then(|runtime| match &runtime.state {
                        StepState::Succeeded { outputs } => outputs.get(&source.output),
                        _ => None,
                    })
                    .is_none() =>
            {
                Some(source.reference())
            }
            ResolvedValueSource::Import(_)
            | ResolvedValueSource::FinalizationContext
            | ResolvedValueSource::Output(_) => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn select_ready_nodes<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    role: WorkflowNodeRole,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let gate_open = matches!(
        (&reduction.state.workflow, role),
        (
            WorkflowState::Executing {
                gate: SchedulingGate::Open,
            },
            WorkflowNodeRole::Step,
        ) | (
            WorkflowState::Finalizing {
                gate: FinalizationGate::Open,
                ..
            },
            WorkflowNodeRole::Finalizer,
        )
    );
    if !gate_open {
        return;
    }
    let active = reduction
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
        .saturating_sub(active);
    let ids = match role {
        WorkflowNodeRole::Step => &reduction.state.definition.ordinary_ids,
        WorkflowNodeRole::Finalizer => &reduction.state.definition.finalizer_ids,
    };
    let selected = ids
        .iter()
        .filter_map(|id| {
            let definition = reduction.state.definition.steps.get(id)?;
            let ready = match role {
                WorkflowNodeRole::Step => ordinary_ready(&reduction.state, id, definition),
                WorkflowNodeRole::Finalizer => {
                    finalizer_ready(&reduction.state, id, definition)
                        && unavailable_references(&reduction.state, definition).is_empty()
                }
            };
            ready.then(|| {
                (
                    id.clone(),
                    resolved_action_inputs(&reduction.state, definition),
                )
            })
        })
        .take(available_slots)
        .collect::<Vec<_>>();
    for (step, inputs) in selected {
        let sequence = transition_step(reduction, &step, StepState::Starting, None);
        let action_id = ActionId::for_transition(sequence);
        set_current_action(&mut reduction.state, &step, action_id);
        reduction.actions.push(RequestedAction {
            id: action_id,
            action: Action::StartStep { step, inputs },
        });
    }
}

fn resolved_action_inputs<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
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
                ResolvedValueSource::FinalizationContext => state
                    .finalization
                    .as_ref()
                    .map(|finalization| {
                        ActionInput::FinalizationContext(Arc::clone(&finalization.context))
                    })
                    .unwrap_or(ActionInput::Unavailable),
                ResolvedValueSource::Output(source) => state
                    .steps
                    .get(&source.node.id)
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

fn ordinary_ready<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
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
            .all(|prerequisite| ordinary_prerequisite_satisfied(state, prerequisite))
}

fn finalizer_ready<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    id: &str,
    definition: &RuntimeStep,
) -> bool {
    let Some(finalization) = &state.finalization else {
        return false;
    };
    state
        .steps
        .get(id)
        .is_some_and(|step| matches!(step.state, StepState::Pending))
        && definition.when.contains(&finalization.trigger)
        && definition.prerequisites.iter().all(|prerequisite| {
            state
                .steps
                .get(&prerequisite.producer)
                .is_some_and(|producer| producer.state.is_terminal())
        })
}

fn enter_finalization_or_finish<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if !matches!(&reduction.state.workflow, WorkflowState::Executing { .. })
        || !reduction.state.definition.ordinary_ids.iter().all(|id| {
            reduction
                .state
                .steps
                .get(id)
                .is_some_and(|step| step.state.is_terminal())
        })
    {
        return;
    }
    let ordinary_outcome = ordinary_outcome(&reduction.state);
    if reduction.state.definition.finalizer_ids.is_empty() {
        finish_run(reduction, ordinary_outcome, None);
        return;
    }
    let trigger = match &ordinary_outcome {
        OrdinaryOutcome::Succeeded => FinalizationTrigger::Succeeded,
        OrdinaryOutcome::Failed { .. } => FinalizationTrigger::Failed,
        OrdinaryOutcome::Cancelled { .. } => FinalizationTrigger::Cancelled,
    };
    let ordinary_issues = ordinary_issues(&reduction.state);
    let primary_failure_step_id = match &ordinary_outcome {
        OrdinaryOutcome::Failed {
            primary_failure, ..
        } => Some(primary_failure.step.as_str()),
        OrdinaryOutcome::Succeeded | OrdinaryOutcome::Cancelled { .. } => None,
    };
    let ordinary_cancellation = match &ordinary_outcome {
        OrdinaryOutcome::Failed {
            later_cancellation, ..
        } => *later_cancellation,
        OrdinaryOutcome::Cancelled { reason } => Some(*reason),
        OrdinaryOutcome::Succeeded => None,
    };
    let context = finalization_context::serialize(FinalizationContext {
        trigger,
        primary_failure_step_id,
        cancellation_reason: ordinary_cancellation,
        ordinary_issues: &ordinary_issues,
    });
    let primary_failure = match &ordinary_outcome {
        OrdinaryOutcome::Failed {
            primary_failure, ..
        } => Some(primary_failure.clone()),
        OrdinaryOutcome::Succeeded | OrdinaryOutcome::Cancelled { .. } => None,
    };
    let from = reduction.state.workflow.clone();
    let to = WorkflowState::Finalizing {
        trigger,
        gate: FinalizationGate::Open,
        primary_failure,
    };
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to.clone();
    reduction.state.finalization = Some(FinalizationRuntime {
        trigger,
        context,
        ordinary_outcome,
        cancellation: None,
        force_abort: false,
    });
    reduction
        .events
        .push(TransitionEvent::Workflow { sequence, from, to });
}

// Finalization entry and ordinary outcome composition both enumerate the closed ordinary
// outcomes, but they own different state changes and should not share a partial mapper.
// jscpd:ignore-start
fn ordinary_outcome<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> OrdinaryOutcome<Cause>
where
    Cause: Clone,
{
    match &state.workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => OrdinaryOutcome::Succeeded,
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { primary_failure },
        } => OrdinaryOutcome::Failed {
            primary_failure: primary_failure.clone(),
            later_cancellation: None,
        },
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_failure: Some(primary_failure),
                },
        } => OrdinaryOutcome::Failed {
            primary_failure: primary_failure.clone(),
            later_cancellation: Some(*reason),
        },
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_failure: None,
                },
        } => OrdinaryOutcome::Cancelled { reason: *reason },
        _ => unreachable!("ordinary outcome is derived only at ordinary quiescence"),
    }
}

// jscpd:ignore-end
fn ordinary_issues<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Vec<OrdinaryIssue> {
    state
        .definition
        .ordinary_ids
        .iter()
        .filter_map(|id| {
            let runtime = state.steps.get(id)?;
            let disposition = match runtime.state {
                StepState::Failed { .. } => OrdinaryIssueDisposition::Failed,
                StepState::Blocked { .. } => OrdinaryIssueDisposition::Blocked,
                _ => return None,
            };
            Some(OrdinaryIssue {
                step_id: id.clone(),
                failure_policy: state.definition.steps.get(id)?.failure_policy,
                disposition,
            })
        })
        .collect()
}

// Finalization and ordinary completion retain distinct quiescence predicates; the similar
// guards are clearer than an abstraction parameterized by role and phase.
// jscpd:ignore-start
fn finish_finalization_if_terminal<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if !matches!(&reduction.state.workflow, WorkflowState::Finalizing { .. })
        || !reduction.state.definition.finalizer_ids.iter().all(|id| {
            reduction
                .state
                .steps
                .get(id)
                .is_some_and(|step| step.state.is_terminal())
        })
    {
        return;
    }
    let Some(finalization) = reduction.state.finalization.clone() else {
        return;
    };
    let summary = finalization_summary(&reduction.state, &finalization);
    let outcome = compose_final_outcome(&reduction.state, &finalization);
    finish_run(reduction, outcome, Some(summary));
}

// jscpd:ignore-end
fn finalization_summary<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    finalization: &FinalizationRuntime<Cause, Deadline>,
) -> FinalizationSummary<Cause, Deadline>
where
    Cause: Clone,
    Deadline: Clone,
{
    let finalizers = state
        .definition
        .finalizer_presentation_order
        .iter()
        .filter_map(|id| {
            let runtime = state.steps.get(id)?;
            let disposition = erase_outputs(&runtime.state)?;
            Some(FinalizerResult {
                finalizer: id.clone(),
                failure_policy: state.definition.steps.get(id)?.failure_policy,
                disposition,
            })
        })
        .collect();
    FinalizationSummary {
        trigger: finalization.trigger,
        finalizers,
        cancellation: finalization.cancellation.clone(),
        force_abort: finalization.force_abort,
    }
}

fn erase_outputs<Cause: Clone, Output>(
    state: &StepState<Cause, Output>,
) -> Option<StepState<Cause, ()>> {
    Some(match state {
        StepState::Succeeded { .. } => StepState::Succeeded {
            outputs: BTreeMap::new(),
        },
        StepState::Failed { phase, cause } => StepState::Failed {
            phase: *phase,
            cause: cause.clone(),
        },
        StepState::Blocked { dependency } => StepState::Blocked {
            dependency: dependency.clone(),
        },
        StepState::InputUnavailable { references } => StepState::InputUnavailable {
            references: references.clone(),
        },
        StepState::NotRun { reason } => StepState::NotRun { reason: *reason },
        StepState::Cancelled { reason } => StepState::Cancelled { reason: *reason },
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Cancelling { .. } => return None,
    })
}

fn compose_final_outcome<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    finalization: &FinalizationRuntime<Cause, Deadline>,
) -> OrdinaryOutcome<Cause>
where
    Cause: Clone,
{
    match &finalization.ordinary_outcome {
        OrdinaryOutcome::Failed {
            primary_failure,
            later_cancellation,
        } => OrdinaryOutcome::Failed {
            primary_failure: primary_failure.clone(),
            later_cancellation: later_cancellation.or_else(|| {
                finalization
                    .cancellation
                    .as_ref()
                    .map(|record| record.reason)
            }),
        },
        OrdinaryOutcome::Cancelled { reason } => OrdinaryOutcome::Cancelled { reason: *reason },
        OrdinaryOutcome::Succeeded => match &state.workflow {
            WorkflowState::Finalizing {
                primary_failure: Some(primary_failure),
                ..
            } => OrdinaryOutcome::Failed {
                primary_failure: primary_failure.clone(),
                later_cancellation: finalization
                    .cancellation
                    .as_ref()
                    .map(|record| record.reason),
            },
            WorkflowState::Finalizing {
                gate: FinalizationGate::Cancelling { reason, .. },
                primary_failure: None,
                ..
            } => OrdinaryOutcome::Cancelled { reason: *reason },
            _ => OrdinaryOutcome::Succeeded,
        },
    }
}

fn finish_run<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    ordinary_outcome: OrdinaryOutcome<Cause>,
    summary: Option<FinalizationSummary<Cause, Deadline>>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let Some(exports) = derive_exports(&reduction.state) else {
        return;
    };
    let (to, outcome) = match ordinary_outcome {
        OrdinaryOutcome::Succeeded => (WorkflowState::Succeeded, RunOutcome::Succeeded),
        OrdinaryOutcome::Failed {
            primary_failure,
            later_cancellation,
        } => (
            WorkflowState::Failed {
                primary_failure: primary_failure.clone(),
                later_cancellation,
            },
            RunOutcome::Failed {
                primary_failure,
                later_cancellation,
            },
        ),
        OrdinaryOutcome::Cancelled { reason } => (
            WorkflowState::Cancelled { reason },
            RunOutcome::Cancelled { reason },
        ),
    };
    let from = reduction.state.workflow.clone();
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to.clone();
    reduction.state.exports = Some(exports.clone());
    reduction.state.finalization_summary = summary.clone();
    reduction
        .events
        .push(TransitionEvent::Workflow { sequence, from, to });
    reduction.actions.push(RequestedAction {
        id: ActionId::for_transition(sequence),
        action: Action::FinishRun { outcome, exports },
    });
}

fn derive_exports<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Option<ExportSet<Output>>
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
                StepState::InputUnavailable { .. } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::InputUnavailable,
                },
                StepState::NotRun {
                    reason: NotRunReason::FinalizerTriggerNotSelected,
                } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::TriggerNotSelected,
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

fn step_accepts<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
    expected_state: StepStateKind,
    action: ActionId,
) -> bool {
    state.steps.get(step).is_some_and(|runtime| {
        runtime.state.kind() == expected_state && runtime.current_action == Some(action)
    })
}

fn step_declares_outputs<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
) -> bool {
    state
        .definition
        .steps
        .get(step)
        .is_some_and(|definition| !definition.declared_outputs.is_empty())
}

fn outputs_match_declaration<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
    outputs: &OutputSet<Output>,
) -> bool {
    state
        .definition
        .steps
        .get(step)
        .is_some_and(|definition| outputs.keys().eq(definition.declared_outputs.iter()))
}

fn set_current_action<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
    action: ActionId,
) {
    if let Some(runtime) = state.steps.get_mut(step) {
        runtime.current_action = Some(action);
    }
}

fn transition_step<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: &str,
    to: StepState<Cause, Output>,
    current_action: Option<ActionId>,
) -> TransitionSequence {
    let sequence = next_sequence(&mut reduction.state);
    let definition = reduction.state.definition.steps.get(step);
    let failure_policy = definition
        .map(|definition| definition.failure_policy)
        .unwrap_or_default();
    let role = definition
        .map(|definition| definition.role)
        .unwrap_or(WorkflowNodeRole::Step);
    if let Some(runtime) = reduction.state.steps.get_mut(step) {
        let from = runtime.state.kind();
        let to_kind = to.kind();
        runtime.state = to;
        runtime.current_action = current_action;
        reduction.events.push(TransitionEvent::Step {
            sequence,
            step: step.to_owned(),
            role,
            failure_policy,
            from,
            to: to_kind,
        });
    }
    sequence
}

fn next_sequence<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
) -> TransitionSequence {
    let sequence = state.last_transition_sequence.next();
    state.last_transition_sequence = sequence;
    sequence
}

#[cfg(test)]
mod tests;
