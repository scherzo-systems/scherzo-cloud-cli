use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::admission::{AdmittedWorkflow, CancellationOperationId, CancellationReason};
use super::document::{FailurePolicy, FinalizationTrigger};
pub(crate) use super::evidence::FailurePhase;
use super::evidence::{
    BlockedDetail, CancellationDetail, FailureDetail, NodeFailureSource, NonExecutionCode,
    NonExecutionDetail, Prerequisite, PrimaryIssue,
};
use super::finalization_context::{
    self, FinalizationContext, OrdinaryIssue, OrdinaryIssueDisposition,
};
use super::validated::{
    ResolvedDirectPrerequisite, ResolvedValueSource, ValidatedCommonStep, ValidatedFinalizer,
    ValidatedMessageSource, ValidatedRecoveryHandler, ValidatedStep, ValidatedStepRecovery,
    ValidatedWorkflow, WorkflowNode, WorkflowNodeRole,
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
        Self(self.0.saturating_add(1))
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

    fn is_force_abort(self) -> bool {
        self.transition_sequence.get() & (1_u64 << 63) != 0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct TargetExecutionNumber(u8);

impl TargetExecutionNumber {
    pub(crate) const FIRST: Self = Self(1);

    pub(crate) const fn get(self) -> u8 {
        self.0
    }

    #[cfg(test)]
    pub(crate) const fn fixture(value: u8) -> Self {
        Self(value)
    }

    fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct RecoveryRoundNumber(u8);

// Recovery rounds and target executions deliberately remain distinct typed ordinals even while
// V1 keeps their values equal; sharing one type would erase the versioned ABI invariant.
// jscpd:ignore-start
impl RecoveryRoundNumber {
    pub(crate) const fn get(self) -> u8 {
        self.0
    }

    #[cfg(test)]
    pub(crate) const fn fixture(value: u8) -> Self {
        Self(value)
    }

    #[cfg(test)]
    pub(crate) fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}
// jscpd:ignore-end

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryHandlerKind {
    Command,
    Agent,
}

impl RecoveryHandlerKind {
    fn from_validated(handler: &ValidatedRecoveryHandler) -> Self {
        match handler {
            ValidatedRecoveryHandler::Command { .. } => Self::Command,
            ValidatedRecoveryHandler::Agent { .. } => Self::Agent,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryHandlerActivity {
    Starting,
    Running,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActiveStepInvocation {
    Target {
        execution_number: TargetExecutionNumber,
    },
    RecoveryHandler {
        round: RecoveryRoundNumber,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProvisionalTargetFailure<Cause> {
    pub(crate) execution_number: TargetExecutionNumber,
    pub(crate) invocation: ActionId,
    pub(crate) phase: FailurePhase,
    pub(crate) detail: FailureDetail,
    pub(crate) cause: Cause,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryDecisionKind {
    Recheck,
    GaveUp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryDecision {
    pub(crate) kind: RecoveryDecisionKind,
    pub(crate) summary: String,
    pub(crate) reason: String,
}

impl RecoveryDecision {
    pub(crate) fn recheck(summary: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            kind: RecoveryDecisionKind::Recheck,
            summary: summary.into(),
            reason: reason.into(),
        }
    }

    pub(crate) fn gave_up(summary: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            kind: RecoveryDecisionKind::GaveUp,
            summary: summary.into(),
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryHandlerFailurePhase {
    Start,
    Execution,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryHandlerOutcome<Cause> {
    Starting,
    Running,
    Recheck {
        summary: String,
        reason: String,
    },
    GaveUp {
        summary: String,
        reason: String,
    },
    Failed {
        phase: RecoveryHandlerFailurePhase,
        cause: Cause,
    },
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryHandlerRecord<Cause> {
    pub(crate) kind: RecoveryHandlerKind,
    pub(crate) invocation: ActionId,
    pub(crate) outcome: RecoveryHandlerOutcome<Cause>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryRoundRecord<Cause> {
    pub(crate) number: RecoveryRoundNumber,
    pub(crate) failed_execution: ProvisionalTargetFailure<Cause>,
    pub(crate) handler: Option<RecoveryHandlerRecord<Cause>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryTerminalDisposition {
    Recovered {
        execution_number: TargetExecutionNumber,
    },
    Exhausted {
        execution_number: TargetExecutionNumber,
    },
    GaveUp {
        round: RecoveryRoundNumber,
    },
    HandlerFailed {
        round: RecoveryRoundNumber,
        phase: RecoveryHandlerFailurePhase,
    },
    Cancelled {
        round: RecoveryRoundNumber,
        active: ActiveStepInvocation,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StepRecoveryState<Cause> {
    pub(crate) configured_rounds: u8,
    pub(crate) handler_kind: Option<RecoveryHandlerKind>,
    pub(crate) rounds: Vec<RecoveryRoundRecord<Cause>>,
    pub(crate) terminal_disposition: Option<RecoveryTerminalDisposition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SchedulingGate {
    Open,
    FailureStopped {
        primary_issue: PrimaryIssue,
    },
    Cancelling {
        reason: CancellationReason,
        prior_issue: Option<PrimaryIssue>,
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
pub(crate) enum WorkflowState<Deadline = ()> {
    Executing {
        gate: SchedulingGate,
    },
    Finalizing {
        trigger: FinalizationTrigger,
        gate: FinalizationGate<Deadline>,
        primary_issue: Option<PrimaryIssue>,
    },
    Succeeded,
    Failed {
        primary_issue: PrimaryIssue,
        later_cancellation: Option<CancellationReason>,
    },
    Cancelled {
        reason: CancellationReason,
    },
}

impl<Deadline> WorkflowState<Deadline> {
    pub(crate) fn map_deadline<Mapped>(
        self,
        map: impl FnOnce(Deadline) -> Mapped,
    ) -> WorkflowState<Mapped> {
        match self {
            Self::Executing { gate } => WorkflowState::Executing { gate },
            Self::Finalizing {
                trigger,
                gate,
                primary_issue,
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
                primary_issue,
            },
            Self::Succeeded => WorkflowState::Succeeded,
            Self::Failed {
                primary_issue,
                later_cancellation,
            } => WorkflowState::Failed {
                primary_issue,
                later_cancellation,
            },
            Self::Cancelled { reason } => WorkflowState::Cancelled { reason },
        }
    }
}

pub(crate) type NotRunReason = NonExecutionCode;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StepState<Output> {
    Pending,
    Starting,
    Running,
    CapturingOutputs,
    Recovering {
        round: RecoveryRoundNumber,
        handler: RecoveryHandlerActivity,
    },
    Cancelling {
        detail: CancellationDetail,
    },
    Succeeded {
        outputs: OutputSet<Output>,
    },
    Failed {
        detail: FailureDetail,
    },
    Blocked {
        detail: BlockedDetail,
    },
    NotRun {
        detail: NonExecutionDetail,
    },
    Cancelled {
        detail: CancellationDetail,
    },
}

impl<Output> StepState<Output> {
    fn kind(&self) -> StepStateKind {
        match self {
            Self::Pending => StepStateKind::Pending,
            Self::Starting => StepStateKind::Starting,
            Self::Running => StepStateKind::Running,
            Self::CapturingOutputs => StepStateKind::CapturingOutputs,
            Self::Recovering { .. } => StepStateKind::Recovering,
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
            Self::Starting
                | Self::Running
                | Self::CapturingOutputs
                | Self::Recovering { .. }
                | Self::Cancelling { .. }
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
    Recovering,
    Cancelling,
    Succeeded,
    Failed,
    Blocked,
    NotRun,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StepRuntimeState<Cause, Output> {
    pub(crate) state: StepState<Output>,
    pub(crate) current_action: Option<ActionId>,
    pub(crate) target_execution: Option<TargetExecutionNumber>,
    // Output capture replaces current_action, so retain the target's original invocation identity.
    pub(crate) target_invocation: Option<ActionId>,
    pub(crate) active_invocation: Option<ActiveStepInvocation>,
    pub(crate) recovery: Option<StepRecoveryState<Cause>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RuntimeRecovery {
    retries: u8,
    handler_kind: Option<RecoveryHandlerKind>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeStep {
    role: WorkflowNodeRole,
    failure_policy: FailurePolicy,
    prerequisites: Arc<[ResolvedDirectPrerequisite]>,
    evidence_prerequisites: Arc<[Prerequisite]>,
    inputs: BTreeMap<String, ResolvedValueSource>,
    declared_outputs: BTreeSet<String>,
    recovery: Option<RuntimeRecovery>,
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
    maximum_transitions: u64,
}

impl RuntimeDefinition {
    fn from_admitted(admitted: &AdmittedWorkflow) -> Self {
        Self::from_workflow(
            &admitted.workflow().definition,
            admitted.execution().limits().maximum_parallel_steps(),
            admitted.capacity().maximum_transitions,
        )
    }

    fn from_workflow(
        workflow: &ValidatedWorkflow,
        maximum_parallel_steps: NonZeroUsize,
        maximum_transitions: u64,
    ) -> Self {
        let ordinary_ids = workflow.steps.keys().cloned().collect();
        let finalizer_ids = workflow.finalizers.keys().cloned().collect();
        let steps = workflow
            .steps
            .iter()
            .map(|(id, step)| {
                (
                    id.clone(),
                    runtime_step(
                        WorkflowNodeRole::Step,
                        step,
                        workflow.recoveries.get(id).and_then(Option::as_ref),
                        BTreeSet::new(),
                    ),
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
            maximum_transitions,
        }
    }
}

fn runtime_finalizer(finalizer: &ValidatedFinalizer) -> RuntimeStep {
    runtime_step(
        WorkflowNodeRole::Finalizer,
        &finalizer.body,
        None,
        finalizer.when.clone(),
    )
}

fn runtime_step(
    role: WorkflowNodeRole,
    step: &ValidatedStep,
    recovery: Option<&ValidatedStepRecovery>,
    when: BTreeSet<FinalizationTrigger>,
) -> RuntimeStep {
    let common = common_step(step);
    RuntimeStep {
        role,
        failure_policy: common.failure_policy,
        prerequisites: Arc::from(common.prerequisites.clone()),
        evidence_prerequisites: Arc::from(common.evidence_prerequisites.clone()),
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
        recovery: recovery.map(|recovery| RuntimeRecovery {
            retries: recovery.retries,
            handler_kind: recovery
                .handler
                .as_ref()
                .map(RecoveryHandlerKind::from_validated),
        }),
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
enum OrdinaryOutcome {
    Succeeded,
    Failed {
        primary_issue: PrimaryIssue,
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
pub(crate) struct FinalizerResult {
    pub(crate) finalizer: String,
    pub(crate) failure_policy: FailurePolicy,
    pub(crate) disposition: StepState<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FinalizationSummary<Deadline = ()> {
    pub(crate) trigger: FinalizationTrigger,
    pub(crate) finalizers: Vec<FinalizerResult>,
    pub(crate) cancellation: Option<FinalizationCancellation<Deadline>>,
    pub(crate) force_abort: bool,
}

// Mapping deadlines mirrors WorkflowState because summaries outlive transitions and are
// projected independently; a shared trait would obscure these two closed contracts.
// jscpd:ignore-start
impl<Deadline> FinalizationSummary<Deadline> {
    pub(crate) fn map_deadline<Mapped>(
        self,
        map: impl FnOnce(Deadline) -> Mapped,
    ) -> FinalizationSummary<Mapped> {
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
struct FinalizationRuntime<Deadline> {
    trigger: FinalizationTrigger,
    context: Arc<[u8]>,
    ordinary_outcome: OrdinaryOutcome,
    cancellation: Option<FinalizationCancellation<Deadline>>,
    force_abort: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeState<Cause, Output, Deadline = ()> {
    definition: Arc<RuntimeDefinition>,
    pub(crate) workflow: WorkflowState<Deadline>,
    // One namespace and one reducer map; role is retained in the definition and events.
    pub(crate) steps: BTreeMap<String, StepRuntimeState<Cause, Output>>,
    pub(crate) exports: Option<ExportSet<Output>>,
    pub(crate) finalization_summary: Option<FinalizationSummary<Deadline>>,
    finalization: Option<FinalizationRuntime<Deadline>>,
    pub(crate) last_cancellation_operation: Option<CancellationOperationId>,
    pub(crate) last_transition_sequence: TransitionSequence,
    transition_capacity_exceeded: bool,
}

impl<Cause, Output, Deadline> RuntimeState<Cause, Output, Deadline> {
    pub(crate) const fn transition_capacity_exceeded(&self) -> bool {
        self.transition_capacity_exceeded
    }
}

#[cfg(test)]
impl<Cause, Output, Deadline> RuntimeState<Cause, Output, Deadline> {
    pub(crate) fn admitted_transition_ceiling(&self) -> u64 {
        self.definition.maximum_transitions
    }
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
    RecoveryHandlerStarted {
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
    },
    RecoveryHandlerStartFailed {
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        cause: Cause,
    },
    RecoveryHandlerCompleted {
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        decision: RecoveryDecision,
    },
    RecoveryHandlerExecutionFailed {
        step: String,
        round: RecoveryRoundNumber,
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
pub(crate) enum RunOutcome {
    Succeeded,
    Failed {
        primary_issue: PrimaryIssue,
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
        execution_number: TargetExecutionNumber,
        inputs: BTreeMap<String, ActionInput<Output>>,
    },
    StartRecoveryHandler {
        step: String,
        round: RecoveryRoundNumber,
        kind: RecoveryHandlerKind,
        history: Vec<RecoveryRoundRecord<Cause>>,
    },
    CaptureOutputs {
        step: String,
        provisional: Provisional,
    },
    CancelStep {
        step: String,
        active: ActiveStepInvocation,
        reason: CancellationReason,
        deadline: Deadline,
    },
    ForceAbortStep {
        step: String,
        active: ActiveStepInvocation,
        reason: CancellationReason,
        deadline: Deadline,
    },
    FinishRun {
        outcome: RunOutcome,
        exports: ExportSet<Output>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestedAction<Provisional, Cause, Output, Deadline> {
    pub(crate) id: ActionId,
    pub(crate) action: Action<Provisional, Cause, Output, Deadline>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TransitionEvent<Deadline> {
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
        from: WorkflowState<Deadline>,
        to: Box<WorkflowState<Deadline>>,
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
    pub(crate) events: Vec<TransitionEvent<Deadline>>,
    pub(crate) actions: Vec<RequestedAction<Provisional, Cause, Output, Deadline>>,
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
        .iter()
        .map(|(step, definition)| {
            (
                step.clone(),
                StepRuntimeState {
                    state: StepState::Pending,
                    current_action: None,
                    target_execution: None,
                    target_invocation: None,
                    active_invocation: None,
                    recovery: definition.recovery.map(|recovery| StepRecoveryState {
                        configured_rounds: recovery.retries,
                        handler_kind: recovery.handler_kind,
                        rounds: Vec::with_capacity(usize::from(recovery.retries)),
                        terminal_disposition: None,
                    }),
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
            transition_capacity_exceeded: false,
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
    Cause: Clone + NodeFailureSource,
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
    Cause: Clone + NodeFailureSource,
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
                mark_recovered(reduction, &step);
                transition_step(
                    reduction,
                    &step,
                    StepState::Succeeded {
                        outputs: BTreeMap::new(),
                    },
                    None,
                );
                clear_active_invocation(&mut reduction.state, &step);
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
            mark_recovered(reduction, &step);
            transition_step(reduction, &step, StepState::Succeeded { outputs }, None);
            clear_active_invocation(&mut reduction.state, &step);
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
        Occurrence::RecoveryHandlerStarted {
            step,
            round,
            action,
        } => {
            if !handler_accepts(
                &reduction.state,
                &step,
                round,
                RecoveryHandlerActivity::Starting,
                action,
            ) {
                return false;
            }
            set_handler_outcome(
                &mut reduction.state,
                &step,
                round,
                RecoveryHandlerOutcome::Running,
            );
            transition_step(
                reduction,
                &step,
                StepState::Recovering {
                    round,
                    handler: RecoveryHandlerActivity::Running,
                },
                Some(action),
            );
        }
        Occurrence::RecoveryHandlerStartFailed {
            step,
            round,
            action,
            cause,
        } => {
            return apply_handler_failure(
                reduction,
                step,
                round,
                action,
                RecoveryHandlerActivity::Starting,
                RecoveryHandlerFailurePhase::Start,
                cause,
            );
        }
        Occurrence::RecoveryHandlerCompleted {
            step,
            round,
            action,
            decision,
        } => {
            return apply_handler_decision(reduction, step, round, action, decision);
        }
        Occurrence::RecoveryHandlerExecutionFailed {
            step,
            round,
            action,
            cause,
        } => {
            return apply_handler_failure(
                reduction,
                step,
                round,
                action,
                RecoveryHandlerActivity::Running,
                RecoveryHandlerFailurePhase::Execution,
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
            let Some((cancelling_reason, active)) =
                cancelling_step(&reduction.state, &step, action)
            else {
                return false;
            };
            let terminal_reason = if action.is_force_abort() {
                CancellationReason::FinalizationForceAbort
            } else {
                cancelling_reason
            };
            mark_recovery_cancelled(&mut reduction.state, &step, active);
            transition_step(
                reduction,
                &step,
                StepState::Cancelled {
                    detail: CancellationDetail::new(terminal_reason),
                },
                None,
            );
            clear_active_invocation(&mut reduction.state, &step);
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
    let prior_issue = match &reduction.state.workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => None,
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { primary_issue },
        } => Some(primary_issue.clone()),
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
            prior_issue,
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
        primary_issue,
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
        primary_issue: primary_issue.clone(),
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
        primary_issue,
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
    let primary_issue = primary_issue.clone();
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
        primary_issue,
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
                transition_step(
                    reduction,
                    &step,
                    StepState::Cancelled {
                        detail: CancellationDetail::new(reason),
                    },
                    None,
                );
            }
            StepStateKind::Starting
            | StepStateKind::Running
            | StepStateKind::CapturingOutputs
            | StepStateKind::Recovering => {
                let Some(active) = reduction
                    .state
                    .steps
                    .get(&step)
                    .and_then(|runtime| runtime.active_invocation)
                else {
                    continue;
                };
                mark_handler_cancelled(&mut reduction.state, &step, active);
                let sequence = transition_step(
                    reduction,
                    &step,
                    StepState::Cancelling {
                        detail: CancellationDetail::new(reason),
                    },
                    None,
                );
                let action = force_context.as_ref().map_or_else(
                    || ActionId::for_transition(sequence),
                    |(operation, _)| ActionId::for_force_abort(*operation, containment_index),
                );
                containment_index += 1;
                set_current_action(&mut reduction.state, &step, action);
                let requested = match &force_context {
                    Some((_, deadline)) => Action::ForceAbortStep {
                        step,
                        active,
                        reason,
                        deadline: deadline.clone(),
                    },
                    None => {
                        let Some(deadline) = graceful_deadline.clone() else {
                            continue;
                        };
                        Action::CancelStep {
                            step,
                            active,
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
                let Some(active) = reduction
                    .state
                    .steps
                    .get(&step)
                    .and_then(|runtime| runtime.active_invocation)
                else {
                    continue;
                };
                reduction.actions.push(RequestedAction {
                    id: action,
                    action: Action::ForceAbortStep {
                        step,
                        active,
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

fn cancelling_step<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
    action: ActionId,
) -> Option<(CancellationReason, ActiveStepInvocation)> {
    let runtime = state.steps.get(step)?;
    match &runtime.state {
        StepState::Cancelling { detail } if runtime.current_action == Some(action) => {
            Some((detail.code, runtime.active_invocation?))
        }
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
    Cause: Clone + NodeFailureSource,
    Output: Clone,
    Deadline: Clone,
{
    if !step_accepts(&reduction.state, &step, expected_state, action) {
        return false;
    }
    let Some(definition) = reduction.state.definition.steps.get(&step).cloned() else {
        return false;
    };
    let Ok(detail) = cause.node_failure_detail(phase) else {
        return false;
    };
    if definition.role == WorkflowNodeRole::Step
        && activate_recovery_round(reduction, &step, phase, detail.clone(), cause.clone())
    {
        return true;
    }
    if definition.recovery.is_some() {
        mark_recovery_exhausted(&mut reduction.state, &step);
    }
    settle_target_failure(reduction, step, definition, detail);
    true
}

fn activate_recovery_round<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: &str,
    phase: FailurePhase,
    detail: FailureDetail,
    cause: Cause,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let Some(runtime) = reduction.state.steps.get(step) else {
        return false;
    };
    let Some(execution_number) = runtime.target_execution else {
        return false;
    };
    let Some(invocation) = runtime.target_invocation else {
        return false;
    };
    let Some(recovery) = runtime.recovery.as_ref() else {
        return false;
    };
    if recovery.rounds.len() >= usize::from(recovery.configured_rounds) {
        return false;
    }
    let Some(round_value) = u8::try_from(recovery.rounds.len())
        .ok()
        .and_then(|round| round.checked_add(1))
    else {
        return false;
    };
    let round = RecoveryRoundNumber(round_value);
    if round.get() != execution_number.get() {
        return false;
    }
    let handler_kind = recovery.handler_kind;
    let failed_execution = ProvisionalTargetFailure {
        execution_number,
        invocation,
        phase,
        detail,
        cause,
    };
    let Some(recovery) = reduction
        .state
        .steps
        .get_mut(step)
        .and_then(|runtime| runtime.recovery.as_mut())
    else {
        return false;
    };
    recovery.rounds.push(RecoveryRoundRecord {
        number: round,
        failed_execution,
        handler: None,
    });

    match handler_kind {
        Some(kind) => {
            if let Some(runtime) = reduction.state.steps.get_mut(step) {
                runtime.active_invocation = Some(ActiveStepInvocation::RecoveryHandler { round });
            }
            let sequence = transition_step(
                reduction,
                step,
                StepState::Recovering {
                    round,
                    handler: RecoveryHandlerActivity::Starting,
                },
                None,
            );
            let action = ActionId::for_transition(sequence);
            let Some(round_record) = reduction
                .state
                .steps
                .get_mut(step)
                .and_then(|runtime| runtime.recovery.as_mut())
                .and_then(|recovery| recovery.rounds.last_mut())
            else {
                return false;
            };
            round_record.handler = Some(RecoveryHandlerRecord {
                kind,
                invocation: action,
                outcome: RecoveryHandlerOutcome::Starting,
            });
            set_current_action(&mut reduction.state, step, action);
            let history = reduction
                .state
                .steps
                .get(step)
                .and_then(|runtime| runtime.recovery.as_ref())
                .map(|recovery| recovery.rounds.clone())
                .unwrap_or_default();
            reduction.actions.push(RequestedAction {
                id: action,
                action: Action::StartRecoveryHandler {
                    step: step.to_owned(),
                    round,
                    kind,
                    history,
                },
            });
        }
        None => {
            let Some(next_execution) = execution_number.next() else {
                return false;
            };
            let Some(definition) = reduction.state.definition.steps.get(step).cloned() else {
                return false;
            };
            let inputs = resolved_action_inputs(&reduction.state, &definition);
            authorize_target(reduction, step.to_owned(), next_execution, inputs);
        }
    }
    true
}

fn apply_handler_decision<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
    round: RecoveryRoundNumber,
    action: ActionId,
    decision: RecoveryDecision,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if !handler_accepts(
        &reduction.state,
        &step,
        round,
        RecoveryHandlerActivity::Running,
        action,
    ) {
        return false;
    }
    let outcome = match decision.kind {
        RecoveryDecisionKind::Recheck => RecoveryHandlerOutcome::Recheck {
            summary: decision.summary,
            reason: decision.reason,
        },
        RecoveryDecisionKind::GaveUp => RecoveryHandlerOutcome::GaveUp {
            summary: decision.summary,
            reason: decision.reason,
        },
    };
    set_handler_outcome(&mut reduction.state, &step, round, outcome);
    match decision.kind {
        RecoveryDecisionKind::Recheck => {
            let Some(execution_number) = latest_target_failure(&reduction.state, &step)
                .and_then(|failure| failure.execution_number.next())
            else {
                return false;
            };
            let Some(definition) = reduction.state.definition.steps.get(&step).cloned() else {
                return false;
            };
            let inputs = resolved_action_inputs(&reduction.state, &definition);
            authorize_target(reduction, step, execution_number, inputs);
        }
        RecoveryDecisionKind::GaveUp => {
            set_recovery_disposition(
                &mut reduction.state,
                &step,
                RecoveryTerminalDisposition::GaveUp { round },
            );
            settle_latest_target_failure(reduction, step);
        }
    }
    true
}

fn apply_handler_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
    round: RecoveryRoundNumber,
    action: ActionId,
    expected: RecoveryHandlerActivity,
    phase: RecoveryHandlerFailurePhase,
    cause: Cause,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if !handler_accepts(&reduction.state, &step, round, expected, action) {
        return false;
    }
    set_handler_outcome(
        &mut reduction.state,
        &step,
        round,
        RecoveryHandlerOutcome::Failed { phase, cause },
    );
    set_recovery_disposition(
        &mut reduction.state,
        &step,
        RecoveryTerminalDisposition::HandlerFailed { round, phase },
    );
    settle_latest_target_failure(reduction, step);
    true
}

fn settle_latest_target_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let Some(failure) = latest_target_failure(&reduction.state, &step).cloned() else {
        return;
    };
    let Some(definition) = reduction.state.definition.steps.get(&step).cloned() else {
        return;
    };
    settle_target_failure(reduction, step, definition, failure.detail);
}

fn settle_target_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
    definition: RuntimeStep,
    detail: FailureDetail,
) where
    Cause: Clone,
    Deadline: Clone,
{
    let primary_issue = PrimaryIssue::failed(
        WorkflowNode {
            id: step.clone(),
            role: definition.role,
        },
        detail.clone(),
    );
    transition_step(reduction, &step, StepState::Failed { detail }, None);
    clear_active_invocation(&mut reduction.state, &step);
    if definition.failure_policy == FailurePolicy::Required {
        match definition.role {
            WorkflowNodeRole::Step => close_ordinary_gate_for_failure(reduction, primary_issue),
            WorkflowNodeRole::Finalizer => select_finalizer_primary_issue(reduction, primary_issue),
        }
    }
}

fn latest_target_failure<'a, Cause, Output, Deadline>(
    state: &'a RuntimeState<Cause, Output, Deadline>,
    step: &str,
) -> Option<&'a ProvisionalTargetFailure<Cause>> {
    state
        .steps
        .get(step)?
        .recovery
        .as_ref()?
        .rounds
        .last()
        .map(|round| &round.failed_execution)
}

fn handler_accepts<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
    round: RecoveryRoundNumber,
    expected: RecoveryHandlerActivity,
    action: ActionId,
) -> bool {
    state.steps.get(step).is_some_and(|runtime| {
        runtime.current_action == Some(action)
            && runtime.active_invocation == Some(ActiveStepInvocation::RecoveryHandler { round })
            && matches!(
                &runtime.state,
                StepState::Recovering {
                    round: active_round,
                    handler,
                } if *active_round == round && *handler == expected
            )
    })
}

fn set_handler_outcome<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
    round: RecoveryRoundNumber,
    outcome: RecoveryHandlerOutcome<Cause>,
) {
    if let Some(handler) = state
        .steps
        .get_mut(step)
        .and_then(|runtime| runtime.recovery.as_mut())
        .and_then(|recovery| recovery.rounds.last_mut())
        .filter(|record| record.number == round)
        .and_then(|record| record.handler.as_mut())
    {
        handler.outcome = outcome;
    }
}

fn set_recovery_disposition<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
    disposition: RecoveryTerminalDisposition,
) {
    if let Some(recovery) = state
        .steps
        .get_mut(step)
        .and_then(|runtime| runtime.recovery.as_mut())
    {
        recovery.terminal_disposition = Some(disposition);
    }
}

fn mark_recovery_exhausted<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
) {
    if let Some(execution_number) = activated_recovery_execution(state, step) {
        set_recovery_disposition(
            state,
            step,
            RecoveryTerminalDisposition::Exhausted { execution_number },
        );
    }
}

fn mark_recovered<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: &str,
) {
    if let Some(execution_number) = activated_recovery_execution(&reduction.state, step) {
        set_recovery_disposition(
            &mut reduction.state,
            step,
            RecoveryTerminalDisposition::Recovered { execution_number },
        );
    }
}

fn activated_recovery_execution<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
) -> Option<TargetExecutionNumber> {
    let runtime = state.steps.get(step)?;
    runtime
        .recovery
        .as_ref()
        .is_some_and(|recovery| !recovery.rounds.is_empty())
        .then_some(runtime.target_execution)
        .flatten()
}

fn mark_handler_cancelled<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
    active: ActiveStepInvocation,
) {
    if let ActiveStepInvocation::RecoveryHandler { round } = active {
        set_handler_outcome(state, step, round, RecoveryHandlerOutcome::Cancelled);
    }
}

fn mark_recovery_cancelled<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
    active: ActiveStepInvocation,
) {
    let Some(round) = state
        .steps
        .get(step)
        .and_then(|runtime| runtime.recovery.as_ref())
        .and_then(|recovery| recovery.rounds.last())
        .map(|round| round.number)
    else {
        return;
    };
    set_recovery_disposition(
        state,
        step,
        RecoveryTerminalDisposition::Cancelled { round, active },
    );
}

fn clear_active_invocation<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
) {
    if let Some(runtime) = state.steps.get_mut(step) {
        runtime.active_invocation = None;
    }
}

// Primary issue selection is phase-specific even though both transitions carry the same
// surrounding workflow fields; keeping them separate makes ordinary precedence explicit.
// jscpd:ignore-start
fn close_ordinary_gate_for_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    primary_issue: PrimaryIssue,
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
        gate: SchedulingGate::FailureStopped { primary_issue },
    };
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to.clone();
    reduction.events.push(TransitionEvent::Workflow {
        sequence,
        from,
        to: Box::new(to),
    });
}

fn select_finalizer_primary_issue<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    primary_issue: PrimaryIssue,
) where
    Cause: Clone,
    Deadline: Clone,
{
    let WorkflowState::Finalizing {
        trigger,
        gate: FinalizationGate::Open,
        primary_issue: None,
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
        primary_issue: Some(primary_issue),
    };
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to.clone();
    reduction.events.push(TransitionEvent::Workflow {
        sequence,
        from,
        to: Box::new(to),
    });
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
    Blocked(BlockedDetail),
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
            PendingDisposition::Blocked(detail) => StepState::Blocked { detail },
            PendingDisposition::NotRun => StepState::NotRun {
                detail: NonExecutionDetail::failure_stop(),
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
        let blockers = ordinary_unsatisfied_prerequisites(state, definition);
        if !blockers.is_empty() {
            return BlockedDetail::new(blockers)
                .ok()
                .map(|detail| (step_id.clone(), PendingDisposition::Blocked(detail)));
        }
        (failure_stopped
            && definition
                .prerequisites
                .iter()
                .all(|prerequisite| ordinary_prerequisite_satisfied(state, prerequisite)))
        .then(|| (step_id.clone(), PendingDisposition::NotRun))
    })
}

fn ordinary_unsatisfied_prerequisites<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    definition: &RuntimeStep,
) -> Vec<Prerequisite> {
    let mut blockers = Vec::new();
    for prerequisite in definition.prerequisites.iter() {
        let Some(producer) = state.steps.get(&prerequisite.producer) else {
            continue;
        };
        if !producer.state.is_terminal() {
            continue;
        }
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
        if prerequisite.control
            && !control_satisfied
            && let Ok(blocker) = Prerequisite::control(prerequisite.producer.clone())
        {
            blockers.push(blocker);
        }
        if prerequisite.data && !succeeded {
            blockers.extend(
                definition
                    .evidence_prerequisites
                    .iter()
                    .filter(|descriptor| {
                        matches!(descriptor, Prerequisite::Body { r#ref }
                            if output_reference_producer(r#ref)
                                == Some(prerequisite.producer.as_str()))
                    })
                    .cloned(),
            );
        }
    }
    blockers
}

fn output_reference_producer(reference: &str) -> Option<&str> {
    let mut segments = reference.split('.');
    match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some("outputs"), Some(producer), Some(_), None) => Some(producer),
        _ => None,
    }
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
                StepState::Failed { .. } | StepState::Blocked { .. }
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
                detail: NonExecutionDetail::finalizer_trigger_not_selected(),
            },
            None,
        );
    }

    while let Some((finalizer, references)) = next_input_unavailable(&reduction.state) {
        let prerequisites = references
            .into_iter()
            .filter_map(|reference| Prerequisite::body(reference).ok());
        let Ok(detail) = BlockedDetail::new(prerequisites) else {
            continue;
        };
        transition_step(reduction, &finalizer, StepState::Blocked { detail }, None);
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
        .evidence_prerequisites
        .iter()
        .filter_map(|descriptor| match descriptor {
            Prerequisite::Body { r#ref }
                if output_reference_producer(r#ref).is_some_and(|producer| {
                    !state
                        .steps
                        .get(producer)
                        .is_some_and(|runtime| matches!(runtime.state, StepState::Succeeded { .. }))
                }) =>
            {
                Some(r#ref.clone())
            }
            Prerequisite::Control { .. }
            | Prerequisite::Condition { .. }
            | Prerequisite::Body { .. } => None,
        })
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
        authorize_target(reduction, step, TargetExecutionNumber::FIRST, inputs);
    }
}

fn authorize_target<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
    execution_number: TargetExecutionNumber,
    inputs: BTreeMap<String, ActionInput<Output>>,
) {
    if let Some(runtime) = reduction.state.steps.get_mut(&step) {
        runtime.target_execution = Some(execution_number);
    }
    let sequence = transition_step(reduction, &step, StepState::Starting, None);
    let action_id = ActionId::for_transition(sequence);
    if let Some(runtime) = reduction.state.steps.get_mut(&step) {
        runtime.target_invocation = Some(action_id);
        runtime.active_invocation = Some(ActiveStepInvocation::Target { execution_number });
    }
    set_current_action(&mut reduction.state, &step, action_id);
    reduction.actions.push(RequestedAction {
        id: action_id,
        action: Action::StartStep {
            step,
            execution_number,
            inputs,
        },
    });
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
    let Some(ordinary_outcome) = ordinary_outcome(&reduction.state) else {
        return;
    };
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
    let primary_issue_step_id = match &ordinary_outcome {
        OrdinaryOutcome::Failed { primary_issue, .. } => Some(primary_issue.node.id.as_str()),
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
        primary_failure_step_id: primary_issue_step_id,
        cancellation_reason: ordinary_cancellation,
        ordinary_issues: &ordinary_issues,
    });
    let primary_issue = match &ordinary_outcome {
        OrdinaryOutcome::Failed { primary_issue, .. } => Some(primary_issue.clone()),
        OrdinaryOutcome::Succeeded | OrdinaryOutcome::Cancelled { .. } => None,
    };
    let from = reduction.state.workflow.clone();
    let to = WorkflowState::Finalizing {
        trigger,
        gate: FinalizationGate::Open,
        primary_issue,
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
    reduction.events.push(TransitionEvent::Workflow {
        sequence,
        from,
        to: Box::new(to),
    });
}

// Finalization entry and ordinary outcome composition both enumerate the closed ordinary
// outcomes, but they own different state changes and should not share a partial mapper.
// jscpd:ignore-start
fn ordinary_outcome<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Option<OrdinaryOutcome>
where
    Cause: Clone,
{
    Some(match &state.workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => OrdinaryOutcome::Succeeded,
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { primary_issue },
        } => OrdinaryOutcome::Failed {
            primary_issue: primary_issue.clone(),
            later_cancellation: None,
        },
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_issue: Some(primary_issue),
                },
        } => OrdinaryOutcome::Failed {
            primary_issue: primary_issue.clone(),
            later_cancellation: Some(*reason),
        },
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_issue: None,
                },
        } => OrdinaryOutcome::Cancelled { reason: *reason },
        WorkflowState::Finalizing { .. }
        | WorkflowState::Succeeded
        | WorkflowState::Failed { .. }
        | WorkflowState::Cancelled { .. } => return None,
    })
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
    finalization: &FinalizationRuntime<Deadline>,
) -> FinalizationSummary<Deadline>
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

fn erase_outputs<Output>(state: &StepState<Output>) -> Option<StepState<()>> {
    Some(match state {
        StepState::Succeeded { .. } => StepState::Succeeded {
            outputs: BTreeMap::new(),
        },
        StepState::Failed { detail } => StepState::Failed {
            detail: detail.clone(),
        },
        StepState::Blocked { detail } => StepState::Blocked {
            detail: detail.clone(),
        },
        StepState::NotRun { detail } => StepState::NotRun { detail: *detail },
        StepState::Cancelled { detail } => StepState::Cancelled { detail: *detail },
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Cancelling { .. } => return None,
    })
}

fn compose_final_outcome<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    finalization: &FinalizationRuntime<Deadline>,
) -> OrdinaryOutcome
where
    Cause: Clone,
{
    match &finalization.ordinary_outcome {
        OrdinaryOutcome::Failed {
            primary_issue,
            later_cancellation,
        } => OrdinaryOutcome::Failed {
            primary_issue: primary_issue.clone(),
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
                primary_issue: Some(primary_issue),
                ..
            } => OrdinaryOutcome::Failed {
                primary_issue: primary_issue.clone(),
                later_cancellation: finalization
                    .cancellation
                    .as_ref()
                    .map(|record| record.reason),
            },
            WorkflowState::Finalizing {
                gate: FinalizationGate::Cancelling { reason, .. },
                primary_issue: None,
                ..
            } => OrdinaryOutcome::Cancelled { reason: *reason },
            _ => OrdinaryOutcome::Succeeded,
        },
    }
}

fn finish_run<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    ordinary_outcome: OrdinaryOutcome,
    summary: Option<FinalizationSummary<Deadline>>,
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
            primary_issue,
            later_cancellation,
        } => (
            WorkflowState::Failed {
                primary_issue: primary_issue.clone(),
                later_cancellation,
            },
            RunOutcome::Failed {
                primary_issue,
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
    reduction.events.push(TransitionEvent::Workflow {
        sequence,
        from,
        to: Box::new(to),
    });
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
                StepState::NotRun {
                    detail:
                        NonExecutionDetail {
                            code: NonExecutionCode::FinalizerTriggerNotSelected,
                        },
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
                | StepState::Recovering { .. }
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
    to: StepState<Output>,
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
    if sequence.get() > state.definition.maximum_transitions {
        state.transition_capacity_exceeded = true;
        return state.last_transition_sequence;
    }
    state.last_transition_sequence = sequence;
    sequence
}

#[cfg(test)]
mod tests;
