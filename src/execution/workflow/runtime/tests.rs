use std::fs;
use std::path::Path;
use std::time::Duration;

use super::*;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationReason, CancellationSource, CaptureLimits, EnvironmentSnapshot,
    ExecutionContext, ExecutionPolicyLimits, ExecutionRootLifecycle, InputLimits, ResolvedImports,
    admit_runner_workflow, admit_workflow,
};
use crate::execution::workflow::resolution;

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestDeadline {
    arbiter_tick: u64,
}

type TestAction = RequestedAction<String, String, String, TestDeadline>;
type TestOccurrence = Occurrence<String, String, String, TestDeadline>;
type TestReduction = Reduction<String, String, String, TestDeadline>;
type TestRuntimeState = RuntimeState<String, String, TestDeadline>;

fn deadline(arbiter_tick: u64) -> TestDeadline {
    TestDeadline { arbiter_tick }
}

fn sequence(value: u64) -> TransitionSequence {
    TransitionSequence(value)
}

fn action_id(value: u64) -> ActionId {
    ActionId::for_transition(sequence(value))
}

fn definition(
    steps: &[(&str, &[&str], &[&str])],
    exports: &[(&str, &str, &str)],
    maximum_parallel_steps: usize,
) -> RuntimeDefinition {
    let ordinary_ids = steps
        .iter()
        .map(|(step, _, _)| (*step).to_owned())
        .collect();
    RuntimeDefinition {
        steps: steps
            .iter()
            .map(|(step, dependencies, outputs)| {
                (
                    (*step).to_owned(),
                    RuntimeStep {
                        role: WorkflowNodeRole::Step,
                        failure_policy: FailurePolicy::Required,
                        condition: None,
                        condition_values: BTreeMap::new(),
                        prerequisites: dependencies
                            .iter()
                            .map(|dependency| ResolvedDirectPrerequisite {
                                producer: (*dependency).to_owned(),
                                control: true,
                                disposition_control: false,
                                data: false,
                                condition_data: false,
                            })
                            .collect::<Vec<_>>()
                            .into(),
                        evidence_prerequisites: dependencies
                            .iter()
                            .filter_map(|dependency| Prerequisite::control(*dependency).ok())
                            .collect::<Vec<_>>()
                            .into(),
                        inputs: BTreeMap::new(),
                        declared_outputs: outputs
                            .iter()
                            .map(|output| (*output).to_owned())
                            .collect(),
                        recovery: None,
                        when: BTreeSet::new(),
                    },
                )
            })
            .collect(),
        ordinary_ids,
        finalizer_ids: BTreeSet::new(),
        finalizer_presentation_order: Vec::new(),
        exports: exports
            .iter()
            .map(|(name, step, output)| {
                (
                    (*name).to_owned(),
                    RuntimeExport {
                        step: (*step).to_owned(),
                        output: (*output).to_owned(),
                    },
                )
            })
            .collect(),
        maximum_parallel_steps: NonZeroUsize::new(maximum_parallel_steps).unwrap(),
        maximum_transitions: 10_000,
        prompt: None,
    }
}

fn initialize_test(definition: RuntimeDefinition) -> TestReduction {
    initialize_definition(ExecutionStart {
        definition,
        initial_cancellation: None,
        initial_cancellation_operation: None,
    })
}

fn step_event(
    value: u64,
    step: &str,
    from: StepStateKind,
    to: StepStateKind,
) -> TransitionEvent<TestDeadline> {
    TransitionEvent::Step {
        sequence: sequence(value),
        step: step.to_owned(),
        role: WorkflowNodeRole::Step,
        failure_policy: FailurePolicy::Required,
        from,
        to,
    }
}

fn workflow_event(
    value: u64,
    from: WorkflowState<TestDeadline>,
    to: WorkflowState<TestDeadline>,
) -> TransitionEvent<TestDeadline> {
    TransitionEvent::Workflow {
        sequence: sequence(value),
        from,
        to: Box::new(to),
    }
}

fn workflow_succeeded_event(value: u64) -> TransitionEvent<TestDeadline> {
    workflow_event(
        value,
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        },
        WorkflowState::Succeeded,
    )
}

fn cancellation_event(
    value: u64,
    reason: CancellationReason,
    arbiter_tick: u64,
) -> TransitionEvent<TestDeadline> {
    TransitionEvent::CancellationAccepted {
        sequence: sequence(value),
        reason,
        deadline: deadline(arbiter_tick),
    }
}

fn failure(step: &str, phase: FailurePhase, cause: &str) -> PrimaryIssue {
    PrimaryIssue::failed(
        WorkflowNode {
            id: step.to_owned(),
            role: WorkflowNodeRole::Step,
        },
        cause.to_owned().node_failure_detail(phase).unwrap(),
    )
}

fn failed_state(phase: FailurePhase, cause: &str) -> StepState<String> {
    StepState::Failed {
        detail: cause.to_owned().node_failure_detail(phase).unwrap(),
    }
}

fn blocked_state(prerequisites: impl IntoIterator<Item = Prerequisite>) -> StepState<String> {
    StepState::Blocked {
        detail: BlockedDetail::new(prerequisites).unwrap(),
    }
}

fn not_run_state(role: WorkflowNodeRole, code: NonExecutionCode) -> StepState<String> {
    StepState::NotRun {
        detail: NonExecutionDetail::for_role(role, code).unwrap(),
    }
}

fn cancelled_state(reason: CancellationReason) -> StepState<String> {
    StepState::Cancelled {
        detail: CancellationDetail::new(reason),
    }
}

fn cancellation(
    reason: CancellationReason,
    arbiter_tick: u64,
) -> CancellationRequest<TestDeadline> {
    CancellationRequest {
        reason,
        deadline: deadline(arbiter_tick),
    }
}

fn cancelling_workflow(
    reason: CancellationReason,
    prior_issue: Option<PrimaryIssue>,
) -> WorkflowState<TestDeadline> {
    WorkflowState::Executing {
        gate: SchedulingGate::Cancelling {
            reason,
            prior_issue,
        },
    }
}

fn start_action(value: u64, step: &str) -> TestAction {
    RequestedAction {
        id: action_id(value),
        action: Action::StartStep {
            step: step.to_owned(),
            execution_number: TargetExecutionNumber::FIRST,
            inputs: BTreeMap::new(),
        },
    }
}

fn capture_action(value: u64, step: &str, provisional: &str) -> TestAction {
    RequestedAction {
        id: action_id(value),
        action: Action::CaptureOutputs {
            step: step.to_owned(),
            provisional: provisional.to_owned(),
        },
    }
}

fn finish_action(value: u64, exports: ExportSet<String>) -> TestAction {
    RequestedAction {
        id: action_id(value),
        action: Action::FinishRun {
            outcome: RunOutcome::Succeeded,
            exports,
        },
    }
}

fn cancel_action(
    value: u64,
    step: &str,
    reason: CancellationReason,
    arbiter_tick: u64,
) -> TestAction {
    RequestedAction {
        id: action_id(value),
        action: Action::CancelStep {
            step: step.to_owned(),
            active: ActiveStepInvocation::Target {
                execution_number: TargetExecutionNumber::FIRST,
            },
            reason,
            deadline: deadline(arbiter_tick),
        },
    }
}

fn finish_failed_action(
    value: u64,
    primary_issue: PrimaryIssue,
    exports: ExportSet<String>,
) -> TestAction {
    finish_failed_after_cancellation_action(value, primary_issue, None, exports)
}

fn finish_failed_after_cancellation_action(
    value: u64,
    primary_issue: PrimaryIssue,
    later_cancellation: Option<CancellationReason>,
    exports: ExportSet<String>,
) -> TestAction {
    RequestedAction {
        id: action_id(value),
        action: Action::FinishRun {
            outcome: RunOutcome::Failed {
                primary_issue,
                later_cancellation,
            },
            exports,
        },
    }
}

fn finish_cancelled_action(
    value: u64,
    reason: CancellationReason,
    exports: ExportSet<String>,
) -> TestAction {
    RequestedAction {
        id: action_id(value),
        action: Action::FinishRun {
            outcome: RunOutcome::Cancelled { reason },
            exports,
        },
    }
}

#[expect(
    clippy::type_complexity,
    reason = "the compact fixture tuples keep graph scenarios readable at call sites"
)]
fn finalizer_definition(
    steps: &[(
        &str,
        FailurePolicy,
        &[FinalizationTrigger],
        &[&str],
        &[&str],
    )],
    finalizers: &[(
        &str,
        FailurePolicy,
        &[FinalizationTrigger],
        &[(&str, &str)],
        &[&str],
    )],
    maximum_parallel_steps: usize,
) -> RuntimeDefinition {
    let ordinary_ids = steps.iter().map(|(id, ..)| (*id).to_owned()).collect();
    let finalizer_ids = finalizers.iter().map(|(id, ..)| (*id).to_owned()).collect();
    let runtime_steps = steps
        .iter()
        .map(|(id, policy, _, _, outputs)| {
            (
                (*id).to_owned(),
                RuntimeStep {
                    role: WorkflowNodeRole::Step,
                    failure_policy: *policy,
                    condition: None,
                    condition_values: BTreeMap::new(),
                    prerequisites: Arc::from([]),
                    evidence_prerequisites: Arc::from([]),
                    inputs: BTreeMap::new(),
                    declared_outputs: outputs.iter().map(|output| (*output).to_owned()).collect(),
                    recovery: None,
                    when: BTreeSet::new(),
                },
            )
        })
        .chain(finalizers.iter().map(|(id, policy, when, inputs, predecessors)| {
            (
                (*id).to_owned(),
                RuntimeStep {
                    role: WorkflowNodeRole::Finalizer,
                    failure_policy: *policy,
                    condition: None,
                    condition_values: BTreeMap::new(),
                    prerequisites: predecessors
                        .iter()
                        .map(|producer| ResolvedDirectPrerequisite {
                            producer: (*producer).to_owned(),
                            control: true,
                            disposition_control: false,
                            data: false,
                            condition_data: false,
                        })
                        .collect::<Vec<_>>()
                        .into(),
                    evidence_prerequisites: predecessors
                        .iter()
                        .filter_map(|producer| Prerequisite::control(*producer).ok())
                        .chain(inputs.iter().filter_map(|(_, source)| {
                            source
                                .starts_with("outputs.")
                                .then(|| Prerequisite::body(*source).ok())
                                .flatten()
                        }))
                        .collect::<Vec<_>>()
                        .into(),
                    inputs: inputs
                        .iter()
                        .map(|(name, source)| {
                            let source = if *source == "finalization.context" {
                                ResolvedValueSource::FinalizationContext
                            } else {
                                let (producer, output) = source
                                    .strip_prefix("outputs.")
                                    .and_then(|tail| tail.split_once('.'))
                                    .expect("fixture output reference");
                                ResolvedValueSource::Output(
                                    crate::execution::workflow::validated::ResolvedOutputSource {
                                        node: crate::execution::workflow::validated::WorkflowNode {
                                            id: producer.to_owned(),
                                            role: WorkflowNodeRole::Step,
                                        },
                                        output: output.to_owned(),
                                        value_type: crate::execution::workflow::validated::WorkflowValueType::File,
                                    },
                                )
                            };
                            ((*name).to_owned(), source)
                        })
                        .collect(),
                    declared_outputs: BTreeSet::new(),
                    recovery: None,
                    when: when.iter().copied().collect(),
                },
            )
        }))
        .collect();
    RuntimeDefinition {
        steps: runtime_steps,
        ordinary_ids,
        finalizer_ids,
        finalizer_presentation_order: finalizers.iter().map(|(id, ..)| (*id).to_owned()).collect(),
        exports: BTreeMap::new(),
        maximum_parallel_steps: NonZeroUsize::new(maximum_parallel_steps).unwrap(),
        maximum_transitions: 10_000,
        prompt: None,
    }
}

fn output_set(entries: &[(&str, &str)]) -> OutputSet<String> {
    entries
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect()
}

fn available_exports(entries: &[(&str, &str)]) -> ExportSet<String> {
    entries
        .iter()
        .map(|(name, output)| {
            (
                (*name).to_owned(),
                ExportValue::Available {
                    output: (*output).to_owned(),
                },
            )
        })
        .collect()
}

fn assert_step(state: &TestRuntimeState, step: &str, expected: StepStateKind) {
    assert_eq!(state.steps[step].state.kind(), expected);
}

fn assert_noop(before: &TestRuntimeState, reduction: &TestReduction) {
    assert_eq!(&reduction.state, before);
    assert!(reduction.events.is_empty());
    assert!(reduction.actions.is_empty());
}

fn reduce_and_advance(state: &mut TestRuntimeState, occurrence: TestOccurrence) -> TestReduction {
    let reduction = reduce(state, occurrence);
    *state = reduction.state.clone();
    reduction
}

fn reduce_ordered(
    state: &TestRuntimeState,
    last_ordinal: &mut u64,
    ordinal: u64,
    occurrence: TestOccurrence,
) -> TestReduction {
    assert!(ordinal > *last_ordinal);
    *last_ordinal = ordinal;
    reduce(state, occurrence)
}

fn reduce_ordered_and_advance(
    state: &mut TestRuntimeState,
    last_ordinal: &mut u64,
    ordinal: u64,
    occurrence: TestOccurrence,
) -> TestReduction {
    let reduction = reduce_ordered(state, last_ordinal, ordinal, occurrence);
    *state = reduction.state.clone();
    reduction
}

fn prepare_failure_phase(
    phase: FailurePhase,
) -> (TestRuntimeState, TestOccurrence, StepStateKind, u64) {
    let initialization = initialize_test(definition(
        &[
            ("aFail", &[], &["result"]),
            ("bStopped", &[], &["result"]),
            ("zJoin", &["bStopped", "aFail"], &["result"]),
            ("zzDescendant", &["zJoin"], &["result"]),
        ],
        &[],
        1,
    ));
    let mut state = initialization.state;
    let (occurrence, source_state, direct_sequence) = match phase {
        FailurePhase::Condition => panic!("condition failures do not enter target recovery"),
        FailurePhase::Start => (
            Occurrence::StepStartFailed {
                step: "aFail".to_owned(),
                action: action_id(1),
                cause: "reported cause".to_owned(),
            },
            StepStateKind::Starting,
            2,
        ),
        FailurePhase::Execution => {
            reduce_and_advance(
                &mut state,
                Occurrence::StepStarted {
                    step: "aFail".to_owned(),
                    action: action_id(1),
                },
            );
            (
                Occurrence::StepExecutionFailed {
                    step: "aFail".to_owned(),
                    action: action_id(1),
                    cause: "reported cause".to_owned(),
                },
                StepStateKind::Running,
                3,
            )
        }
        FailurePhase::OutputCapture => {
            reduce_and_advance(
                &mut state,
                Occurrence::StepStarted {
                    step: "aFail".to_owned(),
                    action: action_id(1),
                },
            );
            reduce_and_advance(
                &mut state,
                Occurrence::StepExecutionCompleted {
                    step: "aFail".to_owned(),
                    action: action_id(1),
                    provisional: "provisional".to_owned(),
                },
            );
            (
                Occurrence::OutputCaptureFailed {
                    step: "aFail".to_owned(),
                    action: action_id(3),
                    cause: "reported cause".to_owned(),
                },
                StepStateKind::CapturingOutputs,
                4,
            )
        }
    };
    (state, occurrence, source_state, direct_sequence)
}

#[test]
fn uncancelled_admitted_workflow_initializes_the_runtime_graph() {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&execution_root).unwrap();
    fs::write(
        source_root.join("workflow.yaml"),
        "schemaVersion: 1\nsteps:\n  zeta:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  alpha:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      report:\n        kind: file\n        from: path\n        path: report.txt\n        mediaType: text/plain\n",
    )
    .unwrap();
    let admitted = admit_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedImports::default(),
        ExecutionContext::new(
            execution_root,
            ExecutionRootLifecycle::EngineOwnedEphemeral,
            ExecutionPolicyLimits::new(
                1,
                CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
                InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                1024 * 1024,
            ),
            EnvironmentSnapshot::default(),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap();

    let initialization = initialize::<String, String, String, TestDeadline>(&admitted, None);

    assert_eq!(initialization.actions, [start_action(1, "alpha")]);
    assert_step(&initialization.state, "alpha", StepStateKind::Starting);
    assert_step(&initialization.state, "zeta", StepStateKind::Pending);
    assert!(initialization.state.exports.is_none());
}

#[test]
fn condition_false_precedes_body_readiness() {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&execution_root).unwrap();
    fs::write(
        source_root.join("workflow.yaml"),
        "schemaVersion: 1
steps:
  consumer:
    kind: cmd
    condition:
      equals:
        - ref: imports.prompt
        - value: run
    inputs:
      value:
        ref: outputs.producer.value
    command:
      argv: [\"true\"]
  producer:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: text
        from: path
        path: value.txt
",
    )
    .unwrap();
    let admitted = admit_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedImports::new(Some(Arc::from("skip")), Arc::from([])),
        ExecutionContext::new(
            execution_root,
            ExecutionRootLifecycle::EngineOwnedEphemeral,
            ExecutionPolicyLimits::new(
                1,
                CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
                InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                1024 * 1024,
            ),
            EnvironmentSnapshot::default(),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap();

    let initialization = initialize::<String, String, String, TestDeadline>(&admitted, None);
    assert_eq!(initialization.actions, [start_action(2, "producer")]);
    assert!(matches!(
        &initialization.state.steps["consumer"].state,
        StepState::Skipped { detail }
            if detail.evaluated_predicates.len() == 1
                && detail.evaluated_predicates[0].path.is_empty()
                && !detail.evaluated_predicates[0].result
    ));
}

#[test]
fn inferred_data_edges_drive_runtime_scheduling_and_blocking() {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&execution_root).unwrap();
    fs::write(
        source_root.join("workflow.yaml"),
        "schemaVersion: 1
steps:
  consumer:
    kind: cmd
    inputs:
      artifact:
        ref: outputs.producer.artifact
    command:
      argv: [\"true\"]
  producer:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      artifact:
        kind: file
        from: path
        path: artifact.txt
        mediaType: text/plain
",
    )
    .unwrap();
    let admitted = admit_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedImports::default(),
        ExecutionContext::new(
            execution_root,
            ExecutionRootLifecycle::EngineOwnedEphemeral,
            ExecutionPolicyLimits::new(
                2,
                CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
                InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                1024 * 1024,
            ),
            EnvironmentSnapshot::default(),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap();

    let initialization = initialize::<String, String, String, TestDeadline>(&admitted, None);
    assert_eq!(initialization.actions, [start_action(1, "producer")]);
    assert_step(&initialization.state, "consumer", StepStateKind::Pending);

    let reduction = reduce(
        &initialization.state,
        Occurrence::StepStartFailed {
            step: "producer".to_owned(),
            action: action_id(1),
            cause: "failed to start".to_owned(),
        },
    );
    let primary_issue = failure("producer", FailurePhase::Start, "failed to start");
    assert_eq!(
        reduction.state.steps["consumer"].state,
        blocked_state([Prerequisite::body("outputs.producer.artifact").unwrap()])
    );
    assert_eq!(
        reduction.state.workflow,
        WorkflowState::Failed {
            primary_issue: primary_issue.clone(),
            later_cancellation: None,
        }
    );
    assert_eq!(
        reduction.actions,
        [finish_failed_action(5, primary_issue, ExportSet::new())]
    );
}

#[test]
fn initial_cancellation_finishes_without_authorizing_a_start() {
    let reason = CancellationReason::UserRequest;
    let expected_exports = BTreeMap::from([
        (
            "childExport".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::Cancelled,
            },
        ),
        (
            "rootExport".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::Cancelled,
            },
        ),
    ]);
    let reduction = initialize_definition::<String, String, String, TestDeadline>(ExecutionStart {
        definition: definition(
            &[
                ("aRoot", &[], &["result"]),
                ("bChild", &["aRoot"], &["result"]),
            ],
            &[
                ("childExport", "bChild", "result"),
                ("rootExport", "aRoot", "result"),
            ],
            1,
        ),
        initial_cancellation: Some(cancellation(reason, 5_000)),
        initial_cancellation_operation: None,
    });
    let cancelling = cancelling_workflow(reason, None);

    assert_eq!(
        reduction.events,
        [
            cancellation_event(1, reason, 5_000),
            step_event(2, "aRoot", StepStateKind::Pending, StepStateKind::Cancelled,),
            step_event(
                3,
                "bChild",
                StepStateKind::Pending,
                StepStateKind::Cancelled,
            ),
            workflow_event(4, cancelling, WorkflowState::Cancelled { reason },),
        ]
    );
    assert_eq!(
        reduction.state.steps["aRoot"].state,
        cancelled_state(reason)
    );
    assert_eq!(
        reduction.state.steps["bChild"].state,
        cancelled_state(reason)
    );
    assert_eq!(
        reduction.state.workflow,
        WorkflowState::Cancelled { reason }
    );
    assert_eq!(reduction.state.exports, Some(expected_exports.clone()));
    assert_eq!(
        reduction.actions,
        [finish_cancelled_action(4, reason, expected_exports)]
    );
    assert!(
        reduction
            .actions
            .iter()
            .all(|action| !matches!(action.action, Action::StartStep { .. }))
    );
}

#[test]
fn runtime_cancellation_cancels_each_active_action_and_waits_for_quiescence() {
    let initialization = initialize_test(definition(
        &[
            ("aStarting", &[], &[]),
            ("bRunning", &[], &[]),
            ("cCapturing", &[], &["result"]),
            ("zWaiting", &["cCapturing"], &["result"]),
        ],
        &[
            ("capturingExport", "cCapturing", "result"),
            ("waitingExport", "zWaiting", "result"),
        ],
        3,
    ));
    assert_eq!(
        initialization.actions,
        [
            start_action(1, "aStarting"),
            start_action(2, "bRunning"),
            start_action(3, "cCapturing"),
        ]
    );
    let mut state = initialization.state;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "bRunning".to_owned(),
            action: action_id(2),
        },
    );
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "cCapturing".to_owned(),
            action: action_id(3),
        },
    );
    reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "cCapturing".to_owned(),
            action: action_id(3),
            provisional: "uncommitted".to_owned(),
        },
    );

    let reason = CancellationReason::RunnerShutdown;
    let mut ordinal = 0;
    let cancelled = reduce_ordered_and_advance(
        &mut state,
        &mut ordinal,
        10,
        Occurrence::CancellationRequested {
            reason,
            deadline: deadline(7_777),
        },
    );
    let cancelling = cancelling_workflow(reason, None);
    assert_eq!(
        cancelled.events,
        [
            cancellation_event(7, reason, 7_777),
            step_event(
                8,
                "aStarting",
                StepStateKind::Starting,
                StepStateKind::Cancelling,
            ),
            step_event(
                9,
                "bRunning",
                StepStateKind::Running,
                StepStateKind::Cancelling,
            ),
            step_event(
                10,
                "cCapturing",
                StepStateKind::CapturingOutputs,
                StepStateKind::Cancelling,
            ),
            step_event(
                11,
                "zWaiting",
                StepStateKind::Pending,
                StepStateKind::Cancelled,
            ),
        ]
    );
    assert_eq!(
        cancelled.actions,
        [
            cancel_action(8, "aStarting", reason, 7_777),
            cancel_action(9, "bRunning", reason, 7_777),
            cancel_action(10, "cCapturing", reason, 7_777),
        ]
    );
    assert_eq!(state.workflow, cancelling);
    assert!(state.exports.is_none());

    let duplicate_cancellation = reduce_ordered(
        &state,
        &mut ordinal,
        11,
        Occurrence::CancellationRequested {
            reason: CancellationReason::UserRequest,
            deadline: deadline(9_999),
        },
    );
    assert_noop(&state, &duplicate_cancellation);

    let first_quiesced = reduce_ordered_and_advance(
        &mut state,
        &mut ordinal,
        12,
        Occurrence::StepQuiesced {
            step: "aStarting".to_owned(),
            action: action_id(8),
        },
    );
    assert_eq!(
        first_quiesced.events,
        [step_event(
            12,
            "aStarting",
            StepStateKind::Cancelling,
            StepStateKind::Cancelled,
        )]
    );
    assert!(first_quiesced.actions.is_empty());
    assert_eq!(state.workflow, cancelling_workflow(reason, None));

    for (next_ordinal, occurrence) in [
        (
            13,
            Occurrence::StepQuiesced {
                step: "aStarting".to_owned(),
                action: action_id(8),
            },
        ),
        (
            14,
            Occurrence::StepExecutionCompleted {
                step: "bRunning".to_owned(),
                action: action_id(2),
                provisional: "late completion".to_owned(),
            },
        ),
        (
            15,
            Occurrence::OutputsCaptured {
                step: "cCapturing".to_owned(),
                action: action_id(6),
                outputs: output_set(&[("result", "late output")]),
            },
        ),
    ] {
        let stale = reduce_ordered(&state, &mut ordinal, next_ordinal, occurrence);
        assert_noop(&state, &stale);
    }

    let second_quiesced = reduce_ordered_and_advance(
        &mut state,
        &mut ordinal,
        16,
        Occurrence::StepQuiesced {
            step: "bRunning".to_owned(),
            action: action_id(9),
        },
    );
    assert_eq!(
        second_quiesced.events,
        [step_event(
            13,
            "bRunning",
            StepStateKind::Cancelling,
            StepStateKind::Cancelled,
        )]
    );
    assert!(second_quiesced.actions.is_empty());
    assert_eq!(state.workflow, cancelling_workflow(reason, None));

    let final_quiesced = reduce_ordered_and_advance(
        &mut state,
        &mut ordinal,
        17,
        Occurrence::StepQuiesced {
            step: "cCapturing".to_owned(),
            action: action_id(10),
        },
    );
    let expected_exports = BTreeMap::from([
        (
            "capturingExport".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::Cancelled,
            },
        ),
        (
            "waitingExport".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::Cancelled,
            },
        ),
    ]);
    assert_eq!(
        final_quiesced.events,
        [
            step_event(
                14,
                "cCapturing",
                StepStateKind::Cancelling,
                StepStateKind::Cancelled,
            ),
            workflow_event(
                15,
                cancelling_workflow(reason, None),
                WorkflowState::Cancelled { reason },
            ),
        ]
    );
    assert_eq!(
        final_quiesced.actions,
        [finish_cancelled_action(
            15,
            reason,
            expected_exports.clone()
        )]
    );
    assert_eq!(state.exports, Some(expected_exports));

    for occurrence in [
        Occurrence::StepQuiesced {
            step: "cCapturing".to_owned(),
            action: action_id(10),
        },
        Occurrence::CancellationRequested {
            reason: CancellationReason::UserRequest,
            deadline: deadline(12_345),
        },
        Occurrence::StepExecutionFailed {
            step: "bRunning".to_owned(),
            action: action_id(2),
            cause: "terminal replay".to_owned(),
        },
    ] {
        let replay = reduce(&state, occurrence);
        assert_noop(&state, &replay);
    }
}

#[test]
fn empty_dag_finishes_during_initialization() {
    let reduction = initialize_test(definition(&[], &[], 1));

    assert_eq!(reduction.state.workflow, WorkflowState::Succeeded);
    assert!(reduction.state.steps.is_empty());
    assert_eq!(reduction.state.exports, Some(BTreeMap::new()));
    assert_eq!(reduction.state.last_transition_sequence, sequence(1));
    assert_eq!(reduction.events, [workflow_succeeded_event(1)]);
    assert_eq!(reduction.actions, [finish_action(1, BTreeMap::new())]);
}

#[test]
fn serial_steps_follow_every_success_state_and_identifier() {
    let initialization =
        initialize_test(definition(&[("a", &[], &[]), ("b", &["a"], &[])], &[], 2));
    assert_eq!(
        initialization.events,
        [step_event(
            1,
            "a",
            StepStateKind::Pending,
            StepStateKind::Starting
        )]
    );
    assert_eq!(initialization.actions, [start_action(1, "a")]);
    assert_step(&initialization.state, "a", StepStateKind::Starting);
    assert_step(&initialization.state, "b", StepStateKind::Pending);

    let mut state = initialization.state;
    let started = reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "a".to_owned(),
            action: action_id(1),
        },
    );
    assert_eq!(
        started.events,
        [step_event(
            2,
            "a",
            StepStateKind::Starting,
            StepStateKind::Running
        )]
    );
    assert!(started.actions.is_empty());

    let first_completed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "a".to_owned(),
            action: action_id(1),
            provisional: "ignored-a".to_owned(),
        },
    );
    assert_eq!(
        first_completed.events,
        [
            step_event(
                3,
                "a",
                StepStateKind::Running,
                StepStateKind::CapturingOutputs,
            ),
            step_event(
                4,
                "a",
                StepStateKind::CapturingOutputs,
                StepStateKind::Succeeded,
            ),
            step_event(5, "b", StepStateKind::Pending, StepStateKind::Starting,),
        ]
    );
    assert_eq!(first_completed.actions, [start_action(5, "b")]);
    assert_step(&state, "a", StepStateKind::Succeeded);
    assert_step(&state, "b", StepStateKind::Starting);

    let second_started = reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "b".to_owned(),
            action: action_id(5),
        },
    );
    assert_eq!(
        second_started.events,
        [step_event(
            6,
            "b",
            StepStateKind::Starting,
            StepStateKind::Running
        )]
    );

    let second_completed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "b".to_owned(),
            action: action_id(5),
            provisional: "ignored-b".to_owned(),
        },
    );
    assert_eq!(
        second_completed.events,
        [
            step_event(
                7,
                "b",
                StepStateKind::Running,
                StepStateKind::CapturingOutputs,
            ),
            step_event(
                8,
                "b",
                StepStateKind::CapturingOutputs,
                StepStateKind::Succeeded,
            ),
            workflow_succeeded_event(9),
        ]
    );
    assert_eq!(
        second_completed.actions,
        [finish_action(9, BTreeMap::new())]
    );
    assert_eq!(state.workflow, WorkflowState::Succeeded);
    assert_eq!(state.last_transition_sequence, sequence(9));

    assert_eq!(state.exports, Some(ExportSet::new()));
}

#[test]
fn branching_dependents_wait_for_the_producers_committed_outputs() {
    let initialization = initialize_test(definition(
        &[
            ("root", &[], &["artifact"]),
            ("zeta", &["root"], &[]),
            ("alpha", &["root"], &[]),
            ("join", &["alpha", "zeta"], &[]),
        ],
        &[],
        2,
    ));
    assert_eq!(initialization.actions, [start_action(1, "root")]);
    let mut state = initialization.state;

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "root".to_owned(),
            action: action_id(1),
        },
    );
    let execution_completed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "root".to_owned(),
            action: action_id(1),
            provisional: "provisional-artifact".to_owned(),
        },
    );
    assert_eq!(
        execution_completed.events,
        [step_event(
            3,
            "root",
            StepStateKind::Running,
            StepStateKind::CapturingOutputs,
        )]
    );
    assert_eq!(
        execution_completed.actions,
        [capture_action(3, "root", "provisional-artifact")]
    );
    assert_step(&state, "alpha", StepStateKind::Pending);
    assert_step(&state, "zeta", StepStateKind::Pending);

    let captured = reduce_and_advance(
        &mut state,
        Occurrence::OutputsCaptured {
            step: "root".to_owned(),
            action: action_id(3),
            outputs: output_set(&[("artifact", "committed-artifact")]),
        },
    );
    assert_eq!(
        captured.events,
        [
            step_event(
                4,
                "root",
                StepStateKind::CapturingOutputs,
                StepStateKind::Succeeded,
            ),
            step_event(5, "alpha", StepStateKind::Pending, StepStateKind::Starting,),
            step_event(6, "zeta", StepStateKind::Pending, StepStateKind::Starting,),
        ]
    );
    assert_eq!(
        captured.actions,
        [start_action(5, "alpha"), start_action(6, "zeta")]
    );

    for (step, start_id) in [("alpha", 5), ("zeta", 6)] {
        reduce_and_advance(
            &mut state,
            Occurrence::StepStarted {
                step: step.to_owned(),
                action: action_id(start_id),
            },
        );
    }
    let alpha_completed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "alpha".to_owned(),
            action: action_id(5),
            provisional: "ignored".to_owned(),
        },
    );
    assert!(alpha_completed.actions.is_empty());
    assert_step(&state, "join", StepStateKind::Pending);

    let zeta_completed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "zeta".to_owned(),
            action: action_id(6),
            provisional: "ignored".to_owned(),
        },
    );
    assert_eq!(zeta_completed.actions, [start_action(13, "join")]);
    assert_eq!(
        zeta_completed.events.last(),
        Some(&step_event(
            13,
            "join",
            StepStateKind::Pending,
            StepStateKind::Starting,
        ))
    );
}

#[test]
fn ready_selection_is_lexical_and_respects_parallelism() {
    let initialization = initialize_test(definition(
        &[
            ("zeta", &[], &[]),
            ("alpha", &[], &[]),
            ("middle", &[], &[]),
        ],
        &[],
        2,
    ));
    assert_eq!(
        initialization.actions,
        [start_action(1, "alpha"), start_action(2, "middle")]
    );
    assert_step(&initialization.state, "zeta", StepStateKind::Pending);
    assert_eq!(
        initialization
            .state
            .steps
            .values()
            .filter(|step| step.state.is_active())
            .count(),
        2
    );

    let mut state = initialization.state;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "alpha".to_owned(),
            action: action_id(1),
        },
    );
    let completed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "alpha".to_owned(),
            action: action_id(1),
            provisional: "ignored".to_owned(),
        },
    );
    assert_eq!(completed.actions, [start_action(6, "zeta")]);
    assert_eq!(
        state
            .steps
            .values()
            .filter(|step| step.state.is_active())
            .count(),
        2
    );
}

#[test]
fn successful_exports_are_committed_to_state_and_the_only_finish_action() {
    let initialization = initialize_test(definition(
        &[("producer", &[], &["result"])],
        &[("publicResult", "producer", "result")],
        1,
    ));
    let mut state = initialization.state;
    let mut actions = initialization.actions;

    let started = reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "producer".to_owned(),
            action: action_id(1),
        },
    );
    actions.extend(started.actions);
    let completed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "producer".to_owned(),
            action: action_id(1),
            provisional: "provisional-result".to_owned(),
        },
    );
    actions.extend(completed.actions);
    let captured = reduce_and_advance(
        &mut state,
        Occurrence::OutputsCaptured {
            step: "producer".to_owned(),
            action: action_id(3),
            outputs: output_set(&[("result", "committed-result")]),
        },
    );
    actions.extend(captured.actions.clone());

    let expected_exports = available_exports(&[("publicResult", "committed-result")]);
    assert_eq!(state.workflow, WorkflowState::Succeeded);
    assert_eq!(state.exports, Some(expected_exports.clone()));
    assert_eq!(captured.events.last(), Some(&workflow_succeeded_event(5)));
    assert!(!captured.events.iter().any(|event| matches!(
        event,
        TransitionEvent::Workflow { to, .. }
            if matches!(to.as_ref(), WorkflowState::Finalizing { .. })
    )));
    assert_eq!(captured.actions, [finish_action(5, expected_exports)]);
    assert_eq!(
        actions
            .iter()
            .filter(|action| matches!(action.action, Action::FinishRun { .. }))
            .count(),
        1
    );

    assert_eq!(state.last_transition_sequence, sequence(5));
    assert!(state.finalization_summary.is_none());

    let mut last_ordinal = 3;
    let late_cancellation = reduce_ordered(
        &state,
        &mut last_ordinal,
        4,
        Occurrence::CancellationRequested {
            reason: CancellationReason::UserRequest,
            deadline: deadline(4_444),
        },
    );
    assert_noop(&state, &late_cancellation);
}

#[test]
fn duplicate_and_stale_success_occurrences_are_noops() {
    let initialization = initialize_test(definition(&[("producer", &[], &["result"])], &[], 1));
    let mut state = initialization.state;

    let stale_start = reduce(
        &state,
        Occurrence::StepStarted {
            step: "producer".to_owned(),
            action: action_id(999),
        },
    );
    assert_noop(&state, &stale_start);

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "producer".to_owned(),
            action: action_id(1),
        },
    );
    let duplicate_start = reduce(
        &state,
        Occurrence::StepStarted {
            step: "producer".to_owned(),
            action: action_id(1),
        },
    );
    assert_noop(&state, &duplicate_start);
    let premature_capture = reduce(
        &state,
        Occurrence::OutputsCaptured {
            step: "producer".to_owned(),
            action: action_id(1),
            outputs: output_set(&[("result", "premature")]),
        },
    );
    assert_noop(&state, &premature_capture);

    reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "producer".to_owned(),
            action: action_id(1),
            provisional: "provisional".to_owned(),
        },
    );
    let duplicate_completion = reduce(
        &state,
        Occurrence::StepExecutionCompleted {
            step: "producer".to_owned(),
            action: action_id(1),
            provisional: "duplicate".to_owned(),
        },
    );
    assert_noop(&state, &duplicate_completion);
    let superseded_capture_action = reduce(
        &state,
        Occurrence::OutputsCaptured {
            step: "producer".to_owned(),
            action: action_id(1),
            outputs: output_set(&[("result", "stale")]),
        },
    );
    assert_noop(&state, &superseded_capture_action);
    let missing_declared_output = reduce(
        &state,
        Occurrence::OutputsCaptured {
            step: "producer".to_owned(),
            action: action_id(3),
            outputs: output_set(&[]),
        },
    );
    assert_noop(&state, &missing_declared_output);
    let undeclared_output = reduce(
        &state,
        Occurrence::OutputsCaptured {
            step: "producer".to_owned(),
            action: action_id(3),
            outputs: output_set(&[("result", "committed"), ("extra", "undeclared")]),
        },
    );
    assert_noop(&state, &undeclared_output);

    reduce_and_advance(
        &mut state,
        Occurrence::OutputsCaptured {
            step: "producer".to_owned(),
            action: action_id(3),
            outputs: output_set(&[("result", "committed")]),
        },
    );
    let terminal_state = state.clone();
    let duplicate_capture = reduce(
        &state,
        Occurrence::OutputsCaptured {
            step: "producer".to_owned(),
            action: action_id(3),
            outputs: output_set(&[("result", "duplicate")]),
        },
    );
    assert_noop(&terminal_state, &duplicate_capture);
}

#[test]
fn failure_first_remains_failed_when_later_cancellation_stops_a_sibling() {
    let initialization = initialize_test(definition(
        &[
            ("aCommitted", &[], &["result"]),
            ("bFail", &[], &["result"]),
            ("cActive", &[], &["result"]),
        ],
        &[
            ("activeExport", "cActive", "result"),
            ("committedExport", "aCommitted", "result"),
            ("failedExport", "bFail", "result"),
        ],
        3,
    ));
    let mut state = initialization.state;
    let mut ordinal = 0;
    for (next_ordinal, occurrence) in [
        (
            1,
            Occurrence::StepStarted {
                step: "aCommitted".to_owned(),
                action: action_id(1),
            },
        ),
        (
            2,
            Occurrence::StepExecutionCompleted {
                step: "aCommitted".to_owned(),
                action: action_id(1),
                provisional: "provisional".to_owned(),
            },
        ),
        (
            3,
            Occurrence::OutputsCaptured {
                step: "aCommitted".to_owned(),
                action: action_id(5),
                outputs: output_set(&[("result", "committed")]),
            },
        ),
        (
            4,
            Occurrence::StepStarted {
                step: "bFail".to_owned(),
                action: action_id(2),
            },
        ),
        (
            5,
            Occurrence::StepStarted {
                step: "cActive".to_owned(),
                action: action_id(3),
            },
        ),
    ] {
        reduce_ordered_and_advance(&mut state, &mut ordinal, next_ordinal, occurrence);
    }

    let failed = reduce_ordered_and_advance(
        &mut state,
        &mut ordinal,
        6,
        Occurrence::StepExecutionFailed {
            step: "bFail".to_owned(),
            action: action_id(2),
            cause: "primary".to_owned(),
        },
    );
    let primary_issue = failure("bFail", FailurePhase::Execution, "primary");
    let failure_stopped = WorkflowState::Executing {
        gate: SchedulingGate::FailureStopped {
            primary_issue: primary_issue.clone(),
        },
    };
    assert_eq!(
        failed.events,
        [
            step_event(9, "bFail", StepStateKind::Running, StepStateKind::Failed,),
            workflow_event(
                10,
                WorkflowState::Executing {
                    gate: SchedulingGate::Open,
                },
                failure_stopped.clone(),
            ),
        ]
    );
    assert!(failed.actions.is_empty());

    let reason = CancellationReason::RunnerShutdown;
    let cancellation = reduce_ordered_and_advance(
        &mut state,
        &mut ordinal,
        7,
        Occurrence::CancellationRequested {
            reason,
            deadline: deadline(8_888),
        },
    );
    let cancelling = cancelling_workflow(reason, Some(primary_issue.clone()));
    assert_eq!(
        cancellation.events,
        [
            cancellation_event(11, reason, 8_888),
            step_event(
                12,
                "cActive",
                StepStateKind::Running,
                StepStateKind::Cancelling,
            ),
        ]
    );
    assert_eq!(
        cancellation.actions,
        [cancel_action(12, "cActive", reason, 8_888)]
    );

    let finished = reduce_ordered_and_advance(
        &mut state,
        &mut ordinal,
        8,
        Occurrence::StepQuiesced {
            step: "cActive".to_owned(),
            action: action_id(12),
        },
    );
    let expected_exports = BTreeMap::from([
        (
            "activeExport".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::Cancelled,
            },
        ),
        (
            "committedExport".to_owned(),
            ExportValue::Available {
                output: "committed".to_owned(),
            },
        ),
        (
            "failedExport".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::Failed,
            },
        ),
    ]);
    let terminal = WorkflowState::Failed {
        primary_issue: primary_issue.clone(),
        later_cancellation: Some(reason),
    };
    assert_eq!(
        finished.events,
        [
            step_event(
                13,
                "cActive",
                StepStateKind::Cancelling,
                StepStateKind::Cancelled,
            ),
            workflow_event(14, cancelling, terminal.clone()),
        ]
    );
    assert_eq!(
        finished.actions,
        [finish_failed_after_cancellation_action(
            14,
            primary_issue,
            Some(reason),
            expected_exports.clone(),
        )]
    );
    assert_eq!(state.workflow, terminal);
    assert_eq!(state.exports, Some(expected_exports));
    assert_eq!(
        state.steps["aCommitted"].state,
        StepState::Succeeded {
            outputs: output_set(&[("result", "committed")]),
        }
    );
}

#[test]
fn cancellation_first_makes_a_later_failure_stale() {
    let initialization = initialize_test(definition(
        &[("aFail", &[], &[]), ("bActive", &[], &[])],
        &[],
        2,
    ));
    let mut state = initialization.state;
    let mut ordinal = 0;
    for (next_ordinal, step, action) in [(1, "aFail", 1), (2, "bActive", 2)] {
        reduce_ordered_and_advance(
            &mut state,
            &mut ordinal,
            next_ordinal,
            Occurrence::StepStarted {
                step: step.to_owned(),
                action: action_id(action),
            },
        );
    }

    let reason = CancellationReason::UserRequest;
    let cancelled = reduce_ordered_and_advance(
        &mut state,
        &mut ordinal,
        3,
        Occurrence::CancellationRequested {
            reason,
            deadline: deadline(9_001),
        },
    );
    assert_eq!(
        cancelled.actions,
        [
            cancel_action(6, "aFail", reason, 9_001),
            cancel_action(7, "bActive", reason, 9_001),
        ]
    );

    let late_failure = reduce_ordered(
        &state,
        &mut ordinal,
        4,
        Occurrence::StepExecutionFailed {
            step: "aFail".to_owned(),
            action: action_id(1),
            cause: "too late".to_owned(),
        },
    );
    assert_noop(&state, &late_failure);

    let first_quiesced = reduce_ordered_and_advance(
        &mut state,
        &mut ordinal,
        5,
        Occurrence::StepQuiesced {
            step: "aFail".to_owned(),
            action: action_id(6),
        },
    );
    assert!(first_quiesced.actions.is_empty());
    assert_eq!(state.workflow, cancelling_workflow(reason, None));

    let finished = reduce_ordered_and_advance(
        &mut state,
        &mut ordinal,
        6,
        Occurrence::StepQuiesced {
            step: "bActive".to_owned(),
            action: action_id(7),
        },
    );
    assert_eq!(
        finished.events,
        [
            step_event(
                9,
                "bActive",
                StepStateKind::Cancelling,
                StepStateKind::Cancelled,
            ),
            workflow_event(
                10,
                cancelling_workflow(reason, None),
                WorkflowState::Cancelled { reason },
            ),
        ]
    );
    assert_eq!(
        finished.actions,
        [finish_cancelled_action(10, reason, ExportSet::new())]
    );
    assert_eq!(state.workflow, WorkflowState::Cancelled { reason });
}

#[test]
fn committed_success_survives_later_cancellation() {
    let initialization = initialize_test(definition(
        &[
            ("aCommitted", &[], &["result"]),
            ("bActive", &[], &["result"]),
        ],
        &[
            ("activeExport", "bActive", "result"),
            ("committedExport", "aCommitted", "result"),
        ],
        2,
    ));
    let mut state = initialization.state;
    let mut ordinal = 0;
    for (next_ordinal, occurrence) in [
        (
            1,
            Occurrence::StepStarted {
                step: "aCommitted".to_owned(),
                action: action_id(1),
            },
        ),
        (
            2,
            Occurrence::StepExecutionCompleted {
                step: "aCommitted".to_owned(),
                action: action_id(1),
                provisional: "provisional".to_owned(),
            },
        ),
        (
            3,
            Occurrence::OutputsCaptured {
                step: "aCommitted".to_owned(),
                action: action_id(4),
                outputs: output_set(&[("result", "committed")]),
            },
        ),
    ] {
        reduce_ordered_and_advance(&mut state, &mut ordinal, next_ordinal, occurrence);
    }

    let reason = CancellationReason::UserRequest;
    let cancellation = reduce_ordered_and_advance(
        &mut state,
        &mut ordinal,
        4,
        Occurrence::CancellationRequested {
            reason,
            deadline: deadline(22_222),
        },
    );
    assert_eq!(
        cancellation.actions,
        [cancel_action(7, "bActive", reason, 22_222)]
    );
    assert_eq!(
        state.steps["aCommitted"].state,
        StepState::Succeeded {
            outputs: output_set(&[("result", "committed")]),
        }
    );

    let stale_start = reduce_ordered(
        &state,
        &mut ordinal,
        5,
        Occurrence::StepStarted {
            step: "bActive".to_owned(),
            action: action_id(2),
        },
    );
    assert_noop(&state, &stale_start);

    let finished = reduce_ordered_and_advance(
        &mut state,
        &mut ordinal,
        6,
        Occurrence::StepQuiesced {
            step: "bActive".to_owned(),
            action: action_id(7),
        },
    );
    let expected_exports = BTreeMap::from([
        (
            "activeExport".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::Cancelled,
            },
        ),
        (
            "committedExport".to_owned(),
            ExportValue::Available {
                output: "committed".to_owned(),
            },
        ),
    ]);
    assert_eq!(
        finished.actions,
        [finish_cancelled_action(9, reason, expected_exports.clone())]
    );
    assert_eq!(state.exports, Some(expected_exports));
}

#[test]
fn every_failure_phase_closes_scheduling_and_reaches_the_fixed_point() {
    for phase in [
        FailurePhase::Start,
        FailurePhase::Execution,
        FailurePhase::OutputCapture,
    ] {
        let (state, occurrence, source_state, direct_sequence) = prepare_failure_phase(phase);
        let duplicate = occurrence.clone();
        let reduction = reduce(&state, occurrence);
        let primary_issue = failure("aFail", phase, "reported cause");
        let failure_stopped = WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped {
                primary_issue: primary_issue.clone(),
            },
        };
        let terminal = WorkflowState::Failed {
            primary_issue: primary_issue.clone(),
            later_cancellation: None,
        };

        assert_eq!(
            reduction.events,
            [
                step_event(
                    direct_sequence,
                    "aFail",
                    source_state,
                    StepStateKind::Failed,
                ),
                workflow_event(
                    direct_sequence + 1,
                    WorkflowState::Executing {
                        gate: SchedulingGate::Open,
                    },
                    failure_stopped.clone(),
                ),
                step_event(
                    direct_sequence + 2,
                    "bStopped",
                    StepStateKind::Pending,
                    StepStateKind::NotRun,
                ),
                step_event(
                    direct_sequence + 3,
                    "zJoin",
                    StepStateKind::Pending,
                    StepStateKind::Blocked,
                ),
                step_event(
                    direct_sequence + 4,
                    "zzDescendant",
                    StepStateKind::Pending,
                    StepStateKind::Blocked,
                ),
                workflow_event(direct_sequence + 5, failure_stopped, terminal.clone(),),
            ]
        );
        assert_eq!(
            reduction.state.steps["aFail"].state,
            failed_state(phase, "reported cause")
        );
        assert_eq!(
            reduction.state.steps["bStopped"].state,
            not_run_state(WorkflowNodeRole::Step, NonExecutionCode::FailureStop)
        );
        assert_eq!(
            reduction.state.steps["zJoin"].state,
            blocked_state([
                Prerequisite::control("aFail").unwrap(),
                Prerequisite::control("bStopped").unwrap(),
            ])
        );
        assert_eq!(
            reduction.state.steps["zzDescendant"].state,
            blocked_state([Prerequisite::control("zJoin").unwrap()])
        );
        assert_eq!(reduction.state.workflow, terminal);
        assert_eq!(
            reduction.actions,
            [finish_failed_action(
                direct_sequence + 5,
                primary_issue,
                ExportSet::new(),
            )]
        );
        assert!(
            reduction
                .actions
                .iter()
                .all(|action| !matches!(action.action, Action::StartStep { .. }))
        );

        let duplicate_reduction = reduce(&reduction.state, duplicate);
        assert_noop(&reduction.state, &duplicate_reduction);
    }
}

#[test]
fn later_active_failure_does_not_replace_the_primary_issue() {
    let initialization = initialize_test(definition(
        &[("alpha", &[], &["result"]), ("zeta", &[], &["result"])],
        &[],
        2,
    ));
    assert_eq!(
        initialization.actions,
        [start_action(1, "alpha"), start_action(2, "zeta")]
    );
    let mut state = initialization.state;

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "alpha".to_owned(),
            action: action_id(1),
        },
    );
    reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "alpha".to_owned(),
            action: action_id(1),
            provisional: "alpha provisional".to_owned(),
        },
    );
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "zeta".to_owned(),
            action: action_id(2),
        },
    );

    let first = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "zeta".to_owned(),
            action: action_id(2),
            cause: "first failure".to_owned(),
        },
    );
    let primary_issue = failure("zeta", FailurePhase::Execution, "first failure");
    let failure_stopped = WorkflowState::Executing {
        gate: SchedulingGate::FailureStopped {
            primary_issue: primary_issue.clone(),
        },
    };
    assert_eq!(state.workflow, failure_stopped);
    assert!(first.actions.is_empty());
    assert_step(&state, "alpha", StepStateKind::CapturingOutputs);

    let duplicate = reduce(
        &state,
        Occurrence::StepExecutionFailed {
            step: "zeta".to_owned(),
            action: action_id(2),
            cause: "duplicate failure".to_owned(),
        },
    );
    assert_noop(&state, &duplicate);

    let later = reduce_and_advance(
        &mut state,
        Occurrence::OutputCaptureFailed {
            step: "alpha".to_owned(),
            action: action_id(4),
            cause: "later failure".to_owned(),
        },
    );
    let terminal = WorkflowState::Failed {
        primary_issue: primary_issue.clone(),
        later_cancellation: None,
    };
    assert_eq!(
        later.events,
        [
            step_event(
                8,
                "alpha",
                StepStateKind::CapturingOutputs,
                StepStateKind::Failed,
            ),
            workflow_event(9, failure_stopped, terminal.clone()),
        ]
    );
    assert_eq!(
        state.steps["alpha"].state,
        failed_state(FailurePhase::OutputCapture, "later failure")
    );
    assert_eq!(state.workflow, terminal);
    assert_eq!(
        later.actions,
        [finish_failed_action(9, primary_issue, ExportSet::new())]
    );
}

#[test]
fn successful_sibling_outputs_and_export_reasons_survive_failure() {
    let initialization = initialize_test(definition(
        &[
            ("aFail", &[], &["result"]),
            ("aFailChild", &["aFail"], &["result"]),
            ("bSibling", &[], &["result"]),
            ("bSiblingChild", &["bSibling"], &["result"]),
            ("zQueued", &[], &["result"]),
        ],
        &[
            ("blockedExport", "aFailChild", "result"),
            ("failedExport", "aFail", "result"),
            ("notRunExport", "zQueued", "result"),
            ("siblingChildExport", "bSiblingChild", "result"),
            ("siblingExport", "bSibling", "result"),
        ],
        2,
    ));
    assert_eq!(
        initialization.actions,
        [start_action(1, "aFail"), start_action(2, "bSibling")]
    );
    let mut state = initialization.state;
    for (step, action) in [("aFail", 1), ("bSibling", 2)] {
        reduce_and_advance(
            &mut state,
            Occurrence::StepStarted {
                step: step.to_owned(),
                action: action_id(action),
            },
        );
    }

    let failed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "aFail".to_owned(),
            action: action_id(1),
            cause: "branch failed".to_owned(),
        },
    );
    assert!(failed.actions.is_empty());
    assert_step(&state, "aFailChild", StepStateKind::Blocked);
    assert_step(&state, "bSibling", StepStateKind::Running);
    assert_step(&state, "bSiblingChild", StepStateKind::Pending);
    assert_step(&state, "zQueued", StepStateKind::NotRun);

    let completed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "bSibling".to_owned(),
            action: action_id(2),
            provisional: "sibling provisional".to_owned(),
        },
    );
    assert_eq!(
        completed.actions,
        [capture_action(9, "bSibling", "sibling provisional")]
    );
    assert_step(&state, "bSiblingChild", StepStateKind::Pending);

    let captured = reduce_and_advance(
        &mut state,
        Occurrence::OutputsCaptured {
            step: "bSibling".to_owned(),
            action: action_id(9),
            outputs: output_set(&[("result", "sibling committed")]),
        },
    );
    let primary_issue = failure("aFail", FailurePhase::Execution, "branch failed");
    let expected_exports = BTreeMap::from([
        (
            "blockedExport".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::Blocked,
            },
        ),
        (
            "failedExport".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::Failed,
            },
        ),
        (
            "notRunExport".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::NotRun,
            },
        ),
        (
            "siblingChildExport".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::NotRun,
            },
        ),
        (
            "siblingExport".to_owned(),
            ExportValue::Available {
                output: "sibling committed".to_owned(),
            },
        ),
    ]);

    assert_eq!(
        state.steps["bSibling"].state,
        StepState::Succeeded {
            outputs: output_set(&[("result", "sibling committed")]),
        }
    );
    assert_eq!(
        state.steps["bSiblingChild"].state,
        not_run_state(WorkflowNodeRole::Step, NonExecutionCode::FailureStop)
    );
    assert_eq!(
        state.workflow,
        WorkflowState::Failed {
            primary_issue: primary_issue.clone(),
            later_cancellation: None,
        }
    );
    assert_eq!(state.exports, Some(expected_exports.clone()));
    assert_eq!(
        captured.actions,
        [finish_failed_action(12, primary_issue, expected_exports,)]
    );
    assert_eq!(
        failed
            .actions
            .iter()
            .chain(&completed.actions)
            .chain(&captured.actions)
            .filter(|action| matches!(action.action, Action::FinishRun { .. }))
            .count(),
        1
    );
}

#[test]
fn advisory_failure_satisfies_control_and_keeps_the_gate_open() {
    let mut runtime_definition =
        definition(&[("lint", &[], &[]), ("package", &["lint"], &[])], &[], 1);
    runtime_definition
        .steps
        .get_mut("lint")
        .unwrap()
        .failure_policy = FailurePolicy::Advisory;
    let initialization = initialize_test(runtime_definition);
    assert_eq!(initialization.actions, [start_action(1, "lint")]);
    let mut state = initialization.state;

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "lint".to_owned(),
            action: action_id(1),
        },
    );
    let failed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "lint".to_owned(),
            action: action_id(1),
            cause: "lint failed".to_owned(),
        },
    );

    assert_eq!(
        state.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        }
    );
    assert_step(&state, "lint", StepStateKind::Failed);
    assert_step(&state, "package", StepStateKind::Starting);
    assert!(failed.events.iter().any(|event| matches!(
        event,
        TransitionEvent::Step {
            step,
            failure_policy: FailurePolicy::Advisory,
            from: StepStateKind::Running,
            to: StepStateKind::Failed,
            ..
        } if step == "lint"
    )));
    assert_eq!(failed.actions, [start_action(4, "package")]);

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "package".to_owned(),
            action: action_id(4),
        },
    );
    let completed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "package".to_owned(),
            action: action_id(4),
            provisional: "unused".to_owned(),
        },
    );
    assert_eq!(state.workflow, WorkflowState::Succeeded);
    assert!(matches!(
        completed.actions.as_slice(),
        [RequestedAction {
            action: Action::FinishRun {
                outcome: RunOutcome::Succeeded,
                ..
            },
            ..
        }]
    ));
}

#[test]
fn advisory_failure_blocks_data_without_synthesis_and_satisfies_later_control() {
    let mut runtime_definition = definition(
        &[
            ("analyze", &[], &["report"]),
            ("summarize", &["analyze"], &[]),
            ("package", &["summarize"], &[]),
        ],
        &[],
        1,
    );
    runtime_definition
        .steps
        .get_mut("analyze")
        .unwrap()
        .failure_policy = FailurePolicy::Advisory;
    let summarize = runtime_definition.steps.get_mut("summarize").unwrap();
    summarize.failure_policy = FailurePolicy::Advisory;
    summarize.prerequisites = Arc::from([ResolvedDirectPrerequisite {
        producer: "analyze".to_owned(),
        control: true,
        disposition_control: false,
        data: true,
        condition_data: false,
    }]);
    summarize.evidence_prerequisites = Arc::from([
        Prerequisite::control("analyze").unwrap(),
        Prerequisite::body("outputs.analyze.report").unwrap(),
    ]);
    let initialization = initialize_test(runtime_definition);
    let mut state = initialization.state;

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "analyze".to_owned(),
            action: action_id(1),
        },
    );
    let failed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "analyze".to_owned(),
            action: action_id(1),
            cause: "analysis failed".to_owned(),
        },
    );

    assert_step(&state, "analyze", StepStateKind::Failed);
    assert_eq!(
        state.steps["summarize"].state,
        blocked_state([Prerequisite::body("outputs.analyze.report").unwrap()])
    );
    assert_step(&state, "package", StepStateKind::Starting);
    assert_eq!(
        state.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        }
    );
    assert_eq!(failed.actions, [start_action(5, "package")]);

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "package".to_owned(),
            action: action_id(5),
        },
    );
    let completed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "package".to_owned(),
            action: action_id(5),
            provisional: "unused".to_owned(),
        },
    );
    assert_eq!(state.workflow, WorkflowState::Succeeded);
    assert!(matches!(
        completed.actions.as_slice(),
        [RequestedAction {
            action: Action::FinishRun {
                outcome: RunOutcome::Succeeded,
                ..
            },
            ..
        }]
    ));
}

#[test]
fn required_failure_remains_primary_after_an_advisory_failure() {
    let mut runtime_definition = definition(&[("lint", &[], &[]), ("test", &[], &[])], &[], 2);
    runtime_definition
        .steps
        .get_mut("lint")
        .unwrap()
        .failure_policy = FailurePolicy::Advisory;
    let initialization = initialize_test(runtime_definition);
    let mut state = initialization.state;
    for (step, action) in [("lint", 1), ("test", 2)] {
        reduce_and_advance(
            &mut state,
            Occurrence::StepStarted {
                step: step.to_owned(),
                action: action_id(action),
            },
        );
    }

    reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "lint".to_owned(),
            action: action_id(1),
            cause: "advisory".to_owned(),
        },
    );
    let required = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "test".to_owned(),
            action: action_id(2),
            cause: "required".to_owned(),
        },
    );
    let primary_issue = failure("test", FailurePhase::Execution, "required");

    assert_eq!(
        state.workflow,
        WorkflowState::Failed {
            primary_issue: primary_issue.clone(),
            later_cancellation: None,
        }
    );
    assert_eq!(
        required.actions,
        [finish_failed_action(8, primary_issue, ExportSet::new())]
    );
}

#[test]
fn duplicate_and_stale_failure_occurrences_are_noops() {
    let initialization = initialize_test(definition(&[("producer", &[], &["result"])], &[], 1));
    let mut state = initialization.state;

    let stale_start_failure = reduce(
        &state,
        Occurrence::StepStartFailed {
            step: "producer".to_owned(),
            action: action_id(999),
            cause: "stale".to_owned(),
        },
    );
    assert_noop(&state, &stale_start_failure);

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "producer".to_owned(),
            action: action_id(1),
        },
    );
    for occurrence in [
        Occurrence::StepStartFailed {
            step: "producer".to_owned(),
            action: action_id(1),
            cause: "wrong phase".to_owned(),
        },
        Occurrence::StepExecutionFailed {
            step: "producer".to_owned(),
            action: action_id(999),
            cause: "wrong action".to_owned(),
        },
    ] {
        let stale = reduce(&state, occurrence);
        assert_noop(&state, &stale);
    }

    reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "producer".to_owned(),
            action: action_id(1),
            provisional: "provisional".to_owned(),
        },
    );
    for occurrence in [
        Occurrence::StepExecutionFailed {
            step: "producer".to_owned(),
            action: action_id(1),
            cause: "superseded action".to_owned(),
        },
        Occurrence::OutputCaptureFailed {
            step: "producer".to_owned(),
            action: action_id(999),
            cause: "wrong action".to_owned(),
        },
    ] {
        let stale = reduce(&state, occurrence);
        assert_noop(&state, &stale);
    }

    let accepted = Occurrence::OutputCaptureFailed {
        step: "producer".to_owned(),
        action: action_id(3),
        cause: "capture failed".to_owned(),
    };
    reduce_and_advance(&mut state, accepted.clone());
    let terminal = state.clone();
    let duplicate = reduce(&state, accepted);
    assert_noop(&terminal, &duplicate);
}

#[test]
fn replaying_the_same_ordered_transcript_is_structurally_identical() {
    let transcript = || {
        vec![
            Occurrence::StepStarted {
                step: "a".to_owned(),
                action: action_id(1),
            },
            Occurrence::StepExecutionCompleted {
                step: "a".to_owned(),
                action: action_id(1),
                provisional: "a-provisional".to_owned(),
            },
            Occurrence::OutputsCaptured {
                step: "a".to_owned(),
                action: action_id(3),
                outputs: output_set(&[("artifact", "a-committed")]),
            },
            Occurrence::StepStarted {
                step: "b".to_owned(),
                action: action_id(5),
            },
            Occurrence::StepStarted {
                step: "c".to_owned(),
                action: action_id(6),
            },
            Occurrence::StepExecutionCompleted {
                step: "c".to_owned(),
                action: action_id(6),
                provisional: "ignored-c".to_owned(),
            },
            Occurrence::StepExecutionCompleted {
                step: "b".to_owned(),
                action: action_id(5),
                provisional: "ignored-b".to_owned(),
            },
            Occurrence::StepStarted {
                step: "d".to_owned(),
                action: action_id(13),
            },
            Occurrence::StepExecutionCompleted {
                step: "d".to_owned(),
                action: action_id(13),
                provisional: "d-provisional".to_owned(),
            },
            Occurrence::OutputsCaptured {
                step: "d".to_owned(),
                action: action_id(15),
                outputs: output_set(&[("report", "d-committed")]),
            },
        ]
    };
    let evaluate = || {
        let mut reductions = Vec::new();
        let initialization = initialize_test(definition(
            &[
                ("a", &[], &["artifact"]),
                ("b", &["a"], &[]),
                ("c", &["a"], &[]),
                ("d", &["b", "c"], &["report"]),
            ],
            &[("finalReport", "d", "report")],
            2,
        ));
        let mut state = initialization.state.clone();
        reductions.push(initialization);
        for occurrence in transcript() {
            let reduction = reduce_and_advance(&mut state, occurrence);
            reductions.push(reduction);
        }
        reductions
    };

    let first = evaluate();
    let second = evaluate();

    assert_eq!(first, second);
    let terminal = &first.last().unwrap().state;
    assert_eq!(terminal.workflow, WorkflowState::Succeeded);
    assert_eq!(terminal.last_transition_sequence, sequence(17));
    assert_eq!(
        terminal.exports,
        Some(available_exports(&[("finalReport", "d-committed")]))
    );
}
#[test]
fn trace_succeeded_trigger_and_failed_release() {
    let definition = finalizer_definition(
        &[("work", FailurePolicy::Required, &[], &[], &[])],
        &[(
            "release",
            FailurePolicy::Required,
            &[FinalizationTrigger::Succeeded],
            &[],
            &[],
        )],
        1,
    );
    let mut state = initialize_test(definition).state;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "work".to_owned(),
            action: action_id(1),
        },
    );
    let boundary = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "work".to_owned(),
            action: action_id(1),
            provisional: String::new(),
        },
    );
    let release = boundary
        .actions
        .iter()
        .find(
            |action| matches!(&action.action, Action::StartStep { step, .. } if step == "release"),
        )
        .unwrap()
        .id;
    assert!(matches!(
        state.workflow,
        WorkflowState::Finalizing {
            trigger: FinalizationTrigger::Succeeded,
            gate: FinalizationGate::Open,
            ..
        }
    ));
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "release".to_owned(),
            action: release,
        },
    );
    let finished = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "release".to_owned(),
            action: release,
            cause: "release failed".to_owned(),
        },
    );
    let WorkflowState::Failed { primary_issue, .. } = &state.workflow else {
        panic!("required release failure did not fail the workflow");
    };
    assert_eq!(primary_issue.node.role, WorkflowNodeRole::Finalizer);
    assert_eq!(primary_issue.node.id, "release");
    assert!(matches!(
        finished.actions.as_slice(),
        [RequestedAction {
            action: Action::FinishRun { .. },
            ..
        }]
    ));
    assert!(state.finalization_summary.is_some());
}

#[test]
fn trace_cancelled_trigger_and_successful_release() {
    let definition = finalizer_definition(
        &[("work", FailurePolicy::Required, &[], &[], &[])],
        &[(
            "release",
            FailurePolicy::Required,
            &[FinalizationTrigger::Cancelled],
            &[],
            &[],
        )],
        1,
    );
    let initialized =
        initialize_definition::<String, String, String, TestDeadline>(ExecutionStart {
            definition,
            initial_cancellation: Some(cancellation(CancellationReason::UserRequest, 10)),
            initial_cancellation_operation: None,
        });
    let start = initialized
        .actions
        .iter()
        .find(
            |action| matches!(&action.action, Action::StartStep { step, .. } if step == "release"),
        )
        .unwrap();
    assert!(matches!(
        initialized.state.workflow,
        WorkflowState::Finalizing {
            trigger: FinalizationTrigger::Cancelled,
            gate: FinalizationGate::Open,
            ..
        }
    ));
    assert!(matches!(
        initialized.state.steps["work"].state,
        StepState::Cancelled { .. }
    ));
    assert!(matches!(start.action, Action::StartStep { .. }));
}

#[test]
fn trace_force_abort_waits_for_owned_work_to_quiesce_and_repeated_force_abort_is_inert() {
    let definition = finalizer_definition(
        &[("work", FailurePolicy::Required, &[], &[], &[])],
        &[
            (
                "release",
                FailurePolicy::Required,
                &[FinalizationTrigger::Succeeded],
                &[],
                &[],
            ),
            (
                "verifyCleanup",
                FailurePolicy::Required,
                &[FinalizationTrigger::Succeeded],
                &[],
                &[],
            ),
        ],
        1,
    );
    let mut state = initialize_test(definition).state;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "work".into(),
            action: action_id(1),
        },
    );
    let boundary = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "work".into(),
            action: action_id(1),
            provisional: String::new(),
        },
    );
    let start = boundary.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "release".into(),
            action: start,
        },
    );
    let aborted = reduce_and_advance(
        &mut state,
        Occurrence::ForceAbortRequested {
            operation: CancellationOperationId::fixture(7),
            deadline: deadline(30),
        },
    );
    let force = aborted
        .actions
        .iter()
        .find(|action| matches!(&action.action, Action::ForceAbortStep { step, .. } if step == "release"))
        .unwrap();
    assert!(matches!(
        state.steps["release"].state,
        StepState::Cancelling { .. }
    ));
    assert!(matches!(
        state.steps["verifyCleanup"].state,
        StepState::Cancelled { .. }
    ));
    assert!(matches!(state.workflow, WorkflowState::Finalizing { .. }));
    let replay = reduce::<String, String, String, TestDeadline>(
        &state,
        Occurrence::ForceAbortRequested {
            operation: CancellationOperationId::fixture(7),
            deadline: deadline(31),
        },
    );
    assert_noop(&state, &replay);
    let force_id = force.id;
    let terminal = reduce_and_advance(
        &mut state,
        Occurrence::StepQuiesced {
            step: "release".into(),
            action: force_id,
        },
    );
    assert_eq!(
        state.workflow,
        WorkflowState::Cancelled {
            reason: CancellationReason::FinalizationForceAbort
        }
    );
    assert!(matches!(
        terminal.actions.as_slice(),
        [RequestedAction {
            action: Action::FinishRun { .. },
            ..
        }]
    ));
}

#[test]
fn trace_failed_finalizer_output_producer_blocks_consumer_without_replacing_primary() {
    let mut definition = finalizer_definition(
        &[("work", FailurePolicy::Required, &[], &[], &[])],
        &[
            (
                "archive",
                FailurePolicy::Required,
                &[FinalizationTrigger::Succeeded],
                &[] as &[(&str, &str)],
                &[] as &[&str],
            ),
            (
                "notify",
                FailurePolicy::Required,
                &[FinalizationTrigger::Succeeded],
                &[("receipt", "outputs.archive.receipt")],
                &["archive"],
            ),
        ],
        1,
    );
    definition
        .steps
        .get_mut("archive")
        .unwrap()
        .declared_outputs
        .insert("receipt".into());
    let mut state = initialize_test(definition).state;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "work".into(),
            action: action_id(1),
        },
    );
    let boundary = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "work".into(),
            action: action_id(1),
            provisional: String::new(),
        },
    );
    let archive = boundary.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "archive".into(),
            action: archive,
        },
    );
    reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "archive".into(),
            action: archive,
            cause: "archive failed".into(),
        },
    );
    let StepState::Blocked { detail } = &state.steps["notify"].state else {
        panic!("notify was not classified at the fixed point");
    };
    assert_eq!(
        detail.prerequisites,
        [Prerequisite::Body {
            r#ref: "outputs.archive.receipt".to_owned(),
        }]
    );
    let WorkflowState::Failed { primary_issue, .. } = &state.workflow else {
        panic!("archive was not primary");
    };
    assert_eq!(primary_issue.node.id, "archive");
}

#[test]
fn trace_advisory_finalizer_failure_preserves_success() {
    let definition = finalizer_definition(
        &[("work", FailurePolicy::Required, &[], &[], &[])],
        &[(
            "notify",
            FailurePolicy::Advisory,
            &[FinalizationTrigger::Succeeded],
            &[],
            &[],
        )],
        1,
    );
    let mut state = initialize_test(definition).state;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "work".into(),
            action: action_id(1),
        },
    );
    let boundary = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "work".into(),
            action: action_id(1),
            provisional: String::new(),
        },
    );
    let notify = boundary.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "notify".into(),
            action: notify,
        },
    );
    reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "notify".into(),
            action: notify,
            cause: "offline".into(),
        },
    );
    assert_eq!(state.workflow, WorkflowState::Succeeded);
    assert!(
        state
            .finalization_summary
            .as_ref()
            .is_some_and(|summary| matches!(
                summary.finalizers[0].disposition,
                StepState::Failed { .. }
            ))
    );
}

#[test]
fn trace_fresh_finalization_cancellation_does_not_replay_ordinary_cancellation() {
    let definition = finalizer_definition(
        &[("work", FailurePolicy::Required, &[], &[], &[])],
        &[(
            "release",
            FailurePolicy::Required,
            &[FinalizationTrigger::Cancelled],
            &[],
            &[],
        )],
        1,
    );
    let initialized =
        initialize_definition::<String, String, String, TestDeadline>(ExecutionStart {
            definition,
            initial_cancellation: Some(cancellation(CancellationReason::UserRequest, 10)),
            initial_cancellation_operation: None,
        });
    let mut state = initialized.state;
    let release = initialized.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "release".into(),
            action: release,
        },
    );
    let cancelled = reduce_and_advance(
        &mut state,
        Occurrence::CancellationOperationRequested {
            operation: CancellationOperationId::fixture(2),
            reason: CancellationReason::RunnerShutdown,
            deadline: deadline(20),
        },
    );
    let cancel = cancelled.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepQuiesced {
            step: "release".into(),
            action: cancel,
        },
    );
    assert_eq!(
        state.workflow,
        WorkflowState::Cancelled {
            reason: CancellationReason::UserRequest
        }
    );
    let summary = state.finalization_summary.as_ref().unwrap();
    assert_eq!(
        summary.cancellation,
        Some(FinalizationCancellation {
            reason: CancellationReason::RunnerShutdown,
            deadline: Some(deadline(20)),
        })
    );
}

#[test]
fn initial_cancellation_rearms_finalizers_and_blocks_unavailable_ordinary_outputs() {
    let definition = finalizer_definition(
        &[("producer", FailurePolicy::Required, &[], &[], &["resource"])],
        &[
            (
                "aContext",
                FailurePolicy::Required,
                &[FinalizationTrigger::Cancelled],
                &[("context", "finalization.context")],
                &[],
            ),
            (
                "bOutput",
                FailurePolicy::Required,
                &[FinalizationTrigger::Cancelled],
                &[("resource", "outputs.producer.resource")],
                &[],
            ),
        ],
        2,
    );
    let initialized =
        initialize_definition::<String, String, String, TestDeadline>(ExecutionStart {
            definition,
            initial_cancellation: Some(cancellation(CancellationReason::UserRequest, 10)),
            initial_cancellation_operation: Some(CancellationOperationId::fixture(1)),
        });
    let state = initialized.state;

    assert_eq!(
        state.workflow,
        WorkflowState::Finalizing {
            trigger: FinalizationTrigger::Cancelled,
            gate: FinalizationGate::Open,
            primary_issue: None,
        }
    );
    assert_eq!(
        state.steps["bOutput"].state,
        blocked_state([Prerequisite::body("outputs.producer.resource").unwrap()])
    );
    let [
        RequestedAction {
            action: Action::StartStep { step, inputs, .. },
            ..
        },
    ] = initialized.actions.as_slice()
    else {
        panic!("only the context-only finalizer should start");
    };
    assert_eq!(step, "aContext");
    let ActionInput::FinalizationContext(context) = &inputs["context"] else {
        panic!("context bytes were not committed with the phase boundary");
    };
    assert_eq!(
        context.as_ref(),
        br#"{"schemaVersion":1,"trigger":"cancelled","primaryIssueStepId":null,"cancellationReason":"user_request","ordinaryIssues":[]}"#
    );

    let stale: TestReduction = reduce(
        &state,
        Occurrence::CancellationOperationRequested {
            operation: CancellationOperationId::fixture(1),
            reason: CancellationReason::UserRequest,
            deadline: deadline(11),
        },
    );
    assert_noop(&state, &stale);
}

#[test]
fn finalizer_classification_precedes_bytewise_independent_selection() {
    let definition = finalizer_definition(
        &[(
            "source",
            FailurePolicy::Advisory,
            &[],
            &[],
            &["alpha", "zeta"],
        )],
        &[
            (
                "aBlocked",
                FailurePolicy::Advisory,
                &[FinalizationTrigger::Succeeded],
                &[
                    ("one", "outputs.source.zeta"),
                    ("two", "outputs.source.alpha"),
                    ("again", "outputs.source.zeta"),
                ],
                &[],
            ),
            (
                "mFiltered",
                FailurePolicy::Advisory,
                &[FinalizationTrigger::Failed],
                &[],
                &[],
            ),
            (
                "zIndependent",
                FailurePolicy::Advisory,
                &[FinalizationTrigger::Succeeded],
                &[],
                &[],
            ),
        ],
        2,
    );
    let mut state = initialize_test(definition).state;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "source".into(),
            action: action_id(1),
        },
    );
    let boundary = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "source".into(),
            action: action_id(1),
            cause: "advisory".into(),
        },
    );

    assert_eq!(
        state.steps["aBlocked"].state,
        blocked_state([
            Prerequisite::body("outputs.source.alpha").unwrap(),
            Prerequisite::body("outputs.source.zeta").unwrap(),
        ])
    );
    assert_eq!(
        state.steps["mFiltered"].state,
        not_run_state(
            WorkflowNodeRole::Finalizer,
            NonExecutionCode::FinalizerTriggerNotSelected,
        )
    );
    assert_eq!(
        boundary
            .events
            .iter()
            .filter_map(|event| match event {
                TransitionEvent::Step {
                    step,
                    role: WorkflowNodeRole::Finalizer,
                    to,
                    ..
                } => Some((step.as_str(), *to)),
                _ => None,
            })
            .collect::<Vec<_>>(),
        [
            ("mFiltered", StepStateKind::NotRun),
            ("aBlocked", StepStateKind::Blocked),
            ("zIndependent", StepStateKind::Starting),
        ]
    );
    assert!(matches!(
        boundary.actions.as_slice(),
        [RequestedAction {
            action: Action::StartStep { step, .. },
            ..
        }] if step == "zIndependent"
    ));
}

#[test]
fn finalizer_ready_prefix_is_bytewise_and_uses_the_execution_parallelism_limit() {
    let definition = finalizer_definition(
        &[("work", FailurePolicy::Required, &[], &[], &[])],
        &[
            (
                "zeta",
                FailurePolicy::Required,
                &[FinalizationTrigger::Succeeded],
                &[],
                &[],
            ),
            (
                "alpha",
                FailurePolicy::Required,
                &[FinalizationTrigger::Succeeded],
                &[],
                &[],
            ),
            (
                "middle",
                FailurePolicy::Required,
                &[FinalizationTrigger::Succeeded],
                &[],
                &[],
            ),
        ],
        2,
    );
    let mut state = initialize_test(definition).state;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "work".into(),
            action: action_id(1),
        },
    );
    let boundary = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "work".into(),
            action: action_id(1),
            provisional: String::new(),
        },
    );

    assert_eq!(
        boundary
            .actions
            .iter()
            .filter_map(|requested| match &requested.action {
                Action::StartStep { step, .. } => Some(step.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        ["alpha", "middle"]
    );
    assert_eq!(state.steps["zeta"].state, StepState::Pending);
}

#[test]
fn force_abort_escalation_preserves_the_graceful_reason_and_deadline() {
    let definition = finalizer_definition(
        &[("work", FailurePolicy::Required, &[], &[], &[])],
        &[(
            "release",
            FailurePolicy::Required,
            &[FinalizationTrigger::Succeeded],
            &[],
            &[],
        )],
        1,
    );
    let mut state = initialize_test(definition).state;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "work".into(),
            action: action_id(1),
        },
    );
    let boundary = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "work".into(),
            action: action_id(1),
            provisional: String::new(),
        },
    );
    let release = boundary.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "release".into(),
            action: release,
        },
    );
    let graceful = reduce_and_advance(
        &mut state,
        Occurrence::CancellationOperationRequested {
            operation: CancellationOperationId::fixture(4),
            reason: CancellationReason::RunnerShutdown,
            deadline: deadline(40),
        },
    );
    let graceful_action = graceful.actions[0].id;
    let forced = reduce_and_advance(
        &mut state,
        Occurrence::ForceAbortRequested {
            operation: CancellationOperationId::fixture(5),
            deadline: deadline(41),
        },
    );
    let forced_action = forced.actions[0].id;
    assert_ne!(forced_action, graceful_action);
    assert_eq!(
        state.workflow,
        WorkflowState::Finalizing {
            trigger: FinalizationTrigger::Succeeded,
            gate: FinalizationGate::Cancelling {
                reason: CancellationReason::RunnerShutdown,
                deadline: Some(deadline(40)),
                force_abort: true,
            },
            primary_issue: None,
        }
    );
    reduce_and_advance(
        &mut state,
        Occurrence::StepQuiesced {
            step: "release".into(),
            action: forced_action,
        },
    );
    let summary = state.finalization_summary.as_ref().unwrap();
    assert_eq!(
        summary.cancellation,
        Some(FinalizationCancellation {
            reason: CancellationReason::RunnerShutdown,
            deadline: Some(deadline(40)),
        })
    );
    assert!(summary.force_abort);
    assert_eq!(
        state.steps["release"].state,
        StepState::Cancelled {
            detail: CancellationDetail::new(CancellationReason::FinalizationForceAbort),
        }
    );
}

#[test]
fn trace_advisory_issue_context_is_exact_sorted_and_immutable() {
    let definition = finalizer_definition(
        &[("lint", FailurePolicy::Advisory, &[], &[], &[])],
        &[(
            "notify",
            FailurePolicy::Advisory,
            &[FinalizationTrigger::Succeeded],
            &[("context", "finalization.context")],
            &[],
        )],
        1,
    );
    let mut state = initialize_test(definition).state;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "lint".into(),
            action: action_id(1),
        },
    );
    let boundary = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "lint".into(),
            action: action_id(1),
            cause: "lint failed".into(),
        },
    );
    let Action::StartStep { inputs, .. } = &boundary.actions[0].action else {
        panic!()
    };
    let ActionInput::FinalizationContext(bytes) = &inputs["context"] else {
        panic!()
    };
    assert_eq!(bytes.as_ref(), br#"{"schemaVersion":1,"trigger":"succeeded","primaryIssueStepId":null,"cancellationReason":null,"ordinaryIssues":[{"stepId":"lint","failurePolicy":"advisory","disposition":"failed"}]}"#);
    let retained = Arc::clone(bytes);
    let notify = boundary.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStartFailed {
            step: "notify".into(),
            action: notify,
            cause: "later".into(),
        },
    );
    assert_eq!(retained.as_ref(), bytes.as_ref());
}

#[test]
fn finalizer_is_admitted_at_most_once_per_healthy_attempt() {
    let definition = finalizer_definition(
        &[("work", FailurePolicy::Required, &[], &[], &[])],
        &[(
            "release",
            FailurePolicy::Required,
            &[FinalizationTrigger::Succeeded],
            &[],
            &[],
        )],
        1,
    );
    let mut state = initialize_test(definition).state;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "work".into(),
            action: action_id(1),
        },
    );
    let boundary = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "work".into(),
            action: action_id(1),
            provisional: String::new(),
        },
    );
    let release = boundary.actions[0].id;
    let started = reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "release".into(),
            action: release,
        },
    );
    assert!(started.actions.is_empty());
    let replay: TestReduction = reduce(
        &state,
        Occurrence::StepStarted {
            step: "release".into(),
            action: release,
        },
    );
    assert!(!replay.occurrence_accepted);
    let completed = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "release".into(),
            action: release,
            provisional: String::new(),
        },
    );
    assert_eq!(
        completed
            .actions
            .iter()
            .filter(|action| matches!(action.action, Action::FinishRun { .. }))
            .count(),
        1
    );
    let terminal_replay: TestReduction = reduce(
        &state,
        Occurrence::StepExecutionCompleted {
            step: "release".into(),
            action: release,
            provisional: String::new(),
        },
    );
    assert!(!terminal_replay.occurrence_accepted);
}

#[test]
fn force_abort_allocates_one_distinct_containment_action_per_active_finalizer() {
    let definition = finalizer_definition(
        &[("work", FailurePolicy::Required, &[], &[], &[])],
        &[
            (
                "alpha",
                FailurePolicy::Required,
                &[FinalizationTrigger::Succeeded],
                &[],
                &[],
            ),
            (
                "beta",
                FailurePolicy::Required,
                &[FinalizationTrigger::Succeeded],
                &[],
                &[],
            ),
        ],
        2,
    );
    let mut state = initialize_test(definition).state;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "work".into(),
            action: action_id(1),
        },
    );
    let boundary = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "work".into(),
            action: action_id(1),
            provisional: String::new(),
        },
    );
    for requested in boundary.actions {
        let Action::StartStep { step, .. } = requested.action else {
            continue;
        };
        reduce_and_advance(
            &mut state,
            Occurrence::StepStarted {
                step,
                action: requested.id,
            },
        );
    }

    let forced = reduce::<String, String, String, TestDeadline>(
        &state,
        Occurrence::ForceAbortRequested {
            operation: CancellationOperationId::fixture(3),
            deadline: deadline(30),
        },
    );
    let containment_ids = forced
        .actions
        .iter()
        .filter_map(|requested| {
            matches!(requested.action, Action::ForceAbortStep { .. }).then_some(requested.id)
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(containment_ids.len(), 2);
    assert!(
        containment_ids
            .iter()
            .all(|id| id.transition_sequence.get() > (1_u64 << 63))
    );
}

fn configure_recovery(
    definition: &mut RuntimeDefinition,
    step: &str,
    retries: u8,
    handler_kind: Option<RecoveryHandlerKind>,
    admitted_ceiling: u64,
) {
    definition.steps.get_mut(step).unwrap().recovery = Some(RuntimeRecovery {
        retries,
        handler_kind,
    });
    definition.maximum_transitions = admitted_ceiling;
}

fn recovery_round(value: u8) -> RecoveryRoundNumber {
    RecoveryRoundNumber(value)
}

#[test]
fn recovery_trace_handlerless_recheck_commits_only_execution_two_outputs() {
    let mut graph = definition(&[("fetch", &[], &["result"])], &[], 1);
    configure_recovery(&mut graph, "fetch", 2, None, 18);
    let initialized = initialize_test(graph);
    let mut state = initialized.state;
    let first_action = initialized.actions[0].id;
    let Action::StartStep {
        execution_number, ..
    } = initialized.actions[0].action
    else {
        panic!("recovery target did not start");
    };
    assert_eq!(execution_number.get(), 1);

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "fetch".into(),
            action: first_action,
        },
    );
    let provisional = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "fetch".into(),
            action: first_action,
            cause: "exit 75".into(),
        },
    );
    assert_eq!(state.steps["fetch"].state, StepState::Starting);
    assert_eq!(state.steps["fetch"].target_execution.unwrap().get(), 2);
    assert!(matches!(
        state.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::Open
        }
    ));
    assert!(provisional.events.iter().all(|event| !matches!(
        event,
        TransitionEvent::Step {
            to: StepStateKind::Failed,
            ..
        }
    )));
    let recovery = state.steps["fetch"].recovery.as_ref().unwrap();
    assert_eq!(recovery.rounds.len(), 1);
    assert_eq!(recovery.rounds[0].failed_execution.cause, "exit 75");
    assert!(recovery.rounds[0].handler.is_none());
    assert!(recovery.terminal_disposition.is_none());
    let [recheck] = provisional.actions.as_slice() else {
        panic!("handlerless recovery did not authorize exactly one recheck");
    };
    let Action::StartStep {
        execution_number,
        inputs,
        ..
    } = &recheck.action
    else {
        panic!("handlerless recovery authorized a non-target action");
    };
    assert_eq!(execution_number.get(), 2);
    assert!(inputs.is_empty());
    assert_ne!(recheck.id, first_action);

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "fetch".into(),
            action: recheck.id,
        },
    );
    let capture = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "fetch".into(),
            action: recheck.id,
            provisional: "execution-two-candidate".into(),
        },
    );
    let capture_action = capture.actions[0].id;
    assert_eq!(
        capture.actions[0].action,
        Action::CaptureOutputs {
            step: "fetch".into(),
            provisional: "execution-two-candidate".into(),
        }
    );
    reduce_and_advance(
        &mut state,
        Occurrence::OutputsCaptured {
            step: "fetch".into(),
            action: capture_action,
            outputs: output_set(&[("result", "execution-two-output")]),
        },
    );

    assert_eq!(
        state.steps["fetch"].state,
        StepState::Succeeded {
            outputs: output_set(&[("result", "execution-two-output")]),
        }
    );
    assert_eq!(state.workflow, WorkflowState::Succeeded);
    assert_eq!(
        state.steps["fetch"]
            .recovery
            .as_ref()
            .unwrap()
            .terminal_disposition,
        Some(RecoveryTerminalDisposition::Recovered {
            execution_number: TargetExecutionNumber(2),
        })
    );
    assert!(ordinary_issues(&state).is_empty());
    assert!(state.last_transition_sequence.get() <= 18);
}

#[test]
fn recovery_trace_gave_up_preserves_the_target_failure_and_starts_no_recheck() {
    let mut graph = definition(&[("verify", &[], &[])], &[], 1);
    configure_recovery(
        &mut graph,
        "verify",
        1,
        Some(RecoveryHandlerKind::Command),
        13,
    );
    let initialized = initialize_test(graph);
    let mut state = initialized.state;
    let target = initialized.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "verify".into(),
            action: target,
        },
    );
    let provisional = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "verify".into(),
            action: target,
            cause: "verification failed".into(),
        },
    );
    let [handler] = provisional.actions.as_slice() else {
        panic!("configured recovery did not authorize one handler");
    };
    let Action::StartRecoveryHandler {
        round,
        kind,
        history,
        ..
    } = &handler.action
    else {
        panic!("configured recovery authorized the wrong action");
    };
    assert_eq!(*round, recovery_round(1));
    assert_eq!(*kind, RecoveryHandlerKind::Command);
    assert_eq!(
        history,
        &state.steps["verify"].recovery.as_ref().unwrap().rounds
    );
    assert_eq!(
        state.steps["verify"].state.kind(),
        StepStateKind::Recovering
    );

    reduce_and_advance(
        &mut state,
        Occurrence::RecoveryHandlerStarted {
            step: "verify".into(),
            round: recovery_round(1),
            action: handler.id,
        },
    );
    let wrong_round = reduce(
        &state,
        Occurrence::RecoveryHandlerCompleted {
            step: "verify".into(),
            round: recovery_round(2),
            action: handler.id,
            decision: RecoveryDecision::gave_up("wrong", "wrong round"),
        },
    );
    assert_noop(&state, &wrong_round);

    let gave_up = reduce_and_advance(
        &mut state,
        Occurrence::RecoveryHandlerCompleted {
            step: "verify".into(),
            round: recovery_round(1),
            action: handler.id,
            decision: RecoveryDecision::gave_up("cannot repair", "source is invalid"),
        },
    );
    assert_eq!(
        state.steps["verify"].state,
        failed_state(FailurePhase::Execution, "verification failed")
    );
    let WorkflowState::Failed {
        primary_issue,
        later_cancellation: None,
    } = &state.workflow
    else {
        panic!("gave_up did not settle a required failure");
    };
    assert_eq!(
        primary_issue.detail,
        failure("verify", FailurePhase::Execution, "verification failed").detail,
    );
    assert_eq!(
        state.steps["verify"]
            .recovery
            .as_ref()
            .unwrap()
            .terminal_disposition,
        Some(RecoveryTerminalDisposition::GaveUp {
            round: recovery_round(1),
        })
    );
    assert!(gave_up.actions.iter().all(|action| !matches!(
        action.action,
        Action::StartStep { .. } | Action::StartRecoveryHandler { .. }
    )));
    assert_eq!(ordinary_issues(&state).len(), 1);
}

#[test]
fn recovery_trace_required_exhaustion_selects_latest_failure_once() {
    let mut graph = definition(&[("compile", &[], &[])], &[], 1);
    configure_recovery(&mut graph, "compile", 1, None, 13);
    let initialized = initialize_test(graph);
    let mut state = initialized.state;
    let first = initialized.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "compile".into(),
            action: first,
        },
    );
    let provisional = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "compile".into(),
            action: first,
            cause: "first failure".into(),
        },
    );
    assert!(provisional.events.iter().all(|event| !matches!(
        event,
        TransitionEvent::Workflow { to, .. }
            if matches!(
                to.as_ref(),
                WorkflowState::Executing {
                    gate: SchedulingGate::FailureStopped { .. },
                }
            )
    )));
    let second = provisional.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "compile".into(),
            action: second,
        },
    );
    let exhausted = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "compile".into(),
            action: second,
            cause: "latest failure".into(),
        },
    );
    let WorkflowState::Failed {
        primary_issue,
        later_cancellation: None,
    } = &state.workflow
    else {
        panic!("required exhaustion did not fail the workflow");
    };
    assert_eq!(
        primary_issue.detail,
        failure("compile", FailurePhase::Execution, "latest failure").detail,
    );
    assert_eq!(
        state.steps["compile"]
            .recovery
            .as_ref()
            .unwrap()
            .terminal_disposition,
        Some(RecoveryTerminalDisposition::Exhausted {
            execution_number: TargetExecutionNumber(2),
        })
    );
    assert_eq!(
        exhausted
            .events
            .iter()
            .filter(|event| matches!(
                event,
                TransitionEvent::Workflow { to, .. }
                    if matches!(
                        to.as_ref(),
                        WorkflowState::Executing {
                            gate: SchedulingGate::FailureStopped { .. },
                        }
                    )
            ))
            .count(),
        1
    );
    assert_eq!(ordinary_issues(&state).len(), 1);
}

#[test]
fn recovery_trace_advisory_exhaustion_releases_control_only_after_terminal_failure() {
    let mut graph = definition(&[("lint", &[], &[]), ("package", &["lint"], &[])], &[], 1);
    graph.steps.get_mut("lint").unwrap().failure_policy = FailurePolicy::Advisory;
    configure_recovery(&mut graph, "lint", 1, None, 18);
    let initialized = initialize_test(graph);
    let mut state = initialized.state;
    let first = initialized.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "lint".into(),
            action: first,
        },
    );
    let provisional = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "lint".into(),
            action: first,
            cause: "lint one".into(),
        },
    );
    assert_eq!(state.steps["package"].state, StepState::Pending);
    assert_eq!(provisional.actions.len(), 1);
    let second = provisional.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "lint".into(),
            action: second,
        },
    );
    let exhausted = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "lint".into(),
            action: second,
            cause: "lint two".into(),
        },
    );
    assert!(matches!(
        state.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::Open
        }
    ));
    assert_eq!(state.steps["lint"].state.kind(), StepStateKind::Failed);
    assert_eq!(state.steps["package"].state.kind(), StepStateKind::Starting);
    let [package] = exhausted.actions.as_slice() else {
        panic!("advisory terminal failure did not release its control consumer");
    };
    assert!(matches!(&package.action, Action::StartStep { step, .. } if step == "package"));
    let issues = ordinary_issues(&state);
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].step_id, "lint");
    assert_eq!(issues[0].failure_policy, FailurePolicy::Advisory);

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "package".into(),
            action: package.id,
        },
    );
    reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "package".into(),
            action: package.id,
            provisional: String::new(),
        },
    );
    assert_eq!(state.workflow, WorkflowState::Succeeded);
}

#[test]
fn recovery_trace_handler_start_failure_stops_without_retrying_the_handler() {
    let mut graph = definition(&[("verify", &[], &[])], &[], 1);
    configure_recovery(
        &mut graph,
        "verify",
        2,
        Some(RecoveryHandlerKind::Command),
        18,
    );
    let initialized = initialize_test(graph);
    let mut state = initialized.state;
    let target = initialized.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "verify".into(),
            action: target,
        },
    );
    let provisional = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "verify".into(),
            action: target,
            cause: "target failure".into(),
        },
    );
    let handler = provisional.actions[0].id;
    let stopped = reduce_and_advance(
        &mut state,
        Occurrence::RecoveryHandlerStartFailed {
            step: "verify".into(),
            round: recovery_round(1),
            action: handler,
            cause: "handler launch failure".into(),
        },
    );
    assert_eq!(state.steps["verify"].state.kind(), StepStateKind::Failed);
    assert_eq!(
        state.steps["verify"]
            .recovery
            .as_ref()
            .unwrap()
            .terminal_disposition,
        Some(RecoveryTerminalDisposition::HandlerFailed {
            round: recovery_round(1),
            phase: RecoveryHandlerFailurePhase::Start,
        })
    );
    assert!(stopped.actions.iter().all(|action| !matches!(
        action.action,
        Action::StartStep { .. } | Action::StartRecoveryHandler { .. }
    )));
    let recovery = state.steps["verify"].recovery.as_ref().unwrap();
    assert_eq!(recovery.rounds.len(), 1);
    assert!(matches!(
        recovery.rounds[0].handler.as_ref().unwrap().outcome,
        RecoveryHandlerOutcome::Failed {
            phase: RecoveryHandlerFailurePhase::Start,
            ..
        }
    ));
}

#[test]
fn recovery_trace_agent_handler_execution_failure_preserves_target_precedence() {
    let mut graph = definition(&[("verify", &[], &[])], &[], 1);
    configure_recovery(
        &mut graph,
        "verify",
        2,
        Some(RecoveryHandlerKind::Agent),
        18,
    );
    let initialized = initialize_test(graph);
    let mut state = initialized.state;
    let target = initialized.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "verify".into(),
            action: target,
        },
    );
    let provisional = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionFailed {
            step: "verify".into(),
            action: target,
            cause: "target failure".into(),
        },
    );
    let handler = provisional.actions[0].id;
    assert!(matches!(
        provisional.actions[0].action,
        Action::StartRecoveryHandler {
            kind: RecoveryHandlerKind::Agent,
            ..
        }
    ));
    reduce_and_advance(
        &mut state,
        Occurrence::RecoveryHandlerStarted {
            step: "verify".into(),
            round: recovery_round(1),
            action: handler,
        },
    );
    reduce_and_advance(
        &mut state,
        Occurrence::RecoveryHandlerExecutionFailed {
            step: "verify".into(),
            round: recovery_round(1),
            action: handler,
            cause: "handler protocol failure".into(),
        },
    );
    let WorkflowState::Failed {
        primary_issue,
        later_cancellation: None,
    } = &state.workflow
    else {
        panic!("handler failure did not settle the required target");
    };
    assert_eq!(
        primary_issue.detail,
        failure("verify", FailurePhase::Execution, "target failure").detail,
    );
    assert_eq!(
        state.steps["verify"].state,
        failed_state(FailurePhase::Execution, "target failure")
    );
    let recovery = state.steps["verify"].recovery.as_ref().unwrap();
    assert!(matches!(
        recovery.rounds[0].handler.as_ref().unwrap().outcome,
        RecoveryHandlerOutcome::Failed {
            phase: RecoveryHandlerFailurePhase::Execution,
            ref cause,
        } if cause == "handler protocol failure"
    ));
}

#[test]
fn recovery_trace_cancellation_preempts_all_five_active_phases() {
    #[derive(Clone, Copy)]
    enum Phase {
        TargetStart,
        TargetExecution,
        TargetCapture,
        HandlerStart,
        HandlerExecution,
    }

    for phase in [
        Phase::TargetStart,
        Phase::TargetExecution,
        Phase::TargetCapture,
        Phase::HandlerStart,
        Phase::HandlerExecution,
    ] {
        let mut graph = definition(&[("verify", &[], &["result"])], &[], 1);
        configure_recovery(
            &mut graph,
            "verify",
            1,
            Some(RecoveryHandlerKind::Command),
            13,
        );
        let initialized = initialize_test(graph);
        let mut state = initialized.state;
        let target = initialized.actions[0].id;
        let (active_action, expected_active, late) = match phase {
            Phase::TargetStart => (
                target,
                ActiveStepInvocation::Target {
                    execution_number: TargetExecutionNumber::FIRST,
                },
                Occurrence::StepStartFailed {
                    step: "verify".into(),
                    action: target,
                    cause: "late start failure".into(),
                },
            ),
            Phase::TargetExecution => {
                reduce_and_advance(
                    &mut state,
                    Occurrence::StepStarted {
                        step: "verify".into(),
                        action: target,
                    },
                );
                (
                    target,
                    ActiveStepInvocation::Target {
                        execution_number: TargetExecutionNumber::FIRST,
                    },
                    Occurrence::StepExecutionFailed {
                        step: "verify".into(),
                        action: target,
                        cause: "late execution failure".into(),
                    },
                )
            }
            Phase::TargetCapture => {
                reduce_and_advance(
                    &mut state,
                    Occurrence::StepStarted {
                        step: "verify".into(),
                        action: target,
                    },
                );
                let capture = reduce_and_advance(
                    &mut state,
                    Occurrence::StepExecutionCompleted {
                        step: "verify".into(),
                        action: target,
                        provisional: "candidate".into(),
                    },
                );
                let capture_action = capture.actions[0].id;
                (
                    capture_action,
                    ActiveStepInvocation::Target {
                        execution_number: TargetExecutionNumber::FIRST,
                    },
                    Occurrence::OutputCaptureFailed {
                        step: "verify".into(),
                        action: capture_action,
                        cause: "late capture failure".into(),
                    },
                )
            }
            Phase::HandlerStart | Phase::HandlerExecution => {
                reduce_and_advance(
                    &mut state,
                    Occurrence::StepStarted {
                        step: "verify".into(),
                        action: target,
                    },
                );
                let recovery = reduce_and_advance(
                    &mut state,
                    Occurrence::StepExecutionFailed {
                        step: "verify".into(),
                        action: target,
                        cause: "provisional target failure".into(),
                    },
                );
                let handler = recovery.actions[0].id;
                if matches!(phase, Phase::HandlerExecution) {
                    reduce_and_advance(
                        &mut state,
                        Occurrence::RecoveryHandlerStarted {
                            step: "verify".into(),
                            round: recovery_round(1),
                            action: handler,
                        },
                    );
                }
                let late = if matches!(phase, Phase::HandlerStart) {
                    Occurrence::RecoveryHandlerStartFailed {
                        step: "verify".into(),
                        round: recovery_round(1),
                        action: handler,
                        cause: "late handler start failure".into(),
                    }
                } else {
                    Occurrence::RecoveryHandlerCompleted {
                        step: "verify".into(),
                        round: recovery_round(1),
                        action: handler,
                        decision: RecoveryDecision::recheck("late", "cancelled"),
                    }
                };
                (
                    handler,
                    ActiveStepInvocation::RecoveryHandler {
                        round: recovery_round(1),
                    },
                    late,
                )
            }
        };
        assert_eq!(state.steps["verify"].current_action, Some(active_action));
        let cancelled = reduce_and_advance(
            &mut state,
            Occurrence::CancellationRequested {
                reason: CancellationReason::UserRequest,
                deadline: deadline(90),
            },
        );
        let [cancel] = cancelled.actions.as_slice() else {
            panic!("cancellation did not address exactly one active role");
        };
        assert_eq!(
            cancel.action,
            Action::CancelStep {
                step: "verify".into(),
                active: expected_active,
                reason: CancellationReason::UserRequest,
                deadline: deadline(90),
            }
        );
        assert_eq!(
            state.steps["verify"].state.kind(),
            StepStateKind::Cancelling
        );
        let stale = reduce(&state, late);
        assert_noop(&state, &stale);
        let quiesced = reduce_and_advance(
            &mut state,
            Occurrence::StepQuiesced {
                step: "verify".into(),
                action: cancel.id,
            },
        );
        assert_eq!(state.steps["verify"].state.kind(), StepStateKind::Cancelled);
        assert_eq!(
            state.workflow,
            WorkflowState::Cancelled {
                reason: CancellationReason::UserRequest,
            }
        );
        assert!(quiesced.actions.iter().all(|action| !matches!(
            action.action,
            Action::StartStep { .. } | Action::StartRecoveryHandler { .. }
        )));
        let recovery = state.steps["verify"].recovery.as_ref().unwrap();
        if matches!(phase, Phase::HandlerStart | Phase::HandlerExecution) {
            assert_eq!(
                recovery.terminal_disposition,
                Some(RecoveryTerminalDisposition::Cancelled {
                    round: recovery_round(1),
                    active: expected_active,
                })
            );
            assert!(matches!(
                recovery.rounds[0].handler.as_ref().unwrap().outcome,
                RecoveryHandlerOutcome::Cancelled
            ));
        } else {
            assert!(recovery.rounds.is_empty());
            assert!(recovery.terminal_disposition.is_none());
        }
    }
}

#[test]
fn recovery_trace_output_capture_failure_only_authorizes_a_full_target_rerun() {
    let mut graph = finalizer_definition(
        &[("build", FailurePolicy::Required, &[], &[], &["artifact"])],
        &[(
            "cleanup",
            FailurePolicy::Required,
            &[FinalizationTrigger::Succeeded],
            &[("context", "finalization.context")],
            &[],
        )],
        1,
    );
    configure_recovery(
        &mut graph,
        "build",
        1,
        Some(RecoveryHandlerKind::Command),
        16,
    );
    let initialized = initialize_test(graph);
    let mut state = initialized.state;
    let first = initialized.actions[0].id;
    let first_inputs = match &initialized.actions[0].action {
        Action::StartStep { inputs, .. } => inputs.clone(),
        _ => panic!("build target did not start"),
    };
    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "build".into(),
            action: first,
        },
    );
    let capture = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "build".into(),
            action: first,
            provisional: "released-candidate".into(),
        },
    );
    let capture_action = capture.actions[0].id;
    let provisional = reduce_and_advance(
        &mut state,
        Occurrence::OutputCaptureFailed {
            step: "build".into(),
            action: capture_action,
            cause: "capture failed after cleanup".into(),
        },
    );
    assert_eq!(state.steps["cleanup"].state, StepState::Pending);
    assert!(matches!(
        provisional.actions.as_slice(),
        [RequestedAction {
            action: Action::StartRecoveryHandler { .. },
            ..
        }]
    ));
    assert!(provisional.actions.iter().all(|action| !matches!(
        action.action,
        Action::CaptureOutputs { .. } | Action::StartStep { .. }
    )));
    let recovery = state.steps["build"].recovery.as_ref().unwrap();
    assert_eq!(
        recovery.rounds[0].failed_execution.phase,
        FailurePhase::OutputCapture
    );
    assert_eq!(recovery.rounds[0].failed_execution.invocation, first);
    let handler = provisional.actions[0].id;
    reduce_and_advance(
        &mut state,
        Occurrence::RecoveryHandlerStarted {
            step: "build".into(),
            round: recovery_round(1),
            action: handler,
        },
    );
    let recheck = reduce_and_advance(
        &mut state,
        Occurrence::RecoveryHandlerCompleted {
            step: "build".into(),
            round: recovery_round(1),
            action: handler,
            decision: RecoveryDecision::recheck("workspace repaired", "rerun target"),
        },
    );
    let [second] = recheck.actions.as_slice() else {
        panic!("recheck did not authorize one complete target");
    };
    let Action::StartStep {
        execution_number,
        inputs,
        ..
    } = &second.action
    else {
        panic!("recheck authorized capture-only work");
    };
    assert_eq!(execution_number.get(), 2);
    assert_eq!(inputs, &first_inputs);
    assert_ne!(second.id, first);
    assert_ne!(second.id, capture_action);
    assert_eq!(state.steps["cleanup"].state, StepState::Pending);

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "build".into(),
            action: second.id,
        },
    );
    let second_capture = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "build".into(),
            action: second.id,
            provisional: "fresh-candidate".into(),
        },
    );
    let final_boundary = reduce_and_advance(
        &mut state,
        Occurrence::OutputsCaptured {
            step: "build".into(),
            action: second_capture.actions[0].id,
            outputs: output_set(&[("artifact", "fresh-output")]),
        },
    );
    assert!(matches!(state.workflow, WorkflowState::Finalizing { .. }));
    assert_eq!(state.steps["cleanup"].state.kind(), StepStateKind::Starting);
    let [
        RequestedAction {
            action: Action::StartStep { step, inputs, .. },
            ..
        },
    ] = final_boundary.actions.as_slice()
    else {
        panic!("finalizer did not wait for terminal recovery success");
    };
    assert_eq!(step, "cleanup");
    let ActionInput::FinalizationContext(context) = &inputs["context"] else {
        panic!("finalizer context was not committed at ordinary quiescence");
    };
    assert_eq!(
        context.as_ref(),
        br#"{"schemaVersion":1,"trigger":"succeeded","primaryIssueStepId":null,"cancellationReason":null,"ordinaryIssues":[]}"#
    );
    assert!(state.last_transition_sequence.get() <= 16);
}

#[test]
fn cloud_output_capture_recovery_stays_within_its_admitted_transition_ceiling() {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&execution_root).unwrap();
    fs::write(
        source_root.join("workflow.yaml"),
        r#"schemaVersion: 1
steps:
  build:
    kind: cmd
    recovery:
      retries: 2
      handler:
        kind: cmd
        command:
          argv: ["true"]
    command:
      argv: ["false"]
    outputs:
      artifact:
        kind: file
        from: path
        path: artifact.txt
        mediaType: text/plain
"#,
    )
    .unwrap();
    let admitted = admit_runner_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedImports::default(),
        ExecutionContext::new(
            execution_root,
            ExecutionRootLifecycle::CallerOwnedRetained,
            ExecutionPolicyLimits::new(
                1,
                CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
                InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                1024 * 1024,
            ),
            EnvironmentSnapshot::default(),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap();
    assert_eq!(admitted.capacity().maximum_transitions, 17);

    let initialized = initialize::<String, String, String, TestDeadline>(&admitted, None);
    let mut state = initialized.state;
    let mut target = initialized.actions[0].id;
    for round in 1_u8..=2 {
        reduce_and_advance(
            &mut state,
            Occurrence::StepStarted {
                step: "build".into(),
                action: target,
            },
        );
        let capture = reduce_and_advance(
            &mut state,
            Occurrence::StepExecutionCompleted {
                step: "build".into(),
                action: target,
                provisional: format!("candidate-{round}"),
            },
        );
        let recovering = reduce_and_advance(
            &mut state,
            Occurrence::OutputCaptureFailed {
                step: "build".into(),
                action: capture.actions[0].id,
                cause: format!("capture failure {round}"),
            },
        );
        let handler = recovering.actions[0].id;
        reduce_and_advance(
            &mut state,
            Occurrence::RecoveryHandlerStarted {
                step: "build".into(),
                round: recovery_round(round),
                action: handler,
            },
        );
        let recheck = reduce_and_advance(
            &mut state,
            Occurrence::RecoveryHandlerCompleted {
                step: "build".into(),
                round: recovery_round(round),
                action: handler,
                decision: RecoveryDecision::recheck("repaired", "rerun target"),
            },
        );
        target = recheck.actions[0].id;
    }

    reduce_and_advance(
        &mut state,
        Occurrence::StepStarted {
            step: "build".into(),
            action: target,
        },
    );
    let capture = reduce_and_advance(
        &mut state,
        Occurrence::StepExecutionCompleted {
            step: "build".into(),
            action: target,
            provisional: "terminal-candidate".into(),
        },
    );
    reduce_and_advance(
        &mut state,
        Occurrence::OutputCaptureFailed {
            step: "build".into(),
            action: capture.actions[0].id,
            cause: "terminal capture failure".into(),
        },
    );

    assert!(matches!(state.workflow, WorkflowState::Failed { .. }));
    assert!(state.last_transition_sequence.get() <= 17);
}
