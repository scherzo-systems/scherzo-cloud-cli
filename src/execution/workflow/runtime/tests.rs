use std::fs;
use std::path::Path;
use std::time::Duration;

use super::*;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationSource, ExecutionContext, ExecutionRootLifecycle,
    ResolvedImports, admit_command_workflow,
};
use crate::execution::workflow::resolution;

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
    RuntimeDefinition {
        steps: steps
            .iter()
            .map(|(step, dependencies, outputs)| {
                (
                    (*step).to_owned(),
                    RuntimeStep {
                        dependencies: dependencies
                            .iter()
                            .map(|dependency| (*dependency).to_owned())
                            .collect::<Vec<_>>()
                            .into(),
                        declared_outputs: outputs
                            .iter()
                            .map(|output| (*output).to_owned())
                            .collect(),
                    },
                )
            })
            .collect(),
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
    }
}

fn step_event(
    value: u64,
    step: &str,
    from: StepStateKind,
    to: StepStateKind,
) -> TransitionEvent<String> {
    TransitionEvent::Step {
        sequence: sequence(value),
        step: step.to_owned(),
        from,
        to,
    }
}

fn workflow_event(
    value: u64,
    from: WorkflowState<String>,
    to: WorkflowState<String>,
) -> TransitionEvent<String> {
    TransitionEvent::Workflow {
        sequence: sequence(value),
        from,
        to,
    }
}

fn workflow_succeeded_event(value: u64) -> TransitionEvent<String> {
    workflow_event(
        value,
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        },
        WorkflowState::Succeeded,
    )
}

fn failure(step: &str, phase: FailurePhase, cause: &str) -> StepFailure<String> {
    StepFailure {
        step: step.to_owned(),
        phase,
        cause: cause.to_owned(),
    }
}

fn start_action(value: u64, step: &str) -> RequestedAction<String, String, String> {
    RequestedAction {
        id: action_id(value),
        action: Action::StartStep {
            step: step.to_owned(),
        },
    }
}

fn capture_action(
    value: u64,
    step: &str,
    provisional: &str,
) -> RequestedAction<String, String, String> {
    RequestedAction {
        id: action_id(value),
        action: Action::CaptureOutputs {
            step: step.to_owned(),
            provisional: provisional.to_owned(),
        },
    }
}

fn finish_action(
    value: u64,
    exports: ExportSet<String>,
) -> RequestedAction<String, String, String> {
    RequestedAction {
        id: action_id(value),
        action: Action::FinishRun {
            outcome: RunOutcome::Succeeded,
            exports,
        },
    }
}

fn finish_failed_action(
    value: u64,
    primary_failure: StepFailure<String>,
    exports: ExportSet<String>,
) -> RequestedAction<String, String, String> {
    RequestedAction {
        id: action_id(value),
        action: Action::FinishRun {
            outcome: RunOutcome::Failed { primary_failure },
            exports,
        },
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

fn assert_step(state: &RuntimeState<String, String>, step: &str, expected: StepStateKind) {
    assert_eq!(state.steps[step].state.kind(), expected);
}

fn assert_noop(
    before: &RuntimeState<String, String>,
    reduction: &Reduction<String, String, String>,
) {
    assert_eq!(&reduction.state, before);
    assert!(reduction.events.is_empty());
    assert!(reduction.actions.is_empty());
}

fn reduce_and_advance(
    state: &mut RuntimeState<String, String>,
    occurrence: Occurrence<String, String, String>,
) -> Reduction<String, String, String> {
    let reduction = reduce(state, occurrence);
    *state = reduction.state.clone();
    reduction
}

fn prepare_failure_phase(
    phase: FailurePhase,
) -> (
    RuntimeState<String, String>,
    Occurrence<String, String, String>,
    StepStateKind,
    u64,
) {
    let initialization = initialize_definition::<String, String, String>(definition(
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
        "schemaVersion: 1\nsteps:\n  zeta:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  alpha:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      report:\n        kind: file\n        path: report.txt\n        mediaType: text/plain\n",
    )
    .unwrap();
    let admitted = admit_command_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedImports::default(),
        ExecutionContext::new(
            execution_root,
            ExecutionRootLifecycle::EngineOwnedEphemeral,
            1,
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap();

    let initialization = initialize::<String, String, String>(&admitted);

    assert_eq!(initialization.actions, [start_action(1, "alpha")]);
    assert_step(&initialization.state, "alpha", StepStateKind::Starting);
    assert_step(&initialization.state, "zeta", StepStateKind::Pending);
    assert!(initialization.state.exports.is_none());
}

#[test]
fn empty_dag_finishes_during_initialization() {
    let reduction = initialize_definition::<String, String, String>(definition(&[], &[], 1));

    assert_eq!(reduction.state.workflow, WorkflowState::Succeeded);
    assert!(reduction.state.steps.is_empty());
    assert_eq!(reduction.state.exports, Some(BTreeMap::new()));
    assert_eq!(reduction.state.last_transition_sequence, sequence(1));
    assert_eq!(reduction.events, [workflow_succeeded_event(1)]);
    assert_eq!(reduction.actions, [finish_action(1, BTreeMap::new())]);
}

#[test]
fn serial_steps_follow_every_success_state_and_identifier() {
    let initialization = initialize_definition::<String, String, String>(definition(
        &[("a", &[], &[]), ("b", &["a"], &[])],
        &[],
        2,
    ));
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
    let initialization = initialize_definition::<String, String, String>(definition(
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
    let initialization = initialize_definition::<String, String, String>(definition(
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
    let initialization = initialize_definition::<String, String, String>(definition(
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
    assert_eq!(captured.actions, [finish_action(5, expected_exports)]);
    assert_eq!(
        actions
            .iter()
            .filter(|action| matches!(action.action, Action::FinishRun { .. }))
            .count(),
        1
    );

    assert_eq!(state.last_transition_sequence, sequence(5));
}

#[test]
fn duplicate_and_stale_success_occurrences_are_noops() {
    let initialization = initialize_definition::<String, String, String>(definition(
        &[("producer", &[], &["result"])],
        &[],
        1,
    ));
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
fn every_failure_phase_closes_scheduling_and_reaches_the_fixed_point() {
    for phase in [
        FailurePhase::Start,
        FailurePhase::Execution,
        FailurePhase::OutputCapture,
    ] {
        let (state, occurrence, source_state, direct_sequence) = prepare_failure_phase(phase);
        let duplicate = occurrence.clone();
        let reduction = reduce(&state, occurrence);
        let primary_failure = failure("aFail", phase, "reported cause");
        let failure_stopped = WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped {
                primary_failure: primary_failure.clone(),
            },
        };
        let terminal = WorkflowState::Failed {
            primary_failure: primary_failure.clone(),
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
            StepState::Failed {
                phase,
                cause: "reported cause".to_owned(),
            }
        );
        assert_eq!(
            reduction.state.steps["bStopped"].state,
            StepState::NotRun {
                reason: NotRunReason::FailureStop,
            }
        );
        assert_eq!(
            reduction.state.steps["zJoin"].state,
            StepState::Blocked {
                dependency: "aFail".to_owned(),
            }
        );
        assert_eq!(
            reduction.state.steps["zzDescendant"].state,
            StepState::Blocked {
                dependency: "zJoin".to_owned(),
            }
        );
        assert_eq!(reduction.state.workflow, terminal);
        assert_eq!(
            reduction.actions,
            [finish_failed_action(
                direct_sequence + 5,
                primary_failure,
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
fn later_active_failure_does_not_replace_the_primary_failure() {
    let initialization = initialize_definition::<String, String, String>(definition(
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
    let primary_failure = failure("zeta", FailurePhase::Execution, "first failure");
    let failure_stopped = WorkflowState::Executing {
        gate: SchedulingGate::FailureStopped {
            primary_failure: primary_failure.clone(),
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
        primary_failure: primary_failure.clone(),
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
        StepState::Failed {
            phase: FailurePhase::OutputCapture,
            cause: "later failure".to_owned(),
        }
    );
    assert_eq!(state.workflow, terminal);
    assert_eq!(
        later.actions,
        [finish_failed_action(9, primary_failure, ExportSet::new())]
    );
}

#[test]
fn successful_sibling_outputs_and_export_reasons_survive_failure() {
    let initialization = initialize_definition::<String, String, String>(definition(
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
    let primary_failure = failure("aFail", FailurePhase::Execution, "branch failed");
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
        StepState::NotRun {
            reason: NotRunReason::FailureStop,
        }
    );
    assert_eq!(
        state.workflow,
        WorkflowState::Failed {
            primary_failure: primary_failure.clone(),
        }
    );
    assert_eq!(state.exports, Some(expected_exports.clone()));
    assert_eq!(
        captured.actions,
        [finish_failed_action(12, primary_failure, expected_exports,)]
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
fn duplicate_and_stale_failure_occurrences_are_noops() {
    let initialization = initialize_definition::<String, String, String>(definition(
        &[("producer", &[], &["result"])],
        &[],
        1,
    ));
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
        let initialization = initialize_definition::<String, String, String>(definition(
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
