use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use time::format_description::well_known::Rfc3339;

use super::*;
use crate::execution::workflow::observation::{
    CommandOutputObservation, SourceSequence, TransitionObservation,
};
use crate::execution::workflow::resolution;
use crate::execution::workflow::runtime::{ActionId, FailurePhase, TransitionSequence};
use crate::execution::workflow::step_runtime::{CommandExecutionFailure, StepExecutionFailure};
use crate::execution::workflow::value::CapturedValue;

#[derive(Clone)]
struct ControlledClock {
    current: Arc<Mutex<ObservationTime>>,
}

impl ControlledClock {
    fn new(current: ObservationTime) -> Self {
        Self {
            current: Arc::new(Mutex::new(current)),
        }
    }

    fn set(&self, current: ObservationTime) {
        *self.current.lock().unwrap() = current;
    }
}

impl ObservationClock for ControlledClock {
    fn sample(&self) -> ObservationTime {
        *self.current.lock().unwrap()
    }
}

fn timestamp(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).unwrap()
}

fn point(base: Instant, milliseconds: u64) -> ObservationTime {
    ObservationTime {
        utc: timestamp("2026-08-04T12:00:00Z") + Duration::from_millis(milliseconds),
        monotonic: base + Duration::from_millis(milliseconds),
    }
}

fn resolved_workflow() -> (tempfile::TempDir, ResolvedWorkflow) {
    let temporary = tempfile::tempdir().unwrap();
    std::fs::write(
        temporary.path().join("workflow.yaml"),
        "schemaVersion: 1
steps:
  prepare:
    kind: cmd
    command:
      argv: [\"prepare\"]
    outputs:
      report:
        kind: file
        path: report.txt
        mediaType: text/plain
  consume:
    kind: cmd
    dependsOn: [prepare]
    command:
      argv: [\"consume\"]
    outputs:
      receipt:
        kind: file
        path: receipt.txt
        mediaType: text/plain
",
    )
    .unwrap();
    let workflow = resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
    (temporary, workflow)
}

fn model(
    workflow: &ResolvedWorkflow,
    clock: ControlledClock,
) -> WorkflowRunViewModel<ControlledClock> {
    let opened_at = clock.sample();
    let timing = RunTimingObservation::new(opened_at);
    timing.mark_execution_started(opened_at);
    WorkflowRunViewModel::new(workflow, 2, timing, clock)
}

fn model_with_capacity(
    workflow: &ResolvedWorkflow,
    clock: ControlledClock,
    capacity: StepLogCapacity,
) -> WorkflowRunViewModel<ControlledClock> {
    let opened_at = clock.sample();
    let timing = RunTimingObservation::new(opened_at);
    timing.mark_execution_started(opened_at);
    WorkflowRunViewModel::with_log_capacity(workflow, 2, timing, clock, capacity)
}

fn step_transition(
    step: &str,
    from: StepStateKind,
    to: StepStateKind,
    detail: Option<ObservedStepTransition>,
) -> ExecutionObservation<OffsetDateTime> {
    ExecutionObservation::Transition(TransitionObservation {
        event: TransitionEvent::Step {
            sequence: TransitionSequence::default(),
            step: step.to_owned(),
            failure_policy: crate::execution::workflow::document::FailurePolicy::Required,
            from,
            to,
        },
        step: detail,
    })
}

fn workflow_transition(
    from: WorkflowState<StepFailureCause>,
    to: WorkflowState<StepFailureCause>,
) -> ExecutionObservation<OffsetDateTime> {
    ExecutionObservation::Transition(TransitionObservation {
        event: TransitionEvent::Workflow {
            sequence: TransitionSequence::default(),
            from,
            to,
        },
        step: None,
    })
}

fn cancellation(deadline: OffsetDateTime) -> ExecutionObservation<OffsetDateTime> {
    ExecutionObservation::Transition(TransitionObservation {
        event: TransitionEvent::CancellationAccepted {
            sequence: TransitionSequence::default(),
            reason: CancellationReason::UserRequest,
            deadline,
        },
        step: None,
    })
}

fn output(
    step: &str,
    source: CommandOutputSource,
    sequence: SourceSequence,
    bytes: impl Into<Arc<[u8]>>,
) -> ExecutionObservation<OffsetDateTime> {
    ExecutionObservation::CommandOutput(CommandOutputObservation {
        step: step.to_owned(),
        invocation: ActionId {
            transition_sequence: TransitionSequence::default(),
        },
        source,
        sequence,
        bytes: bytes.into(),
    })
}

fn step<'a>(snapshot: &'a WorkflowRunViewSnapshot, id: &str) -> &'a WorkflowRunStepView {
    snapshot.steps.iter().find(|step| step.id == id).unwrap()
}

fn maximum_record(index: usize) -> Arc<[u8]> {
    let label = format!("record-{index:03}");
    let mut bytes = vec![b'x'; MAX_NORMALIZED_CHILD_RECORD_BYTES];
    bytes[..label.len()].copy_from_slice(label.as_bytes());
    Arc::from(bytes)
}

#[test]
fn default_log_capacity_partitions_run_budgets_by_step_count() {
    for (step_count, expected_records, expected_bytes) in [
        (1, 4_096, 4 * 1024 * 1024),
        (16, 4_096, 4 * 1024 * 1024),
        (17, 4_096, 3_947_580),
        (64, 4_096, 1024 * 1024),
        (256, 1024, 256 * 1024),
    ] {
        let capacity = StepLogCapacity::for_step_count(step_count);
        assert_eq!(capacity.maximum_records(), expected_records);
        assert_eq!(capacity.maximum_bytes(), expected_bytes);
    }

    for step_count in 1..=256 {
        let capacity = StepLogCapacity::for_step_count(step_count);
        assert!(capacity.maximum_records() * step_count <= RUN_LOG_RECORD_BUDGET);
        assert!(
            u64::try_from(capacity.maximum_bytes()).unwrap() * u64::try_from(step_count).unwrap()
                <= super::super::RUN_LOG_BYTE_BUDGET
        );
    }
}

#[test]
fn execution_duration_excludes_time_spent_opening_the_view() {
    let (_temporary, workflow) = resolved_workflow();
    let base = crate::timing::monotonic_now();
    let opened_at = point(base, 0);
    let clock = ControlledClock::new(opened_at);
    let timing = RunTimingObservation::new(opened_at);
    let view = WorkflowRunViewModel::new(&workflow, 2, timing.clone(), clock.clone());

    clock.set(point(base, 500));
    let opening = view.snapshot();
    assert_eq!(opening.timing.started_at, opened_at.utc);
    assert_eq!(opening.timing.duration, Duration::ZERO);
    assert!(opening.timing.frozen);

    timing.mark_execution_started(point(base, 500));
    clock.set(point(base, 530));
    let executing = view.snapshot();
    assert_eq!(executing.timing.started_at, point(base, 500).utc);
    assert_eq!(executing.timing.duration, Duration::from_millis(30));
    assert!(!executing.timing.frozen);
}

#[tokio::test]
async fn transitions_project_definition_output_cancellation_and_frozen_timing() {
    let (_temporary, workflow) = resolved_workflow();
    let base = crate::timing::monotonic_now();
    let clock = ControlledClock::new(point(base, 0));
    let view = model(&workflow, clock.clone());
    let changes = view.subscribe();

    clock.set(point(base, 10));
    view.observe(step_transition(
        "prepare",
        StepStateKind::Pending,
        StepStateKind::Starting,
        None,
    ))
    .await;
    clock.set(point(base, 25));
    let live = view.snapshot();
    let prepare = step(&live, "prepare");
    assert_eq!(prepare.state, StepStateKind::Starting);
    assert_eq!(
        prepare.timing.as_ref().unwrap().started_at,
        point(base, 10).utc
    );
    assert_eq!(
        prepare.timing.as_ref().unwrap().duration,
        Duration::from_millis(15)
    );
    assert!(!prepare.timing.as_ref().unwrap().frozen);
    assert!(prepare.definition.direct_dependencies().is_empty());
    assert!(prepare.definition.outputs().contains_key("report"));
    assert_eq!(
        prepare.outputs["report"],
        WorkflowRunOutputDisposition::Pending
    );

    clock.set(point(base, 40));
    view.observe(step_transition(
        "prepare",
        StepStateKind::CapturingOutputs,
        StepStateKind::Succeeded,
        Some(ObservedStepTransition::OutputsCommitted {
            outputs: vec!["report".to_owned()],
        }),
    ))
    .await;
    clock.set(point(base, 55));
    view.observe(step_transition(
        "consume",
        StepStateKind::Pending,
        StepStateKind::Starting,
        None,
    ))
    .await;
    let deadline = point(base, 80).utc;
    clock.set(point(base, 60));
    view.observe(cancellation(deadline)).await;
    view.observe(step_transition(
        "consume",
        StepStateKind::Running,
        StepStateKind::Cancelling,
        Some(ObservedStepTransition::Cancelling {
            reason: CancellationReason::UserRequest,
        }),
    ))
    .await;
    clock.set(point(base, 70));
    view.observe(step_transition(
        "consume",
        StepStateKind::Cancelling,
        StepStateKind::Cancelled,
        Some(ObservedStepTransition::Cancelled {
            reason: CancellationReason::UserRequest,
        }),
    ))
    .await;
    clock.set(point(base, 71));
    view.observe(workflow_transition(
        WorkflowState::Executing {
            gate: SchedulingGate::Cancelling {
                reason: CancellationReason::UserRequest,
                prior_failure: None,
            },
        },
        WorkflowState::Cancelled {
            reason: CancellationReason::UserRequest,
        },
    ))
    .await;

    clock.set(point(base, 100));
    let terminal = view.snapshot();
    let prepare = step(&terminal, "prepare");
    assert_eq!(
        prepare.timing.as_ref().unwrap().duration,
        Duration::from_millis(30)
    );
    assert!(prepare.timing.as_ref().unwrap().frozen);
    assert_eq!(
        prepare.outputs["report"],
        WorkflowRunOutputDisposition::Committed
    );
    let consume = step(&terminal, "consume");
    assert_eq!(consume.definition.direct_dependencies(), ["prepare"]);
    assert_eq!(consume.state, StepStateKind::Cancelled);
    assert_eq!(
        consume.outputs["receipt"],
        WorkflowRunOutputDisposition::Unavailable(WorkflowRunOutputUnavailableReason::Cancelled)
    );
    assert_eq!(
        consume.timing.as_ref().unwrap().duration,
        Duration::from_millis(15)
    );
    assert_eq!(
        terminal.cancellation,
        Some(WorkflowRunCancellationView {
            reason: CancellationReason::UserRequest,
            force_stop_deadline: deadline,
        })
    );
    assert!(matches!(terminal.workflow, WorkflowState::Cancelled { .. }));
    assert!(!terminal.quit_eligible);
    assert_eq!(terminal.timing.duration, Duration::from_millis(71));
    assert!(terminal.timing.frozen);
    assert_eq!(*changes.borrow(), terminal.generation);
}

#[tokio::test]
async fn each_step_log_evicts_oldest_records_without_affecting_other_steps() {
    let (_temporary, workflow) = resolved_workflow();
    let base = crate::timing::monotonic_now();
    let clock = ControlledClock::new(point(base, 0));
    let capacity = StepLogCapacity::new(2, MAX_NORMALIZED_CHILD_RECORD_BYTES).unwrap();
    let view = model_with_capacity(&workflow, clock.clone(), capacity);

    let mut long_line = vec![b'x'; MAX_NORMALIZED_CHILD_RECORD_BYTES + 3];
    long_line.push(b'\n');
    clock.set(point(base, 1));
    view.observe(output(
        "prepare",
        CommandOutputSource::StandardError,
        SourceSequence::first(),
        Arc::<[u8]>::from(long_line),
    ))
    .await;
    clock.set(point(base, 2));
    view.observe(output(
        "consume",
        CommandOutputSource::StandardOutput,
        SourceSequence::first(),
        Arc::<[u8]>::from(b"other\n".as_slice()),
    ))
    .await;
    clock.set(point(base, 3));
    view.observe(output(
        "prepare",
        CommandOutputSource::StandardError,
        SourceSequence::first().next(),
        Arc::<[u8]>::from(b"latest\n".as_slice()),
    ))
    .await;

    let snapshot = view.snapshot();
    let prepare = &step(&snapshot, "prepare").log;
    assert_eq!(prepare.observed_records, 3);
    assert_eq!(prepare.retained_records, 2);
    assert_eq!(prepare.discarded_records, 1);
    assert_eq!(
        prepare.discarded_bytes,
        u64::try_from(MAX_NORMALIZED_CHILD_RECORD_BYTES).unwrap()
    );
    assert_eq!(
        prepare.records[0].source,
        WorkflowRunLogSource::Command(CommandOutputSource::StandardError)
    );
    assert!(prepare.records[0].continuation);
    assert_eq!(prepare.records[0].observed_at, point(base, 1).utc);
    assert!(prepare.records[0].accepted_order < prepare.records[1].accepted_order);
    assert_eq!(prepare.records[1].payload.as_ref(), "latest");

    let consume = &step(&snapshot, "consume").log;
    assert_eq!(consume.observed_records, 1);
    assert_eq!(consume.retained_records, 1);
    assert_eq!(consume.discarded_records, 0);
    assert_eq!(
        consume.records[0].source,
        WorkflowRunLogSource::Command(CommandOutputSource::StandardOutput)
    );
    assert_eq!(consume.records[0].payload.as_ref(), "other");

    let render_snapshot = view.snapshot_for_render(1);
    let prepare = &step(&render_snapshot, "prepare").log;
    assert!(prepare.records.is_empty());
    assert_eq!(prepare.retained_records, 2);
    let consume = &step(&render_snapshot, "consume").log;
    assert_eq!(consume.records.len(), 1);
    assert_eq!(consume.records[0].payload.as_ref(), "other");
}

#[tokio::test]
async fn derived_capacity_retains_past_the_old_limit_then_evicts_the_oldest_suffix() {
    let (_temporary, workflow) = resolved_workflow();
    let base = crate::timing::monotonic_now();
    let clock = ControlledClock::new(point(base, 0));
    let view = model(&workflow, clock);

    view.observe(output(
        "consume",
        CommandOutputSource::StandardOutput,
        SourceSequence::first(),
        Arc::<[u8]>::from(b"isolated\n".as_slice()),
    ))
    .await;

    let mut sequence = SourceSequence::first();
    for index in 0..17 {
        view.observe(output(
            "prepare",
            CommandOutputSource::StandardOutput,
            sequence,
            maximum_record(index),
        ))
        .await;
        sequence = sequence.next();
    }

    let consume_before = {
        let snapshot = view.snapshot();
        let prepare = &step(&snapshot, "prepare").log;
        assert_eq!(prepare.observed_records, 17);
        assert_eq!(prepare.retained_records, 17);
        assert!(prepare.retained_bytes > 256 * 1024);
        assert_eq!(prepare.discarded_records, 0);
        assert_eq!(prepare.discarded_bytes, 0);
        assert!(prepare.records[0].payload.starts_with("record-000"));
        step(&snapshot, "consume").log.clone()
    };

    for index in 17..257 {
        view.observe(output(
            "prepare",
            CommandOutputSource::StandardOutput,
            sequence,
            maximum_record(index),
        ))
        .await;
        sequence = sequence.next();
    }

    let snapshot = view.snapshot();
    let prepare = &step(&snapshot, "prepare").log;
    assert_eq!(prepare.observed_records, 257);
    assert_eq!(prepare.retained_records, 256);
    assert_eq!(prepare.retained_bytes, 4 * 1024 * 1024);
    assert_eq!(prepare.discarded_records, 1);
    assert_eq!(
        prepare.discarded_bytes,
        u64::try_from(MAX_NORMALIZED_CHILD_RECORD_BYTES).unwrap()
    );
    assert!(prepare.records[0].payload.starts_with("record-001"));
    assert!(prepare.records[255].payload.starts_with("record-256"));
    assert_eq!(&step(&snapshot, "consume").log, &consume_before);
}

#[test]
fn lifecycle_completion_requires_matching_started_phase() {
    let (_temporary, workflow) = resolved_workflow();
    let base = crate::timing::monotonic_now();
    let clock = ControlledClock::new(point(base, 0));
    let view = model(&workflow, clock);

    view.reconcile_terminal_result(&succeeded_run_result(&workflow, base))
        .unwrap();
    view.mark_quiescent();
    view.complete_publication(WorkflowRunPublicationResult::Succeeded {
        result_directory: "results".to_owned(),
    });
    view.complete_cleanup(WorkflowRunCleanupResult::Succeeded);

    let snapshot = view.snapshot();
    assert_eq!(
        snapshot.publication,
        WorkflowRunPublicationState::NotStarted
    );
    assert_eq!(snapshot.cleanup, WorkflowRunCleanupState::NotStarted);
    assert!(!snapshot.quit_eligible);
}

#[test]
fn completed_successful_lifecycle_requires_explicit_adapter_completion() {
    let (_temporary, workflow) = resolved_workflow();
    let base = crate::timing::monotonic_now();
    let clock = ControlledClock::new(point(base, 0));
    let view = model(&workflow, clock);

    view.reconcile_terminal_result(&succeeded_run_result(&workflow, base))
        .unwrap();
    view.mark_quiescent();
    view.begin_publication();
    view.complete_publication(WorkflowRunPublicationResult::Succeeded {
        result_directory: "results".to_owned(),
    });
    view.begin_cleanup();
    view.complete_cleanup(WorkflowRunCleanupResult::Succeeded);

    let lifecycle_completed = view.snapshot();
    assert_eq!(
        lifecycle_completed.publication,
        WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
            result_directory: "results".to_owned(),
        })
    );
    assert_eq!(
        lifecycle_completed.cleanup,
        WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Succeeded)
    );
    assert!(!lifecycle_completed.quit_eligible);

    view.mark_adapter_lifecycle_completed();
    assert!(view.snapshot().quit_eligible);
}

#[tokio::test]
async fn terminal_result_and_local_lifecycle_do_not_enable_quit() {
    let (_temporary, workflow) = resolved_workflow();
    let base = crate::timing::monotonic_now();
    let clock = ControlledClock::new(point(base, 0));
    let view = model(&workflow, clock.clone());
    let cause = StepFailureCause::Execution(StepExecutionFailure::Command(
        CommandExecutionFailure::UnsuccessfulExit { code: Some(17) },
    ));

    clock.set(point(base, 5));
    view.observe(step_transition(
        "prepare",
        StepStateKind::Running,
        StepStateKind::Failed,
        Some(ObservedStepTransition::Failed {
            phase: FailurePhase::Execution,
            cause: cause.clone(),
        }),
    ))
    .await;
    view.observe(step_transition(
        "consume",
        StepStateKind::Pending,
        StepStateKind::Blocked,
        Some(ObservedStepTransition::Blocked {
            dependency: "prepare".to_owned(),
        }),
    ))
    .await;

    let run = succeeded_run_result(&workflow, base);
    view.reconcile_terminal_result(&run).unwrap();
    let reconciled = view.snapshot();
    assert!(matches!(reconciled.workflow, WorkflowState::Succeeded));
    assert_eq!(step(&reconciled, "prepare").state, StepStateKind::Succeeded);
    assert_eq!(step(&reconciled, "consume").state, StepStateKind::Succeeded);
    assert_eq!(step(&reconciled, "consume").fact, None);
    assert_eq!(
        step(&reconciled, "prepare").outputs["report"],
        WorkflowRunOutputDisposition::Committed
    );
    assert_eq!(
        step(&reconciled, "prepare")
            .timing
            .as_ref()
            .unwrap()
            .started_at,
        point(base, 10).utc
    );
    assert_eq!(reconciled.timing.duration, Duration::from_millis(90));
    assert!(!reconciled.quit_eligible);

    view.mark_quiescent();
    assert!(!view.snapshot().quit_eligible);
    view.begin_publication();
    assert!(!view.snapshot().quit_eligible);
    view.complete_publication(WorkflowRunPublicationResult::Failed(
        WorkflowRunPublicationFailure {
            phase: LocalPublicationPhase::Commit,
            kind: LocalPublicationFailureKind::AtomicPublicationUnavailable,
            export: None,
        },
    ));
    assert!(!view.snapshot().quit_eligible);
    view.begin_cleanup();
    assert!(!view.snapshot().quit_eligible);
    view.complete_cleanup(WorkflowRunCleanupResult::Failed);

    let completed = view.snapshot();
    assert!(!completed.quit_eligible);
    assert!(matches!(completed.workflow, WorkflowState::Succeeded));
    assert_eq!(
        completed.publication,
        WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Failed(
            WorkflowRunPublicationFailure {
                phase: LocalPublicationPhase::Commit,
                kind: LocalPublicationFailureKind::AtomicPublicationUnavailable,
                export: None,
            }
        ))
    );
    assert_eq!(
        completed.cleanup,
        WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Failed)
    );

    view.mark_adapter_lifecycle_completed();
    assert!(view.snapshot().quit_eligible);
    view.mark_adapter_lifecycle_completed();
    assert!(view.snapshot().quit_eligible);
}

fn succeeded_run_result(workflow: &ResolvedWorkflow, base: Instant) -> WorkflowRunResult {
    WorkflowRunResult {
        run_directory: workflow.source.source_root.clone(),
        attempt_number: 1,
        workflow_path: workflow.source.workflow_path.clone(),
        source_root: workflow.source.source_root.clone(),
        content_digest: workflow.content_digest.clone(),
        execution_root: workflow.source.source_root.clone(),
        maximum_parallel_steps: NonZeroUsize::new(2).unwrap(),
        timing: WorkflowRunTiming {
            started_at: point(base, 0).utc,
            finished_at: point(base, 90).utc,
            duration: Duration::from_millis(90),
        },
        outcome: RunOutcome::Succeeded,
        cancellation: None,
        steps: vec![
            super::super::publication::WorkflowRunStep {
                id: "prepare".to_owned(),
                kind: super::super::publication::WorkflowRunStepKind::Command,
                failure_policy: crate::execution::workflow::document::FailurePolicy::Required,
                state: StepState::Succeeded {
                    outputs: BTreeMap::from([(
                        "report".to_owned(),
                        CapturedValue::Text(Arc::from("captured")),
                    )]),
                },
                timing: Some(WorkflowStepTiming {
                    started_at: point(base, 10).utc,
                    duration: Duration::from_millis(30),
                }),
                command_output: None,
            },
            super::super::publication::WorkflowRunStep {
                id: "consume".to_owned(),
                kind: super::super::publication::WorkflowRunStepKind::Command,
                failure_policy: crate::execution::workflow::document::FailurePolicy::Required,
                state: StepState::Succeeded {
                    outputs: BTreeMap::from([(
                        "receipt".to_owned(),
                        CapturedValue::Text(Arc::from("captured")),
                    )]),
                },
                timing: Some(WorkflowStepTiming {
                    started_at: point(base, 45).utc,
                    duration: Duration::from_millis(40),
                }),
                command_output: None,
            },
        ],
        exports: BTreeMap::new(),
        export_sources: BTreeMap::new(),
    }
}
