use std::collections::BTreeMap;
use std::fs;
use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::ops::Add;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::execution::workflow::admission::{
    CancellationPendingPollBarrier, CancellationPolicy, CancellationReason, CancellationSource,
    CaptureLimits, EnvironmentSnapshot, ExecutionContext, ExecutionPolicyLimits, InputLimits,
    ResolvedImports, admit_workflow,
};
use crate::execution::workflow::resolution;
use crate::execution::workflow::runtime::{
    Action, RecoveryDecision, RecoveryTerminalDisposition, StepState, TargetExecutionNumber,
};

const WORKFLOW: &str = r#"schemaVersion: 1
steps:
  task:
    kind: cmd
    command:
      argv: ["true"]
"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TestInstant(Duration);

impl Add<Duration> for TestInstant {
    type Output = Self;

    fn add(self, duration: Duration) -> Self::Output {
        Self(self.0 + duration)
    }
}

#[derive(Clone)]
struct TestClock {
    instant: TestInstant,
    reads: Arc<AtomicUsize>,
}

impl CoordinatorClock for TestClock {
    type Instant = TestInstant;

    fn now(&mut self) -> Self::Instant {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.instant
    }

    async fn wait_until(&self, _deadline: Self::Instant) {
        std::future::pending().await
    }
}

type TestAction<Provisional = String> = RequestedAction<Provisional, String, String, TestInstant>;
type TestCommit = CommittedReduction<String, String, TestInstant>;
type TestResult = CoordinationResult<String, String, TestInstant>;

#[derive(Clone)]
struct DistinctProvisional(&'static str);

impl std::fmt::Debug for DistinctProvisional {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("DistinctProvisional")
            .field(&self.0)
            .finish()
    }
}

impl DriverOccurrenceContent for DistinctProvisional {
    fn update_occurrence_digest(&self, digest: &mut OccurrenceDigestEncoder<'_>) {
        digest.write_bytes(self.0.as_bytes());
    }
}

impl runtime::ProvisionalStepResources for DistinctProvisional {
    fn requires_release(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug)]
struct ReleasableProvisional(&'static str);

impl DriverOccurrenceContent for ReleasableProvisional {
    fn update_occurrence_digest(&self, digest: &mut OccurrenceDigestEncoder<'_>) {
        digest.write_bytes(self.0.as_bytes());
    }
}

impl runtime::ProvisionalStepResources for ReleasableProvisional {
    fn requires_release(&self) -> bool {
        true
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TimelineEntry {
    Commit(OccurrenceOrdinal),
    Action(ActionId),
}

struct RecordingCommitPort {
    commits: mpsc::UnboundedSender<TestCommit>,
    timeline: Arc<Mutex<Vec<TimelineEntry>>>,
}

impl CommitPort<TestCommit> for RecordingCommitPort {
    type Error = std::convert::Infallible;

    fn commit(&mut self, commit: TestCommit) -> impl Future<Output = Result<(), Self::Error>> {
        self.timeline
            .lock()
            .unwrap()
            .push(TimelineEntry::Commit(commit.occurrence_ordinal));
        let _ = self.commits.send(commit);
        ready(Ok(()))
    }
}

struct CommitRelease {
    commit: TestCommit,
    resume: oneshot::Sender<()>,
}

struct ControlledCommitPort {
    commits: mpsc::UnboundedSender<CommitRelease>,
}

impl CommitPort<TestCommit> for ControlledCommitPort {
    type Error = std::convert::Infallible;

    fn commit(&mut self, commit: TestCommit) -> impl Future<Output = Result<(), Self::Error>> {
        let (resume, resumed) = oneshot::channel();
        let sent = self.commits.send(CommitRelease { commit, resume });
        async move {
            if sent.is_ok() {
                let _ = resumed.await;
            }
            Ok(())
        }
    }
}

struct ActionRelease<Provisional = String> {
    action: TestAction<Provisional>,
    resume: oneshot::Sender<()>,
}

struct ControlledActionPort<Provisional = String> {
    actions: mpsc::UnboundedSender<ActionRelease<Provisional>>,
    timeline: Arc<Mutex<Vec<TimelineEntry>>>,
}

impl<Provisional> ActionPort<TestAction<Provisional>> for ControlledActionPort<Provisional> {
    fn release(&mut self, action: TestAction<Provisional>) -> impl Future<Output = ()> {
        self.timeline
            .lock()
            .unwrap()
            .push(TimelineEntry::Action(action.id));
        let (resume, resumed) = oneshot::channel();
        let sent = self.actions.send(ActionRelease { action, resume });
        async move {
            if sent.is_ok() {
                let _ = resumed.await;
            }
        }
    }
}

struct RecordingActionPort<Provisional> {
    actions: Arc<Mutex<Vec<TestAction<Provisional>>>>,
}

impl<Provisional> ActionPort<TestAction<Provisional>> for RecordingActionPort<Provisional> {
    fn release(&mut self, action: TestAction<Provisional>) -> impl Future<Output = ()> {
        self.actions.lock().unwrap().push(action);
        ready(())
    }
}

struct AdmittedFixture {
    _temporary: tempfile::TempDir,
    admitted: AdmittedWorkflow,
}

fn admitted_fixture(source: CancellationSource, grace: Duration) -> AdmittedFixture {
    admitted_fixture_for_workflow(source, grace, WORKFLOW)
}

fn admitted_fixture_for_workflow(
    source: CancellationSource,
    grace: Duration,
    workflow: &str,
) -> AdmittedFixture {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&execution_root).unwrap();
    fs::write(source_root.join("workflow.yaml"), workflow).unwrap();
    let admitted = admit_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedImports::default(),
        ExecutionContext::new(
            execution_root,
            ExecutionPolicyLimits::new(
                1,
                CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
                InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                1024 * 1024,
            ),
            EnvironmentSnapshot::default(),
            CancellationPolicy::new(source, grace),
        ),
    )
    .unwrap();
    AdmittedFixture {
        _temporary: temporary,
        admitted,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScheduleTranscript {
    commits: Vec<TestCommit>,
    actions: Vec<TestAction>,
    timeline: Vec<TimelineEntry>,
    result: TestResult,
}

async fn run_cancellation_schedule() -> ScheduleTranscript {
    let pending_poll = CancellationPendingPollBarrier::new();
    let cancellation = CancellationSource::with_pending_poll_barrier(pending_poll.clone());
    let fixture = admitted_fixture(cancellation.clone(), Duration::from_secs(7));
    let cancellation_request = cancellation.clone();
    let requester = std::thread::spawn(move || {
        pending_poll.wait_until_reached();
        let admitted = cancellation_request.request_cancellation(CancellationReason::UserRequest);
        pending_poll.resume();
        admitted
    });
    let clock_reads = Arc::new(AtomicUsize::new(0));
    let (sender, receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (commit_sender, mut commits) = mpsc::unbounded_channel();
    let (action_sender, mut actions) = mpsc::unbounded_channel();
    let coordinator = Coordinator::new(
        fixture.admitted,
        receiver,
        TestClock {
            instant: TestInstant(Duration::from_secs(100)),
            reads: Arc::clone(&clock_reads),
        },
        RecordingCommitPort {
            commits: commit_sender,
            timeline: Arc::clone(&timeline),
        },
        ControlledActionPort {
            actions: action_sender,
            timeline: Arc::clone(&timeline),
        },
    );

    let driver = async {
        let mut recorded_commits = Vec::new();
        let mut recorded_actions = Vec::new();

        let initialization = commits.recv().await.unwrap();
        assert_eq!(initialization.occurrence_ordinal.get(), 1);
        recorded_commits.push(initialization);
        let start_release = actions.recv().await.unwrap();
        assert!(matches!(
            start_release.action.action,
            Action::StartStep { .. }
        ));
        let step = match &start_release.action.action {
            Action::StartStep { step, .. } => step.clone(),
            _ => String::new(),
        };
        let start_id = start_release.action.id;
        recorded_actions.push(start_release.action.clone());

        sender
            .send(DriverOccurrence::step_started(step.clone(), start_id))
            .await
            .unwrap();
        start_release.resume.send(()).unwrap();

        let cancellation_commit = commits.recv().await.unwrap();
        assert_eq!(cancellation_commit.occurrence_ordinal.get(), 2);
        assert_eq!(cancellation_commit.events.len(), 2);
        assert_eq!(
            cancellation_commit.state.steps[&step].state,
            StepState::Cancelling {
                detail: crate::execution::workflow::evidence::CancellationDetail::new(
                    CancellationReason::UserRequest,
                ),
            }
        );
        recorded_commits.push(cancellation_commit);
        let cancellation_release = actions.recv().await.unwrap();
        let cancel_id = cancellation_release.action.id;
        assert_eq!(cancel_id.transition_sequence.get(), 3);
        assert_eq!(
            cancellation_release.action.action,
            Action::CancelStep {
                step: step.clone(),
                active: runtime::ActiveStepInvocation::Target {
                    execution_number: runtime::TargetExecutionNumber::FIRST,
                },
                reason: CancellationReason::UserRequest,
                deadline: TestInstant(Duration::from_secs(107)),
            }
        );
        recorded_actions.push(cancellation_release.action.clone());
        cancellation_release.resume.send(()).unwrap();

        let stale = commits.recv().await.unwrap();
        assert_eq!(stale.occurrence_ordinal.get(), 3);
        assert!(stale.events.is_empty());
        assert_eq!(stale.state, recorded_commits[1].state);
        recorded_commits.push(stale);
        assert!(actions.try_recv().is_err());

        sender
            .send(DriverOccurrence::step_quiesced(step.clone(), cancel_id))
            .await
            .unwrap();
        let terminal = commits.recv().await.unwrap();
        assert_eq!(terminal.occurrence_ordinal.get(), 4);
        assert_eq!(terminal.events.len(), 2);
        assert_eq!(
            terminal.state.steps[&step].state,
            StepState::Cancelled {
                detail: crate::execution::workflow::evidence::CancellationDetail::new(
                    CancellationReason::UserRequest,
                ),
            }
        );
        assert_eq!(
            terminal.state.workflow,
            WorkflowState::Cancelled {
                reason: CancellationReason::UserRequest,
            }
        );
        recorded_commits.push(terminal);
        let finish_release = actions.recv().await.unwrap();
        assert!(matches!(
            finish_release.action.action,
            Action::FinishRun { .. }
        ));
        recorded_actions.push(finish_release.action.clone());

        assert!(
            sender
                .send(DriverOccurrence::step_quiesced(step, cancel_id))
                .await
                .is_err()
        );
        assert!(actions.try_recv().is_err());
        finish_release.resume.send(()).unwrap();

        (recorded_commits, recorded_actions)
    };

    let (result, (commits, actions)) = tokio::join!(coordinator.run(), driver);
    assert!(requester.join().unwrap());
    assert_eq!(clock_reads.load(Ordering::SeqCst), 1);
    ScheduleTranscript {
        commits,
        actions,
        timeline: timeline.lock().unwrap().clone(),
        result: result.unwrap(),
    }
}

#[tokio::test]
async fn cancellation_during_boundary_commit_is_retained_for_finalization() {
    const FINALIZER_WORKFLOW: &str = r#"schemaVersion: 1
steps:
  task:
    kind: cmd
    command:
      argv: ["true"]
finalizers:
  cleanup:
    kind: cmd
    command:
      argv: ["true"]
"#;
    let cancellation = CancellationSource::new();
    let fixture = admitted_fixture_for_workflow(
        cancellation.clone(),
        Duration::from_secs(7),
        FINALIZER_WORKFLOW,
    );
    let (sender, receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (commit_sender, mut commits) = mpsc::unbounded_channel();
    let (action_sender, mut actions) = mpsc::unbounded_channel();
    let coordinator = Coordinator::<String, String, String, _, _, _>::new(
        fixture.admitted,
        receiver,
        TestClock {
            instant: TestInstant(Duration::ZERO),
            reads: Arc::new(AtomicUsize::new(0)),
        },
        ControlledCommitPort {
            commits: commit_sender,
        },
        ControlledActionPort {
            actions: action_sender,
            timeline,
        },
    );

    let driver = async {
        let initialization = commits.recv().await.unwrap();
        initialization.resume.send(()).unwrap();
        let start = actions.recv().await.unwrap();
        let Action::StartStep { step, .. } = &start.action.action else {
            panic!("workflow did not request its ordinary start");
        };
        assert_eq!(step, "task");
        let start_id = start.action.id;
        start.resume.send(()).unwrap();
        sender
            .send(DriverOccurrence::step_started("task".to_owned(), start_id))
            .await
            .unwrap();
        let started = commits.recv().await.unwrap();
        started.resume.send(()).unwrap();

        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        let ordinary_cancellation = commits.recv().await.unwrap();
        ordinary_cancellation.resume.send(()).unwrap();
        let cancel = actions.recv().await.unwrap();
        assert!(matches!(cancel.action.action, Action::CancelStep { .. }));
        let cancel_id = cancel.action.id;
        cancel.resume.send(()).unwrap();
        sender
            .send(DriverOccurrence::step_quiesced(
                "task".to_owned(),
                cancel_id,
            ))
            .await
            .unwrap();

        // The commit port has durably accepted the boundary but deliberately keeps
        // its future pending so another producer can request phase-local cancellation.
        let boundary = commits.recv().await.unwrap();
        assert!(matches!(
            boundary.commit.state.workflow,
            WorkflowState::Finalizing { .. }
        ));
        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::UserRequest)
        );
        assert!(cancellation.request_cancellation(CancellationReason::RunnerShutdown));
        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::UserRequest)
        );
        boundary.resume.send(()).unwrap();

        let finalizer_start = actions.recv().await.unwrap();
        let Action::StartStep { step, .. } = &finalizer_start.action.action else {
            panic!("workflow did not request its finalizer start");
        };
        assert_eq!(step, "cleanup");
        finalizer_start.resume.send(()).unwrap();
        let cancelled = commits.recv().await.unwrap();
        assert!(cancelled.commit.events.iter().any(|event| matches!(
            event,
            TransitionEvent::FinalizationCancellationAccepted {
                reason: CancellationReason::RunnerShutdown,
                ..
            }
        )));
        cancelled.resume.send(()).unwrap();
        let cancel = actions.recv().await.unwrap();
        assert!(matches!(cancel.action.action, Action::CancelStep { .. }));
        let cancel_id = cancel.action.id;
        cancel.resume.send(()).unwrap();
        sender
            .send(DriverOccurrence::step_quiesced(
                "cleanup".to_owned(),
                cancel_id,
            ))
            .await
            .unwrap();
        let terminal = commits.recv().await.unwrap();
        terminal.resume.send(()).unwrap();
        let finish = actions.recv().await.unwrap();
        assert!(matches!(finish.action.action, Action::FinishRun { .. }));
        finish.resume.send(()).unwrap();
    };

    let (result, ()) = tokio::join!(coordinator.run(), driver);
    assert!(matches!(
        result.unwrap().state.workflow,
        WorkflowState::Cancelled {
            reason: CancellationReason::UserRequest
        }
    ));
}

struct FailingCommitPort;

impl CommitPort<TestCommit> for FailingCommitPort {
    type Error = ();

    fn commit(&mut self, _commit: TestCommit) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Err(()))
    }
}

#[tokio::test]
async fn transition_ceiling_failure_is_typed_and_committed_as_a_diagnostic() {
    let mut fixture = admitted_fixture(CancellationSource::new(), Duration::from_secs(7));
    fixture.admitted.set_transition_ceiling(1);
    let (sender, receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (commit_sender, mut commits) = mpsc::unbounded_channel();
    let (action_sender, mut actions) = mpsc::unbounded_channel();
    let coordinator = Coordinator::<String, String, String, _, _, _>::new(
        fixture.admitted,
        receiver,
        TestClock {
            instant: TestInstant(Duration::ZERO),
            reads: Arc::new(AtomicUsize::new(0)),
        },
        RecordingCommitPort {
            commits: commit_sender,
            timeline: Arc::clone(&timeline),
        },
        ControlledActionPort {
            actions: action_sender,
            timeline,
        },
    );

    let driver = async {
        let initialization = commits.recv().await.unwrap();
        assert_eq!(initialization.state.last_transition_sequence.get(), 1);
        assert_eq!(initialization.diagnostic, None);
        let start = actions.recv().await.unwrap();
        let action = start.action.id;
        start.resume.send(()).unwrap();
        sender
            .send(DriverOccurrence::step_started("task".to_owned(), action))
            .await
            .unwrap();
        let diagnostic = commits.recv().await.unwrap();
        assert_eq!(diagnostic.state.last_transition_sequence.get(), 1);
        assert_eq!(
            diagnostic.diagnostic,
            Some(CoordinationDiagnostic::TransitionCapacityExceeded)
        );
        assert!(!diagnostic.occurrence_accepted);
        assert!(diagnostic.events.is_empty());
        assert!(diagnostic.actions.is_empty());
    };

    let (result, ()) = tokio::join!(coordinator.run(), driver);
    assert_eq!(result, Err(CoordinationError::TransitionCapacityExceeded));
}

#[tokio::test]
async fn initialization_capacity_failure_is_committed_as_a_diagnostic() {
    let mut fixture = admitted_fixture(CancellationSource::new(), Duration::from_secs(7));
    fixture.admitted.set_transition_ceiling(0);
    let (_sender, receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (commit_sender, mut commits) = mpsc::unbounded_channel();
    let (action_sender, _actions) = mpsc::unbounded_channel();
    let coordinator = Coordinator::<String, String, String, _, _, _>::new(
        fixture.admitted,
        receiver,
        TestClock {
            instant: TestInstant(Duration::ZERO),
            reads: Arc::new(AtomicUsize::new(0)),
        },
        RecordingCommitPort {
            commits: commit_sender,
            timeline: Arc::clone(&timeline),
        },
        ControlledActionPort {
            actions: action_sender,
            timeline,
        },
    );

    assert_eq!(
        coordinator.run().await,
        Err(CoordinationError::TransitionCapacityExceeded)
    );
    let diagnostic = commits
        .try_recv()
        .expect("initialization capacity failure was not durably committed");
    assert_eq!(
        diagnostic.diagnostic,
        Some(CoordinationDiagnostic::TransitionCapacityExceeded)
    );
}

#[tokio::test]
async fn failed_commit_prevents_initial_action_release() {
    let fixture = admitted_fixture(CancellationSource::new(), Duration::from_secs(7));
    let (_sender, receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (action_sender, mut actions) = mpsc::unbounded_channel();
    let coordinator = Coordinator::<String, String, String, _, _, _>::new(
        fixture.admitted,
        receiver,
        TestClock {
            instant: TestInstant(Duration::ZERO),
            reads: Arc::new(AtomicUsize::new(0)),
        },
        FailingCommitPort,
        ControlledActionPort {
            actions: action_sender,
            timeline: Arc::clone(&timeline),
        },
    );

    assert_eq!(
        coordinator.run().await,
        Err(CoordinationError::CommitFailed)
    );
    assert!(actions.try_recv().is_err());
    assert!(timeline.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancellation_after_pending_poll_precedes_and_preserves_ready_driver() {
    let first = run_cancellation_schedule().await;
    let second = run_cancellation_schedule().await;

    assert_eq!(first, second);
    assert_eq!(first.result.last_occurrence_ordinal.get(), 4);
    assert_eq!(
        first.timeline,
        [
            TimelineEntry::Commit(OccurrenceOrdinal(1)),
            TimelineEntry::Action(first.actions[0].id),
            TimelineEntry::Commit(OccurrenceOrdinal(2)),
            TimelineEntry::Action(first.actions[1].id),
            TimelineEntry::Commit(OccurrenceOrdinal(3)),
            TimelineEntry::Commit(OccurrenceOrdinal(4)),
            TimelineEntry::Action(first.actions[2].id),
        ]
    );
    assert!(matches!(first.actions[0].action, Action::StartStep { .. }));
    assert!(matches!(first.actions[1].action, Action::CancelStep { .. }));
    assert!(matches!(first.actions[2].action, Action::FinishRun { .. }));
}

#[tokio::test]
async fn acknowledged_completion_claim_precedes_later_cancellation() {
    let cancellation = CancellationSource::new();
    let fixture = admitted_fixture(cancellation.clone(), Duration::from_secs(7));
    let clock_reads = Arc::new(AtomicUsize::new(0));
    let (sender, receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (commit_sender, mut commits) = mpsc::unbounded_channel();
    let (action_sender, mut actions) = mpsc::unbounded_channel();
    let coordinator = Coordinator::new(
        fixture.admitted,
        receiver,
        TestClock {
            instant: TestInstant(Duration::from_secs(100)),
            reads: Arc::clone(&clock_reads),
        },
        RecordingCommitPort {
            commits: commit_sender,
            timeline: Arc::clone(&timeline),
        },
        ControlledActionPort {
            actions: action_sender,
            timeline,
        },
    );

    let driver = async {
        assert_eq!(commits.recv().await.unwrap().occurrence_ordinal.get(), 1);
        let start = actions.recv().await.unwrap();
        let Action::StartStep { step, .. } = &start.action.action else {
            panic!("workflow did not request its command start");
        };
        let step = step.clone();
        let start_id = start.action.id;
        start.resume.send(()).unwrap();

        sender
            .send(DriverOccurrence::step_started(step.clone(), start_id))
            .await
            .unwrap();
        let running = commits.recv().await.unwrap();
        assert_eq!(running.occurrence_ordinal.get(), 2);
        assert_eq!(running.state.steps[&step].state, StepState::Running);

        let claim = sender
            .claim(DriverOccurrence::step_execution_completed(
                step.clone(),
                start_id,
                "completed".to_owned(),
            ))
            .await
            .unwrap();
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        claim.publish().unwrap();

        let terminal = commits.recv().await.unwrap();
        assert_eq!(terminal.occurrence_ordinal.get(), 3);
        assert_eq!(terminal.state.workflow, WorkflowState::Succeeded);
        assert_eq!(
            terminal.state.steps[&step].state,
            StepState::Succeeded {
                outputs: BTreeMap::new(),
            }
        );
        let finish = actions.recv().await.unwrap();
        assert!(matches!(finish.action.action, Action::FinishRun { .. }));
        finish.resume.send(()).unwrap();
    };

    let (result, ()) = tokio::join!(coordinator.run(), driver);
    assert_eq!(result.unwrap().state.workflow, WorkflowState::Succeeded);
    assert_eq!(clock_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn discarded_completion_claim_releases_provisional_resources() {
    let fixture = admitted_fixture(CancellationSource::new(), Duration::from_secs(7));
    let (sender, receiver) =
        occurrence_channel::<ReleasableProvisional, String, String>(NonZeroUsize::new(2).unwrap());
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (commit_sender, mut commits) = mpsc::unbounded_channel();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let coordinator = Coordinator::new(
        fixture.admitted,
        receiver,
        TestClock {
            instant: TestInstant(Duration::ZERO),
            reads: Arc::new(AtomicUsize::new(0)),
        },
        RecordingCommitPort {
            commits: commit_sender,
            timeline,
        },
        RecordingActionPort {
            actions: Arc::clone(&actions),
        },
    );

    let driver = async {
        let initialized = commits.recv().await.unwrap();
        let start = initialized.actions[0].id;
        let claim = sender
            .claim(DriverOccurrence::step_execution_completed(
                "task".to_owned(),
                start,
                ReleasableProvisional("staging"),
            ))
            .await
            .unwrap();
        claim.discard();
        drop(sender);
    };

    let (result, ()) = tokio::join!(coordinator.run(), driver);
    assert_eq!(result, Err(CoordinationError::OccurrenceChannelClosed));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 2);
    let Action::ReleaseStepResources { provisional } = &actions[1].action else {
        panic!("discarded completion did not release its provisional resources");
    };
    assert_eq!(provisional.0, "staging");
}

#[tokio::test]
async fn execution_start_samples_an_already_admitted_cancellation() {
    let cancellation = CancellationSource::new();
    assert!(cancellation.request_cancellation(CancellationReason::RunnerShutdown));
    let fixture = admitted_fixture(cancellation, Duration::from_secs(11));
    let clock_reads = Arc::new(AtomicUsize::new(0));
    let (sender, receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (commit_sender, mut commits) = mpsc::unbounded_channel();
    let (action_sender, mut actions) = mpsc::unbounded_channel();
    let coordinator = Coordinator::<String, String, String, _, _, _>::new(
        fixture.admitted,
        receiver,
        TestClock {
            instant: TestInstant(Duration::from_secs(200)),
            reads: Arc::clone(&clock_reads),
        },
        RecordingCommitPort {
            commits: commit_sender,
            timeline: Arc::clone(&timeline),
        },
        ControlledActionPort {
            actions: action_sender,
            timeline: Arc::clone(&timeline),
        },
    );

    let observer = async {
        let commit = commits.recv().await.unwrap();
        assert_eq!(commit.occurrence_ordinal.get(), 1);
        assert_eq!(
            commit.state.workflow,
            WorkflowState::Cancelled {
                reason: CancellationReason::RunnerShutdown,
            }
        );
        assert_eq!(
            commit.state.last_cancellation_operation,
            Some(crate::execution::workflow::admission::CancellationOperationId::fixture(1))
        );
        let release = actions.recv().await.unwrap();
        assert!(matches!(release.action.action, Action::FinishRun { .. }));
        assert!(
            sender
                .send(DriverOccurrence::step_started(
                    "task".to_owned(),
                    release.action.id,
                ))
                .await
                .is_err()
        );
        release.resume.send(()).unwrap();
    };

    let (result, ()) = tokio::join!(coordinator.run(), observer);
    assert_eq!(clock_reads.load(Ordering::SeqCst), 1);
    assert_eq!(result.unwrap().last_occurrence_ordinal.get(), 1);
    let timeline = timeline.lock().unwrap();
    assert_eq!(timeline.len(), 2);
    assert_eq!(timeline[0], TimelineEntry::Commit(OccurrenceOrdinal(1)));
    assert!(matches!(timeline[1], TimelineEntry::Action(_)));
}

#[tokio::test]
async fn scripted_handlerless_port_retains_provisional_history_before_recheck() {
    const RECOVERY_WORKFLOW: &str = r#"schemaVersion: 1
steps:
  fetch:
    kind: cmd
    recovery:
      retries: 2
    command:
      argv: ["true"]
    outputs:
      result:
        kind: file
        from: path
        path: result.txt
        mediaType: text/plain
"#;
    let fixture = admitted_fixture_for_workflow(
        CancellationSource::new(),
        Duration::from_secs(7),
        RECOVERY_WORKFLOW,
    );
    let (sender, receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (commit_sender, mut commits) = mpsc::unbounded_channel();
    let (action_sender, mut actions) = mpsc::unbounded_channel::<ActionRelease<String>>();
    let coordinator = Coordinator::new(
        fixture.admitted,
        receiver,
        TestClock {
            instant: TestInstant(Duration::ZERO),
            reads: Arc::new(AtomicUsize::new(0)),
        },
        RecordingCommitPort {
            commits: commit_sender,
            timeline: Arc::clone(&timeline),
        },
        ControlledActionPort {
            actions: action_sender,
            timeline,
        },
    );

    let driver = async {
        let initialized = commits.recv().await.unwrap();
        assert_eq!(initialized.occurrence_ordinal.get(), 1);
        assert_eq!(initialized.state.admitted_transition_ceiling(), 18);
        let first = actions.recv().await.unwrap();
        let Action::StartStep {
            step,
            execution_number,
            ..
        } = &first.action.action
        else {
            panic!("scripted port did not receive target execution one");
        };
        assert_eq!(step, "fetch");
        assert_eq!(execution_number.get(), 1);
        let first_id = first.action.id;
        sender
            .send(DriverOccurrence::step_started("fetch".into(), first_id))
            .await
            .unwrap();
        first.resume.send(()).unwrap();

        assert_eq!(commits.recv().await.unwrap().occurrence_ordinal.get(), 2);
        sender
            .send(DriverOccurrence::step_execution_failed(
                "fetch".into(),
                first_id,
                "exit 75".into(),
            ))
            .await
            .unwrap();
        let provisional = commits.recv().await.unwrap();
        assert_eq!(provisional.occurrence_ordinal.get(), 3);
        assert_eq!(provisional.state.steps["fetch"].state, StepState::Starting);
        let history = &provisional.state.steps["fetch"]
            .recovery
            .as_ref()
            .unwrap()
            .rounds;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].failed_execution.cause, "exit 75");
        assert!(provisional.occurrence_accepted);
        assert!(provisional.events.iter().all(|event| {
            !matches!(
                event,
                TransitionEvent::Step {
                    to: runtime::StepStateKind::Failed,
                    ..
                }
            )
        }));
        let second = actions.recv().await.unwrap();
        let Action::StartStep {
            execution_number, ..
        } = second.action.action
        else {
            panic!("handlerless provisional failure did not release a full target recheck");
        };
        assert_eq!(execution_number.get(), 2);
        assert_ne!(second.action.id, first_id);
        let second_id = second.action.id;
        sender
            .send(DriverOccurrence::step_started("fetch".into(), second_id))
            .await
            .unwrap();
        second.resume.send(()).unwrap();

        assert_eq!(commits.recv().await.unwrap().occurrence_ordinal.get(), 4);
        sender
            .send(DriverOccurrence::step_execution_completed(
                "fetch".into(),
                second_id,
                "execution-two-candidate".into(),
            ))
            .await
            .unwrap();
        let capturing = commits.recv().await.unwrap();
        assert_eq!(capturing.occurrence_ordinal.get(), 5);
        let capture = actions.recv().await.unwrap();
        assert!(matches!(
            capture.action.action,
            Action::CaptureOutputs { .. }
        ));
        let capture_id = capture.action.id;
        sender
            .send(DriverOccurrence::outputs_captured(
                "fetch".into(),
                capture_id,
                BTreeMap::from([("result".into(), "execution-two-output".into())]),
            ))
            .await
            .unwrap();
        capture.resume.send(()).unwrap();

        let terminal = commits.recv().await.unwrap();
        assert_eq!(terminal.occurrence_ordinal.get(), 6);
        assert_eq!(terminal.state.workflow, WorkflowState::Succeeded);
        assert_eq!(
            terminal.state.steps["fetch"]
                .recovery
                .as_ref()
                .unwrap()
                .terminal_disposition,
            Some(RecoveryTerminalDisposition::Recovered {
                execution_number: TargetExecutionNumber::fixture(2),
            })
        );
        assert_eq!(
            terminal.state.steps["fetch"].state,
            StepState::Succeeded {
                outputs: BTreeMap::from([("result".into(), "execution-two-output".into())]),
            }
        );
        let finish = actions.recv().await.unwrap();
        assert!(matches!(finish.action.action, Action::FinishRun { .. }));
        finish.resume.send(()).unwrap();
    };

    let (result, ()) = tokio::join!(coordinator.run(), driver);
    assert_eq!(result.unwrap().state.workflow, WorkflowState::Succeeded);
}

#[tokio::test]
async fn changed_payload_for_one_handler_occurrence_identity_fails_closed() {
    const HANDLER_WORKFLOW: &str = r#"schemaVersion: 1
steps:
  verify:
    kind: cmd
    recovery:
      retries: 1
      handler:
        kind: cmd
        command:
          argv: ["true"]
    command:
      argv: ["false"]
"#;
    let fixture = admitted_fixture_for_workflow(
        CancellationSource::new(),
        Duration::from_secs(7),
        HANDLER_WORKFLOW,
    );
    let (sender, receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (commit_sender, mut commits) = mpsc::unbounded_channel();
    let (action_sender, mut actions) = mpsc::unbounded_channel::<ActionRelease<String>>();
    let coordinator = Coordinator::new(
        fixture.admitted,
        receiver,
        TestClock {
            instant: TestInstant(Duration::ZERO),
            reads: Arc::new(AtomicUsize::new(0)),
        },
        RecordingCommitPort {
            commits: commit_sender,
            timeline: Arc::clone(&timeline),
        },
        ControlledActionPort {
            actions: action_sender,
            timeline,
        },
    );

    let driver = async {
        let _ = commits.recv().await.unwrap();
        let target = actions.recv().await.unwrap();
        let target_id = target.action.id;
        sender
            .send(DriverOccurrence::step_started("verify".into(), target_id))
            .await
            .unwrap();
        target.resume.send(()).unwrap();
        let _ = commits.recv().await.unwrap();
        sender
            .send(DriverOccurrence::step_execution_failed(
                "verify".into(),
                target_id,
                "target failure".into(),
            ))
            .await
            .unwrap();
        let recovering = commits.recv().await.unwrap();
        assert!(matches!(
            recovering.state.steps["verify"].state,
            StepState::Recovering { .. }
        ));
        let handler = actions.recv().await.unwrap();
        let Action::StartRecoveryHandler { round, .. } = handler.action.action else {
            panic!("scripted handler port did not receive its action");
        };
        let wrong_round = round.next().unwrap();
        let handler_id = handler.action.id;
        sender
            .send(DriverOccurrence::recovery_handler_started(
                "verify".into(),
                round,
                handler_id,
            ))
            .await
            .unwrap();
        handler.resume.send(()).unwrap();
        let _ = commits.recv().await.unwrap();

        let exact = DriverOccurrence::recovery_handler_completed(
            "verify".into(),
            wrong_round,
            handler_id,
            RecoveryDecision::recheck("unchanged", "wrong round"),
        );
        sender.send(exact.clone()).await.unwrap();
        let first_stale = commits.recv().await.unwrap();
        assert!(!first_stale.occurrence_accepted);
        sender.send(exact).await.unwrap();
        let duplicate = commits.recv().await.unwrap();
        assert!(!duplicate.occurrence_accepted);
        assert!(duplicate.events.is_empty());

        sender
            .send(DriverOccurrence::recovery_handler_completed(
                "verify".into(),
                wrong_round,
                handler_id,
                RecoveryDecision::recheck("changed", "same identity"),
            ))
            .await
            .unwrap();
    };

    let (result, ()) = tokio::join!(coordinator.run(), driver);
    assert_eq!(result, Err(CoordinationError::OccurrenceConflict));
}

#[tokio::test]
async fn conflicting_provisional_payload_uses_content_digest_without_payload_equality() {
    const CAPTURE_WORKFLOW: &str = r#"schemaVersion: 1
steps:
  task:
    kind: cmd
    command:
      argv: ["true"]
    outputs:
      result:
        kind: file
        from: path
        path: result.txt
        mediaType: text/plain
"#;
    let fixture = admitted_fixture_for_workflow(
        CancellationSource::new(),
        Duration::from_secs(7),
        CAPTURE_WORKFLOW,
    );
    let (sender, receiver) =
        occurrence_channel::<DistinctProvisional, String, String>(NonZeroUsize::new(4).unwrap());
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (commit_sender, mut commits) = mpsc::unbounded_channel();
    let (action_sender, mut actions) =
        mpsc::unbounded_channel::<ActionRelease<DistinctProvisional>>();
    let coordinator = Coordinator::new(
        fixture.admitted,
        receiver,
        TestClock {
            instant: TestInstant(Duration::ZERO),
            reads: Arc::new(AtomicUsize::new(0)),
        },
        RecordingCommitPort {
            commits: commit_sender,
            timeline: Arc::clone(&timeline),
        },
        ControlledActionPort {
            actions: action_sender,
            timeline,
        },
    );

    let driver = async {
        let _ = commits.recv().await.unwrap();
        let start = actions.recv().await.unwrap();
        let action = start.action.id;
        sender
            .send(DriverOccurrence::step_started("task".into(), action))
            .await
            .unwrap();
        start.resume.send(()).unwrap();
        let _ = commits.recv().await.unwrap();

        sender
            .send(DriverOccurrence::step_execution_completed(
                "task".into(),
                action,
                DistinctProvisional("first"),
            ))
            .await
            .unwrap();
        let capturing = commits.recv().await.unwrap();
        assert!(capturing.occurrence_accepted);
        let capture = actions.recv().await.unwrap();
        assert!(matches!(
            capture.action.action,
            Action::CaptureOutputs { .. }
        ));
        capture.resume.send(()).unwrap();

        sender
            .send(DriverOccurrence::step_execution_completed(
                "task".into(),
                action,
                DistinctProvisional("changed"),
            ))
            .await
            .unwrap();
    };

    let (result, ()) = tokio::join!(coordinator.run(), driver);
    assert_eq!(result, Err(CoordinationError::OccurrenceConflict));
}

#[tokio::test]
async fn exact_replay_of_an_early_stale_occurrence_remains_inert() {
    let fixture = admitted_fixture(CancellationSource::new(), Duration::from_secs(7));
    let (sender, receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (commit_sender, mut commits) = mpsc::unbounded_channel();
    let (action_sender, mut actions) = mpsc::unbounded_channel();
    let coordinator = Coordinator::new(
        fixture.admitted,
        receiver,
        TestClock {
            instant: TestInstant(Duration::ZERO),
            reads: Arc::new(AtomicUsize::new(0)),
        },
        RecordingCommitPort {
            commits: commit_sender,
            timeline: Arc::clone(&timeline),
        },
        ControlledActionPort {
            actions: action_sender,
            timeline,
        },
    );

    let driver = async {
        let initialized = commits.recv().await.unwrap();
        assert_eq!(initialized.occurrence_ordinal.get(), 1);
        let start = actions.recv().await.unwrap();
        let start_id = start.action.id;
        start.resume.send(()).unwrap();

        let early_failure = DriverOccurrence::step_execution_failed(
            "task".into(),
            start_id,
            "early failure".into(),
        );
        sender.send(early_failure.clone()).await.unwrap();
        let early_stale = commits.recv().await.unwrap();
        assert!(!early_stale.occurrence_accepted);
        assert_eq!(early_stale.state.steps["task"].state, StepState::Starting);

        sender
            .send(DriverOccurrence::step_started("task".into(), start_id))
            .await
            .unwrap();
        let running = commits.recv().await.unwrap();
        assert!(running.occurrence_accepted);
        assert_eq!(running.state.steps["task"].state, StepState::Running);

        sender.send(early_failure).await.unwrap();
        let replayed = commits.recv().await.unwrap();
        assert!(
            !replayed.occurrence_accepted,
            "an exact replay became authoritative after the step entered a compatible state"
        );
        assert_eq!(replayed.state.steps["task"].state, StepState::Running);

        sender
            .send(DriverOccurrence::step_execution_completed(
                "task".into(),
                start_id,
                String::new(),
            ))
            .await
            .unwrap();
        let terminal = commits.recv().await.unwrap();
        assert_eq!(terminal.state.workflow, WorkflowState::Succeeded);
        let finish = actions.recv().await.unwrap();
        finish.resume.send(()).unwrap();
    };

    let (result, ()) = tokio::join!(coordinator.run(), driver);
    assert_eq!(result.unwrap().state.workflow, WorkflowState::Succeeded);
}
