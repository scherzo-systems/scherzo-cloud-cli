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
    CaptureLimits, EnvironmentSnapshot, ExecutionContext, ExecutionPolicyLimits,
    ExecutionRootLifecycle, InputLimits, ResolvedImports, admit_workflow,
};
use crate::execution::workflow::resolution;
use crate::execution::workflow::runtime::{Action, StepState};

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

type TestAction = RequestedAction<String, String, String, TestInstant>;
type TestCommit = CommittedReduction<String, String, TestInstant>;
type TestResult = CoordinationResult<String, String>;

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

struct ActionRelease {
    action: TestAction,
    resume: oneshot::Sender<()>,
}

struct ControlledActionPort {
    actions: mpsc::UnboundedSender<ActionRelease>,
    timeline: Arc<Mutex<Vec<TimelineEntry>>>,
}

impl ActionPort<TestAction> for ControlledActionPort {
    fn release(&mut self, action: TestAction) -> impl Future<Output = ()> {
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

struct AdmittedFixture {
    _temporary: tempfile::TempDir,
    admitted: AdmittedWorkflow,
}

fn admitted_fixture(source: CancellationSource, grace: Duration) -> AdmittedFixture {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&execution_root).unwrap();
    fs::write(source_root.join("workflow.yaml"), WORKFLOW).unwrap();
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
                reason: CancellationReason::UserRequest,
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
                reason: CancellationReason::UserRequest,
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

struct FailingCommitPort;

impl CommitPort<TestCommit> for FailingCommitPort {
    type Error = ();

    fn commit(&mut self, _commit: TestCommit) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Err(()))
    }
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
