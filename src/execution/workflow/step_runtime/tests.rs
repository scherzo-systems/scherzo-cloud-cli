use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::TcpStream as StandardTcpStream;
use std::num::NonZeroUsize;
use std::ops::Add;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};

use super::*;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationReason, CancellationSource, EnvironmentSnapshot,
    ExecutionContext, ExecutionRootLifecycle, ResolvedImports, admit_workflow,
};
use crate::execution::workflow::artifact::{ArtifactStaging, CapturedArtifact};
use crate::execution::workflow::coordinator::{
    CommitPort, CommittedReduction, CoordinationError, CoordinatorClock, OccurrenceReceiver,
    occurrence_channel,
};
use crate::execution::workflow::resolution;
use crate::execution::workflow::runtime::{
    self, Action, Occurrence, RequestedAction, StepState, WorkflowState,
};

const FIXTURE_TEST_NAME: &str = "execution::workflow::step_runtime::tests::command_fixture_process";
const FIXTURE_SOCKET: &str = "WORKFLOW_FIXTURE_SOCKET";
const FIXTURE_EXIT_CODE: &str = "WORKFLOW_FIXTURE_EXIT_CODE";
const FIXTURE_MODE: &str = "WORKFLOW_FIXTURE_MODE";
const FIXTURE_ROLE: &str = "WORKFLOW_FIXTURE_ROLE";
const FIXTURE_MODE_INTERRUPTIBLE: &str = "interruptible-group";
const FIXTURE_MODE_STUBBORN: &str = "stubborn-group";
const FIXTURE_MODE_PARENT_EXITS: &str = "parent-exits";
const FIXTURE_PARENT: &str = "parent";
const FIXTURE_DESCENDANT: &str = "descendant";
const LITERAL_ARGUMENT: &str = "literal * $HOME; [not-a-glob]";
const TEST_WATCHDOG: Duration = Duration::from_secs(10);

type TestOccurrence = Occurrence<(), StepFailureCause, CapturedArtifact, ()>;
type TestReceiver = OccurrenceReceiver<(), StepFailureCause, CapturedArtifact>;
type TestRequestedAction = RequestedAction<(), StepFailureCause, CapturedArtifact, TestInstant>;

#[derive(Debug, Deserialize, Serialize)]
struct FixtureReport {
    role: String,
    arguments: Vec<String>,
    current_directory: PathBuf,
    environment: BTreeMap<String, String>,
    process_group_leader: bool,
    process_group: i32,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct FixtureEvent {
    event: String,
}

enum FixtureCommand {
    Interrupted,
    Released,
}

#[test]
#[ignore = "launched only as the repository-owned workflow command fixture"]
fn command_fixture_process() {
    let socket = std::env::var(FIXTURE_SOCKET).unwrap();
    let exit_code = std::env::var(FIXTURE_EXIT_CODE)
        .unwrap()
        .parse::<i32>()
        .unwrap();
    let mode = std::env::var(FIXTURE_MODE).ok();
    let role = std::env::var(FIXTURE_ROLE).unwrap_or_else(|_| "single".to_owned());
    let mut descendant = if role == FIXTURE_PARENT {
        Some(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(fixture_arguments())
                .env(FIXTURE_ROLE, FIXTURE_DESCENDANT)
                .spawn()
                .unwrap(),
        )
    } else {
        None
    };

    let mut control = StandardTcpStream::connect(socket).unwrap();
    let commands = if mode.as_deref() == Some(FIXTURE_MODE_INTERRUPTIBLE) {
        let (commands, command) = std::sync::mpsc::channel();
        let interrupted_command = commands.clone();
        let mut interrupted = control.try_clone().unwrap();
        ctrlc::set_handler(move || {
            interrupted
                .write_all(b"{\"event\":\"interrupted\"}\n")
                .unwrap();
            interrupted.flush().unwrap();
            interrupted_command
                .send(FixtureCommand::Interrupted)
                .unwrap();
        })
        .unwrap();
        Some((commands, command))
    } else {
        if mode.as_deref() == Some(FIXTURE_MODE_STUBBORN) {
            let mut interrupted = control.try_clone().unwrap();
            ctrlc::set_handler(move || {
                interrupted
                    .write_all(b"{\"event\":\"interrupted\"}\n")
                    .unwrap();
                interrupted.flush().unwrap();
            })
            .unwrap();
        }
        None
    };

    let process_id = rustix::process::getpid();
    let process_group = rustix::process::getpgrp();
    let report = FixtureReport {
        role: role.clone(),
        arguments: std::env::args().skip(1).collect(),
        current_directory: std::env::current_dir().unwrap(),
        environment: std::env::vars().collect(),
        process_group_leader: process_id == process_group,
        process_group: process_group.as_raw_pid(),
    };
    serde_json::to_writer(&mut control, &report).unwrap();
    control.write_all(b"\n").unwrap();
    control.flush().unwrap();

    if let Some((released_command, command)) = commands {
        let mut released = control.try_clone().unwrap();
        drop(std::thread::spawn(move || {
            let mut release = [0_u8; 1];
            released.read_exact(&mut release).unwrap();
            assert_eq!(release, [1]);
            released_command.send(FixtureCommand::Released).unwrap();
        }));
        match command.recv().unwrap() {
            FixtureCommand::Interrupted | FixtureCommand::Released => {}
        }
    } else if mode.as_deref() == Some(FIXTURE_MODE_PARENT_EXITS) && role == FIXTURE_DESCENDANT {
        let mut probe = [0_u8; 1];
        control.read_exact(&mut probe).unwrap();
        assert_eq!(probe, [2]);
        control.write_all(b"{\"event\":\"alive\"}\n").unwrap();
        control.flush().unwrap();
        let mut release = [0_u8; 1];
        control.read_exact(&mut release).unwrap();
        assert_eq!(release, [1]);
    } else {
        let mut release = [0_u8; 1];
        control.read_exact(&mut release).unwrap();
        assert_eq!(release, [1]);
    }
    if mode.as_deref() != Some(FIXTURE_MODE_PARENT_EXITS)
        && let Some(descendant) = descendant.as_mut()
    {
        assert!(descendant.wait().unwrap().success());
    }
    if exit_code != 0 {
        process::exit(exit_code);
    }
}

#[derive(Clone, Copy)]
enum ProgramForm {
    Bare,
    Relative,
    Absolute,
}

struct FixtureRun {
    report: FixtureReport,
    action: ActionId,
    terminal: TestOccurrence,
}

struct PreparedFixtureCommand {
    _temporary: tempfile::TempDir,
    cwd: PathBuf,
    listener: TcpListener,
    admitted: AdmittedWorkflow,
}

struct TestArtifacts {
    _temporary: tempfile::TempDir,
    staging: ArtifactStaging,
}

fn test_artifacts(admitted: &AdmittedWorkflow) -> TestArtifacts {
    let temporary = tempfile::tempdir().unwrap();
    let staging = ArtifactStaging::create(admitted.execution(), temporary.path()).unwrap();
    TestArtifacts {
        _temporary: temporary,
        staging,
    }
}

struct PreparedGroupCommand {
    _temporary: tempfile::TempDir,
    listener: TcpListener,
    admitted: AdmittedWorkflow,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TestInstant(Duration);

impl Add<Duration> for TestInstant {
    type Output = Self;

    fn add(self, duration: Duration) -> Self::Output {
        Self(self.0 + duration)
    }
}

#[derive(Clone, Copy)]
struct TestClock;

impl CoordinatorClock for TestClock {
    type Instant = TestInstant;

    fn now(&mut self) -> Self::Instant {
        TestInstant(Duration::ZERO)
    }

    async fn wait_until(&self, _deadline: Self::Instant) {
        std::future::pending().await
    }
}

#[derive(Clone)]
struct ControlledClock {
    now: TestInstant,
    release: watch::Receiver<bool>,
    registrations: mpsc::UnboundedSender<TestInstant>,
    active_waiters: Arc<AtomicUsize>,
}

struct DeadlineControl {
    release: watch::Sender<bool>,
    registrations: mpsc::UnboundedReceiver<TestInstant>,
    active_waiters: Arc<AtomicUsize>,
}

struct DeadlineWaiterGuard(Arc<AtomicUsize>);

impl Drop for DeadlineWaiterGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ControlledClock {
    fn new(now: TestInstant) -> (Self, DeadlineControl) {
        let (release, released) = watch::channel(false);
        let (registrations, registered) = mpsc::unbounded_channel();
        let active_waiters = Arc::new(AtomicUsize::new(0));
        (
            Self {
                now,
                release: released,
                registrations,
                active_waiters: Arc::clone(&active_waiters),
            },
            DeadlineControl {
                release,
                registrations: registered,
                active_waiters,
            },
        )
    }
}

impl CoordinatorClock for ControlledClock {
    type Instant = TestInstant;

    fn now(&mut self) -> Self::Instant {
        self.now
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        self.active_waiters.fetch_add(1, Ordering::SeqCst);
        let _guard = DeadlineWaiterGuard(Arc::clone(&self.active_waiters));
        let _ = self.registrations.send(deadline);
        let mut release = self.release.clone();
        while !*release.borrow_and_update() {
            if release.changed().await.is_err() {
                return;
            }
        }
    }
}

impl DeadlineControl {
    async fn next_deadline(&mut self) -> TestInstant {
        self.registrations.recv().await.unwrap()
    }

    fn release(&self) {
        self.release.send(true).unwrap();
    }

    fn active_waiters(&self) -> usize {
        self.active_waiters.load(Ordering::SeqCst)
    }
}

type WorkflowCommit = CommittedReduction<StepFailureCause, CapturedArtifact>;

struct RecordingCommitPort {
    commits: mpsc::UnboundedSender<WorkflowCommit>,
}

impl CommitPort<WorkflowCommit> for RecordingCommitPort {
    fn commit(&mut self, commit: WorkflowCommit) -> impl Future<Output = ()> {
        let _ = self.commits.send(commit);
        std::future::ready(())
    }
}

#[tokio::test]
async fn command_uses_contained_cwd_literal_argv_and_isolates_parent_environment() {
    with_watchdog(async {
        for form in [
            ProgramForm::Bare,
            ProgramForm::Relative,
            ProgramForm::Absolute,
        ] {
            let run = run_fixture_command(form, 0).await;
            assert_eq!(run.report.arguments, fixture_arguments());
            let expected_environment =
                ["EXPLICIT_VALUE", "PATH", FIXTURE_EXIT_CODE, FIXTURE_SOCKET]
                    .into_iter()
                    // Foundation adds this process-local setting after macOS starts the
                    // otherwise environment-cleared child.
                    .chain(cfg!(target_os = "macos").then_some("__CF_USER_TEXT_ENCODING"))
                    .collect::<Vec<_>>();
            assert_eq!(
                run.report.environment.keys().cloned().collect::<Vec<_>>(),
                expected_environment
            );
            assert_eq!(
                run.report.environment.get("EXPLICIT_VALUE"),
                Some(&"from-admission".to_owned())
            );
            assert!(run.report.process_group_leader);
            assert_eq!(
                run.terminal,
                Occurrence::StepExecutionCompleted {
                    step: "task".to_owned(),
                    action: run.action,
                    provisional: (),
                }
            );
        }
    })
    .await;
}

#[tokio::test]
async fn workflow_execution_dispatches_start_actions_to_the_step_runtime() {
    with_watchdog(async {
        let PreparedFixtureCommand {
            _temporary,
            cwd,
            listener,
            admitted,
        } = prepare_fixture_command(ProgramForm::Absolute, 0).await;
        let (commit_sender, mut commit_receiver) = mpsc::unbounded_channel();
        let artifacts = test_artifacts(&admitted);
        let execution = execute_workflow(
            admitted,
            &artifacts.staging,
            TestClock,
            RecordingCommitPort {
                commits: commit_sender,
            },
        );
        let command = async {
            let (control, report) = accept_report(&listener).await;
            assert_eq!(report.current_directory, fs::canonicalize(cwd).unwrap());
            release(control).await;
        };

        let (result, ()) = tokio::join!(execution, command);
        let result = result.unwrap();
        assert_eq!(result.state.workflow, WorkflowState::Succeeded);
        assert_eq!(result.last_occurrence_ordinal.get(), 3);

        let mut commits = Vec::new();
        while let Ok(commit) = commit_receiver.try_recv() {
            commits.push(commit);
        }
        assert_eq!(commits.len(), 3);
        let started_action = commits[0].state.steps["task"].current_action.unwrap();
        assert_eq!(commits[0].state.steps["task"].state, StepState::Starting);
        assert_eq!(
            commits[1].state.steps["task"],
            crate::execution::workflow::runtime::StepRuntimeState {
                state: StepState::Running,
                current_action: Some(started_action),
            }
        );
        assert_eq!(
            commits[2].state.steps["task"].state,
            StepState::Succeeded {
                outputs: BTreeMap::new(),
            }
        );
    })
    .await;
}

#[tokio::test]
async fn cancellation_winning_before_capture_completion_discards_staged_artifacts() {
    let PreparedFixtureCommand {
        _temporary,
        cwd,
        admitted,
        ..
    } = prepare_fixture_command_with_output(ProgramForm::Absolute, 0).await;
    fs::write(cwd.join("report.txt"), b"uncommitted report").unwrap();
    let artifacts = test_artifacts(&admitted);
    let initialized =
        runtime::initialize::<(), StepFailureCause, CapturedArtifact, TestInstant>(&admitted, None);
    let start = initialized
        .actions
        .iter()
        .find(|requested| matches!(&requested.action, Action::StartStep { step } if step == "task"))
        .unwrap();
    let running = runtime::reduce::<(), StepFailureCause, CapturedArtifact, TestInstant>(
        &initialized.state,
        Occurrence::StepStarted {
            step: "task".to_owned(),
            action: start.id,
        },
    );
    let capture_requested = runtime::reduce::<(), StepFailureCause, CapturedArtifact, TestInstant>(
        &running.state,
        Occurrence::StepExecutionCompleted {
            step: "task".to_owned(),
            action: start.id,
            provisional: (),
        },
    );
    let capture = capture_requested
        .actions
        .iter()
        .find(|requested| {
            matches!(&requested.action, Action::CaptureOutputs { step, .. } if step == "task")
        })
        .unwrap();
    let cancelled = runtime::reduce::<(), StepFailureCause, CapturedArtifact, TestInstant>(
        &capture_requested.state,
        Occurrence::CancellationRequested {
            reason: CancellationReason::UserRequest,
            deadline: TestInstant(Duration::from_secs(1)),
        },
    );
    assert_eq!(
        cancelled.state.steps["task"].state,
        StepState::Cancelling {
            reason: CancellationReason::UserRequest,
        }
    );
    let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
    let step_runtime = StepRuntime::new(admitted, artifacts.staging.clone(), sender, TestClock);

    step_runtime
        .capture_outputs("task".to_owned(), capture.id)
        .await
        .unwrap();
    assert_eq!(artifacts.staging.staged_artifact_count(), 1);
    let completion = receiver.recv().await.unwrap().into_runtime::<TestInstant>();
    let stale = runtime::reduce(&cancelled.state, completion);

    assert!(stale.events.is_empty());
    assert_eq!(stale.state, cancelled.state);
    assert_eq!(artifacts.staging.staged_artifact_count(), 0);
}

#[tokio::test]
async fn workflow_execution_rejects_staging_bound_to_another_execution() {
    let temporary = tempfile::tempdir().unwrap();
    let admitted_root = temporary.path().join("admitted-execution");
    let other_root = temporary.path().join("other-execution");
    fs::create_dir(&admitted_root).unwrap();
    fs::create_dir(&other_root).unwrap();
    fs::write(other_root.join("report.txt"), b"wrong execution").unwrap();
    let mut source = workflow_source(&[("task", None, &["/bin/true"])]);
    source.push_str(
        "    outputs:\n      report:\n        kind: file\n        path: report.txt\n        mediaType: text/plain\n",
    );
    let admitted = admit_fixture(
        temporary.path(),
        &admitted_root,
        &source,
        EnvironmentSnapshot::default(),
        1,
    );
    let other_admitted = admit_fixture(
        temporary.path(),
        &other_root,
        &source,
        EnvironmentSnapshot::default(),
        1,
    );
    let artifacts = test_artifacts(&other_admitted);
    let (commit_sender, mut commits) = mpsc::unbounded_channel();

    let result = execute_workflow(
        admitted,
        &artifacts.staging,
        TestClock,
        RecordingCommitPort {
            commits: commit_sender,
        },
    )
    .await;

    assert_eq!(result, Err(CoordinationError::ArtifactStagingMismatch));
    assert!(commits.try_recv().is_err());
}

#[tokio::test]
async fn workflow_execution_captures_declared_outputs_before_success() {
    with_watchdog(async {
        let PreparedFixtureCommand {
            _temporary,
            cwd,
            listener,
            admitted,
        } = prepare_fixture_command_with_output(ProgramForm::Absolute, 0).await;
        fs::write(cwd.join("report.txt"), b"committed report").unwrap();
        let artifacts = test_artifacts(&admitted);
        let (commit_sender, _commits) = mpsc::unbounded_channel();
        let execution = execute_workflow(
            admitted,
            &artifacts.staging,
            TestClock,
            RecordingCommitPort {
                commits: commit_sender,
            },
        );
        let command = async {
            let (control, _) = accept_report(&listener).await;
            release(control).await;
        };

        let (result, ()) = tokio::join!(execution, command);
        let result = result.unwrap();

        assert_eq!(result.state.workflow, WorkflowState::Succeeded);
        assert_eq!(result.last_occurrence_ordinal.get(), 4);
        let StepState::Succeeded { outputs } = &result.state.steps["task"].state else {
            panic!("output-producing command did not succeed");
        };
        let captured = &outputs["report"];
        assert_eq!(captured.output_identity(), "report");
        assert_eq!(captured.media_type(), "text/plain");
        assert_eq!(captured.size(), 16);
        let mut bytes = Vec::new();
        artifacts
            .staging
            .copy_to(captured.handle(), &mut bytes)
            .unwrap();
        assert_eq!(bytes, b"committed report");
    })
    .await;
}

#[tokio::test]
async fn bare_program_search_skips_a_candidate_inaccessible_to_the_caller() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let blocked_directory = execution_root.join("blocked-bin");
        let usable_directory = execution_root.join("usable-bin");
        fs::create_dir_all(&blocked_directory).unwrap();
        fs::create_dir(&usable_directory).unwrap();

        let current_executable = std::env::current_exe().unwrap();
        let blocked_executable = blocked_directory.join("workflow-command-fixture");
        fs::write(&blocked_executable, b"not executable by the test caller").unwrap();
        fs::set_permissions(&blocked_executable, fs::Permissions::from_mode(0o001)).unwrap();
        let usable_executable = usable_directory.join("workflow-command-fixture");
        install_fixture(&current_executable, &usable_executable);

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let control_address = listener.local_addr().unwrap().to_string();
        let search_path = std::env::join_paths([&blocked_directory, &usable_directory]).unwrap();
        let environment = EnvironmentSnapshot::new([
            (
                OsString::from(FIXTURE_SOCKET),
                OsString::from(&control_address),
            ),
            (OsString::from(FIXTURE_EXIT_CODE), OsString::from("0")),
            (OsString::from("PATH"), search_path),
        ]);
        let arguments = fixture_arguments();
        let argv = std::iter::once("workflow-command-fixture")
            .chain(arguments.iter().map(String::as_str))
            .collect::<Vec<_>>();
        let source = workflow_source(&[("task", None, argv.as_slice())]);
        let admitted = admit_fixture(temporary.path(), &execution_root, &source, environment, 1);
        let action = start_actions(&admitted)["task"];
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
        let artifacts = test_artifacts(&admitted);
        let runtime = StepRuntime::new(admitted, artifacts.staging.clone(), sender, TestClock);
        let execution =
            tokio::spawn(async move { runtime.execute_step("task".to_owned(), action).await });

        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action,
            }
        );
        let (control, _) = accept_report(&listener).await;
        release(control).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepExecutionCompleted {
                step: "task".to_owned(),
                action,
                provisional: (),
            }
        );
        assert_eq!(execution.await.unwrap(), Ok(()));
    })
    .await;
}

#[tokio::test]
async fn cwd_and_launch_failures_are_typed_start_occurrences() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let outside = temporary.path().join("outside");
        fs::create_dir(&execution_root).unwrap();
        fs::create_dir(&outside).unwrap();
        symlink(&outside, execution_root.join("escape")).unwrap();

        for (cwd, expected) in [
            (
                "missing",
                StepStartFailure::WorkingDirectory(WorkingDirectoryFailure::Unavailable),
            ),
            (
                "escape",
                StepStartFailure::WorkingDirectory(WorkingDirectoryFailure::EscapesExecutionRoot),
            ),
        ] {
            let source = workflow_source(&[("task", Some(cwd), &["./not-started"])]);
            let admitted = admit_fixture(
                temporary.path(),
                &execution_root,
                &source,
                EnvironmentSnapshot::default(),
                1,
            );
            assert_start_failure(admitted, "task", expected).await;
        }

        let source = workflow_source(&[("task", None, &["./missing-executable"])]);
        let admitted = admit_fixture(
            temporary.path(),
            &execution_root,
            &source,
            EnvironmentSnapshot::default(),
            1,
        );
        assert_start_failure(
            admitted,
            "task",
            StepStartFailure::CommandLaunch(CommandLaunchFailure::NotFound),
        )
        .await;
    })
    .await;
}

#[tokio::test]
async fn nonzero_exit_is_a_typed_execution_occurrence_with_the_start_action() {
    with_watchdog(async {
        let run = run_fixture_command(ProgramForm::Absolute, 23).await;
        assert_eq!(
            run.terminal,
            Occurrence::StepExecutionFailed {
                step: "task".to_owned(),
                action: run.action,
                cause: StepFailureCause::Execution(StepExecutionFailure::Command(
                    CommandExecutionFailure::UnsuccessfulExit { code: Some(23) },
                )),
            }
        );
    })
    .await;
}

#[tokio::test]
async fn concurrent_commands_receive_distinct_process_groups() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let control_address = listener.local_addr().unwrap().to_string();
        let executable = std::env::current_exe().unwrap();
        let executable = executable.to_str().unwrap();
        let fixture_arguments = fixture_arguments();
        let argv = std::iter::once(executable)
            .chain(fixture_arguments.iter().map(String::as_str))
            .collect::<Vec<_>>();
        let source = workflow_source(&[
            ("alpha", None, argv.as_slice()),
            ("beta", None, argv.as_slice()),
        ]);
        let environment = fixture_environment(&control_address, 0, execution_root.as_path());
        let admitted = admit_fixture(temporary.path(), &execution_root, &source, environment, 2);
        let actions = start_actions(&admitted);
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let runtime = StepRuntime::new(admitted, artifacts.staging.clone(), sender, TestClock);
        let alpha_runtime = runtime.clone();
        let alpha_action = actions["alpha"];
        let alpha = tokio::spawn(async move {
            alpha_runtime
                .execute_step("alpha".to_owned(), alpha_action)
                .await
        });
        let beta_action = actions["beta"];
        let beta =
            tokio::spawn(async move { runtime.execute_step("beta".to_owned(), beta_action).await });

        let mut started = BTreeMap::new();
        for _ in 0..2 {
            let Occurrence::StepStarted { step, action } = next_occurrence(&mut receiver).await
            else {
                panic!("command did not report its successful launch");
            };
            started.insert(step, action);
        }
        assert_eq!(started, actions);

        let (first_control, first_report) = accept_report(&listener).await;
        let (second_control, second_report) = accept_report(&listener).await;
        assert!(first_report.process_group_leader);
        assert!(second_report.process_group_leader);
        assert_ne!(first_report.process_group, second_report.process_group);
        release(first_control).await;
        release(second_control).await;

        let mut completed = BTreeMap::new();
        for _ in 0..2 {
            let Occurrence::StepExecutionCompleted {
                step,
                action,
                provisional: (),
            } = next_occurrence(&mut receiver).await
            else {
                panic!("command did not report zero-exit completion");
            };
            completed.insert(step, action);
        }
        assert_eq!(completed, actions);
        let (alpha_result, beta_result) = tokio::join!(alpha, beta);
        assert_eq!(alpha_result.unwrap(), Ok(()));
        assert_eq!(beta_result.unwrap(), Ok(()));
    })
    .await;
}

#[tokio::test]
async fn cancellation_before_launch_revokes_the_action_and_duplicate_delivery_is_inert() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_INTERRUPTIBLE).await;
        let deadline = TestInstant(Duration::from_secs(19));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let (clock, control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::new(admitted, artifacts.staging.clone(), sender, clock);

        runtime.release(start.clone()).await;
        runtime.release(cancel.clone()).await;
        runtime.release(cancel).await;
        runtime.release(start).await;

        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );
        assert_eq!(runtime.active_work_count(), 0);
        assert_eq!(control.active_waiters(), 0);
        assert!(receiver.try_recv().is_none());
        let listener = listener.into_std().unwrap();
        assert!(
            matches!(listener.accept(), Err(failure) if failure.kind() == io::ErrorKind::WouldBlock)
        );
    })
    .await;
}

#[tokio::test]
async fn running_cancellation_interrupts_the_child_and_descendant_and_reaps_once() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_INTERRUPTIBLE).await;
        let deadline = TestInstant(Duration::from_secs(23));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let start_action = start.id;
        let (clock, mut control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::new(admitted, artifacts.staging.clone(), sender, clock);

        runtime.release(start).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start_action,
            }
        );
        let mut processes = accept_group(&listener).await;

        runtime.release(cancel.clone()).await;
        runtime.release(cancel).await;
        assert_group_interrupted(&processes).await;
        assert_eq!(control.next_deadline().await, deadline);
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );

        assert_group_closed(&mut processes).await;
        assert_eq!(runtime.active_work_count(), 0);
        assert_eq!(control.active_waiters(), 0);
        assert!(receiver.try_recv().is_none());
    })
    .await;
}

#[tokio::test]
async fn admitted_deadline_forces_the_stubborn_process_group_without_wall_clock_waiting() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_STUBBORN).await;
        let deadline = TestInstant(Duration::from_secs(29));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let start_action = start.id;
        let (clock, mut control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::new(admitted, artifacts.staging.clone(), sender, clock);

        runtime.release(start).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start_action,
            }
        );
        let mut processes = accept_group(&listener).await;

        runtime.release(cancel).await;
        assert_group_interrupted(&processes).await;
        assert_eq!(control.next_deadline().await, deadline);
        assert_eq!(control.active_waiters(), 1);
        control.release();
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );

        assert_group_closed(&mut processes).await;
        assert_eq!(runtime.active_work_count(), 0);
        assert_eq!(control.active_waiters(), 0);
        assert!(receiver.try_recv().is_none());
    })
    .await;
}

#[tokio::test]
async fn natural_exit_before_cancel_delivery_has_no_late_lifecycle_or_cleanup_work() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_INTERRUPTIBLE).await;
        let deadline = TestInstant(Duration::from_secs(31));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let start_action = start.id;
        let (clock, mut control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::new(admitted, artifacts.staging.clone(), sender, clock);

        runtime.release(start).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start_action,
            }
        );
        let mut processes = accept_group(&listener).await;
        release_group(&processes).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepExecutionCompleted {
                step: "task".to_owned(),
                action: start_action,
                provisional: (),
            }
        );

        runtime.release(cancel.clone()).await;
        runtime.release(cancel).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );

        assert_group_closed(&mut processes).await;
        assert_eq!(runtime.active_work_count(), 0);
        assert_eq!(control.active_waiters(), 0);
        assert!(control.registrations.try_recv().is_err());
        assert!(receiver.try_recv().is_none());
    })
    .await;
}

#[tokio::test]
async fn cancellation_after_the_owned_child_exits_still_terminates_its_descendant() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_PARENT_EXITS).await;
        let deadline = TestInstant(Duration::from_secs(37));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let start_action = start.id;
        let (clock, control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::new(admitted, artifacts.staging.clone(), sender, clock);

        runtime.release(start).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start_action,
            }
        );
        let mut processes = accept_group(&listener).await;
        release_control(&processes[FIXTURE_PARENT].0).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepExecutionCompleted {
                step: "task".to_owned(),
                action: start_action,
                provisional: (),
            }
        );

        runtime.release(cancel).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );

        let descendant_was_alive = probe_fixture_alive(&processes[FIXTURE_DESCENDANT].0).await;
        if descendant_was_alive {
            release_control(&processes[FIXTURE_DESCENDANT].0).await;
        }
        assert_group_closed(&mut processes).await;

        assert!(
            !descendant_was_alive,
            "step quiesced while the command descendant could still perform work"
        );
        assert_eq!(runtime.active_work_count(), 0);
        assert_eq!(control.active_waiters(), 0);
        assert!(receiver.try_recv().is_none());
    })
    .await;
}

async fn run_fixture_command(form: ProgramForm, exit_code: i32) -> FixtureRun {
    let PreparedFixtureCommand {
        _temporary,
        cwd,
        listener,
        admitted,
    } = prepare_fixture_command(form, exit_code).await;
    let action = start_actions(&admitted)["task"];
    let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
    let artifacts = test_artifacts(&admitted);
    let runtime = StepRuntime::new(admitted, artifacts.staging.clone(), sender, TestClock);
    let execution =
        tokio::spawn(async move { runtime.execute_step("task".to_owned(), action).await });

    assert_eq!(
        next_occurrence(&mut receiver).await,
        Occurrence::StepStarted {
            step: "task".to_owned(),
            action,
        }
    );
    let (control, report) = accept_report(&listener).await;
    assert_eq!(report.current_directory, fs::canonicalize(cwd).unwrap());
    release(control).await;
    let terminal = next_occurrence(&mut receiver).await;
    assert_eq!(execution.await.unwrap(), Ok(()));
    FixtureRun {
        report,
        action,
        terminal,
    }
}

async fn prepare_fixture_command(form: ProgramForm, exit_code: i32) -> PreparedFixtureCommand {
    prepare_fixture_command_with_declared_output(form, exit_code, false).await
}

async fn prepare_fixture_command_with_output(
    form: ProgramForm,
    exit_code: i32,
) -> PreparedFixtureCommand {
    prepare_fixture_command_with_declared_output(form, exit_code, true).await
}

async fn prepare_fixture_command_with_declared_output(
    form: ProgramForm,
    exit_code: i32,
    declare_output: bool,
) -> PreparedFixtureCommand {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    let cwd = execution_root.join("work");
    fs::create_dir_all(&cwd).unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let control_address = listener.local_addr().unwrap().to_string();
    let current_executable = std::env::current_exe().unwrap();
    let (program, path_directory) = match form {
        ProgramForm::Bare => {
            let path_directory = execution_root.join("bin");
            fs::create_dir(&path_directory).unwrap();
            let installed = path_directory.join("workflow-command-fixture");
            install_fixture(&current_executable, &installed);
            ("workflow-command-fixture".to_owned(), path_directory)
        }
        ProgramForm::Relative => {
            let installed = cwd.join("workflow-command-fixture");
            install_fixture(&current_executable, &installed);
            (
                "./workflow-command-fixture".to_owned(),
                execution_root.clone(),
            )
        }
        ProgramForm::Absolute => (
            current_executable.to_str().unwrap().to_owned(),
            execution_root.clone(),
        ),
    };
    let arguments = fixture_arguments();
    let mut argv = vec![program];
    argv.extend(arguments);
    let argv_references = argv.iter().map(String::as_str).collect::<Vec<_>>();
    let mut source = workflow_source(&[("task", Some("work"), &argv_references)]);
    if declare_output {
        source.push_str(
            "    outputs:\n      report:\n        kind: file\n        path: work/report.txt\n        mediaType: text/plain\n",
        );
    }
    let environment = fixture_environment(&control_address, exit_code, &path_directory);
    let admitted = admit_fixture(temporary.path(), &execution_root, &source, environment, 1);
    PreparedFixtureCommand {
        _temporary: temporary,
        cwd,
        listener,
        admitted,
    }
}

async fn prepare_group_command(mode: &str) -> PreparedGroupCommand {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&execution_root).unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let control_address = listener.local_addr().unwrap().to_string();
    let executable = std::env::current_exe().unwrap();
    let executable = executable.to_str().unwrap();
    let arguments = fixture_arguments();
    let argv = std::iter::once(executable)
        .chain(arguments.iter().map(String::as_str))
        .collect::<Vec<_>>();
    let source = workflow_source(&[("task", None, argv.as_slice())]);
    let environment = EnvironmentSnapshot::new([
        (
            OsString::from(FIXTURE_SOCKET),
            OsString::from(control_address),
        ),
        (OsString::from(FIXTURE_EXIT_CODE), OsString::from("0")),
        (OsString::from(FIXTURE_MODE), OsString::from(mode)),
        (OsString::from(FIXTURE_ROLE), OsString::from(FIXTURE_PARENT)),
    ]);
    let admitted = admit_fixture(temporary.path(), &execution_root, &source, environment, 1);
    PreparedGroupCommand {
        _temporary: temporary,
        listener,
        admitted,
    }
}

fn running_cancellation_actions(
    admitted: &AdmittedWorkflow,
    deadline: TestInstant,
) -> (TestRequestedAction, TestRequestedAction) {
    let initialized =
        runtime::initialize::<(), StepFailureCause, CapturedArtifact, TestInstant>(admitted, None);
    let start = initialized
        .actions
        .iter()
        .find(|requested| matches!(requested.action, Action::StartStep { .. }))
        .unwrap()
        .clone();
    let started = runtime::reduce::<(), StepFailureCause, CapturedArtifact, TestInstant>(
        &initialized.state,
        Occurrence::StepStarted {
            step: "task".to_owned(),
            action: start.id,
        },
    );
    let cancelled = runtime::reduce(
        &started.state,
        Occurrence::CancellationRequested {
            reason: CancellationReason::UserRequest,
            deadline,
        },
    );
    let cancel = cancelled
        .actions
        .into_iter()
        .find(|requested| matches!(requested.action, Action::CancelStep { .. }))
        .unwrap();
    (start, cancel)
}

async fn accept_group(listener: &TcpListener) -> BTreeMap<String, (TcpStream, FixtureReport)> {
    let mut processes = BTreeMap::new();
    for _ in 0..2 {
        let (stream, report) = accept_report(listener).await;
        assert!(
            processes
                .insert(report.role.clone(), (stream, report))
                .is_none()
        );
    }

    let parent = &processes[FIXTURE_PARENT].1;
    let descendant = &processes[FIXTURE_DESCENDANT].1;
    assert!(parent.process_group_leader);
    assert!(!descendant.process_group_leader);
    assert_eq!(parent.process_group, descendant.process_group);
    processes
}

async fn assert_group_interrupted(processes: &BTreeMap<String, (TcpStream, FixtureReport)>) {
    let parent = next_fixture_event(&processes[FIXTURE_PARENT].0);
    let descendant = next_fixture_event(&processes[FIXTURE_DESCENDANT].0);
    let (parent, descendant) = tokio::join!(parent, descendant);
    assert_eq!(parent.event, "interrupted");
    assert_eq!(descendant.event, "interrupted");
}

async fn release_group(processes: &BTreeMap<String, (TcpStream, FixtureReport)>) {
    release_control(&processes[FIXTURE_DESCENDANT].0).await;
    release_control(&processes[FIXTURE_PARENT].0).await;
}

async fn assert_group_closed(processes: &mut BTreeMap<String, (TcpStream, FixtureReport)>) {
    for (stream, _) in processes.values_mut() {
        await_fixture_eof(stream).await;
    }
}

fn fixture_arguments() -> Vec<String> {
    [
        "--ignored",
        "--exact",
        FIXTURE_TEST_NAME,
        "--nocapture",
        "--skip",
        LITERAL_ARGUMENT,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn fixture_environment(
    control_address: &str,
    exit_code: i32,
    path_directory: &Path,
) -> EnvironmentSnapshot {
    EnvironmentSnapshot::new([
        (
            OsString::from(FIXTURE_SOCKET),
            OsString::from(control_address),
        ),
        (
            OsString::from(FIXTURE_EXIT_CODE),
            OsString::from(exit_code.to_string()),
        ),
        (
            OsString::from("PATH"),
            path_directory.as_os_str().to_owned(),
        ),
        (
            OsString::from("EXPLICIT_VALUE"),
            OsString::from("from-admission"),
        ),
        (
            OsString::from("SCHERZO_INHERITED"),
            OsString::from("must-not-reach-command"),
        ),
    ])
}

fn install_fixture(source: &Path, destination: &Path) {
    symlink(source, destination).unwrap();
}

fn workflow_source(steps: &[(&str, Option<&str>, &[&str])]) -> String {
    let mut source = String::from("schemaVersion: 1\nsteps:\n");
    for (step, cwd, argv) in steps {
        source.push_str(&format!("  {step}:\n    kind: cmd\n"));
        if let Some(cwd) = cwd {
            source.push_str(&format!(
                "    cwd: {}\n",
                serde_json::to_string(cwd).unwrap()
            ));
        }
        source.push_str(&format!(
            "    command:\n      argv: {}\n",
            serde_json::to_string(argv).unwrap()
        ));
    }
    source
}

fn admit_fixture(
    temporary_root: &Path,
    execution_root: &Path,
    source: &str,
    environment: EnvironmentSnapshot,
    maximum_parallel_steps: usize,
) -> AdmittedWorkflow {
    let source_root = temporary_root.join(format!(
        "source-{}",
        temporary_root.read_dir().unwrap().count()
    ));
    fs::create_dir(&source_root).unwrap();
    fs::write(source_root.join("workflow.yaml"), source).unwrap();
    admit_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedImports::default(),
        ExecutionContext::new(
            execution_root.to_owned(),
            ExecutionRootLifecycle::EngineOwnedEphemeral,
            maximum_parallel_steps,
            1024 * 1024,
            environment,
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap()
}

fn start_actions(admitted: &AdmittedWorkflow) -> BTreeMap<String, ActionId> {
    runtime::initialize::<(), StepFailureCause, CapturedArtifact, ()>(admitted, None)
        .actions
        .into_iter()
        .filter_map(|requested| match requested.action {
            Action::StartStep { step } => Some((step, requested.id)),
            Action::CaptureOutputs { .. }
            | Action::CancelStep { .. }
            | Action::FinishRun { .. } => None,
        })
        .collect()
}

async fn assert_start_failure(admitted: AdmittedWorkflow, step: &str, expected: StepStartFailure) {
    let action = start_actions(&admitted)[step];
    let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
    let artifacts = test_artifacts(&admitted);
    let runtime = StepRuntime::new(admitted, artifacts.staging.clone(), sender, TestClock);
    assert_eq!(runtime.execute_step(step.to_owned(), action).await, Ok(()));
    assert_eq!(
        next_occurrence(&mut receiver).await,
        Occurrence::StepStartFailed {
            step: step.to_owned(),
            action,
            cause: StepFailureCause::Start(expected),
        }
    );
}

async fn next_occurrence(receiver: &mut TestReceiver) -> TestOccurrence {
    receiver.recv().await.unwrap().into_runtime()
}

async fn accept_report(listener: &TcpListener) -> (TcpStream, FixtureReport) {
    let (stream, _) = listener.accept().await.unwrap();
    let report = read_fixture_line(&stream).await;
    (stream, serde_json::from_slice(&report).unwrap())
}

async fn next_fixture_event(stream: &TcpStream) -> FixtureEvent {
    serde_json::from_slice(&read_fixture_line(stream).await).unwrap()
}

async fn read_fixture_line(stream: &TcpStream) -> Vec<u8> {
    try_read_fixture_line(stream)
        .await
        .unwrap_or_else(|| panic!("fixture closed before its control message"))
}

async fn try_read_fixture_line(stream: &TcpStream) -> Option<Vec<u8>> {
    let mut line = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        stream.readable().await.unwrap();
        match stream.try_read(&mut buffer) {
            Ok(0) => return None,
            Ok(read) => {
                line.extend_from_slice(&buffer[..read]);
                if line.last() == Some(&b'\n') {
                    line.pop();
                    return Some(line);
                }
                assert!(line.len() <= 16 * 1024);
            }
            Err(failure) if failure.kind() == io::ErrorKind::WouldBlock => {}
            Err(failure)
                if matches!(
                    failure.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                ) =>
            {
                return None;
            }
            Err(failure) => panic!("fixture control read failed: {failure:?}"),
        }
    }
}

async fn probe_fixture_alive(stream: &TcpStream) -> bool {
    if !send_fixture_control(stream, 2).await {
        return false;
    }
    let Some(event) = try_read_fixture_line(stream).await else {
        return false;
    };
    assert_eq!(
        serde_json::from_slice::<FixtureEvent>(&event)
            .unwrap()
            .event,
        "alive"
    );
    true
}

async fn await_fixture_eof(stream: &TcpStream) {
    let mut buffer = [0_u8; 256];
    loop {
        stream.readable().await.unwrap();
        match stream.try_read(&mut buffer) {
            Ok(0) => return,
            Ok(_) => panic!("fixture sent an unexpected late control message"),
            Err(failure) if failure.kind() == io::ErrorKind::WouldBlock => {}
            Err(failure) => panic!("fixture closure read failed: {failure:?}"),
        }
    }
}

async fn release(stream: TcpStream) {
    release_control(&stream).await;
}

async fn release_control(stream: &TcpStream) {
    assert!(send_fixture_control(stream, 1).await);
}

async fn send_fixture_control(stream: &TcpStream, control: u8) -> bool {
    loop {
        stream.writable().await.unwrap();
        match stream.try_write(&[control]) {
            Ok(1) => return true,
            Ok(_) => {}
            Err(failure) if failure.kind() == io::ErrorKind::WouldBlock => {}
            Err(failure)
                if matches!(
                    failure.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                ) =>
            {
                return false;
            }
            Err(failure) => panic!("fixture control write failed: {failure:?}"),
        }
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is allowed only to keep a broken process handshake from hanging"
)]
async fn with_watchdog<Output>(future: impl Future<Output = Output>) -> Output {
    match tokio::time::timeout(TEST_WATCHDOG, future).await {
        Ok(output) => output,
        Err(_) => panic!("workflow command fixture watchdog expired"),
    }
}
