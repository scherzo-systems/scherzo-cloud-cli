use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::future::Future;
use std::io;
use std::ops::Add;
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::sync::{mpsc, watch};

use super::*;
use crate::execution::pi::ValidatedPiInstallation;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationReason, CancellationSource, CaptureLimits, EnvironmentSnapshot,
    ExecutionContext, ExecutionPolicyLimits, ExecutionRootLifecycle, InputLimits,
    ResolvedAttachment, ResolvedImports, admit_workflow,
};
use crate::execution::workflow::agent::scripted::{
    ScriptedAgentDispatcher, ScriptedAgentValue, scripted_agent_dispatcher,
};
use crate::execution::workflow::agent::{
    AgentFailureCause, AgentLifecycleMilestone, AgentObservation, AgentObservationEnvelope,
    WorkflowRunId,
};
use crate::execution::workflow::agent_diagnostics::AgentDiagnosticSessionStore;
use crate::execution::workflow::agent_input::AgentInputStaging;
use crate::execution::workflow::artifact::{ArtifactReadFailure, ArtifactStaging};
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::input::InputStaging;
use crate::execution::workflow::observation::{
    CommandOutputSource, ExecutionObservation, ExecutionObserver, NoopExecutionObserver,
    TransitionObservation,
};
use crate::execution::workflow::resolution;
use crate::execution::workflow::runtime::{
    ExportValue, FailurePhase, NotRunReason, StepState, StepStateKind, TransitionEvent,
};
use crate::execution::workflow::step_runtime::{
    AgentExecution, CommandExecutionFailure, StepExecutionFailure, StepFailureCause,
    StepStartFailure,
};

const FIXTURE_TEST_NAME: &str = "execution::workflow::step_runtime::tests::command_fixture_process";
const STDIN_FIXTURE_TEST_NAME: &str =
    "execution::workflow::execution::tests::command_stdin_fixture_process";
const FIXTURE_ARGUMENT: &str = "literal * $HOME; [not-a-glob]";
const TEST_WATCHDOG: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TestInstant(Duration);

impl Add<Duration> for TestInstant {
    type Output = Self;

    fn add(self, duration: Duration) -> Self::Output {
        Self(self.0 + duration)
    }
}

type RecordedObservations = Arc<Mutex<Vec<ExecutionObservation<TestInstant>>>>;
type ObservationReceiver = mpsc::UnboundedReceiver<ExecutionObservation<TestInstant>>;

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

async fn execute_workflow<Clock, Observer, Dispatcher>(
    admitted: AdmittedWorkflow,
    artifacts: &ArtifactStaging,
    inputs: &InputStaging,
    diagnostics: &StepDiagnosticLog,
    agents: AgentExecution<Dispatcher>,
    clock: Clock,
    observer: Observer,
) -> Result<WorkflowExecutionResult, CoordinationError>
where
    Clock: CoordinatorClock,
    Clock::Instant: Sync,
    Observer: ExecutionObserver<Clock::Instant>,
    Dispatcher: WorkflowAgentDispatcher<Clock::Instant, Observer>,
{
    super::execute_workflow(
        admitted,
        artifacts,
        inputs,
        diagnostics,
        agents,
        clock,
        NoopCommitPort,
        observer,
        crate::execution::workflow::process_group::ProcessGuardRegistry::default(),
    )
    .await
}

#[derive(Clone)]
struct RecordingObserver {
    entries: RecordedObservations,
    notifications: mpsc::UnboundedSender<ExecutionObservation<TestInstant>>,
    terminal_gate: Option<TerminalGate>,
    step_success_gate: Option<TerminalGate>,
}

#[derive(Clone)]
struct TerminalGate {
    reached: mpsc::UnboundedSender<()>,
    release: watch::Receiver<bool>,
}

impl RecordingObserver {
    fn new() -> (Self, RecordedObservations, ObservationReceiver) {
        let entries = Arc::new(Mutex::new(Vec::new()));
        let (notifications, observed) = mpsc::unbounded_channel();
        (
            Self {
                entries: Arc::clone(&entries),
                notifications,
                terminal_gate: None,
                step_success_gate: None,
            },
            entries,
            observed,
        )
    }

    fn with_terminal_gate() -> (
        Self,
        RecordedObservations,
        ObservationReceiver,
        mpsc::UnboundedReceiver<()>,
        watch::Sender<bool>,
    ) {
        let (mut observer, entries, observed) = Self::new();
        let (reached, terminal_reached) = mpsc::unbounded_channel();
        let (release, released) = watch::channel(false);
        observer.terminal_gate = Some(TerminalGate {
            reached,
            release: released,
        });
        (observer, entries, observed, terminal_reached, release)
    }

    fn with_step_success_gate() -> (
        Self,
        RecordedObservations,
        ObservationReceiver,
        mpsc::UnboundedReceiver<()>,
        watch::Sender<bool>,
    ) {
        let (mut observer, entries, observed) = Self::new();
        let (reached, success_reached) = mpsc::unbounded_channel();
        let (release, released) = watch::channel(false);
        observer.step_success_gate = Some(TerminalGate {
            reached,
            release: released,
        });
        (observer, entries, observed, success_reached, release)
    }
}

impl ExecutionObserver<TestInstant> for RecordingObserver {
    fn observe(
        &self,
        observation: ExecutionObservation<TestInstant>,
    ) -> impl Future<Output = ()> + Send {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(observation.clone());
        let _ = self.notifications.send(observation.clone());
        let gate = if is_terminal_cancellation(&observation) {
            self.terminal_gate.clone()
        } else if is_step_success(&observation) {
            self.step_success_gate.clone()
        } else {
            None
        };
        async move {
            let Some(mut gate) = gate else {
                return;
            };
            let _ = gate.reached.send(());
            while !*gate.release.borrow_and_update() {
                if gate.release.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}

fn is_terminal_cancellation(observation: &ExecutionObservation<TestInstant>) -> bool {
    matches!(
        observation,
        ExecutionObservation::Transition(TransitionObservation {
            event: TransitionEvent::Workflow {
                to: WorkflowState::Cancelled { .. },
                ..
            },
            ..
        })
    )
}

fn is_step_success(observation: &ExecutionObservation<TestInstant>) -> bool {
    matches!(
        observation,
        ExecutionObservation::Transition(TransitionObservation {
            event: TransitionEvent::Step {
                to: StepStateKind::Succeeded,
                ..
            },
            ..
        })
    )
}

struct ExecutionFixture {
    _temporary: tempfile::TempDir,
    execution_root: PathBuf,
    source_root: PathBuf,
    admitted: AdmittedWorkflow,
    artifacts: ArtifactStaging,
    inputs: InputStaging,
    agent_inputs: AgentInputStaging,
    diagnostic_sessions: AgentDiagnosticSessionStore,
}

fn execution_fixture(
    source: &str,
    imports: ResolvedImports,
    environment: EnvironmentSnapshot,
    cancellation: CancellationSource,
    parallelism: usize,
    log_bytes: u64,
) -> ExecutionFixture {
    execution_fixture_with_source_files(
        source,
        &[],
        imports,
        environment,
        cancellation,
        parallelism,
        log_bytes,
    )
}

fn execution_fixture_with_source_files(
    source: &str,
    source_files: &[(&str, &[u8])],
    imports: ResolvedImports,
    environment: EnvironmentSnapshot,
    cancellation: CancellationSource,
    parallelism: usize,
    log_bytes: u64,
) -> ExecutionFixture {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("execution");
    let staging_root = temporary.path().join("staging");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&execution_root).unwrap();
    fs::create_dir(&staging_root).unwrap();
    fs::write(source_root.join("workflow.yaml"), source).unwrap();
    for (path, bytes) in source_files {
        let path = source_root.join(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, bytes).unwrap();
    }
    let admitted = admit_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        imports,
        ExecutionContext::new(
            execution_root.clone(),
            ExecutionRootLifecycle::CallerOwnedRetained,
            ExecutionPolicyLimits::new(
                parallelism,
                CaptureLimits::new(16, 1024 * 1024, 8 * 1024 * 1024),
                InputLimits::new(16, 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024),
                log_bytes,
            ),
            environment,
            CancellationPolicy::new(cancellation, Duration::from_secs(1)),
        )
        .with_pi_installation(ValidatedPiInstallation::fixture("/validated/pi".into())),
    )
    .unwrap();
    let artifacts = ArtifactStaging::create(admitted.execution(), &staging_root).unwrap();
    let inputs = InputStaging::create(admitted.execution(), &staging_root).unwrap();
    let agent_inputs = AgentInputStaging::create(admitted.execution(), &staging_root).unwrap();
    let attempt_directory = temporary.path().join("run/attempts/000001");
    fs::create_dir_all(&attempt_directory).unwrap();
    let attempt_handle: OwnedFd = fs::File::open(&attempt_directory).unwrap().into();
    let diagnostic_sessions = AgentDiagnosticSessionStore::create(
        &attempt_handle,
        &attempt_directory,
        Arc::from("00000000-0000-4000-8000-000000000001"),
        1,
    )
    .unwrap();
    ExecutionFixture {
        _temporary: temporary,
        execution_root,
        source_root,
        admitted,
        artifacts,
        inputs,
        agent_inputs,
        diagnostic_sessions,
    }
}

#[tokio::test]
#[ignore = "launched with a live adapter input by the closed-stdin regression test"]
async fn command_stdin_fixture_process() {
    let path = env::var_os("PATH").unwrap_or_else(|| OsString::from("/bin:/usr/bin"));
    let fixture = execution_fixture(
        &format!(
            "schemaVersion: 1\nsteps:\n  eof:\n    kind: cmd\n    command:\n      argv: {}\n",
            serde_json::to_string(&["sh", "-c", "if IFS= read -r unexpected; then exit 91; fi",])
                .unwrap(),
        ),
        ResolvedImports::default(),
        EnvironmentSnapshot::new([("PATH", path)]),
        CancellationSource::new(),
        1,
        32,
    );
    let result = execute_workflow(
        fixture.admitted,
        &fixture.artifacts,
        &fixture.inputs,
        &StepDiagnosticLog::default(),
        AgentExecution::disabled(),
        TestClock,
        NoopExecutionObserver,
    )
    .await
    .unwrap();
    assert_eq!(result.outcome, RunOutcome::Succeeded);
}

#[tokio::test]
async fn command_receives_eof_instead_of_the_adapters_live_input() {
    with_watchdog(async {
        let mut engine = Command::new(env::current_exe().unwrap());
        engine
            .args(["--ignored", "--exact", STDIN_FIXTURE_TEST_NAME])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut engine = engine.spawn().unwrap();
        let _live_adapter_input = engine.stdin.take().unwrap();
        assert!(engine.wait().await.unwrap().success());
    })
    .await;
}

#[tokio::test]
async fn admitted_producer_consumer_executes_with_inputs_observations_and_export() {
    with_watchdog(async {
        let path = env::var_os("PATH").unwrap_or_else(|| OsString::from("/bin:/usr/bin"));
        let producer_script = r#"set -eu
if IFS= read -r unexpected; then exit 91; fi
{
  printf '%s|' "$(cat "$SCHERZO_STEP_INPUTS/values/prompt")"
  cat "$SCHERZO_STEP_INPUTS/collections/attachments/000000"
  printf '|'
  cat "$SCHERZO_STEP_INPUTS/collections/attachments/000001"
} > produced.txt
printf producer-standard-output
printf producer-standard-error >&2
"#;
        let consumer_script = r#"set -eu
if IFS= read -r unexpected; then exit 92; fi
cat "$SCHERZO_STEP_INPUTS/values/artifact" > exported.txt
printf consumer-standard-output
printf consumer-standard-error >&2
"#;
        let source = format!(
            "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    inputs:\n      prompt:\n        ref: imports.prompt\n      attachments:\n        ref: imports.attachments\n    command:\n      argv: {}\n    outputs:\n      produced:\n        kind: file\n        path: produced.txt\n        mediaType: text/plain\n  consume:\n    kind: cmd\n    inputs:\n      artifact:\n        ref: outputs.produce.produced\n    command:\n      argv: {}\n    outputs:\n      delivered:\n        kind: file\n        path: exported.txt\n        mediaType: text/plain\nexports:\n  result:\n    ref: outputs.consume.delivered\n",
            serde_json::to_string(&["sh", "-c", producer_script]).unwrap(),
            serde_json::to_string(&["sh", "-c", consumer_script]).unwrap(),
        );
        let fixture = execution_fixture(
            &source,
            ResolvedImports::new(
                Some(Arc::from("typed prompt")),
                Arc::from([
                    ResolvedAttachment::new(Arc::from("text/plain"), Arc::from(*b"first")),
                    ResolvedAttachment::new(Arc::from("text/plain"), Arc::from(*b"second")),
                ]),
            ),
            EnvironmentSnapshot::new([("PATH", path)]),
            CancellationSource::new(),
            2,
            3,
        );
        let expected_provenance = fixture.admitted.workflow().source.clone();
        let expected_digest = fixture.admitted.workflow().content_digest.clone();
        fs::remove_dir_all(&fixture.source_root).unwrap();
        let diagnostics = StepDiagnosticLog::default();
        let (observer, entries, _observed) = RecordingObserver::new();

        let result = execute_workflow(
            fixture.admitted,
            &fixture.artifacts,
            &fixture.inputs,
            &diagnostics,
            AgentExecution::disabled(),
            TestClock,
            observer,
        )
        .await
        .unwrap();

        assert_eq!(result.outcome, RunOutcome::Succeeded);
        assert!(matches!(result.steps["produce"], StepState::Succeeded { .. }));
        assert!(matches!(result.steps["consume"], StepState::Succeeded { .. }));
        assert_eq!(result.provenance, expected_provenance);
        assert_eq!(result.content_digest, expected_digest);
        assert!(fixture.execution_root.exists());
        assert_eq!(fixture.inputs.active_view_count(), 0);
        assert_eq!(fixture.inputs.reservation_usage(), (0, 0, 0));

        let ExportValue::Available { output } = &result.exports["result"] else {
            panic!("exported file was unavailable");
        };
        let file = output.as_file().unwrap();
        let mut exported = Vec::new();
        fixture
            .artifacts
            .copy_to(file.handle(), &mut exported)
            .unwrap();
        assert_eq!(exported, b"typed prompt|first|second");

        let entries = entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_stream(&entries, "produce", CommandOutputSource::StandardOutput, b"producer-standard-output");
        assert_stream(&entries, "produce", CommandOutputSource::StandardError, b"producer-standard-error");
        assert_stream(&entries, "consume", CommandOutputSource::StandardOutput, b"consumer-standard-output");
        assert_stream(&entries, "consume", CommandOutputSource::StandardError, b"consumer-standard-error");
        assert!(entries.iter().any(|entry| matches!(entry, ExecutionObservation::Transition(_))));
        assert_eq!(diagnostics.get("produce").unwrap().standard_output().bytes(), b"pro");

        fixture.artifacts.release().unwrap();
        let mut unavailable = Vec::new();
        assert!(matches!(
            fixture.artifacts.copy_to(file.handle(), &mut unavailable),
            Err(ArtifactReadFailure::Unavailable | ArtifactReadFailure::UnknownHandle)
        ));
    })
    .await;
}

#[tokio::test]
async fn failure_stops_new_work_but_retains_the_successful_sibling_output() {
    with_watchdog(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let executable = env::current_exe().unwrap();
        let fixture_args = fixture_arguments();
        let fail_script = fixture_script(23, "fail", false);
        let sibling_script = fixture_script(0, "sibling", true);
        let queued_script = fixture_script(0, "queued", false);
        let source = format!(
            "schemaVersion: 1\nsteps:\n  aFail:\n    kind: cmd\n    command:\n      argv: {}\n  bSibling:\n    kind: cmd\n    command:\n      argv: {}\n    outputs:\n      retained:\n        kind: file\n        path: retained.txt\n        mediaType: text/plain\n  cFailChild:\n    kind: cmd\n    dependsOn: [aFail]\n    command:\n      argv: {}\n  zQueued:\n    kind: cmd\n    command:\n      argv: {}\n  zzQueuedChild:\n    kind: cmd\n    dependsOn: [zQueued]\n    command:\n      argv: {}\nexports:\n  retained:\n    ref: outputs.bSibling.retained\n",
            command_argv(&fail_script, &executable, &fixture_args),
            command_argv(&sibling_script, &executable, &fixture_args),
            command_argv(&queued_script, &executable, &fixture_args),
            command_argv(&queued_script, &executable, &fixture_args),
            command_argv(&queued_script, &executable, &fixture_args),
        );
        let fixture = execution_fixture(
            &source,
            ResolvedImports::default(),
            fixture_environment(&listener),
            CancellationSource::new(),
            2,
            1024,
        );
        let diagnostics = StepDiagnosticLog::default();
        let (observer, _entries, mut observed) = RecordingObserver::new();
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn(async move {
            execute_workflow(
                fixture.admitted,
                &artifacts,
                &inputs,
                &diagnostics,
                AgentExecution::disabled(),
                TestClock,
                observer,
            )
            .await
        });

        let first = accept_fixture(&listener).await;
        let second = accept_fixture(&listener).await;
        let mut commands = BTreeMap::from([(first.0.clone(), first.1), (second.0.clone(), second.1)]);
        assert_eq!(commands.keys().cloned().collect::<Vec<_>>(), ["fail", "sibling"]);
        release_fixture(commands.remove("fail").unwrap()).await;
        wait_for_step_transition(&mut observed, "aFail", StepStateKind::Failed).await;
        release_fixture(commands.remove("sibling").unwrap()).await;

        let result = execution.await.unwrap().unwrap();
        assert!(matches!(
            &result.steps["aFail"],
            StepState::Failed {
                phase: FailurePhase::Execution,
                cause: StepFailureCause::Execution(StepExecutionFailure::Command(
                    CommandExecutionFailure::UnsuccessfulExit { code: Some(23) }
                )),
            }
        ));
        assert_eq!(
            result.steps["cFailChild"],
            StepState::Blocked {
                dependency: "aFail".to_owned()
            }
        );
        assert_eq!(
            result.steps["zQueued"],
            StepState::NotRun {
                reason: NotRunReason::FailureStop
            }
        );
        assert_eq!(
            result.steps["zzQueuedChild"],
            StepState::Blocked {
                dependency: "zQueued".to_owned()
            }
        );
        let ExportValue::Available { output } = &result.exports["retained"] else {
            panic!("successful sibling output was not retained");
        };
        let mut retained = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut retained)
            .unwrap();
        assert_eq!(retained, b"retained sibling");
    })
    .await;
}

#[tokio::test]
async fn controlled_cancellation_orders_events_and_waits_for_terminal_delivery() {
    with_watchdog(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let cancellation = CancellationSource::new();
        let source = format!(
            "schemaVersion: 1\nsteps:\n  active:\n    kind: cmd\n    command:\n      argv: {}\n  pending:\n    kind: cmd\n    dependsOn: [active]\n    command:\n      argv: {}\n",
            serde_json::to_string(&std::iter::once(env::current_exe().unwrap().to_string_lossy().into_owned()).chain(fixture_arguments()).collect::<Vec<_>>()).unwrap(),
            serde_json::to_string(&["true"]).unwrap(),
        );
        let mut environment = fixture_environment(&listener).variables().clone();
        environment.insert(OsString::from("WORKFLOW_FIXTURE_MODE"), OsString::from("interruptible-group"));
        environment.insert(OsString::from("WORKFLOW_FIXTURE_OUTPUT_BYTES"), OsString::from("19"));
        environment.insert(OsString::from("WORKFLOW_FIXTURE_ROLE"), OsString::from("active"));
        let fixture = execution_fixture(
            &source,
            ResolvedImports::default(),
            EnvironmentSnapshot::new(environment),
            cancellation.clone(),
            1,
            4,
        );
        let diagnostics = StepDiagnosticLog::default();
        let (observer, entries, _observed, mut terminal_reached, release_terminal) =
            RecordingObserver::with_terminal_gate();
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn(async move {
            execute_workflow(
                fixture.admitted,
                &artifacts,
                &inputs,
                &diagnostics,
                AgentExecution::disabled(),
                TestClock,
                observer,
            )
            .await
        });

        let (_role, command) = accept_fixture(&listener).await;
        assert_eq!(read_fixture_event(&command).await["event"], "output-written");
        assert!(cancellation.request_cancellation(CancellationReason::TerminationRequest));
        assert!(!cancellation.request_cancellation(CancellationReason::RunnerShutdown));
        assert_eq!(read_fixture_event(&command).await["event"], "interrupted");

        terminal_reached.recv().await.unwrap();
        assert!(!execution.is_finished());
        release_terminal.send(true).unwrap();
        let result = execution.await.unwrap().unwrap();
        assert_eq!(
            result.outcome,
            RunOutcome::Cancelled {
                reason: CancellationReason::TerminationRequest
            }
        );
        assert_eq!(
            result.steps["active"],
            StepState::Cancelled {
                reason: CancellationReason::TerminationRequest
            }
        );
        assert_eq!(
            result.steps["pending"],
            StepState::Cancelled {
                reason: CancellationReason::TerminationRequest
            }
        );

        let entries = entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let transitions = entries
            .iter()
            .filter_map(|entry| match entry {
                ExecutionObservation::Transition(transition) => Some(&transition.event),
                ExecutionObservation::CommandOutput(_)
                | ExecutionObservation::CommandOutputClosed(_)
                | ExecutionObservation::Agent(_) => None,
            })
            .collect::<Vec<_>>();
        let accepted = transitions.iter().position(|transition| matches!(
            transition,
            TransitionEvent::CancellationAccepted {
                reason: CancellationReason::TerminationRequest,
                deadline,
                ..
            } if *deadline == TestInstant(Duration::from_secs(1))
        )).unwrap();
        let derived = transitions.iter().position(|transition| matches!(
            transition,
            TransitionEvent::Step {
                step,
                to: StepStateKind::Cancelling,
                ..
            } if step == "active"
        )).unwrap();
        assert!(accepted < derived);
        assert_stream_contains(
            &entries,
            "active",
            CommandOutputSource::StandardOutput,
            &[b'o'; 19],
        );
        assert_stream_contains(
            &entries,
            "active",
            CommandOutputSource::StandardError,
            &[b'e'; 19],
        );
    })
    .await;
}

#[tokio::test]
async fn initial_cancellation_is_observed_without_starting_a_step() {
    with_watchdog(async {
        let cancellation = CancellationSource::new();
        assert!(cancellation.request_cancellation(CancellationReason::CallerOutputFailure));
        let fixture = execution_fixture(
            "schemaVersion: 1\nsteps:\n  never:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
            ResolvedImports::default(),
            EnvironmentSnapshot::default(),
            cancellation,
            1,
            32,
        );
        let diagnostics = StepDiagnosticLog::default();
        let (observer, entries, _observed) = RecordingObserver::new();
        let result = execute_workflow(
            fixture.admitted,
            &fixture.artifacts,
            &fixture.inputs,
            &diagnostics,
            AgentExecution::disabled(),
            TestClock,
            observer,
        )
        .await
        .unwrap();
        assert_eq!(
            result.outcome,
            RunOutcome::Cancelled {
                reason: CancellationReason::CallerOutputFailure
            }
        );
        let transitions = entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter_map(|entry| match entry {
                ExecutionObservation::Transition(transition) => Some(transition.event.clone()),
                ExecutionObservation::CommandOutput(_)
                | ExecutionObservation::CommandOutputClosed(_)
                | ExecutionObservation::Agent(_) => None,
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            transitions.as_slice(),
            [
                TransitionEvent::CancellationAccepted {
                    reason: CancellationReason::CallerOutputFailure,
                    deadline,
                    ..
                },
                TransitionEvent::Step {
                    step,
                    from: StepStateKind::Pending,
                    to: StepStateKind::Cancelled,
                    ..
                },
                TransitionEvent::Workflow {
                    to: WorkflowState::Cancelled {
                        reason: CancellationReason::CallerOutputFailure
                    },
                    ..
                }
            ] if *deadline == TestInstant(Duration::from_secs(1)) && step == "never"
        ));
    })
    .await;
}

const AGENT_PROFILE: &str = r#"agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: xhigh
"#;

fn agent_runtime(
    fixture: &ExecutionFixture,
    adapter: ScriptedAgentDispatcher,
) -> AgentExecution<ScriptedAgentDispatcher> {
    AgentExecution::enabled(
        WorkflowRunId::from(Arc::from("run-fixed")),
        fixture.agent_inputs.clone(),
        fixture.diagnostic_sessions.clone(),
        adapter,
    )
}

#[tokio::test]
async fn agent_response_and_file_commit_atomically_before_command_data_flow() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  produce:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      response:
        kind: agent_response
      artifact:
        kind: file
        path: agent.txt
        mediaType: text/plain
  consume:
    kind: cmd
    inputs:
      response:
        ref: outputs.produce.response
    command:
      argv: ["/bin/sh", "-c", "IFS= read -r value < \"$SCHERZO_STEP_INPUTS/values/response\" || true; printf '%s' \"$value\" > consumed.txt"]
    outputs:
      consumed:
        kind: file
        path: consumed.txt
        mediaType: text/plain
exports:
  response:
    ref: outputs.produce.response
  artifact:
    ref: outputs.produce.artifact
  consumed:
    ref: outputs.consume.consumed
"#
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("prompt.md", b"produce the declared outputs")],
            ResolvedImports::default(),
            EnvironmentSnapshot::default(),
            CancellationSource::new(),
            2,
            1024,
        );
        fs::write(fixture.execution_root.join("agent.txt"), b"agent artifact").unwrap();
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let diagnostics = StepDiagnosticLog::default();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &diagnostics,
                    agents,
                    TestClock,
                    NoopExecutionObserver,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        started.control().start().await.unwrap();
        assert_eq!(started.identity().run().as_ref(), "run-fixed");
        assert_eq!(started.identity().step(), "produce");
        started
            .control()
            .observe(AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::HarnessStarted,
            })
            .await
            .unwrap();
        started
            .control()
            .propose(ScriptedAgentValue::Response(Arc::from("agent response")))
            .await
            .unwrap();
        started.control().complete().await.unwrap();

        let result = execution.await.unwrap().unwrap();
        assert_eq!(result.outcome, RunOutcome::Succeeded);
        let StepState::Succeeded { outputs } = &result.steps["produce"] else {
            panic!("agent producer did not succeed");
        };
        assert_eq!(outputs.len(), 2);
        assert!(matches!(
            &outputs["response"],
            CapturedValue::Text(value) if value.as_ref() == "agent response"
        ));
        let ExportValue::Available { output } = &result.exports["consumed"] else {
            panic!("downstream command output was unavailable");
        };
        let mut consumed = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut consumed)
            .unwrap();
        assert_eq!(consumed, b"agent response");
        let ExportValue::Available { output } = &result.exports["artifact"] else {
            panic!("agent file output was unavailable");
        };
        let mut artifact = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut artifact)
            .unwrap();
        assert_eq!(artifact, b"agent artifact");
        assert_eq!(fixture.agent_inputs.active_view_count(), 0);
    })
    .await;
}

#[tokio::test]
async fn structured_agent_result_flows_only_through_its_explicit_command_binding() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  produce:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      result:
        kind: agent_result
        schema: result.schema.json
  consume:
    kind: cmd
    inputs:
      result:
        ref: outputs.produce.result
    command:
      argv: ["/bin/sh", "-c", "IFS= read -r value < \"$SCHERZO_STEP_INPUTS/values/result\" || true; printf '%s' \"$value\" > consumed.json"]
    outputs:
      consumed:
        kind: file
        path: consumed.json
        mediaType: application/json
exports:
  result:
    ref: outputs.produce.result
  consumed:
    ref: outputs.consume.consumed
"#
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[
                ("prompt.md", b"return a result"),
                (
                    "result.schema.json",
                    br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
                ),
            ],
            ResolvedImports::default(),
            EnvironmentSnapshot::default(),
            CancellationSource::new(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &StepDiagnosticLog::default(),
                    agents,
                    TestClock,
                    NoopExecutionObserver,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        started.control().start().await.unwrap();
        started
            .control()
            .propose(ScriptedAgentValue::Result(Arc::new(json!({
                "z": 2,
                "a": 1
            }))))
            .await
            .unwrap();
        started.control().complete().await.unwrap();

        let result = execution.await.unwrap().unwrap();
        let ExportValue::Available { output } = &result.exports["result"] else {
            panic!("structured result was unavailable");
        };
        assert!(matches!(
            output,
            CapturedValue::Json(value) if value.as_ref() == &json!({"z": 2, "a": 1})
        ));
        let ExportValue::Available { output } = &result.exports["consumed"] else {
            panic!("result consumer was unavailable");
        };
        let mut consumed = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut consumed)
            .unwrap();
        assert_eq!(consumed, br#"{"a":1,"z":2}"#);
    })
    .await;
}

#[tokio::test]
async fn agent_consumes_committed_agent_and_file_outputs_through_runtime_graph() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  aResponse:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      response:
        kind: agent_response
  bResult:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      result:
        kind: agent_result
        schema: result.schema.json
  cFile:
    kind: cmd
    command:
      argv: ["/bin/sh", "-c", "true"]
    outputs:
      artifact:
        kind: file
        path: upstream.txt
        mediaType: text/plain
  zConsumer:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - ref: outputs.aResponse.response
        attachments:
          - ref: outputs.bResult.result
          - ref: outputs.cFile.artifact
"#
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[
                ("prompt.md", b"consume committed values"),
                (
                    "result.schema.json",
                    br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
                ),
            ],
            ResolvedImports::default(),
            EnvironmentSnapshot::default(),
            CancellationSource::new(),
            3,
            1024,
        );
        fs::write(fixture.execution_root.join("upstream.txt"), b"file exact").unwrap();
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let mut execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &StepDiagnosticLog::default(),
                    agents,
                    TestClock,
                    NoopExecutionObserver,
                )
                .await
            }
        });

        let first = control.wait_until_started().await.unwrap();
        let second = control.wait_until_started().await.unwrap();
        let producers = BTreeMap::from([
            (first.identity().step().to_owned(), first.control().clone()),
            (second.identity().step().to_owned(), second.control().clone()),
        ]);
        assert_eq!(
            producers.keys().map(String::as_str).collect::<Vec<_>>(),
            ["aResponse", "bResult"]
        );
        for producer in producers.values() {
            producer.start().await.unwrap();
        }
        producers["aResponse"]
            .propose(ScriptedAgentValue::Response(Arc::from("response exact")))
            .await
            .unwrap();
        producers["bResult"]
            .propose(ScriptedAgentValue::Result(Arc::new(json!({
                "z": 2,
                "a": 1
            }))))
            .await
            .unwrap();
        producers["aResponse"].complete().await.unwrap();
        producers["bResult"].complete().await.unwrap();

        let consumer = tokio::select! {
            consumer = control.wait_until_started() => match consumer {
                Ok(consumer) => consumer,
                Err(failure) => panic!(
                    "adapter stopped before the dependent agent started ({failure:?}): {:?}",
                    (&mut execution).await
                ),
            },
            result = &mut execution => panic!(
                "workflow finished before the dependent agent started: {result:?}"
            ),
        };
        assert_eq!(consumer.identity().step(), "zConsumer");
        assert_eq!(consumer.message(), "response exact");
        assert_eq!(consumer.attachments().len(), 2);
        assert_eq!(consumer.attachments()[0].media_type(), "application/json");
        assert_eq!(
            fs::read(consumer.attachments()[0].path()).unwrap(),
            br#"{"a":1,"z":2}"#
        );
        assert_eq!(consumer.attachments()[1].media_type(), "text/plain");
        assert_eq!(
            fs::read(consumer.attachments()[1].path()).unwrap(),
            b"file exact"
        );
        consumer.control().start().await.unwrap();
        consumer.control().complete().await.unwrap();

        let result = execution.await.unwrap().unwrap();
        assert_eq!(result.outcome, RunOutcome::Succeeded);
        assert!(matches!(
            result.steps["zConsumer"],
            StepState::Succeeded { .. }
        ));
        assert_eq!(fixture.agent_inputs.active_view_count(), 0);
    })
    .await;
}

#[tokio::test]
async fn failed_agent_file_capture_commits_neither_response_nor_file() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  produce:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      response:
        kind: agent_response
      missing:
        kind: file
        path: missing.txt
        mediaType: text/plain
  consume:
    kind: cmd
    inputs:
      response:
        ref: outputs.produce.response
    command:
      argv: ["/bin/true"]
exports:
  response:
    ref: outputs.produce.response
  missing:
    ref: outputs.produce.missing
"#
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("prompt.md", b"produce output")],
            ResolvedImports::default(),
            EnvironmentSnapshot::default(),
            CancellationSource::new(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &StepDiagnosticLog::default(),
                    agents,
                    TestClock,
                    NoopExecutionObserver,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        started.control().start().await.unwrap();
        started
            .control()
            .propose(ScriptedAgentValue::Response(Arc::from("must not commit")))
            .await
            .unwrap();
        started.control().complete().await.unwrap();

        let result = execution.await.unwrap().unwrap();
        assert!(matches!(
            result.steps["produce"],
            StepState::Failed {
                phase: FailurePhase::OutputCapture,
                ..
            }
        ));
        assert_eq!(
            result.steps["consume"],
            StepState::Blocked {
                dependency: "produce".to_owned()
            }
        );
        assert!(matches!(
            result.exports["response"],
            ExportValue::Unavailable { .. }
        ));
        assert!(matches!(
            result.exports["missing"],
            ExportValue::Unavailable { .. }
        ));
        assert_eq!(fixture.agent_inputs.active_view_count(), 0);
    })
    .await;
}

#[derive(Debug, Eq, PartialEq)]
struct AgentEngineTranscript {
    observations: Vec<AgentObservationEnvelope>,
    terminal_transitions: Vec<StepStateKind>,
}

#[tokio::test]
async fn no_value_agent_observations_are_repeatable_and_never_become_outputs() {
    let first = run_no_value_agent_transcript().await;
    let second = run_no_value_agent_transcript().await;
    assert_eq!(first, second);
    assert_eq!(first.observations.len(), 2);
    assert_eq!(first.observations[0].sequence().get(), 1);
    assert_eq!(first.observations[1].sequence().get(), 2);
    assert_eq!(first.terminal_transitions, [StepStateKind::Succeeded]);
}

async fn run_no_value_agent_transcript() -> AgentEngineTranscript {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  observe:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
"#
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("prompt.md", b"observe only")],
            ResolvedImports::default(),
            EnvironmentSnapshot::default(),
            CancellationSource::new(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let (observer, entries, _observed) = RecordingObserver::new();
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &StepDiagnosticLog::default(),
                    agents,
                    TestClock,
                    observer,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        started.control().start().await.unwrap();
        for observation in [
            AgentObservation::AssistantText {
                text: Arc::from("not an implicit response"),
            },
            AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::HarnessQuiescent,
            },
        ] {
            started.control().observe(observation).await.unwrap();
        }
        started.control().complete().await.unwrap();
        let result = execution.await.unwrap().unwrap();
        let StepState::Succeeded { outputs } = &result.steps["observe"] else {
            panic!("no-value agent did not succeed");
        };
        assert!(outputs.is_empty());
        assert!(result.exports.is_empty());
        let entries = entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let observations = entries
            .iter()
            .filter_map(|observation| match observation {
                ExecutionObservation::Agent(observation) => Some(observation.clone()),
                ExecutionObservation::Transition(_)
                | ExecutionObservation::CommandOutput(_)
                | ExecutionObservation::CommandOutputClosed(_) => None,
            })
            .collect();
        let terminal_transitions = entries
            .iter()
            .filter_map(|observation| match observation {
                ExecutionObservation::Transition(TransitionObservation {
                    event: TransitionEvent::Step { step, to, .. },
                    ..
                }) if step == "observe"
                    && matches!(
                        to,
                        StepStateKind::Succeeded | StepStateKind::Failed | StepStateKind::Cancelled
                    ) =>
                {
                    Some(*to)
                }
                ExecutionObservation::Transition(_)
                | ExecutionObservation::CommandOutput(_)
                | ExecutionObservation::CommandOutputClosed(_)
                | ExecutionObservation::Agent(_) => None,
            })
            .collect();
        AgentEngineTranscript {
            observations,
            terminal_transitions,
        }
    })
    .await
}

#[tokio::test]
async fn committed_agent_completion_wins_a_later_cancellation_before_delivery_finishes() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  complete:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      response:
        kind: agent_response
exports:
  response:
    ref: outputs.complete.response
"#
        );
        let cancellation = CancellationSource::new();
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("prompt.md", b"complete")],
            ResolvedImports::default(),
            EnvironmentSnapshot::default(),
            cancellation.clone(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let (observer, _entries, _observed, mut success_reached, release_success) =
            RecordingObserver::with_step_success_gate();
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &StepDiagnosticLog::default(),
                    agents,
                    TestClock,
                    observer,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        started.control().start().await.unwrap();
        started
            .control()
            .propose(ScriptedAgentValue::Response(Arc::from("winner")))
            .await
            .unwrap();
        started.control().complete().await.unwrap();
        success_reached.recv().await.unwrap();
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        assert!(!execution.is_finished());
        release_success.send(true).unwrap();

        let result = execution.await.unwrap().unwrap();
        assert_eq!(result.outcome, RunOutcome::Succeeded);
        assert!(matches!(
            result.exports["response"],
            ExportValue::Available {
                output: CapturedValue::Text(ref value)
            } if value.as_ref() == "winner"
        ));
        assert_eq!(fixture.agent_inputs.active_view_count(), 0);
    })
    .await;
}

#[tokio::test]
async fn agent_failure_stops_pending_command_but_keeps_active_agent_outputs() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  aFail:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
  bCommit:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      response:
        kind: agent_response
      artifact:
        kind: file
        path: retained.txt
        mediaType: text/plain
  cActive:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
  zStopped:
    kind: cmd
    dependsOn: [aFail]
    command:
      argv: ["/bin/true"]
exports:
  response:
    ref: outputs.bCommit.response
  artifact:
    ref: outputs.bCommit.artifact
"#
        );
        let cancellation = CancellationSource::new();
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("prompt.md", b"execute")],
            ResolvedImports::default(),
            EnvironmentSnapshot::default(),
            cancellation.clone(),
            3,
            1024,
        );
        fs::write(fixture.execution_root.join("retained.txt"), b"retained").unwrap();
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let (observer, _entries, mut observed) = RecordingObserver::new();
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &StepDiagnosticLog::default(),
                    agents,
                    TestClock,
                    observer,
                )
                .await
            }
        });

        let first = control.wait_until_started().await.unwrap();
        let second = control.wait_until_started().await.unwrap();
        let third = control.wait_until_started().await.unwrap();
        let controls = BTreeMap::from([
            (first.identity().step().to_owned(), first.control().clone()),
            (
                second.identity().step().to_owned(),
                second.control().clone(),
            ),
            (third.identity().step().to_owned(), third.control().clone()),
        ]);
        assert_eq!(
            controls.keys().map(String::as_str).collect::<Vec<_>>(),
            ["aFail", "bCommit", "cActive"]
        );
        for control in controls.values() {
            control.start().await.unwrap();
        }
        controls["bCommit"]
            .propose(ScriptedAgentValue::Response(Arc::from("committed")))
            .await
            .unwrap();
        controls["bCommit"].complete().await.unwrap();
        wait_for_step_transition(&mut observed, "bCommit", StepStateKind::Succeeded).await;
        controls["aFail"]
            .fail(AgentFailureCause::HarnessFailed {
                detail: crate::execution::workflow::agent::AgentHarnessFailureDetail::ModelError,
            })
            .await
            .unwrap();
        wait_for_step_transition(&mut observed, "aFail", StepStateKind::Failed).await;
        assert!(cancellation.request_cancellation(CancellationReason::RunnerShutdown));

        let result = execution.await.unwrap().unwrap();
        assert!(matches!(
            result.outcome,
            RunOutcome::Failed {
                later_cancellation: Some(CancellationReason::RunnerShutdown),
                ..
            }
        ));
        assert_eq!(
            result.steps["zStopped"],
            StepState::Blocked {
                dependency: "aFail".to_owned()
            }
        );
        assert_eq!(
            result.steps["cActive"],
            StepState::Cancelled {
                reason: CancellationReason::RunnerShutdown
            }
        );
        assert!(matches!(
            result.exports["response"],
            ExportValue::Available {
                output: CapturedValue::Text(_)
            }
        ));
        assert!(matches!(
            result.exports["artifact"],
            ExportValue::Available {
                output: CapturedValue::File(_)
            }
        ));
    })
    .await;
}

#[tokio::test]
async fn cancellation_discards_provisional_agent_value_and_waits_for_adapter_quiescence() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  active:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      response:
        kind: agent_response
      artifact:
        kind: file
        path: side-effect.txt
        mediaType: text/plain
  pending:
    kind: cmd
    dependsOn: [active]
    command:
      argv: ["/bin/true"]
exports:
  response:
    ref: outputs.active.response
  artifact:
    ref: outputs.active.artifact
"#
        );
        let cancellation = CancellationSource::new();
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("prompt.md", b"remain active")],
            ResolvedImports::default(),
            EnvironmentSnapshot::default(),
            cancellation.clone(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &StepDiagnosticLog::default(),
                    agents,
                    TestClock,
                    NoopExecutionObserver,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        started.control().start().await.unwrap();
        started
            .control()
            .propose(ScriptedAgentValue::Response(Arc::from("provisional")))
            .await
            .unwrap();
        let side_effect = fixture.execution_root.join("side-effect.txt");
        fs::write(&side_effect, b"ordinary filesystem side effect").unwrap();
        let mut barrier = started.control().block().unwrap();
        barrier.wait_until_blocked().await.unwrap();
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        assert!(!execution.is_finished());
        barrier.release().unwrap();

        let result = execution.await.unwrap().unwrap();
        assert_eq!(
            result.outcome,
            RunOutcome::Cancelled {
                reason: CancellationReason::UserRequest
            }
        );
        assert_eq!(
            result.steps["active"],
            StepState::Cancelled {
                reason: CancellationReason::UserRequest
            }
        );
        assert_eq!(
            result.steps["pending"],
            StepState::Cancelled {
                reason: CancellationReason::UserRequest
            }
        );
        assert!(matches!(
            result.exports["response"],
            ExportValue::Unavailable { .. }
        ));
        assert!(matches!(
            result.exports["artifact"],
            ExportValue::Unavailable { .. }
        ));
        assert_eq!(
            fs::read(side_effect).unwrap(),
            b"ordinary filesystem side effect",
            "cancellation must not roll back ordinary filesystem writes"
        );
        assert_eq!(fixture.artifacts.staged_artifact_count(), 0);
        assert_eq!(fixture.agent_inputs.active_view_count(), 0);
    })
    .await;
}

#[tokio::test]
async fn harness_start_failure_is_a_start_failure() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  task:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
"#
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("prompt.md", b"start")],
            ResolvedImports::default(),
            EnvironmentSnapshot::default(),
            CancellationSource::new(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &StepDiagnosticLog::default(),
                    agents,
                    TestClock,
                    NoopExecutionObserver,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        started
            .control()
            .fail(AgentFailureCause::HarnessStartFailed)
            .await
            .unwrap();

        let result = execution.await.unwrap().unwrap();
        assert!(
            matches!(
                result.steps["task"],
                StepState::Failed {
                    phase: FailurePhase::Start,
                    cause: StepFailureCause::Start(StepStartFailure::Agent(
                        AgentFailureCause::HarnessStartFailed
                    )),
                }
            ),
            "a pre-start harness failure must not transition the step through running: {:?}",
            result.steps["task"]
        );
    })
    .await;
}

fn assert_stream(
    entries: &[ExecutionObservation<TestInstant>],
    step: &str,
    source: CommandOutputSource,
    expected: &[u8],
) {
    assert_eq!(observed_stream(entries, step, source), expected);
}

fn assert_stream_contains(
    entries: &[ExecutionObservation<TestInstant>],
    step: &str,
    source: CommandOutputSource,
    expected: &[u8],
) {
    assert!(
        observed_stream(entries, step, source)
            .windows(expected.len())
            .any(|window| window == expected)
    );
}

fn observed_stream(
    entries: &[ExecutionObservation<TestInstant>],
    step: &str,
    source: CommandOutputSource,
) -> Vec<u8> {
    let observations = entries
        .iter()
        .filter_map(|entry| match entry {
            ExecutionObservation::CommandOutput(output)
                if output.step == step && output.source == source =>
            {
                Some(output)
            }
            ExecutionObservation::Transition(_)
            | ExecutionObservation::CommandOutput(_)
            | ExecutionObservation::CommandOutputClosed(_)
            | ExecutionObservation::Agent(_) => None,
        })
        .collect::<Vec<_>>();
    assert!(!observations.is_empty());
    assert!(
        observations
            .windows(2)
            .all(|pair| pair[0].sequence.get() + 1 == pair[1].sequence.get())
    );
    assert!(
        observations
            .iter()
            .all(|output| output.invocation == observations[0].invocation)
    );
    let closed = entries
        .iter()
        .filter_map(|entry| match entry {
            ExecutionObservation::CommandOutputClosed(closed)
                if closed.step == step && closed.source == source =>
            {
                Some(closed)
            }
            ExecutionObservation::Transition(_)
            | ExecutionObservation::CommandOutput(_)
            | ExecutionObservation::CommandOutputClosed(_)
            | ExecutionObservation::Agent(_) => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].invocation, observations[0].invocation);
    assert_eq!(
        closed[0].sequence.get(),
        observations.last().unwrap().sequence.get() + 1
    );
    observations
        .iter()
        .flat_map(|output| output.bytes.iter().copied())
        .collect()
}

fn fixture_arguments() -> Vec<String> {
    [
        "--ignored",
        "--exact",
        FIXTURE_TEST_NAME,
        "--nocapture",
        "--skip",
        FIXTURE_ARGUMENT,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn fixture_script(exit_code: i32, role: &str, writes_output: bool) -> String {
    let finish = if writes_output {
        "status=$?; if [ \"$status\" -eq 0 ]; then printf 'retained sibling' > retained.txt; fi; exit \"$status\""
    } else {
        "exit $?"
    };
    format!(
        "WORKFLOW_FIXTURE_EXIT_CODE={exit_code} WORKFLOW_FIXTURE_ROLE={role} \"$1\" \"$@\"; {finish}"
    )
}

fn command_argv(script: &str, executable: &Path, fixture_args: &[String]) -> String {
    serde_json::to_string(
        &std::iter::once("sh".to_owned())
            .chain(["-c".to_owned(), script.to_owned(), "fixture".to_owned()])
            .chain(std::iter::once(executable.to_string_lossy().into_owned()))
            .chain(fixture_args.iter().cloned())
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

fn fixture_environment(listener: &TcpListener) -> EnvironmentSnapshot {
    EnvironmentSnapshot::new([
        (
            OsString::from("WORKFLOW_FIXTURE_SOCKET"),
            OsString::from(listener.local_addr().unwrap().to_string()),
        ),
        (
            OsString::from("WORKFLOW_FIXTURE_EXIT_CODE"),
            OsString::from("0"),
        ),
        (
            OsString::from("PATH"),
            env::var_os("PATH").unwrap_or_else(|| OsString::from("/bin:/usr/bin")),
        ),
    ])
}

async fn accept_fixture(listener: &TcpListener) -> (String, TcpStream) {
    let (stream, _) = listener.accept().await.unwrap();
    let report = read_fixture_event(&stream).await;
    (report["role"].as_str().unwrap().to_owned(), stream)
}

async fn read_fixture_event(stream: &TcpStream) -> Value {
    let mut line = Vec::new();
    let mut buffer = [0_u8; 1];
    loop {
        stream.readable().await.unwrap();
        match stream.try_read(&mut buffer) {
            Ok(0) => panic!("fixture closed before reporting"),
            Ok(read) => {
                line.extend_from_slice(&buffer[..read]);
                if line.last() == Some(&b'\n') {
                    line.pop();
                    return serde_json::from_slice(&line).unwrap();
                }
            }
            Err(failure) if failure.kind() == io::ErrorKind::WouldBlock => {}
            Err(failure) => panic!("fixture read failed: {failure:?}"),
        }
    }
}

async fn release_fixture(stream: TcpStream) {
    loop {
        stream.writable().await.unwrap();
        match stream.try_write(&[1]) {
            Ok(1) => return,
            Ok(_) => {}
            Err(failure) if failure.kind() == io::ErrorKind::WouldBlock => {}
            Err(failure) => panic!("fixture release failed: {failure:?}"),
        }
    }
}

async fn wait_for_step_transition(
    observed: &mut mpsc::UnboundedReceiver<ExecutionObservation<TestInstant>>,
    expected_step: &str,
    expected_state: StepStateKind,
) {
    loop {
        if matches!(
            observed.recv().await,
            Some(ExecutionObservation::Transition(TransitionObservation {
                event:
                    TransitionEvent::Step {
                        step,
                        to,
                        ..
                    },
                ..
            })) if step == expected_step && to == expected_state
        ) {
            return;
        }
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is allowed only as an anti-hang watchdog, not a behavior assertion"
)]
async fn with_watchdog<Output>(future: impl Future<Output = Output>) -> Output {
    match tokio::time::timeout(TEST_WATCHDOG, future).await {
        Ok(output) => output,
        Err(_) => panic!("workflow execution test watchdog expired"),
    }
}
