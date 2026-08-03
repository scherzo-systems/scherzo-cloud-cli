use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::future::Future;
use std::io;
use std::ops::Add;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::sync::{mpsc, watch};

use super::*;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationReason, CancellationSource, CaptureLimits, EnvironmentSnapshot,
    ExecutionContext, ExecutionPolicyLimits, ExecutionRootLifecycle, InputLimits,
    ResolvedAttachment, ResolvedImports, admit_workflow,
};
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
    CommandExecutionFailure, StepExecutionFailure, StepFailureCause,
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

#[derive(Clone)]
struct RecordingObserver {
    entries: RecordedObservations,
    notifications: mpsc::UnboundedSender<ExecutionObservation<TestInstant>>,
    terminal_gate: Option<TerminalGate>,
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
        let gate = self.terminal_gate.clone();
        async move {
            if !is_terminal_cancellation(&observation) {
                return;
            }
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

struct ExecutionFixture {
    _temporary: tempfile::TempDir,
    execution_root: PathBuf,
    source_root: PathBuf,
    admitted: AdmittedWorkflow,
    artifacts: ArtifactStaging,
    inputs: InputStaging,
}

fn execution_fixture(
    source: &str,
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
        ),
    )
    .unwrap();
    let artifacts = ArtifactStaging::create(admitted.execution(), &staging_root).unwrap();
    let inputs = InputStaging::create(admitted.execution(), &staging_root).unwrap();
    ExecutionFixture {
        _temporary: temporary,
        execution_root,
        source_root,
        admitted,
        artifacts,
        inputs,
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
                | ExecutionObservation::CommandOutputClosed(_) => None,
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
                | ExecutionObservation::CommandOutputClosed(_) => None,
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
            | ExecutionObservation::CommandOutputClosed(_) => None,
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
            | ExecutionObservation::CommandOutputClosed(_) => None,
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
