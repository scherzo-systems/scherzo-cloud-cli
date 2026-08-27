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
use crate::execution::claude_code::ValidatedClaudeCodeInstallation;
use crate::execution::codex::ValidatedCodexInstallation;
use crate::execution::pi::ValidatedPiInstallation;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationReason, CancellationSource, CaptureLimits, EnvironmentSnapshot,
    ExecutionContext, ExecutionPolicyLimits, ExecutionRootLifecycle, InputLimits,
    ResolvedAttachment, ResolvedImports, admit_local_workflow, admit_workflow,
};
use crate::execution::workflow::agent::scripted::{
    ScriptedAgentDispatcher, ScriptedAgentValue, scripted_agent_dispatcher,
};
use crate::execution::workflow::agent::{
    AgentCompatibilityProfile, AgentFailureCause, AgentLifecycleMilestone, AgentObservation,
    AgentObservationEnvelope, AgentValueKind, WorkflowRunId,
};
use crate::execution::workflow::agent_diagnostics::AgentDiagnosticSessionStore;
use crate::execution::workflow::agent_input::AgentInputStaging;
use crate::execution::workflow::artifact::{ArtifactReadFailure, ArtifactStaging};
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::input::InputStaging;
use crate::execution::workflow::invocation_accounting::{InvocationAccountingLog, InvocationUsage};
use crate::execution::workflow::observation::{
    CommandOutputSource, ExecutionObservation, ExecutionObserver, NoopExecutionObserver,
    TransitionObservation,
};
use crate::execution::workflow::recovery::{
    RECOVERY_AGENT_INSTRUCTIONS, RECOVERY_CONTEXT_VARIABLE, RecoveryDecisionFailureKind,
    RecoveryHandlerFailure, read_recovery_context,
};
use crate::execution::workflow::resolution;
use crate::execution::workflow::runtime::{
    ExportValue, FailurePhase, NotRunReason, RecoveryHandlerOutcome, StepState, StepStateKind,
    TransitionEvent,
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
) -> Result<WorkflowExecutionResult<Clock::Instant>, CoordinationError>
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
        .with_pi_installation(ValidatedPiInstallation::fixture("/validated/pi".into()))
        .with_claude_code_installation(ValidatedClaudeCodeInstallation::fixture(
            "/validated/claude".into(),
        ))
        .with_codex_installation(ValidatedCodexInstallation::fixture(
            "/validated/codex".into(),
        )),
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
async fn source_neutral_command_handler_repairs_and_rechecks_with_private_authority() {
    with_watchdog(async {
        let source = r#"schemaVersion: 1
steps:
  repair:
    kind: cmd
    recovery:
      retries: 1
      handler:
        kind: cmd
        command:
          argv:
            - /bin/sh
            - -c
            - |
              if IFS= read -r unexpected; then exit 91; fi
              test -r "$SCHERZO_RECOVERY_CONTEXT"
              test ! -w "$SCHERZO_RECOVERY_CONTEXT"
              /bin/grep -q '"schemaVersion": 1' "$SCHERZO_RECOVERY_CONTEXT"
              /bin/grep -q '"recoveryRound": 1' "$SCHERZO_RECOVERY_CONTEXT"
              /bin/grep -q '"executionNumber": 1' "$SCHERZO_RECOVERY_CONTEXT"
              /bin/grep -q '"command_stderr"' "$SCHERZO_RECOVERY_CONTEXT"
              test -z "${SCHERZO_INHERITED+x}"
              printf '%s\n%s\n' SCHERZO_RECOVERY_CONTEXT SCHERZO_RECOVERY_RESULT > recovery-environment.txt
              printf '%s\n%s\n' "$SCHERZO_RECOVERY_CONTEXT" "$SCHERZO_RECOVERY_RESULT" > recovery-private-paths.txt
              printf repaired > repaired.marker
              printf '%s' '{"schemaVersion":1,"decision":"recheck","summary":"repaired workspace","reason":"target should pass unchanged"}' > "$SCHERZO_RECOVERY_RESULT"
              printf 'handler ordinary output is diagnostic only'
    command:
      argv:
        - /bin/sh
        - -c
        - |
          printf 'target diagnostic' >&2
          if test -f repaired.marker; then
            printf 'terminal output' > artifact.txt
            exit 0
          fi
          printf 'provisional output' > artifact.txt
          exit 75
    outputs:
      artifact:
        kind: file
        from: path
        path: artifact.txt
        mediaType: text/plain
exports:
  artifact:
    ref: outputs.repair.artifact
"#;
        let fixture = execution_fixture(
            source,
            ResolvedImports::default(),
            EnvironmentSnapshot::new([
                ("PATH", "/bin:/usr/bin"),
                ("EXPLICIT_VALUE", "retained"),
                ("SCHERZO_INHERITED", "must-be-scrubbed"),
            ]),
            CancellationSource::new(),
            1,
            1024,
        );
        let diagnostics = StepDiagnosticLog::default();
        let result = execute_workflow(
            fixture.admitted,
            &fixture.artifacts,
            &fixture.inputs,
            &diagnostics,
            AgentExecution::disabled(),
            TestClock,
            NoopExecutionObserver,
        )
        .await
        .unwrap();

        assert_eq!(result.outcome, RunOutcome::Succeeded);
        let ExportValue::Available { output } = &result.exports["artifact"] else {
            panic!("the recovered target output must be available");
        };
        let mut bytes = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut bytes)
            .unwrap();
        assert_eq!(bytes, b"terminal output");
        assert_eq!(
            fs::read_to_string(fixture.execution_root.join("recovery-environment.txt")).unwrap(),
            "SCHERZO_RECOVERY_CONTEXT\nSCHERZO_RECOVERY_RESULT\n"
        );
        let private_paths = fs::read_to_string(
            fixture.execution_root.join("recovery-private-paths.txt"),
        )
        .unwrap();
        assert!(private_paths.lines().all(|path| !Path::new(path).exists()));
        assert_eq!(fs::read(fixture.execution_root.join("repaired.marker")).unwrap(), b"repaired");
        let invocations = diagnostics.invocation_ids("repair");
        assert_eq!(invocations.len(), 3);
        assert_eq!(invocations.iter().copied().collect::<std::collections::BTreeSet<_>>().len(), 3);
        let handler_diagnostic = diagnostics.get_invocation("repair", invocations[1]).unwrap();
        assert_eq!(
            handler_diagnostic.standard_output().bytes(),
            b"handler ordinary output is diagnostic only"
        );
        assert_eq!(
            diagnostics.get("repair").unwrap().standard_output().bytes(),
            b"",
            "handler diagnostics must not become ordinary step output"
        );
    })
    .await;
}

#[tokio::test]
async fn omitted_recovery_handler_cwd_inherits_command_target_cwd() {
    with_watchdog(async {
        let source = r#"schemaVersion: 1
steps:
  repair:
    kind: cmd
    cwd: nested
    recovery:
      retries: 1
      handler:
        kind: cmd
        command:
          argv:
            - /bin/sh
            - -c
            - |
              printf repaired > repaired.marker
              printf '%s' '{"schemaVersion":1,"decision":"recheck","summary":"repaired workspace","reason":"rerun the target"}' > "$SCHERZO_RECOVERY_RESULT"
    command:
      argv: [/bin/sh, -c, "test -f repaired.marker || exit 75"]
"#;
        let fixture = execution_fixture(
            source,
            ResolvedImports::default(),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            CancellationSource::new(),
            1,
            1024,
        );
        fs::create_dir(fixture.execution_root.join("nested")).unwrap();

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
        assert_eq!(
            fs::read(fixture.execution_root.join("nested/repaired.marker")).unwrap(),
            b"repaired"
        );
    })
    .await;
}

#[tokio::test]
async fn semantic_outputs_recovery_reruns_complete_target() {
    with_watchdog(async {
        let source = r#"schemaVersion: 1
steps:
  repair:
    kind: cmd
    recovery:
      retries: 1
      handler:
        kind: cmd
        command:
          argv:
            - /bin/sh
            - -c
            - |
              test ! -e artifact.txt
              printf repaired > repaired.marker
              printf '%s' '{"schemaVersion":1,"decision":"recheck","summary":"restored generation precondition","reason":"rerun the complete target"}' > "$SCHERZO_RECOVERY_RESULT"
    command:
      argv:
        - /bin/sh
        - -c
        - |
          printf x >> target-runs.txt
          if test -f repaired.marker; then
            printf 'captured only from execution 2' > artifact.txt
          fi
    outputs:
      artifact:
        kind: file
        from: path
        path: artifact.txt
        mediaType: text/plain
exports:
  artifact:
    ref: outputs.repair.artifact
"#;
        let fixture = execution_fixture(
            source,
            ResolvedImports::default(),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            CancellationSource::new(),
            1,
            1024,
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
        assert_eq!(
            fs::read(fixture.execution_root.join("target-runs.txt")).unwrap(),
            b"xx"
        );
        let ExportValue::Available { output } = &result.exports["artifact"] else {
            panic!("execution 2 must own the terminal output");
        };
        let mut bytes = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut bytes)
            .unwrap();
        assert_eq!(bytes, b"captured only from execution 2");
        assert_eq!(fixture.artifacts.reservation_usage(), (0, 0));
        assert_eq!(fixture.inputs.reservation_usage(), (0, 0, 0));
        assert_eq!(fixture.agent_inputs.active_view_count(), 0);
    })
    .await;
}

#[tokio::test]
async fn command_handler_failures_stop_once_without_authorizing_recheck() {
    with_watchdog(async {
        let scenarios = [
            (
                "start",
                r#"command:
          argv: [/definitely-missing-recovery-handler]"#,
                RecoveryHandlerFailure::CommandLaunchFailed,
            ),
            (
                "execution",
                r#"command:
          argv: [/bin/sh, -c, "printf '%s' '{\"schemaVersion\":1,\"decision\":\"recheck\",\"summary\":\"looks valid\",\"reason\":\"but exit fails\"}' > \"$SCHERZO_RECOVERY_RESULT\"; exit 9"]"#,
                RecoveryHandlerFailure::CommandExitFailed { code: Some(9) },
            ),
            (
                "missing",
                r#"command:
          argv: [/bin/sh, -c, "true"]"#,
                RecoveryHandlerFailure::ResultMissing,
            ),
            (
                "validation",
                r#"command:
          argv: [/bin/sh, -c, "printf '{' > \"$SCHERZO_RECOVERY_RESULT\""]"#,
                RecoveryHandlerFailure::DecisionInvalid(
                    RecoveryDecisionFailureKind::InvalidJson,
                ),
            ),
            (
                "settlement",
                r#"command:
          argv:
            - /bin/sh
            - -c
            - |
              printf '%s' '{"schemaVersion":1,"decision":"recheck","summary":"valid","reason":"before settlement sabotage"}' > "$SCHERZO_RECOVERY_RESULT"
              root=${SCHERZO_RECOVERY_CONTEXT%/context/context.json}
              mv "$root" "$root-moved""#,
                RecoveryHandlerFailure::SettlementFailed,
            ),
        ];

        for (name, command, expected) in scenarios {
            let source = format!(
                "schemaVersion: 1\nsteps:\n  repair:\n    kind: cmd\n    recovery:\n      retries: 1\n      handler:\n        kind: cmd\n        {command}\n    command:\n      argv: [/bin/sh, -c, \"printf x >> target-count.txt; exit 75\"]\n"
            );
            let fixture = execution_fixture(
                &source,
                ResolvedImports::default(),
                EnvironmentSnapshot::new([(
                    OsString::from("PATH"),
                    env::var_os("PATH").unwrap(),
                )]),
                CancellationSource::new(),
                1,
                1024,
            );
            let diagnostics = StepDiagnosticLog::default();
            let coordinated = crate::execution::workflow::step_runtime::execute_workflow_observed(
                fixture.admitted,
                &fixture.artifacts,
                &fixture.inputs,
                &diagnostics,
                TestClock,
                NoopCommitPort,
                NoopExecutionObserver,
                AgentExecution::disabled(),
                crate::execution::workflow::process_group::ProcessGuardRegistry::default(),
            )
            .await
            .unwrap_or_else(|failure| panic!("{name} scenario failed to coordinate: {failure:?}"));
            assert!(matches!(coordinated.state.workflow, WorkflowState::Failed { .. }));
            assert_eq!(
                fs::read(fixture.execution_root.join("target-count.txt")).unwrap(),
                b"x",
                "{name} handler failure must not authorize target execution 2"
            );
            let recovery = coordinated.state.steps["repair"].recovery.as_ref().unwrap();
            assert_eq!(recovery.rounds.len(), 1);
            let RecoveryHandlerOutcome::Failed {
                cause: StepFailureCause::RecoveryHandler(actual),
                ..
            } = &recovery.rounds[0].handler.as_ref().unwrap().outcome
            else {
                panic!("{name} did not retain one typed handler failure");
            };
            assert_eq!(actual, &expected, "{name} handler failure kind");
        }
    })
    .await;
}

#[tokio::test]
async fn configured_inactive_local_recovery_preserves_target_execution() {
    let source = "schemaVersion: 1\nsteps:\n  guarded:\n    kind: cmd\n    recovery:\n      retries: 1\n    command:\n      argv: [\"/bin/sh\", \"-c\", \": > target-started\"]\n";
    let fixture = execution_fixture(
        source,
        ResolvedImports::default(),
        EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
        CancellationSource::new(),
        1,
        32,
    );
    let admitted = admit_local_workflow(
        resolution::resolve(&fixture.source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedImports::default(),
        ExecutionContext::new(
            fixture.execution_root.clone(),
            ExecutionRootLifecycle::CallerOwnedRetained,
            ExecutionPolicyLimits::new(
                1,
                CaptureLimits::new(16, 1024 * 1024, 8 * 1024 * 1024),
                InputLimits::new(16, 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024),
                32,
            ),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap();

    let result = execute_workflow(
        admitted,
        &fixture.artifacts,
        &fixture.inputs,
        &StepDiagnosticLog::default(),
        AgentExecution::disabled(),
        TestClock,
        NoopExecutionObserver,
    )
    .await;
    assert_eq!(result.unwrap().outcome, RunOutcome::Succeeded);
    assert!(fixture.execution_root.join("target-started").exists());
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
async fn command_finalizer_receives_the_engine_context_after_ordinary_quiescence() {
    with_watchdog(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let executable = env::current_exe().unwrap();
        let fixture_args = fixture_arguments();
        let source = format!(
            "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command:\n      argv: {}\nfinalizers:\n  release:\n    kind: cmd\n    inputs:\n      context: {{ ref: finalization.context }}\n    command:\n      argv: {}\n",
            command_argv(
                &fixture_script(0, "work", false),
                &executable,
                &fixture_args
            ),
            command_argv(
                &fixture_script(0, "release", false),
                &executable,
                &fixture_args
            ),
        );
        let fixture = execution_fixture(
            &source,
            ResolvedImports::default(),
            fixture_environment(&listener),
            CancellationSource::new(),
            1,
            32,
        );
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn(async move {
            execute_workflow(
                fixture.admitted,
                &artifacts,
                &inputs,
                &StepDiagnosticLog::default(),
                AgentExecution::disabled(),
                TestClock,
                NoopExecutionObserver,
            )
            .await
        });

        let (role, work) = accept_fixture(&listener).await;
        assert_eq!(role, "work");
        release_fixture(work).await;
        let (role, release) = accept_fixture(&listener).await;
        assert_eq!(role, "release");
        release_fixture(release).await;

        let result = execution.await.unwrap().unwrap();
        assert_eq!(result.outcome, RunOutcome::Succeeded);
        assert!(matches!(
            result.steps["release"],
            StepState::Succeeded { .. }
        ));
        let summary = result.finalization_summary.unwrap();
        assert_eq!(
            summary.trigger,
            crate::execution::workflow::document::FinalizationTrigger::Succeeded
        );
        assert!(matches!(
            summary.finalizers[0].disposition,
            StepState::Succeeded { .. }
        ));
    })
    .await;
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
            "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    inputs:\n      prompt:\n        ref: imports.prompt\n      attachments:\n        ref: imports.attachments\n    command:\n      argv: {}\n    outputs:\n      produced:\n        kind: file\n        from: path\n        path: produced.txt\n        mediaType: text/plain\n  consume:\n    kind: cmd\n    inputs:\n      artifact:\n        ref: outputs.produce.produced\n    command:\n      argv: {}\n    outputs:\n      delivered:\n        kind: file\n        from: path\n        path: exported.txt\n        mediaType: text/plain\nexports:\n  result:\n    ref: outputs.consume.delivered\n",
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
            "schemaVersion: 1\nsteps:\n  aFail:\n    kind: cmd\n    command:\n      argv: {}\n  bSibling:\n    kind: cmd\n    command:\n      argv: {}\n    outputs:\n      retained:\n        kind: file\n        from: path\n        path: retained.txt\n        mediaType: text/plain\n  cFailChild:\n    kind: cmd\n    dependsOn: [aFail]\n    command:\n      argv: {}\n  zQueued:\n    kind: cmd\n    command:\n      argv: {}\n  zzQueuedChild:\n    kind: cmd\n    dependsOn: [zQueued]\n    command:\n      argv: {}\nexports:\n  retained:\n    ref: outputs.bSibling.retained\n",
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

fn agent_runtime_with_accounting(
    fixture: &ExecutionFixture,
    adapter: ScriptedAgentDispatcher,
    accounting: InvocationAccountingLog,
) -> AgentExecution<ScriptedAgentDispatcher> {
    AgentExecution::enabled_with_accounting(
        WorkflowRunId::from(Arc::from("run-fixed")),
        fixture.agent_inputs.clone(),
        fixture.diagnostic_sessions.clone(),
        adapter,
        accounting,
    )
}

fn recovery_profile_source(profile: AgentCompatibilityProfile) -> &'static str {
    match profile {
        AgentCompatibilityProfile::PiJsonV1 => {
            r#"agentProfiles:
  recovery:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: xhigh
"#
        }
        AgentCompatibilityProfile::ClaudeCodeStreamJsonV1 => {
            r#"agentProfiles:
  recovery:
    harness:
      kind: claude_code
      config:
        model: claude-opus-4-1
        effort: xhigh
"#
        }
        AgentCompatibilityProfile::CodexAppServerV1 => {
            r#"agentProfiles:
  recovery:
    harness:
      kind: codex
      config:
        model: gpt-5.4
        effort: xhigh
"#
        }
    }
}

#[tokio::test]
async fn omitted_recovery_handler_cwd_inherits_agent_target_cwd() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{}steps:
  repair:
    kind: cmd
    cwd: nested
    recovery:
      retries: 1
      handler:
        kind: agent
        profile: recovery
        prompt: recovery.md
    command:
      argv: [/bin/sh, -c, "exit 75"]
"#,
            recovery_profile_source(AgentCompatibilityProfile::PiJsonV1)
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("recovery.md", b"Inspect the target working directory.")],
            ResolvedImports::default(),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            CancellationSource::new(),
            1,
            1024,
        );
        fs::create_dir(fixture.execution_root.join("nested")).unwrap();
        let (adapter, mut control) = scripted_agent_dispatcher();
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            let agents = agent_runtime(&fixture, adapter);
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
        assert_eq!(
            started.working_directory(),
            fs::canonicalize(fixture.execution_root.join("nested")).unwrap()
        );
        started.control().start().await.unwrap();
        started
            .control()
            .propose(ScriptedAgentValue::Result(Arc::new(json!({
                "schemaVersion": 1,
                "decision": "gave_up",
                "summary": "inspection complete",
                "reason": "the fixture only checks cwd inheritance"
            }))))
            .await
            .unwrap();
        started.control().complete().await.unwrap();
        assert!(matches!(
            execution.await.unwrap().unwrap().outcome,
            RunOutcome::Failed { .. }
        ));
    })
    .await;
}

#[tokio::test]
async fn duplicate_key_agent_decision_stops_without_recheck() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{}steps:
  repair:
    kind: cmd
    recovery:
      retries: 1
      handler:
        kind: agent
        profile: recovery
        prompt: recovery.md
    command:
      argv: [/bin/sh, -c, "printf x >> target-count.txt; exit 75"]
"#,
            recovery_profile_source(AgentCompatibilityProfile::PiJsonV1)
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("recovery.md", b"Submit a recovery decision.")],
            ResolvedImports::default(),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            CancellationSource::new(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let diagnostics = StepDiagnosticLog::default();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            let agents = agent_runtime(&fixture, adapter);
            async move {
                crate::execution::workflow::step_runtime::execute_workflow_observed(
                    admitted,
                    &artifacts,
                    &inputs,
                    &diagnostics,
                    TestClock,
                    NoopCommitPort,
                    NoopExecutionObserver,
                    agents,
                    crate::execution::workflow::process_group::ProcessGuardRegistry::default(),
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        started.control().start().await.unwrap();
        started
            .control()
            .propose(ScriptedAgentValue::RawResult(Arc::from(
                br#"{"schemaVersion":1,"decision":"recheck","decision":"gave_up","summary":"ambiguous","reason":"duplicate authority"}"#
                    .as_slice(),
            )))
            .await
            .unwrap();
        started.control().complete().await.unwrap();

        let coordinated = execution.await.unwrap().unwrap();
        assert!(matches!(coordinated.state.workflow, WorkflowState::Failed { .. }));
        assert_eq!(
            fs::read(fixture.execution_root.join("target-count.txt")).unwrap(),
            b"x",
            "an ambiguous agent decision must not authorize execution 2"
        );
        let recovery = coordinated.state.steps["repair"].recovery.as_ref().unwrap();
        let RecoveryHandlerOutcome::Failed {
            cause: StepFailureCause::RecoveryHandler(actual),
            ..
        } = &recovery.rounds[0].handler.as_ref().unwrap().outcome
        else {
            panic!("the duplicate decision must retain one typed handler failure");
        };
        assert_eq!(
            actual,
            &RecoveryHandlerFailure::AgentResultInvalid(
                RecoveryDecisionFailureKind::DuplicateKey
            )
        );
    })
    .await;
}

#[tokio::test]
async fn all_profiles_use_one_fresh_authoritative_recovery_protocol() {
    with_watchdog(async {
        for profile in [
            AgentCompatibilityProfile::PiJsonV1,
            AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
            AgentCompatibilityProfile::CodexAppServerV1,
        ] {
            let source = format!(
                r#"schemaVersion: 1
{}steps:
  repair:
    kind: cmd
    recovery:
      retries: 1
      handler:
        kind: agent
        profile: recovery
        prompt: recovery.md
    command:
      argv: [/bin/sh, -c, "test -f repaired.marker"]
"#,
                recovery_profile_source(profile)
            );
            let fixture = execution_fixture_with_source_files(
                &source,
                &[(
                    "recovery.md",
                    b"Repair the generated workspace, then request a recheck.",
                )],
                ResolvedImports::default(),
                EnvironmentSnapshot::new([
                    ("PATH", "/bin:/usr/bin"),
                    ("SCHERZO_INHERITED", "must-be-scrubbed"),
                ]),
                CancellationSource::new(),
                1,
                1024,
            );
            let (adapter, mut control) = scripted_agent_dispatcher();
            let accounting = InvocationAccountingLog::default();
            let agents = agent_runtime_with_accounting(&fixture, adapter, accounting.clone());
            let diagnostics = StepDiagnosticLog::default();
            let execution_diagnostics = diagnostics.clone();
            let artifacts = fixture.artifacts.clone();
            let inputs = fixture.inputs.clone();
            let (observer, observations, _observed) = RecordingObserver::new();
            let execution = tokio::spawn({
                let admitted = fixture.admitted.clone();
                async move {
                    execute_workflow(
                        admitted,
                        &artifacts,
                        &inputs,
                        &execution_diagnostics,
                        agents,
                        TestClock,
                        observer,
                    )
                    .await
                }
            });

            let started = control.wait_until_started().await.unwrap();
            assert_eq!(started.profile(), profile);
            assert_eq!(started.system_prompt(), RECOVERY_AGENT_INSTRUCTIONS);
            assert_eq!(
                started.message(),
                "Repair the generated workspace, then request a recheck."
            );
            assert_eq!(started.value_kind(), AgentValueKind::Result);
            assert!(started.attachments().is_empty());
            assert!(started.result_endpoint_directory().is_dir());
            assert!(started.diagnostic_directory().is_dir());
            let context_path = PathBuf::from(
                started
                    .environment()
                    .variable(std::ffi::OsStr::new(RECOVERY_CONTEXT_VARIABLE))
                    .unwrap(),
            );
            assert!(
                started
                    .environment()
                    .variable(std::ffi::OsStr::new("SCHERZO_INHERITED"))
                    .is_none()
            );
            let context = read_recovery_context(&fs::read(&context_path).unwrap()).unwrap();
            assert_eq!(context.target.id, "repair");
            assert_eq!(context.recovery_round, 1);
            assert_eq!(context.failed_execution.execution_number, 1);
            assert_eq!(context.failed_execution.invocation_id, 1);
            assert!(
                context
                    .diagnostics
                    .iter()
                    .all(|entry| entry.trust == "untrusted")
            );
            let handler_action = started.identity().invocation();
            let result_endpoint = started.result_endpoint_directory().to_owned();
            let diagnostic_directory = started.diagnostic_directory().to_owned();
            started.control().start().await.unwrap();
            started
                .control()
                .observe(AgentObservation::AssistantText {
                    text: Arc::from("I choose gave_up in prose, which is not authority."),
                })
                .await
                .unwrap();
            started
                .control()
                .observe(AgentObservation::Usage {
                    input_tokens: 7,
                    output_tokens: 3,
                })
                .await
                .unwrap();
            fs::write(
                fixture.execution_root.join("repaired.marker"),
                b"agent mutation",
            )
            .unwrap();
            started
                .control()
                .propose(ScriptedAgentValue::Result(Arc::new(json!({
                    "schemaVersion": 1,
                    "decision": "recheck",
                    "summary": "repaired workspace",
                    "reason": "run the unchanged target"
                }))))
                .await
                .unwrap();
            started.control().complete().await.unwrap();

            let result = execution.await.unwrap().unwrap();
            assert_eq!(result.outcome, RunOutcome::Succeeded);
            assert!(!context_path.exists());
            assert!(!result_endpoint.exists());
            assert!(diagnostic_directory.exists());
            assert_eq!(
                accounting.usage(handler_action),
                Some(InvocationUsage {
                    input_tokens: 7,
                    output_tokens: 3,
                })
            );
            let native = accounting.native_session(handler_action).unwrap();
            assert_eq!(native.profile, profile);
            assert_ne!(native.diagnostic_identity.as_ref(), "unavailable");
            match profile {
                AgentCompatibilityProfile::PiJsonV1
                | AgentCompatibilityProfile::ClaudeCodeStreamJsonV1 => {
                    assert!(native.native_session_identity.is_some());
                }
                AgentCompatibilityProfile::CodexAppServerV1 => {
                    assert!(native.native_session_identity.is_none());
                }
            }
            let observations = observations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert!(observations.iter().any(|observation| matches!(
                observation,
                ExecutionObservation::Agent(envelope)
                    if envelope.invocation() == handler_action
                        && matches!(envelope.observation(), AgentObservation::AssistantText { .. })
            )));
            assert_eq!(diagnostics.invocation_ids("repair").len(), 2);
        }
    })
    .await;
}

#[tokio::test]
async fn failed_agent_target_handler_and_recheck_use_three_fresh_accounted_invocations() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{}steps:
  repair:
    kind: agent
    recovery:
      retries: 1
      handler:
        kind: agent
        profile: recovery
        prompt: recovery.md
    agent:
      profile: recovery
      systemPrompt: target.md
      message:
        text:
          - file: target.md
"#,
            recovery_profile_source(AgentCompatibilityProfile::PiJsonV1)
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[
                ("target.md", b"Run the unchanged target protocol."),
                (
                    "recovery.md",
                    b"Repair the workspace and request a recheck.",
                ),
            ],
            ResolvedImports::default(),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            CancellationSource::new(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let accounting = InvocationAccountingLog::default();
        let agents = agent_runtime_with_accounting(&fixture, adapter, accounting.clone());
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

        let target_one = control.wait_until_started().await.unwrap();
        assert_eq!(target_one.value_kind(), AgentValueKind::None);
        assert_eq!(
            target_one.system_prompt(),
            "Run the unchanged target protocol."
        );
        let target_one_action = target_one.identity().invocation();
        let target_one_staging = target_one.result_endpoint_directory().to_owned();
        let target_one_diagnostics = target_one.diagnostic_directory().to_owned();
        target_one.control().start().await.unwrap();
        target_one
            .control()
            .observe(AgentObservation::Usage {
                input_tokens: 2,
                output_tokens: 1,
            })
            .await
            .unwrap();
        target_one
            .control()
            .fail(AgentFailureCause::HarnessProtocolFailed)
            .await
            .unwrap();

        let handler = control.wait_until_started().await.unwrap();
        assert!(!target_one_staging.exists());
        assert_eq!(handler.value_kind(), AgentValueKind::Result);
        assert_eq!(handler.system_prompt(), RECOVERY_AGENT_INSTRUCTIONS);
        let handler_action = handler.identity().invocation();
        let handler_staging = handler.result_endpoint_directory().to_owned();
        let handler_diagnostics = handler.diagnostic_directory().to_owned();
        handler.control().start().await.unwrap();
        handler
            .control()
            .observe(AgentObservation::Usage {
                input_tokens: 3,
                output_tokens: 2,
            })
            .await
            .unwrap();
        fs::write(
            fixture.execution_root.join("agent-repaired.marker"),
            b"visible",
        )
        .unwrap();
        handler
            .control()
            .propose(ScriptedAgentValue::Result(Arc::new(json!({
                "schemaVersion": 1,
                "decision": "recheck",
                "summary": "agent repaired workspace",
                "reason": "run the unchanged target"
            }))))
            .await
            .unwrap();
        handler.control().complete().await.unwrap();

        let target_two = control.wait_until_started().await.unwrap();
        assert!(!handler_staging.exists());
        assert_eq!(target_two.value_kind(), AgentValueKind::None);
        assert_eq!(
            target_two.system_prompt(),
            "Run the unchanged target protocol."
        );
        assert_eq!(target_two.message(), "Run the unchanged target protocol.");
        assert_eq!(
            fs::read(fixture.execution_root.join("agent-repaired.marker")).unwrap(),
            b"visible"
        );
        let target_two_action = target_two.identity().invocation();
        let target_two_diagnostics = target_two.diagnostic_directory().to_owned();
        assert_ne!(target_one_action, handler_action);
        assert_ne!(handler_action, target_two_action);
        assert_ne!(target_one_diagnostics, handler_diagnostics);
        assert_ne!(handler_diagnostics, target_two_diagnostics);
        target_two.control().start().await.unwrap();
        target_two
            .control()
            .observe(AgentObservation::Usage {
                input_tokens: 5,
                output_tokens: 4,
            })
            .await
            .unwrap();
        target_two.control().complete().await.unwrap();

        let result = execution.await.unwrap().unwrap();
        assert_eq!(result.outcome, RunOutcome::Succeeded);
        assert_eq!(
            accounting.usage(target_one_action),
            Some(InvocationUsage {
                input_tokens: 2,
                output_tokens: 1,
            })
        );
        assert_eq!(
            accounting.usage(handler_action),
            Some(InvocationUsage {
                input_tokens: 3,
                output_tokens: 2,
            })
        );
        assert_eq!(
            accounting.usage(target_two_action),
            Some(InvocationUsage {
                input_tokens: 5,
                output_tokens: 4,
            })
        );
        assert_eq!(accounting.recorded_invocations().len(), 3);
    })
    .await;
}

#[tokio::test]
async fn cancellation_waits_for_recovery_agent_quiescence_and_rejects_late_decision() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{}steps:
  repair:
    kind: cmd
    recovery:
      retries: 1
      handler:
        kind: agent
        profile: recovery
        prompt: recovery.md
    command:
      argv: [/bin/sh, -c, "exit 75"]
"#,
            recovery_profile_source(AgentCompatibilityProfile::PiJsonV1)
        );
        let cancellation = CancellationSource::new();
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("recovery.md", b"Wait for operator control.")],
            ResolvedImports::default(),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            cancellation.clone(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let diagnostics = StepDiagnosticLog::default();
        let execution_diagnostics = diagnostics.clone();
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
                    &execution_diagnostics,
                    agents,
                    TestClock,
                    NoopExecutionObserver,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        let context_path = PathBuf::from(
            started
                .environment()
                .variable(std::ffi::OsStr::new(RECOVERY_CONTEXT_VARIABLE))
                .unwrap(),
        );
        let result_endpoint = started.result_endpoint_directory().to_owned();
        started.control().start().await.unwrap();
        let mut barrier = started.control().block().unwrap();
        barrier.wait_until_blocked().await.unwrap();
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        assert!(
            !execution.is_finished(),
            "terminal cancellation must wait for the blocked adapter"
        );
        let late_control = started.control().clone();
        let late_decision = tokio::spawn(async move {
            late_control
                .propose(ScriptedAgentValue::Result(Arc::new(json!({
                    "schemaVersion": 1,
                    "decision": "recheck",
                    "summary": "too late",
                    "reason": "cancellation already owns authority"
                }))))
                .await
        });
        barrier.release().unwrap();
        assert!(late_decision.await.unwrap().is_err());

        let result = (&mut execution).await.unwrap().unwrap();
        assert_eq!(
            result.outcome,
            RunOutcome::Cancelled {
                reason: CancellationReason::UserRequest
            }
        );
        assert!(!context_path.exists());
        assert!(!result_endpoint.exists());
        assert_eq!(diagnostics.invocation_ids("repair").len(), 1);
    })
    .await;
}

#[tokio::test]
async fn semantic_outputs_atomic_mixed_success() {
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
        kind: text
        from: agent_response
      summary:
        kind: text
        from: path
        path: summary.txt
      data:
        kind: json
        from: path
        path: data.json
        schema: result.schema.json
      artifact:
        kind: file
        from: path
        path: agent.txt
        mediaType: text/plain
  consume:
    kind: cmd
    inputs:
      response:
        ref: outputs.produce.response
      summary:
        ref: outputs.produce.summary
      data:
        ref: outputs.produce.data
    command:
      argv: ["/bin/sh", "-c", "printf consumed > consumed.txt"]
    outputs:
      consumed:
        kind: file
        from: path
        path: consumed.txt
        mediaType: text/plain
exports:
  response:
    ref: outputs.produce.response
  summary:
    ref: outputs.produce.summary
  data:
    ref: outputs.produce.data
  artifact:
    ref: outputs.produce.artifact
  consumed:
    ref: outputs.consume.consumed
"#
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[
                ("prompt.md", b"produce the declared outputs"),
                (
                    "result.schema.json",
                    br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
                ),
            ],
            ResolvedImports::default(),
            EnvironmentSnapshot::default(),
            CancellationSource::new(),
            2,
            1024,
        );
        fs::write(fixture.execution_root.join("agent.txt"), b"agent artifact").unwrap();
        fs::write(
            fixture.execution_root.join("summary.txt"),
            b"\xef\xbb\xbfline one\r\nline two\n",
        )
        .unwrap();
        fs::write(
            fixture.execution_root.join("data.json"),
            br#"{ "z": 2, "a": 1 }"#,
        )
        .unwrap();
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
        assert_eq!(outputs.len(), 4);
        assert!(matches!(
            &outputs["response"],
            CapturedValue::Text(value) if value.as_ref() == "agent response"
        ));
        assert!(matches!(
            &outputs["summary"],
            CapturedValue::Text(value)
                if value.carrier() == b"\xef\xbb\xbfline one\r\nline two\n"
        ));
        assert!(matches!(
            &outputs["data"],
            CapturedValue::Json(value) if value.carrier() == br#"{"a":1,"z":2}"#
        ));
        let ExportValue::Available { output } = &result.exports["consumed"] else {
            panic!("downstream command output was unavailable");
        };
        let mut consumed = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut consumed)
            .unwrap();
        assert_eq!(consumed, b"consumed");
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
        kind: json
        from: agent_result
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
        from: path
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
async fn semantic_outputs_command_and_agent_path_matrix() {
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
        kind: text
        from: agent_response
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
        kind: json
        from: agent_result
        schema: result.schema.json
  cPath:
    kind: cmd
    command:
      argv: ["/bin/sh", "-c", "true"]
    outputs:
      text:
        kind: text
        from: path
        path: path.txt
      json:
        kind: json
        from: path
        path: path.json
        schema: result.schema.json
      artifact:
        kind: file
        from: path
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
          - ref: outputs.cPath.text
        attachments:
          - ref: outputs.bResult.result
          - ref: outputs.cPath.json
          - ref: outputs.cPath.artifact
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
        fs::write(
            fixture.execution_root.join("path.txt"),
            b"path text\r\nwith trailing newline\n",
        )
        .unwrap();
        fs::write(
            fixture.execution_root.join("path.json"),
            br#"{ "z": 2, "a": 1 }"#,
        )
        .unwrap();
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
        assert_eq!(
            consumer.message(),
            "response exact\n\npath text\r\nwith trailing newline\n"
        );
        assert_eq!(consumer.attachments().len(), 3);
        assert_eq!(consumer.attachments()[0].media_type(), "application/json");
        assert_eq!(
            fs::read(consumer.attachments()[0].path()).unwrap(),
            br#"{"a":1,"z":2}"#
        );
        assert_eq!(consumer.attachments()[1].media_type(), "application/json");
        assert_eq!(
            fs::read(consumer.attachments()[1].path()).unwrap(),
            br#"{"a":1,"z":2}"#
        );
        assert_eq!(consumer.attachments()[2].media_type(), "text/plain");
        assert_eq!(
            fs::read(consumer.attachments()[2].path()).unwrap(),
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
async fn semantic_outputs_atomic_mixed_failure() {
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
        kind: text
        from: agent_response
      missing:
        kind: file
        from: path
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
        let StepState::Failed {
            phase: FailurePhase::OutputCapture,
            cause:
                StepFailureCause::OutputCapture(
                    super::super::step_runtime::OutputCaptureFailure::Capture(failure),
                ),
        } = &result.steps["produce"]
        else {
            panic!("agent producer did not report the exact capture failure");
        };
        assert_eq!(failure.output_identity(), "missing");
        assert_eq!(
            failure.kind(),
            crate::execution::workflow::artifact::CaptureFailureKind::Missing
        );
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
        assert_eq!(fixture.artifacts.staged_artifact_count(), 0);
        assert_eq!(fixture.artifacts.budget_usage(), (0, 0));
        assert_eq!(fixture.artifacts.reservation_usage(), (0, 0));
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
        kind: text
        from: agent_response
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
        kind: text
        from: agent_response
      artifact:
        kind: file
        from: path
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
        kind: text
        from: agent_response
      artifact:
        kind: file
        from: path
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
                &result.steps["task"],
                StepState::Failed {
                    phase: FailurePhase::Start,
                    cause: StepFailureCause::Start(StepStartFailure::Agent(failure)),
                } if matches!(failure.cause(), AgentFailureCause::HarnessStartFailed)
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
