use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::future::{Future, ready};
use std::io::{BufRead, Read as _, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::unix::fs::{PermissionsExt as _, symlink};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rustix::process::Pid;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};

use super::adapter::{CodexAppServerV1Adapter, prepare_launch};
use super::*;
use crate::execution::workflow::admission::{
    CancellationReason, CancellationSource, EnvironmentSnapshot,
};
use crate::execution::workflow::agent::{
    AdmittedAgentAdapter, AgentCompatibilityProfile, AgentInvocation, AgentInvocationIdentity,
    AgentInvocationLimits, AgentInvocationStaging, AgentObservationEnvelope, AgentObservationSink,
    AgentProcessContext, AgentProcessControl, AgentPrompt, AgentStartReceiver,
    AgentTerminalReceiver, AgentValueMode, PositiveDuration, RetainedResultSchema,
    StagedAgentAttachment, WorkflowRunId, agent_start_channel, agent_terminal_channel,
    invoke_agent_adapter,
};
use crate::execution::workflow::agent_diagnostics::AgentDiagnosticSession;
use crate::execution::workflow::codex::CodexConfig;
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::execution_root::AdmittedExecutionRoot;
use crate::execution::workflow::observation::NoopExecutionObserver;
use crate::execution::workflow::process_group::{ProcessGuardRegistry, process_group_is_quiescent};
use crate::execution::workflow::result_validation::{
    ResultValidationWorker, RunningResultValidation, ValidationWorkerDecision,
    ValidationWorkerRequest,
};
use crate::execution::workflow::runtime::{ActionId, TransitionSequence};

const MODEL: &str = "scherzo-loopback";
const PROVIDER: &str = "loopback";
const RESPONSE: &str = "driver response";
const THREAD_ID: &str = "018f7f1e-7b5a-7d13-8f19-2b6a4c8d0e12";
const TURN_ID: &str = "turn-fixture";
const CORRECTION_TURN_ID: &str = "turn-correction";
const PLACEHOLDER_KEY: &str = "scherzo-loopback-placeholder";
const FAKE_CODEX: &str = r#"#!/bin/sh
set -eu
for argument in "$@"; do
  printf '%s\0' "$argument"
  case "$argument" in
    sqlite_home=\"*\")
      CODEX_FIXTURE_SQLITE_HOME=${argument#sqlite_home=\"}
      CODEX_FIXTURE_SQLITE_HOME=${CODEX_FIXTURE_SQLITE_HOME%\"}
      export CODEX_FIXTURE_SQLITE_HOME
      ;;
  esac
done > "$CODEX_FIXTURE_ARGUMENTS"
printf 'bounded Codex fixture diagnostic\n' >&2
exec "$CODEX_FIXTURE_HELPER" \
  --exact execution::workflow::codex_app_server_v1::adapter_tests::codex_process_fixture \
  --ignored --test-threads=1 \
  3>&1 >/dev/null
"#;

#[derive(Clone, Copy)]
struct PendingClock;

impl CoordinatorClock for PendingClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, _deadline: Self::Instant) {
        std::future::pending().await
    }
}

#[derive(Clone)]
struct ReleasedClock {
    deadlines: mpsc::UnboundedSender<Duration>,
    release: watch::Receiver<bool>,
}

// This clock carries Codex-specific stdin-deadline synchronization; sharing it with
// another profile's fixture would couple independent protocol timing contracts.
// jscpd:ignore-start
impl CoordinatorClock for ReleasedClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        let _ = self.deadlines.send(deadline);
        let mut release = self.release.clone();
        while !*release.borrow_and_update() {
            if release.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}
// jscpd:ignore-end

#[derive(Clone)]
struct ControlledClock {
    deadlines: mpsc::UnboundedSender<Duration>,
    expired: watch::Receiver<bool>,
}

struct ClockControl {
    deadlines: mpsc::UnboundedReceiver<Duration>,
    expired: watch::Sender<bool>,
}

impl ControlledClock {
    fn new() -> (Self, ClockControl) {
        let (deadlines, registrations) = mpsc::unbounded_channel();
        let (expired, expiration) = watch::channel(false);
        (
            Self {
                deadlines,
                expired: expiration,
            },
            ClockControl {
                deadlines: registrations,
                expired,
            },
        )
    }
}

// This clock records Codex result validation and settlement deadlines independently
// from other harness fixtures so their synchronization cannot become coupled.
// jscpd:ignore-start
impl CoordinatorClock for ControlledClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        let _ = self.deadlines.send(deadline);
        let mut expired = self.expired.clone();
        while !*expired.borrow_and_update() {
            if expired.changed().await.is_err() {
                return;
            }
        }
    }
}
// jscpd:ignore-end
#[derive(Clone, Default)]
struct RecordingObservationSink(Arc<Mutex<Vec<AgentObservationEnvelope>>>);

impl RecordingObservationSink {
    fn snapshot(&self) -> Vec<AgentObservationEnvelope> {
        self.0.lock().unwrap().clone()
    }
}

fn assert_last_observation_is_quiescent(observations: &[AgentObservationEnvelope]) {
    assert!(matches!(
        observations
            .last()
            .map(AgentObservationEnvelope::observation),
        Some(AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::HarnessQuiescent,
        })
    ));
}

fn assert_started_failure(
    scenario: &str,
    outcome: AgentOutcome,
    started: bool,
    expected: AgentFailureCause,
) {
    assert!(started, "{scenario}: {outcome:?}");
    assert_failure_cause(outcome, expected, scenario);
}

fn assert_failure_cause(outcome: AgentOutcome, expected: AgentFailureCause, scenario: &str) {
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("{scenario}: expected failure");
    };
    assert_eq!(failure.cause(), &expected, "{scenario}");
}

// This sink belongs to the Codex process fixture so its observations and synchronization
// cannot be accidentally shared with another native conformance harness.
// jscpd:ignore-start
impl AgentObservationSink for RecordingObservationSink {
    fn observe(&self, observation: AgentObservationEnvelope) -> impl Future<Output = ()> + Send {
        self.0.lock().unwrap().push(observation);
        ready(())
    }
}
// jscpd:ignore-end

type TestInvocation =
    AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, RecordingObservationSink>;

// Keep the authoritative validator's inline worker local to the Codex process fixture;
// sharing another native adapter's worker would couple otherwise independent transcripts.
// jscpd:ignore-start
#[derive(Clone, Copy)]
struct InlineValidationWorker;

impl ResultValidationWorker for InlineValidationWorker {
    type Running = ReadyValidation;

    fn start(&self, request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        Ok(ReadyValidation {
            decision: Some(request.evaluate()),
        })
    }
}

struct ReadyValidation {
    decision: Option<Result<ValidationWorkerDecision, ()>>,
}

impl RunningResultValidation for ReadyValidation {
    fn wait(&mut self) -> impl Future<Output = Result<ValidationWorkerDecision, ()>> + Send {
        ready(self.decision.take().unwrap())
    }

    fn request_stop(&mut self) {}

    fn quiesce(self) -> impl Future<Output = ()> + Send {
        ready(())
    }
}
// jscpd:ignore-end

struct ProcessFixture {
    _temporary: tempfile::TempDir,
    invocation: Option<TestInvocation>,
    observations: RecordingObservationSink,
    diagnostics: StepDiagnosticLog,
    executable: PathBuf,
    arguments: PathBuf,
    requests: PathBuf,
    process: PathBuf,
    ready: PathBuf,
    proceed: PathBuf,
    descendant: PathBuf,
    codex_home: PathBuf,
    diagnostic_session: PathBuf,
    sqlite_home: PathBuf,
    expected_cwd: PathBuf,
}

impl ProcessFixture {
    fn new(scenario: &str, value_mode: AgentValueMode, maximum_response_bytes: u64) -> Self {
        Self::with_provider(scenario, value_mode, maximum_response_bytes, None)
    }

    fn with_provider(
        scenario: &str,
        value_mode: AgentValueMode,
        maximum_response_bytes: u64,
        provider_address: Option<std::net::SocketAddr>,
    ) -> Self {
        Self::with_provider_and_attachments(
            scenario,
            value_mode,
            maximum_response_bytes,
            provider_address,
            &[],
        )
    }

    fn with_attachments(attachments: &[(&[u8], &str, &str)]) -> Self {
        Self::with_provider_and_attachments("absent", AgentValueMode::None, 1024, None, attachments)
    }

    fn with_provider_and_attachments(
        scenario: &str,
        value_mode: AgentValueMode,
        maximum_response_bytes: u64,
        provider_address: Option<std::net::SocketAddr>,
        attachments: &[(&[u8], &str, &str)],
    ) -> Self {
        // Codex owns fresh native process, control, and state roots; sharing another
        // harness fixture would invalidate profile-specific persistence evidence.
        // jscpd:ignore-start
        let temporary = tempfile::tempdir().unwrap();
        let temporary_root = std::fs::canonicalize(temporary.path()).unwrap();
        let execution_root = temporary_root.join("execution");
        let cwd = execution_root.join("worktree");
        let staging = temporary_root.join("staging");
        let attachment_directory = staging.join("attachments");
        let result_endpoint = staging.join("result-endpoint");
        let controls = temporary_root.join("controls");
        let home = temporary_root.join("home");
        let codex_home = temporary_root.join("codex-home");
        let diagnostic_session = temporary_root.join("diagnostics/session");
        for directory in [
            &cwd,
            &staging,
            &attachment_directory,
            &result_endpoint,
            &controls,
            &home,
            &codex_home,
        ] {
            std::fs::create_dir_all(directory).unwrap();
        }
        // jscpd:ignore-end
        std::fs::write(cwd.join("AGENTS.md"), b"root resource marker\n").unwrap();
        std::fs::create_dir_all(cwd.join("nested")).unwrap();
        std::fs::write(cwd.join("nested/AGENTS.md"), b"nested resource marker\n").unwrap();
        let executable = temporary_root.join("codex");
        if scenario != "launch-failure" {
            std::fs::write(&executable, FAKE_CODEX).unwrap();
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let arguments = controls.join("arguments");
        let requests = controls.join("requests.jsonl");
        let process = controls.join("process.pid");
        let ready = controls.join("ready");
        let proceed = controls.join("proceed");
        let descendant = controls.join("descendant.pid");
        // Codex's exact process fixture owns its synthetic environment and controls;
        // sharing another profile's fixture would blur native launch evidence.
        // jscpd:ignore-start
        let mut environment = BTreeMap::from([
            (
                OsString::from("PATH"),
                std::env::var_os("PATH").unwrap_or_else(|| OsString::from("/usr/bin:/bin")),
            ),
            (OsString::from("HOME"), home.into_os_string()),
            (
                OsString::from("CODEX_HOME"),
                codex_home.as_os_str().to_owned(),
            ),
            (
                OsString::from("CODEX_FIXTURE_SQLITE_HOME_PATH"),
                result_endpoint.as_os_str().to_owned(),
            ),
            (
                OsString::from("CODEX_FIXTURE_HELPER"),
                std::env::current_exe().unwrap().into_os_string(),
            ),
            (
                OsString::from("CODEX_FIXTURE_ARGUMENTS"),
                arguments.as_os_str().to_owned(),
            ),
            (
                OsString::from("CODEX_FIXTURE_REQUESTS"),
                requests.as_os_str().to_owned(),
            ),
            (
                OsString::from("CODEX_FIXTURE_PROCESS"),
                process.as_os_str().to_owned(),
            ),
            (
                OsString::from("CODEX_FIXTURE_READY"),
                ready.as_os_str().to_owned(),
            ),
            (
                OsString::from("CODEX_FIXTURE_PROCEED"),
                proceed.as_os_str().to_owned(),
            ),
            (
                OsString::from("CODEX_FIXTURE_DESCENDANT"),
                descendant.as_os_str().to_owned(),
            ),
            (
                OsString::from("CODEX_FIXTURE_SCENARIO"),
                OsString::from(scenario),
            ),
            (
                OsString::from("CODEX_FIXTURE_RESPONSE"),
                OsString::from(match scenario {
                    "exact-limit"
                    | "failure-after-output"
                    | "interruption-after-output"
                    | "nonzero-after-output" => "12345",
                    "oversized" => "123456",
                    _ => RESPONSE,
                }),
            ),
        ]);
        if let Some(address) = provider_address {
            environment.insert(
                OsString::from("CODEX_FIXTURE_PROVIDER_ADDRESS"),
                OsString::from(address.to_string()),
            );
            environment.insert(
                OsString::from("CODEX_API_KEY"),
                OsString::from(PLACEHOLDER_KEY),
            );
        }
        // jscpd:ignore-end
        // The Codex fixture stages exact ordered identities for localImage and sealed-path
        // assertions rather than sharing another native transport's attachment setup.
        // jscpd:ignore-start
        let staged_attachments = attachments
            .iter()
            .enumerate()
            .map(|(index, (bytes, media_type, diagnostic_name))| {
                let path = attachment_directory.join(format!("{index:06}"));
                std::fs::write(&path, bytes).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();
                StagedAgentAttachment::new(
                    path,
                    Arc::from(*media_type),
                    Some(Arc::from(*diagnostic_name)),
                )
            })
            .collect::<Vec<_>>();
        // The production staging directory remains owner-writable while each payload is
        // read-only, so fixture teardown can remove invocation-owned attachment bytes.
        std::fs::set_permissions(
            &attachment_directory,
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        // jscpd:ignore-end
        let admitted_root = AdmittedExecutionRoot::admit(&execution_root).unwrap();
        let working_directory = admitted_root
            .select_working_directory(Some("worktree"))
            .unwrap();
        let expected_cwd = working_directory.protocol_path().unwrap();
        let observations = RecordingObservationSink::default();
        let diagnostics = StepDiagnosticLog::default();
        // The fixture deliberately materializes the full Codex invocation contract; sharing
        // this wiring would couple its profile, staging, and limits to another harness test.
        // jscpd:ignore-start
        let invocation = AgentInvocation::new(
            AgentInvocationIdentity::new(
                WorkflowRunId::from(Arc::from("run-codex-fixture")),
                Arc::from("agent-step"),
                ActionId {
                    transition_sequence: TransitionSequence::default(),
                },
            ),
            AdmittedAgentAdapter::new(
                AgentCompatibilityProfile::CodexAppServerV1,
                executable.clone(),
                Arc::from("0.147.0"),
                CodexConfig {
                    model: MODEL.to_owned(),
                    effort: "high".to_owned(),
                },
            ),
            AgentProcessContext::new(working_directory, EnvironmentSnapshot::new(environment)),
            AgentInvocationStaging::new(result_endpoint.clone()),
            AgentDiagnosticSession::codex_fixture(diagnostic_session.clone()),
            AgentPrompt::new(
                Arc::from("scherzo system instructions"),
                Arc::from("ordinary user turn"),
            ),
            Arc::from(staged_attachments),
            value_mode,
            invocation_limits(
                maximum_response_bytes,
                if scenario == "result-oversized" {
                    64
                } else {
                    1024
                },
            ),
            CancellationSource::new(),
            ProcessGuardRegistry::default(),
            observations.clone(),
        );
        // jscpd:ignore-end
        Self {
            _temporary: temporary,
            invocation: Some(invocation),
            observations,
            diagnostics,
            executable,
            arguments,
            requests,
            process,
            ready,
            proceed,
            descendant,
            codex_home,
            diagnostic_session,
            sqlite_home: result_endpoint,
            expected_cwd,
        }
    }

    fn with_exact_binary(provider_address: std::net::SocketAddr) -> Self {
        let fixture = Self::with_provider(
            "exact-binary",
            response_mode(),
            1024,
            Some(provider_address),
        );
        let exact = PathBuf::from(
            std::env::var_os("SCHERZO_CODEX_APP_SERVER_CONFORMANCE_EXECUTABLE").unwrap(),
        );
        std::fs::remove_file(&fixture.executable).unwrap();
        symlink(exact, &fixture.executable).unwrap();
        let config = format!(
            "model_provider = \"loopback\"\n\
             [model_providers.loopback]\n\
             name = \"Scherzo loopback\"\n\
             base_url = \"http://{provider_address}\"\n\
             env_key = \"CODEX_API_KEY\"\n\
             wire_api = \"responses\"\n\
             request_max_retries = 0\n\
             stream_max_retries = 0\n"
        );
        std::fs::write(fixture.codex_home.join("config.toml"), config).unwrap();
        fixture
    }

    fn ambient_rollout(&self) -> PathBuf {
        self.codex_home.join(format!(
            "sessions/2026/08/18/rollout-2026-08-18T00-00-00-{THREAD_ID}.jsonl"
        ))
    }

    fn retained_rollout(&self) -> PathBuf {
        self.diagnostic_session.join("rollout.jsonl")
    }

    fn thread_correlation(&self) -> PathBuf {
        self.diagnostic_session.join("thread.json")
    }

    fn protocol_rejection(&self) -> PathBuf {
        self.diagnostic_session
            .parent()
            .unwrap()
            .join("protocol-rejection.json")
    }
}

fn response_mode() -> AgentValueMode {
    AgentValueMode::Response {
        output: Arc::from("response"),
    }
}

fn result_mode(schema: Value) -> AgentValueMode {
    let bytes = Arc::<[u8]>::from(serde_json::to_vec(&schema).unwrap());
    AgentValueMode::Result {
        output: Arc::from("result"),
        schema: RetainedResultSchema::compile(bytes, Arc::new(schema)).unwrap(),
    }
}

// These limits deliberately materialize the complete Codex fixture envelope rather than
// inheriting another profile's protocol-limit type or test defaults.
// jscpd:ignore-start
fn invocation_limits(
    maximum_response_bytes: u64,
    maximum_result_bytes: u64,
) -> AgentInvocationLimits<CodexAppServerV1ProtocolLimits> {
    AgentInvocationLimits::new(
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroUsize::new(16).unwrap(),
        NonZeroU64::new(4096).unwrap(),
        NonZeroU64::new(maximum_response_bytes).unwrap(),
        NonZeroU64::new(maximum_result_bytes).unwrap(),
        NonZeroU64::new(512).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        CodexAppServerV1ProtocolLimits::profile(),
    )
}
// jscpd:ignore-end

// Codex fixture startup selects its test-only provider and exact terminal channel locally.
// jscpd:ignore-start
fn start_fixture(
    invocation: TestInvocation,
    diagnostics: StepDiagnosticLog,
) -> (
    tokio::task::JoinHandle<()>,
    AgentStartReceiver,
    AgentTerminalReceiver,
) {
    start_fixture_with_clock(invocation, diagnostics, PendingClock)
}

fn start_fixture_with_clock<Clock: CoordinatorClock>(
    invocation: TestInvocation,
    diagnostics: StepDiagnosticLog,
    clock: Clock,
) -> (
    tokio::task::JoinHandle<()>,
    AgentStartReceiver,
    AgentTerminalReceiver,
) {
    let value_mode = invocation.value_mode().clone();
    let adapter = CodexAppServerV1Adapter::with_validation_worker(
        diagnostics,
        NonZeroU64::new(1024).unwrap(),
        clock,
        NoopExecutionObserver,
        InlineValidationWorker,
        Some(Arc::from(PROVIDER)),
    );
    let (started, start) = agent_start_channel();
    let (terminal, outcome) = agent_terminal_channel(&value_mode);
    let task = tokio::spawn(async move {
        invoke_agent_adapter(&adapter, invocation, started, terminal).await;
    });
    (task, start, outcome)
}
// jscpd:ignore-end

// Process-record inspection is specific to this guarded App Server fixture's quiescence
// proof, so it remains separate from other harness fixture runners.
// jscpd:ignore-start
async fn run_fixture(mut fixture: ProcessFixture) -> (ProcessFixture, AgentOutcome, bool) {
    let invocation = fixture.invocation.take().unwrap();
    let (task, start, outcome) = start_fixture(invocation, fixture.diagnostics.clone());
    task.await.unwrap();
    let outcome = outcome.receive().await.unwrap();
    let started = start.receive().await.is_ok();
    if fixture.process.is_file() {
        let process = fixture_process(&fixture.process);
        assert!(process_group_is_quiescent(process));
    }
    (fixture, outcome, started)
}
// jscpd:ignore-end

async fn run_response_process(
    scenario: &str,
    maximum_response_bytes: u64,
) -> (ProcessFixture, AgentOutcome, bool) {
    run_fixture(ProcessFixture::new(
        scenario,
        response_mode(),
        maximum_response_bytes,
    ))
    .await
}

fn assert_fixture_quiescent(fixture: &ProcessFixture) {
    assert!(process_group_is_quiescent(fixture_process(
        &fixture.process
    )));
}

fn assert_rollout_retained(fixture: &ProcessFixture) {
    assert!(fixture.retained_rollout().is_file());
    assert!(!contains_rollout(&fixture.codex_home));
    let correlation: Value =
        serde_json::from_slice(&std::fs::read(fixture.thread_correlation()).unwrap()).unwrap();
    assert!(
        correlation["threadId"]
            .as_str()
            .is_some_and(super::is_codex_thread_id)
    );
    assert_eq!(
        correlation["nativeRollout"]["relativeFile"],
        "rollout.jsonl"
    );
    assert!(
        !fixture
            .diagnostic_session
            .join("rollout-rejection.json")
            .exists()
    );
}

fn contains_rollout(directory: &Path) -> bool {
    std::fs::read_dir(directory).is_ok_and(|entries| {
        entries.filter_map(Result::ok).any(|entry| {
            let path = entry.path();
            if path.is_dir() {
                contains_rollout(&path)
            } else {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .is_some_and(|name| name.starts_with("rollout-") && name.ends_with(".jsonl"))
            }
        })
    })
}

fn fixture_process(path: &Path) -> Pid {
    let raw = std::fs::read_to_string(path)
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    Pid::from_raw(raw).unwrap()
}

fn captured_requests(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time bounds fixture hangs but never makes an assertion safe"
)]
async fn with_watchdog<Output>(future: impl Future<Output = Output>) -> Output {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("CodexAppServerV1 fixture watchdog expired")
}

// The child-guard process has no in-process readiness channel. This bounded OS-boundary
// poll observes an explicit fixture file; the watchdog is only an anti-hang bound.
#[expect(
    clippy::disallowed_methods,
    reason = "an explicit cross-process file is the readiness event; the delay only spaces OS polls"
)]
async fn wait_for_fixture_file(path: &Path) {
    while !path.is_file() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn write_server_frame(output: &mut impl Write, value: Value) {
    serde_json::to_writer(&mut *output, &value).unwrap();
    output.write_all(b"\n").unwrap();
    output.flush().unwrap();
}

fn read_client_frame(input: &mut impl BufRead, capture: &mut impl Write) -> Value {
    let mut line = String::new();
    assert!(input.read_line(&mut line).unwrap() > 0);
    capture.write_all(line.as_bytes()).unwrap();
    capture.flush().unwrap();
    serde_json::from_str(line.trim_end()).unwrap()
}

fn expect_client_eof(input: &mut impl std::io::Read) {
    let mut trailing = Vec::new();
    input.read_to_end(&mut trailing).unwrap();
    assert!(trailing.is_empty());
}

fn native_rollout_path() -> PathBuf {
    if std::env::var("CODEX_FIXTURE_SCENARIO").as_deref() == Ok("rollout-outside") {
        return PathBuf::from(std::env::var_os("CODEX_FIXTURE_REQUESTS").unwrap())
            .parent()
            .unwrap()
            .join("attacker-rollout.jsonl");
    }
    PathBuf::from(std::env::var_os("CODEX_HOME").unwrap()).join(format!(
        "sessions/2026/08/18/rollout-2026-08-18T00-00-00-{THREAD_ID}.jsonl"
    ))
}

fn thread_document(cwd: &str) -> Value {
    json!({
        "id": THREAD_ID,
        "sessionId": THREAD_ID,
        "forkedFromId": null,
        "parentThreadId": null,
        "ephemeral": false,
        "path": native_rollout_path(),
        "cliVersion": "0.147.0",
        "turns": [],
        "cwd": cwd,
        "modelProvider": PROVIDER,
    })
}

fn materialize_native_rollout(cwd: &str, scenario: &str) {
    let path = native_rollout_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let retained_thread_id = if scenario == "rollout-identity-mismatch" {
        "018f7f1e-7b5a-7d13-8f19-2b6a4c8d0e13"
    } else {
        THREAD_ID
    };
    let session_meta = json!({
        "timestamp": "2026-08-18T00:00:00Z",
        "type": "session_meta",
        "payload": {
            "id": retained_thread_id,
            "session_id": retained_thread_id,
            "cwd": cwd,
            "cli_version": "0.147.0",
        }
    });
    let bytes = format!(
        "{}\n{{\"fixture\":{scenario:?}}}\n",
        serde_json::to_string(&session_meta).unwrap()
    );
    if scenario == "rollout-symlink" {
        let target = PathBuf::from(std::env::var_os("CODEX_FIXTURE_REQUESTS").unwrap())
            .parent()
            .unwrap()
            .join("attacker-rollout.jsonl");
        std::fs::write(&target, bytes).unwrap();
        symlink(target, path).unwrap();
    } else {
        std::fs::write(path, bytes).unwrap();
    }
}

fn turn_document(status: &str, items: Vec<Value>) -> Value {
    json!({"id": TURN_ID, "items": items, "status": status})
}

fn send_item_started(output: &mut impl Write, id: &str, kind: &str, extra: Value) {
    let mut item = json!({"id": id, "type": kind});
    if let (Some(item), Some(extra)) = (item.as_object_mut(), extra.as_object()) {
        item.extend(extra.clone());
    }
    write_server_frame(
        output,
        json!({
            "method": "item/started",
            "params": {"threadId": THREAD_ID, "turnId": TURN_ID, "item": item}
        }),
    );
}

fn send_item_completed(output: &mut impl Write, item: Value) {
    write_server_frame(
        output,
        json!({
            "method": "item/completed",
            "params": {"threadId": THREAD_ID, "turnId": TURN_ID, "item": item}
        }),
    );
}

fn send_native_error(
    output: &mut impl Write,
    message: &str,
    codex_error_info: Value,
    will_retry: bool,
) {
    write_server_frame(
        output,
        json!({
            "method": "error",
            "params": {
                "threadId": THREAD_ID,
                "turnId": TURN_ID,
                "error": {
                    "message": message,
                    "codexErrorInfo": codex_error_info,
                },
                "willRetry": will_retry,
            }
        }),
    );
}

fn completed_provisional_response(output: &mut impl Write) -> Value {
    send_item_started(
        output,
        "message-1",
        "agentMessage",
        json!({"text": "", "phase": null}),
    );
    let item = json!({
        "id": "message-1",
        "type": "agentMessage",
        "text": "provisional response",
        "phase": "final_answer",
    });
    send_item_completed(output, item.clone());
    item
}

fn send_turn_terminal(
    output: &mut impl Write,
    status: &str,
    items: Vec<Value>,
    error: Option<(&str, Value)>,
) {
    let mut turn = turn_document(status, items);
    if let (Some(turn), Some((message, info))) = (turn.as_object_mut(), error) {
        turn.insert(
            "error".to_owned(),
            json!({"message": message, "codexErrorInfo": info}),
        );
    }
    write_server_frame(
        output,
        json!({
            "method": "turn/completed",
            "params": {"threadId": THREAD_ID, "turn": turn}
        }),
    );
}

fn send_result_turn(
    output: &mut impl Write,
    turn_id: &str,
    item_id: &str,
    candidate: Option<&str>,
    status: &str,
) {
    let mut items = Vec::new();
    if let Some(candidate) = candidate {
        write_server_frame(
            output,
            json!({
                "method": "item/started",
                "params": {
                    "threadId": THREAD_ID,
                    "turnId": turn_id,
                    "item": {"id": item_id, "type": "agentMessage", "text": ""},
                },
            }),
        );
        let item = json!({
            "id": item_id,
            "type": "agentMessage",
            "text": candidate,
            "phase": "final_answer",
        });
        write_server_frame(
            output,
            json!({
                "method": "item/completed",
                "params": {
                    "threadId": THREAD_ID,
                    "turnId": turn_id,
                    "item": item,
                },
            }),
        );
        items.push(item);
    }
    write_server_frame(
        output,
        json!({
            "method": "turn/completed",
            "params": {
                "threadId": THREAD_ID,
                "turn": {"id": turn_id, "items": items, "status": status},
            },
        }),
    );
}
fn provider_response(thread: &Value) -> String {
    let address = std::env::var("CODEX_FIXTURE_PROVIDER_ADDRESS").unwrap();
    let instructions = thread["params"]["developerInstructions"].as_str().unwrap();
    let body = serde_json::to_vec(&json!({
        "model": MODEL,
        "instructions": instructions,
        "input": [
            {"role": "developer", "content": "root resource marker"},
            {"role": "developer", "content": "nested resource marker"},
            {"role": "developer", "content": "skill resource marker"},
        ],
        "tools": [{"type": "function", "name": "native_mcp_tool"}],
        "stream": true,
    }))
    .unwrap();
    let mut stream = std::net::TcpStream::connect(address).unwrap();
    write!(
        stream,
        "POST /responses HTTP/1.1\r\nhost: loopback\r\nauthorization: Bearer {PLACEHOLDER_KEY}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(&body).unwrap();
    stream.flush().unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    let (_, payload) = response.split_once("\r\n\r\n").unwrap();
    let completed = payload
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["type"] == "response.output_item.done")
        .unwrap();
    completed["item"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[test]
#[ignore = "launched as the deterministic Codex App Server process fixture"]
#[expect(
    clippy::zombie_processes,
    reason = "the authenticated fixture guard force-terminates and reaps this deliberate stubborn descendant"
)]
fn codex_process_fixture() {
    let scenario = std::env::var("CODEX_FIXTURE_SCENARIO").unwrap();
    std::fs::write(
        std::env::var_os("CODEX_FIXTURE_PROCESS").unwrap(),
        format!("{}\n", std::process::id()),
    )
    .unwrap();
    if scenario == "stderr-flood" {
        std::io::stderr().write_all(&vec![b'x'; 4096]).unwrap();
        std::io::stderr().flush().unwrap();
    }
    let mut capture =
        std::fs::File::create(std::env::var_os("CODEX_FIXTURE_REQUESTS").unwrap()).unwrap();
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/fd/3")
        .unwrap();

    let initialize = read_client_frame(&mut input, &mut capture);
    assert_eq!(initialize["id"], 1);
    if scenario == "cancel-before-initialize" {
        std::fs::write(std::env::var_os("CODEX_FIXTURE_READY").unwrap(), b"ready\n").unwrap();
        expect_client_eof(&mut input);
        return;
    }
    if scenario == "initialize-eof" {
        return;
    }
    if scenario == "initialize-rejected" {
        write_server_frame(
            &mut output,
            json!({"id": 1, "error": {"code": -32602, "message": "rejected"}}),
        );
        return;
    }
    write_server_frame(
        &mut output,
        json!({
            "id": 1,
            "result": {
                "userAgent": "codex/0.147.0",
                "codexHome": std::env::var("CODEX_HOME").unwrap(),
            }
        }),
    );

    let initialized = read_client_frame(&mut input, &mut capture);
    assert_eq!(initialized["method"], "initialized");
    let config_read = read_client_frame(&mut input, &mut capture);
    assert_eq!(config_read["id"], 2);
    if scenario == "config-read-rejected" {
        write_server_frame(
            &mut output,
            json!({"id": 2, "error": {"code": -32603, "message": "config failed"}}),
        );
        return;
    }
    write_server_frame(
        &mut output,
        json!({
            "method": "configWarning",
            "params": {"summary": "synthetic effective configuration warning"}
        }),
    );
    write_server_frame(
        &mut output,
        json!({
            "id": 2,
            "result": {
                "config": {
                    "developer_instructions": "native developer instructions",
                    "sqlite_home": std::env::var("CODEX_FIXTURE_SQLITE_HOME").unwrap(),
                    "model_provider": PROVIDER,
                    "model_providers": {"loopback": {"wire_api": "responses"}},
                    "projects": {"fixture-project": {"trust_level": "trusted"}},
                    "hooks": {"enabled": true},
                    "mcp_servers": {"native": {"required": true}},
                    "skills": {"enabled": true}
                },
                "origins": {"developer_instructions": {"name": {"type": "user"}}},
                "layers": [{"name": {"type": "user"}}]
            }
        }),
    );
    if scenario == "sqlite-home-replaced" {
        let sqlite_home =
            PathBuf::from(std::env::var_os("CODEX_FIXTURE_SQLITE_HOME_PATH").unwrap());
        let displaced = sqlite_home.with_file_name("displaced-sqlite-home");
        std::fs::rename(&sqlite_home, &displaced).unwrap();
        symlink(std::env::var_os("CODEX_HOME").unwrap(), &sqlite_home).unwrap();
        std::fs::write(
            PathBuf::from(std::env::var_os("CODEX_FIXTURE_SQLITE_HOME").unwrap())
                .join("state_5.sqlite"),
            b"transient resumable state\n",
        )
        .unwrap();
    }

    let thread = read_client_frame(&mut input, &mut capture);
    assert_eq!(thread["id"], 3);
    let cwd = thread["params"]["cwd"].as_str().unwrap();
    if scenario == "cancel-during-thread-start" {
        std::fs::write(std::env::var_os("CODEX_FIXTURE_READY").unwrap(), b"ready\n").unwrap();
        expect_client_eof(&mut input);
        return;
    }
    if scenario == "thread-start-rejected" {
        write_server_frame(
            &mut output,
            json!({"id": 3, "error": {"code": -32603, "message": "thread failed"}}),
        );
        return;
    }
    write_server_frame(
        &mut output,
        json!({
            "id": 3,
            "result": {
                "thread": thread_document(cwd),
                "model": MODEL,
                "modelProvider": PROVIDER,
                "cwd": cwd,
                "approvalPolicy": "never",
                "sandbox": {"type": "dangerFullAccess"}
            }
        }),
    );

    let turn = read_client_frame(&mut input, &mut capture);
    assert_eq!(turn["id"], 4);
    if scenario == "turn-start-rejected" {
        write_server_frame(
            &mut output,
            json!({"id": 4, "error": {"code": -32603, "message": "turn failed"}}),
        );
        return;
    }
    materialize_native_rollout(cwd, &scenario);
    write_server_frame(
        &mut output,
        json!({"method": "thread/started", "params": {"thread": thread_document(cwd)}}),
    );
    if scenario == "premature-turn-started" {
        write_server_frame(
            &mut output,
            json!({"method": "turn/started", "params": {
                "threadId": THREAD_ID,
                "turn": turn_document("inProgress", vec![])
            }}),
        );
        return;
    }
    write_server_frame(
        &mut output,
        json!({"id": 4, "result": {"turn": turn_document("inProgress", vec![])}}),
    );
    if scenario == "failure-before-start-authentication" {
        send_native_error(
            &mut output,
            "diagnostic text is not identity",
            json!("unauthorized"),
            false,
        );
        send_turn_terminal(
            &mut output,
            "failed",
            vec![],
            Some(("different terminal prose", json!("unauthorized"))),
        );
        expect_client_eof(&mut input);
        return;
    }
    let started_turn_id = if scenario == "mismatched-turn-started" {
        "other-turn"
    } else {
        TURN_ID
    };
    write_server_frame(
        &mut output,
        json!({"method": "turn/started", "params": {
            "threadId": THREAD_ID,
            "turn": {"id": started_turn_id, "items": [], "status": "inProgress"}
        }}),
    );
    if scenario == "mismatched-turn-started" {
        return;
    }

    if scenario == "stalled-request-responses" {
        send_item_started(
            &mut output,
            "interactive-1",
            "commandExecution",
            json!({"status": "inProgress"}),
        );
        std::fs::write(std::env::var_os("CODEX_FIXTURE_READY").unwrap(), b"ready\n").unwrap();
        let proceed = PathBuf::from(std::env::var_os("CODEX_FIXTURE_PROCEED").unwrap());
        while !proceed.is_file() {
            crate::timing::sleep(Duration::from_millis(1));
        }
        for id in 0..8_000_i64 {
            write_server_frame(
                &mut output,
                json!({
                    "id": id,
                    "method": "item/commandExecution/requestApproval",
                    "params": {
                        "threadId": THREAD_ID,
                        "turnId": TURN_ID,
                        "itemId": "interactive-1",
                        "startedAtMs": 1,
                    },
                }),
            );
        }
        loop {
            std::thread::park();
        }
    }

    if let Some((method, kind, expected_result)) = match scenario.as_str() {
        "request-command-approval" => Some((
            "item/commandExecution/requestApproval",
            Some("commandExecution"),
            json!({"decision": "decline"}),
        )),
        "request-file-approval" => Some((
            "item/fileChange/requestApproval",
            Some("fileChange"),
            json!({"decision": "decline"}),
        )),
        "request-permissions" => Some((
            "item/permissions/requestApproval",
            Some("commandExecution"),
            json!({"permissions": {}}),
        )),
        "request-user-input" => Some((
            "item/tool/requestUserInput",
            Some("commandExecution"),
            json!({"answers": {}}),
        )),
        "request-mcp-elicitation" => Some((
            "mcpServer/elicitation/request",
            None,
            json!({"action": "decline"}),
        )),
        _ => None,
    } {
        if let Some(kind) = kind {
            send_item_started(
                &mut output,
                "interactive-1",
                kind,
                json!({"status": "inProgress"}),
            );
        }
        let params = match method {
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => json!({
                "threadId": THREAD_ID,
                "turnId": TURN_ID,
                "itemId": "interactive-1",
                "startedAtMs": 1,
            }),
            "item/permissions/requestApproval" => json!({
                "threadId": THREAD_ID,
                "turnId": TURN_ID,
                "itemId": "interactive-1",
                "startedAtMs": 1,
                "cwd": cwd,
                "permissions": {},
            }),
            "item/tool/requestUserInput" => json!({
                "threadId": THREAD_ID,
                "turnId": TURN_ID,
                "itemId": "interactive-1",
                "isBlocking": true,
                "questions": [],
            }),
            "mcpServer/elicitation/request" => json!({
                "threadId": THREAD_ID,
                "turnId": TURN_ID,
                "serverName": "fixture-mcp",
                "mode": "form",
                "message": "fixture",
                "requestedSchema": {"type": "object", "properties": {}},
            }),
            _ => unreachable!(),
        };
        write_server_frame(
            &mut output,
            json!({"id": "interactive-request", "method": method, "params": params}),
        );
        let response = read_client_frame(&mut input, &mut capture);
        assert_eq!(response["id"], "interactive-request");
        assert_eq!(response["result"], expected_result);
        if let Some(kind) = kind {
            let item = if kind == "commandExecution" {
                json!({
                    "id": "interactive-1",
                    "type": kind,
                    "status": "declined",
                    "aggregatedOutput": "",
                })
            } else {
                json!({"id": "interactive-1", "type": kind, "status": "declined"})
            };
            send_item_completed(&mut output, item);
        }
        send_turn_terminal(&mut output, "completed", vec![], None);
        expect_client_eof(&mut input);
        return;
    }

    if scenario == "unknown-request" {
        write_server_frame(
            &mut output,
            json!({
                "id": "unknown-request",
                "method": "future/interactiveRequest",
                "params": {"threadId": THREAD_ID, "turnId": TURN_ID},
            }),
        );
        let interrupt = read_client_frame(&mut input, &mut capture);
        assert_eq!(interrupt["id"], 5);
        assert_eq!(interrupt["method"], "turn/interrupt");
        assert_eq!(interrupt["params"]["threadId"], THREAD_ID);
        assert_eq!(interrupt["params"]["turnId"], TURN_ID);
        write_server_frame(&mut output, json!({"id": 5, "result": {}}));
        send_turn_terminal(&mut output, "interrupted", vec![], None);
        expect_client_eof(&mut input);
        return;
    }

    if scenario == "cancellation-pending-request" {
        send_item_started(
            &mut output,
            "interactive-1",
            "commandExecution",
            json!({"status": "inProgress"}),
        );
        write_server_frame(
            &mut output,
            json!({
                "id": "pending-approval",
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": THREAD_ID,
                    "turnId": TURN_ID,
                    "itemId": "interactive-1",
                    "startedAtMs": 1,
                },
            }),
        );
        std::fs::write(std::env::var_os("CODEX_FIXTURE_READY").unwrap(), b"ready\n").unwrap();
        let first = read_client_frame(&mut input, &mut capture);
        let interrupt = if first["id"] == "pending-approval" {
            assert_eq!(first["result"], json!({"decision": "decline"}));
            read_client_frame(&mut input, &mut capture)
        } else {
            first
        };
        assert_eq!(interrupt["method"], "turn/interrupt");
        write_server_frame(&mut output, json!({"id": 5, "result": {}}));
        send_item_completed(
            &mut output,
            json!({
                "id": "interactive-1",
                "type": "commandExecution",
                "status": "declined",
                "aggregatedOutput": "",
            }),
        );
        send_turn_terminal(&mut output, "interrupted", vec![], None);
        expect_client_eof(&mut input);
        return;
    }

    if matches!(
        scenario.as_str(),
        "cancellation-blocked" | "cancellation-after-output" | "cancellation-stubborn"
    ) {
        let items = if scenario == "cancellation-after-output" {
            vec![completed_provisional_response(&mut output)]
        } else {
            vec![]
        };
        std::fs::write(std::env::var_os("CODEX_FIXTURE_READY").unwrap(), b"ready\n").unwrap();
        let interrupt = read_client_frame(&mut input, &mut capture);
        assert_eq!(interrupt["id"], 5);
        assert_eq!(interrupt["method"], "turn/interrupt");
        if scenario == "cancellation-stubborn" {
            let descendant = std::process::Command::new("/bin/sh")
                .args(["-c", "trap '' INT TERM; while :; do sleep 60; done"])
                .spawn()
                .unwrap();
            std::fs::write(
                std::env::var_os("CODEX_FIXTURE_DESCENDANT").unwrap(),
                format!("{}\n", descendant.id()),
            )
            .unwrap();
            loop {
                std::thread::park();
            }
        }
        write_server_frame(&mut output, json!({"id": 5, "result": {}}));
        send_turn_terminal(&mut output, "interrupted", items, None);
        expect_client_eof(&mut input);
        return;
    }

    if scenario.starts_with("result-") {
        let first_candidate = match scenario.as_str() {
            "result-correction"
            | "result-exhausted"
            | "result-correction-failed"
            | "result-correction-interrupted" => Some(r#"{"result":-1}"#),
            "result-oversized" => Some(
                r#"{"result":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#,
            ),
            "result-settlement-blocked" => Some(r#"{"result":7}"#),
            "result-root-object" => Some(r#"{"result":{"answer":7}}"#),
            "result-root-array" => Some(r#"{"result":[1,2]}"#),
            "result-root-string" => Some(r#"{"result":"value"}"#),
            "result-root-number" => Some(r#"{"result":7}"#),
            "result-root-boolean" => Some(r#"{"result":true}"#),
            "result-root-null" => Some(r#"{"result":null}"#),
            "result-missing" => None,
            _ => panic!("unknown result scenario"),
        };
        send_result_turn(
            &mut output,
            TURN_ID,
            "result-first",
            first_candidate,
            "completed",
        );
        if scenario == "result-settlement-blocked" {
            let mut trailing = Vec::new();
            input.read_to_end(&mut trailing).unwrap();
            assert!(trailing.is_empty());
            loop {
                std::thread::park();
            }
        }
        if scenario.starts_with("result-root-") {
            let mut trailing = Vec::new();
            input.read_to_end(&mut trailing).unwrap();
            assert!(trailing.is_empty());
            return;
        }
        if scenario != "result-missing" {
            let correction = read_client_frame(&mut input, &mut capture);
            assert_eq!(correction["id"], 6);
            assert_eq!(correction["method"], "turn/start");
            assert_eq!(correction["params"]["threadId"], THREAD_ID);
            assert_eq!(
                correction["params"]["outputSchema"],
                json!({
                    "type": "object",
                    "properties": {"result": {}},
                    "required": ["result"],
                    "additionalProperties": false,
                }),
            );
            write_server_frame(
                &mut output,
                json!({
                    "id": 6,
                    "result": {"turn": {
                        "id": CORRECTION_TURN_ID,
                        "items": [],
                        "status": "inProgress",
                    }},
                }),
            );
            write_server_frame(
                &mut output,
                json!({
                    "method": "turn/started",
                    "params": {
                        "threadId": THREAD_ID,
                        "turn": {"id": CORRECTION_TURN_ID, "items": [], "status": "inProgress"},
                    },
                }),
            );
            let (candidate, status) = match scenario.as_str() {
                "result-correction" => (Some(r#"{"result":7}"#), "completed"),
                "result-exhausted" => (Some(r#"{"result":0}"#), "completed"),
                "result-oversized" => (Some(r#"{"result":"ok"}"#), "completed"),
                "result-correction-failed" => (None, "failed"),
                "result-correction-interrupted" => (None, "interrupted"),
                _ => unreachable!(),
            };
            send_result_turn(
                &mut output,
                CORRECTION_TURN_ID,
                "result-second",
                candidate,
                status,
            );
        }
        let mut trailing = Vec::new();
        input.read_to_end(&mut trailing).unwrap();
        assert!(trailing.is_empty());
        return;
    }
    let response = if scenario == "normal" {
        provider_response(&thread)
    } else {
        std::env::var("CODEX_FIXTURE_RESPONSE").unwrap()
    };
    if scenario == "normal" {
        write_server_frame(
            &mut output,
            json!({"method": "mcpServer/startupStatus/updated", "params": {
                "threadId": THREAD_ID, "name": "native", "status": "ready"
            }}),
        );
        let hook = json!({"id": "hook-1", "eventName": "userPromptSubmit"});
        write_server_frame(
            &mut output,
            json!({"method": "hook/started", "params": {
                "threadId": THREAD_ID, "turnId": TURN_ID, "run": hook
            }}),
        );
        write_server_frame(
            &mut output,
            json!({"method": "hook/completed", "params": {
                "threadId": THREAD_ID, "turnId": TURN_ID, "run": hook
            }}),
        );
        send_item_started(
            &mut output,
            "reason-1",
            "reasoning",
            json!({"summary": [], "content": []}),
        );
        write_server_frame(
            &mut output,
            json!({"method": "item/reasoning/summaryTextDelta", "params": {
                "threadId": THREAD_ID, "turnId": TURN_ID, "itemId": "reason-1", "summaryIndex": 0, "delta": "bounded reasoning"
            }}),
        );
        send_item_completed(
            &mut output,
            json!({"id": "reason-1", "type": "reasoning", "summary": ["bounded reasoning"], "content": []}),
        );
        send_item_started(
            &mut output,
            "command-1",
            "commandExecution",
            json!({"status": "inProgress"}),
        );
        send_item_completed(
            &mut output,
            json!({"id": "command-1", "type": "commandExecution", "status": "completed", "aggregatedOutput": "tool output"}),
        );
    }

    if scenario == "retry-then-success" {
        send_native_error(
            &mut output,
            "transient stream diagnostic",
            json!({"responseStreamDisconnected": {"httpStatusCode": 500}}),
            true,
        );
    }

    if matches!(
        scenario.as_str(),
        "failure-after-start-mcp"
            | "failure-after-start-hook"
            | "failure-after-start-model"
            | "failure-after-start-provider"
            | "failure-after-start-provider-other-prose"
            | "failure-after-start-authentication"
            | "failure-after-partial-output"
            | "retry-exhausted"
            | "truncated-provider-stream"
    ) {
        if scenario == "failure-after-start-mcp" {
            write_server_frame(
                &mut output,
                json!({"method": "mcpServer/startupStatus/updated", "params": {
                    "threadId": THREAD_ID,
                    "name": "required-mcp",
                    "status": "failed",
                    "error": "bounded MCP diagnostic",
                }}),
            );
        }
        if scenario == "failure-after-start-hook" {
            let hook = json!({"id": "hook-failure", "eventName": "userPromptSubmit"});
            write_server_frame(
                &mut output,
                json!({"method": "hook/started", "params": {
                    "threadId": THREAD_ID, "turnId": TURN_ID, "run": hook
                }}),
            );
            write_server_frame(
                &mut output,
                json!({"method": "hook/completed", "params": {
                    "threadId": THREAD_ID,
                    "turnId": TURN_ID,
                    "run": {
                        "id": "hook-failure",
                        "eventName": "userPromptSubmit",
                        "status": "failed",
                        "statusMessage": "bounded hook diagnostic",
                    }
                }}),
            );
        }
        let mut items = Vec::new();
        if scenario == "failure-after-partial-output" {
            items.push(completed_provisional_response(&mut output));
        }
        let (message, info) = match scenario.as_str() {
            "failure-after-start-model" => ("model diagnostic", json!("badRequest")),
            "failure-after-start-provider" => {
                ("provider diagnostic one", json!("internalServerError"))
            }
            "failure-after-start-provider-other-prose" => {
                ("unrelated provider prose", json!("internalServerError"))
            }
            "failure-after-start-authentication" => {
                ("authentication diagnostic", json!("unauthorized"))
            }
            "retry-exhausted" => {
                send_native_error(
                    &mut output,
                    "first truncated stream diagnostic",
                    json!({"responseStreamDisconnected": {"httpStatusCode": 500}}),
                    true,
                );
                (
                    "retry exhaustion diagnostic",
                    json!({"responseTooManyFailedAttempts": {"httpStatusCode": 500}}),
                )
            }
            "truncated-provider-stream" => (
                "truncated stream diagnostic",
                json!({"responseStreamDisconnected": {"httpStatusCode": 200}}),
            ),
            _ => ("native execution diagnostic", json!("other")),
        };
        send_native_error(&mut output, message, info.clone(), false);
        send_turn_terminal(
            &mut output,
            "failed",
            items,
            Some(("terminal prose differs", info)),
        );
        expect_client_eof(&mut input);
        return;
    }

    let send_message = !matches!(scenario.as_str(), "absent" | "delta-only");
    if scenario == "delta-only" {
        send_item_started(
            &mut output,
            "message-1",
            "agentMessage",
            json!({"text": "", "phase": null}),
        );
        write_server_frame(
            &mut output,
            json!({"method": "item/agentMessage/delta", "params": {
                "threadId": THREAD_ID, "turnId": TURN_ID, "itemId": "message-1", "delta": response
            }}),
        );
    } else if send_message {
        let response = if scenario == "empty" {
            ""
        } else {
            response.as_str()
        };
        send_item_started(
            &mut output,
            "message-1",
            "agentMessage",
            json!({"text": "", "phase": null}),
        );
        if !response.is_empty() {
            write_server_frame(
                &mut output,
                json!({"method": "item/agentMessage/delta", "params": {
                    "threadId": THREAD_ID, "turnId": TURN_ID, "itemId": "message-1", "delta": response
                }}),
            );
        }
        send_item_completed(
            &mut output,
            json!({"id": "message-1", "type": "agentMessage", "text": response, "phase": "final_answer"}),
        );
    }

    match scenario.as_str() {
        "malformed-after-output" => {
            output.write_all(b"{malformed\n").unwrap();
            output.flush().unwrap();
            return;
        }
        "invalid-utf8-after-output" => {
            output.write_all(&[0xff, b'\n']).unwrap();
            output.flush().unwrap();
            return;
        }
        "truncated-after-output" => {
            output.write_all(b"{\"method\":\"turn/completed\"").unwrap();
            output.flush().unwrap();
            return;
        }
        _ => {}
    }

    if scenario == "normal" {
        write_server_frame(
            &mut output,
            json!({"method": "thread/tokenUsage/updated", "params": {
                "threadId": THREAD_ID,
                "turnId": TURN_ID,
                "tokenUsage": {"total": {"inputTokens": 3, "outputTokens": 2}}
            }}),
        );
    }
    let status = match scenario.as_str() {
        "failure-after-output" => "failed",
        "interruption-after-output" => "interrupted",
        _ => "completed",
    };
    let items = if send_message {
        let response = if scenario == "empty" {
            ""
        } else {
            response.as_str()
        };
        vec![
            json!({"id": "message-1", "type": "agentMessage", "text": response, "phase": "final_answer"}),
        ]
    } else {
        vec![]
    };
    write_server_frame(
        &mut output,
        json!({"method": "turn/completed", "params": {
            "threadId": THREAD_ID,
            "turn": turn_document(status, items)
        }}),
    );
    expect_client_eof(&mut input);
    if scenario == "nonzero-after-output" {
        std::process::exit(7);
    }
}

struct ProviderRequest {
    path: String,
    authorization: String,
    body: Value,
}

struct LoopbackResponsesProvider {
    address: std::net::SocketAddr,
    request: mpsc::UnboundedReceiver<ProviderRequest>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl LoopbackResponsesProvider {
    async fn start(response: &str) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (requests, request) = mpsc::unbounded_channel();
        let (stop, mut shutdown) = oneshot::channel();
        let response = response.to_owned();
        let task = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = &mut shutdown => {}
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    serve_provider_request(stream, requests, response).await;
                }
            }
        });
        Self {
            address,
            request,
            shutdown: Some(stop),
            task,
        }
    }

    async fn next_request(&mut self) -> ProviderRequest {
        self.request.recv().await.unwrap()
    }

    async fn shutdown(mut self) {
        let _ = self.shutdown.take().unwrap().send(());
        self.task.await.unwrap();
    }
}

async fn serve_provider_request(
    mut stream: TcpStream,
    requests: mpsc::UnboundedSender<ProviderRequest>,
    response: String,
) {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).await.unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        assert!(bytes.len() <= 64 * 1024);
    };
    let header = std::str::from_utf8(&bytes[..header_end]).unwrap();
    let mut lines = header.split("\r\n");
    let request_line = lines.next().unwrap();
    let path = request_line
        .split_ascii_whitespace()
        .nth(1)
        .unwrap()
        .to_owned();
    let mut content_length = None;
    let mut authorization = String::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').unwrap();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = Some(value.trim().parse::<usize>().unwrap());
        } else if name.eq_ignore_ascii_case("authorization") {
            authorization = value.trim().to_owned();
        }
    }
    let content_length = content_length.unwrap();
    assert!(content_length <= 1024 * 1024);
    while bytes.len() < header_end + content_length {
        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).await.unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
    }
    let body = serde_json::from_slice(&bytes[header_end..header_end + content_length]).unwrap();
    requests
        .send(ProviderRequest {
            path,
            authorization,
            body,
        })
        .unwrap();
    let events = [
        json!({
            "type": "response.created",
            "response": {"id": "response-loopback"}
        }),
        json!({
            "type": "response.output_item.done",
            "item": {
                "type": "message",
                "role": "assistant",
                "id": "message-loopback",
                "content": [{"type": "output_text", "text": response}]
            }
        }),
        json!({
            "type": "response.completed",
            "response": {
                "id": "response-loopback",
                "usage": {
                    "input_tokens": 0,
                    "input_tokens_details": null,
                    "output_tokens": 0,
                    "output_tokens_details": null,
                    "total_tokens": 0
                }
            }
        }),
    ];
    let payload = events
        .into_iter()
        .map(|event| {
            format!(
                "event: {}\ndata: {event}\n\n",
                event["type"].as_str().unwrap()
            )
        })
        .collect::<String>();
    let header = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        payload.len()
    );
    stream.write_all(header.as_bytes()).await.unwrap();
    stream.write_all(payload.as_bytes()).await.unwrap();
    stream.shutdown().await.unwrap();
}

pub(super) mod normal {
    use super::*;

    #[test]
    fn launch_is_strict_stdio_with_invocation_scoped_project_and_hook_trust() {
        let fixture = ProcessFixture::new("absent", AgentValueMode::None, 1024);
        let plan = prepare_launch(fixture.invocation.as_ref().unwrap()).unwrap();
        let arguments = plan.arguments();
        assert_eq!(arguments[0], OsStr::new("--dangerously-bypass-hook-trust"));
        assert_eq!(arguments[1], OsStr::new("-c"));
        let trust = arguments[2].to_str().unwrap();
        assert!(trust.starts_with("projects={\""));
        assert!(trust.contains("trust_level=\"trusted\""));
        assert!(trust.contains(fixture.expected_cwd.to_str().unwrap()));
        assert_eq!(arguments[3], OsStr::new("-c"));
        assert_eq!(
            arguments[4],
            OsStr::new(&format!(
                "sqlite_home={}",
                serde_json::to_string(
                    crate::execution::workflow::child_guard::BOUND_DIRECTORY_PLACEHOLDER
                )
                .unwrap()
            ))
        );
        assert_eq!(
            &arguments[5..],
            [
                OsStr::new("app-server"),
                OsStr::new("--strict-config"),
                OsStr::new("--listen"),
                OsStr::new("stdio://"),
            ]
        );
    }

    #[tokio::test]
    async fn ordered_attachments_use_exact_wrappers_native_images_and_sealed_notices() {
        let canonical_json = br#"{"a":1,"z":2}"#;
        let fixtures: [(&[u8], &str, &str); 8] = [
            (b"native text attachment", "text/plain", "caller.txt"),
            (
                canonical_json,
                "Application/JSON; Charset=UTF-8",
                "caller.json",
            ),
            (b"", "text/plain; charset=utf-8", "empty.txt"),
            (b"png bytes", "IMAGE/PNG; profile=fixture", "caller.png"),
            (b"jpeg bytes", "image/jpeg", "caller.jpg"),
            (b"pdf bytes", "application/pdf", "caller.pdf"),
            (b"invalid \xff text", "text/plain", "invalid.txt"),
            (b"general bytes", "application/octet-stream", "caller.bin"),
        ];
        let fixture = ProcessFixture::with_attachments(&fixtures);
        let invocation = fixture.invocation.as_ref().unwrap();
        let sealed_paths = invocation
            .attachments()
            .iter()
            .map(|attachment| attachment.path().to_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        let before = invocation
            .attachments()
            .iter()
            .map(|attachment| std::fs::read(attachment.path()).unwrap())
            .collect::<Vec<_>>();
        let plan = prepare_launch(invocation).unwrap();
        let input = plan.initial_input();

        assert_eq!(input.len(), fixtures.len() + 1);
        assert_eq!(
            input[0],
            json!({"type": "text", "text": "ordinary user turn"})
        );
        assert_eq!(
            input[1],
            json!({
                "type": "text",
                "text": "Scherzo attachment 000000 (text/plain) follows:\nnative text attachment",
            })
        );
        assert_eq!(
            input[2],
            json!({
                "type": "text",
                "text": "Scherzo attachment 000001 (Application/JSON; Charset=UTF-8) follows:\n{\"a\":1,\"z\":2}",
            })
        );
        assert_eq!(
            input[3],
            json!({
                "type": "text",
                "text": "Scherzo attachment 000002 (text/plain; charset=utf-8) follows:\n",
            })
        );
        assert_eq!(
            input[4],
            json!({"type": "localImage", "path": sealed_paths[3]})
        );
        assert_eq!(
            input[5],
            json!({"type": "localImage", "path": sealed_paths[4]})
        );
        for (input_index, attachment_index, media_type) in [
            (6, 5, "application/pdf"),
            (7, 6, "text/plain"),
            (8, 7, "application/octet-stream"),
        ] {
            assert_eq!(
                input[input_index],
                json!({
                    "type": "text",
                    "text": format!(
                        "Scherzo attachment {attachment_index:06} has media type {media_type} and is available to runner tools at {}.",
                        sealed_paths[attachment_index]
                    ),
                })
            );
        }
        let serialized = serde_json::to_string(input).unwrap();
        for caller_name in fixtures.map(|(_, _, name)| name) {
            assert!(!serialized.contains(caller_name));
        }
        assert_eq!(
            invocation
                .attachments()
                .iter()
                .map(|attachment| std::fs::read(attachment.path()).unwrap())
                .collect::<Vec<_>>(),
            before,
        );
        assert_eq!(
            input
                .iter()
                .filter(|item| item["type"] == "localImage")
                .count(),
            2,
        );

        let expected_input = input.to_vec();
        let requests = fixture.requests.clone();
        let codex_home = fixture.codex_home.clone();
        drop(plan);
        let (fixture, outcome, started) = with_watchdog(run_fixture(fixture)).await;
        assert!(started);
        assert_eq!(
            outcome,
            AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
        );
        assert_eq!(
            captured_requests(&requests)[4]["params"]["input"],
            Value::Array(expected_input)
        );
        assert!(
            !codex_home
                .join("sessions")
                .join("2026/08/18")
                .join(format!("rollout-2026-08-18T00-00-00-{THREAD_ID}.jsonl"))
                .exists()
        );
        drop(fixture);
    }

    #[tokio::test]
    async fn ordinary_response_turn_uses_loopback_and_settles_after_quiescence() {
        with_watchdog(async {
            let mut provider = LoopbackResponsesProvider::start(RESPONSE).await;
            let fixture = ProcessFixture::with_provider(
                "normal",
                response_mode(),
                1024,
                Some(provider.address),
            );
            let observations = fixture.observations.clone();
            let arguments = fixture.arguments.clone();
            let requests = fixture.requests.clone();
            let run = tokio::spawn(run_fixture(fixture));
            let request = provider.next_request().await;
            assert_eq!(request.path, "/responses");
            assert_eq!(request.authorization, format!("Bearer {PLACEHOLDER_KEY}"));
            assert_eq!(request.body["model"], MODEL);
            let instructions = request.body["instructions"].as_str().unwrap();
            assert_eq!(
                instructions,
                "native developer instructions\n\nscherzo system instructions"
            );
            let serialized = serde_json::to_string(&request.body).unwrap();
            for marker in [
                "root resource marker",
                "nested resource marker",
                "skill resource marker",
                "native_mcp_tool",
            ] {
                assert!(serialized.contains(marker));
            }
            let (fixture, outcome, started) = run.await.unwrap();
            assert!(started);
            let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome
            else {
                panic!("bounded normal response must complete");
            };
            assert_eq!(response.as_str(), RESPONSE);
            assert_rollout_retained(&fixture);

            let captured = captured_requests(&requests);
            assert_eq!(captured.len(), 5);
            assert_eq!(captured[0]["method"], "initialize");
            assert_eq!(captured[1]["method"], "initialized");
            assert_eq!(captured[2]["method"], "config/read");
            assert_eq!(captured[3]["method"], "thread/start");
            assert_eq!(captured[4]["method"], "turn/start");
            assert!(captured.iter().all(|request| {
                !matches!(
                    request["method"].as_str(),
                    Some("thread/resume" | "thread/fork" | "thread/read")
                )
            }));
            let thread = &captured[3]["params"];
            assert_eq!(thread["approvalPolicy"], "never");
            assert_eq!(thread["ephemeral"], false);
            assert_eq!(thread["sandbox"], "danger-full-access");
            assert_eq!(thread["modelProvider"], PROVIDER);
            assert_eq!(thread["config"], json!({"bypass_hook_trust": true}));
            assert!(thread.get("baseInstructions").is_none());
            assert_eq!(
                thread["developerInstructions"],
                "native developer instructions\n\nscherzo system instructions"
            );
            let turn = &captured[4]["params"];
            assert_eq!(turn["model"], MODEL);
            assert_eq!(turn["effort"], "high");
            assert_eq!(turn["approvalPolicy"], "never");
            assert_eq!(
                turn["sandboxPolicy"],
                json!({"type": "externalSandbox", "networkAccess": "enabled"})
            );

            let observations = observations.snapshot();
            assert!(observations.windows(2).all(|pair| {
                pair[0].sequence().get().checked_add(1) == Some(pair[1].sequence().get())
            }));
            assert!(observations.iter().any(|observation| matches!(
                observation.observation(),
                AgentObservation::AssistantText { text } if text.as_ref() == RESPONSE
            )));
            assert!(observations.iter().any(|observation| matches!(
                observation.observation(),
                AgentObservation::Reasoning { text } if text.as_ref() == "bounded reasoning"
            )));
            assert!(observations.iter().any(|observation| matches!(
                observation.observation(),
                AgentObservation::Usage {
                    input_tokens: 3,
                    output_tokens: 2
                }
            )));
            assert!(observations.iter().any(|observation| matches!(
                observation.observation(),
                AgentObservation::ToolCall { name, phase: AgentToolCallPhase::Completed, .. }
                    if name.as_ref() == "commandExecution"
            )));
            assert_last_observation_is_quiescent(&observations);
            let started_index = observations
                .iter()
                .position(|observation| {
                    matches!(
                        observation.observation(),
                        AgentObservation::Lifecycle {
                            milestone: AgentLifecycleMilestone::HarnessStarted,
                        }
                    )
                })
                .unwrap();
            let completed_index = observations
                .iter()
                .position(|observation| {
                    matches!(
                        observation.observation(),
                        AgentObservation::Lifecycle {
                            milestone: AgentLifecycleMilestone::HarnessCompleted,
                        }
                    )
                })
                .unwrap();
            assert!(started_index < completed_index);

            let captured_arguments = std::fs::read(arguments).unwrap();
            assert!(captured_arguments.ends_with(b"stdio://\0"));
            provider.shutdown().await;
            drop(fixture);
        })
        .await;
    }

    #[tokio::test]
    async fn ordinary_no_value_turn_has_the_same_clean_settlement_boundary() {
        with_watchdog(async {
            let fixture = ProcessFixture::new("no-value", AgentValueMode::None, 5);
            let (fixture, outcome, started) = run_fixture(fixture).await;
            assert!(
                started,
                "terminal outcome: {outcome:?}; requests: {:?}; stderr: {:?}",
                captured_requests(&fixture.requests),
                fixture
                    .diagnostics
                    .get("agent-step")
                    .map(
                        |diagnostic| String::from_utf8_lossy(diagnostic.standard_error().bytes())
                            .into_owned()
                    )
            );
            assert_eq!(
                outcome,
                AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
            );
        })
        .await;
    }
}

pub(super) mod exact_binary {
    use super::*;

    #[tokio::test]
    #[ignore = "requires the repository-pinned exact Codex 0.147.0 executable"]
    async fn retained_rollout_uses_native_configuration_without_ambient_history() {
        with_watchdog(async {
            let mut provider = LoopbackResponsesProvider::start(RESPONSE).await;
            let fixture = ProcessFixture::with_exact_binary(provider.address);
            let codex_home = fixture.codex_home.clone();
            let sqlite_home = fixture.sqlite_home.clone();
            let config = std::fs::read(codex_home.join("config.toml")).unwrap();
            let mut run = tokio::spawn(run_fixture(fixture));
            let request = tokio::select! {
                request = provider.next_request() => request,
                finished = &mut run => {
                    let (fixture, outcome, started) = finished.unwrap();
                    panic!(
                        "exact Codex ended before provider request: started={started} outcome={outcome:?} stderr={:?}",
                        fixture.diagnostics.get("agent-step").map(|diagnostic| {
                            String::from_utf8_lossy(diagnostic.standard_error().bytes()).into_owned()
                        })
                    );
                }
            };
            assert_eq!(request.path, "/responses");
            assert_eq!(request.authorization, format!("Bearer {PLACEHOLDER_KEY}"));
            let (fixture, outcome, started) = run.await.unwrap();
            assert!(started, "exact Codex outcome: {outcome:?}");
            let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome
            else {
                panic!("exact Codex must complete one loopback response: {outcome:?}");
            };
            assert_eq!(response.as_str(), RESPONSE);
            assert_rollout_retained(&fixture);
            assert_eq!(
                std::fs::read(codex_home.join("config.toml")).unwrap(),
                config
            );
            assert!(!codex_home.join("state_5.sqlite").exists());
            assert!(sqlite_home.join("state_5.sqlite").is_file());
            let correlation = std::fs::read(fixture.thread_correlation()).unwrap();
            assert!(
                !correlation
                    .windows(PLACEHOLDER_KEY.len())
                    .any(|window| window == PLACEHOLDER_KEY.as_bytes())
            );
            provider.shutdown().await;
        })
        .await;
    }
}

pub(super) mod response_authority {
    use super::*;

    async fn run_response_fixture(scenario: &str) -> (AgentOutcome, bool) {
        let (_, outcome, started) = run_response_process(scenario, 5).await;
        (outcome, started)
    }

    #[tokio::test]
    async fn only_the_bounded_settled_completed_message_commits() {
        with_watchdog(async {
            for scenario in ["absent", "empty"] {
                let (outcome, started) = run_response_fixture(scenario).await;
                assert!(started);
                assert_eq!(
                    outcome,
                    AgentOutcome::Completed(CompletedAgentInvocation::NoResponse),
                    "{scenario}"
                );
            }

            let (outcome, started) = run_response_fixture("exact-limit").await;
            assert!(started);
            let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome
            else {
                panic!("exact-limit response must complete");
            };
            assert_eq!(response.as_str(), "12345");

            for (scenario, expected) in [
                ("oversized", AgentFailureCause::CapturedValueTooLarge),
                ("delta-only", AgentFailureCause::HarnessProtocolFailed),
                (
                    "failure-after-output",
                    AgentFailureCause::HarnessFailed {
                        detail: AgentHarnessFailureDetail::ModelError,
                    },
                ),
                (
                    "interruption-after-output",
                    AgentFailureCause::HarnessFailed {
                        detail: AgentHarnessFailureDetail::ModelAborted,
                    },
                ),
                (
                    "nonzero-after-output",
                    AgentFailureCause::HarnessFailed {
                        detail: AgentHarnessFailureDetail::UnsuccessfulExit,
                    },
                ),
            ] {
                let (outcome, started) = run_response_fixture(scenario).await;
                assert_started_failure(scenario, outcome, started, expected);
            }
        })
        .await;
    }
}

pub(super) mod structured_result {
    use super::*;

    fn positive_integer_schema() -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "integer",
            "minimum": 1,
        })
    }

    #[tokio::test]
    async fn authoritative_validation_accepts_every_json_root_category() {
        with_watchdog(async {
            for (scenario, root_type, expected) in [
                ("result-root-object", "object", json!({"answer": 7})),
                ("result-root-array", "array", json!([1, 2])),
                ("result-root-string", "string", json!("value")),
                ("result-root-number", "number", json!(7)),
                ("result-root-boolean", "boolean", json!(true)),
                ("result-root-null", "null", Value::Null),
            ] {
                let fixture = ProcessFixture::new(
                    scenario,
                    result_mode(json!({
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "type": root_type,
                    })),
                    1024,
                );
                let (_, outcome, started) = run_fixture(fixture).await;
                assert!(started, "{scenario}: {outcome:?}");
                let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome
                else {
                    panic!("{scenario} must produce one accepted result");
                };
                assert_eq!(result.value(), &expected, "{scenario}");
            }
        })
        .await;
    }

    #[tokio::test]
    async fn schema_rejection_is_corrected_once_on_the_same_settled_thread() {
        with_watchdog(async {
            let fixture = ProcessFixture::new(
                "result-correction",
                result_mode(positive_integer_schema()),
                1024,
            );
            let observations = fixture.observations.clone();
            let requests = fixture.requests.clone();
            let (fixture, outcome, started) = run_fixture(fixture).await;
            assert!(started);
            let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
                panic!(
                    "corrected result must complete: {outcome:?}; requests: {:?}; stderr: {:?}",
                    captured_requests(&fixture.requests),
                    fixture.diagnostics.get("agent-step").map(|diagnostic| {
                        String::from_utf8_lossy(diagnostic.standard_error().bytes()).into_owned()
                    }),
                );
            };
            assert_eq!(result.value(), &json!(7));

            let requests = captured_requests(&requests);
            assert_eq!(requests.len(), 6);
            let first = &requests[4];
            let correction = &requests[5];
            assert_eq!(first["method"], "turn/start");
            assert_eq!(correction["method"], "turn/start");
            assert_eq!(first["params"]["threadId"], THREAD_ID);
            assert_eq!(correction["params"]["threadId"], THREAD_ID);
            assert_eq!(
                first["params"]["outputSchema"],
                correction["params"]["outputSchema"]
            );
            let feedback = correction["params"]["input"][0]["text"].as_str().unwrap();
            assert!(!feedback.is_empty());
            assert!(feedback.len() <= 512);

            let observations = observations.snapshot();
            assert_eq!(
                observations
                    .iter()
                    .filter(|observation| matches!(
                        observation.observation(),
                        AgentObservation::ValueRejected {
                            kind: AgentValueKind::Result,
                            ..
                        }
                    ))
                    .count(),
                1,
            );
            assert_last_observation_is_quiescent(&observations);
        })
        .await;
    }

    #[tokio::test]
    async fn accepted_result_that_misses_settlement_grace_is_discarded_after_quiescence() {
        with_watchdog(async {
            let (clock, mut control) = ControlledClock::new();
            let mut fixture = ProcessFixture::new(
                "result-settlement-blocked",
                result_mode(positive_integer_schema()),
                1024,
            );
            let invocation = fixture.invocation.take().unwrap();
            let (task, start, outcome) =
                start_fixture_with_clock(invocation, fixture.diagnostics.clone(), clock);
            start.receive().await.unwrap();
            assert_eq!(control.deadlines.recv().await, Some(Duration::from_secs(1)));
            assert_eq!(control.deadlines.recv().await, Some(Duration::from_secs(1)));
            control.expired.send_replace(true);
            task.await.unwrap();
            assert_eq!(
                outcome.receive().await.unwrap(),
                AgentOutcome::Failed(AgentFailureCause::ResultSettlementFailed.into()),
            );
            assert!(process_group_is_quiescent(fixture_process(
                &fixture.process
            )));
        })
        .await;
    }

    #[tokio::test]
    async fn oversized_exhausted_missing_and_failed_corrections_commit_no_candidate() {
        with_watchdog(async {
            let oversized = ProcessFixture::new(
                "result-oversized",
                result_mode(json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "type": "string",
                })),
                1024,
            );
            let (_, outcome, started) = run_fixture(oversized).await;
            assert!(started);
            let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
                panic!("oversized candidate must be corrected: {outcome:?}");
            };
            assert_eq!(result.value(), &json!("ok"));

            for (scenario, expected) in [
                ("result-exhausted", AgentFailureCause::MissingResult),
                ("result-missing", AgentFailureCause::MissingResult),
                (
                    "result-correction-failed",
                    AgentFailureCause::HarnessFailed {
                        detail: AgentHarnessFailureDetail::ModelError,
                    },
                ),
                (
                    "result-correction-interrupted",
                    AgentFailureCause::HarnessFailed {
                        detail: AgentHarnessFailureDetail::ModelAborted,
                    },
                ),
            ] {
                let fixture =
                    ProcessFixture::new(scenario, result_mode(positive_integer_schema()), 1024);
                let (_, outcome, started) = run_fixture(fixture).await;
                assert_started_failure(scenario, outcome, started, expected);
            }
        })
        .await;
    }
}

pub(super) mod start_failure {
    use super::*;

    #[tokio::test]
    async fn every_launch_and_setup_stage_is_typed_and_quiescent() {
        with_watchdog(async {
            let launch = ProcessFixture::new("launch-failure", AgentValueMode::None, 1024);
            let (_, outcome, started) = run_fixture(launch).await;
            assert!(!started);
            assert_eq!(
                outcome,
                AgentOutcome::Failed(
                    AgentFailureCause::HarnessSetupFailed {
                        stage: AgentHarnessSetupStage::ExecutableLaunch,
                    }
                    .into(),
                )
            );

            for (scenario, stage) in [
                (
                    "initialize-rejected",
                    AgentHarnessSetupStage::Initialization,
                ),
                ("initialize-eof", AgentHarnessSetupStage::Initialization),
                (
                    "config-read-rejected",
                    AgentHarnessSetupStage::EffectiveConfiguration,
                ),
                ("thread-start-rejected", AgentHarnessSetupStage::ThreadStart),
                ("turn-start-rejected", AgentHarnessSetupStage::TurnStart),
                (
                    "premature-turn-started",
                    AgentHarnessSetupStage::StartAcknowledgement,
                ),
                (
                    "mismatched-turn-started",
                    AgentHarnessSetupStage::StartAcknowledgement,
                ),
            ] {
                let fixture = ProcessFixture::new(scenario, AgentValueMode::None, 1024);
                let (fixture, outcome, started) = run_fixture(fixture).await;
                assert!(!started, "{scenario}");
                assert_failure_cause(
                    outcome,
                    AgentFailureCause::HarnessSetupFailed { stage },
                    scenario,
                );
                assert!(fixture.protocol_rejection().is_file());
                if scenario == "turn-start-rejected" {
                    assert!(fixture.thread_correlation().is_file());
                    assert!(!fixture.retained_rollout().exists());
                    assert!(!fixture.ambient_rollout().exists());
                } else if matches!(
                    scenario,
                    "premature-turn-started" | "mismatched-turn-started"
                ) {
                    assert_rollout_retained(&fixture);
                }
            }
        })
        .await;
    }
}

pub(super) mod unattended_requests {
    use super::*;

    #[tokio::test]
    async fn known_requests_receive_only_the_fixed_unattended_response() {
        with_watchdog(async {
            for scenario in [
                "request-command-approval",
                "request-file-approval",
                "request-permissions",
                "request-user-input",
                "request-mcp-elicitation",
            ] {
                let fixture = ProcessFixture::new(scenario, AgentValueMode::None, 1024);
                let requests = fixture.requests.clone();
                let (fixture, outcome, started) = run_fixture(fixture).await;
                assert!(started, "{scenario}: {outcome:?}");
                assert_eq!(
                    outcome,
                    AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
                    "{scenario}",
                );
                let requests = captured_requests(&requests);
                assert_eq!(requests.len(), 6, "{scenario}: {requests:?}");
                assert!(process_group_is_quiescent(fixture_process(
                    &fixture.process
                )));
                let response = requests.last().unwrap();
                assert_eq!(response.get("id"), Some(&json!("interactive-request")));
                assert!(response.get("error").is_none());
                let serialized = serde_json::to_string(response).unwrap();
                for granted in [
                    "accept",
                    "acceptForSession",
                    "strictAutoReview",
                    "networkAccess",
                ] {
                    assert!(!serialized.contains(granted), "{scenario}: {serialized}");
                }
            }
        })
        .await;
    }

    #[tokio::test]
    async fn unknown_request_interrupts_and_fails_after_quiescence() {
        with_watchdog(async {
            let fixture = ProcessFixture::new("unknown-request", AgentValueMode::None, 1024);
            let requests = fixture.requests.clone();
            let process = fixture.process.clone();
            let (fixture, outcome, started) = run_fixture(fixture).await;
            assert!(started);
            assert_failure_cause(
                outcome,
                AgentFailureCause::HarnessProtocolFailed,
                "unknown-request",
            );
            let requests = captured_requests(&requests);
            assert_eq!(requests.last().unwrap()["method"], "turn/interrupt");
            assert!(process_group_is_quiescent(fixture_process(&process)));
            drop(fixture);
        })
        .await;
    }
}

pub(super) mod failure_ordering {
    use super::*;

    #[tokio::test]
    async fn correlated_native_failures_keep_structured_identity_across_orderings() {
        with_watchdog(async {
            for (scenario, expected_started, expected_detail) in [
                (
                    "failure-before-start-authentication",
                    false,
                    AgentHarnessFailureDetail::ModelError,
                ),
                (
                    "failure-after-start-mcp",
                    true,
                    AgentHarnessFailureDetail::ModelError,
                ),
                (
                    "failure-after-start-hook",
                    true,
                    AgentHarnessFailureDetail::ModelError,
                ),
                (
                    "failure-after-start-model",
                    true,
                    AgentHarnessFailureDetail::ModelError,
                ),
                (
                    "failure-after-start-provider",
                    true,
                    AgentHarnessFailureDetail::ModelError,
                ),
                (
                    "failure-after-start-provider-other-prose",
                    true,
                    AgentHarnessFailureDetail::ModelError,
                ),
                (
                    "failure-after-start-authentication",
                    true,
                    AgentHarnessFailureDetail::ModelError,
                ),
                (
                    "failure-after-partial-output",
                    true,
                    AgentHarnessFailureDetail::ModelError,
                ),
                (
                    "retry-exhausted",
                    true,
                    AgentHarnessFailureDetail::ModelOutputTruncated,
                ),
                (
                    "truncated-provider-stream",
                    true,
                    AgentHarnessFailureDetail::ModelOutputTruncated,
                ),
            ] {
                let (fixture, outcome, started) = run_response_process(scenario, 1024).await;
                assert_eq!(started, expected_started, "{scenario}: {outcome:?}");
                assert_eq!(
                    outcome,
                    AgentOutcome::Failed(
                        AgentFailureCause::HarnessFailed {
                            detail: expected_detail,
                        }
                        .into(),
                    ),
                    "{scenario}",
                );
                assert_fixture_quiescent(&fixture);
                assert_rollout_retained(&fixture);
            }
        })
        .await;
    }

    #[tokio::test]
    async fn native_retry_can_recover_without_a_harness_retry() {
        with_watchdog(async {
            let fixture = ProcessFixture::new("retry-then-success", response_mode(), 1024);
            let observations = fixture.observations.clone();
            let (_, outcome, started) = run_fixture(fixture).await;
            assert!(started);
            let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome
            else {
                panic!("native retry must recover");
            };
            assert_eq!(response.as_str(), RESPONSE);
            let milestones = observations
                .snapshot()
                .into_iter()
                .filter_map(|observation| match observation.observation() {
                    AgentObservation::Lifecycle { milestone } => Some(*milestone),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert!(milestones.contains(&AgentLifecycleMilestone::RetryStarted));
            assert!(milestones.contains(&AgentLifecycleMilestone::RetryCompleted));
        })
        .await;
    }
}

pub(super) mod adversarial_lifecycle {
    use super::*;

    #[tokio::test]
    async fn malformed_output_after_a_candidate_never_commits() {
        with_watchdog(async {
            for scenario in [
                "malformed-after-output",
                "invalid-utf8-after-output",
                "truncated-after-output",
            ] {
                let (fixture, outcome, started) = run_response_process(scenario, 1024).await;
                assert!(started, "{scenario}: {outcome:?}");
                assert_failure_cause(outcome, AgentFailureCause::HarnessProtocolFailed, scenario);
                assert_fixture_quiescent(&fixture);
                assert_rollout_retained(&fixture);
                assert!(fixture.protocol_rejection().is_file());
            }
        })
        .await;
    }

    #[tokio::test]
    async fn adversarial_native_paths_are_diagnostic_only_and_never_imported() {
        with_watchdog(async {
            for (scenario, expected_reason) in [
                ("rollout-outside", "path_outside_state_boundary"),
                ("rollout-symlink", "unexpected_file_kind"),
                ("rollout-identity-mismatch", "thread_identity_mismatch"),
            ] {
                let fixture = ProcessFixture::new(scenario, response_mode(), 1024);
                let (fixture, outcome, started) = run_fixture(fixture).await;
                assert!(started, "{scenario}: {outcome:?}");
                let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome
                else {
                    panic!("{scenario}: path retention must not change workflow authority");
                };
                assert_eq!(response.as_str(), RESPONSE);
                assert!(fixture.thread_correlation().is_file());
                assert!(!fixture.retained_rollout().exists());
                let rejection: Value = serde_json::from_slice(
                    &std::fs::read(fixture.diagnostic_session.join("rollout-rejection.json"))
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(rejection["reason"], expected_reason);
            }
        })
        .await;
    }

    #[tokio::test]
    async fn transient_sqlite_home_remains_bound_for_the_process_lifetime() {
        with_watchdog(async {
            let fixture = ProcessFixture::new("sqlite-home-replaced", response_mode(), 1024);
            let (fixture, outcome, started) = run_fixture(fixture).await;
            assert!(started, "outcome: {outcome:?}");
            let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome
            else {
                panic!("SQLite path replacement must not change workflow authority");
            };
            assert_eq!(response.as_str(), RESPONSE);
            assert_rollout_retained(&fixture);
            assert!(
                !fixture.codex_home.join("state_5.sqlite").exists(),
                "replacing sqlite_home must not create an ambient resumable index"
            );
            assert!(
                fixture
                    .sqlite_home
                    .with_file_name("displaced-sqlite-home")
                    .join("state_5.sqlite")
                    .is_file()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn stalled_server_response_writes_fail_on_the_controlled_deadline() {
        with_watchdog(async {
            let mut fixture =
                ProcessFixture::new("stalled-request-responses", AgentValueMode::None, 1024);
            let process = fixture.process.clone();
            let ready = fixture.ready.clone();
            let proceed = fixture.proceed.clone();
            let invocation = fixture.invocation.take().unwrap();
            let (deadline_sender, mut deadlines) = mpsc::unbounded_channel();
            let (release, release_receiver) = watch::channel(false);
            let clock = ReleasedClock {
                deadlines: deadline_sender,
                release: release_receiver,
            };
            let (task, start, outcome) =
                start_fixture_with_clock(invocation, fixture.diagnostics.clone(), clock);
            start.receive().await.unwrap();
            wait_for_fixture_file(&ready).await;
            while deadlines.try_recv().is_ok() {}
            std::fs::write(proceed, b"proceed\n").unwrap();
            assert_eq!(deadlines.recv().await, Some(Duration::from_secs(1)));
            release.send(true).unwrap();

            task.await.unwrap();
            assert_failure_cause(
                outcome.receive().await.unwrap(),
                AgentFailureCause::HarnessProtocolFailed,
                "stalled-request-responses",
            );
            assert!(process_group_is_quiescent(fixture_process(&process)));
        })
        .await;
    }

    #[tokio::test]
    async fn stderr_flood_is_fully_drained_but_retained_only_to_its_limit() {
        with_watchdog(async {
            let fixture = ProcessFixture::new("stderr-flood", AgentValueMode::None, 1024);
            let diagnostics = fixture.diagnostics.clone();
            let (_, outcome, started) = run_fixture(fixture).await;
            assert!(started);
            assert_eq!(
                outcome,
                AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
            );
            let diagnostic = diagnostics.get("agent-step").unwrap();
            let stream = diagnostic.standard_error();
            assert_eq!(stream.bytes().len(), 1024);
            assert!(stream.truncation().is_some());
            assert!(stream.fully_drained());
        })
        .await;
    }
}

pub(super) mod cancellation {
    use super::*;

    struct RunningCancellationFixture {
        fixture: ProcessFixture,
        task: tokio::task::JoinHandle<()>,
        start: Option<AgentStartReceiver>,
        outcome: AgentTerminalReceiver,
        cancellation: CancellationSource,
        process_control: AgentProcessControl,
    }

    impl RunningCancellationFixture {
        fn start(mut fixture: ProcessFixture) -> Self {
            let invocation = fixture.invocation.take().unwrap();
            let cancellation = invocation.cancellation().clone();
            let process_control = invocation.process_control().clone();
            let (task, start, outcome) = start_fixture(invocation, fixture.diagnostics.clone());
            Self {
                fixture,
                task,
                start: Some(start),
                outcome,
                cancellation,
                process_control,
            }
        }

        async fn await_started(&mut self) {
            self.start.take().unwrap().receive().await.unwrap();
        }

        fn cancel(&self) {
            assert!(
                self.cancellation
                    .request_cancellation(CancellationReason::UserRequest)
            );
            self.process_control.interrupt();
        }

        async fn finish(self) -> (ProcessFixture, AgentOutcome) {
            self.task.await.unwrap();
            let outcome = self.outcome.receive().await.unwrap();
            (self.fixture, outcome)
        }
    }

    fn assert_user_cancelled(outcome: AgentOutcome) {
        assert_eq!(
            outcome,
            AgentOutcome::Cancelled {
                reason: CancellationReason::UserRequest,
            }
        );
    }

    fn assert_cancelled_rollout_retained(fixture: &ProcessFixture, outcome: AgentOutcome) {
        assert_user_cancelled(outcome);
        assert_fixture_quiescent(fixture);
        assert_rollout_retained(fixture);
    }

    async fn assert_prestart_cancellation(scenario: &str, expected_requests: usize) {
        let fixture = ProcessFixture::new(scenario, AgentValueMode::None, 1024);
        let mut running = RunningCancellationFixture::start(fixture);
        wait_for_fixture_file(&running.fixture.ready).await;
        running.cancel();
        let start = running.start.take().unwrap();
        let (fixture, outcome) = running.finish().await;
        assert!(start.receive().await.is_err());
        assert_user_cancelled(outcome);
        assert_eq!(
            captured_requests(&fixture.requests).len(),
            expected_requests
        );
        assert_fixture_quiescent(&fixture);
    }

    #[tokio::test]
    async fn pre_start_and_active_turn_cancellation_use_the_native_boundary() {
        with_watchdog(async {
            assert_prestart_cancellation("cancel-before-initialize", 1).await;
            assert_prestart_cancellation("cancel-during-thread-start", 4).await;

            let fixture = ProcessFixture::new("cancellation-blocked", AgentValueMode::None, 1024);
            let mut running = RunningCancellationFixture::start(fixture);
            running.await_started().await;
            wait_for_fixture_file(&running.fixture.ready).await;
            running.cancel();
            let (fixture, outcome) = running.finish().await;
            assert_eq!(
                captured_requests(&fixture.requests).last().unwrap()["method"],
                "turn/interrupt",
            );
            assert_cancelled_rollout_retained(&fixture, outcome);

            let fixture =
                ProcessFixture::new("cancellation-pending-request", AgentValueMode::None, 1024);
            let mut running = RunningCancellationFixture::start(fixture);
            running.await_started().await;
            wait_for_fixture_file(&running.fixture.ready).await;
            running.cancel();
            let (fixture, outcome) = running.finish().await;
            let requests = captured_requests(&fixture.requests);
            assert_eq!(requests.last().unwrap()["method"], "turn/interrupt");
            if requests.len() == 7 {
                assert_eq!(requests[5]["result"], json!({"decision": "decline"}));
            } else {
                assert_eq!(requests.len(), 6, "{requests:?}");
            }
            assert_cancelled_rollout_retained(&fixture, outcome);
        })
        .await;
    }

    #[tokio::test]
    async fn cancellation_discards_partial_output_and_force_cleans_stubborn_descendants() {
        with_watchdog(async {
            let fixture = ProcessFixture::new("cancellation-after-output", response_mode(), 1024);
            let mut running = RunningCancellationFixture::start(fixture);
            running.await_started().await;
            wait_for_fixture_file(&running.fixture.ready).await;
            running.cancel();
            let (fixture, outcome) = running.finish().await;
            assert_cancelled_rollout_retained(&fixture, outcome);

            let fixture = ProcessFixture::new("cancellation-stubborn", AgentValueMode::None, 1024);
            let mut running = RunningCancellationFixture::start(fixture);
            running.await_started().await;
            wait_for_fixture_file(&running.fixture.ready).await;
            running.cancel();
            wait_for_fixture_file(&running.fixture.descendant).await;
            assert!(!running.task.is_finished());
            running.process_control.force();
            let (fixture, outcome) = running.finish().await;
            assert_cancelled_rollout_retained(&fixture, outcome);
        })
        .await;
    }
}
