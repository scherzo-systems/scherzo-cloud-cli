use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::future::{Future, pending, ready};
use std::io::Write as _;
use std::num::NonZeroU64;
use std::os::fd::OwnedFd;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rustix::process::{Pid, getpgid};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use super::adapter::{PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL, PiJsonV1Adapter};
use super::*;
use crate::execution::pi::{
    PI_JSON_V1_QUALIFICATION_VERSION, PiCompatibilityProfile, validate_pi_installation,
};
use crate::execution::workflow::admission::{
    CancellationReason, CancellationSource, EnvironmentSnapshot,
};
use crate::execution::workflow::agent::{
    AdmittedAgentAdapter, AgentCompatibilityProfile, AgentInvocation, AgentInvocationIdentity,
    AgentInvocationStaging, AgentObservation, AgentObservationEnvelope, AgentObservationSink,
    AgentOutcome, AgentProcessControl, AgentPrompt, AgentStartReceiver, AgentTerminalReceiver,
    AgentValueMode, CompletedAgentInvocation, RetainedJsonSchema, StagedAgentAttachment,
    WorkflowRunId, agent_start_channel, agent_terminal_channel, failed_agent_outcome,
    invoke_agent_adapter,
};
// The black-box fixture intentionally owns its imports instead of depending on the
// executable-stub fixture module solely to share test wiring.
// jscpd:ignore-start
use crate::execution::workflow::agent_diagnostics::AgentDiagnosticSessionStore;
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::execution_root::AdmittedExecutionRoot;
use crate::execution::workflow::observation::NoopExecutionObserver;
use crate::execution::workflow::pi::{PiConfig, Thinking};
use crate::execution::workflow::result_validation::{
    ResultValidationWorker, ValidationWorkerRequest,
};
use crate::execution::workflow::runtime::{ActionId, TransitionSequence};
// jscpd:ignore-end

const FAKE_PROVIDER_EXTENSION: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/execution/workflow/pi-json-v1-extension/src/conformance/fake-provider.ts"
));
const PINNED_TEST_WATCHDOG: Duration = Duration::from_secs(20);
const MODEL_NAME: &str = "scherzo-fake/conformance";

fn conformance_executable() -> Option<PathBuf> {
    let expected_suffix = format!("-pi-{PI_JSON_V1_QUALIFICATION_VERSION}/bin/pi");
    std::env::var_os("SCHERZO_PI_CONFORMANCE_EXECUTABLE")
        .map(PathBuf::from)
        .filter(|path| path.to_string_lossy().ends_with(&expected_suffix))
}

fn require_conformance_executable() -> PathBuf {
    conformance_executable().unwrap_or_else(|| {
        panic!(
            "SCHERZO_PI_CONFORMANCE_EXECUTABLE must name the pinned Pi {PI_JSON_V1_QUALIFICATION_VERSION} executable"
        )
    })
}

#[derive(Clone, Copy)]
struct ConformanceClock;

impl CoordinatorClock for ConformanceClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        if deadline == PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL {
            // Keep the outer anti-hang watchdog schedulable between OS-state probes.
            let probe = tokio::spawn(async {});
            let _ = probe.await;
        } else {
            pending().await
        }
    }
}

#[derive(Clone)]
struct RecordingSink {
    sender: mpsc::UnboundedSender<AgentObservationEnvelope>,
}

impl AgentObservationSink for RecordingSink {
    fn observe(&self, observation: AgentObservationEnvelope) -> impl Future<Output = ()> + Send {
        let _ = self.sender.send(observation);
        ready(())
    }
}

type ConformanceInvocation = AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, RecordingSink>;

struct ControlledRequest {
    value: Value,
    response: Option<oneshot::Sender<Value>>,
}

impl ControlledRequest {
    fn kind(&self) -> &str {
        self.value["kind"].as_str().unwrap_or("<invalid>")
    }

    fn release(mut self, response: Value) {
        self.response.take().unwrap().send(response).unwrap();
    }
}

struct FakeProviderController {
    socket_path: PathBuf,
    requests: mpsc::UnboundedReceiver<ControlledRequest>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeProviderController {
    fn start() -> Self {
        static NEXT_SOCKET: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let ordinal = NEXT_SOCKET.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let socket_path = Path::new("/tmp").join(format!(
            ".scherzo-pi-conformance-{}-{ordinal}.sock",
            std::process::id()
        ));
        let _ = fs::remove_file(&socket_path);
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let (request_sender, requests) = mpsc::unbounded_channel();
        let (shutdown_sender, mut shutdown) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else {
                            break;
                        };
                        let request_sender = request_sender.clone();
                        connections.spawn(async move {
                            serve_control_connection(stream, request_sender).await;
                        });
                    }
                }
            }
            connections.shutdown().await;
        });
        Self {
            socket_path,
            requests,
            shutdown: Some(shutdown_sender),
            task,
        }
    }

    async fn next(&mut self, expected_kind: &str) -> ControlledRequest {
        let request = self.requests.recv().await.unwrap();
        assert_eq!(
            request.kind(),
            expected_kind,
            "unexpected controlled transition"
        );
        request
    }

    async fn shutdown(mut self) {
        let _ = self.shutdown.take().unwrap().send(());
        self.task.await.unwrap();
        fs::remove_file(&self.socket_path).unwrap();
    }
}

async fn serve_control_connection(
    mut stream: tokio::net::UnixStream,
    requests: mpsc::UnboundedSender<ControlledRequest>,
) {
    let mut length = [0_u8; 4];
    if stream.read_exact(&mut length).await.is_err() {
        return;
    }
    let length = u32::from_be_bytes(length);
    if u64::from(length) > 16 * 1024 * 1024 {
        return;
    }
    let mut payload = vec![0_u8; usize::try_from(length).unwrap()];
    if stream.read_exact(&mut payload).await.is_err() {
        return;
    }
    let Ok(value) = serde_json::from_slice(&payload) else {
        return;
    };
    let (respond, response) = oneshot::channel();
    if requests
        .send(ControlledRequest {
            value,
            response: Some(respond),
        })
        .is_err()
    {
        return;
    }
    let Ok(response) = response.await else {
        return;
    };
    let Ok(payload) = serde_json::to_vec(&response) else {
        return;
    };
    let Ok(length) = u32::try_from(payload.len()) else {
        return;
    };
    if let Err(error) = stream.write_all(&length.to_be_bytes()).await {
        eprintln!("control response length write failed: {error}");
        return;
    }
    if let Err(error) = stream.write_all(&payload).await {
        eprintln!("control response payload write failed: {error}");
        return;
    }
    if let Err(error) = stream.shutdown().await {
        eprintln!("control response shutdown failed: {error}");
    }
}

struct RealPiFixture {
    _temporary: tempfile::TempDir,
    invocation: Option<ConformanceInvocation>,
    cancellation: CancellationSource,
    process_control: AgentProcessControl,
    controller: FakeProviderController,
    observations: mpsc::UnboundedReceiver<AgentObservationEnvelope>,
    diagnostics: StepDiagnosticLog,
    agent_directory: PathBuf,
    global_settings: Vec<u8>,
    global_auth: Vec<u8>,
    trust_path: PathBuf,
    project_directory: PathBuf,
    attachment_contents: Vec<Vec<u8>>,
    session_directory: PathBuf,
    session_metadata: PathBuf,
}

impl RealPiFixture {
    fn new(value_mode: AgentValueMode, retry: bool, hold_settlement: bool) -> Option<Self> {
        Self::new_with_options(value_mode, retry, hold_settlement, false, 600_000)
    }

    fn with_immediate_retry(value_mode: AgentValueMode) -> Option<Self> {
        Self::new_with_options(value_mode, true, false, false, 0)
    }

    fn with_threshold_compaction(value_mode: AgentValueMode) -> Option<Self> {
        Self::new_with_options(value_mode, false, false, true, 600_000)
    }

    fn new_with_options(
        value_mode: AgentValueMode,
        retry: bool,
        hold_settlement: bool,
        force_compaction: bool,
        retry_base_delay_ms: u64,
    ) -> Option<Self> {
        let executable = conformance_executable()?;
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let project_directory = execution_root.join("worktree");
        let staging = temporary.path().join("staging");
        let result_endpoint = staging.join("result-endpoint");
        let agent_directory = temporary.path().join("agent");
        let home = temporary.path().join("home");
        let xdg_config = temporary.path().join("xdg-config");
        let xdg_cache = temporary.path().join("xdg-cache");
        let xdg_data = temporary.path().join("xdg-data");
        let xdg_state = temporary.path().join("xdg-state");
        let attempt_directory = temporary.path().join("run/attempts/000001");
        for directory in [
            &project_directory,
            &staging,
            &result_endpoint,
            &agent_directory,
            &home,
            &xdg_config,
            &xdg_cache,
            &xdg_data,
            &xdg_state,
            &attempt_directory,
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        fs::set_permissions(&result_endpoint, fs::Permissions::from_mode(0o700)).unwrap();

        materialize_project(&project_directory, retry, retry_base_delay_ms);
        let mut settings = json!({"defaultProjectTrust": "ask"});
        if force_compaction {
            settings["compaction"] = json!({"keepRecentTokens": 100});
        }
        let mut global_settings = serde_json::to_vec(&settings).unwrap();
        global_settings.push(b'\n');
        let global_auth = b"{}\n".to_vec();
        fs::write(agent_directory.join("settings.json"), &global_settings).unwrap();
        fs::write(agent_directory.join("auth.json"), &global_auth).unwrap();
        let trust_path = agent_directory.join("trust.json");
        assert!(!trust_path.exists());

        let attachment_contents = vec![
            b"first ordered text attachment\n".to_vec(),
            one_pixel_png(),
            b"third ordered non-image attachment\n".to_vec(),
        ];
        let attachment_paths = attachment_contents
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                let path = staging.join(format!("attachment-{index}"));
                fs::write(&path, bytes).unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
                path
            })
            .collect::<Vec<_>>();

        let controller = FakeProviderController::start();
        let path = std::env::var_os("PATH").unwrap_or_else(|| OsString::from("/usr/bin:/bin"));
        let mut environment = BTreeMap::<OsString, OsString>::from([
            (OsString::from("PATH"), path),
            (OsString::from("HOME"), home.into_os_string()),
            (
                OsString::from("PI_CODING_AGENT_DIR"),
                agent_directory.as_os_str().to_owned(),
            ),
            (
                OsString::from("XDG_CONFIG_HOME"),
                xdg_config.into_os_string(),
            ),
            (OsString::from("XDG_CACHE_HOME"), xdg_cache.into_os_string()),
            (OsString::from("XDG_DATA_HOME"), xdg_data.into_os_string()),
            (OsString::from("XDG_STATE_HOME"), xdg_state.into_os_string()),
            (OsString::from("PI_OFFLINE"), OsString::from("1")),
            (OsString::from("PI_SKIP_VERSION_CHECK"), OsString::from("1")),
            (OsString::from("PI_TELEMETRY"), OsString::from("0")),
            (OsString::from("FORCE_COLOR"), OsString::from("0")),
            (OsString::from("NO_COLOR"), OsString::from("1")),
            (
                OsString::from("SCHERZO_PI_FAKE_PROVIDER_SOCKET"),
                controller.socket_path.as_os_str().to_owned(),
            ),
            (
                OsString::from("SCHERZO_PI_STUBBORN_FIXTURE_EXECUTABLE"),
                std::env::current_exe().unwrap().into_os_string(),
            ),
        ]);
        if hold_settlement {
            environment.insert(
                OsString::from("SCHERZO_PI_FAKE_HOLD_SETTLEMENT"),
                OsString::from("1"),
            );
        }
        let admitted_root = AdmittedExecutionRoot::admit(&execution_root).unwrap();
        let working_directory = admitted_root
            .select_working_directory(Some("worktree"))
            .unwrap();
        let run = format!(
            "pinned-pi-conformance-{}",
            temporary.path().file_name().unwrap().to_string_lossy()
        );
        let identity = AgentInvocationIdentity::new(
            WorkflowRunId::from(Arc::from(run)),
            Arc::from("agent-step"),
            ActionId {
                transition_sequence: TransitionSequence::default(),
            },
        );
        let (observation_sender, observations) = mpsc::unbounded_channel();
        let cancellation = CancellationSource::new();
        let adapter = AdmittedAgentAdapter::new(
            AgentCompatibilityProfile::PiJsonV1,
            fs::canonicalize(executable).unwrap(),
            Arc::from(PI_JSON_V1_QUALIFICATION_VERSION),
            PiConfig {
                model: MODEL_NAME.to_owned(),
                thinking: Thinking::XHigh,
            },
        );
        let attempt_handle: OwnedFd = fs::File::open(&attempt_directory).unwrap().into();
        let diagnostic_sessions = AgentDiagnosticSessionStore::create(
            &attempt_handle,
            &attempt_directory,
            Arc::from("00000000-0000-4000-8000-000000000001"),
            1,
        )
        .unwrap();
        let diagnostic_session = diagnostic_sessions
            .allocate(
                &identity,
                AgentCompatibilityProfile::PiJsonV1,
                PI_JSON_V1_QUALIFICATION_VERSION,
            )
            .unwrap();
        let session_directory = diagnostic_session
            .pi_native_session_directory()
            .unwrap()
            .to_owned();
        let session_metadata = session_directory.parent().unwrap().join("metadata.json");
        let invocation = AgentInvocation::new(
            identity,
            adapter,
            crate::execution::workflow::agent::AgentProcessContext::new(
                working_directory,
                EnvironmentSnapshot::new(environment),
            ),
            AgentInvocationStaging::new(result_endpoint),
            diagnostic_session,
            AgentPrompt::new(
                Arc::from("WORKFLOW_SYSTEM_PROMPT_MARKER"),
                Arc::from("@/caller-controlled/path\n--option-looking\nmessage tail"),
            ),
            Arc::from([
                StagedAgentAttachment::new(
                    attachment_paths[0].clone(),
                    Arc::from("text/plain"),
                    None,
                ),
                StagedAgentAttachment::new(
                    attachment_paths[1].clone(),
                    Arc::from("image/png"),
                    None,
                ),
                StagedAgentAttachment::new(
                    attachment_paths[2].clone(),
                    Arc::from("application/octet-stream"),
                    None,
                ),
            ]),
            value_mode,
            super::adapter_tests::invocation_limits(),
            cancellation.clone(),
            crate::execution::workflow::process_group::ProcessGuardRegistry::default(),
            RecordingSink {
                sender: observation_sender,
            },
        );
        let process_control = invocation.process_control().clone();
        Some(Self {
            _temporary: temporary,
            invocation: Some(invocation),
            cancellation,
            process_control,
            controller,
            observations,
            diagnostics: StepDiagnosticLog::default(),
            agent_directory,
            global_settings,
            global_auth,
            trust_path,
            project_directory,
            attachment_contents,
            session_directory,
            session_metadata,
        })
    }

    fn assert_configuration_unchanged(&self) {
        self.assert_retained_diagnostic_state();
        let native_sessions = self.native_sessions();
        assert_eq!(
            native_sessions.len(),
            1,
            "a completed assistant message must retain one native Pi session artifact"
        );
        assert!(native_sessions[0].is_file());
        assert!(!fs::read(&native_sessions[0]).unwrap().is_empty());
    }

    fn assert_pre_response_cancellation_state(&self) {
        self.assert_retained_diagnostic_state();
        assert!(
            self.native_sessions().is_empty(),
            "Pi 0.84 must not synthesize a session file before an assistant message completes"
        );
    }

    fn assert_retained_diagnostic_state(&self) {
        assert_eq!(
            fs::read(self.agent_directory.join("settings.json")).unwrap(),
            self.global_settings
        );
        assert_eq!(
            fs::read(self.agent_directory.join("auth.json")).unwrap(),
            self.global_auth
        );
        assert!(
            !self.trust_path.exists(),
            "--approve must not persist project trust"
        );
        assert!(
            !self.agent_directory.join("sessions").exists(),
            "the profile-owned session directory must override ambient storage"
        );
        assert!(self.session_directory.is_absolute());
        assert_eq!(
            fs::metadata(&self.session_directory)
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o700
        );
        let metadata: Value =
            serde_json::from_slice(&fs::read(&self.session_metadata).unwrap()).unwrap();
        assert_eq!(
            metadata,
            json!({
                "schemaVersion": 1,
                "localRunId": "00000000-0000-4000-8000-000000000001",
                "attemptNumber": 1,
                "stepId": "agent-step",
                "invocationId": 0,
                "profile": "PiJsonV1",
                "piVersion": PI_JSON_V1_QUALIFICATION_VERSION,
                "nativeSession": {
                    "relativeDirectory": "session",
                    "formatVersion": 3
                }
            })
        );
    }

    fn native_sessions(&self) -> Vec<PathBuf> {
        fs::read_dir(&self.session_directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    }
}

fn materialize_project(project: &Path, retry: bool, retry_base_delay_ms: u64) {
    fs::create_dir_all(project.join(".pi/extensions")).unwrap();
    fs::create_dir_all(project.join(".pi/prompts")).unwrap();
    fs::create_dir_all(project.join(".pi/skills/pi-resource-skill")).unwrap();
    fs::create_dir_all(project.join(".agents/skills/agents-resource-skill")).unwrap();
    fs::write(
        project.join("AGENTS.md"),
        "PROJECT_CONTEXT_MARKER from AGENTS.md\n",
    )
    .unwrap();
    fs::write(
        project.join(".pi/APPEND_SYSTEM.md"),
        "PROJECT_APPEND_SYSTEM_MARKER\n",
    )
    .unwrap();
    fs::write(
        project.join(".pi/prompts/project-template.md"),
        "Project prompt template marker\n",
    )
    .unwrap();
    for (path, name) in [
        (
            project.join(".pi/skills/pi-resource-skill/SKILL.md"),
            "pi-resource-skill",
        ),
        (
            project.join(".agents/skills/agents-resource-skill/SKILL.md"),
            "agents-resource-skill",
        ),
    ] {
        fs::write(
            path,
            format!(
                "---\nname: {name}\ndescription: Project resource proof\n---\n\nSKILL_MARKER_{name}\n"
            ),
        )
        .unwrap();
    }
    fs::write(
        project.join(".pi/extensions/fake-provider.ts"),
        FAKE_PROVIDER_EXTENSION,
    )
    .unwrap();
    fs::write(
        project.join("settings-proof.ts"),
        concat!(
            "import type { ExtensionAPI } from \"@earendil-works/pi-coding-agent\";\n",
            "import { Type } from \"typebox\";\n",
            "export default function (pi: ExtensionAPI) {\n",
            "  pi.registerTool({ name: \"settings_proof\", label: \"Settings proof\", ",
            "description: \"Proves project settings extension loading\", ",
            "parameters: Type.Object({}), async execute() { return { content: ",
            "[{ type: \"text\" as const, text: \"settings loaded\" }], details: {} }; } });\n",
            "}\n"
        ),
    )
    .unwrap();
    fs::write(
        project.join(".pi/settings.json"),
        serde_json::to_vec_pretty(&json!({
            "extensions": ["../settings-proof.ts"],
            "skills": ["skills/pi-resource-skill/SKILL.md"],
            "retry": {
                "enabled": retry,
                "maxRetries": 3,
                "baseDelayMs": retry_base_delay_ms
            }
        }))
        .unwrap(),
    )
    .unwrap();
}

fn one_pixel_png() -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAIAAAD8GO2jAAAAJ0lEQVR42u3NsQkAAAjAsP7/tF7hIASyp6lTCQQCgUAgEAgEgi/BAjLD/C5w/SM9AAAAAElFTkSuQmCC")
        .unwrap()
}

fn result_mode() -> AgentValueMode {
    let document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "count": {"type": "integer", "minimum": 1},
            "x": {"type": "string"}
        },
        "patternProperties": {"^x$": {"type": "string"}},
        "required": ["count"],
        "additionalProperties": false
    });
    result_value_mode(document)
}

fn result_value_mode(document: Value) -> AgentValueMode {
    let bytes = Arc::<[u8]>::from(serde_json::to_vec(&document).unwrap());
    AgentValueMode::Result {
        output: Arc::from("result"),
        schema: RetainedJsonSchema::compile(bytes, Arc::new(document)).unwrap(),
    }
}

fn streamed_nested_result_mode() -> AgentValueMode {
    let document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "summary": {"type": "string"},
            "sections": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "heading": {"type": "string"},
                        "tickets": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "ordinal": {"type": "integer"},
                                    "title": {"type": "string"},
                                    "description": {"type": "string"},
                                    "labels": {
                                        "type": "array",
                                        "items": {"type": "string"}
                                    }
                                },
                                "required": ["ordinal", "title", "description", "labels"],
                                "additionalProperties": false
                            }
                        }
                    },
                    "required": ["heading", "tickets"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["summary", "sections"],
        "additionalProperties": false
    });
    result_value_mode(document)
}

fn streamed_nested_result() -> Value {
    let sections = (0..32)
        .map(|ordinal| {
            json!({
                "heading": format!("Section {ordinal:02}"),
                "tickets": [{
                    "ordinal": ordinal,
                    "title": format!("Nested work item {ordinal:02}"),
                    "description": format!(
                        "{}-{ordinal:02}",
                        "qualification-payload-".repeat(10)
                    ),
                    "labels": ["workflow", "deterministic"]
                }]
            })
        })
        .collect::<Vec<_>>();
    json!({
        "summary": "Deterministic streamed result retained without semantic transformation.",
        "sections": sections
    })
}

struct RunningRealPi {
    fixture: RealPiFixture,
    task: tokio::task::JoinHandle<()>,
    started: Option<AgentStartReceiver>,
    terminal: AgentTerminalReceiver,
}

impl RunningRealPi {
    fn launch(fixture: RealPiFixture) -> Self {
        let mut fixture = fixture;
        let invocation = fixture.invocation.take().unwrap();
        let value_mode = invocation.value_mode().clone();
        let (started_callback, started) = agent_start_channel();
        let (terminal_callback, terminal) = agent_terminal_channel(&value_mode);
        let adapter = PiJsonV1Adapter::with_validation_worker(
            fixture.diagnostics.clone(),
            NonZeroU64::new(16 * 1024).unwrap(),
            ConformanceClock,
            NoopExecutionObserver,
            super::adapter_tests::InlineValidationWorker,
        );
        let task = tokio::spawn(async move {
            invoke_agent_adapter(&adapter, invocation, started_callback, terminal_callback).await;
        });
        Self {
            fixture,
            task,
            started: Some(started),
            terminal,
        }
    }

    async fn release_startup(&mut self) -> Value {
        let request = self.fixture.controller.next("before_agent_start").await;
        let value = request.value.clone();
        request.release(json!({"kind": "release"}));
        value
    }

    async fn await_started(&mut self) {
        self.started.take().unwrap().receive().await.unwrap();
    }

    async fn finish(self) -> (RealPiFixture, AgentOutcome) {
        let outcome = self.terminal.receive().await.unwrap();
        self.task.await.unwrap();
        (self.fixture, outcome)
    }

    fn into_finishing(
        self,
    ) -> (
        RealPiFixture,
        tokio::task::JoinHandle<()>,
        AgentTerminalReceiver,
    ) {
        let Self {
            fixture,
            task,
            started,
            terminal,
        } = self;
        assert!(started.is_none());
        (fixture, task, terminal)
    }
}

fn normalized_semantics(mut value: Value, isolation_root: &Path) -> Value {
    fn normalize(value: &mut Value, isolation_roots: &[&str]) {
        match value {
            Value::Object(object) => {
                object.remove("timestamp");
                for child in object.values_mut() {
                    normalize(child, isolation_roots);
                }
            }
            Value::Array(values) => {
                for child in values {
                    normalize(child, isolation_roots);
                }
            }
            Value::String(text) => {
                for isolation_root in isolation_roots {
                    *text = text.replace(isolation_root, "<isolated-root>");
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    let resolved_isolation_root = std::fs::canonicalize(isolation_root).unwrap();
    normalize(
        &mut value,
        &[
            isolation_root.to_str().unwrap(),
            resolved_isolation_root.to_str().unwrap(),
        ],
    );
    value
}

fn tool_names(request: &Value) -> BTreeSet<&str> {
    request["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect()
}

fn result_tool_name(request: &Value) -> &str {
    tool_names(request)
        .into_iter()
        .find(|name| name.starts_with("scherzo_result_"))
        .unwrap()
}

fn retained_assistant_tool_call(
    fixture: &RealPiFixture,
    tool_name: &str,
    tool_call_id: &str,
) -> Value {
    for session in fixture.native_sessions() {
        let bytes = fs::read(session).unwrap();
        for line in bytes.split(|byte| *byte == b'\n') {
            let Ok(entry) = serde_json::from_slice::<Value>(line) else {
                continue;
            };
            let Some(content) = entry["message"]["content"].as_array() else {
                continue;
            };
            if entry["type"] == "message"
                && entry["message"]["role"] == "assistant"
                && content.iter().any(|block| {
                    block["type"] == "toolCall"
                        && block["id"] == tool_call_id
                        && block["name"] == tool_name
                })
            {
                return entry["message"].clone();
            }
        }
    }
    panic!("retained Pi session did not contain the expected assistant tool call");
}

async fn run_terminal_case(
    value_mode: AgentValueMode,
    response: Value,
) -> Option<(RealPiFixture, AgentOutcome)> {
    let fixture = RealPiFixture::new(value_mode, false, false)?;
    let mut running = RunningRealPi::launch(fixture);
    running.release_startup().await;
    let request = running.fixture.controller.next("model").await;
    request.release(response);
    running.await_started().await;
    Some(running.finish().await)
}

async fn launch_result_case() -> (RunningRealPi, ControlledRequest, String) {
    let fixture = RealPiFixture::new(result_mode(), false, false).unwrap();
    let mut running = RunningRealPi::launch(fixture);
    running.release_startup().await;
    let first = running.fixture.controller.next("model").await;
    let tool_name = result_tool_name(&first.value).to_owned();
    (running, first, tool_name)
}

fn count_one_result_response(call_id: &str, tool_name: &str) -> Value {
    json!({
        "kind": "toolCalls",
        "calls": [{
            "id": call_id,
            "name": tool_name,
            "arguments": {"result": {"count": 1}}
        }]
    })
}

async fn assert_count_one_result(fixture: RealPiFixture, outcome: AgentOutcome) {
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
        panic!("corrected terminating result did not complete");
    };
    assert_eq!(result.value(), &json!({"count": 1}));
    fixture.assert_configuration_unchanged();
    fixture.controller.shutdown().await;
}

#[test]
#[ignore = "requires pinned harness"]
fn pinned_real_pi_00_fake_provider_has_no_network_or_timer_success_path() {
    let _executable = require_conformance_executable();
    assert!(FAKE_PROVIDER_EXTENSION.contains("from \"node:net\""));
    for forbidden in [
        "fetch(",
        "setTimeout(",
        "setInterval(",
        "http.request",
        "https.request",
    ] {
        assert!(
            !FAKE_PROVIDER_EXTENSION.contains(forbidden),
            "fake provider must not contain {forbidden}"
        );
    }
}

#[test]
#[ignore = "requires pinned harness"]
fn pinned_real_pi_01_qualification_anchor_is_exact_and_supported() {
    let executable = require_conformance_executable();
    let installation = validate_pi_installation(&executable).unwrap();
    assert_eq!(
        installation.version().as_str(),
        PI_JSON_V1_QUALIFICATION_VERSION
    );
    assert_eq!(installation.profile(), PiCompatibilityProfile::PiJsonV1);
    println!(
        "qualified Pi version={} profile={} range={} executable={}",
        installation.version().as_str(),
        installation.profile().as_str(),
        crate::execution::pi::PI_JSON_V1_SUPPORTED_RANGE,
        installation.executable().display()
    );
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_pi_02_launch_resources_attachments_and_response_conform() {
    let _executable = require_conformance_executable();
    tokio::time::timeout(PINNED_TEST_WATCHDOG, async {
        let fixture = RealPiFixture::new(
            AgentValueMode::Response {
                output: Arc::from("response"),
            },
            false,
            false,
        )
        .unwrap();
        let mut running = RunningRealPi::launch(fixture);
        let startup = running.release_startup().await;
        assert_eq!(startup["projectTrusted"], true);
        assert!(
            startup["prompt"]
                .as_str()
                .unwrap()
                .contains("@/caller-controlled/path")
        );
        let system_prompt = startup["systemPrompt"].as_str().unwrap();
        for marker in [
            "WORKFLOW_SYSTEM_PROMPT_MARKER",
            "PROJECT_CONTEXT_MARKER",
            "PROJECT_APPEND_SYSTEM_MARKER",
        ] {
            assert!(system_prompt.contains(marker), "missing {marker}");
        }
        let commands = startup["commands"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|command| command["name"].as_str())
            .collect::<BTreeSet<_>>();
        assert!(commands.contains("project-template"));
        assert!(
            commands.contains("skill:pi-resource-skill"),
            "commands: {commands:?}"
        );
        assert!(
            commands.contains("skill:agents-resource-skill"),
            "commands: {commands:?}"
        );
        let startup_tools = startup["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect::<BTreeSet<_>>();
        assert!(startup_tools.contains("settings_proof"));
        assert!(startup_tools.contains("conformance_gate"));

        let model_request = running.fixture.controller.next("model").await;
        let model_semantics = model_request.value.clone();
        assert_eq!(model_request.value["reasoning"], "xhigh");
        let messages = model_request.value["messages"].as_array().unwrap();
        let serialized_messages = serde_json::to_string(messages).unwrap();
        let first_attachment = serialized_messages
            .find("first ordered text attachment")
            .unwrap();
        let image_attachment = serialized_messages
            .find("attachment-1")
            .unwrap_or_else(|| panic!("ordered image path missing from provider context"));
        let third_attachment = serialized_messages
            .find("third ordered non-image attachment")
            .unwrap();
        assert!(first_attachment < image_attachment);
        assert!(image_attachment < third_attachment);
        assert!(serialized_messages.contains("@/caller-controlled/path"));
        assert!(tool_names(&model_request.value).contains("settings_proof"));
        model_request.release(json!({
            "kind": "text",
            "blocks": ["hello", " world"],
            "stopReason": "stop"
        }));

        running.await_started().await;
        let (mut fixture, outcome) = running.finish().await;
        if !matches!(outcome, AgentOutcome::Completed(_)) {
            eprintln!("real Pi outcome: {outcome:?}");
            eprintln!(
                "real Pi diagnostic: {:?}",
                fixture.diagnostics.get("agent-step")
            );
            while let Ok(observation) = fixture.observations.try_recv() {
                eprintln!("real Pi observation: {:?}", observation.observation());
            }
        }
        assert_eq!(
            outcome,
            AgentOutcome::Completed(CompletedAgentInvocation::Response(
                crate::execution::workflow::agent::BoundedAgentResponse::from_bounded(Arc::from(
                    "hello world"
                ))
            ))
        );
        assert_eq!(
            fixture.attachment_contents[0],
            b"first ordered text attachment\n"
        );
        fixture.assert_configuration_unchanged();
        assert!(
            fixture
                .project_directory
                .join(".pi/extensions/fake-provider.ts")
                .exists()
        );
        let first_semantics = normalized_semantics(
            json!({"startup": startup, "model": model_semantics}),
            fixture._temporary.path(),
        );
        let first_session_directory = fixture.session_directory.clone();
        fixture.controller.shutdown().await;

        let repeated = RealPiFixture::new(
            AgentValueMode::Response {
                output: Arc::from("response"),
            },
            false,
            false,
        )
        .unwrap();
        let mut repeated = RunningRealPi::launch(repeated);
        let repeated_startup = repeated.release_startup().await;
        let repeated_model = repeated.fixture.controller.next("model").await;
        let repeated_model_semantics = repeated_model.value.clone();
        repeated_model.release(json!({
            "kind": "text",
            "blocks": ["hello", " world"],
            "stopReason": "stop"
        }));
        repeated.await_started().await;
        let (repeated, repeated_outcome) = repeated.finish().await;
        assert!(matches!(
            repeated_outcome,
            AgentOutcome::Completed(CompletedAgentInvocation::Response(_))
        ));
        assert_eq!(
            normalized_semantics(
                json!({"startup": repeated_startup, "model": repeated_model_semantics}),
                repeated._temporary.path(),
            ),
            first_semantics
        );
        repeated.assert_configuration_unchanged();
        assert_ne!(repeated.session_directory, first_session_directory);
        repeated.controller.shutdown().await;
    })
    .await
    .expect("pinned real-Pi launch conformance watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_pi_03_no_value_and_typed_terminal_failures_conform() {
    let _executable = require_conformance_executable();
    tokio::time::timeout(PINNED_TEST_WATCHDOG, async {
        let cases = [
            (
                AgentValueMode::None,
                json!({"kind": "text", "blocks": ["ignored in no-value mode"], "stopReason": "stop"}),
                AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
            ),
            (
                AgentValueMode::Response { output: Arc::from("response") },
                json!({"kind": "text", "blocks": ["truncated"], "stopReason": "length"}),
                failed_agent_outcome(AgentFailureCause::HarnessFailed {
                    detail: crate::execution::workflow::agent::AgentHarnessFailureDetail::ModelOutputTruncated,
                }),
            ),
            (
                AgentValueMode::None,
                json!({"kind": "failure", "stopReason": "error", "message": "deterministic model rejection"}),
                failed_agent_outcome(AgentFailureCause::HarnessFailed {
                    detail: crate::execution::workflow::agent::AgentHarnessFailureDetail::ModelError,
                }),
            ),
            (
                AgentValueMode::None,
                json!({"kind": "failure", "stopReason": "aborted", "message": "provider aborted"}),
                failed_agent_outcome(AgentFailureCause::HarnessFailed {
                    detail: crate::execution::workflow::agent::AgentHarnessFailureDetail::ModelAborted,
                }),
            ),
            (
                AgentValueMode::Response { output: Arc::from("response") },
                json!({"kind": "toolCalls", "calls": [{"id": "call-terminal", "name": "conformance_terminate", "arguments": {}}]}),
                failed_agent_outcome(AgentFailureCause::HarnessFailed {
                    detail: crate::execution::workflow::agent::AgentHarnessFailureDetail::UnexpectedTerminalToolUse,
                }),
            ),
        ];
        for (value_mode, response, expected) in cases {
            let (fixture, outcome) = run_terminal_case(value_mode, response).await.unwrap();
            assert_eq!(outcome, expected);
            fixture.assert_configuration_unchanged();
            fixture.controller.shutdown().await;
        }
    })
    .await
    .expect("pinned real-Pi terminal conformance watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_pi_04_result_rejection_sibling_correction_and_termination_conform() {
    let _executable = require_conformance_executable();
    tokio::time::timeout(PINNED_TEST_WATCHDOG, async {
        let (mut running, first, tool_name) = launch_result_case().await;
        first.release(json!({
            "kind": "toolCalls",
            "calls": [{"id": "call-invalid", "name": tool_name, "arguments": {"result": {"count": 0}}}]
        }));

        loop {
            let Some(observation) = running.fixture.observations.recv().await else {
                panic!(
                    "real Pi ended before schema rejection: {:?}",
                    running.fixture.diagnostics.get("agent-step")
                );
            };
            if matches!(
                observation.observation(),
                AgentObservation::ValueRejected {
                    kind: AgentValueKind::Result,
                    ..
                }
            ) {
                break;
            }
        }
        let corrected_after_schema_rejection = running.fixture.controller.next("model").await;
        let messages = corrected_after_schema_rejection.value["messages"]
            .as_array()
            .unwrap();
        assert!(messages.iter().any(|message| {
            message["role"] == "toolResult"
                && message["toolCallId"] == "call-invalid"
                && message["isError"] == true
                && serde_json::to_string(message)
                    .unwrap()
                    .contains("workflow schema")
        }));
        corrected_after_schema_rejection.release(json!({
            "kind": "toolCalls",
            "calls": [
                {"id": "call-sibling-result", "name": tool_name, "arguments": {"result": {"count": 2}}},
                {"id": "call-sibling-tool", "name": "conformance_gate", "arguments": {"value": "sibling"}}
            ]
        }));

        let sibling_tool = running.fixture.controller.next("tool").await;
        assert_eq!(sibling_tool.value["toolCallId"], "call-sibling-tool");
        sibling_tool.release(json!({"kind": "release"}));
        let corrected_after_sibling = running.fixture.controller.next("model").await;
        let messages = corrected_after_sibling.value["messages"].as_array().unwrap();
        assert!(messages.iter().any(|message| {
            message["role"] == "toolResult"
                && message["toolCallId"] == "call-sibling-result"
                && message["isError"] == true
                && serde_json::to_string(message)
                    .unwrap()
                    .contains("without sibling tool calls")
        }));
        corrected_after_sibling.release(json!({
            "kind": "toolCalls",
            "calls": [{"id": "call-valid", "name": tool_name, "arguments": {"result": {"count": 1}}}]
        }));

        running.await_started().await;
        let (fixture, outcome) = running.finish().await;
        assert_count_one_result(fixture, outcome).await;
    })
    .await
    .expect("pinned real-Pi result conformance watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_pi_04_accepted_result_cancels_threshold_compaction() {
    let _executable = require_conformance_executable();
    tokio::time::timeout(PINNED_TEST_WATCHDOG, async {
        let fixture = RealPiFixture::with_threshold_compaction(result_mode()).unwrap();
        let mut running = RunningRealPi::launch(fixture);
        running.release_startup().await;
        let model_request = running.fixture.controller.next("model").await;
        let tool_name = result_tool_name(&model_request.value).to_owned();
        model_request.release(json!({
            "kind": "toolCalls",
            "calls": [{
                "id": "call-compaction-history",
                "name": "conformance_gate",
                "arguments": {"value": "compaction-history".repeat(1024)}
            }]
        }));
        let tool_request = running.fixture.controller.next("tool").await;
        assert_eq!(
            tool_request.value["toolCallId"],
            "call-compaction-history"
        );
        tool_request.release(json!({"kind": "release"}));
        let result_request = running.fixture.controller.next("model").await;
        result_request.release(json!({
            "kind": "toolCalls",
            "inputTokens": 127_000,
            "calls": [{
                "id": "call-threshold-result",
                "name": tool_name,
                "arguments": {"result": {"count": 1}}
            }]
        }));

        running.await_started().await;
        let (mut fixture, outcome) = running.finish().await;
        let milestones = std::iter::from_fn(|| fixture.observations.try_recv().ok())
            .filter_map(|observation| match observation.observation() {
                AgentObservation::Lifecycle { milestone } => Some(*milestone),
                _ => None,
            })
            .collect::<Vec<_>>();
        let compaction_started = milestones
            .iter()
            .position(|milestone| {
                *milestone
                    == crate::execution::workflow::agent::AgentLifecycleMilestone::CompactionStarted
            })
            .expect("Pi must attempt threshold compaction after the accepted result");
        let compaction_completed = milestones
            .iter()
            .position(|milestone| {
                *milestone
                    == crate::execution::workflow::agent::AgentLifecycleMilestone::CompactionCompleted
            })
            .expect("the extension must cancel threshold compaction cleanly");
        let harness_quiescent = milestones
            .iter()
            .position(|milestone| {
                *milestone
                    == crate::execution::workflow::agent::AgentLifecycleMilestone::HarnessQuiescent
            })
            .expect("Pi must settle after canceled threshold compaction");
        assert!(compaction_started < compaction_completed);
        assert!(compaction_completed < harness_quiescent);
        assert_count_one_result(fixture, outcome).await;
    })
    .await
    .expect("pinned real-Pi threshold-compaction settlement watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_pi_04_provider_finalized_thinking_reaches_result_settlement() {
    let _executable = require_conformance_executable();
    tokio::time::timeout(PINNED_TEST_WATCHDOG, async {
        const FIRST_REASONING_SUMMARY: &str = "Inspecting the requested result shape.";
        const SECOND_REASONING_SUMMARY: &str = "Submitting one nested result call.";
        const FINALIZED_THINKING: &str =
            "Provider finalized the reasoning for the nested result call.";
        let fixture = RealPiFixture::new(streamed_nested_result_mode(), false, false).unwrap();
        let mut running = RunningRealPi::launch(fixture);
        running.release_startup().await;
        let model_request = running.fixture.controller.next("model").await;
        let tool_name = result_tool_name(&model_request.value).to_owned();
        let registered_tool = model_request.value["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == tool_name)
            .unwrap();
        assert_eq!(registered_tool["parameters"]["type"], "object");
        assert_eq!(registered_tool["parameters"]["required"], json!(["result"]));
        assert_eq!(
            registered_tool["parameters"]["additionalProperties"],
            false
        );

        let result_value = streamed_nested_result();
        let provider_arguments = json!({"result": result_value.clone()});
        let provider_bytes = serde_json::to_vec(&provider_arguments).unwrap();
        assert!(
            (10 * 1024..=12 * 1024).contains(&provider_bytes.len()),
            "representative provider arguments were {} bytes",
            provider_bytes.len()
        );
        model_request.release(json!({
            "kind": "streamedToolCall",
            "thinking": [FIRST_REASONING_SUMMARY, SECOND_REASONING_SUMMARY],
            "finalizedThinking": FINALIZED_THINKING,
            "call": {
                "id": "call-streamed-nested-result",
                "name": tool_name,
                "arguments": provider_arguments
            }
        }));
        running.await_started().await;

        let (mut fixture, task, terminal) = running.into_finishing();
        let mut terminal = Box::pin(terminal.receive());
        let outcome = tokio::select! {
            biased;
            correction = fixture.controller.next("model") => {
                let rejected_assistant = correction.value["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|message| {
                        message["role"] == "assistant"
                            && message["content"].as_array().is_some_and(|content| {
                                content.iter().any(|block| {
                                    block["type"] == "toolCall"
                                        && block["id"] == "call-streamed-nested-result"
                                })
                            })
                    })
                    .cloned()
                    .unwrap();
                correction.release(json!({
                    "kind": "text",
                    "blocks": ["No corrected result is provided."],
                    "stopReason": "stop"
                }));
                let outcome = terminal.await.unwrap();
                task.await.unwrap();
                fixture.controller.shutdown().await;
                panic!(
                    "provider-finalized result requested correction instead of settling; provider context assistant={rejected_assistant}; outcome={outcome:?}"
                );
            }
            outcome = &mut terminal => outcome.unwrap(),
        };
        task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("provider-finalized nested result did not complete: {outcome:?}");
        };

        let retained = retained_assistant_tool_call(
            &fixture,
            &tool_name,
            "call-streamed-nested-result",
        );
        let content = retained["content"].as_array().unwrap();
        let tool_call_index = content
            .iter()
            .position(|block| block["id"] == "call-streamed-nested-result")
            .unwrap();
        assert!(content[..tool_call_index].iter().any(|block| {
            block["type"] == "thinking" && block["thinking"] == FINALIZED_THINKING
        }));
        assert_eq!(
            content[tool_call_index]["arguments"],
            json!({"result": result_value.clone()})
        );
        let reasoning = std::iter::from_fn(|| fixture.observations.try_recv().ok())
            .filter_map(|observation| match observation.observation() {
                AgentObservation::Reasoning { text } => Some(text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            reasoning,
            [
                Arc::<str>::from(FIRST_REASONING_SUMMARY),
                Arc::<str>::from(SECOND_REASONING_SUMMARY)
            ]
        );
        assert!(!reasoning
            .iter()
            .any(|text| text.as_ref() == FINALIZED_THINKING));

        assert_eq!(result.value(), &result_value);
        let mut expected_canonical = Vec::new();
        crate::execution::workflow::canonical_json::to_writer(
            &mut expected_canonical,
            &result_value,
        )
        .unwrap();
        assert_eq!(result.canonical_json(), expected_canonical);
        println!(
            "provider-finalized result outcome=CompletedAgentInvocation::Result provider_arguments_bytes={}",
            provider_bytes.len()
        );

        fixture.assert_configuration_unchanged();
        fixture.controller.shutdown().await;
    })
    .await
    .expect("pinned real-Pi streamed-result conformance watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_pi_04_tolerates_thinking_end_snapshot_disagreement() {
    let _executable = require_conformance_executable();
    tokio::time::timeout(PINNED_TEST_WATCHDOG, async {
        const STREAMED_THINKING: &str = "An observational reasoning summary.";
        const FINALIZED_THINKING: &str = "The provider-finalized thinking snapshot.";
        const MISMATCHED_EVENT_CONTENT: &str = "A different thinking-end event snapshot.";
        let fixture = RealPiFixture::new(AgentValueMode::None, false, false).unwrap();
        let mut running = RunningRealPi::launch(fixture);
        running.release_startup().await;
        running.await_started().await;
        let model_request = running.fixture.controller.next("model").await;
        model_request.release(json!({
            "kind": "streamedToolCall",
            "thinking": [STREAMED_THINKING],
            "finalizedThinking": FINALIZED_THINKING,
            "thinkingEndContent": MISMATCHED_EVENT_CONTENT,
            "call": {
                "id": "call-mismatched-thinking-end",
                "name": "conformance_gate",
                "arguments": {"value": "continues"}
            }
        }));

        let tool_request = running.fixture.controller.next("tool").await;
        assert_eq!(
            tool_request.value["toolCallId"],
            "call-mismatched-thinking-end"
        );
        tool_request.release(json!({"kind": "release"}));
        let final_request = running.fixture.controller.next("model").await;
        final_request.release(json!({
            "kind": "text",
            "blocks": ["completed after observational mismatch"],
            "stopReason": "stop"
        }));

        let (mut fixture, outcome) = running.finish().await;
        assert!(matches!(
            outcome,
            AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
        ));
        let reasoning = std::iter::from_fn(|| fixture.observations.try_recv().ok())
            .filter_map(|observation| match observation.observation() {
                AgentObservation::Reasoning { text } => Some(text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            reasoning
                .iter()
                .any(|text| text.as_ref() == STREAMED_THINKING)
        );
        assert!(
            !reasoning
                .iter()
                .any(|text| text.as_ref() == FINALIZED_THINKING)
        );

        fixture.assert_retained_diagnostic_state();
        fixture.controller.shutdown().await;
    })
    .await
    .expect("pinned real-Pi thinking-end tolerance watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_pi_recovers_after_a_partial_tool_call_transport_failure() {
    let _executable = require_conformance_executable();
    tokio::time::timeout(PINNED_TEST_WATCHDOG, async {
        let fixture = RealPiFixture::with_immediate_retry(result_mode()).unwrap();
        let mut running = RunningRealPi::launch(fixture);
        running.release_startup().await;
        let interrupted = running.fixture.controller.next("model").await;
        let tool_name = result_tool_name(&interrupted.value).to_owned();
        interrupted.release(json!({
            "kind": "partialToolCallFailure",
            "thinking": ["Preparing work before the connection closes."],
            "call": {
                "id": "call-interrupted",
                "name": "conformance_gate",
                "arguments": {"value": "must-not-execute"}
            },
            "message": "WebSocket closed 1006 Connection ended"
        }));
        running.await_started().await;

        let (mut fixture, task, terminal) = running.into_finishing();
        let mut terminal = Box::pin(terminal.receive());
        // Receiving the retry model request rather than a tool request is the execution barrier:
        // Pi discarded the interrupted call before beginning any extension tool execution.
        let recovered = tokio::select! {
            request = fixture.controller.next("model") => request,
            outcome = &mut terminal => {
                panic!(
                    "PiJsonV1 ended before Pi's partial-call retry: outcome={outcome:?}, diagnostic={:?}",
                    fixture.diagnostics.get("agent-step")
                )
            }
        };
        assert!(!recovered.value["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| {
                message["role"] == "toolResult"
                    && message["toolCallId"] == "call-interrupted"
            }));
        recovered.release(count_one_result_response(
            "call-recovered-result",
            &tool_name,
        ));

        let outcome = terminal.await.unwrap();
        task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("Pi's partial-call native retry did not complete: {outcome:?}");
        };
        assert_eq!(result.value(), &json!({"count": 1}));

        let mut provider_error = false;
        let mut retry_lifecycle = Vec::new();
        while let Ok(envelope) = fixture.observations.try_recv() {
            match envelope.observation() {
                AgentObservation::Diagnostic { message, .. }
                    if message.as_ref() == "WebSocket closed 1006 Connection ended" =>
                {
                    provider_error = true;
                }
                AgentObservation::Lifecycle { milestone }
                    if matches!(
                        milestone,
                        crate::execution::workflow::agent::AgentLifecycleMilestone::RetryStarted
                            | crate::execution::workflow::agent::AgentLifecycleMilestone::RetryCompleted
                    ) => retry_lifecycle.push(*milestone),
                AgentObservation::ToolCall { call_id, .. }
                    if call_id.as_ref() == "call-interrupted" =>
                {
                    panic!("delta-only updates exposed an interrupted call without final identity")
                }
                AgentObservation::ToolResult { call_id, .. }
                    if call_id.as_ref() == "call-interrupted" =>
                {
                    panic!("the interrupted tool call produced a result")
                }
                _ => {}
            }
        }
        assert!(provider_error);
        assert_eq!(
            retry_lifecycle,
            [
                crate::execution::workflow::agent::AgentLifecycleMilestone::RetryStarted,
                crate::execution::workflow::agent::AgentLifecycleMilestone::RetryCompleted
            ]
        );

        let retained = retained_assistant_tool_call(
            &fixture,
            "conformance_gate",
            "call-interrupted",
        );
        let diagnostic = retained["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .find(|diagnostic| diagnostic["type"] == "provider_transport_failure")
            .unwrap();
        assert_eq!(diagnostic["details"]["eventsEmitted"], true);
        assert_eq!(
            diagnostic["details"]["phase"],
            "after_message_stream_start"
        );

        fixture.assert_configuration_unchanged();
        fixture.controller.shutdown().await;
    })
    .await
    .expect("pinned real-Pi partial-call retry watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_pi_recovers_after_a_truncated_result_tool_call() {
    let _executable = require_conformance_executable();
    tokio::time::timeout(PINNED_TEST_WATCHDOG, async {
        let (mut running, first, tool_name) = launch_result_case().await;
        first.release(json!({
            "kind": "truncatedToolCall",
            "call": {
                "id": "call-truncated",
                "name": tool_name,
                "arguments": {"result": {"count": 1}}
            }
        }));
        running.await_started().await;

        let (mut fixture, task, terminal) = running.into_finishing();
        let mut terminal = Box::pin(terminal.receive());
        let corrected = tokio::select! {
            request = fixture.controller.next("model") => request,
            outcome = &mut terminal => {
                panic!("PiJsonV1 rejected Pi's truncated-call recovery: {outcome:?}")
            }
        };
        let messages = corrected.value["messages"].as_array().unwrap();
        assert!(messages.iter().any(|message| {
            message["role"] == "toolResult"
                && message["toolCallId"] == "call-truncated"
                && message["isError"] == true
                && serde_json::to_string(message)
                    .unwrap()
                    .contains("output token limit")
        }));
        corrected.release(count_one_result_response("call-corrected", &tool_name));

        let outcome = terminal.await.unwrap();
        task.await.unwrap();
        assert_count_one_result(fixture, outcome).await;
    })
    .await
    .expect("pinned real-Pi truncated result recovery watchdog expired");
}

#[derive(Clone)]
struct BlockingValidationWorker {
    reached: mpsc::UnboundedSender<()>,
}

impl ResultValidationWorker for BlockingValidationWorker {
    type Running = PendingResultValidation;

    fn start(&self, _request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        let _ = self.reached.send(());
        Ok(PendingResultValidation)
    }
}

fn launch_with_blocking_validation(
    fixture: RealPiFixture,
    worker: BlockingValidationWorker,
) -> RunningRealPi {
    let mut fixture = fixture;
    let invocation = fixture.invocation.take().unwrap();
    let value_mode = invocation.value_mode().clone();
    let (started_callback, started) = agent_start_channel();
    let (terminal_callback, terminal) = agent_terminal_channel(&value_mode);
    let adapter = PiJsonV1Adapter::with_validation_worker(
        fixture.diagnostics.clone(),
        NonZeroU64::new(16 * 1024).unwrap(),
        ConformanceClock,
        NoopExecutionObserver,
        worker,
    );
    let task = tokio::spawn(async move {
        invoke_agent_adapter(&adapter, invocation, started_callback, terminal_callback).await;
    });
    RunningRealPi {
        fixture,
        task,
        started: Some(started),
        terminal,
    }
}

async fn cancel_and_finish(running: RunningRealPi) -> RealPiFixture {
    assert!(
        running
            .fixture
            .cancellation
            .request_cancellation(CancellationReason::UserRequest)
    );
    assert_eq!(
        running.terminal.receive().await.unwrap(),
        AgentOutcome::Cancelled {
            reason: CancellationReason::UserRequest
        }
    );
    running.task.await.unwrap();
    running.fixture
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_pi_05_cancellation_quiesces_model_tool_retry_validation_and_settlement() {
    let _executable = require_conformance_executable();
    tokio::time::timeout(PINNED_TEST_WATCHDOG, async {
        // Keep each controlled request alive until cancellation settles so controller EOF
        // cannot race the process interrupt and manufacture a different terminal phase.
        // Model phase: the provider request itself is the barrier.
        let fixture = RealPiFixture::new(AgentValueMode::None, false, false).unwrap();
        let mut running = RunningRealPi::launch(fixture);
        running.release_startup().await;
        let blocked_model = running.fixture.controller.next("model").await;
        running.await_started().await;
        let fixture = cancel_and_finish(running).await;
        fixture.assert_pre_response_cancellation_state();
        drop(blocked_model);
        fixture.controller.shutdown().await;

        // Tool phase: Pi must abort the exact in-flight extension tool.
        let fixture = RealPiFixture::new(AgentValueMode::None, false, false).unwrap();
        let mut running = RunningRealPi::launch(fixture);
        running.release_startup().await;
        let model = running.fixture.controller.next("model").await;
        model.release(json!({
            "kind": "toolCalls",
            "calls": [{"id": "call-blocked-tool", "name": "conformance_gate", "arguments": {"value": "blocked"}}]
        }));
        let blocked_tool = running.fixture.controller.next("tool").await;
        let fixture = cancel_and_finish(running).await;
        fixture.assert_configuration_unchanged();
        drop(blocked_tool);
        fixture.controller.shutdown().await;

        // Retry phase: cancellation wins after the native retry milestone and before its timer.
        let fixture = RealPiFixture::new(AgentValueMode::None, true, false).unwrap();
        let mut running = RunningRealPi::launch(fixture);
        running.release_startup().await;
        let model = running.fixture.controller.next("model").await;
        model.release(json!({
            "kind": "failure",
            "stopReason": "error",
            "message": "rate limit exceeded"
        }));
        loop {
            let observation = running.fixture.observations.recv().await.unwrap();
            if matches!(
                observation.observation(),
                AgentObservation::Lifecycle {
                    milestone: crate::execution::workflow::agent::AgentLifecycleMilestone::RetryStarted
                }
            ) {
                break;
            }
        }
        let fixture = cancel_and_finish(running).await;
        fixture.assert_configuration_unchanged();
        fixture.controller.shutdown().await;

        // Validation phase: the worker barrier proves cancellation stops validation and Pi.
        let fixture = RealPiFixture::new(result_mode(), false, false).unwrap();
        let (reached, mut validation_reached) = mpsc::unbounded_channel();
        let mut running = launch_with_blocking_validation(
            fixture,
            BlockingValidationWorker { reached },
        );
        running.release_startup().await;
        let model = running.fixture.controller.next("model").await;
        let tool_name = result_tool_name(&model.value).to_owned();
        model.release(json!({
            "kind": "toolCalls",
            "calls": [{"id": "call-validation", "name": tool_name, "arguments": {"result": {"count": 1}}}]
        }));
        validation_reached.recv().await.unwrap();
        let fixture = cancel_and_finish(running).await;
        fixture.assert_configuration_unchanged();
        fixture.controller.shutdown().await;

        // Settlement phase: a Valid terminating result is provisional while shutdown is held.
        let fixture = RealPiFixture::new(result_mode(), false, true).unwrap();
        let mut running = RunningRealPi::launch(fixture);
        running.release_startup().await;
        let model = running.fixture.controller.next("model").await;
        let tool_name = result_tool_name(&model.value).to_owned();
        model.release(json!({
            "kind": "toolCalls",
            "calls": [{"id": "call-settlement", "name": tool_name, "arguments": {"result": {"count": 1}}}]
        }));
        let blocked_settlement = running.fixture.controller.next("settlement").await;
        let fixture = cancel_and_finish(running).await;
        fixture.assert_configuration_unchanged();
        drop(blocked_settlement);
        fixture.controller.shutdown().await;
    })
    .await
    .expect("pinned real-Pi cancellation conformance watchdog expired");
}

#[test]
#[ignore = "launched as the interrupt-resistant Pi conformance descendant fixture"]
fn stubborn_descendant_process_fixture() {
    let _interrupt = crate::execution::workflow::test_support::process_fixture_interrupt_receiver();
    let mut ready = std::io::stdout().lock();
    ready.write_all(b"SCHERZO_STUBBORN_READY").unwrap();
    ready.flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_pi_06_cancellation_kills_a_stubborn_process_group_descendant() {
    let _executable = require_conformance_executable();
    tokio::time::timeout(PINNED_TEST_WATCHDOG, async {
        let fixture = RealPiFixture::new(AgentValueMode::None, false, false).unwrap();
        let mut running = RunningRealPi::launch(fixture);
        running.release_startup().await;
        let model = running.fixture.controller.next("model").await;
        model.release(json!({
            "kind": "toolCalls",
            "calls": [{
                "id": "call-stubborn",
                "name": "conformance_stubborn",
                "arguments": {}
            }]
        }));
        let stubborn = running.fixture.controller.next("stubborn").await;
        assert_eq!(stubborn.value["toolCallId"], "call-stubborn");
        let descendant =
            Pid::from_raw(i32::try_from(stubborn.value["processId"].as_i64().unwrap()).unwrap())
                .unwrap();
        let process_group = getpgid(Some(descendant)).unwrap();
        drop(stubborn);
        assert!(
            running
                .fixture
                .cancellation
                .request_cancellation(CancellationReason::UserRequest)
        );
        running.fixture.process_control.force();
        assert_eq!(
            running.terminal.receive().await.unwrap(),
            AgentOutcome::Cancelled {
                reason: CancellationReason::UserRequest
            }
        );
        running.task.await.unwrap();
        assert!(
            descendant_has_no_live_task(descendant, process_group),
            "the stubborn descendant retained a live task after terminal reporting"
        );
        running.fixture.assert_configuration_unchanged();
        running.fixture.controller.shutdown().await;
    })
    .await
    .expect("pinned real-Pi stubborn-descendant watchdog expired");
}

fn descendant_has_no_live_task(descendant: Pid, expected_group: Pid) -> bool {
    match getpgid(Some(descendant)) {
        Err(_) => return true,
        Ok(observed_group) if observed_group != expected_group => return true,
        Ok(_) => {}
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        let Ok(stat) = fs::read_to_string(format!("/proc/{}/stat", descendant.as_raw_pid())) else {
            return true;
        };
        let Some(fields) = stat.rsplit_once(") ").map(|(_, fields)| fields) else {
            return false;
        };
        let mut fields = fields.split_ascii_whitespace();
        let state = fields.next();
        let _parent = fields.next();
        let process_group = fields.next().and_then(|field| field.parse::<i32>().ok());
        state == Some("Z") && process_group == Some(expected_group.as_raw_pid())
    }

    #[cfg(target_vendor = "apple")]
    {
        darwin_process_is_zombie(descendant, expected_group)
    }

    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    {
        false
    }
}

#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "Darwin exposes zombie status for an unreaped orphan only through proc_pidinfo"
)]
fn darwin_process_is_zombie(descendant: Pid, expected_group: Pid) -> bool {
    let mut information = std::mem::MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let Ok(information_size) = i32::try_from(std::mem::size_of::<libc::proc_bsdinfo>()) else {
        return false;
    };
    // SAFETY: proc_pidinfo writes at most information_size bytes into the correctly sized
    // proc_bsdinfo buffer, which is initialized only after a complete structure is reported.
    let received = unsafe {
        libc::proc_pidinfo(
            descendant.as_raw_pid(),
            libc::PROC_PIDTBSDINFO,
            0,
            information.as_mut_ptr().cast(),
            information_size,
        )
    };
    if received != information_size {
        return getpgid(Some(descendant)).is_err();
    }
    // SAFETY: the exact proc_bsdinfo byte count was reported above.
    let information = unsafe { information.assume_init() };
    information.pbi_status == libc::SZOMB
        && information.pbi_pgid == expected_group.as_raw_pid().unsigned_abs()
}
