use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::future::{Future, pending, ready};
use std::num::NonZeroU64;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rustix::process::{Pid, getpgid};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use super::adapter::PiJsonV1Adapter;
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
    AgentValueMode, CompletedAgentInvocation, RetainedResultSchema, StagedAgentAttachment,
    WorkflowRunId, agent_start_channel, agent_terminal_channel, invoke_agent_adapter,
};
// The black-box fixture intentionally owns its imports instead of depending on the
// executable-stub fixture module solely to share test wiring.
// jscpd:ignore-start
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
    option_env!("SCHERZO_PI_CONFORMANCE_EXECUTABLE").map(PathBuf::from)
}

#[derive(Clone, Copy)]
struct NeverClock;

impl CoordinatorClock for NeverClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, _deadline: Self::Instant) {
        pending().await
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
}

impl RealPiFixture {
    fn new(value_mode: AgentValueMode, retry: bool, hold_settlement: bool) -> Option<Self> {
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
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        fs::set_permissions(&result_endpoint, fs::Permissions::from_mode(0o700)).unwrap();

        materialize_project(&project_directory, retry);
        let global_settings = b"{\"defaultProjectTrust\":\"ask\"}\n".to_vec();
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
        let invocation = AgentInvocation::new(
            identity,
            AdmittedAgentAdapter::new(
                AgentCompatibilityProfile::PiJsonV1,
                fs::canonicalize(executable).unwrap(),
                Arc::from(PI_JSON_V1_QUALIFICATION_VERSION),
                PiConfig {
                    model: MODEL_NAME.to_owned(),
                    thinking: Thinking::XHigh,
                },
            ),
            crate::execution::workflow::agent::AgentProcessContext::new(
                working_directory,
                EnvironmentSnapshot::new(environment),
            ),
            AgentInvocationStaging::new(result_endpoint),
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
        })
    }

    fn assert_configuration_unchanged(&self) {
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
            "--no-session must not persist a session"
        );
    }
}

fn materialize_project(project: &Path, retry: bool) {
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
                "baseDelayMs": 600_000
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
    let bytes = Arc::<[u8]>::from(serde_json::to_vec(&document).unwrap());
    AgentValueMode::Result {
        output: Arc::from("result"),
        schema: RetainedResultSchema::compile(bytes, Arc::new(document)).unwrap(),
    }
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
            NeverClock,
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
}

fn normalized_semantics(mut value: Value, isolation_root: &Path) -> Value {
    fn normalize(value: &mut Value, isolation_root: &str) {
        match value {
            Value::Object(object) => {
                object.remove("timestamp");
                for child in object.values_mut() {
                    normalize(child, isolation_root);
                }
            }
            Value::Array(values) => {
                for child in values {
                    normalize(child, isolation_root);
                }
            }
            Value::String(text) => {
                *text = text.replace(isolation_root, "<isolated-root>");
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    normalize(&mut value, isolation_root.to_str().unwrap());
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

#[test]
fn pinned_real_pi_00_fake_provider_has_no_network_or_timer_success_path() {
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
fn pinned_real_pi_01_qualification_anchor_is_exact_and_supported() {
    let Some(executable) = conformance_executable() else {
        return;
    };
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
async fn pinned_real_pi_02_launch_resources_attachments_and_response_conform() {
    if conformance_executable().is_none() {
        return;
    }
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
async fn pinned_real_pi_03_no_value_and_typed_terminal_failures_conform() {
    if conformance_executable().is_none() {
        return;
    }
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
                AgentOutcome::Failed {
                    cause: AgentFailureCause::HarnessFailed {
                        detail: crate::execution::workflow::agent::AgentHarnessFailureDetail::ModelOutputTruncated,
                    },
                },
            ),
            (
                AgentValueMode::None,
                json!({"kind": "failure", "stopReason": "error", "message": "deterministic model rejection"}),
                AgentOutcome::Failed {
                    cause: AgentFailureCause::HarnessFailed {
                        detail: crate::execution::workflow::agent::AgentHarnessFailureDetail::ModelError,
                    },
                },
            ),
            (
                AgentValueMode::None,
                json!({"kind": "failure", "stopReason": "aborted", "message": "provider aborted"}),
                AgentOutcome::Failed {
                    cause: AgentFailureCause::HarnessFailed {
                        detail: crate::execution::workflow::agent::AgentHarnessFailureDetail::ModelAborted,
                    },
                },
            ),
            (
                AgentValueMode::Response { output: Arc::from("response") },
                json!({"kind": "toolCalls", "calls": [{"id": "call-terminal", "name": "conformance_terminate", "arguments": {}}]}),
                AgentOutcome::Failed {
                    cause: AgentFailureCause::HarnessFailed {
                        detail: crate::execution::workflow::agent::AgentHarnessFailureDetail::UnexpectedTerminalToolUse,
                    },
                },
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
async fn pinned_real_pi_04_result_rejection_sibling_correction_and_termination_conform() {
    if conformance_executable().is_none() {
        return;
    }
    tokio::time::timeout(PINNED_TEST_WATCHDOG, async {
        let fixture = RealPiFixture::new(result_mode(), false, false).unwrap();
        let mut running = RunningRealPi::launch(fixture);
        running.release_startup().await;

        let first = running.fixture.controller.next("model").await;
        let tool_name = result_tool_name(&first.value).to_owned();
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
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("corrected terminating result did not complete");
        };
        assert_eq!(result.value(), &json!({"count": 1}));
        fixture.assert_configuration_unchanged();
        fixture.controller.shutdown().await;
    })
    .await
    .expect("pinned real-Pi result conformance watchdog expired");
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
        NeverClock,
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
async fn pinned_real_pi_05_cancellation_quiesces_model_tool_retry_validation_and_settlement() {
    if conformance_executable().is_none() {
        return;
    }
    tokio::time::timeout(PINNED_TEST_WATCHDOG, async {
        // Model phase: the provider request itself is the barrier.
        let fixture = RealPiFixture::new(AgentValueMode::None, false, false).unwrap();
        let mut running = RunningRealPi::launch(fixture);
        running.release_startup().await;
        let blocked_model = running.fixture.controller.next("model").await;
        running.await_started().await;
        drop(blocked_model);
        let fixture = cancel_and_finish(running).await;
        fixture.assert_configuration_unchanged();
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
        drop(blocked_tool);
        let fixture = cancel_and_finish(running).await;
        fixture.assert_configuration_unchanged();
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
        drop(blocked_settlement);
        let fixture = cancel_and_finish(running).await;
        fixture.assert_configuration_unchanged();
        fixture.controller.shutdown().await;
    })
    .await
    .expect("pinned real-Pi cancellation conformance watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
async fn pinned_real_pi_06_cancellation_kills_a_stubborn_process_group_descendant() {
    if conformance_executable().is_none() {
        return;
    }
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
