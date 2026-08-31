use std::ffi::OsString;
use std::fs;
use std::future::{Future, pending};
use std::io::Read as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nix::sys::stat::Mode;
use nix::unistd::mkfifo;
use serde_json::{Value, json};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use super::FIXED_INVOCATION_ENVIRONMENT;
use super::adapter::PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL;
use crate::execution::claude_code::CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION as QUALIFICATION_VERSION;
use crate::execution::workflow::admission::EnvironmentSnapshot;
use crate::execution::workflow::agent::{
    AdmittedAgentAdapter, AgentCompatibilityProfile, AgentInvocationIdentity, AgentObservation,
    AgentObservationEnvelope, AgentObservationSink, WorkflowRunId,
};
use crate::execution::workflow::claude_code::{ClaudeCodeConfig, ClaudeCodeEffort};
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::runtime::{ActionId, TransitionSequence};

const MAXIMUM_HTTP_HEADER_BYTES: usize = 64 * 1024;
const MAXIMUM_PROVIDER_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const PLACEHOLDER_API_KEY: &str = "scherzo-loopback-placeholder";
/// Opaque placeholder for the native `signature_delta` that accompanies a thinking
/// block. Claude Code forwards this value without interpreting it in one exchange.
const THINKING_SIGNATURE: &str = "c2NoZXJ6by1sb29wYmFjay10aGlua2luZy1zaWduYXR1cmU=";
const STRUCTURED_OUTPUT_TOOL_NAME: &str = "StructuredOutput";

#[derive(Clone)]
pub(super) struct FixtureSignal {
    path: Arc<Path>,
    reader: Arc<fs::File>,
}

impl FixtureSignal {
    pub(super) fn create(path: PathBuf) -> Self {
        mkfifo(&path, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        let reader = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&path)
            .unwrap();
        Self {
            path: Arc::from(path),
            reader: Arc::new(reader),
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) async fn receive(&self) -> Vec<u8> {
        let reader = AsyncFd::new(self.reader.try_clone().unwrap()).unwrap();
        let mut signal = [0; 64];
        loop {
            let mut ready = reader.readable().await.unwrap();
            match ready.try_io(|reader| {
                let mut reader = reader.get_ref();
                reader.read(&mut signal)
            }) {
                Ok(Ok(0)) => panic!("fixture signal closed without a value"),
                Ok(Ok(read)) => return signal[..read].to_vec(),
                Ok(Err(error)) => panic!("failed to read fixture signal: {error}"),
                Err(_) => {}
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct PendingClock;

// Exact-binary profiles keep independent clocks because their process probes coexist with
// different native validation and settlement channels.
// jscpd:ignore-start
impl CoordinatorClock for PendingClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        if deadline == PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL {
            let probe = tokio::spawn(async {});
            let _ = probe.await;
        } else {
            pending().await
        }
    }
}
// jscpd:ignore-end

#[derive(Clone, Default)]
pub(super) struct RecordingObservationSink(Arc<Mutex<Vec<AgentObservationEnvelope>>>);

impl RecordingObservationSink {
    /// Concatenates, in observation order and without a separator, the text of every
    /// observation the selector accepts. Native text and reasoning both arrive as
    /// arbitrarily split deltas, so only the reassembled stream is comparable.
    pub(super) fn concatenated_text(
        &self,
        select: impl Fn(&AgentObservation) -> Option<&str>,
    ) -> String {
        self.snapshot()
            .iter()
            .filter_map(|envelope| select(envelope.observation()))
            .collect()
    }

    pub(super) fn snapshot(&self) -> Vec<AgentObservationEnvelope> {
        self.0.lock().unwrap().clone()
    }
}

impl AgentObservationSink for RecordingObservationSink {
    fn observe(&self, observation: AgentObservationEnvelope) -> impl Future<Output = ()> + Send {
        self.0.lock().unwrap().push(observation);
        async {}
    }
}

pub(super) fn invocation_identity(run: &str, step: &str) -> AgentInvocationIdentity {
    AgentInvocationIdentity::new(
        WorkflowRunId::from(Arc::from(run)),
        Arc::from(step),
        ActionId {
            transition_sequence: TransitionSequence::default(),
        },
    )
}

pub(super) fn admitted_adapter(
    executable: PathBuf,
    model: &str,
) -> AdmittedAgentAdapter<ClaudeCodeConfig> {
    AdmittedAgentAdapter::new(
        AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
        executable,
        Arc::from(QUALIFICATION_VERSION),
        ClaudeCodeConfig {
            model: model.to_owned(),
            effort: ClaudeCodeEffort::XHigh,
        },
    )
}

pub(super) struct SyntheticClaudeCodeRoot {
    _temporary: tempfile::TempDir,
    project: PathBuf,
    home: PathBuf,
    config: PathBuf,
    private: PathBuf,
    system_prompt: PathBuf,
}

impl SyntheticClaudeCodeRoot {
    pub(super) fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let project = temporary.path().join("project");
        let home = temporary.path().join("home");
        let config = temporary.path().join("claude-config");
        let private = temporary.path().join("adapter-private");
        for directory in [&project, &home, &config, &private] {
            fs::create_dir(directory).unwrap();
        }
        fs::write(
            project.join("README.md"),
            b"synthetic conformance project\n",
        )
        .unwrap();
        let system_prompt = temporary.path().join("system-prompt.txt");
        fs::write(
            &system_prompt,
            b"Operate only on the deterministic synthetic conformance request.\n",
        )
        .unwrap();

        let repository = fs::canonicalize(env!("CARGO_MANIFEST_DIR"))
            .unwrap()
            .parent()
            .unwrap()
            .to_owned();
        let canonical_project = fs::canonicalize(&project).unwrap();
        assert!(!canonical_project.starts_with(repository));

        Self {
            _temporary: temporary,
            project: canonical_project,
            home,
            config,
            private,
            system_prompt,
        }
    }

    pub(super) fn project(&self) -> &Path {
        &self.project
    }

    pub(super) fn system_prompt(&self) -> &Path {
        &self.system_prompt
    }

    pub(super) fn private(&self) -> &Path {
        &self.private
    }

    pub(super) fn retained_transcript(&self) -> PathBuf {
        self.private.join("diagnostics/session/transcript.jsonl")
    }

    pub(super) fn retained_resources(&self) -> PathBuf {
        self.private.join("diagnostics/session/resources")
    }

    pub(super) fn ambient_session_paths(&self, session_id: &str) -> [PathBuf; 2] {
        let project = self
            .config
            .join("projects")
            .join(super::adapter::native_project_slug(
                self.project.to_str().unwrap(),
            ));
        [
            project.join(format!("{session_id}.jsonl")),
            project.join(session_id),
        ]
    }

    pub(super) fn environment_snapshot(&self, provider: &LoopbackProvider) -> EnvironmentSnapshot {
        EnvironmentSnapshot::new([
            (OsString::from("HOME"), self.home.as_os_str().to_owned()),
            (
                OsString::from("CLAUDE_CONFIG_DIR"),
                self.config.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_CODE_PROJECT_DIR_NAME"),
                OsString::from("ambient-project-name"),
            ),
            (OsString::from("PATH"), OsString::from("/usr/bin:/bin")),
            (OsString::from("CI"), OsString::from("1")),
            (OsString::from("NO_COLOR"), OsString::from("1")),
            (
                OsString::from("ANTHROPIC_API_KEY"),
                OsString::from(PLACEHOLDER_API_KEY),
            ),
            (
                OsString::from("ANTHROPIC_BASE_URL"),
                OsString::from(provider.base_url()),
            ),
        ])
    }

    pub(super) fn configure_command(&self, command: &mut Command, provider: &LoopbackProvider) {
        command
            .env_clear()
            .current_dir(&self.project)
            .env("HOME", &self.home)
            .env("CLAUDE_CONFIG_DIR", &self.config)
            .env("PATH", "/usr/bin:/bin")
            .env("CI", "1")
            .env("NO_COLOR", "1")
            .env("ANTHROPIC_API_KEY", PLACEHOLDER_API_KEY)
            .env("ANTHROPIC_BASE_URL", provider.base_url());
        for (name, value) in FIXED_INVOCATION_ENVIRONMENT {
            command.env(name, value);
        }
    }
}

pub(super) struct ControlledProviderRequest {
    path: String,
    body: Value,
    used_placeholder_key: bool,
    response: Option<oneshot::Sender<LoopbackProviderResponse>>,
}

impl ControlledProviderRequest {
    pub(super) fn path(&self) -> &str {
        &self.path
    }

    pub(super) fn body(&self) -> &Value {
        &self.body
    }

    pub(super) const fn used_placeholder_key(&self) -> bool {
        self.used_placeholder_key
    }

    pub(super) fn release_text(self, text: &str) {
        self.release_blocks(vec![LoopbackBlock::text(text)]);
    }

    pub(super) fn release_structured_output(self, envelope: Value) {
        self.release_blocks(vec![LoopbackBlock::tool_use(
            STRUCTURED_OUTPUT_TOOL_NAME,
            envelope,
        )]);
    }

    pub(super) fn release_tool_use(self, name: &str, input: Value) {
        self.release_blocks(vec![LoopbackBlock::tool_use(name, input)]);
    }

    /// Releases one native assistant message built from the supplied ordered content
    /// blocks. Callers that need a specific block sequence, rather than one convenience
    /// shape, use this to fix exactly what Claude Code must correlate.
    pub(super) fn release_blocks(mut self, blocks: Vec<LoopbackBlock>) {
        self.response
            .take()
            .unwrap()
            .send(LoopbackProviderResponse::Blocks(blocks))
            .unwrap();
    }

    pub(super) fn release_invalid_request(mut self) {
        self.response
            .take()
            .unwrap()
            .send(LoopbackProviderResponse::InvalidRequest)
            .unwrap();
    }
}

/// One content block of a native assistant message, in the shape the Anthropic
/// streaming API delivers it. `Thinking` accumulates through deltas, while
/// `RedactedThinking` arrives complete in its `content_block_start`.
#[derive(Debug)]
pub(super) enum LoopbackBlock {
    Text(String),
    Thinking(Vec<String>),
    RedactedThinking(String),
    ToolUse { name: String, input: Value },
}

impl LoopbackBlock {
    pub(super) fn text(text: &str) -> Self {
        Self::Text(text.to_owned())
    }

    pub(super) fn thinking(segments: &[&str]) -> Self {
        Self::Thinking(
            segments
                .iter()
                .map(|segment| (*segment).to_owned())
                .collect(),
        )
    }

    pub(super) fn redacted_thinking(data: &str) -> Self {
        Self::RedactedThinking(data.to_owned())
    }

    pub(super) fn tool_use(name: &str, input: Value) -> Self {
        Self::ToolUse {
            name: name.to_owned(),
            input,
        }
    }
}

#[derive(Debug)]
enum LoopbackProviderResponse {
    Blocks(Vec<LoopbackBlock>),
    InvalidRequest,
}

pub(super) struct LoopbackProvider {
    address: std::net::SocketAddr,
    requests: mpsc::UnboundedReceiver<ControlledProviderRequest>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl LoopbackProvider {
    pub(super) async fn start() -> Self {
        // The Claude fixture is bounded HTTP/SSE while Pi uses a Unix-socket extension;
        // keep their small controller loops separate so neither implies shared protocol.
        // jscpd:ignore-start
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
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
                            serve_connection(stream, request_sender).await;
                        });
                    }
                }
            }
            connections.shutdown().await;
        });
        // jscpd:ignore-end
        Self {
            address,
            requests,
            shutdown: Some(shutdown_sender),
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }

    pub(super) async fn next_request(&mut self) -> ControlledProviderRequest {
        self.requests.recv().await.unwrap()
    }

    pub(super) fn has_pending_request(&mut self) -> bool {
        self.requests.try_recv().is_ok()
    }

    pub(super) async fn shutdown(mut self) {
        let _ = self.shutdown.take().unwrap().send(());
        self.task.await.unwrap();
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    requests: mpsc::UnboundedSender<ControlledProviderRequest>,
) {
    let Some(request) = read_request(&mut stream).await else {
        return;
    };
    // This controller exchange releases one Anthropic-compatible HTTP response; Pi's
    // similarly shaped channel releases native extension events with different authority.
    // jscpd:ignore-start
    let (respond, response) = oneshot::channel();
    if requests
        .send(ControlledProviderRequest {
            path: request.path,
            body: request.body,
            used_placeholder_key: request.used_placeholder_key,
            response: Some(respond),
        })
        .is_err()
    {
        return;
    }
    let Ok(response) = response.await else {
        return;
    };
    // jscpd:ignore-end
    let (status, content_type, payload) = match response {
        LoopbackProviderResponse::InvalidRequest => (
            "400 Bad Request",
            "application/json",
            serde_json::to_vec(&json!({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "message": "synthetic provider rejection",
                },
                "request_id": "req_scherzo_loopback",
            }))
            .unwrap(),
        ),
        response => ("200 OK", "text/event-stream", response_payload(response)),
    };
    let header = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncache-control: no-cache\r\nconnection: close\r\ncontent-length: {}\r\n\r\n",
        payload.len()
    );
    if stream.write_all(header.as_bytes()).await.is_err() {
        return;
    }
    let _ = stream.write_all(&payload).await;
    let _ = stream.shutdown().await;
}

struct ProviderRequest {
    path: String,
    body: Value,
    used_placeholder_key: bool,
}

async fn read_request(stream: &mut TcpStream) -> Option<ProviderRequest> {
    let mut bytes = Vec::new();
    let header_end = loop {
        if bytes.len() >= MAXIMUM_HTTP_HEADER_BYTES {
            return None;
        }
        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(index) = find_subslice(&bytes, b"\r\n\r\n") {
            break index + 4;
        }
    };

    let header = std::str::from_utf8(&bytes[..header_end]).ok()?;
    let mut lines = header.split("\r\n");
    let mut request_line = lines.next()?.split_ascii_whitespace();
    if request_line.next()? != "POST" {
        return None;
    }
    let path = request_line.next()?.to_owned();
    if request_line.next()? != "HTTP/1.1" || request_line.next().is_some() {
        return None;
    }

    let mut content_length = None;
    let mut used_placeholder_key = false;
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':')?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse::<usize>().ok();
        } else if name.eq_ignore_ascii_case("x-api-key") {
            used_placeholder_key = value == PLACEHOLDER_API_KEY;
        }
    }
    let content_length = content_length?;
    if content_length > MAXIMUM_PROVIDER_REQUEST_BYTES {
        return None;
    }
    let total = header_end.checked_add(content_length)?;
    while bytes.len() < total {
        let remaining = total - bytes.len();
        let mut chunk = [0_u8; 4096];
        let chunk_length = remaining.min(chunk.len());
        let read = stream.read(&mut chunk[..chunk_length]).await.ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    if bytes.len() != total {
        return None;
    }
    let body = serde_json::from_slice(&bytes[header_end..]).ok()?;
    Some(ProviderRequest {
        path,
        body,
        used_placeholder_key,
    })
}

fn find_subslice(bytes: &[u8], needle: &[u8]) -> Option<usize> {
    bytes
        .windows(needle.len())
        .position(|window| window == needle)
}

fn response_payload(response: LoopbackProviderResponse) -> Vec<u8> {
    let events = match response {
        LoopbackProviderResponse::Blocks(blocks) => block_response_events(&blocks),
        LoopbackProviderResponse::InvalidRequest => {
            panic!("invalid requests use a non-SSE response")
        }
    };

    let mut payload = Vec::new();
    for (name, event) in events {
        payload.extend_from_slice(format!("event: {name}\ndata: ").as_bytes());
        serde_json::to_writer(&mut payload, &event).unwrap();
        payload.extend_from_slice(b"\n\n");
    }
    payload
}

fn block_response_events(blocks: &[LoopbackBlock]) -> Vec<(&'static str, Value)> {
    let mut events = vec![message_start_event("msg_scherzo_loopback")];
    for (index, block) in blocks.iter().enumerate() {
        events.extend(content_block_events(index, block));
    }
    let stop_reason = if blocks
        .iter()
        .any(|block| matches!(block, LoopbackBlock::ToolUse { .. }))
    {
        "tool_use"
    } else {
        "end_turn"
    };
    events.extend(terminal_message_events(stop_reason));
    events
}

fn content_block_events(index: usize, block: &LoopbackBlock) -> Vec<(&'static str, Value)> {
    let (start, deltas) = match block {
        LoopbackBlock::Text(text) => (
            json!({"type": "text", "text": ""}),
            vec![json!({"type": "text_delta", "text": text})],
        ),
        LoopbackBlock::Thinking(segments) => {
            let mut deltas = segments
                .iter()
                .map(|segment| json!({"type": "thinking_delta", "thinking": segment}))
                .collect::<Vec<_>>();
            deltas.push(json!({"type": "signature_delta", "signature": THINKING_SIGNATURE}));
            (
                json!({"type": "thinking", "thinking": "", "signature": ""}),
                deltas,
            )
        }
        // The native API delivers a redacted thinking block complete, with no deltas.
        LoopbackBlock::RedactedThinking(data) => (
            json!({"type": "redacted_thinking", "data": data}),
            Vec::new(),
        ),
        LoopbackBlock::ToolUse { name, input } => (
            json!({
                "type": "tool_use",
                "id": format!("tool_scherzo_loopback_{index}"),
                "name": name,
                "input": {},
            }),
            vec![json!({
                "type": "input_json_delta",
                "partial_json": serde_json::to_string(input).unwrap(),
            })],
        ),
    };

    let mut events = vec![(
        "content_block_start",
        json!({"type": "content_block_start", "index": index, "content_block": start}),
    )];
    events.extend(deltas.into_iter().map(|delta| {
        (
            "content_block_delta",
            json!({"type": "content_block_delta", "index": index, "delta": delta}),
        )
    }));
    events.push((
        "content_block_stop",
        json!({"type": "content_block_stop", "index": index}),
    ));
    events
}

fn message_start_event(message_id: &str) -> (&'static str, Value) {
    (
        "message_start",
        json!({
            "type": "message_start",
            "message": {
                "id": message_id,
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": "scherzo-loopback",
                "stop_reason": null,
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 1,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0,
                    "output_tokens": 0,
                },
            },
        }),
    )
}

fn terminal_message_events(stop_reason: &str) -> [(&'static str, Value); 2] {
    [
        (
            "message_delta",
            json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": null},
                "usage": {"output_tokens": 3},
            }),
        ),
        ("message_stop", json!({"type": "message_stop"})),
    ]
}

pub(super) fn version_probe_environment(root: &Path) -> [(OsString, OsString); 2] {
    [
        (OsString::from("HOME"), root.join("home").into_os_string()),
        (
            OsString::from("CLAUDE_CONFIG_DIR"),
            root.join("claude-config").into_os_string(),
        ),
    ]
}
