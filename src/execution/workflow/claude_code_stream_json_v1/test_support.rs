use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use super::FIXED_INVOCATION_ENVIRONMENT;

const MAXIMUM_HTTP_HEADER_BYTES: usize = 64 * 1024;
const MAXIMUM_PROVIDER_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const PLACEHOLDER_API_KEY: &str = "scherzo-loopback-placeholder";

pub(super) struct SyntheticClaudeCodeRoot {
    _temporary: tempfile::TempDir,
    project: PathBuf,
    home: PathBuf,
    config: PathBuf,
    system_prompt: PathBuf,
}

impl SyntheticClaudeCodeRoot {
    pub(super) fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let project = temporary.path().join("project");
        let home = temporary.path().join("home");
        let config = temporary.path().join("claude-config");
        for directory in [&project, &home, &config] {
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
            system_prompt,
        }
    }

    pub(super) fn project(&self) -> &Path {
        &self.project
    }

    pub(super) fn system_prompt(&self) -> &Path {
        &self.system_prompt
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

    pub(super) fn release_text(mut self, text: &str) {
        self.response
            .take()
            .unwrap()
            .send(LoopbackProviderResponse::Text(text.to_owned()))
            .unwrap();
    }
}

#[derive(Debug)]
enum LoopbackProviderResponse {
    Text(String),
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
    let payload = response_payload(response);
    let header = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache\r\nconnection: close\r\ncontent-length: {}\r\n\r\n",
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
    let LoopbackProviderResponse::Text(text) = response;
    let events = [
        (
            "message_start",
            json!({
                "type": "message_start",
                "message": {
                    "id": "msg_scherzo_loopback",
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
        ),
        (
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            }),
        ),
        (
            "content_block_delta",
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": text},
            }),
        ),
        (
            "content_block_stop",
            json!({"type": "content_block_stop", "index": 0}),
        ),
        (
            "message_delta",
            json!({
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn", "stop_sequence": null},
                "usage": {"output_tokens": 3},
            }),
        ),
        ("message_stop", json!({"type": "message_stop"})),
    ];

    let mut payload = Vec::new();
    for (name, event) in events {
        payload.extend_from_slice(format!("event: {name}\ndata: ").as_bytes());
        serde_json::to_writer(&mut payload, &event).unwrap();
        payload.extend_from_slice(b"\n\n");
    }
    payload
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
