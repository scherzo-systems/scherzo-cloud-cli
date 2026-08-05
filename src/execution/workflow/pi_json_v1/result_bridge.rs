use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::num::NonZeroU64;
use std::ops::Add as _;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _, symlink};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use jsonschema::{Draft, PatternOptions, Retrieve, Uri};
use ring::digest::{SHA256, digest};
use rustix::net::{RecvFlags, recv};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::PiJsonV1ProtocolLimits;
use crate::execution::workflow::agent::{
    AgentInvocationIdentity, PositiveDuration, RetainedResultSchema,
};
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::schema_common::lowercase_hex;

const JSON_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";
const RESOURCE_ID_PREFIX: &str = "https://schemas.scherzo.invalid/workflow-result/";
const TOOL_NAME_PREFIX: &str = "scherzo_result_";
const EXTENSION_FILE_NAME: &str = "pi-json-v1-result-extension.ts";
const SOCKET_FILE_NAME: &str = "result-validation.sock";
const SOCKET_ALIAS_NAME: &str = "e";
const SOCKET_ALIAS_ROOT: &str = "/tmp";
const CONFIG_MARKER: &str = "\"__SCHERZO_PI_JSON_V1_CONFIG_JSON__\"";
const CHANNEL_FAILURE_CAUSE: &str = "The result-validation channel failed.";
const EXTENSION_TEMPLATE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/execution/workflow/pi-json-v1-extension/src/pi-json-v1-extension.ts"
));

#[derive(Debug)]
pub(super) struct PreparedResultBridge {
    tool_name: Arc<str>,
    extension_path: PathBuf,
    socket_path: PathBuf,
    socket_alias_directory: PathBuf,
    server: ResultSocketServer,
}

impl PreparedResultBridge {
    pub(super) fn prepare<Clock: CoordinatorClock>(
        identity: &AgentInvocationIdentity,
        staging_directory: &Path,
        schema: &RetainedResultSchema,
        limits: PiJsonV1ProtocolLimits,
        receive_deadline: PositiveDuration,
        clock: Clock,
    ) -> Result<Self, ()> {
        validate_result_endpoint_directory(staging_directory)?;
        let tool_name = Arc::<str>::from(result_tool_name(identity)?);
        let socket_path = staging_directory.join(SOCKET_FILE_NAME);
        let extension_path = staging_directory.join(EXTENSION_FILE_NAME);
        let socket_alias_directory = socket_alias_directory(&tool_name)?;
        let socket_alias = socket_alias_directory.join(SOCKET_ALIAS_NAME);
        let socket_address = socket_alias.join(SOCKET_FILE_NAME);
        let transport = derive_transport_schema(schema)?;
        let source = materialize_extension(&ExtensionConfig {
            tool_name: &tool_name,
            socket_path: socket_address.to_str().ok_or(())?,
            parameters: &transport.native_parameters,
        })?;

        create_socket_alias(&socket_alias_directory, &socket_alias, staging_directory)?;
        let listener = match UnixListener::bind(&socket_address) {
            Ok(listener) => listener,
            Err(_) => {
                let _ = remove_socket_alias(&socket_alias_directory, &socket_alias);
                return Err(());
            }
        };
        if let Err(()) = make_socket_private(&socket_path) {
            drop(listener);
            let _ = fs::remove_file(&socket_path);
            let _ = remove_socket_alias(&socket_alias_directory, &socket_alias);
            return Err(());
        }
        if write_extension(&extension_path, source.as_bytes()).is_err() {
            drop(listener);
            let _ = fs::remove_file(&socket_path);
            let _ = remove_socket_alias(&socket_alias_directory, &socket_alias);
            return Err(());
        }
        let server = ResultSocketServer::start(
            listener,
            limits.maximum_frame_bytes(),
            receive_deadline,
            clock,
        );
        Ok(Self {
            tool_name,
            extension_path,
            socket_path,
            socket_alias_directory,
            server,
        })
    }

    pub(super) fn tool_name(&self) -> &Arc<str> {
        &self.tool_name
    }

    pub(super) fn extension_path(&self) -> &Path {
        &self.extension_path
    }

    pub(super) async fn receive(&mut self) -> ResultSocketEvent {
        self.server.receive().await
    }

    pub(super) async fn shutdown(self) -> Result<(), ()> {
        let Self {
            extension_path,
            socket_path,
            socket_alias_directory,
            server,
            ..
        } = self;
        let server_result = server.shutdown().await;
        let socket_result = remove_materialized_file(&socket_path);
        let extension_result = remove_materialized_file(&extension_path);
        let alias_result = remove_socket_alias(
            &socket_alias_directory,
            &socket_alias_directory.join(SOCKET_ALIAS_NAME),
        );
        server_result
            .and(socket_result)
            .and(extension_result)
            .and(alias_result)
    }
}

#[derive(Debug)]
pub(super) enum ResultSocketEvent {
    Request(IncomingResultRequest),
    ProtocolFailure,
    Closed,
}

#[derive(Debug)]
pub(super) struct IncomingResultRequest {
    request: ValidatePiResultV1Request,
    response: oneshot::Sender<ResponseCommand>,
}

impl IncomingResultRequest {
    pub(super) fn request(&self) -> &ValidatePiResultV1Request {
        &self.request
    }

    pub(super) async fn respond(self, response: ValidatePiResultV1Response) -> Result<(), ()> {
        let (delivered, delivery) = oneshot::channel();
        self.response
            .send(ResponseCommand {
                response,
                delivered,
            })
            .map_err(|_| ())?;
        delivery.await.map_err(|_| ())?
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct ValidatePiResultV1Request {
    kind: ValidateRequestKind,
    #[serde(rename = "toolCallId")]
    tool_call_id: String,
    #[serde(rename = "toolName")]
    tool_name: String,
    arguments: Value,
}

impl ValidatePiResultV1Request {
    pub(super) fn tool_call_id(&self) -> &str {
        &self.tool_call_id
    }

    pub(super) fn tool_name(&self) -> &str {
        &self.tool_name
    }

    pub(super) fn arguments(&self) -> &Value {
        &self.arguments
    }

    pub(super) fn candidate(&self) -> Option<&Value> {
        let arguments = self.arguments.as_object()?;
        (arguments.len() == 1).then_some(arguments.get("result")?)
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
enum ValidateRequestKind {
    ValidatePiResultV1,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind")]
pub(super) enum ValidatePiResultV1Response {
    Valid,
    Rejected { feedback: String },
    Fatal { cause: &'static str },
}

impl ValidatePiResultV1Response {
    pub(super) const fn valid() -> Self {
        Self::Valid
    }

    pub(super) fn rejected(feedback: &str) -> Self {
        Self::Rejected {
            feedback: feedback.to_owned(),
        }
    }

    pub(super) const fn fatal(cause: &'static str) -> Self {
        Self::Fatal { cause }
    }
}

#[derive(Debug)]
struct ResponseCommand {
    response: ValidatePiResultV1Response,
    delivered: oneshot::Sender<Result<(), ()>>,
}

#[derive(Debug)]
struct ResultSocketServer {
    events: mpsc::Receiver<ResultSocketEvent>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl ResultSocketServer {
    fn start<Clock: CoordinatorClock>(
        listener: UnixListener,
        maximum_frame_bytes: NonZeroU64,
        receive_deadline: PositiveDuration,
        clock: Clock,
    ) -> Self {
        let (events_sender, events) = mpsc::channel(1);
        let (stop, stop_receiver) = watch::channel(false);
        let task = tokio::spawn(serve_socket(
            listener,
            events_sender,
            stop_receiver,
            maximum_frame_bytes,
            receive_deadline,
            clock,
        ));
        Self { events, stop, task }
    }

    async fn receive(&mut self) -> ResultSocketEvent {
        self.events
            .recv()
            .await
            .unwrap_or(ResultSocketEvent::Closed)
    }

    async fn shutdown(self) -> Result<(), ()> {
        let Self { events, stop, task } = self;
        drop(events);
        stop.send_replace(true);
        task.await.map_err(|_| ())
    }
}

async fn serve_socket<Clock: CoordinatorClock>(
    listener: UnixListener,
    events: mpsc::Sender<ResultSocketEvent>,
    mut stop: watch::Receiver<bool>,
    maximum_frame_bytes: NonZeroU64,
    receive_deadline: PositiveDuration,
    mut clock: Clock,
) {
    loop {
        let accepted = tokio::select! {
            biased;
            _ = wait_for_stop(&mut stop) => return,
            accepted = listener.accept() => accepted,
        };
        let Ok((stream, _address)) = accepted else {
            let _ = events.send(ResultSocketEvent::ProtocolFailure).await;
            return;
        };
        let deadline = clock.now().add(receive_deadline.get());
        if !serve_connection(
            stream,
            &events,
            &mut stop,
            maximum_frame_bytes,
            &clock,
            deadline,
        )
        .await
        {
            return;
        }
    }
}

async fn serve_connection<Clock: CoordinatorClock>(
    mut stream: UnixStream,
    events: &mpsc::Sender<ResultSocketEvent>,
    stop: &mut watch::Receiver<bool>,
    maximum_frame_bytes: NonZeroU64,
    clock: &Clock,
    deadline: Clock::Instant,
) -> bool {
    let request = match read_request_frame(&mut stream, stop, maximum_frame_bytes, clock, deadline)
        .await
    {
        FrameRead::Request(request) => request,
        FrameRead::ProtocolFailure => {
            let response = ValidatePiResultV1Response::fatal(CHANNEL_FAILURE_CAUSE);
            let _ = write_response_frame(&mut stream, stop, &response, maximum_frame_bytes).await;
            let _ = events.send(ResultSocketEvent::ProtocolFailure).await;
            return true;
        }
        FrameRead::Stopped => return false,
    };

    let (response_sender, response) = oneshot::channel();
    if events
        .send(ResultSocketEvent::Request(IncomingResultRequest {
            request,
            response: response_sender,
        }))
        .await
        .is_err()
    {
        return false;
    }
    let command = match wait_for_response_command(&mut stream, stop, response).await {
        Ok(command) => command,
        Err(ResponseWaitFailure::TrailingRequest) => {
            discard_buffered_request(&stream);
            let response = ValidatePiResultV1Response::fatal(CHANNEL_FAILURE_CAUSE);
            let _ = write_response_frame(&mut stream, stop, &response, maximum_frame_bytes).await;
            let _ = events.send(ResultSocketEvent::ProtocolFailure).await;
            return true;
        }
        Err(ResponseWaitFailure::SenderDropped) => {
            let _ = events.send(ResultSocketEvent::ProtocolFailure).await;
            return true;
        }
        Err(ResponseWaitFailure::Stopped) => return false,
    };
    let delivered =
        write_response_frame(&mut stream, stop, &command.response, maximum_frame_bytes).await;
    let _ = command.delivered.send(delivered);
    true
}

enum ResponseWaitFailure {
    TrailingRequest,
    SenderDropped,
    Stopped,
}

async fn wait_for_response_command(
    stream: &mut UnixStream,
    stop: &mut watch::Receiver<bool>,
    response: oneshot::Receiver<ResponseCommand>,
) -> Result<ResponseCommand, ResponseWaitFailure> {
    let mut response = response;
    let mut trailing = [0_u8; 1];
    tokio::select! {
        biased;
        _ = wait_for_stop(stop) => Err(ResponseWaitFailure::Stopped),
        read = stream.read(&mut trailing) => match read {
            Ok(0) => tokio::select! {
                biased;
                _ = wait_for_stop(stop) => Err(ResponseWaitFailure::Stopped),
                command = &mut response => command.map_err(|_| ResponseWaitFailure::SenderDropped),
            },
            Ok(_) | Err(_) => Err(ResponseWaitFailure::TrailingRequest),
        },
        command = &mut response => match command {
            Ok(_) if has_trailing_request(stream) => Err(ResponseWaitFailure::TrailingRequest),
            Ok(command) => Ok(command),
            Err(_) => Err(ResponseWaitFailure::SenderDropped),
        },
    }
}

// Query the socket directly because Tokio's cached readiness can lag a peer write
// that completed before the validator's response became ready.
fn has_trailing_request(stream: &UnixStream) -> bool {
    let mut trailing = [0_u8; 1];
    match recv(stream, &mut trailing, RecvFlags::DONTWAIT | RecvFlags::PEEK) {
        Ok((_, 0)) => false,
        Err(rustix::io::Errno::AGAIN) => false,
        Ok(_) | Err(_) => true,
    }
}

fn discard_buffered_request(stream: &UnixStream) {
    let mut trailing = [0_u8; 1024];
    loop {
        match recv(stream, &mut trailing, RecvFlags::DONTWAIT) {
            Ok((_, 0)) | Err(rustix::io::Errno::AGAIN) => return,
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

enum FrameRead {
    Request(ValidatePiResultV1Request),
    ProtocolFailure,
    Stopped,
}

async fn read_request_frame<Clock: CoordinatorClock>(
    stream: &mut UnixStream,
    stop: &mut watch::Receiver<bool>,
    maximum_frame_bytes: NonZeroU64,
    clock: &Clock,
    deadline: Clock::Instant,
) -> FrameRead {
    let mut length = [0_u8; 4];
    let deadline_wait = clock.wait_until(deadline.clone());
    tokio::pin!(deadline_wait);
    let read_length = tokio::select! {
        biased;
        _ = wait_for_stop(stop) => return FrameRead::Stopped,
        () = &mut deadline_wait => return FrameRead::ProtocolFailure,
        result = stream.read_exact(&mut length) => result,
    };
    if read_length.is_err() {
        return FrameRead::ProtocolFailure;
    }
    let payload_length = u64::from(u32::from_be_bytes(length));
    if payload_length > maximum_frame_bytes.get() {
        return FrameRead::ProtocolFailure;
    }
    let Ok(payload_length) = usize::try_from(payload_length) else {
        return FrameRead::ProtocolFailure;
    };
    let mut payload = Vec::new();
    if payload.try_reserve_exact(payload_length).is_err() {
        return FrameRead::ProtocolFailure;
    }
    payload.resize(payload_length, 0);
    let deadline_wait = clock.wait_until(deadline.clone());
    tokio::pin!(deadline_wait);
    let read_payload = tokio::select! {
        biased;
        _ = wait_for_stop(stop) => return FrameRead::Stopped,
        () = &mut deadline_wait => return FrameRead::ProtocolFailure,
        result = stream.read_exact(&mut payload) => result,
    };
    if read_payload.is_err() {
        return FrameRead::ProtocolFailure;
    }

    // Pi 0.83's Bun node:net compatibility closes both socket halves on end(),
    // so a request-side EOF would also discard the response. Reject any already
    // buffered trailing frame bytes, then let the length prefix delimit the one
    // request while the server owns closing the connection after its response.
    let mut trailing = [0_u8; 1];
    match stream.try_read(&mut trailing) {
        Ok(0) => {}
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
        Ok(_) | Err(_) => return FrameRead::ProtocolFailure,
    }
    serde_json::from_slice(&payload)
        .map(FrameRead::Request)
        .unwrap_or(FrameRead::ProtocolFailure)
}

async fn write_response_frame(
    stream: &mut UnixStream,
    stop: &mut watch::Receiver<bool>,
    response: &ValidatePiResultV1Response,
    maximum_frame_bytes: NonZeroU64,
) -> Result<(), ()> {
    let payload = serde_json::to_vec(response).map_err(|_| ())?;
    let payload_length = u64::try_from(payload.len()).map_err(|_| ())?;
    if payload_length > maximum_frame_bytes.get() {
        return Err(());
    }
    let payload_length = u32::try_from(payload_length).map_err(|_| ())?;
    let mut frame = Vec::with_capacity(4_usize.saturating_add(payload.len()));
    frame.extend_from_slice(&payload_length.to_be_bytes());
    frame.extend_from_slice(&payload);
    let written = tokio::select! {
        biased;
        _ = wait_for_stop(stop) => return Err(()),
        result = stream.write_all(&frame) => result,
    };
    written.map_err(|_| ())?;
    stream.shutdown().await.map_err(|_| ())
}

async fn wait_for_stop(stop: &mut watch::Receiver<bool>) {
    if *stop.borrow_and_update() {
        return;
    }
    while stop.changed().await.is_ok() {
        if *stop.borrow_and_update() {
            return;
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExtensionConfig<'a> {
    tool_name: &'a str,
    socket_path: &'a str,
    parameters: &'a Value,
}

fn materialize_extension(config: &ExtensionConfig<'_>) -> Result<String, ()> {
    let mut markers = EXTENSION_TEMPLATE.match_indices(CONFIG_MARKER);
    let (marker_index, _) = markers.next().ok_or(())?;
    if markers.next().is_some() {
        return Err(());
    }
    let config_json = serde_json::to_string(config).map_err(|_| ())?;
    let encoded_config_json = serde_json::to_string(&config_json).map_err(|_| ())?;
    let mut source = String::with_capacity(
        EXTENSION_TEMPLATE
            .len()
            .saturating_sub(CONFIG_MARKER.len())
            .saturating_add(encoded_config_json.len()),
    );
    source.push_str(&EXTENSION_TEMPLATE[..marker_index]);
    source.push_str(&encoded_config_json);
    source.push_str(&EXTENSION_TEMPLATE[marker_index + CONFIG_MARKER.len()..]);
    Ok(source)
}

pub(super) fn result_tool_name(identity: &AgentInvocationIdentity) -> Result<String, ()> {
    let mut context = ring::digest::Context::new(&SHA256);
    update_identity_component(&mut context, identity.run().as_ref().as_bytes())?;
    update_identity_component(&mut context, identity.step().as_bytes())?;
    context.update(
        &identity
            .invocation()
            .transition_sequence
            .get()
            .to_be_bytes(),
    );
    let encoded = lowercase_hex(context.finish().as_ref());
    Ok(format!("{TOOL_NAME_PREFIX}{}", &encoded[..32]))
}

fn update_identity_component(
    context: &mut ring::digest::Context,
    component: &[u8],
) -> Result<(), ()> {
    let length = u64::try_from(component.len()).map_err(|_| ())?;
    context.update(&length.to_be_bytes());
    context.update(component);
    Ok(())
}

#[derive(Debug)]
struct TransportSchema {
    complete_wrapper: Value,
    native_parameters: Value,
    resource_id: String,
    used_regex_fallback: bool,
}

fn derive_transport_schema(schema: &RetainedResultSchema) -> Result<TransportSchema, ()> {
    let synthetic_resource_id = || {
        format!(
            "{RESOURCE_ID_PREFIX}{}",
            lowercase_hex(digest(&SHA256, schema.bytes()).as_ref())
        )
    };
    let mut embedded = schema.document().clone();
    let embedded_object = embedded.as_object_mut().ok_or(())?;
    let resource_id = match embedded_object.get("$id") {
        Some(Value::String(authored_id)) if !authored_id.starts_with('#') => authored_id.clone(),
        Some(Value::String(_)) | None => {
            let resource_id = synthetic_resource_id();
            embedded_object.insert("$id".to_owned(), Value::String(resource_id.clone()));
            resource_id
        }
        Some(_) => return Err(()),
    };

    let complete_wrapper = json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "$defs": {"workflowResult": embedded},
        "type": "object",
        "properties": {"result": {"$ref": resource_id}},
        "required": ["result"],
        "additionalProperties": false
    });
    validate_complete_wrapper(&complete_wrapper)?;

    let used_regex_fallback = schema_uses_regex(schema.document());
    let native_parameters = if used_regex_fallback {
        json!({
            "type": "object",
            "properties": {"result": {}},
            "required": ["result"],
            "additionalProperties": false
        })
    } else {
        complete_wrapper.clone()
    };
    Ok(TransportSchema {
        complete_wrapper,
        native_parameters,
        resource_id,
        used_regex_fallback,
    })
}

struct RejectRetrieval;

impl Retrieve for RejectRetrieval {
    fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(io::Error::other(format!("schema retrieval is disabled for {uri}")).into())
    }
}

fn validate_complete_wrapper(wrapper: &Value) -> Result<(), ()> {
    jsonschema::Validator::options()
        .with_draft(Draft::Draft202012)
        .with_pattern_options(PatternOptions::regex())
        .with_retriever(RejectRetrieval)
        .build(wrapper)
        .map(|_| ())
        .map_err(|_| ())
}

fn schema_uses_regex(schema: &Value) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    if object.contains_key("pattern") || object.contains_key("patternProperties") {
        return true;
    }
    single_schema_keywords()
        .iter()
        .filter_map(|keyword| object.get(*keyword))
        .any(schema_uses_regex)
        || array_schema_keywords()
            .iter()
            .filter_map(|keyword| object.get(*keyword).and_then(Value::as_array))
            .flatten()
            .any(schema_uses_regex)
        || map_schema_keywords()
            .iter()
            .filter_map(|keyword| object.get(*keyword).and_then(Value::as_object))
            .flat_map(Map::values)
            .any(schema_uses_regex)
}

fn single_schema_keywords() -> &'static [&'static str] {
    &[
        "additionalProperties",
        "contains",
        "contentSchema",
        "else",
        "if",
        "items",
        "not",
        "propertyNames",
        "then",
        "unevaluatedItems",
        "unevaluatedProperties",
    ]
}

fn array_schema_keywords() -> &'static [&'static str] {
    &["allOf", "anyOf", "oneOf", "prefixItems"]
}

fn map_schema_keywords() -> &'static [&'static str] {
    &[
        "$defs",
        "dependentSchemas",
        "patternProperties",
        "properties",
    ]
}

fn validate_result_endpoint_directory(directory: &Path) -> Result<(), ()> {
    let metadata = fs::symlink_metadata(directory).map_err(|_| ())?;
    if !metadata.file_type().is_dir() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(());
    }
    Ok(())
}

fn socket_alias_directory(tool_name: &str) -> Result<PathBuf, ()> {
    let identity = tool_name.strip_prefix(TOOL_NAME_PREFIX).ok_or(())?;
    #[cfg(test)]
    let identity = format!("{identity}-{}", std::process::id());
    Ok(Path::new(SOCKET_ALIAS_ROOT).join(format!(".szp-{identity}")))
}

fn create_socket_alias(directory: &Path, alias: &Path, target: &Path) -> Result<(), ()> {
    // AF_UNIX limits the address bytes even when the actual private staging path is valid.
    // A private, deterministic short alias keeps the socket itself in result-endpoint while
    // giving Pi and Node a portable Linux/macOS address that fits the native limit.
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(directory).map_err(|_| ())?;
    if fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).is_err() {
        let _ = fs::remove_dir(directory);
        return Err(());
    }
    if symlink(target, alias).is_err() {
        let _ = fs::remove_dir(directory);
        return Err(());
    }
    Ok(())
}

fn remove_socket_alias(directory: &Path, alias: &Path) -> Result<(), ()> {
    let alias_result = remove_materialized_file(alias);
    let directory_result = match fs::remove_dir(directory) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(()),
    };
    alias_result.and(directory_result)
}

fn make_socket_private(path: &Path) -> Result<(), ()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|_| ())
}

fn write_extension(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.flush()?;
    file.set_permissions(fs::Permissions::from_mode(0o400))
}

fn remove_materialized_file(path: &Path) -> Result<(), ()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(()),
    }
}

#[cfg(test)]
mod tests;
