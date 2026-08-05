use std::fs;
use std::num::NonZeroU64;
use std::os::unix::fs::PermissionsExt as _;
use std::sync::Arc;
use std::time::Duration;

use jsonschema::Validator;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;
use crate::execution::workflow::agent::WorkflowRunId;
use crate::execution::workflow::runtime::{ActionId, TransitionSequence};

const RETAINED_FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/execution/workflow/pi-json-v1-extension/fixtures/workflow-result.schema.json"
));
const MATERIALIZATION_INPUT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/execution/workflow/pi-json-v1-extension/fixtures/materialization-input.json"
));
const GENERATED_EXTENSION: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/execution/workflow/pi-json-v1-extension/fixtures/generated/pi-json-v1-extension.ts"
));

fn retained(bytes: &[u8]) -> RetainedResultSchema {
    let document = Arc::new(serde_json::from_slice(bytes).unwrap());
    RetainedResultSchema::compile(Arc::from(bytes), document).unwrap()
}

#[derive(Clone)]
enum TestClock {
    Pending,
    Controlled {
        registrations: tokio::sync::mpsc::UnboundedSender<Duration>,
        release: tokio::sync::watch::Receiver<bool>,
    },
}

impl CoordinatorClock for TestClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        let Self::Controlled {
            registrations,
            release,
        } = self
        else {
            return std::future::pending().await;
        };
        let _ = registrations.send(deadline);
        let mut release = release.clone();
        while !*release.borrow_and_update() {
            if release.changed().await.is_err() {
                return;
            }
        }
    }
}

fn receive_deadline() -> PositiveDuration {
    PositiveDuration::new(Duration::from_secs(5)).unwrap()
}

fn fixed_identity(run: &str, step: &str) -> AgentInvocationIdentity {
    AgentInvocationIdentity::new(
        WorkflowRunId::from(Arc::from(run)),
        Arc::from(step),
        ActionId {
            transition_sequence: TransitionSequence::default(),
        },
    )
}

fn wrapper_validator(wrapper: &Value) -> Validator {
    Validator::options()
        .with_draft(Draft::Draft202012)
        .with_pattern_options(PatternOptions::regex())
        .with_retriever(RejectRetrieval)
        .build(wrapper)
        .unwrap()
}

#[test]
fn resource_wrappers_preserve_fragment_roots_and_retained_bytes() {
    let cases = [
        (
            json!({
                "$schema": JSON_SCHEMA_DIALECT,
                "$defs": {"Value": {"type": "string"}},
                "$ref": "#/$defs/Value"
            }),
            json!({"result": "value"}),
            json!({"result": 1}),
        ),
        (
            json!({
                "$schema": JSON_SCHEMA_DIALECT,
                "$defs": {"Value": {"$anchor": "value", "type": "integer"}},
                "$ref": "#value"
            }),
            json!({"result": 1}),
            json!({"result": "value"}),
        ),
        (
            json!({
                "$schema": JSON_SCHEMA_DIALECT,
                "$defs": {
                    "Value": {
                        "$dynamicAnchor": "value",
                        "type": "array",
                        "items": {"$dynamicRef": "#value"}
                    }
                },
                "$ref": "#value"
            }),
            json!({"result": []}),
            json!({"result": [1]}),
        ),
        (
            json!({
                "$schema": JSON_SCHEMA_DIALECT,
                "$id": "https://author.example/result.json",
                "$defs": {"Value": {"type": "boolean"}},
                "$ref": "#/$defs/Value"
            }),
            json!({"result": true}),
            json!({"result": "true"}),
        ),
    ];

    for (document, valid, invalid) in cases {
        let retained_bytes = serde_json::to_vec_pretty(&document).unwrap();
        let schema = retained(&retained_bytes);
        let retained_document = schema.document().clone();
        let transport = derive_transport_schema(&schema).unwrap();
        let validator = wrapper_validator(&transport.complete_wrapper);

        assert!(validator.is_valid(&valid));
        assert!(!validator.is_valid(&invalid));
        assert_eq!(schema.bytes(), retained_bytes);
        assert_eq!(schema.document(), &retained_document);
        let expected_resource_id = document
            .get("$id")
            .cloned()
            .unwrap_or_else(|| Value::String(transport.resource_id.clone()));
        assert_eq!(
            transport.complete_wrapper["$defs"]["workflowResult"]["$id"],
            expected_resource_id
        );
        assert_eq!(
            transport.complete_wrapper["properties"]["result"]["$ref"],
            expected_resource_id
        );
        assert_eq!(
            Value::String(transport.resource_id.clone()),
            expected_resource_id
        );
        if document.get("$id").is_some() {
            assert_eq!(
                transport.complete_wrapper["$defs"]["workflowResult"],
                document
            );
        }
    }
}

#[test]
fn unproven_native_regex_paths_use_the_permissive_result_schema_only() {
    let schema = retained(
        serde_json::to_vec(&json!({
            "$schema": JSON_SCHEMA_DIALECT,
            "type": "object",
            "patternProperties": {"^(a+)+$": {"type": "string"}}
        }))
        .unwrap()
        .as_slice(),
    );
    let transport = derive_transport_schema(&schema).unwrap();

    assert!(transport.used_regex_fallback);
    assert_eq!(
        transport.native_parameters["properties"]["result"],
        json!({})
    );
    assert!(
        transport.complete_wrapper["$defs"]["workflowResult"]
            .get("patternProperties")
            .is_some()
    );
    assert!(
        transport.native_parameters["properties"]["result"]
            .get("patternProperties")
            .is_none()
    );
}

#[test]
fn fixed_identity_and_schema_materialize_the_checked_extension_bytes() {
    let schema = retained(RETAINED_FIXTURE);
    let transport = derive_transport_schema(&schema).unwrap();
    let input: Value = serde_json::from_str(MATERIALIZATION_INPUT).unwrap();
    assert_eq!(transport.native_parameters, input["parameters"]);

    let config = ExtensionConfig {
        tool_name: input["toolName"].as_str().unwrap(),
        socket_path: input["socketPath"].as_str().unwrap(),
        parameters: &transport.native_parameters,
    };
    let first = materialize_extension(&config).unwrap();
    let second = materialize_extension(&config).unwrap();
    assert_eq!(first, second);
    assert_eq!(first, GENERATED_EXTENSION);

    let fixed = result_tool_name(&fixed_identity("run-fixed", "step-fixed")).unwrap();
    let repeated = result_tool_name(&fixed_identity("run-fixed", "step-fixed")).unwrap();
    let distinct = result_tool_name(&fixed_identity("run-distinct", "step-fixed")).unwrap();
    assert_eq!(fixed, repeated);
    assert_ne!(fixed, distinct);
    assert_ne!(
        socket_alias_directory(&fixed).unwrap(),
        socket_alias_directory(&distinct).unwrap()
    );
    assert!(fixed.starts_with(TOOL_NAME_PREFIX));
    assert!(
        fixed
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte == b'_' || byte.is_ascii_digit())
    );
}

#[tokio::test]
async fn socket_accepts_one_exact_bounded_request_and_returns_one_response() {
    let temporary = tempfile::tempdir().unwrap();
    let request = request_bytes("call-fixed", "scherzo_result_fixed", json!({"answer": 42}));
    let maximum = NonZeroU64::new(u64::try_from(request.len()).unwrap()).unwrap();
    let (listener, socket_address, socket_path, alias_directory) =
        bind_test_socket(temporary.path(), "socket-exact");
    let mut server =
        ResultSocketServer::start(listener, maximum, receive_deadline(), TestClock::Pending);
    let client = tokio::spawn(exchange(socket_address, framed(&request)));

    let ResultSocketEvent::Request(incoming) = server.receive().await else {
        panic!("exact request must reach the validator");
    };
    assert_eq!(incoming.request().tool_call_id(), "call-fixed");
    assert_eq!(incoming.request().candidate(), Some(&json!({"answer": 42})));
    incoming
        .respond(ValidatePiResultV1Response::valid())
        .await
        .unwrap();
    assert_eq!(client.await.unwrap(), json!({"kind": "Valid"}));
    server.shutdown().await.unwrap();
    remove_materialized_file(&socket_path).unwrap();
    remove_socket_alias(&alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME)).unwrap();
}

#[tokio::test]
async fn delayed_second_frame_on_one_connection_is_fatal() {
    let temporary = tempfile::tempdir().unwrap();
    let first = request_bytes("call-first", "scherzo_result_fixed", json!({"answer": 1}));
    let second = request_bytes("call-second", "scherzo_result_fixed", json!({"answer": 2}));
    let (listener, socket_address, socket_path, alias_directory) =
        bind_test_socket(temporary.path(), "socket-delayed-second-frame");
    let mut server = ResultSocketServer::start(
        listener,
        NonZeroU64::new(1024).unwrap(),
        receive_deadline(),
        TestClock::Pending,
    );
    let mut client = UnixStream::connect(socket_address).await.unwrap();
    client.write_all(&framed(&first)).await.unwrap();

    let ResultSocketEvent::Request(incoming) = server.receive().await else {
        panic!("the first request must reach validation");
    };
    client.write_all(&framed(&second)).await.unwrap();
    // The protocol failure is allowed to discard the validator's pending success.
    let _ = incoming.respond(ValidatePiResultV1Response::valid()).await;

    let mut response = Vec::new();
    let read = client.read_to_end(&mut response).await;
    server.shutdown().await.unwrap();
    remove_materialized_file(&socket_path).unwrap();
    remove_socket_alias(&alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME)).unwrap();

    assert!(
        read.is_ok(),
        "a delayed second request must receive Fatal instead of a connection reset: {read:?}"
    );
    assert!(response.len() >= 4);
    assert_eq!(
        serde_json::from_slice::<Value>(&response[4..]).unwrap(),
        json!({"kind": "Fatal", "cause": CHANNEL_FAILURE_CAUSE}),
        "a second request queued before the first response must invalidate the connection"
    );
}

#[tokio::test]
async fn delayed_oversized_second_frame_returns_framed_fatal() {
    let temporary = tempfile::tempdir().unwrap();
    let first = request_bytes("call-first", "scherzo_result_fixed", json!({"answer": 1}));
    let second = request_bytes(
        "call-second",
        "scherzo_result_fixed",
        json!({"answer": "x".repeat(4096)}),
    );
    let maximum = NonZeroU64::new(1024).unwrap();
    assert!(u64::try_from(first.len()).unwrap() <= maximum.get());
    assert!(u64::try_from(second.len()).unwrap() > maximum.get());
    let (listener, socket_address, socket_path, alias_directory) =
        bind_test_socket(temporary.path(), "socket-delayed-oversized-second-frame");
    let mut server =
        ResultSocketServer::start(listener, maximum, receive_deadline(), TestClock::Pending);
    let mut client = UnixStream::connect(socket_address).await.unwrap();
    client.write_all(&framed(&first)).await.unwrap();

    let ResultSocketEvent::Request(incoming) = server.receive().await else {
        panic!("the first request must reach validation");
    };
    client.write_all(&framed(&second)).await.unwrap();
    let _ = incoming.respond(ValidatePiResultV1Response::valid()).await;

    let mut response = Vec::new();
    let read = client.read_to_end(&mut response).await;
    server.shutdown().await.unwrap();
    remove_materialized_file(&socket_path).unwrap();
    remove_socket_alias(&alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME)).unwrap();

    assert!(
        read.is_ok(),
        "a delayed oversized request must receive Fatal instead of a connection reset: {read:?}"
    );
    assert!(response.len() >= 4);
    assert_eq!(
        serde_json::from_slice::<Value>(&response[4..]).unwrap(),
        json!({"kind": "Fatal", "cause": CHANNEL_FAILURE_CAUSE})
    );
}

#[tokio::test]
async fn oversized_malformed_and_multi_frame_connections_are_fatal() {
    for (malformed, maximum) in [
        (65_u32.to_be_bytes().to_vec(), 64_u64),
        (framed(b"not-json"), 1024),
        (
            {
                let mut frame = framed(&request_bytes(
                    "call-fixed",
                    "scherzo_result_fixed",
                    json!(true),
                ));
                frame.push(0);
                frame
            },
            1024,
        ),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let (listener, socket_address, socket_path, alias_directory) =
            bind_test_socket(temporary.path(), "socket-malformed");
        let mut server = ResultSocketServer::start(
            listener,
            NonZeroU64::new(maximum).unwrap(),
            receive_deadline(),
            TestClock::Pending,
        );
        let client = tokio::spawn(exchange(socket_address, malformed));

        assert!(matches!(
            server.receive().await,
            ResultSocketEvent::ProtocolFailure
        ));
        assert_eq!(
            client.await.unwrap(),
            json!({"kind": "Fatal", "cause": CHANNEL_FAILURE_CAUSE})
        );
        server.shutdown().await.unwrap();
        remove_materialized_file(&socket_path).unwrap();
        remove_socket_alias(&alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME)).unwrap();
    }
}

#[tokio::test]
async fn incomplete_frame_reaches_the_receive_deadline() {
    let temporary = tempfile::tempdir().unwrap();
    let (listener, socket_address, socket_path, alias_directory) =
        bind_test_socket(temporary.path(), "socket-incomplete");
    let (registrations, mut registered_deadlines) = tokio::sync::mpsc::unbounded_channel();
    let (release, release_receiver) = tokio::sync::watch::channel(false);
    let mut server = ResultSocketServer::start(
        listener,
        NonZeroU64::new(1024).unwrap(),
        receive_deadline(),
        TestClock::Controlled {
            registrations,
            release: release_receiver,
        },
    );
    let mut client = UnixStream::connect(socket_address).await.unwrap();
    client.write_all(&16_u32.to_be_bytes()).await.unwrap();
    client.write_all(b"{").await.unwrap();

    assert_eq!(
        registered_deadlines.recv().await,
        Some(Duration::from_secs(5))
    );
    assert_eq!(
        registered_deadlines.recv().await,
        Some(Duration::from_secs(5))
    );
    release.send_replace(true);
    assert!(matches!(
        server.receive().await,
        ResultSocketEvent::ProtocolFailure
    ));

    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    assert!(response.len() >= 4);
    assert_eq!(
        serde_json::from_slice::<Value>(&response[4..]).unwrap(),
        json!({"kind": "Fatal", "cause": CHANNEL_FAILURE_CAUSE})
    );
    server.shutdown().await.unwrap();
    remove_materialized_file(&socket_path).unwrap();
    remove_socket_alias(&alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME)).unwrap();
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, not a behavior assertion"
)]
#[tokio::test]
async fn shutdown_quiesces_with_queued_protocol_failures() {
    let temporary = tempfile::tempdir().unwrap();
    let (listener, socket_address, socket_path, alias_directory) =
        bind_test_socket(temporary.path(), "socket-queued-failures");
    let server = ResultSocketServer::start(
        listener,
        NonZeroU64::new(1024).unwrap(),
        receive_deadline(),
        TestClock::Pending,
    );

    for _ in 0..2 {
        assert_eq!(
            exchange(socket_address.clone(), framed(b"not-json")).await,
            json!({"kind": "Fatal", "cause": CHANNEL_FAILURE_CAUSE})
        );
    }

    tokio::time::timeout(Duration::from_secs(10), server.shutdown())
        .await
        .expect("queued protocol failures must not block shutdown")
        .unwrap();
    remove_materialized_file(&socket_path).unwrap();
    remove_socket_alias(&alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME)).unwrap();
}

#[tokio::test]
async fn prepared_socket_and_extension_are_private_until_explicit_quiescence() {
    let temporary = tempfile::tempdir().unwrap();
    fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let bridge = PreparedResultBridge::prepare(
        &fixed_identity("run-fixed", "step-fixed"),
        temporary.path(),
        &retained(RETAINED_FIXTURE),
        PiJsonV1ProtocolLimits::profile(),
        receive_deadline(),
        TestClock::Pending,
    )
    .unwrap();
    let extension_path = bridge.extension_path().to_owned();
    let socket_path = bridge.socket_path.clone();
    let alias_directory = bridge.socket_alias_directory.clone();
    let socket_address = alias_directory
        .join(SOCKET_ALIAS_NAME)
        .join(SOCKET_FILE_NAME);
    let fixed_extension_bytes = fs::read(&extension_path).unwrap();

    assert!(extension_path.exists());
    assert!(socket_path.exists());
    assert_eq!(
        fs::metadata(&extension_path).unwrap().permissions().mode() & 0o777,
        0o400
    );
    assert_eq!(
        fs::metadata(&socket_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(&alias_directory).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert!(socket_address.to_string_lossy().len() < 100);
    bridge.shutdown().await.unwrap();
    assert!(!extension_path.exists());
    assert!(!socket_path.exists());
    assert!(!alias_directory.exists());

    let second_temporary = tempfile::tempdir().unwrap();
    fs::set_permissions(second_temporary.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let second = PreparedResultBridge::prepare(
        &fixed_identity("run-fixed", "step-fixed"),
        second_temporary.path(),
        &retained(RETAINED_FIXTURE),
        PiJsonV1ProtocolLimits::profile(),
        receive_deadline(),
        TestClock::Pending,
    )
    .unwrap();
    assert_eq!(
        fs::read(second.extension_path()).unwrap(),
        fixed_extension_bytes
    );
    second.shutdown().await.unwrap();
}

#[test]
fn fragment_only_authored_root_id_preserves_standalone_semantics() {
    let document = json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "$id": "#",
        "type": "integer"
    });
    let retained_bytes = serde_json::to_vec(&document).unwrap();
    let schema = retained(&retained_bytes);
    let standalone = wrapper_validator(schema.document());
    let transport = derive_transport_schema(&schema).unwrap();
    let wrapped = wrapper_validator(&transport.complete_wrapper);

    assert!(standalone.is_valid(&json!(1)));
    assert_eq!(
        wrapped.is_valid(&json!({"result": 1})),
        standalone.is_valid(&json!(1)),
        "the native wrapper must accept every candidate accepted by the authored schema"
    );
}

fn request_bytes(call_id: &str, tool_name: &str, result: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "kind": "ValidatePiResultV1",
        "toolCallId": call_id,
        "toolName": tool_name,
        "arguments": {"result": result}
    }))
    .unwrap()
}

fn framed(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn bind_test_socket(target: &Path, run: &str) -> (UnixListener, PathBuf, PathBuf, PathBuf) {
    let tool_name = result_tool_name(&fixed_identity(run, "socket-test")).unwrap();
    let alias_directory = socket_alias_directory(&tool_name).unwrap();
    let alias = alias_directory.join(SOCKET_ALIAS_NAME);
    create_socket_alias(&alias_directory, &alias, target).unwrap();
    let socket_address = alias.join(SOCKET_FILE_NAME);
    let listener = UnixListener::bind(&socket_address).unwrap();
    (
        listener,
        socket_address,
        target.join(SOCKET_FILE_NAME),
        alias_directory,
    )
}

async fn exchange(socket_address: PathBuf, request: Vec<u8>) -> Value {
    let mut stream = UnixStream::connect(socket_address).await.unwrap();
    stream.write_all(&request).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    assert!(response.len() >= 4);
    let payload_length = u32::from_be_bytes(response[..4].try_into().unwrap());
    assert_eq!(usize::try_from(payload_length).unwrap(), response.len() - 4);
    serde_json::from_slice(&response[4..]).unwrap()
}
