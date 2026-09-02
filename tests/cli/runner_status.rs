use super::*;

use std::os::unix::net::UnixListener;

use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::http::{HeaderValue, header};

fn write_status_config(directory: &tempfile::TempDir, socket_path: &std::path::Path) -> String {
    let config = directory.path().join("runner.json");
    fs::write(
        &config,
        serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 1,
            "deploymentMode": "production",
            "runnerStatePath": directory.path().join("state-that-status-must-not-read.json"),
            "controlSocketPath": socket_path,
            "workRoot": "/path-status-must-not-read"
        }))
        .unwrap(),
    )
    .unwrap();
    config.to_string_lossy().into_owned()
}

struct ServeStatusFixture {
    _root: tempfile::TempDir,
    _runtime: tempfile::TempDir,
    config: String,
}

impl ServeStatusFixture {
    fn new(endpoint: &str) -> Self {
        Self::new_inner(endpoint, false)
    }

    fn with_pending(endpoint: &str) -> Self {
        Self::new_inner(endpoint, true)
    }

    fn new_inner(endpoint: &str, pending: bool) -> Self {
        let root = private_credential_directory();
        let runtime = tempfile::tempdir_in("/tmp").unwrap();
        fs::set_permissions(runtime.path(), Permissions::from_mode(0o700)).unwrap();
        let state_directory = root.path().join("state");
        let source = root.path().join("source");
        let work = root.path().join("work");
        fs::create_dir(&state_directory).unwrap();
        fs::set_permissions(&state_directory, Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(&source).unwrap();
        fs::create_dir(&work).unwrap();
        fs::set_permissions(&work, Permissions::from_mode(0o700)).unwrap();
        fs::write(
            source.join("workflow.yaml"),
            "schemaVersion: 1\nsteps: {}\n",
        )
        .unwrap();
        let state_path = state_directory.join("runner.json");
        let credential_secret = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0_u8; 32]);
        let mut state = serde_json::json!({
            "schemaVersion": 1,
            "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
            "connectionUrl": endpoint,
            "currentCredential": {
                "id": "rrc_01k0z6r1w8f4jy2m7q9v3x5abc",
                "secret": credential_secret,
                "activationId": "rna_01k0z6r1w8f4jy2m7q9v3x5abc",
                "enrolledAt": "2026-08-06T12:00:00Z"
            },
            "updatedAt": "2026-08-06T12:00:00Z"
        });
        if pending {
            let pending_secret =
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([2_u8; 32]);
            state["pendingCredential"] = serde_json::json!({
                "id": "rrc_01k0z6r1w8f4jy2m7q9v3x5abd",
                "secret": pending_secret,
                "activationId": "rna_01k0z6r1w8f4jy2m7q9v3x5abd",
                "enrolledAt": "2026-08-06T13:00:00Z"
            });
        }
        fs::write(&state_path, serde_json::to_vec(&state).unwrap()).unwrap();
        fs::set_permissions(&state_path, Permissions::from_mode(0o600)).unwrap();
        let config_path = root.path().join("config.json");
        fs::write(
            &config_path,
            serde_json::to_vec(&serde_json::json!({
                "schemaVersion": 1,
                "deploymentMode": "development",
                "runnerStatePath": state_path,
                "controlSocketPath": runtime.path().join("runner.sock"),
                "workRoot": work
            }))
            .unwrap(),
        )
        .unwrap();
        Self {
            _root: root,
            _runtime: runtime,
            config: config_path.to_string_lossy().into_owned(),
        }
    }
}

fn cloud_welcome() -> Message {
    Message::Text(
        serde_json::json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abh",
            "sentAt": "2026-07-23T00:00:00Z",
            "type": "welcome",
            "payloadVersion": 1,
            "payload": {
                "sessionId": "rsn_01k0z6r1w8f4jy2m7q9v3x5abj",
                "pingIntervalSeconds": 10,
                "pongTimeoutSeconds": 30,
                "leasePolicy": {
                    "schemaVersion": 2,
                    "forceStopAndReapBudgetMilliseconds": 5000,
                    "terminalReportDeliveryBudgetMilliseconds": 5000,
                    "renewalDeliveryBudgetMilliseconds": 5000,
                    "leaseDurationMilliseconds": 320000,
                    "fencingMarginMilliseconds": 11000
                }
            }
        })
        .to_string()
        .into(),
    )
}

fn cloud_observation_ack(message_id: &str, sequence: u64) -> Message {
    Message::Text(
        serde_json::json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abk",
            "sentAt": "2026-07-23T00:00:01Z",
            "type": "observation_ack",
            "payloadVersion": 1,
            "payload": {
                "acknowledgedMessageId": message_id,
                "acknowledgedSequence": sequence
            }
        })
        .to_string()
        .into(),
    )
}

fn cloud_assignment_offer_with_mismatched_sources() -> Message {
    Message::Text(
        serde_json::json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abm",
            "sentAt": "2026-07-23T00:00:02Z",
            "type": "assignment_offer",
            "payloadVersion": 1,
            "payload": {
                "effectId": "eff_01k0z6r1w8f4jy2m7q9v3x5abg",
                "assignmentId": "asn_01k0z6r1w8f4jy2m7q9v3x5abn",
                "runId": "run_01k0z6r1w8f4jy2m7q9v3x5abp",
                "projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
                "executionSpec": {
                    "executionSpecId": "xsp_01k0z6r1w8f4jy2m7q9v3x5abq",
                    "schemaVersion": 1,
                    "executionLimits": {
                        "maximumParallelSteps": 1,
                        "cancellationGraceSeconds": 1
                    },
                    "sourceBranch": "main",
                    "workflowDefinitionSource": {
                        "repositoryConnectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
                        "objectFormat": "sha1",
                        "commitOid": "0123456789abcdef0123456789abcdef01234567",
                        "workflowPath": "workflow.yaml",
                        "workflowSourceClosureDigest": {
                            "algorithm": "sha256",
                            "value": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        }
                    },
                    "primaryWorkspaceSource": {
                        "kind": "connected_repository",
                        "providerKind": "github",
                        "repositoryConnectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
                        "objectFormat": "sha1",
                        "commitOid": "1123456789abcdef0123456789abcdef01234567",
                        "materializationContract": "git_full_clone_v1"
                    },
                    "capacity": {
                        "executionContract": "workflow_v1_cloud_inputs_artifacts@1",
                        "sourceClosureDigest": {
                            "algorithm": "sha256",
                            "value": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        },
                        "generalMaximumTransitions": 8,
                        "selectedMaximumTransitions": 7,
                        "maximumInvocations": 1,
                        "maximumRetainedBytesPerInvocation": 4194304,
                        "diagnosticRetentionBytes": 8388608,
                        "nativeSessionRetentionBytes": 4194304,
                        "aggregateRetentionBytes": 12582912,
                        "encodedOutboxBytes": 85458944
                    },
                    "runInputs": null
                },
                "attemptId": "atm_01k0z6r1w8f4jy2m7q9v3x5abc"
            }
        })
        .to_string()
        .into(),
    )
}

#[allow(
    clippy::result_large_err,
    reason = "tungstenite's handshake callback requires its large error type"
)]
async fn accept_runner_socket(
    stream: tokio::net::TcpStream,
) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
    tokio_tungstenite::accept_hdr_async(
        stream,
        |_request: &tokio_tungstenite::tungstenite::handshake::server::Request,
         mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            response.headers_mut().insert(
                header::SEC_WEBSOCKET_PROTOCOL,
                HeaderValue::from_static("scherzo.runner.v1"),
            );
            Ok(response)
        },
    )
    .await
    .unwrap()
}

#[test]
fn runner_status_queries_only_the_configured_local_socket() {
    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = runtime.path().join("runner.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let config = write_status_config(&directory, &socket_path);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 64];
        let read = stream.read(&mut request).unwrap();
        assert_eq!(
            &request[..read],
            b"{\"schemaVersion\":1,\"operation\":\"status\"}\n"
        );
        stream
            .write_all(b"{\"schemaVersion\":1,\"outcome\":\"ok\",\"status\":{\"processState\":\"running\",\"bootId\":\"rbt_01k0z6r1w8f4jy2m7q9v3x5abc\",\"uptimeMilliseconds\":273600000,\"connectionState\":\"connected\",\"lastConnectedAt\":\"2026-08-06T19:00:00Z\",\"currentCredentialId\":\"rrc_01k0z6r1w8f4jy2m7q9v3x5abc\",\"assignmentCounts\":{\"preparing\":0,\"accepted\":0,\"running\":1,\"finishing\":0,\"reporting\":0}}}\n")
            .unwrap();
    });

    let output = run_with_env(
        &["runner", "status", "--config", &config],
        &[(
            "SCHERZO_CLOUD_API_URL",
            "https://cloud-status-must-not-be-read.example",
        )],
    );

    assert!(output.status.success());
    let expected = [
        "process:        running\n",
        "boot:           rbt_01k0z6r1w8f4jy2m7q9v3x5abc\n",
        "uptime:         3d 4h\n",
        "connection:     connected\n",
        "last connected: 2026-08-06T19:00:00Z\n",
        "credential:     ",
        "rrc_01k0z6r1w8f4jy2m7q9v3x5abc",
        "\nassignments:    1 total (running: 1)\n",
    ]
    .concat();
    assert_eq!(output.stdout, expected.as_bytes());
    assert!(output.stderr.is_empty());
    server.join().unwrap();
}

#[test]
fn terminal_authentication_remains_locally_inspectable() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/v1/runner/connect", listener.local_addr().unwrap());
    let fixture = ServeStatusFixture::new(&endpoint);
    let (request_seen, request_received) = mpsc::sync_channel(1);
    let (response_release, response_released) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        request_seen.send(()).unwrap();
        response_released.recv().unwrap();
        stream
            .write_all(b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n")
            .unwrap();
    });
    let mut serve = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args(["runner", "serve", "--config", &fixture.config])
        .env("OTEL_SDK_DISABLED", "true")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    request_received.recv().unwrap();
    response_release.send(()).unwrap();
    server.join().unwrap();

    let status = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (status, _) = poll_until(
            "runner status reporting terminal authentication",
            || {
                let output = run(&["runner", "status", "--config", &fixture.config]);
                let serve_status = serve.try_wait().unwrap();
                (output, serve_status)
            },
            |(output, serve_status)| {
                (output.status.success()
                    && String::from_utf8_lossy(&output.stdout).contains("authentication_failed"))
                    || serve_status.is_some()
            },
        );
        status
    }));
    if serve.try_wait().unwrap().is_none() {
        serve.kill().unwrap();
    }
    let serve_output = serve.wait_with_output().unwrap();
    let status = match status {
        Ok(status) => status,
        Err(payload) => std::panic::resume_unwind(payload),
    };

    assert!(
        status.status.success(),
        "Runner Serve became unreachable after terminal authentication: {}",
        String::from_utf8_lossy(&serve_output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&status.stdout).contains("authentication_failed"),
        "unexpected status output: {}",
        String::from_utf8_lossy(&status.stdout)
    );
}

#[test]
fn pending_handshake_preserves_contiguous_current_boot_sequences() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("ws://{}/v1/runner/connect", listener.local_addr().unwrap());
    let fixture = ServeStatusFixture::with_pending(&endpoint);
    let (sequences_sent, sequences_received) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let (stream, _) = listener.accept().await.unwrap();
                let mut current = accept_runner_socket(stream).await;
                let Some(Ok(Message::Text(current_hello))) = current.next().await else {
                    panic!("current connection omitted hello");
                };
                let current_hello: serde_json::Value =
                    serde_json::from_str(&current_hello).unwrap();
                current.send(cloud_welcome()).await.unwrap();
                current
                    .send(cloud_observation_ack(
                        current_hello["messageId"].as_str().unwrap(),
                        current_hello["sequence"].as_u64().unwrap(),
                    ))
                    .await
                    .unwrap();

                // Hold the pending HTTP upgrade so any current-session frames
                // must remain contiguous before the candidate hello is sent.
                let (pending_stream, _) = listener.accept().await.unwrap();
                // Keep this sequencing proof independent of source-provider I/O. A
                // mismatched identity pair produces a synchronous structured rejection
                // while the candidate handshake remains deliberately blocked.
                current
                    .send(cloud_assignment_offer_with_mismatched_sources())
                    .await
                    .unwrap();
                let mut latest_current_sequence = 0;
                while latest_current_sequence < 3 {
                    let Some(Ok(Message::Text(frame))) = current.next().await else {
                        panic!("current connection omitted assignment observations");
                    };
                    let frame: serde_json::Value = serde_json::from_str(&frame).unwrap();
                    latest_current_sequence =
                        latest_current_sequence.max(frame["sequence"].as_u64().unwrap());
                    if frame["type"] == "assignment_rejected" {
                        assert_eq!(
                            frame["payload"]["decline"],
                            serde_json::json!({
                                "type": "execution_spec_invalid",
                                "reason": "invalid_source_projection"
                            })
                        );
                        break;
                    }
                    assert_ne!(frame["type"], "assignment_accepted");
                }

                let mut pending = accept_runner_socket(pending_stream).await;
                let Some(Ok(Message::Text(pending_hello))) = pending.next().await else {
                    panic!("pending connection omitted hello");
                };
                let pending_hello: serde_json::Value =
                    serde_json::from_str(&pending_hello).unwrap();
                sequences_sent
                    .send((
                        latest_current_sequence,
                        pending_hello["sequence"].as_u64().unwrap(),
                    ))
                    .unwrap();
            });
    });
    let mut serve = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args(["runner", "serve", "--config", &fixture.config])
        .env("OTEL_SDK_DISABLED", "true")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let (latest_current_sequence, pending_hello_sequence) = sequences_received.recv().unwrap();
    serve.kill().unwrap();
    serve.wait().unwrap();
    server.join().unwrap();

    assert!(
        pending_hello_sequence > latest_current_sequence,
        "pending hello sequence {pending_hello_sequence} regressed behind current sequence {latest_current_sequence}"
    );
}

fn run_status_with_response(response: &'static [u8]) -> Output {
    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = runtime.path().join("runner.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 64];
        let _ = stream.read(&mut request).unwrap();
        stream.write_all(response).unwrap();
    });
    let config = write_status_config(&directory, &socket_path);

    let output = run(&["runner", "status", "--config", &config]);

    server.join().unwrap();
    output
}

#[test]
fn runner_status_maps_control_protocol_errors_through_the_protocol_outcome() {
    let output = run_status_with_response(
        b"{\"schemaVersion\":1,\"outcome\":\"error\",\"error\":\"unsupported_version\"}\n",
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}

#[test]
fn runner_status_maps_malformed_control_responses_through_the_protocol_outcome() {
    let output = run_status_with_response(b"not-json\n");

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}

#[test]
fn runner_status_reports_absent_and_refused_sockets_as_not_reachable() {
    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = runtime.path().join("runner.sock");
    let config = write_status_config(&directory, &socket_path);
    let output = run(&["runner", "status", "--config", &config]);
    assert_eq!(output.status.code(), Some(4));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());

    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = runtime.path().join("runner.sock");
    drop(UnixListener::bind(&socket_path).unwrap());
    let config = write_status_config(&directory, &socket_path);
    let output = run(&["runner", "status", "--config", &config]);
    assert_eq!(output.status.code(), Some(4));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}
