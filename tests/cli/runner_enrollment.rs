use super::*;

use std::os::unix::net::{UnixListener, UnixStream};

const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
const CURRENT_CREDENTIAL_ID: &str = "rrc_01k0z6r1w8f4jy2m7q9v3x5abc";
const PENDING_CREDENTIAL_ID: &str = "rrc_01k0z6r1w8f4jy2m7q9v3x5abd";
const ACTIVATION_ID: &str = "rna_01k0z6r1w8f4jy2m7q9v3x5abd";
const CURRENT_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const PENDING_SECRET: &str = "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI";

struct PendingEnrollmentFixture {
    _root: tempfile::TempDir,
    _runtime: tempfile::TempDir,
    config: String,
    activation: String,
    state: std::path::PathBuf,
    socket: std::path::PathBuf,
}

impl PendingEnrollmentFixture {
    fn new() -> Self {
        let root = private_credential_directory();
        let runtime = tempfile::tempdir_in("/tmp").unwrap();
        fs::set_permissions(runtime.path(), Permissions::from_mode(0o700)).unwrap();
        let state_directory = root.path().join("state");
        fs::create_dir(&state_directory).unwrap();
        fs::set_permissions(&state_directory, Permissions::from_mode(0o700)).unwrap();
        let state = state_directory.join("runner-state.json");
        fs::write(
            &state,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schemaVersion": 1,
                "runnerId": RUNNER_ID,
                "connectionUrl": "ws://127.0.0.1:1/v1/runner/connect",
                "currentCredential": {
                    "id": CURRENT_CREDENTIAL_ID,
                    "secret": CURRENT_SECRET,
                    "activationId": "rna_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "enrolledAt": "2026-08-06T12:00:00Z"
                },
                "pendingCredential": {
                    "id": PENDING_CREDENTIAL_ID,
                    "secret": PENDING_SECRET,
                    "activationId": ACTIVATION_ID,
                    "enrolledAt": "2026-08-06T13:00:00Z"
                },
                "updatedAt": "2026-08-06T13:00:00Z"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::set_permissions(&state, Permissions::from_mode(0o600)).unwrap();

        let socket = runtime.path().join("runner.sock");
        let config = root.path().join("runner.json");
        fs::write(
            &config,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schemaVersion": 1,
                "deploymentMode": "development",
                "runnerStatePath": state,
                "controlSocketPath": socket,
                "workRoot": root.path().join("work")
            }))
            .unwrap(),
        )
        .unwrap();

        let activation = root.path().join("replacement-activation.json");
        fs::write(
            &activation,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schemaVersion": 1,
                "activationUrl": format!(
                    "http://127.0.0.1:1/v1/runner-enrollments/{ACTIVATION_ID}/activate"
                ),
                "activationToken": format!("{ACTIVATION_ID}.{CURRENT_SECRET}"),
                "runnerId": RUNNER_ID,
                "expiresAt": "2099-08-06T20:00:00Z"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::set_permissions(&activation, Permissions::from_mode(0o600)).unwrap();

        Self {
            _root: root,
            _runtime: runtime,
            config: config.to_string_lossy().into_owned(),
            activation: activation.to_string_lossy().into_owned(),
            state,
            socket,
        }
    }

    fn command(&self) -> [&str; 8] {
        [
            "runner",
            "enroll",
            "--replace-credential",
            "--activation-file",
            &self.activation,
            "--config",
            &self.config,
            "--json",
        ]
    }
}

fn accept_control_request(listener: &UnixListener) -> (UnixStream, Vec<u8>) {
    let (mut stream, _) = listener.accept().unwrap();
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    while stream.read_exact(&mut byte).is_ok() {
        request.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
    }
    (stream, request)
}

#[test]
fn enrolls_then_promotes_a_replacement_without_exposing_credential_material() {
    let fixture = PendingEnrollmentFixture::new();
    let mut state: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.state).unwrap()).unwrap();
    state.as_object_mut().unwrap().remove("pendingCredential");
    fs::write(&fixture.state, serde_json::to_vec_pretty(&state).unwrap()).unwrap();
    fs::set_permissions(&fixture.state, Permissions::from_mode(0o600)).unwrap();

    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let cloud_address = cloud.local_addr().unwrap();
    let mut artifact: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.activation).unwrap()).unwrap();
    artifact["activationUrl"] = serde_json::Value::String(format!(
        "http://{cloud_address}/v1/runner-enrollments/{ACTIVATION_ID}/activate"
    ));
    fs::write(
        &fixture.activation,
        serde_json::to_vec_pretty(&artifact).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&fixture.activation, Permissions::from_mode(0o600)).unwrap();
    let cloud_server = thread::spawn(move || {
        let (mut stream, _) = cloud.accept().unwrap();
        let request = String::from_utf8(read_request(&mut stream)).unwrap();
        let body = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 1,
            "runnerId": RUNNER_ID,
            "runnerName": "runner-test",
            "organization": {
                "id": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
                "displayName": "Test organization"
            },
            "runnerPool": {
                "id": "rpl_01k0z6r1w8f4jy2m7q9v3x5abc",
                "name": "builders"
            },
            "credentialId": PENDING_CREDENTIAL_ID,
            "connectionUrl": "ws://127.0.0.1:1/v1/runner/connect"
        }))
        .unwrap();
        write!(
            stream,
            "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
        request
    });

    let control = UnixListener::bind(&fixture.socket).unwrap();
    let state_path = fixture.state.clone();
    let control_server = thread::spawn(move || {
        let (mut stream, request) = accept_control_request(&control);
        let mut state: serde_json::Value =
            serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
        let pending = state
            .as_object_mut()
            .unwrap()
            .remove("pendingCredential")
            .expect("replacement enrollment should stage pending state");
        state["currentCredential"] = pending;
        state["lastPromotion"] = serde_json::json!({
            "credentialId": PENDING_CREDENTIAL_ID,
            "activationId": ACTIVATION_ID,
            "promotedAt": "2026-08-06T13:01:00Z"
        });
        state["updatedAt"] = serde_json::Value::String("2026-08-06T13:01:00Z".to_owned());
        fs::write(&state_path, serde_json::to_vec_pretty(&state).unwrap()).unwrap();
        fs::set_permissions(&state_path, Permissions::from_mode(0o600)).unwrap();
        stream
            .write_all(
                format!(
                    "{{\"schemaVersion\":1,\"outcome\":\"reloaded\",\"credentialId\":\"{PENDING_CREDENTIAL_ID}\"}}\n"
                )
                .as_bytes(),
            )
            .unwrap();
        request
    });

    let output = run(&fixture.command());
    assert!(output.status.success(), "{output:?}");
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["outcome"], "rotation_completed");
    assert_eq!(report["cloudEnrollment"]["outcome"], "enrolled");
    assert_eq!(report["livePromotion"]["outcome"], "promoted");
    assert!(output.stderr.is_empty());

    let cloud_request = cloud_server.join().unwrap();
    assert!(
        cloud_request
            .to_ascii_lowercase()
            .contains("idempotency-key:")
    );
    let control_request = String::from_utf8(control_server.join().unwrap()).unwrap();
    assert_eq!(
        control_request,
        "{\"schemaVersion\":1,\"operation\":\"reload_credential\"}\n"
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.state).unwrap()).unwrap();
    assert_eq!(state["currentCredential"]["id"], PENDING_CREDENTIAL_ID);
    assert!(state.get("pendingCredential").is_none());
    let enrolled_secret = state["currentCredential"]["secret"].as_str().unwrap();
    for output in [&output.stdout, &output.stderr, control_request.as_bytes()] {
        for secret in [CURRENT_SECRET, PENDING_SECRET, enrolled_secret] {
            assert!(
                !output
                    .windows(secret.len())
                    .any(|part| part == secret.as_bytes())
            );
        }
    }
}

#[test]
fn retries_a_staged_replacement_through_the_secret_free_control_request() {
    let fixture = PendingEnrollmentFixture::new();
    let listener = UnixListener::bind(&fixture.socket).unwrap();
    let server = thread::spawn(move || {
        let (mut stream, request) = accept_control_request(&listener);
        stream
            .write_all(
                format!(
                    "{{\"schemaVersion\":1,\"outcome\":\"reloaded\",\"credentialId\":\"{PENDING_CREDENTIAL_ID}\"}}\n"
                )
                .as_bytes(),
            )
            .unwrap();
        request
    });

    let output = run(&fixture.command());
    assert!(output.status.success(), "{output:?}");
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["outcome"], "rotation_completed");
    assert_eq!(report["cloudEnrollment"]["outcome"], "already_enrolled");
    assert_eq!(report["livePromotion"]["outcome"], "promoted");
    assert!(output.stderr.is_empty());

    let request = String::from_utf8(server.join().unwrap()).unwrap();
    assert_eq!(
        request,
        "{\"schemaVersion\":1,\"operation\":\"reload_credential\"}\n"
    );
    for output in [&output.stdout, &output.stderr, request.as_bytes()] {
        assert!(
            !output
                .windows(CURRENT_SECRET.len())
                .any(|part| part == CURRENT_SECRET.as_bytes())
        );
        assert!(
            !output
                .windows(PENDING_SECRET.len())
                .any(|part| part == PENDING_SECRET.as_bytes())
        );
    }
}

#[test]
fn explicit_state_update_failure_is_not_reinterpreted_as_success() {
    let fixture = PendingEnrollmentFixture::new();
    let listener = UnixListener::bind(&fixture.socket).unwrap();
    let state_path = fixture.state.clone();
    let server = thread::spawn(move || {
        let (mut stream, request) = accept_control_request(&listener);
        let mut state: serde_json::Value =
            serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
        let pending = state
            .as_object_mut()
            .unwrap()
            .remove("pendingCredential")
            .expect("fixture should begin with a pending credential");
        state["currentCredential"] = pending;
        state["lastPromotion"] = serde_json::json!({
            "credentialId": PENDING_CREDENTIAL_ID,
            "activationId": ACTIVATION_ID,
            "promotedAt": "2026-08-06T13:01:00Z"
        });
        state["updatedAt"] = serde_json::Value::String("2026-08-06T13:01:00Z".to_owned());
        fs::write(&state_path, serde_json::to_vec_pretty(&state).unwrap()).unwrap();
        stream
            .write_all(
                b"{\"schemaVersion\":1,\"outcome\":\"error\",\"error\":\"state_update_failed\"}\n",
            )
            .unwrap();
        request
    });

    let output = run(&fixture.command());
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["outcome"], "rotation_incomplete");
    assert_eq!(report["cloudEnrollment"]["outcome"], "already_enrolled");
    assert_eq!(report["livePromotion"]["outcome"], "pending");
    assert_eq!(report["livePromotion"]["error"], "state_update_failed");
    assert!(output.stderr.is_empty());
    let request = String::from_utf8(server.join().unwrap()).unwrap();
    assert_eq!(
        request,
        "{\"schemaVersion\":1,\"operation\":\"reload_credential\"}\n"
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.state).unwrap()).unwrap();
    assert_eq!(state["currentCredential"]["id"], PENDING_CREDENTIAL_ID);
    assert!(state.get("pendingCredential").is_none());
}

#[test]
fn reports_cloud_enrollment_separately_when_runner_serve_is_unreachable() {
    let fixture = PendingEnrollmentFixture::new();
    let original = fs::read(&fixture.state).unwrap();
    let output = run(&fixture.command());
    assert_eq!(output.status.code(), Some(4), "{output:?}");
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["outcome"], "rotation_incomplete");
    assert_eq!(report["cloudEnrollment"]["outcome"], "already_enrolled");
    assert_eq!(report["livePromotion"]["outcome"], "pending");
    assert_eq!(report["livePromotion"]["error"], "runner_serve_unreachable");
    assert!(output.stderr.is_empty());
    assert_eq!(fs::read(&fixture.state).unwrap(), original);
    for output in [&output.stdout, &output.stderr] {
        assert!(
            !output
                .windows(CURRENT_SECRET.len())
                .any(|part| part == CURRENT_SECRET.as_bytes())
        );
        assert!(
            !output
                .windows(PENDING_SECRET.len())
                .any(|part| part == PENDING_SECRET.as_bytes())
        );
    }
}
