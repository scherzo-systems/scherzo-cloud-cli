use std::collections::BTreeSet;

use crate::api::test_support::ScriptedHttpServer;

use super::*;

const ACTIVATION_ID: &str = "rna_01k0z6r1w8f4jy2m7q9v3x5abc";
const OTHER_ACTIVATION_ID: &str = "rna_01k0z6r1w8f4jy2m7q9v3x5abd";
const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
const CREDENTIAL_ID: &str = "rrc_01k0z6r1w8f4jy2m7q9v3x5abc";
const REPLACEMENT_CREDENTIAL_ID: &str = "rrc_01k0z6r1w8f4jy2m7q9v3x5abd";
const ACTIVATION_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const RUNNER_STATE_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/schemas/runner-state-v1.schema.json"
));
const OPERATOR_CONFIG_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/schemas/runner-operator-config-v1.schema.json"
));
const ENROLLMENT_JOURNAL_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/schemas/runner-enrollment-journal-v1.schema.json"
));
const TERMINAL_RECEIPT_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/schemas/runner-enrollment-terminal-receipt-v1.schema.json"
));

fn assert_json_matches_schema(format: &str, schema_source: &str, bytes: &[u8]) {
    let schema: serde_json::Value =
        serde_json::from_str(schema_source).expect("decode mirrored runner host schema");
    let document: serde_json::Value =
        serde_json::from_slice(bytes).expect("decode produced runner host document");
    let validator = jsonschema::draft202012::new(&schema).expect("compile runner host schema");
    assert!(
        validator.is_valid(&document),
        "{format} producer output does not match its checked-in schema"
    );
}

#[test]
fn url_policy_requires_secure_or_explicit_loopback_development_transport() {
    for (mode, value, kind, accepted) in [
        (
            DeploymentMode::Production,
            "https://api.scherzo.dev/v1/runner-enrollments/rna_01k0z6r1w8f4jy2m7q9v3x5abc/activate",
            CloudURLKind::Activation,
            true,
        ),
        (
            DeploymentMode::Production,
            "http://localhost:8080/v1/runner-enrollments/rna_01k0z6r1w8f4jy2m7q9v3x5abc/activate",
            CloudURLKind::Activation,
            false,
        ),
        (
            DeploymentMode::Development,
            "http://127.0.0.1:8080/v1/runner-enrollments/rna_01k0z6r1w8f4jy2m7q9v3x5abc/activate",
            CloudURLKind::Activation,
            true,
        ),
        (
            DeploymentMode::Development,
            "http://127.1:8080/v1/runner-enrollments/rna_01k0z6r1w8f4jy2m7q9v3x5abc/activate",
            CloudURLKind::Activation,
            false,
        ),
        (
            DeploymentMode::Development,
            "http://host.docker.internal:8080/v1/runner-enrollments/rna_01k0z6r1w8f4jy2m7q9v3x5abc/activate",
            CloudURLKind::Activation,
            false,
        ),
        (
            DeploymentMode::Development,
            "ws://[::1]:8080/v1/runner/connect",
            CloudURLKind::Connection,
            true,
        ),
        (
            DeploymentMode::Development,
            "ws://192.168.1.3:8080/v1/runner/connect",
            CloudURLKind::Connection,
            false,
        ),
        (
            DeploymentMode::Production,
            "wss://user@example.test/v1/runner/connect",
            CloudURLKind::Connection,
            false,
        ),
        (
            DeploymentMode::Production,
            "wss://@example.test/v1/runner/connect",
            CloudURLKind::Connection,
            false,
        ),
        (
            DeploymentMode::Production,
            "wss://example.test/v1/runner/connect#fragment",
            CloudURLKind::Connection,
            false,
        ),
        (
            DeploymentMode::Production,
            "wss://example.test/v1/runner/connect?endpoint=other",
            CloudURLKind::Connection,
            false,
        ),
    ] {
        assert_eq!(
            validate_cloud_url(value, mode, kind).is_ok(),
            accepted,
            "{value}"
        );
    }
}

#[test]
fn operator_config_written_bytes_match_schema_in_both_deployment_modes() {
    let temporary = tempfile::tempdir().expect("create config fixture");
    let config_path = temporary.path().join("runner.json");
    for mode in ["production", "development"] {
        let document = serde_json::json!({
            "schemaVersion": 1,
            "deploymentMode": mode,
            "runnerStatePath": "/var/lib/scherzo-cloud/runner-state.json",
            "controlSocketPath": "/run/scherzo-cloud/runner.sock",
            "workRoot": "/var/lib/scherzo-cloud/work"
        });
        fs::write(
            &config_path,
            serde_json::to_vec(&document).expect("encode config"),
        )
        .expect("write config");
        let bytes = fs::read(&config_path).expect("read written operator config");
        assert_json_matches_schema("runner operator config", OPERATOR_CONFIG_SCHEMA, &bytes);
        assert!(load_operator_config(&config_path).is_ok(), "{mode}");
    }
}

#[test]
fn activation_artifact_accepts_a_plain_relative_destination() {
    assert_eq!(
        artifact_parent(Path::new("activation.json")),
        Path::new(".")
    );
}

#[test]
fn atomic_state_write_uses_private_permissions_and_rejects_links() {
    let directory = tempfile::TempDir::new().expect("create directory");
    fs::set_permissions(directory.path(), Permissions::from_mode(0o700))
        .expect("protect directory");
    let path = directory.path().join("state.json");
    atomic_write_json(&path, &serde_json::json!({"schemaVersion": 1})).expect("write state");
    assert_eq!(
        fs::metadata(&path).expect("state metadata").mode() & 0o777,
        0o600
    );

    let link = directory.path().join("link.json");
    std::os::unix::fs::symlink(&path, &link).expect("create symbolic link");
    assert!(atomic_write_json(&link, &serde_json::json!({"schemaVersion": 1})).is_err());

    let hard_link = directory.path().join("hard-link.json");
    fs::hard_link(&path, &hard_link).expect("create hard link");
    let original = fs::read(&path).expect("read original state");
    assert!(atomic_write_json(&path, &serde_json::json!({"schemaVersion": 2})).is_err());
    assert_eq!(fs::read(&path).expect("reread original state"), original);
}

#[test]
fn successful_enrollment_commits_schema_valid_state_without_transmitting_secret() {
    let server = ScriptedHttpServer::respond(enrollment_response());
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());

    let outcome = enroll(
        Some(&fixture.activation_path),
        &fixture.config_path,
        false,
        false,
    )
    .expect("enroll runner");
    let EnrollmentOutcome::Enrolled {
        response,
        replacement,
    } = outcome
    else {
        panic!("enrollment unexpectedly returned gone");
    };
    assert!(!replacement);
    assert_eq!(response.credential_id(), CREDENTIAL_ID);
    assert!(!fixture.journal_path().exists());

    let state_metadata = fs::metadata(&fixture.state_path).expect("state metadata");
    assert_eq!(state_metadata.mode() & 0o777, 0o600);
    assert_eq!(state_metadata.nlink(), 1);
    assert_eq!(
        fs::metadata(fixture.state_path.parent().expect("state parent"))
            .expect("state directory metadata")
            .mode()
            & 0o777,
        0o700,
    );
    let state_bytes = fs::read(&fixture.state_path).expect("read protected state");
    assert_json_matches_schema("runner state", RUNNER_STATE_SCHEMA, &state_bytes);
    let state: RunnerState = serde_json::from_slice(&state_bytes).expect("decode protected state");
    assert_eq!(state.runner_id, RUNNER_ID);
    assert_eq!(state.current_credential.id, CREDENTIAL_ID);
    assert!(valid_secret(&state.current_credential.secret));

    let service = load_runner_service_configuration(&fixture.config_path)
        .expect("load Runner Serve configuration from enrolled state");
    assert_eq!(service.runner_id, RUNNER_ID);
    assert_eq!(service.connection_url, state.connection_url);
    assert_eq!(service.credential_id, CREDENTIAL_ID);
    assert_eq!(service.credential_secret, state.current_credential.secret);

    let request = server.finish_one();
    let body = request_body(&request);
    let decoded: serde_json::Value = serde_json::from_str(body).expect("decode request body");
    let keys = decoded
        .as_object()
        .expect("request object")
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        keys,
        BTreeSet::from([
            "credentialSecretVerifier".to_owned(),
            "schemaVersion".to_owned(),
        ])
    );
    let complete_credential = format!("{CREDENTIAL_ID}.{}", state.current_credential.secret);
    assert!(!request.contains(&state.current_credential.secret));
    assert!(!request.contains(&complete_credential));
}

#[test]
fn replacement_enrollment_preserves_current_and_reuses_the_staged_pending_credential() {
    let server = ScriptedHttpServer::respond(enrollment_response_with_credential(
        REPLACEMENT_CREDENTIAL_ID,
    ));
    let fixture = EnrollmentFixture::new(&server, OTHER_ACTIVATION_ID, future_time());
    let state_directory = fixture.state_path.parent().expect("state directory");
    ensure_private_directory(state_directory).expect("create state directory");
    atomic_write_json(
        &fixture.state_path,
        &serde_json::json!({
            "schemaVersion": 1,
            "runnerId": RUNNER_ID,
            "connectionUrl": "ws://127.0.0.1:8765/v1/runner/connect",
            "currentCredential": {
                "id": CREDENTIAL_ID,
                "secret": ACTIVATION_SECRET,
                "activationId": ACTIVATION_ID,
                "enrolledAt": "2026-08-06T12:00:00Z"
            },
            "updatedAt": "2026-08-06T12:00:00Z"
        }),
    )
    .expect("write current runner state");

    let outcome = enroll(
        Some(&fixture.activation_path),
        &fixture.config_path,
        true,
        false,
    )
    .expect("enroll replacement credential");
    assert!(matches!(
        outcome,
        EnrollmentOutcome::Enrolled {
            replacement: true,
            ..
        }
    ));
    server.finish_one();

    let state: RunnerState =
        serde_json::from_slice(&fs::read(&fixture.state_path).expect("read replacement state"))
            .expect("decode replacement state");
    assert_eq!(state.runner_id, RUNNER_ID);
    assert_eq!(state.current_credential.id, CREDENTIAL_ID);
    assert_eq!(
        state
            .pending_credential
            .as_ref()
            .map(|value| value.id.as_str()),
        Some(REPLACEMENT_CREDENTIAL_ID)
    );
    assert!(!fixture.journal_path().exists());
    assert_eq!(
        replacement_disposition(&fixture.config_path, RUNNER_ID, REPLACEMENT_CREDENTIAL_ID)
            .expect("read replacement disposition"),
        ReplacementDisposition::Pending
    );

    let staged_state = fs::read(&fixture.state_path).expect("read staged state");
    assert_json_matches_schema("runner state", RUNNER_STATE_SCHEMA, &staged_state);
    let retry = enroll(
        Some(&fixture.activation_path),
        &fixture.config_path,
        true,
        false,
    )
    .expect("reuse staged pending credential without another Cloud request");
    assert!(matches!(
        retry,
        EnrollmentOutcome::ReplacementCredential {
            runner_id,
            credential_id,
        } if runner_id == RUNNER_ID && credential_id == REPLACEMENT_CREDENTIAL_ID
    ));
    assert_eq!(
        fs::read(&fixture.state_path).expect("read retried state"),
        staged_state
    );

    let service =
        load_runner_service_configuration(&fixture.config_path).expect("load pending state");
    service
        .state_access
        .promote(RUNNER_ID, CREDENTIAL_ID, REPLACEMENT_CREDENTIAL_ID)
        .expect("promote replacement state");
    assert_eq!(
        replacement_disposition(&fixture.config_path, RUNNER_ID, REPLACEMENT_CREDENTIAL_ID)
            .expect("read promoted disposition"),
        ReplacementDisposition::Current
    );
    let promoted_state = fs::read(&fixture.state_path).expect("read promoted state");
    assert_json_matches_schema("runner state", RUNNER_STATE_SCHEMA, &promoted_state);
    let retry = enroll(
        Some(&fixture.activation_path),
        &fixture.config_path,
        true,
        false,
    )
    .expect("recover a lost promotion response without another Cloud request");
    assert!(matches!(
        retry,
        EnrollmentOutcome::ReplacementCredential {
            runner_id,
            credential_id,
        } if runner_id == RUNNER_ID && credential_id == REPLACEMENT_CREDENTIAL_ID
    ));
    assert_eq!(
        fs::read(&fixture.state_path).expect("read recovered promoted state"),
        promoted_state
    );
}

#[test]
fn enrollment_rejects_redirects_without_losing_the_journal() {
    let server = ScriptedHttpServer::respond(redirect_response(
        "http://127.0.0.1:9/v1/runner-enrollments/redirected/activate",
    ));
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());

    assert!(matches!(
        enroll(
            Some(&fixture.activation_path),
            &fixture.config_path,
            false,
            false,
        ),
        Err(EnrollmentError::InvalidResponse)
    ));
    let journal = fs::read(fixture.journal_path()).expect("read redirect journal");
    assert!(String::from_utf8_lossy(&journal).contains(ACTIVATION_ID));
    server.finish_one();
}

#[test]
fn enrollment_journal_and_terminal_receipt_match_schemas_across_retries() {
    let server = ScriptedHttpServer::respond_in_sequence(vec![
        empty_response("500 Internal Server Error"),
        empty_response("401 Unauthorized"),
        empty_response("409 Conflict"),
        empty_response("410 Gone"),
    ]);
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());

    assert!(matches!(
        enroll(
            Some(&fixture.activation_path),
            &fixture.config_path,
            false,
            false,
        ),
        Err(EnrollmentError::NetworkAmbiguous)
    ));
    let journal_path = fixture.journal_path();
    let original = fs::read(&journal_path).expect("read staged journal");
    assert_json_matches_schema(
        "runner enrollment journal",
        ENROLLMENT_JOURNAL_SCHEMA,
        &original,
    );
    let journal: EnrollmentJournal = serde_json::from_slice(&original).expect("decode journal");
    assert!(valid_secret(&journal.credential_secret));

    assert!(matches!(
        enroll(None, &fixture.config_path, false, true),
        Err(EnrollmentError::Unauthorized)
    ));
    assert_eq!(
        fs::read(&journal_path).expect("read unauthorized journal"),
        original
    );
    assert!(matches!(
        enroll(None, &fixture.config_path, false, true),
        Err(EnrollmentError::Conflict)
    ));
    assert_eq!(
        fs::read(&journal_path).expect("read conflicting journal"),
        original
    );

    let outcome = enroll(None, &fixture.config_path, false, true).expect("resolve gone journal");
    assert!(matches!(
        outcome,
        EnrollmentOutcome::Gone { activation_id } if activation_id == ACTIVATION_ID
    ));
    let receipt_bytes = fs::read(&journal_path).expect("read terminal receipt");
    assert_json_matches_schema(
        "runner enrollment terminal receipt",
        TERMINAL_RECEIPT_SCHEMA,
        &receipt_bytes,
    );
    let receipt: TerminalReceipt = serde_json::from_slice(&receipt_bytes).expect("decode receipt");
    assert!(valid_terminal_receipt(&receipt));
    for secret_member in [
        "activationToken",
        "credentialSecret",
        "credentialSecretVerifier",
        "idempotencyKey",
        ACTIVATION_SECRET,
        &journal.credential_secret,
    ] {
        assert!(!String::from_utf8_lossy(&receipt_bytes).contains(secret_member));
    }

    let requests = server.finish();
    let first_body = request_body(&requests[0]);
    let first_authorization = request_header(&requests[0], "authorization");
    let first_key = request_header(&requests[0], "idempotency-key");
    for request in &requests[1..] {
        assert_eq!(request_body(request), first_body);
        assert_eq!(
            request_header(request, "authorization"),
            first_authorization
        );
        assert_eq!(request_header(request, "idempotency-key"), first_key);
    }

    let replacement_server =
        ScriptedHttpServer::respond(empty_response("500 Internal Server Error"));
    let replacement = fixture.artifact(&replacement_server, OTHER_ACTIVATION_ID, future_time());
    let replacement_path = fixture.root.path().join("replacement-activation.json");
    write_artifact(&replacement_path, &replacement);
    assert!(matches!(
        enroll(Some(&replacement_path), &fixture.config_path, false, false,),
        Err(EnrollmentError::NetworkAmbiguous)
    ));
    let replacement_journal = fs::read(&journal_path).expect("read replacement journal");
    assert!(String::from_utf8_lossy(&replacement_journal).contains(OTHER_ACTIVATION_ID));
    replacement_server.finish_one();
}

#[test]
fn resume_finishes_after_state_commits_before_journal_removal() {
    let server = ScriptedHttpServer::respond(enrollment_response());
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());
    let state_directory = fixture.state_path.parent().expect("state directory");
    ensure_private_directory(state_directory).expect("create state directory");
    let journal = stage_journal(&fixture.journal_path(), fixture.artifact.clone(), false)
        .expect("stage journal");
    let response: EnrollmentResponse =
        serde_json::from_value(enrollment_response_document()).expect("decode enrollment response");
    persist_enrollment_state(
        &fixture.state_path,
        &journal,
        &response,
        DeploymentMode::Development,
    )
    .expect("commit state before simulated crash");
    let committed_state = fs::read(&fixture.state_path).expect("read committed state");
    assert!(fixture.journal_path().exists());

    let outcome = enroll(None, &fixture.config_path, false, true).expect("resume enrollment");
    assert!(matches!(outcome, EnrollmentOutcome::Enrolled { .. }));
    assert_eq!(
        fs::read(&fixture.state_path).expect("read recovered state"),
        committed_state
    );
    assert!(!fixture.journal_path().exists());
    server.finish_one();
}

#[test]
fn replacement_resume_recognizes_a_startup_promotion_before_journal_cleanup() {
    let server = ScriptedHttpServer::respond(enrollment_response_with_credential(
        REPLACEMENT_CREDENTIAL_ID,
    ));
    let fixture = EnrollmentFixture::new(&server, OTHER_ACTIVATION_ID, future_time());
    let state_directory = fixture.state_path.parent().expect("state directory");
    ensure_private_directory(state_directory).expect("create state directory");
    atomic_write_json(
        &fixture.state_path,
        &serde_json::json!({
            "schemaVersion": 1,
            "runnerId": RUNNER_ID,
            "connectionUrl": "ws://127.0.0.1:8765/v1/runner/connect",
            "currentCredential": {
                "id": CREDENTIAL_ID,
                "secret": ACTIVATION_SECRET,
                "activationId": ACTIVATION_ID,
                "enrolledAt": "2026-08-06T12:00:00Z"
            },
            "updatedAt": "2026-08-06T12:00:00Z"
        }),
    )
    .expect("write current runner state");
    let journal = stage_journal(&fixture.journal_path(), fixture.artifact.clone(), true)
        .expect("stage replacement journal");
    let response: EnrollmentResponse = serde_json::from_value(
        enrollment_response_document_with_credential(REPLACEMENT_CREDENTIAL_ID),
    )
    .expect("decode replacement response");
    persist_enrollment_state(
        &fixture.state_path,
        &journal,
        &response,
        DeploymentMode::Development,
    )
    .expect("stage pending replacement");
    let service =
        load_runner_service_configuration(&fixture.config_path).expect("load pending state");
    service
        .state_access
        .promote(RUNNER_ID, CREDENTIAL_ID, REPLACEMENT_CREDENTIAL_ID)
        .expect("simulate startup promotion");
    let promoted_state = fs::read(&fixture.state_path).expect("read promoted state");
    assert!(fixture.journal_path().exists());

    let outcome = enroll(None, &fixture.config_path, false, true)
        .expect("resume replacement after startup promotion");
    assert!(matches!(
        outcome,
        EnrollmentOutcome::Enrolled {
            replacement: true,
            ..
        }
    ));
    assert_eq!(
        fs::read(&fixture.state_path).expect("read resumed promoted state"),
        promoted_state
    );
    assert!(!fixture.journal_path().exists());
    server.finish_one();
}

#[test]
fn journal_rejects_a_verifier_that_does_not_match_its_secret() {
    let journal = EnrollmentJournal {
        schema_version: 1,
        activation_artifact: ActivationArtifact {
            schema_version: 1,
            activation_url: format!(
                "https://api.scherzo.dev/v1/runner-enrollments/{ACTIVATION_ID}/activate"
            ),
            activation_token: format!("{ACTIVATION_ID}.{ACTIVATION_SECRET}"),
            runner_id: RUNNER_ID.to_owned(),
            expires_at: future_time(),
        },
        credential_secret: ACTIVATION_SECRET.to_owned(),
        credential_secret_verifier: ACTIVATION_SECRET.to_owned(),
        idempotency_key: "enrollment-idempotency".to_owned(),
        replace_credential: false,
        staged_at: future_time(),
    };
    assert!(matches!(
        validate_journal(&journal, DeploymentMode::Production),
        Err(EnrollmentError::InvalidJournal)
    ));
}

#[test]
fn resume_sends_an_expired_journal_instead_of_applying_local_expiry() {
    let server = ScriptedHttpServer::respond(enrollment_response());
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, expired_time());
    assert!(matches!(
        read_activation_artifact(&fixture.activation_path, DeploymentMode::Development),
        Err(EnrollmentError::ExpiredArtifact)
    ));

    let state_directory = fixture.state_path.parent().expect("state directory");
    ensure_private_directory(state_directory).expect("create state directory");
    stage_journal(&fixture.journal_path(), fixture.artifact.clone(), false)
        .expect("stage expired journal");
    let outcome = enroll(None, &fixture.config_path, false, true).expect("resume expired journal");
    assert!(matches!(outcome, EnrollmentOutcome::Enrolled { .. }));
    assert!(!fixture.journal_path().exists());
    server.finish_one();
}

struct EnrollmentFixture {
    root: tempfile::TempDir,
    artifact: ActivationArtifact,
    activation_path: PathBuf,
    config_path: PathBuf,
    state_path: PathBuf,
}

impl EnrollmentFixture {
    fn new(server: &ScriptedHttpServer, activation_id: &str, expires_at: String) -> Self {
        let root = tempfile::TempDir::new().expect("create enrollment fixture");
        fs::set_permissions(root.path(), Permissions::from_mode(0o700))
            .expect("protect enrollment fixture");
        let state_path = root.path().join("state/runner-state.json");
        let config_path = root.path().join("runner-config.json");
        let artifact = activation_artifact(server, activation_id, expires_at);
        let activation_path = root.path().join("activation.json");
        write_artifact(&activation_path, &artifact);
        let config = serde_json::json!({
            "schemaVersion": 1,
            "deploymentMode": "development",
            "runnerStatePath": state_path,
            "controlSocketPath": root.path().join("run/runner.sock"),
            "workRoot": root.path().join("work")
        });
        fs::write(
            &config_path,
            serde_json::to_vec_pretty(&config).expect("encode runner config"),
        )
        .expect("write runner config");
        Self {
            root,
            artifact,
            activation_path,
            config_path,
            state_path,
        }
    }

    fn artifact(
        &self,
        server: &ScriptedHttpServer,
        activation_id: &str,
        expires_at: String,
    ) -> ActivationArtifact {
        activation_artifact(server, activation_id, expires_at)
    }

    fn journal_path(&self) -> PathBuf {
        self.state_path
            .parent()
            .expect("state directory")
            .join(JOURNAL_FILE)
    }
}

fn activation_artifact(
    server: &ScriptedHttpServer,
    activation_id: &str,
    expires_at: String,
) -> ActivationArtifact {
    let base = server.api_url.trim_end_matches("/api/");
    ActivationArtifact {
        schema_version: 1,
        activation_url: format!("{base}/v1/runner-enrollments/{activation_id}/activate"),
        activation_token: format!("{activation_id}.{ACTIVATION_SECRET}"),
        runner_id: RUNNER_ID.to_owned(),
        expires_at,
    }
}

fn write_artifact(path: &Path, artifact: &ActivationArtifact) {
    let mut file = create_new_private_file(path).expect("create activation artifact");
    artifact
        .write_json(&mut file)
        .expect("write activation artifact");
    file.sync_all().expect("sync activation artifact");
}

fn enrollment_response() -> Vec<u8> {
    enrollment_response_with_credential(CREDENTIAL_ID)
}

fn enrollment_response_with_credential(credential_id: &str) -> Vec<u8> {
    json_response(&enrollment_response_document_with_credential(credential_id))
}

fn enrollment_response_document() -> serde_json::Value {
    enrollment_response_document_with_credential(CREDENTIAL_ID)
}

fn enrollment_response_document_with_credential(credential_id: &str) -> serde_json::Value {
    serde_json::json!({
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
        "credentialId": credential_id,
        "connectionUrl": "ws://127.0.0.1:8765/v1/runner/connect"
    })
}

fn json_response(value: &serde_json::Value) -> Vec<u8> {
    let body = serde_json::to_vec(value).expect("encode response");
    format!(
        "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes()
    .into_iter()
    .chain(body)
    .collect()
}

fn empty_response(status: &str) -> Vec<u8> {
    format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").into_bytes()
}

fn redirect_response(location: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    .into_bytes()
}

fn request_body(request: &str) -> &str {
    request
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .expect("request should contain a body separator")
}

fn request_header<'a>(request: &'a str, expected_name: &str) -> &'a str {
    request
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case(expected_name)
                .then_some(value.trim())
        })
        .expect("request should contain the expected header")
}

fn future_time() -> String {
    (crate::timing::utc_now() + time::Duration::days(1))
        .format(&Rfc3339)
        .expect("format future time")
}

fn expired_time() -> String {
    (crate::timing::utc_now() - time::Duration::days(3))
        .format(&Rfc3339)
        .expect("format expired time")
}
