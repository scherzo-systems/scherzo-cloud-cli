use super::*;

use std::process::Stdio;

const TOKEN: &str = "unique-runner-command-token-sentinel";
const ORGANIZATION: &str = "acme-research";
const ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
const POOL_ID: &str = "rpl_01k0z6r1w8f4jy2m7q9v3x5abc";
const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
const ACTIVATION_ID: &str = "rna_01k0z6r1w8f4jy2m7q9v3x5abc";
const ACTIVATION_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn prepared_runner(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, String) {
    let server = ScriptedServer::respond(responses);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let credential_path = credential_path.to_str().unwrap().to_owned();
    (server, credential_directory, credential_path)
}

fn request_body(request: &str) -> &str {
    request
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .expect("request should contain a body separator")
}

fn pool_body() -> serde_json::Value {
    serde_json::json!({
        "id": POOL_ID,
        "organizationId": ORGANIZATION_ID,
        "name": "builders",
        "createdAt": "2026-08-09T12:00:00Z",
        "updatedAt": "2026-08-09T12:00:00Z"
    })
}

fn activation_issuance_body() -> serde_json::Value {
    serde_json::json!({
        "activation": {
            "id": ACTIVATION_ID,
            "runnerId": RUNNER_ID,
            "state": "issued",
            "issuedAt": "2026-08-09T12:00:00Z",
            "expiresAt": "2026-08-09T13:00:00Z"
        },
        "artifact": {
            "schemaVersion": 1,
            "activationUrl": format!("https://api.scherzo.dev/v1/runner-enrollments/{ACTIVATION_ID}/activate"),
            "activationToken": format!("{ACTIVATION_ID}.{ACTIVATION_SECRET}"),
            "runnerId": RUNNER_ID,
            "expiresAt": "2026-08-09T13:00:00Z"
        }
    })
}

fn registration_body() -> serde_json::Value {
    serde_json::json!({
        "id": RUNNER_ID,
        "organizationId": ORGANIZATION_ID,
        "runnerPool": {"id": POOL_ID, "name": "builders"},
        "name": "builder-one",
        "administration": {
            "mode": "draining",
            "createdAt": "2026-08-09T12:00:00Z",
            "updatedAt": "2026-08-09T12:01:00Z"
        },
        "enrollment": {
            "state": "credentialed",
            "firstEnrolledAt": "2026-08-09T12:02:00Z",
            "validCredentialCount": 1
        },
        "connectivity": {
            "state": "online",
            "connectedAt": "2026-08-09T12:03:00Z",
            "lastSeenAt": "2026-08-09T12:04:00Z"
        },
        "activity": {"state": "assigned", "currentAssignmentCount": 1},
        "advertisedMetadata": {
            "runnerVersion": "1.2.3",
            "protocolVersion": 1,
            "advertisedCapacity": 7
        }
    })
}

#[test]
fn runner_show_without_a_credential_reports_sign_in_on_stderr() {
    let output = run(&["runner", "show", ORGANIZATION, RUNNER_ID]);

    assert_eq!(output.status.code(), Some(3));
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        concat!(
            "error: runner administration requires sign-in\n\n",
            "Sign in first:\n",
            "  scherzo-cloud auth login\n"
        )
    );
}

#[test]
fn pool_create_sends_the_name_and_reports_the_created_pool() {
    let response = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/runner-pools/rpl_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&pool_body()).unwrap(),
    );
    let (server, _directory, credential_path) = prepared_runner(vec![response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "runner",
            "pool",
            "create",
            ORGANIZATION,
            "--name",
            "builders",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!(
            concat!(
                "✓ Runner pool created.\n\n",
                "  Pool:         {}\n",
                "  Name:         builders\n",
                "  Organization: {}\n",
                "  Deployment:   {}\n"
            ),
            POOL_ID, ORGANIZATION_ID, server.api_url
        )
    );
    assert!(output.stderr.is_empty());

    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "POST /api/v1/organizations/{ORGANIZATION}/runner-pools HTTP/1.1\r\n"
    )));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {TOKEN}")
    );
    assert_eq!(header_value(&request, "content-type"), "application/json");
    let key = header_value(&request, "idempotency-key");
    assert_eq!(key.len(), 64);
    assert!(key.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&request)).unwrap(),
        serde_json::json!({"name": "builders"})
    );
}

#[test]
fn runner_create_stdout_contains_only_the_transferable_artifact() {
    let registration = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runner-registrations/rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&registration_body()).unwrap(),
    );
    let issuance = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runner-registrations/rnr_01k0z6r1w8f4jy2m7q9v3x5abc/activations/rna_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&activation_issuance_body()).unwrap(),
    );
    let (server, _directory, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", pool_body()),
        registration,
        issuance,
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "runner",
            "create",
            ORGANIZATION,
            "--pool",
            POOL_ID,
            "--activation-file",
            "-",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let artifact: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(artifact, activation_issuance_body()["artifact"]);
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("Runner rnr_"));
    assert!(!stderr.contains(ACTIVATION_SECRET));

    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[1].starts_with(&format!(
        "POST /api/v1/organizations/{ORGANIZATION}/runner-registrations HTTP/1.1\r\n"
    )));
    assert!(requests[2].starts_with(&format!(
        "POST /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}/activations HTTP/1.1\r\n"
    )));
}

#[test]
fn activation_stdout_contains_only_the_transferable_artifact() {
    let issuance = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runner-registrations/rnr_01k0z6r1w8f4jy2m7q9v3x5abc/activations/rna_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&activation_issuance_body()).unwrap(),
    );
    let (server, _directory, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", registration_body()),
        issuance,
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "runner",
            "activation",
            "create",
            ORGANIZATION,
            RUNNER_ID,
            "--activation-file",
            "-",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let artifact: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(artifact, activation_issuance_body()["artifact"]);
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("Runner activation created"));
    assert!(!stderr.contains(ACTIVATION_SECRET));

    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].starts_with(&format!(
        "POST /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}/activations HTTP/1.1\r\n"
    )));
}

#[test]
fn enrollment_accepts_an_artifact_from_explicit_stdin() {
    let enrollment = serde_json::json!({
        "schemaVersion": 1,
        "runnerId": RUNNER_ID,
        "runnerName": "builder-one",
        "organization": {
            "id": ORGANIZATION_ID,
            "displayName": "Acme Research"
        },
        "runnerPool": {
            "id": POOL_ID,
            "name": "builders"
        },
        "credentialId": "rrc_01k0z6r1w8f4jy2m7q9v3x5abc",
        "connectionUrl": "ws://127.0.0.1:8765/v1/runner/connect"
    });
    let server = ScriptedServer::respond(vec![json_http_response("201 Created", enrollment)]);
    let directory = tempfile::tempdir().unwrap();
    let state_path = directory.path().join("state/runner-state.json");
    let config_path = directory.path().join("runner.json");
    fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 1,
            "deploymentMode": "development",
            "runnerStatePath": state_path,
            "controlSocketPath": directory.path().join("run/runner.sock"),
            "workRoot": directory.path().join("work"),
            "developmentWorkflow": {
                "workflowId": "wfl_01k0z6r1w8f4jy2m7q9v3x5abc",
                "sourceRoot": directory.path().join("source"),
                "workflowPath": "build/workflow.json"
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let activation_url = format!(
        "{}/v1/runner-enrollments/{ACTIVATION_ID}/activate",
        server.api_url.trim_end_matches("/api")
    );
    let artifact = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 1,
        "activationUrl": activation_url,
        "activationToken": format!("{ACTIVATION_ID}.{ACTIVATION_SECRET}"),
        "runnerId": RUNNER_ID,
        "expiresAt": "2999-01-01T00:00:00Z"
    }))
    .unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args([
            "runner",
            "enroll",
            "--activation-file",
            "-",
            "--config",
            config_path.to_str().unwrap(),
            "--json",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(&artifact).unwrap();
    let output = child.wait_with_output().unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "enrolled");
    let state: serde_json::Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    let secret = state["currentCredential"]["secret"].as_str().unwrap();
    assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));

    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "POST /v1/runner-enrollments/{ACTIVATION_ID}/activate HTTP/1.1\r\n"
    )));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {ACTIVATION_ID}.{ACTIVATION_SECRET}")
    );
    assert!(!request_body(&request).contains(secret));
}

#[cfg(target_os = "linux")]
#[test]
fn enrollment_rejects_terminal_stdin_before_reading_configuration() {
    let pty = nix::pty::openpty(
        None::<&nix::pty::Winsize>,
        None::<&nix::sys::termios::Termios>,
    )
    .unwrap();
    let child_input = rustix::io::dup(&pty.slave).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let missing_config = directory.path().join("missing-runner-config.json");
    let output = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args([
            "runner",
            "enroll",
            "--activation-file",
            "-",
            "--config",
            missing_config.to_str().unwrap(),
        ])
        .stdin(Stdio::from(child_input))
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("terminal stdin cannot supply an activation artifact"));
    assert!(!stderr.contains("operator configuration is invalid"));
}

#[test]
fn runner_show_reports_independent_cloud_and_informational_projections() {
    let (server, _directory, credential_path) =
        prepared_runner(vec![json_http_response("200 OK", registration_body())]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "runner",
            "show",
            ORGANIZATION,
            RUNNER_ID,
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    for expected in [
        "  Administration\n    Mode:       draining",
        "  Enrollment\n    State:      credentialed\n    Credentials: 1 valid",
        "  Connectivity\n    State:      online",
        "  Activity\n    State:      assigned\n    Assignments: 1 current",
        "  Advertised metadata (informational)\n    Runner version: 1.2.3",
    ] {
        assert!(
            stdout.contains(expected),
            "missing {expected:?} in {stdout:?}"
        );
    }
    assert!(!stdout.contains("Status:"));
    assert!(stdout.ends_with(&format!("\n  Deployment: {}\n", server.api_url)));
    assert!(output.stderr.is_empty());

    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID} HTTP/1.1\r\n"
    )));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {TOKEN}")
    );
}
