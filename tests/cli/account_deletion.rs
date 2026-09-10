use std::process::Stdio;

use super::*;

const ACCOUNT_TOKEN: &str = "unique-account-deletion-session-token";
const PRINCIPAL_ID: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5abc";

fn schedule_response() -> Vec<u8> {
    http_response_with_headers(
        "202 Accepted",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "id": PRINCIPAL_ID,
            "kind": "principal",
            "state": "deletion_pending",
            "requestedAt": "2026-09-05T12:00:00Z",
            "deadline": "2026-10-05T12:00:00Z",
            "updatedAt": "2026-09-05T12:00:00Z"
        }))
        .unwrap(),
    )
}

fn cancellation_response() -> Vec<u8> {
    cancellation_response_with_headers(&[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)])
}

fn cancellation_response_with_headers(headers: &[(&str, &str)]) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        headers,
        &serde_json::to_vec(&serde_json::json!({
            "id": PRINCIPAL_ID,
            "kind": "principal",
            "state": "active",
            "updatedAt": "2026-09-06T12:00:00Z"
        }))
        .unwrap(),
    )
}

fn lifecycle_problem(status: &str, code: u16, problem_type: &str) -> Vec<u8> {
    problem_http_response(
        status,
        serde_json::json!({
            "type": problem_type,
            "title": "lifecycle-problem-title-sentinel",
            "status": code,
            "detail": "lifecycle-problem-detail-sentinel"
        }),
    )
}

fn cancellation_flow_responses(terminal: Vec<u8>) -> Vec<Vec<u8>> {
    vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "unique-account-cancellation-device-code",
                "user_code": "KEEP-ACCOUNT",
                "verification_uri": "https://auth.fixture.example/activate",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-account-cancellation-proof",
                "token_type": "Bearer",
                "expires_in": 300
            }),
        ),
        terminal,
    ]
}

fn prepared_account_request(
    responses: Vec<Vec<u8>>,
) -> (ScriptedServer, tempfile::TempDir, std::path::PathBuf) {
    let server = ScriptedServer::respond(responses);
    let directory = private_credential_directory();
    let credential_path = directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        ACCOUNT_TOKEN,
        "2999-01-01T00:00:00Z",
    );
    (server, directory, credential_path)
}

#[test]
fn account_deletion_request_schedules_thirty_days_and_removes_the_local_session() {
    let (server, _directory, credential_path) =
        prepared_account_request(vec![Vec::new(), schedule_response()]);
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "account",
            "deletion",
            "request",
            "--yes",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!(
            concat!(
                "✓ Account deletion scheduled.\n\n",
                "principal: {}\n",
                "state: deletion_pending\n",
                "requested: 2026-09-05T12:00:00Z\n",
                "deadline: 2026-10-05T12:00:00Z\n",
                "updated: 2026-09-05T12:00:00Z\n",
                "local credential: removed\n",
                "deployment: {}\n\n",
                "Cancel before the deadline with fresh browser proof:\n",
                "  scherzo-cloud account deletion cancel\n"
            ),
            PRINCIPAL_ID, server.api_url
        )
    );
    assert!(output.stderr.is_empty());
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert!(stored["credentials"].as_array().unwrap().is_empty());

    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
    assert!(requests[0].starts_with("POST /api/v1/me/deletion HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&requests[0], "authorization"),
        format!("Bearer {ACCOUNT_TOKEN}")
    );
    assert_eq!(
        header_value(&requests[0], "accept"),
        "application/json, application/problem+json"
    );
    assert_eq!(requests[0].split_once("\r\n\r\n").unwrap().1, "");
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
}

#[test]
fn account_deletion_accepts_a_schedule_updated_at_the_deadline() {
    let response = http_response_with_headers(
        "202 Accepted",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "id": PRINCIPAL_ID,
            "kind": "principal",
            "state": "deletion_pending",
            "requestedAt": "2026-09-05T12:00:00Z",
            "deadline": "2026-10-05T12:00:00Z",
            "updatedAt": "2026-10-05T12:00:00Z"
        }))
        .unwrap(),
    );
    let (server, _directory, credential_path) = prepared_account_request(vec![response]);
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "account",
            "deletion",
            "request",
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "scheduled");
    assert_eq!(result["schedule"]["updatedAt"], "2026-10-05T12:00:00Z");
    assert_eq!(result["localCredential"], "removed");
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert!(stored["credentials"].as_array().unwrap().is_empty());
    assert!(output.stderr.is_empty());
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn account_deletion_preserves_a_concurrently_replaced_credential_pair() {
    let mut server = ScriptedServer::respond_with_paused_last_response(vec![schedule_response()]);
    let directory = private_credential_directory();
    let credential_path = directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        ACCOUNT_TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );
    let empty_path = tempfile::tempdir().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args([
            "account",
            "deletion",
            "request",
            "--yes",
            "--json",
            "--allow-insecure-http",
        ])
        .env_remove(CREDENTIALS_FILE_VARIABLE)
        .env("PATH", empty_path.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for variable in DEPLOYMENT_VARIABLES
        .into_iter()
        .chain(RUNNER_TELEMETRY_VARIABLES)
    {
        command.env_remove(variable);
    }
    for (name, value) in &environment {
        command.env(name, value);
    }
    let child = command.spawn().unwrap();

    let request = server.next_request();
    assert!(request.starts_with("POST /api/v1/me/deletion HTTP/1.1\r\n"));
    let mut replacement: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    replacement["credentials"][0]["refreshToken"] =
        serde_json::Value::String("concurrent-replacement-refresh-token".to_owned());
    fs::write(
        &credential_path,
        serde_json::to_vec_pretty(&replacement).unwrap(),
    )
    .unwrap();
    server.release_paused_response();

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["localCredential"], "changed");
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert_eq!(
        stored["credentials"][0]["refreshToken"],
        "concurrent-replacement-refresh-token"
    );
    assert!(output.stderr.is_empty());
    assert!(server.finish().is_empty());
}

#[test]
fn account_deletion_request_emits_json_for_an_invalid_api_response() {
    let response = http_response(
        "202 Accepted",
        Some("application/json"),
        &serde_json::to_vec(&serde_json::json!({
            "id": PRINCIPAL_ID,
            "kind": "principal",
            "state": "deletion_pending",
            "requestedAt": "2026-09-05T12:00:00Z",
            "deadline": "2026-10-05T12:00:00Z",
            "updatedAt": "2026-09-05T12:00:00Z"
        }))
        .unwrap(),
    );
    let (server, _directory, credential_path) = prepared_account_request(vec![response]);
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );
    let before = fs::read(&credential_path).unwrap();

    let output = run_with_env(
        &[
            "account",
            "deletion",
            "request",
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "invalid_response"
        })
    );
    assert!(output.stderr.is_empty());
    assert_eq!(fs::read(&credential_path).unwrap(), before);
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn account_deletion_request_reports_owner_and_existing_schedule_conflicts() {
    for (problem_type, expected_outcome) in [
        (
            "https://api.scherzo.dev/problems/human-owner-required",
            "human_owner_required",
        ),
        (
            "https://api.scherzo.dev/problems/lifecycle-transition-unavailable",
            "transition_unavailable",
        ),
    ] {
        let response = lifecycle_problem("409 Conflict", 409, problem_type);
        let (server, _directory, credential_path) = prepared_account_request(vec![response]);
        let environment = deployment_environment_with_issuer(
            &server.api_url,
            &server.issuer,
            credential_path.to_str().unwrap(),
        );
        let before = fs::read(&credential_path).unwrap();

        let output = run_with_env(
            &[
                "account",
                "deletion",
                "request",
                "--yes",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["schemaVersion"], 1);
        assert_eq!(result["deployment"], server.api_url);
        assert_eq!(result["outcome"], expected_outcome);
        assert!(result.get("schedule").is_none());
        assert!(result.get("localCredential").is_none());
        assert!(result.get("title").is_none());
        assert!(result.get("detail").is_none());
        assert!(output.stderr.is_empty());
        assert_eq!(fs::read(&credential_path).unwrap(), before);
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn account_deletion_cancellation_uses_fresh_unstored_browser_proof() {
    let server = ScriptedServer::respond(cancellation_flow_responses(cancellation_response()));
    let directory = private_credential_directory();
    let credential_path = directory.path().join("credentials.json");
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "account",
            "deletion",
            "cancel",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let events = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["event"], "activation_required");
    assert_eq!(events[0]["operation"], "account_deletion_cancellation");
    assert_eq!(
        events[1],
        serde_json::json!({
            "schemaVersion": 1,
            "event": "result",
            "deployment": server.api_url,
            "outcome": "cancelled",
            "transition": {
                "id": PRINCIPAL_ID,
                "kind": "principal",
                "state": "active",
                "updatedAt": "2026-09-06T12:00:00Z"
            },
            "localCredential": "unchanged",
            "proofCredentialStored": false
        })
    );
    assert!(output.stderr.is_empty());
    assert!(!credential_path.exists());

    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].starts_with("POST /auth/oauth/device/code HTTP/1.1\r\n"));
    assert_eq!(
        request_form(&requests[0]).get("scope").map(String::as_str),
        Some("openid profile email")
    );
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert!(requests[2].starts_with("DELETE /api/v1/me/deletion HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&requests[2], "authorization"),
        "Bearer unique-account-cancellation-proof"
    );
    assert_eq!(requests[2].split_once("\r\n\r\n").unwrap().1, "");
    let combined = events
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<String>();
    assert!(!combined.contains("unique-account-cancellation-proof"));
    assert!(!combined.contains("unique-account-cancellation-device-code"));
}

#[test]
fn account_deletion_cancellation_emits_a_terminal_result_for_an_invalid_api_response() {
    let terminal = cancellation_response_with_headers(&[]);
    let server = ScriptedServer::respond(cancellation_flow_responses(terminal));
    let directory = private_credential_directory();
    let credential_path = directory.path().join("credentials.json");
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "account",
            "deletion",
            "cancel",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let events = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["event"], "activation_required");
    assert_eq!(
        events[1],
        serde_json::json!({
            "schemaVersion": 1,
            "event": "result",
            "deployment": server.api_url,
            "outcome": "invalid_response",
            "localCredential": "unchanged",
            "proofCredentialStored": false
        })
    );
    assert!(output.stderr.is_empty());
    assert!(!credential_path.exists());
    assert_eq!(server.finish().len(), 3);
}

#[test]
fn account_deletion_cancellation_reports_reauthentication_and_already_cancelled_states() {
    for (problem_type, expected_outcome) in [
        (
            "https://api.scherzo.dev/problems/reauthentication-required",
            "reauthentication_required",
        ),
        (
            "https://api.scherzo.dev/problems/lifecycle-transition-unavailable",
            "transition_unavailable",
        ),
    ] {
        let (status, code) = if expected_outcome == "reauthentication_required" {
            ("403 Forbidden", 403)
        } else {
            ("409 Conflict", 409)
        };
        let terminal = lifecycle_problem(status, code, problem_type);
        let server = ScriptedServer::respond(cancellation_flow_responses(terminal));
        let directory = private_credential_directory();
        let credential_path = directory.path().join("credentials.json");
        let environment = deployment_environment_with_issuer(
            &server.api_url,
            &server.issuer,
            credential_path.to_str().unwrap(),
        );

        let output = run_with_env(
            &[
                "account",
                "deletion",
                "cancel",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        let events = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1]["outcome"], expected_outcome);
        assert_eq!(events[1]["localCredential"], "unchanged");
        assert_eq!(events[1]["proofCredentialStored"], false);
        assert!(events[1].get("transition").is_none());
        assert!(output.stderr.is_empty());
        assert!(!credential_path.exists());
        assert_eq!(server.finish().len(), 3);
    }
}

#[test]
fn account_deletion_request_requires_explicit_confirmation() {
    let output = run(&["account", "deletion", "request"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
}
