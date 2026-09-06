use super::*;

const CURRENT_TOKEN: &str = "unique-current-identity-session-token";
const CURRENT_IDENTITY_ID: &str = "idn_01k0z6r1w8f4jy2m7q9v3x5abc";
const LINKED_IDENTITY_ID: &str = "idn_01k0z6r1w8f4jy2m7q9v3x5abd";

fn identity(id: &str, issuer: &str, subject: &str, current: bool) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "kind": "oidc",
        "issuer": issuer,
        "subject": subject,
        "assertedEmail": format!("{subject}@example.test"),
        "emailVerified": true,
        "createdAt": "2026-09-05T12:00:00Z",
        "current": current
    })
}

fn identity_page(items: serde_json::Value, next_cursor: Option<&str>) -> Vec<u8> {
    let mut body = serde_json::json!({"items": items});
    if let Some(cursor) = next_cursor {
        body["nextCursor"] = serde_json::Value::String(cursor.to_owned());
    }
    json_http_response("200 OK", body)
}

fn identity_problem(status: &str, code: u16, problem_type: &str) -> Vec<u8> {
    problem_http_response(
        status,
        serde_json::json!({
            "type": problem_type,
            "title": "Identity operation result",
            "status": code
        }),
    )
}

fn link_success(identity: serde_json::Value) -> Vec<u8> {
    http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/me/identities/idn_01k0z6r1w8f4jy2m7q9v3x5abd",
            ),
        ],
        &serde_json::to_vec(&identity).unwrap(),
    )
}

fn identity_link_flow_responses(mut terminal: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    let mut responses = vec![
        identity_page(
            serde_json::json!([identity(
                CURRENT_IDENTITY_ID,
                "https://issuer.example/",
                "ada-current",
                true
            )]),
            None,
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "unique-private-link-device-code",
                "user_code": "LINK-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "verification_uri_complete": "https://auth.fixture.example/activate?user_code=LINK-CODE",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-proposed-identity-access-token",
                "token_type": "Bearer",
                "expires_in": 300
            }),
        ),
    ];
    responses.append(&mut terminal);
    responses
}

fn prepared_identity_command(
    responses: Vec<Vec<u8>>,
) -> (ScriptedServer, tempfile::TempDir, std::path::PathBuf) {
    let server = ScriptedServer::respond(responses);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        CURRENT_TOKEN,
        "2999-01-01T00:00:00Z",
    );
    (server, credential_directory, credential_path)
}

#[test]
fn identity_family_without_a_leaf_prints_help_without_loading_deployment() {
    let output = run_with_env(
        &["auth", "identities"],
        &[("SCHERZO_CLOUD_API_URL", "partial-override-is-ignored")],
    );

    assert!(output.status.success());
    assert!(!output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}

#[test]
fn list_returns_one_exact_page_with_current_and_provenance_fields() {
    let server = ScriptedServer::respond(vec![identity_page(
        serde_json::json!([
            identity(
                CURRENT_IDENTITY_ID,
                "https://issuer.example/",
                "ada-current",
                true
            ),
            identity(
                LINKED_IDENTITY_ID,
                "https://work.example/",
                "ada-work",
                false
            )
        ]),
        Some("opaque-next-page"),
    )]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        CURRENT_TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "auth",
            "identities",
            "list",
            "--limit",
            "2",
            "--cursor",
            "page-before",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "listed",
            "items": [
                identity(CURRENT_IDENTITY_ID, "https://issuer.example/", "ada-current", true),
                identity(LINKED_IDENTITY_ID, "https://work.example/", "ada-work", false)
            ],
            "nextCursor": "opaque-next-page"
        })
    );
    assert!(output.stderr.is_empty());
    let request = server.finish().pop().unwrap();
    assert!(
        request.starts_with("GET /api/v1/me/identities?limit=2&cursor=page-before HTTP/1.1\r\n")
    );
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {CURRENT_TOKEN}")
    );
}

#[test]
fn link_uses_fresh_separate_browser_proof_and_keeps_the_local_session() {
    let linked = identity(
        LINKED_IDENTITY_ID,
        "https://work.example/",
        "ada-work",
        false,
    );
    let (server, _directory, credential_path) =
        prepared_identity_command(identity_link_flow_responses(vec![
            Vec::new(),
            link_success(linked.clone()),
        ]));
    let before = fs::read(&credential_path).unwrap();
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "auth",
            "identities",
            "link",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let events = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["event"], "activation_required");
    assert_eq!(events[0]["operation"], "identity_link");
    assert_eq!(
        events[1],
        serde_json::json!({
            "schemaVersion": 1,
            "event": "result",
            "deployment": server.api_url,
            "outcome": "linked",
            "identity": linked,
            "localSessionIdentity": "unchanged"
        })
    );
    assert!(output.stderr.is_empty());
    assert_eq!(fs::read(&credential_path).unwrap(), before);
    let requests = server.finish();
    assert_eq!(requests.len(), 5);
    assert!(requests[0].starts_with("GET /api/v1/me/identities?limit=1 HTTP/1.1\r\n"));
    assert!(requests[1].starts_with("POST /auth/oauth/device/code HTTP/1.1\r\n"));
    assert_eq!(
        request_form(&requests[1]).get("scope").map(String::as_str),
        Some("openid profile email")
    );
    assert!(requests[2].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert_eq!(requests[3], requests[4]);
    assert!(requests[3].starts_with("POST /api/v1/me/identities HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&requests[3], "authorization"),
        format!("Bearer {CURRENT_TOKEN}")
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(requests[3].split_once("\r\n\r\n").unwrap().1)
            .unwrap(),
        serde_json::json!({
            "proposedIdentityAccessToken": "unique-proposed-identity-access-token"
        })
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for secret in [
        CURRENT_TOKEN,
        "unique-private-link-device-code",
        "unique-proposed-identity-access-token",
    ] {
        assert!(!combined.contains(secret));
    }
}

#[test]
fn link_keeps_the_preflight_acting_session_across_browser_proof() {
    use std::io::{BufRead as _, BufReader};
    use std::process::Stdio;

    let linked = identity(
        LINKED_IDENTITY_ID,
        "https://work.example/",
        "ada-work",
        false,
    );
    let mut server = ScriptedServer::start(
        identity_link_flow_responses(vec![link_success(linked)]),
        Some(2),
    );
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        CURRENT_TOKEN,
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
            "auth",
            "identities",
            "link",
            "--json",
            "--allow-insecure-http",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE)
        .env("PATH", empty_path.path());
    for variable in DEPLOYMENT_VARIABLES
        .into_iter()
        .chain(RUNNER_TELEMETRY_VARIABLES)
    {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let mut child = command.spawn().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut activation_line = String::new();
    stdout.read_line(&mut activation_line).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(activation_line.trim()).unwrap()["event"],
        "activation_required"
    );

    assert!(
        server
            .next_request()
            .starts_with("GET /api/v1/me/identities?limit=1 HTTP/1.1\r\n")
    );
    assert!(
        server
            .next_request()
            .starts_with("POST /auth/oauth/device/code HTTP/1.1\r\n")
    );
    assert!(
        server
            .next_request()
            .starts_with("POST /auth/oauth/token HTTP/1.1\r\n")
    );

    fs::remove_file(&credential_path).unwrap();
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "replacement-acting-session-token",
        "2999-01-01T00:00:00Z",
    );
    server.release_paused_response();

    let status = child.wait().unwrap();
    assert!(status.success());
    let link_request = server.next_request();
    assert!(link_request.starts_with("POST /api/v1/me/identities HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&link_request, "authorization"),
        format!("Bearer {CURRENT_TOKEN}"),
        "the browser proof must not be attached to an account that replaced the acting session while the command was waiting"
    );
}

#[test]
fn link_reports_identity_unavailable_without_exposing_or_replacing_credentials() {
    let conflict = identity_problem(
        "409 Conflict",
        409,
        "https://api.scherzo.dev/problems/identity-unavailable",
    );
    let (server, _directory, credential_path) =
        prepared_identity_command(identity_link_flow_responses(vec![conflict]));
    let before = fs::read(&credential_path).unwrap();
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "auth",
            "identities",
            "link",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let events = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["outcome"], "identity_unavailable");
    assert_eq!(events[1]["localSessionIdentity"], "unchanged");
    assert_eq!(fs::read(&credential_path).unwrap(), before);
    assert!(output.stderr.is_empty());
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    let combined = String::from_utf8_lossy(&output.stdout);
    assert!(!combined.contains(CURRENT_TOKEN));
    assert!(!combined.contains("unique-proposed-identity-access-token"));
}

#[test]
fn link_does_not_report_an_unchanged_session_after_rejected_credential_cleanup() {
    let rejected = identity_problem(
        "401 Unauthorized",
        401,
        "https://api.scherzo.dev/problems/unauthorized",
    );
    let (server, _directory, credential_path) =
        prepared_identity_command(identity_link_flow_responses(vec![
            rejected,
            json_http_response(
                "400 Bad Request",
                serde_json::json!({"error": "invalid_grant"}),
            ),
        ]));
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "auth",
            "identities",
            "link",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(3));
    let events = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["outcome"], "unauthenticated");
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert!(stored["credentials"].as_array().unwrap().is_empty());
    assert_eq!(events[1]["localSessionIdentity"], "removed");
    assert!(output.stderr.is_empty());
    assert_eq!(server.finish().len(), 5);
}

#[test]
fn removing_the_former_session_identity_keeps_the_forced_login_session() {
    let removal = http_response_with_headers(
        "204 No Content",
        None,
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &[],
    );
    let (server, _directory, credential_path) = prepared_identity_command(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "unique-replacement-device-code",
                "user_code": "REPLACE-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-replacement-session-token",
                "refresh_token": "unique-replacement-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "principal": {
                    "id": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "type": "human",
                    "state": "active"
                }
            }),
        ),
        removal,
    ]);
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let login = run_with_env(
        &[
            "auth",
            "login",
            "--force",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(login.status.success());
    let replacement_session = fs::read(&credential_path).unwrap();
    assert!(
        replacement_session
            .windows("unique-replacement-session-token".len())
            .any(|window| window == b"unique-replacement-session-token")
    );

    let removal = run_with_env(
        &[
            "auth",
            "identities",
            "remove",
            CURRENT_IDENTITY_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(removal.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&removal.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "removed",
            "identityId": CURRENT_IDENTITY_ID,
            "localSessionIdentity": "unchanged"
        })
    );
    assert!(removal.stderr.is_empty());
    assert_eq!(fs::read(&credential_path).unwrap(), replacement_session);
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert!(requests[3].starts_with(&format!(
        "DELETE /api/v1/me/identities/{CURRENT_IDENTITY_ID} HTTP/1.1\r\n"
    )));
    assert_eq!(
        header_value(&requests[3], "authorization"),
        "Bearer unique-replacement-session-token"
    );
    assert_eq!(requests[3].split_once("\r\n\r\n").unwrap().1, "");
}

#[test]
fn remove_reports_freshness_retention_and_private_target_outcomes() {
    let cases = [
        (
            identity_problem(
                "403 Forbidden",
                403,
                "https://api.scherzo.dev/problems/reauthentication-required",
            ),
            "reauthentication_required",
        ),
        (
            identity_problem(
                "404 Not Found",
                404,
                "https://api.scherzo.dev/problems/identity-not-found",
            ),
            "not_found",
        ),
        (
            identity_problem(
                "409 Conflict",
                409,
                "https://api.scherzo.dev/problems/identity-removal-unavailable",
            ),
            "removal_unavailable",
        ),
    ];

    for (response, expected_outcome) in cases {
        let (server, _directory, credential_path) = prepared_identity_command(vec![response]);
        let environment = deployment_environment_with_issuer(
            &server.api_url,
            &server.issuer,
            credential_path.to_str().unwrap(),
        );

        let output = run_with_env(
            &[
                "auth",
                "identities",
                "remove",
                LINKED_IDENTITY_ID,
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["outcome"], expected_outcome);
        assert!(output.stderr.is_empty());
        server.finish();
    }
}

#[test]
fn linked_identity_commands_require_the_human_session_before_network_work() {
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let environment =
        deployment_environment("http://127.0.0.1:9/api", credential_path.to_str().unwrap());

    for args in [
        &[
            "auth",
            "identities",
            "list",
            "--json",
            "--allow-insecure-http",
        ][..],
        &[
            "auth",
            "identities",
            "link",
            "--json",
            "--allow-insecure-http",
        ][..],
    ] {
        let output = run_with_env(args, &environment);
        assert_eq!(output.status.code(), Some(3));
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["outcome"], "unauthenticated");
        assert!(output.stderr.is_empty());
    }
}
