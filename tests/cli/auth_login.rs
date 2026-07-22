use std::io::{BufRead, BufReader};
use std::process::Stdio;

use super::*;

struct ScriptedServer {
    api_url: String,
    issuer: String,
    requests: Receiver<String>,
    thread: JoinHandle<()>,
    remaining_requests: usize,
    response_release: Option<mpsc::SyncSender<()>>,
}

impl ScriptedServer {
    fn respond(responses: Vec<Vec<u8>>) -> Self {
        Self::start(responses, false)
    }

    fn respond_with_paused_last_response(responses: Vec<Vec<u8>>) -> Self {
        Self::start(responses, true)
    }

    fn start(responses: Vec<Vec<u8>>, pause_last_response: bool) -> Self {
        let remaining_requests = responses.len();
        let listener = TcpListener::bind("127.0.0.1:0").expect("fixture listener should bind");
        let address = listener.local_addr().unwrap();
        let (sender, requests) = mpsc::channel();
        let (response_release, mut release_receiver) = if pause_last_response {
            let (sender, receiver) = mpsc::sync_channel(0);
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        let thread = thread::spawn(move || {
            for (index, response) in responses.into_iter().enumerate() {
                let (mut stream, _) = listener.accept().expect("fixture request should arrive");
                let request = read_request(&mut stream);
                sender
                    .send(String::from_utf8(request).expect("request should be text"))
                    .unwrap();
                if index + 1 == remaining_requests {
                    if let Some(receiver) = release_receiver.take() {
                        receiver.recv().expect("paused response should be released");
                    }
                }
                let _ = stream.write_all(&response);
            }
        });

        Self {
            api_url: format!("http://{address}/api"),
            issuer: format!("http://{address}/auth/"),
            requests,
            thread,
            remaining_requests,
            response_release,
        }
    }

    fn next_request(&mut self) -> String {
        let request = self
            .requests
            .recv_timeout(Duration::from_secs(2))
            .expect("fixture should capture request");
        self.remaining_requests -= 1;
        request
    }

    fn release_paused_response(&mut self) {
        self.response_release
            .take()
            .expect("fixture should have a paused response")
            .send(())
            .expect("paused response should be released");
    }

    fn finish(mut self) -> Vec<String> {
        assert!(
            self.response_release.is_none(),
            "paused response should be released before finishing"
        );
        let mut requests = Vec::with_capacity(self.remaining_requests);
        while self.remaining_requests > 0 {
            requests.push(self.next_request());
        }
        self.thread.join().expect("fixture server should stop");
        requests
    }
}

fn http_response(status: &str, content_type: Option<&str>, body: &[u8]) -> Vec<u8> {
    let content_type = content_type
        .map(|value| format!("Content-Type: {value}\r\n"))
        .unwrap_or_default();
    let mut response = format!(
        "HTTP/1.1 {status}\r\nConnection: close\r\n{content_type}Content-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    response
}

fn json_http_response(status: &str, value: serde_json::Value) -> Vec<u8> {
    http_response(
        status,
        Some("application/json"),
        &serde_json::to_vec(&value).unwrap(),
    )
}

fn problem_http_response(status: &str, value: serde_json::Value) -> Vec<u8> {
    http_response(
        status,
        Some("application/problem+json"),
        &serde_json::to_vec(&value).unwrap(),
    )
}

fn login_environment<'a>(
    server: &'a ScriptedServer,
    credential_path: &'a str,
) -> [(&'static str, &'a str); 5] {
    [
        (CREDENTIALS_FILE_VARIABLE, credential_path),
        ("SCHERZO_CLOUD_API_URL", &server.api_url),
        ("SCHERZO_CLOUD_AUTH_ISSUER", &server.issuer),
        ("SCHERZO_CLOUD_AUTH_AUDIENCE", "https://api.fixture.example"),
        ("SCHERZO_CLOUD_AUTH_CLIENT_ID", "fixture-public-client"),
    ]
}

fn json_lines(output: &[u8]) -> Vec<serde_json::Value> {
    String::from_utf8_lossy(output)
        .lines()
        .map(|line| serde_json::from_str(line).expect("output line should be JSON"))
        .collect()
}

#[test]
fn forced_login_emits_ndjson_persists_token_and_confirms_principal() {
    let server = ScriptedServer::respond(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "unique-private-device-code",
                "user_code": "ABCD-EFGH",
                "verification_uri": "https://auth.fixture.example/activate",
                "verification_uri_complete": "https://auth.fixture.example/activate?user_code=ABCD-EFGH",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "400 Bad Request",
            serde_json::json!({ "error": "authorization_pending" }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-new-access-token",
                "token_type": "Bearer",
                "expires_in": 300,
                "refresh_token": "unique-ignored-refresh-token"
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "id": "prn_fixture",
                "type": "human",
                "state": "active",
                "displayName": "Ada Lovelace"
            }),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-replaced-access-token",
        "2999-01-01T00:00:00Z",
    );
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);

    let output = run_with_env(
        &[
            "auth",
            "login",
            "--force",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let events = json_lines(&output.stdout);
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0],
        serde_json::json!({
            "schemaVersion": 1,
            "event": "activation_required",
            "deployment": server.api_url,
            "verificationUri": "https://auth.fixture.example/activate",
            "verificationUriComplete": "https://auth.fixture.example/activate?user_code=ABCD-EFGH",
            "userCode": "ABCD-EFGH",
            "expiresAt": events[0]["expiresAt"]
        })
    );
    time::OffsetDateTime::parse(
        events[0]["expiresAt"].as_str().unwrap(),
        &time::format_description::well_known::Rfc3339,
    )
    .expect("activation expiration should be RFC 3339");
    assert_eq!(
        events[1],
        serde_json::json!({
            "schemaVersion": 1,
            "event": "status",
            "status": {
                "schemaVersion": 1,
                "state": "authenticated",
                "deployment": server.api_url,
                "principal": {
                    "id": "prn_fixture",
                    "type": "human",
                    "state": "active",
                    "displayName": "Ada Lovelace"
                }
            }
        })
    );
    assert!(output.stderr.is_empty());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for secret in [
        "unique-private-device-code",
        "unique-replaced-access-token",
        "unique-new-access-token",
        "unique-ignored-refresh-token",
    ] {
        assert!(!combined.contains(secret));
    }

    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert!(requests[0].starts_with("POST /auth/oauth/device/code HTTP/1.1\r\n"));
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert!(requests[2].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert!(requests[3].starts_with("GET /api/v1/me HTTP/1.1\r\n"));
    assert!(requests[3].contains("authorization: Bearer unique-new-access-token\r\n"));

    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert_eq!(stored["credentials"].as_array().unwrap().len(), 1);
    assert_eq!(
        stored["credentials"][0]["accessToken"],
        "unique-new-access-token"
    );
    let expires_at = time::OffsetDateTime::parse(
        stored["credentials"][0]["expiresAt"].as_str().unwrap(),
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    assert!(expires_at > time::OffsetDateTime::now_utc());
    assert!(stored.to_string().find("refresh").is_none());
}

#[test]
fn existing_authenticated_credential_short_circuits_device_authorization() {
    let server = ScriptedServer::respond(vec![json_http_response(
        "200 OK",
        serde_json::json!({
            "id": "prn_existing",
            "type": "human",
            "state": "active"
        }),
    )]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-existing-access-token",
        "2999-01-01T00:00:00Z",
    );
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);

    let output = run_with_env(
        &["auth", "login", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    let events = json_lines(&output.stdout);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event"], "status");
    assert_eq!(events[0]["status"]["state"], "authenticated");
    assert!(output.stderr.is_empty());
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("GET /api/v1/me HTTP/1.1\r\n"));
}

#[test]
fn human_login_writes_activation_and_terminal_result_to_stdout() {
    let server = ScriptedServer::respond(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "unique-human-private-device-code",
                "user_code": "HUMAN-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "400 Bad Request",
            serde_json::json!({ "error": "access_denied" }),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);

    let output = run_with_env(
        &["auth", "login", "--force", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        concat!(
            "Sign in to Scherzo Cloud\n",
            "\n",
            "  Open: https://auth.fixture.example/activate\n",
            "  Code: HUMAN-CODE\n",
            "\n",
            "Waiting for authorization...\n",
            "\n",
            "Sign-in failed during token_polling: denied.\n"
        )
    );
    assert!(
        !output
            .stdout
            .windows("unique-human-private-device-code".len())
            .any(|window| window == b"unique-human-private-device-code")
    );
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn human_login_prefers_the_direct_link_and_explains_required_signup() {
    let server = ScriptedServer::respond(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "unique-human-signup-device-code",
                "user_code": "SIGNUP-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "verification_uri_complete": "https://auth.fixture.example/activate?user_code=SIGNUP-CODE",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-human-signup-access-token",
                "token_type": "Bearer",
                "expires_in": 300
            }),
        ),
        problem_http_response(
            "403 Forbidden",
            serde_json::json!({
                "type": "https://api.scherzo.dev/problems/principal-not-provisioned",
                "title": "Principal not provisioned",
                "status": 403
            }),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);

    let output = run_with_env(
        &["auth", "login", "--force", "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        concat!(
            "Sign in to Scherzo Cloud\n",
            "\n",
            "  Open: https://auth.fixture.example/activate?user_code=SIGNUP-CODE\n",
            "  Code: SIGNUP-CODE\n",
            "\n",
            "Waiting for authorization...\n",
            "\n",
            "✓ Signed in to Scherzo Cloud.\n",
            "! Your Scherzo Cloud account still needs to be set up.\n"
        )
    );
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn human_login_names_an_existing_authenticated_principal() {
    let server = ScriptedServer::respond(vec![json_http_response(
        "200 OK",
        serde_json::json!({
            "id": "prn_ada",
            "type": "human",
            "state": "active",
            "displayName": "Ada Lovelace"
        }),
    )]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-existing-human-access-token",
        "2999-01-01T00:00:00Z",
    );
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);

    let output = run_with_env(&["auth", "login", "--allow-insecure-http"], &environment);

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "✓ Signed in as Ada Lovelace.\n"
    );
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn unauthenticated_existing_check_starts_device_authorization() {
    let server = ScriptedServer::respond(vec![
        problem_http_response(
            "401 Unauthorized",
            serde_json::json!({
                "type": "https://api.scherzo.dev/problems/unauthorized",
                "title": "Unauthorized",
                "status": 401
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "private-expiring-device-code",
                "user_code": "EXPIRE-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "400 Bad Request",
            serde_json::json!({ "error": "expired_token" }),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);

    let output = run_with_env(
        &["auth", "login", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let events = json_lines(&output.stdout);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["event"], "activation_required");
    assert_eq!(events[1]["event"], "failed");
    assert_eq!(events[1]["outcome"], "expired");
    assert_eq!(events[1]["phase"], "token_polling");
    assert!(output.stderr.is_empty());
    let requests = server.finish();
    assert!(requests[0].starts_with("GET /api/v1/me HTTP/1.1\r\n"));
    assert!(!requests[0].contains("authorization:"));
    assert!(requests[1].starts_with("POST /auth/oauth/device/code HTTP/1.1\r\n"));
    assert!(requests[2].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
}

#[test]
fn existing_status_unreachable_emits_failure_without_device_authorization() {
    let server = ScriptedServer::respond(vec![http_response("503 Service Unavailable", None, &[])]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);

    let output = run_with_env(
        &["auth", "login", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        json_lines(&output.stdout),
        vec![serde_json::json!({
            "schemaVersion": 1,
            "event": "failed",
            "deployment": server.api_url,
            "outcome": "unreachable",
            "phase": "existing_credential_check",
            "category": "server"
        })]
    );
    assert!(output.stderr.is_empty());
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("GET /api/v1/me HTTP/1.1\r\n"));
}

#[test]
fn existing_status_protocol_error_emits_failure_without_device_authorization() {
    let server = ScriptedServer::respond(vec![json_http_response("200 OK", serde_json::json!({}))]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);

    let output = run_with_env(
        &["auth", "login", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        json_lines(&output.stdout),
        vec![serde_json::json!({
            "schemaVersion": 1,
            "event": "failed",
            "deployment": server.api_url,
            "outcome": "protocol_error",
            "phase": "existing_credential_check"
        })]
    );
    assert!(output.stderr.is_empty());
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("GET /api/v1/me HTTP/1.1\r\n"));
}

#[test]
fn denied_forced_login_preserves_the_previous_credential() {
    let server = ScriptedServer::respond(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "unique-denied-device-code",
                "user_code": "DENY-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "400 Bad Request",
            serde_json::json!({ "error": "access_denied" }),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-previous-access-token",
        "2999-01-01T00:00:00Z",
    );
    let before = fs::read(&credential_path).unwrap();
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);

    let output = run_with_env(
        &[
            "auth",
            "login",
            "--force",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let events = json_lines(&output.stdout);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["event"], "activation_required");
    assert!(events[0].get("verificationUriComplete").is_none());
    assert_eq!(
        events[1],
        serde_json::json!({
            "schemaVersion": 1,
            "event": "failed",
            "deployment": server.api_url,
            "outcome": "denied",
            "phase": "token_polling"
        })
    );
    assert!(output.stderr.is_empty());
    assert_eq!(fs::read(&credential_path).unwrap(), before);
    server.finish();
}

#[test]
fn login_unreachable_failures_report_the_active_phase() {
    let cases = [
        (
            vec![http_response("503 Service Unavailable", None, &[])],
            "device_authorization",
            1,
        ),
        (
            vec![
                json_http_response(
                    "200 OK",
                    serde_json::json!({
                        "device_code": "private-device-code",
                        "user_code": "RETRY-CODE",
                        "verification_uri": "https://auth.fixture.example/activate",
                        "expires_in": 600,
                        "interval": 1
                    }),
                ),
                http_response("503 Service Unavailable", None, &[]),
            ],
            "token_polling",
            2,
        ),
    ];

    for (responses, expected_phase, expected_events) in cases {
        let server = ScriptedServer::respond(responses);
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        let credential_path_string = credential_path.to_str().unwrap();
        let environment = login_environment(&server, credential_path_string);

        let output = run_with_env(
            &[
                "auth",
                "login",
                "--force",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        let events = json_lines(&output.stdout);
        assert_eq!(events.len(), expected_events);
        let failure = events.last().unwrap();
        assert_eq!(failure["event"], "failed");
        assert_eq!(failure["outcome"], "unreachable");
        assert_eq!(failure["phase"], expected_phase);
        assert_eq!(failure["category"], "server");
        assert!(output.stderr.is_empty());
        server.finish();
    }
}

#[test]
fn login_protocol_failures_report_the_active_phase() {
    let cases = [
        (
            vec![json_http_response("200 OK", serde_json::json!({}))],
            "device_authorization",
            1,
        ),
        (
            vec![
                json_http_response(
                    "200 OK",
                    serde_json::json!({
                        "device_code": "private-device-code",
                        "user_code": "PROTO-CODE",
                        "verification_uri": "https://auth.fixture.example/activate",
                        "expires_in": 600,
                        "interval": 1
                    }),
                ),
                json_http_response("200 OK", serde_json::json!({})),
            ],
            "token_polling",
            2,
        ),
        (
            vec![
                json_http_response(
                    "200 OK",
                    serde_json::json!({
                        "device_code": "private-device-code",
                        "user_code": "PROTO-CODE",
                        "verification_uri": "https://auth.fixture.example/activate",
                        "expires_in": 600,
                        "interval": 1
                    }),
                ),
                json_http_response(
                    "200 OK",
                    serde_json::json!({
                        "access_token": "unique-protocol-token",
                        "token_type": "Bearer",
                        "expires_in": 300
                    }),
                ),
                json_http_response("200 OK", serde_json::json!({})),
            ],
            "principal_confirmation",
            2,
        ),
    ];

    for (responses, expected_phase, expected_events) in cases {
        let server = ScriptedServer::respond(responses);
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        let credential_path_string = credential_path.to_str().unwrap();
        let environment = login_environment(&server, credential_path_string);

        let output = run_with_env(
            &[
                "auth",
                "login",
                "--force",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        let events = json_lines(&output.stdout);
        assert_eq!(events.len(), expected_events);
        let failure = events.last().unwrap();
        assert_eq!(failure["event"], "failed");
        assert_eq!(failure["outcome"], "protocol_error");
        assert_eq!(failure["phase"], expected_phase);
        assert!(output.stderr.is_empty());
        server.finish();
    }
}

#[test]
fn local_login_failure_emits_no_auth_event_or_network_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let api_url = format!("http://{address}/api");
    let issuer = format!("http://{address}/auth/");
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let malformed = br#"{"accessToken":"unique-local-login-secret""#;
    let mut credential_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&credential_path)
        .unwrap();
    credential_file.write_all(malformed).unwrap();
    drop(credential_file);
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = [
        (CREDENTIALS_FILE_VARIABLE, credential_path_string),
        ("SCHERZO_CLOUD_API_URL", api_url.as_str()),
        ("SCHERZO_CLOUD_AUTH_ISSUER", issuer.as_str()),
        ("SCHERZO_CLOUD_AUTH_AUDIENCE", "https://api.fixture.example"),
        ("SCHERZO_CLOUD_AUTH_CLIENT_ID", "fixture-public-client"),
    ];

    let output = run_with_env(
        &[
            "auth",
            "login",
            "--force",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("credential file is malformed"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("unique-local-login-secret"));
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    assert_eq!(fs::read(&credential_path).unwrap(), malformed);
}

#[test]
fn interrupting_login_emits_cancellation_and_exits_130() {
    let server = ScriptedServer::respond(vec![json_http_response(
        "200 OK",
        serde_json::json!({
            "device_code": "unique-cancelled-device-code",
            "user_code": "CANCEL-CODE",
            "verification_uri": "https://auth.fixture.example/activate",
            "expires_in": 600,
            "interval": 60
        }),
    )]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-cancellation-previous-token",
        "2999-01-01T00:00:00Z",
    );
    let before = fs::read(&credential_path).unwrap();
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args([
            "auth",
            "login",
            "--force",
            "--json",
            "--allow-insecure-http",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let mut child = command.spawn().expect("login process should start");
    let stdout = child.stdout.take().unwrap();
    let mut stdout = BufReader::new(stdout);
    let mut first_line = String::new();
    stdout
        .read_line(&mut first_line)
        .expect("activation event should be readable");
    let activation: serde_json::Value = serde_json::from_str(first_line.trim()).unwrap();
    assert_eq!(activation["event"], "activation_required");

    let pid = rustix::process::Pid::from_raw(child.id() as _).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::INT).unwrap();
    let status = child.wait().expect("login process should stop");
    let mut remaining_stdout = String::new();
    stdout.read_to_string(&mut remaining_stdout).unwrap();
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();

    assert_eq!(status.code(), Some(130));
    let cancellation: serde_json::Value =
        serde_json::from_str(remaining_stdout.trim()).expect("cancellation event should be JSON");
    assert_eq!(
        cancellation,
        serde_json::json!({
            "schemaVersion": 1,
            "event": "cancelled",
            "deployment": server.api_url
        })
    );
    assert!(stderr.is_empty());
    assert!(!first_line.contains("unique-cancelled-device-code"));
    assert!(!remaining_stdout.contains("unique-cancelled-device-code"));
    assert!(!first_line.contains("unique-cancellation-previous-token"));
    assert!(!remaining_stdout.contains("unique-cancellation-previous-token"));
    assert_eq!(fs::read(&credential_path).unwrap(), before);
    server.finish();
}

#[test]
fn interrupt_after_persistence_does_not_report_cancellation() {
    let mut server = ScriptedServer::respond_with_paused_last_response(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "private-committed-device-code",
                "user_code": "COMMIT-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-committed-access-token",
                "token_type": "Bearer",
                "expires_in": 300
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "id": "prn_committed",
                "type": "human",
                "state": "active",
                "displayName": "Committed Login"
            }),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args([
            "auth",
            "login",
            "--force",
            "--json",
            "--allow-insecure-http",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let mut child = command.spawn().expect("login process should start");
    let stdout = child.stdout.take().unwrap();
    let mut stdout = BufReader::new(stdout);
    let mut activation_line = String::new();
    stdout
        .read_line(&mut activation_line)
        .expect("activation event should be readable");

    let device_request = server.next_request();
    let token_request = server.next_request();
    let principal_request = server.next_request();
    assert!(device_request.starts_with("POST /auth/oauth/device/code HTTP/1.1\r\n"));
    assert!(token_request.starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert!(principal_request.starts_with("GET /api/v1/me HTTP/1.1\r\n"));

    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert_eq!(
        stored["credentials"][0]["accessToken"],
        "unique-committed-access-token"
    );

    let pid = rustix::process::Pid::from_raw(child.id() as _).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::INT).unwrap();
    thread::sleep(Duration::from_millis(100));
    server.release_paused_response();

    let status = child.wait().expect("login process should stop");
    let mut remaining_stdout = String::new();
    stdout.read_to_string(&mut remaining_stdout).unwrap();
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();

    assert!(status.success());
    let events = json_lines(format!("{activation_line}{remaining_stdout}").as_bytes());
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["event"], "activation_required");
    assert_eq!(events[1]["event"], "status");
    assert_eq!(events[1]["status"]["state"], "authenticated");
    assert!(!remaining_stdout.contains("cancelled"));
    assert!(stderr.is_empty());
    assert!(server.finish().is_empty());
}

#[test]
fn issued_token_is_retained_when_principal_confirmation_is_unreachable() {
    let server = ScriptedServer::respond(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "private-device-code",
                "user_code": "WAIT-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-retained-access-token",
                "token_type": "Bearer",
                "expires_in": 300
            }),
        ),
        http_response("503 Service Unavailable", None, &[]),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);

    let output = run_with_env(
        &[
            "auth",
            "login",
            "--force",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let events = json_lines(&output.stdout);
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[1],
        serde_json::json!({
            "schemaVersion": 1,
            "event": "failed",
            "deployment": server.api_url,
            "outcome": "unreachable",
            "phase": "principal_confirmation",
            "category": "server"
        })
    );
    assert!(output.stderr.is_empty());
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert_eq!(
        stored["credentials"][0]["accessToken"],
        "unique-retained-access-token"
    );
    server.finish();
}

#[test]
fn unauthenticated_principal_confirmation_emits_status_and_removes_token() {
    let server = ScriptedServer::respond(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "private-device-code",
                "user_code": "REJECT-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-rejected-new-token",
                "token_type": "Bearer",
                "expires_in": 300
            }),
        ),
        problem_http_response(
            "401 Unauthorized",
            serde_json::json!({
                "type": "https://api.scherzo.dev/problems/unauthorized",
                "title": "Unauthorized",
                "status": 401
            }),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);

    let output = run_with_env(
        &[
            "auth",
            "login",
            "--force",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let events = json_lines(&output.stdout);
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["event"], "status");
    assert_eq!(events[1]["status"]["state"], "unauthenticated");
    assert!(output.stderr.is_empty());
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert!(stored["credentials"].as_array().unwrap().is_empty());
    server.finish();
}

#[test]
fn signup_required_principal_confirmation_preserves_opaque_actions() {
    let actions = serde_json::json!([{
        "id": "future.action",
        "kind": "future-kind",
        "guide": "https://example.invalid/guide",
        "unknown": true
    }]);
    let server = ScriptedServer::respond(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "private-device-code",
                "user_code": "SIGNUP-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-signup-token",
                "token_type": "Bearer",
                "expires_in": 300
            }),
        ),
        problem_http_response(
            "403 Forbidden",
            serde_json::json!({
                "type": "https://api.scherzo.dev/problems/principal-not-provisioned",
                "title": "Principal not provisioned",
                "status": 403,
                "actions": actions
            }),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = login_environment(&server, credential_path_string);

    let output = run_with_env(
        &[
            "auth",
            "login",
            "--force",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let events = json_lines(&output.stdout);
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["event"], "status");
    assert_eq!(events[1]["status"]["state"], "signup_required");
    assert_eq!(events[1]["status"]["actions"], actions);
    assert!(events[1]["status"].get("nextAction").is_none());
    assert!(output.stderr.is_empty());
    server.finish();
}
