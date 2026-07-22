use std::fs::{self, OpenOptions, Permissions};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::process::{Command, Output};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[path = "cli/auth_login.rs"]
mod auth_login;

const BUILD_VERSION: &str = match option_env!("SCHERZO_CLOUD_VERSION") {
    Some(version) => version,
    None => env!("CARGO_PKG_VERSION"),
};
const BUILD_IDENTITY: &str = match option_env!("SCHERZO_CLOUD_BUILD_IDENTITY") {
    Some(identity) => identity,
    None => "unknown",
};

const CREDENTIALS_FILE_VARIABLE: &str = "SCHERZO_CLOUD_CREDENTIALS_FILE";
const DEPLOYMENT_VARIABLES: [&str; 4] = [
    "SCHERZO_CLOUD_API_URL",
    "SCHERZO_CLOUD_AUTH_ISSUER",
    "SCHERZO_CLOUD_AUTH_AUDIENCE",
    "SCHERZO_CLOUD_AUTH_CLIENT_ID",
];

fn run(args: &[&str]) -> Output {
    run_with_env(args, &[])
}

fn run_with_env(args: &[&str], environment: &[(&str, &str)]) -> Output {
    let credential_directory =
        tempfile::tempdir().expect("temporary credential directory should be created");
    fs::set_permissions(credential_directory.path(), Permissions::from_mode(0o700))
        .expect("temporary credential directory should be private");
    let default_credential_path = credential_directory.path().join("credentials.json");
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command.args(args).env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    if !environment
        .iter()
        .any(|(name, _)| *name == CREDENTIALS_FILE_VARIABLE)
    {
        command.env(CREDENTIALS_FILE_VARIABLE, default_credential_path);
    }
    for (name, value) in environment {
        command.env(name, value);
    }

    command.output().expect("scherzo-cloud should run")
}

struct OneShotServer {
    api_url: String,
    request: Receiver<String>,
    thread: JoinHandle<()>,
}

impl OneShotServer {
    fn respond(status: &str, content_type: Option<&str>, body: &[u8]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("fixture listener should bind");
        let address = listener.local_addr().unwrap();
        let content_type = content_type
            .map(|value| format!("Content-Type: {value}\r\n"))
            .unwrap_or_default();
        let mut response = format!(
            "HTTP/1.1 {status}\r\nConnection: close\r\n{content_type}Content-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        let (sender, request) = mpsc::sync_channel(1);
        let thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("fixture request should arrive");
            let request = read_request(&mut stream);
            sender
                .send(String::from_utf8(request).expect("request should be text"))
                .unwrap();
            stream.write_all(&response).unwrap();
        });

        Self {
            api_url: format!("http://{address}/api"),
            request,
            thread,
        }
    }

    fn finish(self) -> String {
        let request = self
            .request
            .recv_timeout(Duration::from_secs(2))
            .expect("fixture should capture one request");
        self.thread.join().expect("fixture server should stop");
        request
    }
}

fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    let mut expected_length = None;
    loop {
        let read = stream
            .read(&mut buffer)
            .expect("request should be readable");
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        if let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
            let body_start = header_end + 4;
            let length = *expected_length.get_or_insert_with(|| {
                String::from_utf8_lossy(&request[..body_start])
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|value| value.parse::<usize>().ok())
                    })
                    .unwrap_or_default()
            });
            if request.len() >= body_start + length {
                break;
            }
        }
        assert!(request.len() < 128 * 1024);
    }
    request
}

fn private_credential_directory() -> tempfile::TempDir {
    let directory = tempfile::tempdir().expect("temporary credential directory should be created");
    fs::set_permissions(directory.path(), Permissions::from_mode(0o700))
        .expect("temporary credential directory should be private");
    directory
}

fn deployment_environment<'a>(
    api_url: &'a str,
    credential_path: &'a str,
) -> [(&'static str, &'a str); 5] {
    [
        (CREDENTIALS_FILE_VARIABLE, credential_path),
        ("SCHERZO_CLOUD_API_URL", api_url),
        ("SCHERZO_CLOUD_AUTH_ISSUER", "http://auth.fixture.example/"),
        ("SCHERZO_CLOUD_AUTH_AUDIENCE", "https://api.fixture.example"),
        ("SCHERZO_CLOUD_AUTH_CLIENT_ID", "fixture-public-client"),
    ]
}

fn write_credential_fixture(
    credential_path: &std::path::Path,
    api_url: &str,
    access_token: &str,
    expires_at: &str,
) {
    write_credential_fixture_for_deployment(
        credential_path,
        api_url,
        "http://auth.fixture.example/",
        access_token,
        expires_at,
    );
}

fn write_credential_fixture_for_deployment(
    credential_path: &std::path::Path,
    api_url: &str,
    issuer: &str,
    access_token: &str,
    expires_at: &str,
) {
    let mut credential_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(credential_path)
        .expect("credential fixture should open");
    serde_json::to_writer_pretty(
        &mut credential_file,
        &serde_json::json!({
            "schemaVersion": 1,
            "credentials": [{
                "deployment": {
                    "apiUrl": api_url,
                    "issuer": issuer,
                    "audience": "https://api.fixture.example",
                    "clientId": "fixture-public-client"
                },
                "accessToken": access_token,
                "expiresAt": expires_at
            }]
        }),
    )
    .unwrap();
    credential_file.write_all(b"\n").unwrap();
}

#[test]
fn no_arguments_print_composed_root_help() {
    let output = run(&[]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("Usage: scherzo-cloud [OPTIONS] [COMMAND]"));
    assert!(stdout.contains("auth     Manage your Scherzo Cloud sign-in"));
    assert!(stdout.contains("version  Print version information"));
    assert!(stdout.contains("runner   Run and manage the Scherzo Cloud runner"));
    assert!(stdout.contains("--allow-insecure-http"));
    assert!(output.stderr.is_empty());
}

#[test]
fn auth_without_a_subcommand_prints_composed_help_without_loading_deployment() {
    let output = run_with_env(
        &["auth"],
        &[("SCHERZO_CLOUD_API_URL", "partial-override-is-ignored")],
    );
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("Usage: scherzo-cloud auth [OPTIONS] [COMMAND]"));
    assert!(stdout.contains("login   Sign in to Scherzo Cloud"));
    assert!(stdout.contains("status  Show your Scherzo Cloud sign-in status"));
    assert!(stdout.contains("logout  Sign out of Scherzo Cloud on this device"));
    assert!(output.stderr.is_empty());
}

#[test]
fn runner_without_a_subcommand_prints_composed_help() {
    let output = run(&["runner"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("Usage: scherzo-cloud runner [OPTIONS] [COMMAND]"));
    assert!(stdout.contains("serve  Connect to Scherzo Cloud and serve run assignments"));
    assert!(output.stderr.is_empty());
}

#[test]
fn nested_help_flags_use_the_composed_command_tree() {
    let auth = run(&["auth", "--help"]);
    let login = run(&["auth", "login", "--help"]);
    let runner = run(&["runner", "--help"]);
    let serve = run(&["runner", "serve", "--help"]);

    assert!(auth.status.success());
    assert!(
        String::from_utf8_lossy(&auth.stdout)
            .contains("status  Show your Scherzo Cloud sign-in status")
    );
    assert!(auth.stderr.is_empty());

    assert!(login.status.success());
    let login_stdout = String::from_utf8_lossy(&login.stdout);
    assert!(login_stdout.contains("Usage: scherzo-cloud auth login [OPTIONS]"));
    assert!(login_stdout.contains("--json"));
    assert!(login_stdout.contains("--force"));
    assert!(login_stdout.contains("--allow-insecure-http"));
    assert!(login.stderr.is_empty());

    assert!(runner.status.success());
    assert!(
        String::from_utf8_lossy(&runner.stdout)
            .contains("serve  Connect to Scherzo Cloud and serve run assignments")
    );
    assert!(runner.stderr.is_empty());

    assert!(serve.status.success());
    assert!(String::from_utf8_lossy(&serve.stdout).contains("Usage: scherzo-cloud runner serve"));
    assert!(serve.stderr.is_empty());
}

#[test]
fn version_command_and_flag_report_the_build_version() {
    let expected = format!("scherzo-cloud {BUILD_VERSION}\n");

    for args in [["version"].as_slice(), ["--version"].as_slice()] {
        let output = run(args);

        assert!(output.status.success());
        assert_eq!(output.stdout, expected.as_bytes());
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn structured_version_reports_the_version_one_contract() {
    let output = run(&["version", "--json"]);
    let executable_path = std::fs::canonicalize(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .expect("scherzo-cloud executable path should resolve");
    let actual: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("version output should be JSON");
    let expected = serde_json::json!({
        "schemaVersion": 1,
        "command": "scherzo-cloud",
        "version": BUILD_VERSION,
        "executablePath": executable_path.to_string_lossy(),
        "buildIdentity": BUILD_IDENTITY,
    });

    assert!(output.status.success());
    assert_eq!(actual, expected);
    assert!(output.stdout.ends_with(b"\n"));
    assert!(output.stderr.is_empty());
}

#[test]
fn insecure_http_flag_is_global() {
    for args in [
        ["--allow-insecure-http", "auth", "status"],
        ["auth", "status", "--allow-insecure-http"],
    ] {
        let body = br#"{"type":"https://api.scherzo.dev/problems/unauthorized","title":"Unauthorized","status":401}"#;
        let server =
            OneShotServer::respond("401 Unauthorized", Some("application/problem+json"), body);
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        let credential_path_string = credential_path.to_str().unwrap();
        let environment = deployment_environment(&server.api_url, credential_path_string);

        let output = run_with_env(&args, &environment);

        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        server.finish();
    }
}

#[test]
fn partial_deployment_override_fails_before_auth_dispatch() {
    let output = run_with_env(
        &["auth", "status", "--json"],
        &[("SCHERZO_CLOUD_API_URL", "https://api.fixture.example")],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(
        output.stderr,
        b"Error: configure Scherzo Cloud sign-in: development deployment overrides must be set together; missing SCHERZO_CLOUD_AUTH_ISSUER, SCHERZO_CLOUD_AUTH_AUDIENCE, SCHERZO_CLOUD_AUTH_CLIENT_ID\n"
    );
}

#[test]
fn networked_auth_requires_http_opt_in_but_local_logout_does_not() {
    let body = br#"{"type":"https://api.scherzo.dev/problems/unauthorized","title":"Unauthorized","status":401}"#;
    let server = OneShotServer::respond("401 Unauthorized", Some("application/problem+json"), body);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&server.api_url, credential_path_string);
    let rejected = run_with_env(&["auth", "status"], &environment);
    let accepted = run_with_env(&["auth", "status", "--allow-insecure-http"], &environment);
    let local_logout = run_with_env(&["auth", "logout"], &environment);

    assert_eq!(rejected.status.code(), Some(1));
    assert!(rejected.stdout.is_empty());
    assert_eq!(
        rejected.stderr,
        b"Error: configure Scherzo Cloud sign-in: SCHERZO_CLOUD_API_URL uses insecure HTTP; rerun with --allow-insecure-http to permit it\n"
    );

    assert!(accepted.status.success());
    assert!(accepted.stderr.is_empty());

    assert!(local_logout.status.success());
    assert_eq!(
        local_logout.stdout,
        b"You're already signed out of Scherzo Cloud on this device.\n"
    );
    assert!(local_logout.stderr.is_empty());
    server.finish();
}

#[test]
fn structured_status_reports_an_authenticated_principal() {
    let body =
        br#"{"id":"prn_fixture","type":"human","state":"active","displayName":"Ada Lovelace"}"#;
    let server = OneShotServer::respond("200 OK", Some("application/json"), body);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        "unique-authenticated-synthetic-token",
        "2999-01-01T00:00:00Z",
    );
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&server.api_url, credential_path_string);

    let output = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "state": "authenticated",
            "deployment": server.api_url,
            "principal": {
                "id": "prn_fixture",
                "type": "human",
                "state": "active",
                "displayName": "Ada Lovelace"
            }
        })
    );
    assert!(output.stdout.ends_with(b"\n"));
    assert!(output.stderr.is_empty());
    assert!(
        !String::from_utf8_lossy(&output.stdout).contains("unique-authenticated-synthetic-token")
    );
    let request = server.finish();
    assert!(request.starts_with("GET /api/v1/me HTTP/1.1\r\n"));
    assert!(request.contains("authorization: Bearer unique-authenticated-synthetic-token\r\n"));
}

#[test]
fn structured_status_preserves_signup_actions_without_synthesizing_fields() {
    let actions = serde_json::json!([{
        "id": "future.action",
        "kind": "unknown-kind",
        "guide": "https://example.invalid/future",
        "additional": { "preserved": true }
    }]);
    let body = serde_json::to_vec(&serde_json::json!({
        "type": "https://api.scherzo.dev/problems/principal-not-provisioned",
        "title": "Principal not provisioned",
        "status": 403,
        "actions": actions
    }))
    .unwrap();
    let server = OneShotServer::respond("403 Forbidden", Some("application/problem+json"), &body);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        "unique-signup-synthetic-token",
        "2999-01-01T00:00:00Z",
    );
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&server.api_url, credential_path_string);

    let output = run_with_env(
        &["--allow-insecure-http", "auth", "status", "--json"],
        &environment,
    );

    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "schemaVersion": 1,
            "state": "signup_required",
            "deployment": server.api_url,
            "actions": actions
        })
    );
    assert!(value.get("nextAction").is_none());
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn structured_status_omits_absent_optional_fields() {
    let cases = [
        (
            "200 OK",
            "application/json",
            br#"{"id":"prn_fixture","type":"human","state":"active"}"#.as_slice(),
            "principal",
            "displayName",
        ),
        (
            "403 Forbidden",
            "application/problem+json",
            br#"{"type":"https://api.scherzo.dev/problems/principal-not-provisioned","title":"Principal not provisioned","status":403}"#.as_slice(),
            "",
            "actions",
        ),
    ];

    for (http_status, content_type, body, parent, absent_field) in cases {
        let server = OneShotServer::respond(http_status, Some(content_type), body);
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        let credential_path_string = credential_path.to_str().unwrap();
        let environment = deployment_environment(&server.api_url, credential_path_string);

        let output = run_with_env(
            &["auth", "status", "--json", "--allow-insecure-http"],
            &environment,
        );

        assert!(output.status.success());
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let container = if parent.is_empty() {
            &value
        } else {
            &value[parent]
        };
        assert!(container.get(absent_field).is_none());
        assert!(value.get("nextAction").is_none());
        server.finish();
    }
}

#[test]
fn status_without_a_credential_still_contacts_the_server_without_authorization() {
    let body = br#"{"type":"https://api.scherzo.dev/problems/unauthorized","title":"Unauthorized","status":401}"#;
    let server = OneShotServer::respond("401 Unauthorized", Some("application/problem+json"), body);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&server.api_url, credential_path_string);

    let output = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "state": "unauthenticated",
            "deployment": server.api_url
        })
    );
    assert!(output.stderr.is_empty());
    let request = server.finish();
    assert!(!request.contains("authorization:"));
}

#[test]
fn rejected_or_expired_status_credentials_are_removed() {
    for (access_token, expires_at, expect_authorization) in [
        (
            "unique-rejected-synthetic-token",
            "2999-01-01T00:00:00Z",
            true,
        ),
        (
            "unique-expired-synthetic-token",
            "2000-01-01T00:00:00Z",
            false,
        ),
    ] {
        let body = br#"{"type":"https://api.scherzo.dev/problems/unauthorized","title":"Unauthorized","status":401}"#;
        let server =
            OneShotServer::respond("401 Unauthorized", Some("application/problem+json"), body);
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        write_credential_fixture(&credential_path, &server.api_url, access_token, expires_at);
        let credential_path_string = credential_path.to_str().unwrap();
        let environment = deployment_environment(&server.api_url, credential_path_string);

        let output = run_with_env(
            &["auth", "status", "--json", "--allow-insecure-http"],
            &environment,
        );

        assert!(output.status.success());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["state"],
            "unauthenticated"
        );
        assert!(output.stderr.is_empty());
        let request = server.finish();
        assert_eq!(
            request.contains("authorization: Bearer"),
            expect_authorization
        );
        let stored: serde_json::Value =
            serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
        assert!(stored["credentials"].as_array().unwrap().is_empty());
    }
}

#[test]
fn unreachable_status_is_a_successful_recognized_result() {
    for (http_status, expected_category) in [
        ("429 Too Many Requests", "rate_limited"),
        ("503 Service Unavailable", "server"),
    ] {
        let server = OneShotServer::respond(http_status, None, &[]);
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        let credential_path_string = credential_path.to_str().unwrap();
        let environment = deployment_environment(&server.api_url, credential_path_string);

        let output = run_with_env(
            &["auth", "status", "--json", "--allow-insecure-http"],
            &environment,
        );

        assert!(output.status.success());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
            serde_json::json!({
                "schemaVersion": 1,
                "state": "unreachable",
                "deployment": server.api_url,
                "category": expected_category
            })
        );
        assert!(output.stderr.is_empty());
        server.finish();
    }
}

#[test]
fn connection_failure_is_a_successful_unreachable_status() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let api_url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&api_url, credential_path_string);

    let output = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "state": "unreachable",
            "deployment": api_url,
            "category": "connection"
        })
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn credential_failure_emits_no_status_or_network_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}", listener.local_addr().unwrap());
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let malformed = br#"{"accessToken":"unique-local-status-secret""#;
    let mut credential_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&credential_path)
        .unwrap();
    credential_file.write_all(malformed).unwrap();
    drop(credential_file);
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&api_url, credential_path_string);

    let output = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("credential file is malformed"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("unique-local-status-secret"));
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    assert_eq!(fs::read(&credential_path).unwrap(), malformed);
}

#[test]
fn protocol_failure_emits_no_status_and_does_not_leak_credentials() {
    let body = br#"{"unique":"unique-malformed-response-secret"}"#;
    let server = OneShotServer::respond("200 OK", Some("text/plain"), body);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        "unique-protocol-synthetic-token",
        "2999-01-01T00:00:00Z",
    );
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&server.api_url, credential_path_string);

    let output = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("violates the public API contract"));
    assert!(!stderr.contains("unique-protocol-synthetic-token"));
    assert!(!stderr.contains("unique-malformed-response-secret"));
    server.finish();
}

#[test]
fn malformed_unauthorized_response_deletes_the_rejected_credential() {
    let server = OneShotServer::respond(
        "401 Unauthorized",
        Some("application/problem+json"),
        b"not-json",
    );
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        "unique-malformed-401-token",
        "2999-01-01T00:00:00Z",
    );
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&server.api_url, credential_path_string);

    let output = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert!(stored["credentials"].as_array().unwrap().is_empty());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("unique-malformed-401-token"));
    server.finish();
}

#[test]
fn human_status_writes_the_recognized_result_to_stdout() {
    let body = br#"{"type":"https://api.scherzo.dev/problems/unauthorized","title":"Unauthorized","status":401}"#;
    let server = OneShotServer::respond("401 Unauthorized", Some("application/problem+json"), body);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&server.api_url, credential_path_string);

    let output = run_with_env(&["auth", "status", "--allow-insecure-http"], &environment);

    assert!(output.status.success());
    assert_eq!(output.stdout, b"! You're not signed in to Scherzo Cloud.\n");
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn logout_removes_only_the_selected_local_credential() {
    let credential_directory =
        tempfile::tempdir().expect("temporary credential directory should be created");
    fs::set_permissions(credential_directory.path(), Permissions::from_mode(0o700))
        .expect("temporary credential directory should be private");
    let credential_path = credential_directory.path().join("credentials.json");
    let mut credential_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&credential_path)
        .expect("credential fixture should open");
    let credential_fixture = serde_json::json!({
        "schemaVersion": 1,
        "credentials": [
            {
                "deployment": {
                    "apiUrl": "https://api.fixture.example",
                    "issuer": "https://auth.fixture.example/",
                    "audience": "https://api.fixture.example",
                    "clientId": "fixture-public-client"
                },
                "accessToken": "selected-synthetic-token",
                "expiresAt": "2026-07-22T12:00:00Z"
            },
            {
                "deployment": {
                    "apiUrl": "https://other-api.fixture.example",
                    "issuer": "https://other-auth.fixture.example/",
                    "audience": "https://other-api.fixture.example",
                    "clientId": "other-public-client"
                },
                "accessToken": "retained-synthetic-token",
                "expiresAt": "2026-07-22T12:00:00Z"
            }
        ]
    });
    serde_json::to_writer_pretty(&mut credential_file, &credential_fixture)
        .expect("credential fixture should serialize");
    credential_file
        .write_all(b"\n")
        .expect("credential fixture should end with a newline");
    drop(credential_file);
    let credential_path = credential_path
        .to_str()
        .expect("temporary credential path should be UTF-8");
    let environment = [
        (CREDENTIALS_FILE_VARIABLE, credential_path),
        ("SCHERZO_CLOUD_API_URL", "https://api.fixture.example"),
        ("SCHERZO_CLOUD_AUTH_ISSUER", "https://auth.fixture.example/"),
        ("SCHERZO_CLOUD_AUTH_AUDIENCE", "https://api.fixture.example"),
        ("SCHERZO_CLOUD_AUTH_CLIENT_ID", "fixture-public-client"),
    ];

    let first = run_with_env(&["auth", "logout", "--json"], &environment);
    let second = run_with_env(&["auth", "logout", "--json"], &environment);

    assert!(first.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&first.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": "https://api.fixture.example",
            "credentialRemoved": true
        })
    );
    assert!(first.stdout.ends_with(b"\n"));
    assert!(
        !first
            .stdout
            .windows("selected-synthetic-token".len())
            .any(|part| { part == b"selected-synthetic-token" })
    );
    assert!(first.stderr.is_empty());

    assert!(second.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&second.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": "https://api.fixture.example",
            "credentialRemoved": false
        })
    );
    assert!(second.stderr.is_empty());

    let stored: serde_json::Value = serde_json::from_slice(
        &fs::read(credential_path).expect("credential file should remain readable"),
    )
    .expect("credential file should remain valid JSON");
    let credentials = stored["credentials"].as_array().unwrap();
    assert_eq!(credentials.len(), 1);
    assert_eq!(
        credentials[0]["deployment"]["apiUrl"],
        "https://other-api.fixture.example"
    );
    assert_eq!(
        fs::metadata(credential_path).unwrap().permissions().mode() & 0o7777,
        0o600
    );
}

#[test]
fn logout_preserves_malformed_credentials_without_leaking_contents() {
    let credential_directory =
        tempfile::tempdir().expect("temporary credential directory should be created");
    fs::set_permissions(credential_directory.path(), Permissions::from_mode(0o700))
        .expect("temporary credential directory should be private");
    let credential_path = credential_directory.path().join("credentials.json");
    let malformed = br#"{"accessToken":"unique-malformed-synthetic-secret""#;
    let mut credential_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&credential_path)
        .expect("credential fixture should open");
    credential_file.write_all(malformed).unwrap();
    drop(credential_file);
    let credential_path_string = credential_path
        .to_str()
        .expect("temporary credential path should be UTF-8");

    let output = run_with_env(
        &["auth", "logout", "--json"],
        &[(CREDENTIALS_FILE_VARIABLE, credential_path_string)],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("credential file is malformed"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("unique-malformed-synthetic-secret"));
    assert_eq!(fs::read(credential_path).unwrap(), malformed);
}

#[test]
fn runner_serve_is_an_explicit_unimplemented_stub() {
    let output = run(&["runner", "serve"]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(
        output.stderr,
        b"Error: scherzo-cloud runner serve is not implemented yet\n"
    );
}

#[test]
fn unknown_commands_are_usage_errors() {
    let output = run(&["unknown"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unrecognized subcommand 'unknown'"));
}
