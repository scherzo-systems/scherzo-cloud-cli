#![allow(
    clippy::disallowed_macros,
    reason = "integration tests use Cargo-provided build and executable paths"
)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "integration tests use panic shortcuts to express assertion failures"
)]

use std::fs::{self, OpenOptions, Permissions};
use std::io;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
#[cfg(target_os = "linux")]
use std::process::Stdio;
use std::process::{Command, Output};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

#[path = "cli/account_signup.rs"]
mod account_signup;
mod api_test_support {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/api/test_support.rs"
    ));
}
use api_test_support::read_request;
#[path = "cli/artifact_validate.rs"]
mod artifact_validate;
#[path = "cli/auth_login.rs"]
mod auth_login;
#[path = "cli/claude_code_installation.rs"]
mod claude_code_installation;
#[path = "cli/codex_installation.rs"]
mod codex_installation;
#[path = "cli/organization.rs"]
mod organization;
#[path = "cli/pi_installation.rs"]
mod pi_installation;
#[path = "cli/project.rs"]
mod project;
#[path = "cli/recovery.rs"]
mod recovery;
#[path = "cli/run.rs"]
mod run_command;
#[path = "cli/runner_administration.rs"]
mod runner_administration;
#[path = "cli/runner_enrollment.rs"]
mod runner_enrollment;
#[path = "cli/runner_status.rs"]
mod runner_status;
#[path = "cli/workflow_reference.rs"]
mod workflow_reference;
#[path = "cli/workflow_retry.rs"]
mod workflow_retry;
#[path = "cli/workflow_run.rs"]
mod workflow_run;
#[path = "cli/workflow_schema.rs"]
mod workflow_schema;
#[path = "cli/workflow_status.rs"]
mod workflow_status;
#[path = "cli/workflow_validate.rs"]
mod workflow_validate;
#[path = "cli/workflow_view.rs"]
mod workflow_view;

const BUILD_VERSION: &str = match option_env!("SCHERZO_CLOUD_VERSION") {
    Some(version) => version,
    None => env!("CARGO_PKG_VERSION"),
};
const BUILD_IDENTITY: &str = match option_env!("SCHERZO_CLOUD_BUILD_IDENTITY") {
    Some(identity) => identity,
    None => "unknown",
};

struct TestPty {
    master: std::os::fd::OwnedFd,
    slave: std::os::fd::OwnedFd,
}

/// Opens a pseudoterminal pair for terminal-boundary tests.
///
/// The peer is opened by name instead of through Linux's `TIOCGPTPEER` ioctl, and close-on-exec
/// is applied to the controlling end after `posix_openpt`, so the helper builds on every target
/// the CLI supports rather than only on Linux.
fn open_test_pty(size: Option<&rustix::termios::Winsize>) -> io::Result<TestPty> {
    use rustix::fs::{Mode, OFlags, open};
    use rustix::io::{FdFlags, fcntl_setfd};
    use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};

    let master = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY)?;
    fcntl_setfd(&master, FdFlags::CLOEXEC)?;
    grantpt(&master)?;
    unlockpt(&master)?;
    let peer = ptsname(&master, Vec::new())?;
    let slave = open(
        peer,
        OFlags::RDWR | OFlags::NOCTTY | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    if let Some(size) = size {
        rustix::termios::tcsetwinsize(&slave, *size)?;
    }
    Ok(TestPty { master, slave })
}

fn install_interrupt_handler(handler: impl FnOnce() + Send + 'static) {
    let (ready, registered) = mpsc::sync_channel(0);
    let _ = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let mut interrupt =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
            ready.send(()).unwrap();
            let _ = interrupt.recv().await;
            handler();
        });
    });
    registered.recv().unwrap();
}

/// Polls an external boundary that has no explicit readiness signal.
///
/// The internal deadline remains below nextest's 60-second integration-test slow timeout so a
/// failed poll reports its last observation before the suite-level hang backstop takes ownership.
#[expect(
    clippy::disallowed_methods,
    reason = "shared bounded polling is limited to external boundaries without readiness signals"
)]
fn poll_until<T: std::fmt::Debug>(
    description: &str,
    mut observe: impl FnMut() -> T,
    mut condition: impl FnMut(&T) -> bool,
) -> T {
    const POLL_INTERVAL: Duration = Duration::from_millis(10);
    const POLL_TIMEOUT: Duration = Duration::from_secs(30);

    let deadline = std::time::Instant::now() + POLL_TIMEOUT;
    loop {
        let observation = observe();
        if condition(&observation) {
            return observation;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "{description} did not complete within {POLL_TIMEOUT:?}; last observed state: {observation:?}"
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
}

const ECHO_IDEMPOTENCY_KEY: &str = api_test_support::REQUEST_IDEMPOTENCY_KEY_ECHO;
const CREDENTIALS_FILE_VARIABLE: &str = "SCHERZO_CLOUD_CREDENTIALS_FILE";
const DEPLOYMENT_VARIABLES: [&str; 4] = [
    "SCHERZO_CLOUD_API_URL",
    "SCHERZO_CLOUD_AUTH_ISSUER",
    "SCHERZO_CLOUD_AUTH_AUDIENCE",
    "SCHERZO_CLOUD_AUTH_CLIENT_ID",
];
const RUNNER_TELEMETRY_VARIABLES: [&str; 9] = [
    "OTEL_SDK_DISABLED",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
    "OTEL_EXPORTER_OTLP_HEADERS",
    "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
    "OTEL_EXPORTER_OTLP_TIMEOUT",
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
    let default_path = tempfile::tempdir().expect("temporary empty PATH should be created");
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command.args(args).env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES
        .into_iter()
        .chain(RUNNER_TELEMETRY_VARIABLES)
    {
        command.env_remove(variable);
    }
    if !environment
        .iter()
        .any(|(name, _)| *name == CREDENTIALS_FILE_VARIABLE)
    {
        command.env(CREDENTIALS_FILE_VARIABLE, default_credential_path);
    }
    if !environment.iter().any(|(name, _)| *name == "PATH") {
        command.env("PATH", default_path.path());
    }
    for (name, value) in environment {
        command.env(name, value);
    }

    command.output().expect("scherzo-cloud should run")
}

fn assert_human_doctor_detail_matches_json(
    human: &Output,
    json: &serde_json::Value,
    human_label: &str,
    json_key: &str,
) {
    let expected = json["checks"][0]["details"][json_key]
        .as_str()
        .unwrap_or_else(|| panic!("JSON doctor report should contain {json_key}"));
    let expected_line = format!("{human_label}: {expected}");
    let stdout = String::from_utf8_lossy(&human.stdout);
    assert!(
        stdout.lines().any(|line| line.trim() == expected_line),
        "human doctor report should contain {expected_line:?}: {stdout}"
    );
}

#[test]
fn human_doctor_retains_observed_versions_after_capability_failures() {
    let pi_help = pi_installation::COMPLETE_HELP
        .replace("  --approve, -a Trust project files for this run\n", "");
    let pi = pi_installation::PiFixture::new("0.84.7", &pi_help, true);
    let claude_help =
        claude_code_installation::COMPLETE_HELP.replace("  --session-id <uuid> Use session\n", "");
    let claude = claude_code_installation::ClaudeCodeFixture::new(
        "2.1.235 (Claude Code)",
        &claude_help,
        true,
    );
    let path = std::env::join_paths([pi.path_directory(), claude.path_directory()]).unwrap();

    let output = run_with_env(
        &[
            "runner",
            "doctor",
            "--check",
            "execution.harness.pi-json-v1",
            "--check",
            "execution.harness.claude-code-stream-json-v1",
        ],
        &[("PATH", path.to_str().unwrap())],
    );

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("code: unsupported_pi_capability"));
    assert!(stdout.contains("code: unsupported_claude_code_capability"));
    let missing = ["observed version: 0.84.7", "observed version: 2.1.235"]
        .into_iter()
        .filter(|field| !stdout.lines().any(|line| line.trim() == *field))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "capability-failure report omitted observed versions {missing:?}: {stdout}"
    );
}

#[test]
fn human_doctor_retains_observed_versions_after_late_probe_failures() {
    let malformed_pi = pi_installation::PiFixture::new("0.84.7", "not Pi help\n", true);
    let malformed_claude = claude_code_installation::ClaudeCodeFixture::new(
        "2.1.235 (Claude Code)",
        "not Claude help\n",
        true,
    );
    let malformed_path = std::env::join_paths([
        malformed_pi.path_directory(),
        malformed_claude.path_directory(),
    ])
    .unwrap();
    let failed_pi = pi_installation::PiFixture::with_execution_and_capability_hook(
        "0.84.7",
        pi_installation::COMPLETE_HELP,
        true,
        "exit 97",
        "exit 88",
    );
    let failed_claude =
        claude_code_installation::ClaudeCodeFixture::with_execution_and_capability_hook(
            "2.1.235 (Claude Code)",
            claude_code_installation::COMPLETE_HELP,
            true,
            "exit 97",
            "exit 88",
        );
    let failed_path =
        std::env::join_paths([failed_pi.path_directory(), failed_claude.path_directory()]).unwrap();

    let mut missing = Vec::new();
    for (path, pi_code, claude_code) in [
        (
            &malformed_path,
            "malformed_pi_capabilities",
            "malformed_claude_code_capabilities",
        ),
        (
            &failed_path,
            "unexecutable_pi_installation",
            "unexecutable_claude_code_installation",
        ),
    ] {
        let output = run_with_env(
            &[
                "runner",
                "doctor",
                "--check",
                "execution.harness.pi-json-v1",
                "--check",
                "execution.harness.claude-code-stream-json-v1",
            ],
            &[("PATH", path.to_str().unwrap())],
        );

        assert_eq!(output.status.code(), Some(1));
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains(&format!("code: {pi_code}")));
        assert!(stdout.contains(&format!("code: {claude_code}")));
        for version in ["0.84.7", "2.1.235"] {
            if !stdout
                .lines()
                .any(|line| line.trim() == format!("observed version: {version}"))
            {
                missing.push(format!("{pi_code}/{claude_code}: {version}"));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "late probe failures omitted observed versions: {missing:?}"
    );
}

fn fake_git(body: &str) -> tempfile::TempDir {
    let directory = tempfile::tempdir().expect("temporary Git directory should be created");
    let git_path = directory.path().join("git");
    let mut git = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o755)
        .open(git_path)
        .expect("fake Git should be created");
    git.write_all(b"#!/bin/sh\n")
        .expect("fake Git should include its interpreter");
    git.write_all(body.as_bytes())
        .expect("fake Git should include its body");
    git.write_all(b"\n")
        .expect("fake Git should end with a newline");
    directory
}

struct OneShotServer {
    api_url: String,
    server: api_test_support::ScriptedHttpServer,
}

impl OneShotServer {
    fn respond(status: &str, content_type: Option<&str>, body: &[u8]) -> Self {
        let server = api_test_support::ScriptedHttpServer::respond(http_response(
            status,
            content_type,
            body,
        ));
        Self {
            api_url: server.api_url.trim_end_matches('/').to_owned(),
            server,
        }
    }

    fn finish(self) -> String {
        self.server.finish_one()
    }
}

struct ScriptedServer {
    api_url: String,
    issuer: String,
    server: api_test_support::ScriptedHttpServer,
}

impl ScriptedServer {
    fn respond(responses: Vec<Vec<u8>>) -> Self {
        Self::start(responses, None)
    }

    fn respond_with_paused_last_response(responses: Vec<Vec<u8>>) -> Self {
        let pause_index = responses.len().checked_sub(1);
        Self::start(responses, pause_index)
    }

    fn respond_with_paused_first_response(responses: Vec<Vec<u8>>) -> Self {
        Self::start(responses, Some(0))
    }

    fn start(responses: Vec<Vec<u8>>, pause_response: Option<usize>) -> Self {
        let server = api_test_support::ScriptedHttpServer::respond_in_sequence_with_pause(
            responses,
            pause_response,
        );
        let api_url = server.api_url.trim_end_matches('/').to_owned();
        let issuer = format!(
            "{}auth/",
            server
                .api_url
                .strip_suffix("api/")
                .expect("fixture API URL should end with api/")
        );
        Self {
            api_url,
            issuer,
            server,
        }
    }

    fn next_request(&mut self) -> String {
        self.server.next_request()
    }

    fn release_paused_response(&mut self) {
        self.server.release_response();
    }

    fn finish(self) -> Vec<String> {
        self.server.finish()
    }
}

fn http_response(status: &str, content_type: Option<&str>, body: &[u8]) -> Vec<u8> {
    http_response_with_headers(status, content_type, &[], body)
}

fn http_response_with_headers(
    status: &str,
    content_type: Option<&str>,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Vec<u8> {
    let mut response = format!("HTTP/1.1 {status}\r\nConnection: close\r\n");
    if let Some(content_type) = content_type {
        response.push_str(&format!("Content-Type: {content_type}\r\n"));
    }
    for (name, value) in headers {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    let mut response = response.into_bytes();
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

fn header_value<'a>(request: &'a str, name: &str) -> &'a str {
    let prefix = format!("{}: ", name.to_ascii_lowercase());
    request
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .expect("request should contain expected header")
        .trim_end_matches('\r')
}

fn request_form(request: &str) -> std::collections::HashMap<String, String> {
    let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
    url::form_urlencoded::parse(body.as_bytes())
        .into_owned()
        .collect()
}

fn private_credential_directory() -> tempfile::TempDir {
    let directory = tempfile::tempdir().expect("temporary credential directory should be created");
    fs::set_permissions(directory.path(), Permissions::from_mode(0o700))
        .expect("temporary credential directory should be private");
    directory
}

#[cfg(target_os = "linux")]
fn wait_for_refresh_lock_attempt(process_id: u32) {
    let descriptor_directory = format!("/proc/{process_id}/fd");
    poll_until(
        "second CLI process opening the deployment refresh lock",
        || {
            fs::read_dir(&descriptor_directory)
                .into_iter()
                .flatten()
                .filter_map(Result::ok)
                .filter_map(|entry| fs::read_link(entry.path()).ok())
                .any(|path| path.to_string_lossy().contains(".refresh."))
        },
        |attempted| *attempted,
    );
}

fn write_runner_config(directory: &tempfile::TempDir, connection_url: &str) -> String {
    let state_path = directory.path().join("runner-state.json");
    let config_path = directory.path().join("runner.json");
    let work_root = directory.path().join("work");
    fs::create_dir(&work_root).expect("create runner work root");
    fs::set_permissions(&work_root, Permissions::from_mode(0o700))
        .expect("protect runner work root");
    let mode = if connection_url.starts_with("ws://") {
        "development"
    } else {
        "production"
    };
    fs::write(
        &state_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schemaVersion": 1,
            "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abd",
            "connectionUrl": connection_url,
            "currentCredential": {
                "id": "rrc_01k0z6r1w8f4jy2m7q9v3x5abd",
                "secret": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "activationId": "rna_01k0z6r1w8f4jy2m7q9v3x5abd",
                "enrolledAt": "2026-08-06T12:00:00Z"
            },
            "updatedAt": "2026-08-06T12:00:00Z"
        }))
        .expect("encode runner state"),
    )
    .expect("write runner state");
    fs::set_permissions(&state_path, Permissions::from_mode(0o600)).expect("protect runner state");
    let config = serde_json::json!({
        "schemaVersion": 1,
        "deploymentMode": mode,
        "runnerStatePath": state_path,
        "controlSocketPath": directory.path().join("runner.sock"),
        "workRoot": work_root
    });
    fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config).expect("encode runner config"),
    )
    .expect("write runner config");
    config_path.to_string_lossy().into_owned()
}

fn deployment_environment<'a>(
    api_url: &'a str,
    credential_path: &'a str,
) -> [(&'static str, &'a str); 5] {
    deployment_environment_with_issuer(api_url, "http://auth.fixture.example/", credential_path)
}

fn deployment_environment_with_issuer<'a>(
    api_url: &'a str,
    issuer: &'a str,
    credential_path: &'a str,
) -> [(&'static str, &'a str); 5] {
    [
        (CREDENTIALS_FILE_VARIABLE, credential_path),
        ("SCHERZO_CLOUD_API_URL", api_url),
        ("SCHERZO_CLOUD_AUTH_ISSUER", issuer),
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
    write_credential_fixture_with_refresh_token(
        credential_path,
        api_url,
        issuer,
        access_token,
        expires_at,
        "unique-fixture-refresh-token",
    );
}

fn write_credential_fixture_with_refresh_token(
    credential_path: &std::path::Path,
    api_url: &str,
    issuer: &str,
    access_token: &str,
    expires_at: &str,
    refresh_token: &str,
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
                "expiresAt": expires_at,
                "refreshToken": refresh_token
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
    assert!(!stdout.is_empty());
    assert!(output.stderr.is_empty());
}

#[test]
fn artifact_without_a_subcommand_prints_composed_help() {
    let output = run(&["artifact"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(!stdout.is_empty());
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
    assert!(!stdout.is_empty());
    assert!(output.stderr.is_empty());
}

#[test]
fn runner_without_a_subcommand_prints_composed_help() {
    let output = run(&["runner"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(!stdout.is_empty());
    assert!(output.stderr.is_empty());
}

#[test]
fn workflow_without_a_subcommand_prints_composed_help() {
    let output = run(&["workflow"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(!stdout.is_empty());
    assert!(output.stderr.is_empty());
}

#[test]
fn version_command_and_flag_report_the_resolved_build_version() {
    let expected = format!("scherzo-cloud {BUILD_VERSION}\n");

    for args in [["version"].as_slice(), ["--version"].as_slice()] {
        let output = run(args);

        assert!(output.status.success());
        assert_eq!(output.stdout, expected.as_bytes());
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn structured_version_reports_the_resolved_build_version_contract() {
    let output = run(&["version", "--json"]);
    let expected_executable_path = std::fs::canonicalize(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .expect("scherzo-cloud executable path should resolve");
    let mut actual: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("version output should be JSON");
    let actual_executable_path = actual
        .get("executablePath")
        .and_then(serde_json::Value::as_str)
        .expect("version output should contain an executable path");
    assert_eq!(
        std::fs::canonicalize(actual_executable_path)
            .expect("reported executable path should resolve"),
        expected_executable_path
    );
    actual
        .as_object_mut()
        .expect("version output should be an object")
        .remove("executablePath");
    let expected = serde_json::json!({
        "schemaVersion": 1,
        "command": "scherzo-cloud",
        "version": BUILD_VERSION,
        "buildIdentity": BUILD_IDENTITY,
    });

    assert!(output.status.success());
    assert_eq!(actual, expected);
    assert!(output.stdout.ends_with(b"\n"));
    assert!(output.stderr.is_empty());
}

#[test]
fn insecure_http_flag_is_scoped_to_networked_leaf_commands() {
    let misplaced = run(&["--allow-insecure-http", "auth", "status"]);

    assert_eq!(misplaced.status.code(), Some(2));
    assert!(misplaced.stdout.is_empty());

    let body = br#"{"type":"https://api.scherzo.dev/problems/unauthorized","title":"Unauthorized","status":401}"#;
    let server = OneShotServer::respond("401 Unauthorized", Some("application/problem+json"), body);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&server.api_url, credential_path_string);

    let output = run_with_env(&["auth", "status", "--allow-insecure-http"], &environment);

    assert_eq!(output.status.code(), Some(3));
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn partial_deployment_override_fails_before_auth_dispatch() {
    let output = run_with_env(
        &["auth", "status", "--json"],
        &[("SCHERZO_CLOUD_API_URL", "https://api.fixture.example")],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
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

    assert_eq!(accepted.status.code(), Some(3));
    assert!(accepted.stderr.is_empty());

    assert!(local_logout.status.success());
    assert!(!local_logout.stdout.is_empty());
    assert!(local_logout.stderr.is_empty());
    server.finish();
}

#[test]
fn status_does_not_apply_transport_policy_to_the_unused_issuer() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let api_url = format!("https://{}/api", listener.local_addr().unwrap());
    drop(listener);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&api_url, credential_path_string);

    let output = run_with_env(&["auth", "status", "--json"], &environment);

    assert_eq!(output.status.code(), Some(4));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["state"],
        "unreachable"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn structured_status_preserves_authenticated_actions_without_interpreting_them() {
    let actions = serde_json::json!([
        {
            "id": "future.action",
            "kind": "unknown-kind",
            "guide": "http://127.0.0.1:1/guarded-guide",
            "command": "do-not-execute",
            "additional": { "preserved": true }
        },
        "unknown-action-shape"
    ]);
    let body = serde_json::to_vec(&serde_json::json!({
        "principal": {
            "id": "prn_fixture",
            "type": "human",
            "state": "active",
            "displayName": "Ada Lovelace"
        },
        "actions": actions
    }))
    .unwrap();
    let server = OneShotServer::respond("200 OK", Some("application/json"), &body);
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
            },
            "actions": actions
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
fn human_authenticated_status_reproduces_opaque_actions() {
    let actions = serde_json::json!([
        {
            "id": "future.action",
            "kind": "unknown-kind",
            "guide": "http://127.0.0.1:1/guarded-guide",
            "command": "do-not-execute"
        },
        "unknown-action-shape"
    ]);
    let body = serde_json::to_vec(&serde_json::json!({
        "principal": {
            "id": "prn_fixture",
            "type": "human",
            "state": "active",
            "displayName": "Ada Lovelace"
        },
        "actions": actions.clone()
    }))
    .unwrap();
    let server = OneShotServer::respond("200 OK", Some("application/json"), &body);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&server.api_url, credential_path_string);

    let output = run_with_env(&["auth", "status", "--allow-insecure-http"], &environment);

    assert!(output.status.success());
    let lines: Vec<_> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect();
    assert!(!lines[0].is_empty());
    let reproduced: Vec<serde_json::Value> = lines[1..]
        .iter()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(reproduced, actions.as_array().unwrap().to_owned());
    assert!(output.stderr.is_empty());
    server.finish();
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
        &["auth", "status", "--json", "--allow-insecure-http"],
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
            br#"{"principal":{"id":"prn_fixture","type":"human","state":"active"}}"#.as_slice(),
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

    assert_eq!(output.status.code(), Some(3));
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
fn expired_status_credential_is_refreshed_and_rotated_before_api_use() {
    let server = ScriptedServer::respond(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-rotated-status-access-token",
                "refresh_token": "unique-rotated-status-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "principal": {"id": "prn_refreshed", "type": "human", "state": "active"}
            }),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_with_refresh_token(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-expired-status-access-token",
        "2000-01-01T00:00:00Z",
        "unique-original-status-refresh-token",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["state"],
        "authenticated"
    );
    assert!(output.stderr.is_empty());
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert_eq!(
        request_form(&requests[0])
            .get("grant_type")
            .map(String::as_str),
        Some("refresh_token")
    );
    assert_eq!(
        request_form(&requests[0])
            .get("refresh_token")
            .map(String::as_str),
        Some("unique-original-status-refresh-token")
    );
    assert!(requests[1].contains("authorization: Bearer unique-rotated-status-access-token\r\n"));
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert_eq!(
        stored["credentials"][0]["accessToken"],
        "unique-rotated-status-access-token"
    );
    assert_eq!(
        stored["credentials"][0]["refreshToken"],
        "unique-rotated-status-refresh-token"
    );
}

#[test]
fn rejected_status_access_token_refreshes_once_and_retries_once() {
    let unauthorized = problem_http_response(
        "401 Unauthorized",
        serde_json::json!({
            "type": "https://api.scherzo.dev/problems/unauthorized",
            "title": "Unauthorized",
            "status": 401
        }),
    );
    let server = ScriptedServer::respond(vec![
        unauthorized,
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-recovered-status-access-token",
                "refresh_token": "unique-recovered-status-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "principal": {"id": "prn_recovered", "type": "human", "state": "active"}
            }),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_with_refresh_token(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-rejected-status-access-token",
        "2999-01-01T00:00:00Z",
        "unique-rejected-status-refresh-token",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].contains("authorization: Bearer unique-rejected-status-access-token\r\n"));
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert!(requests[2].contains("authorization: Bearer unique-recovered-status-access-token\r\n"));
}

#[test]
fn a_second_status_rejection_stops_without_an_authentication_loop() {
    let unauthorized = || {
        problem_http_response(
            "401 Unauthorized",
            serde_json::json!({
                "type": "https://api.scherzo.dev/problems/unauthorized",
                "title": "Unauthorized",
                "status": 401
            }),
        )
    };
    let server = ScriptedServer::respond(vec![
        unauthorized(),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-twice-rejected-status-access-token",
                "refresh_token": "unique-twice-rejected-status-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        unauthorized(),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-initially-rejected-status-access-token",
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(3));
    assert_eq!(server.finish().len(), 3);
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert!(stored["credentials"].as_array().unwrap().is_empty());
}

#[test]
fn terminal_refresh_rejection_removes_session_and_checks_anonymously() {
    let unauthorized = || {
        problem_http_response(
            "401 Unauthorized",
            serde_json::json!({
                "type": "https://api.scherzo.dev/problems/unauthorized",
                "title": "Unauthorized",
                "status": 401
            }),
        )
    };
    let server = ScriptedServer::respond(vec![
        unauthorized(),
        json_http_response(
            "400 Bad Request",
            serde_json::json!({"error": "invalid_grant"}),
        ),
        unauthorized(),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-terminal-status-access-token",
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(3));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["state"],
        "unauthenticated"
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(!requests[2].contains("authorization:"));
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert!(stored["credentials"].as_array().unwrap().is_empty());
}

#[test]
fn transient_refresh_failure_preserves_the_rotating_session() {
    for (status, category) in [
        ("429 Too Many Requests", "rate_limited"),
        ("503 Service Unavailable", "server"),
    ] {
        let response = http_response(status, None, &[]);
        let server = ScriptedServer::respond(vec![response]);
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        write_credential_fixture_with_refresh_token(
            &credential_path,
            &server.api_url,
            &server.issuer,
            "unique-transient-status-access-token",
            "2000-01-01T00:00:00Z",
            "unique-transient-status-refresh-token",
        );
        let before = fs::read(&credential_path).unwrap();
        let environment = deployment_environment_with_issuer(
            &server.api_url,
            &server.issuer,
            credential_path.to_str().unwrap(),
        );

        let output = run_with_env(
            &["auth", "status", "--json", "--allow-insecure-http"],
            &environment,
        );

        assert_eq!(output.status.code(), Some(4));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["category"],
            category
        );
        assert_eq!(fs::read(&credential_path).unwrap(), before);
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert!(
            requests
                .iter()
                .all(|request| request.starts_with("POST /auth/oauth/token HTTP/1.1\r\n"))
        );
    }
}

#[test]
fn connection_failure_during_refresh_preserves_the_rotating_session() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let api_url = "http://api.fixture.test/api";
    let issuer = format!("http://{address}/auth/");
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_with_refresh_token(
        &credential_path,
        api_url,
        &issuer,
        "unique-connection-refresh-access-token",
        "2000-01-01T00:00:00Z",
        "unique-connection-refresh-token",
    );
    let before = fs::read(&credential_path).unwrap();
    let environment =
        deployment_environment_with_issuer(api_url, &issuer, credential_path.to_str().unwrap());

    let output = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(4));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["category"],
        "connection"
    );
    assert_eq!(fs::read(&credential_path).unwrap(), before);
}

#[test]
fn one_ambiguous_refresh_response_is_retried_and_rotated_atomically() {
    let server = ScriptedServer::respond(vec![
        Vec::new(),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-ambiguous-recovered-access-token",
                "refresh_token": "unique-ambiguous-recovered-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "principal": {"id": "prn_ambiguous", "type": "human", "state": "active"}
            }),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_with_refresh_token(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-ambiguous-expired-access-token",
        "2000-01-01T00:00:00Z",
        "unique-ambiguous-original-refresh-token",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    for request in &requests[..2] {
        assert_eq!(
            request_form(request)
                .get("refresh_token")
                .map(String::as_str),
            Some("unique-ambiguous-original-refresh-token")
        );
    }
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert_eq!(
        stored["credentials"][0]["refreshToken"],
        "unique-ambiguous-recovered-refresh-token"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn concurrent_processes_exchange_one_rotating_refresh_token_once() {
    let mut server = ScriptedServer::respond_with_paused_first_response(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-concurrent-rotated-access-token",
                "refresh_token": "unique-concurrent-rotated-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "principal": {"id": "prn_concurrent_one", "type": "human", "state": "active"}
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "principal": {"id": "prn_concurrent_two", "type": "human", "state": "active"}
            }),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_with_refresh_token(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-concurrent-expired-access-token",
        "2000-01-01T00:00:00Z",
        "unique-concurrent-original-refresh-token",
    );
    let credential_path = credential_path.to_str().unwrap();
    let api_url = server.api_url.clone();
    let issuer = server.issuer.clone();
    let environment = deployment_environment_with_issuer(&api_url, &issuer, credential_path);
    let command = || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
        command
            .args(["auth", "status", "--json", "--allow-insecure-http"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
            .env_remove(CREDENTIALS_FILE_VARIABLE)
            .env("PATH", "");
        for variable in DEPLOYMENT_VARIABLES {
            command.env_remove(variable);
        }
        for &(name, value) in &environment {
            command.env(name, value);
        }
        command
    };

    let first = command()
        .spawn()
        .expect("first status process should start");
    let refresh_request = server.next_request();
    assert!(refresh_request.starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    let second = command()
        .spawn()
        .expect("second status process should start");
    #[cfg(target_os = "linux")]
    wait_for_refresh_lock_attempt(second.id());
    server.release_paused_response();

    let first = first
        .wait_with_output()
        .expect("first status process should finish");
    let second = second
        .wait_with_output()
        .expect("second status process should finish");
    assert!(first.status.success());
    assert!(second.status.success());
    assert!(first.stderr.is_empty());
    assert!(second.stderr.is_empty());
    let captured = [
        first.stdout.as_slice(),
        first.stderr.as_slice(),
        second.stdout.as_slice(),
        second.stderr.as_slice(),
    ]
    .concat();
    for secret in [
        "unique-concurrent-expired-access-token",
        "unique-concurrent-original-refresh-token",
        "unique-concurrent-rotated-access-token",
        "unique-concurrent-rotated-refresh-token",
    ] {
        assert!(
            !captured
                .windows(secret.len())
                .any(|part| part == secret.as_bytes())
        );
    }
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| {
        request.starts_with("GET /api/v1/me HTTP/1.1\r\n")
            && request.contains("authorization: Bearer unique-concurrent-rotated-access-token\r\n")
    }));
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(credential_path).unwrap()).unwrap();
    assert_eq!(
        stored["credentials"][0]["refreshToken"],
        "unique-concurrent-rotated-refresh-token"
    );
}

#[test]
fn unreachable_status_uses_unavailable_exit_code() {
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

        assert_eq!(output.status.code(), Some(4));
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
fn connection_failure_uses_unavailable_exit_code_and_status() {
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

    assert_eq!(output.status.code(), Some(4));
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
    assert!(!stderr.contains("unique-protocol-synthetic-token"));
    assert!(!stderr.contains("unique-malformed-response-secret"));
    server.finish();
}

#[test]
fn malformed_unauthorized_response_deletes_the_rejected_credential() {
    let malformed = http_response(
        "401 Unauthorized",
        Some("application/problem+json"),
        b"not-json",
    );
    let server = ScriptedServer::respond(vec![
        malformed.clone(),
        json_http_response(
            "400 Bad Request",
            serde_json::json!({"error": "invalid_grant"}),
        ),
        malformed,
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-malformed-401-token",
        "2999-01-01T00:00:00Z",
    );
    let credential_path_string = credential_path.to_str().unwrap();
    let environment =
        deployment_environment_with_issuer(&server.api_url, &server.issuer, credential_path_string);

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
    assert_eq!(server.finish().len(), 3);
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

    assert_eq!(output.status.code(), Some(3));
    assert!(!output.stdout.is_empty());
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn logout_revokes_and_removes_only_the_selected_renewable_session() {
    let server = ScriptedServer::respond(vec![http_response("200 OK", None, &[])]);
    let credential_directory = private_credential_directory();
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
                    "apiUrl": &server.api_url,
                    "issuer": &server.issuer,
                    "audience": "https://api.fixture.example",
                    "clientId": "fixture-public-client"
                },
                "accessToken": "selected-synthetic-token",
                "expiresAt": "2026-07-22T12:00:00Z",
                "refreshToken": "selected-synthetic-refresh-token"
            },
            {
                "deployment": {
                    "apiUrl": "https://other-api.fixture.example",
                    "issuer": "https://other-auth.fixture.example/",
                    "audience": "https://other-api.fixture.example",
                    "clientId": "other-public-client"
                },
                "accessToken": "retained-synthetic-token",
                "expiresAt": "2026-07-22T12:00:00Z",
                "refreshToken": "retained-synthetic-refresh-token"
            }
        ]
    });
    serde_json::to_writer_pretty(&mut credential_file, &credential_fixture).unwrap();
    credential_file.write_all(b"\n").unwrap();
    drop(credential_file);
    let credential_path_string = credential_path.to_str().unwrap();
    let environment =
        deployment_environment_with_issuer(&server.api_url, &server.issuer, credential_path_string);

    let first = run_with_env(
        &["auth", "logout", "--json", "--allow-insecure-http"],
        &environment,
    );
    let second = run_with_env(
        &["auth", "logout", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert!(first.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&first.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "credentialRemoved": true,
            "revocation": "confirmed"
        })
    );
    assert!(first.stderr.is_empty());
    assert!(second.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&second.stdout).unwrap()["revocation"],
        "not_applicable"
    );
    assert!(second.stderr.is_empty());

    let request = server.finish().pop().unwrap();
    assert!(request.starts_with("POST /auth/oauth/revoke HTTP/1.1\r\n"));
    let form = request_form(&request);
    assert_eq!(
        form.get("token").map(String::as_str),
        Some("selected-synthetic-refresh-token")
    );
    assert_eq!(
        form.get("client_id").map(String::as_str),
        Some("fixture-public-client")
    );
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    let credentials = stored["credentials"].as_array().unwrap();
    assert_eq!(credentials.len(), 1);
    assert_eq!(
        credentials[0]["deployment"]["apiUrl"],
        "https://other-api.fixture.example"
    );
}

#[test]
fn logout_removes_local_session_when_revocation_is_unconfirmed() {
    let server = ScriptedServer::respond(vec![Vec::new()]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "unique-unconfirmed-logout-access-token",
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &["auth", "logout", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["revocation"],
        "unconfirmed"
    );
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert!(stored["credentials"].as_array().unwrap().is_empty());
    server.finish();
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
    assert!(!String::from_utf8_lossy(&output.stderr).contains("unique-malformed-synthetic-secret"));
    assert_eq!(fs::read(credential_path).unwrap(), malformed);
}

#[test]
fn runner_doctor_lists_registered_checks_without_running_git() {
    let empty_path = tempfile::tempdir().expect("temporary empty PATH should be created");
    let path = empty_path
        .path()
        .to_str()
        .expect("temporary PATH should be UTF-8");

    let output = run_with_env(&["runner", "doctor", "--list-checks"], &[("PATH", path)]);

    assert!(output.status.success());
    assert_eq!(
        output.stdout,
        b"environment.command.git\nexecution.harness.pi-json-v1\nexecution.harness.claude-code-stream-json-v1\nexecution.harness.codex-app-server-v1\n"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn runner_doctor_reports_git_success_in_human_form() {
    let git_directory = fake_git(
        "printf '%s\\n' 'git version 2.42.0 unique-raw-command-output'\nprintf '%s\\n' 'unique-child-standard-error' >&2",
    );
    let path = git_directory
        .path()
        .to_str()
        .expect("temporary PATH should be UTF-8");

    let output = run_with_env(&["runner", "doctor"], &[("PATH", path)]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.is_empty());
    assert!(!stdout.contains("unique-raw-command-output"));
    assert!(output.stderr.is_empty());
}

#[test]
fn runner_doctor_reports_schema_one_json() {
    let git_directory = fake_git("printf '%s\\n' 'git version 2.42.0'");
    let path = git_directory
        .path()
        .to_str()
        .expect("temporary PATH should be UTF-8");

    let output = run_with_env(
        &[
            "runner",
            "doctor",
            "--check",
            "environment.command.git",
            "--json",
        ],
        &[("PATH", path)],
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("runner doctor report should be JSON");

    assert!(output.status.success());
    assert_eq!(
        report,
        serde_json::json!({
            "schemaVersion": 1,
            "command": "scherzo-cloud runner doctor",
            "checks": [{
                "id": "environment.command.git",
                "title": "Git",
                "status": "pass",
                "code": "ok",
                "message": "Git 2.42.0 is available (minimum 2.29.0).",
                "details": {
                    "minimumVersion": "2.29.0",
                    "version": "2.42.0"
                }
            }],
            "summary": {
                "passed": 1,
                "failed": 0
            }
        })
    );
    assert!(report.get("ready").is_none());
    assert!(output.stdout.ends_with(b"\n"));
    assert!(output.stderr.is_empty());
}

#[test]
fn runner_doctor_reports_missing_git() {
    let empty_path = tempfile::tempdir().expect("temporary empty PATH should be created");
    let path = empty_path
        .path()
        .to_str()
        .expect("temporary PATH should be UTF-8");

    let output = run_with_env(&["runner", "doctor"], &[("PATH", path)]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert_eq!(output.status.code(), Some(1));
    assert!(stdout.contains("command_not_found"));
    assert!(output.stderr.is_empty());
}

#[test]
fn runner_doctor_rejects_unknown_check_without_running_git() {
    let git_directory =
        fake_git("printf invoked > \"$MARKER\"\nprintf '%s\\n' 'git version 2.42.0'");
    let marker = git_directory.path().join("invoked");
    let path = git_directory
        .path()
        .to_str()
        .expect("temporary PATH should be UTF-8");
    let marker_path = marker.to_str().expect("marker path should be UTF-8");

    let output = run_with_env(
        &["runner", "doctor", "--check", "no.such.check"],
        &[("PATH", path), ("MARKER", marker_path)],
    );

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(!marker.exists());
}

#[test]
fn runner_doctor_does_not_load_human_configuration() {
    let git_directory = fake_git("printf '%s\\n' 'git version 2.42.0'");
    let path = git_directory
        .path()
        .to_str()
        .expect("temporary PATH should be UTF-8");

    let output = run_with_env(
        &["runner", "doctor"],
        &[
            ("PATH", path),
            ("SCHERZO_CLOUD_API_URL", "partial-override-is-ignored"),
            (CREDENTIALS_FILE_VARIABLE, "/dev/null/credentials.json"),
        ],
    );

    assert!(output.status.success());
    assert!(!output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}

#[test]
fn runner_doctor_list_options_conflict() {
    for args in [
        ["runner", "doctor", "--list-checks", "--json"].as_slice(),
        [
            "runner",
            "doctor",
            "--list-checks",
            "--check",
            "environment.command.git",
        ]
        .as_slice(),
    ] {
        let output = run(args);

        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn otlp_configuration_is_ignored_outside_valid_runner_serve() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind unused OTLP receiver");
    listener
        .set_nonblocking(true)
        .expect("make unused OTLP receiver nonblocking");
    let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
    let environment = [
        ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", endpoint.as_str()),
        (
            "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
            "authorization=HEADER-SENTINEL",
        ),
        ("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "50"),
    ];

    for args in [
        ["--help"].as_slice(),
        ["version"].as_slice(),
        ["runner", "--help"].as_slice(),
        ["runner", "doctor", "--list-checks"].as_slice(),
        ["runner", "serve", "--help"].as_slice(),
        ["runner", "serve"].as_slice(),
    ] {
        let output = run_with_env(args, &environment);
        assert!(!String::from_utf8_lossy(&output.stderr).contains("HEADER-SENTINEL"));
    }

    let runner_directory = private_credential_directory();
    let config_path = write_runner_config(
        &runner_directory,
        "https://not-a-websocket.example.test/v1/runner/connect",
    );
    let invalid_config = run_with_env(&["runner", "serve", "--config", &config_path], &environment);
    assert_eq!(invalid_config.status.code(), Some(1));
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
}

#[test]
fn privacy_and_malformed_configuration_make_no_otlp_request() {
    for (privacy, malformed_endpoint, expected_diagnostic) in [
        (Some("true"), false, None),
        (Some("not-a-boolean"), false, Some("privacy_invalid")),
        (None, true, Some("endpoint_invalid")),
    ] {
        let otlp_listener = TcpListener::bind("127.0.0.1:0").expect("bind unused OTLP receiver");
        otlp_listener
            .set_nonblocking(true)
            .expect("make unused OTLP receiver nonblocking");
        let endpoint = format!(
            "http://{}/v1/traces{}",
            otlp_listener.local_addr().unwrap(),
            if malformed_endpoint {
                "?secret=value"
            } else {
                ""
            }
        );
        // This telemetry fixture needs Runner Serve to terminate; credential
        // rejection now intentionally leaves the local control service alive.
        let gateway = OneShotServer::respond("400 Bad Request", None, &[]);
        let gateway_url = gateway.api_url.replacen("http://", "ws://", 1).replacen(
            "/api",
            "/v1/runner/connect",
            1,
        );
        // Runner control sockets require a short lexical Unix path; this
        // fixture exercises telemetry rather than hostile socket paths.
        let runner_directory = tempfile::tempdir_in("/tmp").expect("short runner directory");
        fs::set_permissions(runner_directory.path(), Permissions::from_mode(0o700))
            .expect("protect short runner directory");
        let config_path = write_runner_config(&runner_directory, &gateway_url);
        let mut environment = vec![
            ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", endpoint.as_str()),
            ("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "50"),
        ];
        if let Some(privacy) = privacy {
            environment.push(("OTEL_SDK_DISABLED", privacy));
        }

        let output = run_with_env(&["runner", "serve", "--config", &config_path], &environment);

        assert_eq!(output.status.code(), Some(1));
        gateway.finish();
        assert!(matches!(
            otlp_listener.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
        let stderr = String::from_utf8_lossy(&output.stderr);
        match expected_diagnostic {
            Some(classification) => assert!(stderr.contains(&format!(
                "\"diagnostic.classification\":\"{classification}\""
            ))),
            None => assert!(!stderr.contains("diagnostic.classification")),
        }
        assert!(!stderr.contains("secret=value"));
    }
}

#[test]
fn runner_serve_requires_one_configuration_and_rejects_removed_flags() {
    let missing = run(&["runner", "serve"]);
    assert_eq!(missing.status.code(), Some(2));
    assert!(missing.stdout.is_empty());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("--config <PATH>"));

    let directory = private_credential_directory();
    let config_path =
        write_runner_config(&directory, "wss://gateway.example.test/v1/runner/connect");
    for arguments in [
        vec![
            "--gateway-url",
            "wss://gateway.example.test/v1/runner/connect",
        ],
        vec!["--credential-file", "runner.credential"],
        vec!["--allow-insecure-http"],
        vec!["--workflow-source-root", "schemas"],
        vec!["--workflow-path", "workflow-v1.schema.json"],
        vec!["--work-root", "tests"],
    ] {
        let mut command = vec!["runner", "serve", "--config", config_path.as_str()];
        command.extend(arguments);
        let output = run(&command);
        assert_eq!(output.status.code(), Some(2));
    }
}

#[test]
fn runner_serve_redacts_invalid_protected_state() {
    let directory = private_credential_directory();
    let config_path =
        write_runner_config(&directory, "wss://gateway.example.test/v1/runner/connect");
    let state_path = directory.path().join("runner-state.json");
    let secret_marker = "RUNNER-CREDENTIAL-MUST-NOT-LEAK";
    fs::write(&state_path, secret_marker).expect("replace runner state with invalid content");
    fs::set_permissions(&state_path, Permissions::from_mode(0o600))
        .expect("protect invalid runner state");

    let output = run(&["runner", "serve", "--config", &config_path]);
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains(secret_marker));
}

#[test]
fn unknown_commands_are_usage_errors() {
    let output = run(&["unknown"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
}
