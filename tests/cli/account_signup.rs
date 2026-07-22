use super::*;

fn signup_environment<'a>(
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

fn prepared_signup(
    responses: Vec<Vec<u8>>,
    access_token: &str,
) -> (
    ScriptedServer,
    tempfile::TempDir,
    std::path::PathBuf,
    String,
) {
    let server = ScriptedServer::respond(responses);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        access_token,
        "2999-01-01T00:00:00Z",
    );
    let credential_path_string = credential_path.to_str().unwrap().to_owned();
    (
        server,
        credential_directory,
        credential_path,
        credential_path_string,
    )
}

fn created_response() -> Vec<u8> {
    json_http_response(
        "201 Created",
        serde_json::json!({
            "id": "prn_fixture",
            "type": "human",
            "state": "active",
            "displayName": "Ada Lovelace"
        }),
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

#[test]
fn account_without_a_subcommand_prints_help_without_loading_deployment() {
    let output = run_with_env(
        &["account"],
        &[("SCHERZO_CLOUD_API_URL", "partial-override-is-ignored")],
    );
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("Usage: scherzo-cloud account [COMMAND]"));
    assert!(stdout.contains("signup  Create your Scherzo Cloud account"));
    assert!(output.stderr.is_empty());
}

#[test]
fn human_signup_creates_and_reports_the_account() {
    let (server, _directory, _path, credential_path) = prepared_signup(
        vec![created_response()],
        "unique-human-signup-synthetic-token",
    );
    let environment = signup_environment(&server, &credential_path);

    let output = run_with_env(
        &["account", "signup", "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        output.stdout,
        concat!(
            "✓ Scherzo Cloud account created.\n",
            "\n",
            "  Account:    Ada Lovelace\n",
            "  Principal:  prn_fixture\n",
            "  Deployment: "
        )
        .as_bytes()
        .iter()
        .copied()
        .chain(server.api_url.bytes())
        .chain(std::iter::once(b'\n'))
        .collect::<Vec<_>>()
    );
    assert!(output.stderr.is_empty());
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with("POST /api/v1/signup HTTP/1.1\r\n"));
    assert!(request.contains("authorization: Bearer unique-human-signup-synthetic-token\r\n"));
    let idempotency_key = header_value(&request, "idempotency-key");
    assert_eq!(idempotency_key.len(), 64);
    assert!(idempotency_key.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert!(
        !String::from_utf8_lossy(&output.stdout).contains("unique-human-signup-synthetic-token")
    );
}

#[test]
fn structured_signup_reports_the_authenticated_principal() {
    let (server, _directory, _path, credential_path) = prepared_signup(
        vec![created_response()],
        "unique-json-signup-synthetic-token",
    );
    let environment = signup_environment(&server, &credential_path);

    let output = run_with_env(
        &["account", "signup", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "authenticated",
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
    server.finish();
}

#[test]
fn signup_retries_an_ambiguous_transport_failure_with_the_same_key() {
    let (server, _directory, _path, credential_path) = prepared_signup(
        vec![Vec::new(), created_response()],
        "unique-retry-signup-synthetic-token",
    );
    let environment = signup_environment(&server, &credential_path);

    let output = run_with_env(
        &["account", "signup", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
    for request in requests {
        assert!(request.contains("authorization: Bearer unique-retry-signup-synthetic-token\r\n"));
    }
}

#[test]
fn signup_rejects_http_before_transmitting_the_credential() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &api_url,
        "unique-untransmitted-signup-token",
        "2999-01-01T00:00:00Z",
    );
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&api_url, credential_path_string);

    let output = run_with_env(&["account", "signup", "--json"], &environment);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(
        output.stderr,
        b"Error: create Scherzo Cloud account: the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it\n"
    );
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
}

#[test]
fn signup_without_a_credential_does_not_contact_the_api() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    let credential_path_string = credential_path.to_str().unwrap();
    let environment = deployment_environment(&api_url, credential_path_string);

    let output = run_with_env(&["account", "signup", "--json"], &environment);

    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": api_url,
            "outcome": "unauthenticated"
        })
    );
    assert!(output.stderr.is_empty());
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
}

#[test]
fn signup_reports_policy_denial_without_claiming_a_principal() {
    let response = problem_http_response(
        "403 Forbidden",
        serde_json::json!({
            "type": "https://api.scherzo.dev/problems/signup-not-permitted",
            "title": "Signup not permitted",
            "status": 403,
            "detail": "The platform signup policy does not permit signup."
        }),
    );
    let (server, _directory, _path, credential_path) =
        prepared_signup(vec![response], "unique-policy-signup-synthetic-token");
    let environment = signup_environment(&server, &credential_path);

    let output = run_with_env(
        &["account", "signup", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        result,
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "signup_not_permitted"
        })
    );
    assert!(result.get("principal").is_none());
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn already_provisioned_signup_directs_the_human_to_status() {
    let response = problem_http_response(
        "409 Conflict",
        serde_json::json!({
            "type": "https://api.scherzo.dev/problems/principal-already-provisioned",
            "title": "Principal already provisioned",
            "status": 409
        }),
    );
    let (server, _directory, _path, credential_path) =
        prepared_signup(vec![response], "unique-existing-signup-synthetic-token");
    let environment = signup_environment(&server, &credential_path);

    let output = run_with_env(
        &["account", "signup", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        output.stdout,
        b"! This identity already has a Scherzo Cloud account.\n\nRun:\n  scherzo-cloud auth status\n"
    );
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn rejected_signup_credential_is_removed_without_leaking_it() {
    let response = problem_http_response(
        "401 Unauthorized",
        serde_json::json!({
            "type": "https://api.scherzo.dev/problems/unauthorized",
            "title": "Unauthorized",
            "status": 401
        }),
    );
    let token = "unique-rejected-signup-synthetic-token";
    let (server, _directory, credential_path, credential_path_string) =
        prepared_signup(vec![response], token);
    let environment = signup_environment(&server, &credential_path_string);

    let output = run_with_env(
        &["account", "signup", "--json", "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "unauthenticated"
    );
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(credential_path).unwrap()).unwrap();
    assert!(stored["credentials"].as_array().unwrap().is_empty());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!combined.contains(token));
    server.finish();
}
