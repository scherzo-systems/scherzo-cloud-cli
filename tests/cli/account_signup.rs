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
