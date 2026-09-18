use super::*;

const CREDENTIAL_ID: &str = "crd_01k0z6r1w8f4jy2m7q9v3x5abc";
const API_KEY: &str = "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const CREATED_AT: &str = "2026-01-02T03:04:05Z";

fn service_key_file(directory: &tempfile::TempDir) -> String {
    let path = directory.path().join("service.key");
    fs::write(&path, format!("{API_KEY}\n")).expect("service API-key fixture should be written");
    fs::set_permissions(&path, Permissions::from_mode(0o600))
        .expect("service API-key fixture should be private");
    path.to_string_lossy().into_owned()
}

fn service_environment<'a>(api_url: &'a str, credentials: &'a str) -> [(&'static str, &'a str); 5] {
    deployment_environment(api_url, credentials)
}

#[test]
fn credential_management_requires_an_explicit_service_key() {
    let output = run(&["service-principal", "credential", "list"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--service-api-key-file <PATH|->"));
}

#[test]
fn service_principal_creation_writes_the_key_only_to_the_protected_destination() {
    let response_key = API_KEY;
    let response = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "principal": {
                "id": "prn_service",
                "type": "service",
                "state": "active",
                "displayName": "Build agent"
            },
            "initialCredential": {
                "id": CREDENTIAL_ID,
                "apiKey": response_key,
                "createdAt": CREATED_AT
            }
        }))
        .unwrap(),
    );
    let server = ScriptedServer::respond(vec![response]);
    let human_credentials = private_credential_directory();
    let credentials_path = human_credentials.path().join("credentials.json");
    write_credential_fixture(
        &credentials_path,
        &server.api_url,
        "human-access-token",
        "2999-01-01T00:00:00Z",
    );
    let credentials = credentials_path.to_string_lossy().into_owned();
    let key_directory = tempfile::tempdir().unwrap();
    let destination = key_directory.path().join("issued.key");
    let destination_text = destination.to_string_lossy().into_owned();

    let output = run_with_env(
        &[
            "service-principal",
            "create",
            "--display-name",
            "Build agent",
            "--api-key-file",
            &destination_text,
            "--json",
            "--allow-insecure-http",
        ],
        &service_environment(&server.api_url, &credentials),
    );

    assert!(output.status.success());
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "created");
    assert_eq!(document["principal"]["type"], "service");
    assert_eq!(document["initialCredential"]["id"], CREDENTIAL_ID);
    assert_eq!(document["apiKeyFile"], destination_text);
    assert!(!String::from_utf8_lossy(&output.stdout).contains(API_KEY));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(API_KEY));
    assert_eq!(
        fs::read_to_string(&destination).unwrap(),
        format!("{API_KEY}\n")
    );
    assert_eq!(
        fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let request = server.finish().pop().unwrap();
    assert_eq!(
        header_value(&request, "authorization"),
        "Bearer human-access-token"
    );
}

#[test]
fn issuance_to_stdout_emits_only_the_api_key_on_stdout() {
    let response = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "id": CREDENTIAL_ID,
            "apiKey": API_KEY,
            "createdAt": CREATED_AT
        }))
        .unwrap(),
    );
    let server = ScriptedServer::respond(vec![response]);
    let directory = private_credential_directory();
    let service_key = service_key_file(&directory);
    let credentials = directory.path().join("unused-human.json");
    let credentials = credentials.to_string_lossy().into_owned();

    let output = run_with_env(
        &[
            "service-principal",
            "credential",
            "issue",
            "--service-api-key-file",
            &service_key,
            "--api-key-file",
            "-",
            "--allow-insecure-http",
        ],
        &service_environment(&server.api_url, &credentials),
    );

    assert!(output.status.success());
    assert_eq!(output.stdout, format!("{API_KEY}\n").as_bytes());
    assert!(!String::from_utf8_lossy(&output.stderr).contains(API_KEY));
    server.finish();
}

#[test]
fn issuance_delivers_the_committed_key_when_interrupted_after_dispatch() {
    let response = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "id": CREDENTIAL_ID,
            "apiKey": API_KEY,
            "createdAt": CREATED_AT
        }))
        .unwrap(),
    );
    let mut server = ScriptedServer::respond_with_paused_last_response(vec![response]);
    let directory = private_credential_directory();
    let service_key = service_key_file(&directory);
    let destination = directory.path().join("replacement.key");
    let destination_text = destination.to_string_lossy().into_owned();
    let missing_human_credentials = directory.path().join("human.json");
    let environment =
        service_environment(&server.api_url, missing_human_credentials.to_str().unwrap());
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args([
            "service-principal",
            "credential",
            "issue",
            "--service-api-key-file",
            &service_key,
            "--api-key-file",
            &destination_text,
            "--json",
            "--allow-insecure-http",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let child = command.spawn().unwrap();

    let request = server.next_request();
    assert!(request.starts_with("POST /api/v1/me/credentials HTTP/1.1\r\n"));
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    server.release_paused_response();
    let output = child.wait_with_output().unwrap();

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "issued");
    assert_eq!(document["credential"]["id"], CREDENTIAL_ID);
    assert_eq!(document["apiKeyFile"], destination_text);
    assert!(!String::from_utf8_lossy(&output.stdout).contains(API_KEY));
    assert_eq!(
        fs::read_to_string(destination).unwrap(),
        format!("{API_KEY}\n")
    );
    assert!(server.finish().is_empty());
}

#[test]
fn issuance_replay_reports_unrecoverable_secret_and_removes_the_reserved_file() {
    let response = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "id": CREDENTIAL_ID,
            "createdAt": CREATED_AT
        }))
        .unwrap(),
    );
    let server = ScriptedServer::respond(vec![response]);
    let directory = private_credential_directory();
    let service_key = service_key_file(&directory);
    let missing_human_credentials = directory.path().join("human.json");
    let missing_human_credentials = missing_human_credentials.to_string_lossy().into_owned();
    let destination = directory.path().join("replacement.key");
    let destination_text = destination.to_string_lossy().into_owned();

    let output = run_with_env(
        &[
            "service-principal",
            "credential",
            "issue",
            "--service-api-key-file",
            &service_key,
            "--api-key-file",
            &destination_text,
            "--json",
            "--allow-insecure-http",
        ],
        &service_environment(&server.api_url, &missing_human_credentials),
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "api_key_unavailable",
            "credential": {
                "id": CREDENTIAL_ID,
                "createdAt": CREATED_AT
            }
        })
    );
    assert!(output.stderr.is_empty());
    assert!(!destination.exists());
    let request = server.finish().pop().unwrap();
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {API_KEY}")
    );
}

#[test]
fn rejected_service_key_uses_the_authentication_exit_and_structured_outcome() {
    let response = problem_http_response(
        "401 Unauthorized",
        serde_json::json!({
            "type": "https://api.scherzo.dev/problems/unauthorized",
            "title": "Unauthorized",
            "status": 401
        }),
    );
    let server = ScriptedServer::respond(vec![response]);
    let directory = private_credential_directory();
    let service_key = service_key_file(&directory);
    let credentials = directory.path().join("unused-human.json");
    let credentials = credentials.to_string_lossy().into_owned();

    let output = run_with_env(
        &[
            "service-principal",
            "credential",
            "list",
            "--service-api-key-file",
            &service_key,
            "--json",
            "--allow-insecure-http",
        ],
        &service_environment(&server.api_url, &credentials),
    );

    assert_eq!(output.status.code(), Some(3));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "unauthenticated");
    assert!(!String::from_utf8_lossy(&output.stdout).contains(API_KEY));
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn service_credential_list_is_non_secret_and_rejects_public_key_files_locally() {
    let response = json_http_response(
        "200 OK",
        serde_json::json!({
            "items": [{
                "id": CREDENTIAL_ID,
                "createdAt": CREATED_AT,
                "current": true
            }]
        }),
    );
    let server = ScriptedServer::respond(vec![response]);
    let directory = private_credential_directory();
    let service_key = service_key_file(&directory);
    let credentials = directory.path().join("human.json");
    let credentials = credentials.to_string_lossy().into_owned();

    let output = run_with_env(
        &[
            "service-principal",
            "credential",
            "list",
            "--service-api-key-file",
            &service_key,
            "--json",
            "--allow-insecure-http",
        ],
        &service_environment(&server.api_url, &credentials),
    );
    assert!(output.status.success());
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["items"][0]["current"], true);
    assert!(document["items"][0].get("apiKey").is_none());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(API_KEY));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(API_KEY));
    server.finish();

    fs::set_permissions(&service_key, Permissions::from_mode(0o644)).unwrap();
    let output = run_with_env(
        &[
            "service-principal",
            "credential",
            "list",
            "--service-api-key-file",
            &service_key,
            "--allow-insecure-http",
        ],
        &service_environment("http://127.0.0.1:1", &credentials),
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("file mode must be 0600"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(API_KEY));
}
