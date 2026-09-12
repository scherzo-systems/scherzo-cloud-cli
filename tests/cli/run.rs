use super::*;

use base64::Engine as _;
use ring::digest::{SHA256, digest};

#[cfg(target_os = "linux")]
use std::process::Stdio;

const TOKEN: &str = "unique-cloud-run-command-token-sentinel";
const REFRESHED_TOKEN: &str = "unique-cloud-run-refreshed-token-sentinel";
const ORGANIZATION: &str = "acme-research";
const ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
const PROJECT_ID: &str = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
const RUN_ID: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
const ATTEMPT_ID: &str = "atm_01k0z6r1w8f4jy2m7q9v3x5abc";
const EXECUTION_SPEC_ID: &str = "xsp_01k0z6r1w8f4jy2m7q9v3x5abc";
const REPOSITORY_CONNECTION_ID: &str = "rpc_01k0z6r1w8f4jy2m7q9v3x5abc";
const INPUT_SET_ID: &str = "ris_01k0z6r1w8f4jy2m7q9v3x5abc";
const WORKFLOW_PATH: &str = "workflows/build.yaml";

fn prepared_run(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, String) {
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

fn acceptance_body(run_id: &str, replayed: bool) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "runId": run_id,
        "replayed": replayed
    }))
    .unwrap()
}

fn acceptance_response(replayed: bool) -> Vec<u8> {
    acceptance_response_for(
        "202 Accepted",
        RUN_ID,
        replayed,
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
    )
}

fn acceptance_response_for(
    status: &str,
    run_id: &str,
    replayed: bool,
    headers: &[(&str, &str)],
) -> Vec<u8> {
    http_response_with_headers(
        status,
        Some("application/json"),
        headers,
        &acceptance_body(run_id, replayed),
    )
}

fn chunked_json_response(status: &str, body: &[u8]) -> Vec<u8> {
    let mut response = format!(
        "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    response.extend_from_slice(b"\r\n0\r\n\r\n");
    response
}

fn run_body() -> serde_json::Value {
    run_body_with_state("running")
}

fn run_body_with_state(state: &str) -> serde_json::Value {
    serde_json::json!({
        "id": RUN_ID,
        "organizationId": ORGANIZATION_ID,
        "projectId": PROJECT_ID,
        "displayName": "Release checks",
        "executionSpecId": EXECUTION_SPEC_ID,
        "state": state,
        "version": 7,
        "currentAttemptId": ATTEMPT_ID,
        "currentAttemptNumber": 2,
        "sourceBranch": "release/next",
        "workflowDefinitionSource": {
            "repositoryConnectionId": REPOSITORY_CONNECTION_ID,
            "objectFormat": "sha1",
            "commitOid": "0123456789abcdef0123456789abcdef01234567",
            "workflowPath": WORKFLOW_PATH,
            "workflowSourceClosureDigest": {
                "algorithm": "sha256",
                "value": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            }
        },
        "primaryWorkspaceSource": {
            "kind": "connected_repository",
            "providerKind": "github",
            "repositoryConnectionId": REPOSITORY_CONNECTION_ID,
            "objectFormat": "sha1",
            "commitOid": "0123456789abcdef0123456789abcdef01234567",
            "materializationContract": "git_full_clone_v1"
        },
        "inputs": {
            "inputSetId": INPUT_SET_ID,
            "inputCount": 2,
            "attachmentCount": 2,
            "aggregateBytes": 4096,
            "availability": "available"
        },
        "createdAt": "2026-08-10T12:00:00Z",
        "updatedAt": "2026-08-10T12:05:00Z"
    })
}

fn create_args(json: bool) -> Vec<&'static str> {
    let mut args = vec![
        "run",
        "create",
        ORGANIZATION,
        "--project-id",
        PROJECT_ID,
        "--workflow-path",
        WORKFLOW_PATH,
        "--source-branch",
        "release/next",
        "--display-name",
        "Release checks",
    ];
    if json {
        args.push("--json");
    }
    args.push("--allow-insecure-http");
    args
}

fn create_args_with_text_input<'a>(
    input_name: &'a str,
    input_file: &'a str,
    json: bool,
) -> Vec<&'a str> {
    let mut args: Vec<&'a str> = create_args(json);
    let insertion = args.len() - 1;
    args.insert(insertion, "--input-text-file");
    args.insert(insertion + 1, input_name);
    args.insert(insertion + 2, input_file);
    args
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let observed = digest(&SHA256, bytes);
    let mut sha256 = [0_u8; 32];
    sha256.copy_from_slice(observed.as_ref());
    sha256
}

fn hex_digest(bytes: &[u8]) -> String {
    sha256(bytes)
        .into_iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn text_manifest_digest(bytes: &[u8]) -> String {
    let canonical = format!(
        "{{\"inputs\":{{\"request\":{{\"kind\":\"text\",\"sha256\":\"{}\",\"sizeBytes\":{}}}}},\"schemaVersion\":1}}",
        hex_digest(bytes),
        bytes.len()
    );
    hex_digest(canonical.as_bytes())
}

fn text_input_set_body(
    bytes: &[u8],
    state: &str,
    uploaded: bool,
    replayed: bool,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "id": INPUT_SET_ID,
        "organizationId": ORGANIZATION_ID,
        "projectId": PROJECT_ID,
        "boundsProfile": 1,
        "manifest": {
            "schemaVersion": 1,
            "inputs": {
                "request": {
                    "kind": "text",
                    "sizeBytes": bytes.len(),
                    "sha256": hex_digest(bytes)
                }
            }
        },
        "manifestDigest": {
            "algorithm": "sha256",
            "value": text_manifest_digest(bytes)
        },
        "inputCount": 1,
        "attachmentCount": 0,
        "aggregateSizeBytes": bytes.len(),
        "state": state,
        "createdAt": "2026-08-24T01:00:00Z",
        "openDeadlineAt": "2026-08-25T01:00:00Z",
        "members": [{"memberId": "inputs/request", "uploadConfirmed": uploaded}],
        "replayed": replayed
    });
    if state == "sealed" {
        body["sealedAt"] = serde_json::json!("2026-08-24T01:02:00Z");
        body["sealedDeadlineAt"] = serde_json::json!("2026-08-25T01:02:00Z");
    }
    body
}

fn create_input_set_response(bytes: &[u8], replayed: bool) -> Vec<u8> {
    http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/run-input-sets/ris_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&text_input_set_body(bytes, "open", false, replayed)).unwrap(),
    )
}

fn upload_capability_response(bytes: &[u8], url: &str) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "capabilityExpiresAt": "2026-08-24T01:05:00Z",
            "members": [{
                "memberId": "inputs/request",
                "url": url,
                "requiredHeaders": {
                    "contentLength": bytes.len().to_string(),
                    "contentType": "text/plain; charset=utf-8",
                    "ifNoneMatch": "*",
                    "xAmzChecksumSha256": base64::engine::general_purpose::STANDARD.encode(sha256(bytes))
                }
            }]
        }))
        .unwrap(),
    )
}

fn seal_input_set_response(bytes: &[u8], replayed: bool) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&text_input_set_body(bytes, "sealed", true, replayed)).unwrap(),
    )
}

fn request_body(request: &str) -> serde_json::Value {
    serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
}

fn assert_no_secret_output(output: &Output, secrets: &[&str]) {
    let mut bytes = output.stdout.clone();
    bytes.extend_from_slice(&output.stderr);
    let text = String::from_utf8_lossy(&bytes);
    for secret in secrets {
        assert!(!text.contains(secret), "output exposed secret {secret:?}");
    }
}

fn assert_invalid_response(output: &Output) {
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "invalid_response"
    );
}

fn run_show_with_response(response: Vec<u8>) -> (Output, ScriptedServer) {
    let (server, _directory, credential_path) = prepared_run(vec![response]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    (output, server)
}

#[test]
fn run_create_sends_inputless_request_and_reports_plain_and_json_receipts() {
    for json in [false, true] {
        let (server, _directory, credential_path) = prepared_run(vec![acceptance_response(false)]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(&create_args(json), &environment);

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        if json {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
                serde_json::json!({
                    "schemaVersion": 1,
                    "deployment": server.api_url,
                    "outcome": "accepted",
                    "organizationRef": ORGANIZATION,
                    "runId": RUN_ID,
                    "replayed": false
                })
            );
        } else {
            let stdout = String::from_utf8(output.stdout).unwrap();
            for field in [
                format!("run: {RUN_ID}"),
                "replayed: no".to_owned(),
                format!("organization: {ORGANIZATION}"),
                format!("deployment: {}", server.api_url),
            ] {
                assert!(stdout.lines().any(|line| line == field));
            }
        }
        let request = server.finish().pop().unwrap();
        assert!(request.starts_with(&format!(
            "POST /api/v1/organizations/{ORGANIZATION}/runs HTTP/1.1\r\n"
        )));
        assert_eq!(
            request_body(&request),
            serde_json::json!({
                "projectId": PROJECT_ID,
                "workflowPath": WORKFLOW_PATH,
                "sourceBranch": "release/next",
                "displayName": "Release checks"
            })
        );
        assert_eq!(
            header_value(&request, "authorization"),
            format!("Bearer {TOKEN}")
        );
        let key = header_value(&request, "idempotency-key");
        assert_eq!(key.len(), 64);
        assert!(key.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}

#[test]
fn run_create_stages_present_named_text_input_files_before_binding_the_sealed_set() {
    for input_bytes in [b"".as_slice(), b"Write a concise limerick.\n".as_slice()] {
        let input_directory = tempfile::tempdir().unwrap();
        let input_path = input_directory.path().join("request.txt");
        fs::write(&input_path, input_bytes).unwrap();
        let input_path = input_path.to_str().unwrap();
        let storage = OneShotServer::respond("204 No Content", None, b"");
        let signed_url = format!(
            "{}/private/request?signature=unique-input-capability-sentinel",
            storage.api_url
        );
        let (server, _credential_directory, credential_path) = prepared_run(vec![
            create_input_set_response(input_bytes, false),
            upload_capability_response(input_bytes, &signed_url),
            seal_input_set_response(input_bytes, false),
            acceptance_response(false),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &create_args_with_text_input("request", input_path, true),
            &environment,
        );

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["outcome"], "accepted");
        assert_eq!(receipt["runId"], RUN_ID);
        let mut secrets = vec![TOKEN, "unique-input-capability-sentinel"];
        if !input_bytes.is_empty() {
            secrets.push(std::str::from_utf8(input_bytes).unwrap());
        }
        assert_no_secret_output(&output, &secrets);

        let requests = server.finish();
        assert_eq!(requests.len(), 4);
        assert!(requests[0].starts_with(&format!(
            "POST /api/v1/organizations/{ORGANIZATION}/run-input-sets HTTP/1.1\r\n"
        )));
        assert_eq!(
            request_body(&requests[0]),
            serde_json::json!({
                "projectId": PROJECT_ID,
                "schemaVersion": 1,
                "inputs": {
                    "request": {
                        "kind": "text",
                        "sizeBytes": input_bytes.len(),
                        "sha256": hex_digest(input_bytes)
                    }
                }
            })
        );
        assert!(requests[1].starts_with(&format!(
            "POST /api/v1/organizations/{ORGANIZATION}/run-input-sets/{INPUT_SET_ID}/upload-capabilities HTTP/1.1\r\n"
        )));
        assert_eq!(
            request_body(&requests[1]),
            serde_json::json!({"members": ["inputs/request"]})
        );
        assert!(requests[2].starts_with(&format!(
            "POST /api/v1/organizations/{ORGANIZATION}/run-input-sets/{INPUT_SET_ID}/seal HTTP/1.1\r\n"
        )));
        assert!(requests[3].starts_with(&format!(
            "POST /api/v1/organizations/{ORGANIZATION}/runs HTTP/1.1\r\n"
        )));
        assert_eq!(
            request_body(&requests[3]),
            serde_json::json!({
                "projectId": PROJECT_ID,
                "workflowPath": WORKFLOW_PATH,
                "sourceBranch": "release/next",
                "displayName": "Release checks",
                "inputSetId": INPUT_SET_ID
            })
        );
        for request in &requests {
            assert_eq!(
                header_value(request, "authorization"),
                format!("Bearer {TOKEN}")
            );
        }
        let mutation_keys = [&requests[0], &requests[2], &requests[3]]
            .map(|request| header_value(request, "idempotency-key"));
        assert!(mutation_keys.iter().all(|key| key.len() == 64));
        assert_ne!(mutation_keys[0], mutation_keys[1]);
        assert_ne!(mutation_keys[1], mutation_keys[2]);
        assert!(!requests[1].contains("idempotency-key:"));

        let upload = storage.finish();
        assert!(upload.starts_with("PUT /api/private/request?signature="));
        assert_eq!(
            upload.split_once("\r\n\r\n").unwrap().1.as_bytes(),
            input_bytes
        );
        assert_eq!(
            header_value(&upload, "content-length"),
            input_bytes.len().to_string()
        );
        assert_eq!(
            header_value(&upload, "content-type"),
            "text/plain; charset=utf-8"
        );
        assert_eq!(header_value(&upload, "if-none-match"), "*");
        assert_eq!(
            header_value(&upload, "x-amz-checksum-sha256"),
            base64::engine::general_purpose::STANDARD.encode(sha256(input_bytes))
        );
        let mut header_names = upload
            .split_once("\r\n\r\n")
            .unwrap()
            .0
            .lines()
            .skip(1)
            .map(|line| line.split_once(':').unwrap().0.to_ascii_lowercase())
            .collect::<Vec<_>>();
        header_names.sort();
        assert_eq!(
            header_names,
            [
                "content-length",
                "content-type",
                "host",
                "if-none-match",
                "x-amz-checksum-sha256",
            ]
            .map(str::to_owned)
        );
    }
}

#[test]
fn invalid_and_oversized_named_text_input_files_stop_before_cloud_access() {
    let input_directory = tempfile::tempdir().unwrap();
    let invalid_path = input_directory.path().join("invalid.txt");
    fs::write(&invalid_path, b"secret-valid-prefix\xff").unwrap();
    let oversized_path = input_directory.path().join("oversized.txt");
    fs::write(&oversized_path, vec![b'x'; 1024 * 1024 + 1]).unwrap();
    let valid_path = input_directory.path().join("valid.txt");
    fs::write(&valid_path, b"private valid input sentinel").unwrap();

    for (name, path) in [
        ("request", invalid_path.as_path()),
        ("request", oversized_path.as_path()),
        ("Request", valid_path.as_path()),
        ("request", input_directory.path()),
    ] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        write_credential_fixture(&credential_path, &api_url, TOKEN, "2999-01-01T00:00:00Z");
        let environment = deployment_environment(&api_url, credential_path.to_str().unwrap());

        let output = run_with_env(
            &create_args_with_text_input(name, path.to_str().unwrap(), true),
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        assert!(output.stderr.is_empty());
        let failure: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(failure["outcome"], "invalid_input");
        assert_no_secret_output(
            &output,
            &[TOKEN, "secret-valid-prefix", "private valid input sentinel"],
        );
        assert!(matches!(
            listener.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
    }
}

#[test]
fn exactly_one_mib_text_input_reaches_input_set_creation() {
    let input_directory = tempfile::tempdir().unwrap();
    let input_path = input_directory.path().join("boundary.txt");
    let input_bytes = vec![b'z'; 1024 * 1024];
    fs::write(&input_path, &input_bytes).unwrap();
    let (server, _credential_directory, credential_path) =
        prepared_run(vec![problem_http_response(
            "403 Forbidden",
            serde_json::json!({
                "type": "https://api.scherzo.dev/problems/forbidden",
                "title": "Forbidden",
                "status": 403
            }),
        )]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &create_args_with_text_input("request", input_path.to_str().unwrap(), true),
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "forbidden"
    );
    let request = server.finish().pop().unwrap();
    assert!(request.contains("/run-input-sets HTTP/1.1"));
    assert_eq!(
        request_body(&request)["inputs"]["request"]["sizeBytes"],
        1024 * 1024
    );
    assert_eq!(
        request_body(&request)["inputs"]["request"]["sha256"],
        hex_digest(&input_bytes)
    );
}

#[test]
fn credential_rejection_reuses_the_create_key_and_reports_the_server_replay() {
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
                "access_token": REFRESHED_TOKEN,
                "refresh_token": "unique-cloud-run-refreshed-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        acceptance_response(true),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(&create_args(true), &environment);

    assert!(output.status.success());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["runId"], RUN_ID);
    assert_eq!(receipt["replayed"], true);
    assert_no_secret_output(&output, &[TOKEN, REFRESHED_TOKEN]);
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].starts_with("POST /api/v1/organizations/"));
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert!(requests[2].starts_with("POST /api/v1/organizations/"));
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[2], "idempotency-key")
    );
    assert_eq!(request_body(&requests[0]), request_body(&requests[2]));
    assert_eq!(
        header_value(&requests[2], "authorization"),
        format!("Bearer {REFRESHED_TOKEN}")
    );
}

#[test]
fn malformed_unauthorized_response_still_refreshes_the_human_session() {
    let server = ScriptedServer::respond(vec![
        http_response_with_headers(
            "401 Unauthorized",
            Some("application/problem+json"),
            &[],
            b"not-json",
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": REFRESHED_TOKEN,
                "refresh_token": "unique-cloud-run-refreshed-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        acceptance_response(true),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(&create_args(true), &environment);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["runId"], RUN_ID);
    assert_no_secret_output(&output, &[TOKEN, REFRESHED_TOKEN]);
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
}

#[test]
fn interrupted_create_without_an_idempotency_echo_is_invalid_response() {
    let truncated_response = format!(
        "HTTP/1.1 202 Accepted\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 4096\r\nLocation: /v1/organizations/{ORGANIZATION}/runs/{RUN_ID}\r\n\r\n{{\"runId\":\"{RUN_ID}\""
    )
    .into_bytes();
    let (server, _directory, credential_path) = prepared_run(vec![truncated_response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(&create_args(true), &environment);

    assert_invalid_response(&output);
    assert_no_secret_output(&output, &[TOKEN]);
    server.finish();
}

#[test]
fn ambiguous_transport_retry_reuses_the_create_key_and_request() {
    let (server, _directory, credential_path) =
        prepared_run(vec![Vec::new(), acceptance_response(true)]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(&create_args(true), &environment);

    assert!(output.status.success());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["runId"], RUN_ID);
    assert_eq!(receipt["replayed"], true);
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
    assert_eq!(request_body(&requests[0]), request_body(&requests[1]));
}

#[test]
fn text_input_sequence_refreshes_fresh_authority_and_recovers_ambiguous_mutations() {
    let input_bytes = b"Explain why 2 + 3 = 5.\n";
    let input_directory = tempfile::tempdir().unwrap();
    let input_path = input_directory.path().join("request.txt");
    fs::write(&input_path, input_bytes).unwrap();
    let storage = OneShotServer::respond("", None, b"");
    let signed_url = format!(
        "{}/private/request?signature=unique-refreshed-capability-sentinel",
        storage.api_url
    );
    let server = ScriptedServer::respond(vec![
        create_input_set_response(input_bytes, false),
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
                "access_token": REFRESHED_TOKEN,
                "refresh_token": "unique-input-refreshed-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        upload_capability_response(input_bytes, &signed_url),
        Vec::new(),
        seal_input_set_response(input_bytes, true),
        Vec::new(),
        acceptance_response(true),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &create_args_with_text_input("request", input_path.to_str().unwrap(), true),
        &environment,
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["outcome"], "accepted");
    assert_eq!(receipt["replayed"], true);
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            REFRESHED_TOKEN,
            "unique-refreshed-capability-sentinel",
            std::str::from_utf8(input_bytes).unwrap(),
        ],
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 8);
    assert!(requests[1].contains("/upload-capabilities HTTP/1.1"));
    assert!(requests[2].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert!(requests[3].contains("/upload-capabilities HTTP/1.1"));
    assert!(!requests[1].contains("idempotency-key:"));
    assert!(!requests[3].contains("idempotency-key:"));
    assert_eq!(
        header_value(&requests[1], "authorization"),
        format!("Bearer {TOKEN}")
    );
    assert_eq!(
        header_value(&requests[3], "authorization"),
        format!("Bearer {REFRESHED_TOKEN}")
    );
    assert_eq!(
        header_value(&requests[4], "idempotency-key"),
        header_value(&requests[5], "idempotency-key")
    );
    assert_eq!(
        requests[4].split_once("\r\n\r\n").unwrap().1,
        requests[5].split_once("\r\n\r\n").unwrap().1
    );
    assert_eq!(
        header_value(&requests[6], "idempotency-key"),
        header_value(&requests[7], "idempotency-key")
    );
    assert_eq!(request_body(&requests[6]), request_body(&requests[7]));
    let upload = storage.finish();
    assert!(upload.starts_with("PUT "));
    assert!(!upload.contains("authorization:"));
    assert_eq!(
        upload.split_once("\r\n\r\n").unwrap().1.as_bytes(),
        input_bytes
    );
}

#[test]
fn definite_upload_rejection_with_truncated_body_never_reaches_seal_or_run() {
    let input_bytes = b"definite upload rejection sentinel\n";
    let input_directory = tempfile::tempdir().unwrap();
    let input_path = input_directory.path().join("request.txt");
    fs::write(&input_path, input_bytes).unwrap();
    let storage = api_test_support::ScriptedHttpServer::respond(
        b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 100\r\n\r\nshort".to_vec(),
    );
    let signed_url = format!("{}private/request?signature=private", storage.api_url);
    let (server, _credential_directory, credential_path) = prepared_run(vec![
        create_input_set_response(input_bytes, false),
        upload_capability_response(input_bytes, &signed_url),
        seal_input_set_response(input_bytes, false),
        acceptance_response(false),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &create_args_with_text_input("request", input_path.to_str().unwrap(), true),
        &environment,
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    // A corrected command consumes only the first two scripted responses. Drain the fixture's
    // deliberately unreachable responses after the command so the same test terminates on both
    // sides of the assertion without making those requests part of command behavior.
    if !output.status.success() {
        let authority = server
            .api_url
            .strip_prefix("http://")
            .unwrap()
            .split('/')
            .next()
            .unwrap();
        for index in 0..2 {
            let mut stream = std::net::TcpStream::connect(authority).unwrap();
            write!(
                stream,
                "GET /review-fixture-drain/{index} HTTP/1.1\r\nHost: {authority}\r\nIdempotency-Key: fixture-drain-{index}\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            let mut ignored = Vec::new();
            stream.read_to_end(&mut ignored).unwrap();
        }
    }
    let requests = server.finish();
    storage.finish_one();
    let command_continued = !requests[2].starts_with("GET /review-fixture-drain/");

    assert!(
        !output.status.success()
            && result["outcome"] != "accepted"
            && result.get("runId").is_none()
            && !command_continued,
        "a definite 403 must abort before seal and CreateRun; observed status={:?}, outcome={:?}, run_id_present={}, continued={command_continued}",
        output.status.code(),
        result["outcome"],
        result.get("runId").is_some(),
    );
}

#[test]
fn text_input_upload_redirect_is_not_followed_or_reported_as_acceptance() {
    let input_bytes = b"redirect safety sentinel\n";
    let input_directory = tempfile::tempdir().unwrap();
    let input_path = input_directory.path().join("request.txt");
    fs::write(&input_path, input_bytes).unwrap();
    let redirect_secret = "unique-redirect-target-sentinel";
    let storage = api_test_support::ScriptedHttpServer::respond(http_response_with_headers(
        "307 Temporary Redirect",
        None,
        &[(
            "Location",
            "https://storage.invalid/private/redirect?signature=unique-redirect-target-sentinel",
        )],
        b"storage response details",
    ));
    let signed_url = format!(
        "{}private/request?signature=unique-original-capability-sentinel",
        storage.api_url
    );
    let (server, _credential_directory, credential_path) = prepared_run(vec![
        create_input_set_response(input_bytes, false),
        upload_capability_response(input_bytes, &signed_url),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &create_args_with_text_input("request", input_path.to_str().unwrap(), true),
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "conflict");
    assert!(result.get("runId").is_none());
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            redirect_secret,
            "unique-original-capability-sentinel",
            std::str::from_utf8(input_bytes).unwrap(),
            "storage response details",
        ],
    );
    assert_eq!(server.finish().len(), 2);
    let upload = storage.finish_one();
    assert!(upload.starts_with("PUT "));
    assert!(!upload.contains("authorization:"));
}

#[test]
fn precondition_replay_is_resolved_by_authoritative_sealing() {
    let input_bytes = b"already uploaded named input sentinel\n";
    let input_directory = tempfile::tempdir().unwrap();
    let input_path = input_directory.path().join("request.txt");
    fs::write(&input_path, input_bytes).unwrap();
    let storage = OneShotServer::respond("412 Precondition Failed", None, b"");
    let signed_url = format!("{}/private/request?signature=private", storage.api_url);
    let (server, _credential_directory, credential_path) = prepared_run(vec![
        create_input_set_response(input_bytes, false),
        upload_capability_response(input_bytes, &signed_url),
        seal_input_set_response(input_bytes, false),
        acceptance_response(false),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &create_args_with_text_input("request", input_path.to_str().unwrap(), true),
        &environment,
    );

    assert!(
        output.status.success(),
        "a 412 replay must be resolved by sealing: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "accepted");
    assert_eq!(result["runId"], RUN_ID);
    assert_eq!(server.finish().len(), 4);
    storage.finish();
}

#[test]
fn run_show_reports_the_complete_projection_in_plain_and_json_modes() {
    for json in [false, true] {
        let (server, _directory, credential_path) =
            prepared_run(vec![json_http_response("200 OK", run_body())]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut args = vec!["run", "show", ORGANIZATION, RUN_ID];
        if json {
            args.push("--json");
        }
        args.push("--allow-insecure-http");

        let output = run_with_env(&args, &environment);

        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        if json {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
                serde_json::json!({
                    "schemaVersion": 1,
                    "deployment": server.api_url,
                    "outcome": "found",
                    "run": run_body()
                })
            );
        } else {
            let stdout = String::from_utf8(output.stdout).unwrap();
            for field in [
                format!("run: {RUN_ID}"),
                "state: running".to_owned(),
                "version: 7".to_owned(),
                format!("attempt: {ATTEMPT_ID} (number 2)"),
                "source branch: release/next".to_owned(),
                "  workflow: workflows/build.yaml".to_owned(),
                "  provider: github".to_owned(),
                format!("  input set: {INPUT_SET_ID}"),
                "  availability: available".to_owned(),
                "created: 2026-08-10T12:00:00Z".to_owned(),
                "updated: 2026-08-10T12:05:00Z".to_owned(),
            ] {
                assert!(
                    stdout.lines().any(|line| line == field),
                    "missing {field:?} in {stdout:?}"
                );
            }
        }
        let request = server.finish().pop().unwrap();
        assert!(request.starts_with(&format!(
            "GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID} HTTP/1.1\r\n"
        )));
    }
}

#[test]
fn run_wait_emits_terminal_json_with_state_specific_exit_status() {
    for (state, expected_exit) in [
        ("succeeded", 0),
        ("failed", 1),
        ("cancelled", 1),
        ("interrupted", 1),
        ("rejected", 1),
    ] {
        let (server, _directory, credential_path) = prepared_run(vec![json_http_response(
            "200 OK",
            run_body_with_state(state),
        )]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &[
                "run",
                "wait",
                ORGANIZATION,
                RUN_ID,
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(expected_exit));
        assert!(output.stderr.is_empty());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["schemaVersion"], 1);
        assert_eq!(result["deployment"], server.api_url);
        assert_eq!(result["outcome"], state);
        assert_eq!(result["run"]["id"], RUN_ID);
        assert_eq!(result["run"]["state"], state);
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with(&format!(
            "GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID} HTTP/1.1\r\n"
        )));
    }
}

#[test]
fn run_wait_emits_the_terminal_plain_projection() {
    let (server, _directory, credential_path) = prepared_run(vec![json_http_response(
        "200 OK",
        run_body_with_state("failed"),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &["run", "wait", ORGANIZATION, RUN_ID, "--allow-insecure-http"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.lines().any(|line| line == "✗ Run failed."));
    assert!(stdout.lines().any(|line| line == format!("run: {RUN_ID}")));
    assert!(stdout.lines().any(|line| line == "state: failed"));
    server.finish();
}

#[test]
fn run_wait_refreshes_authentication_and_recovers_from_one_server_failure() {
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
                "access_token": REFRESHED_TOKEN,
                "refresh_token": "unique-cloud-run-wait-refreshed-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        problem_http_response(
            "500 Internal Server Error",
            serde_json::json!({
                "type": "https://api.scherzo.dev/problems/internal-server-error",
                "title": "Internal Server Error",
                "status": 500
            }),
        ),
        json_http_response("200 OK", run_body_with_state("succeeded")),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "run",
            "wait",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--timeout",
            "10s",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "succeeded");
    assert_eq!(result["run"]["id"], RUN_ID);
    assert_no_secret_output(&output, &[TOKEN, REFRESHED_TOKEN]);
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert!(requests[0].starts_with("GET /api/v1/organizations/"));
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert!(requests[2].starts_with("GET /api/v1/organizations/"));
    assert!(requests[3].starts_with("GET /api/v1/organizations/"));
    assert_eq!(
        header_value(&requests[3], "authorization"),
        format!("Bearer {REFRESHED_TOKEN}")
    );
}

#[test]
fn run_wait_preserves_fatal_response_classifications() {
    let unauthenticated = run(&["run", "wait", ORGANIZATION, RUN_ID, "--json"]);
    assert_eq!(unauthenticated.status.code(), Some(3));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&unauthenticated.stdout).unwrap()["outcome"],
        "unauthenticated"
    );

    let cases = [
        (
            problem_http_response(
                "403 Forbidden",
                serde_json::json!({
                    "type": "https://api.scherzo.dev/problems/forbidden",
                    "title": "Forbidden",
                    "status": 403
                }),
            ),
            "forbidden",
        ),
        (
            problem_http_response(
                "404 Not Found",
                serde_json::json!({
                    "type": "https://api.scherzo.dev/problems/not-found",
                    "title": "Not Found",
                    "status": 404
                }),
            ),
            "not_found",
        ),
        (
            json_http_response("200 OK", serde_json::json!({"state": "running"})),
            "invalid_response",
        ),
    ];

    for (response, expected) in cases {
        let (server, _directory, credential_path) = prepared_run(vec![response]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &[
                "run",
                "wait",
                ORGANIZATION,
                RUN_ID,
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        assert!(output.stderr.is_empty());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], expected);
        assert_eq!(result["runId"], RUN_ID);
        server.finish();
    }
}

#[test]
fn run_show_rejects_a_projection_for_a_different_run() {
    let mut response_body = run_body();
    response_body["id"] = serde_json::json!("run_01k0z6r1w8f4jy2m7q9v3x5abd");

    let (output, server) = run_show_with_response(json_http_response("200 OK", response_body));

    assert_invalid_response(&output);
    server.finish();
}

#[test]
fn run_create_rejects_malformed_success_envelopes() {
    let cases = [
        acceptance_response_for("202 Accepted", RUN_ID, false, &[]),
        acceptance_response_for(
            "201 Created",
            RUN_ID,
            false,
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
                ),
            ],
        ),
        acceptance_response_for(
            "202 Accepted",
            RUN_ID,
            false,
            &[
                ("Idempotency-Key", "mismatched-request-key"),
                (
                    "Location",
                    "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
                ),
            ],
        ),
        acceptance_response_for(
            "202 Accepted",
            RUN_ID,
            false,
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
                ),
            ],
        ),
        acceptance_response_for(
            "202 Accepted",
            RUN_ID,
            false,
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abd",
                ),
            ],
        ),
    ];
    for response in cases {
        let (server, _directory, credential_path) = prepared_run(vec![response]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(&create_args(true), &environment);

        assert_invalid_response(&output);
        server.finish();
    }
}

#[test]
fn run_operations_reject_responses_larger_than_the_api_limit() {
    let mut create_body = acceptance_body(RUN_ID, false);
    create_body.extend(std::iter::repeat_n(b' ', 1024 * 1024));
    let create_response = http_response_with_headers(
        "202 Accepted",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &create_body,
    );
    let (server, _directory, credential_path) = prepared_run(vec![create_response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(&create_args(true), &environment);

    assert_invalid_response(&output);
    server.finish();

    let mut show_body = serde_json::to_vec(&run_body()).unwrap();
    show_body.extend(std::iter::repeat_n(b' ', 1024 * 1024));
    let show_response = chunked_json_response("200 OK", &show_body);
    let (output, server) = run_show_with_response(show_response);

    assert_invalid_response(&output);
    server.finish();
}

#[test]
fn run_semantic_response_validation_rejects_contract_invalid_values() {
    let invalid_run_id = "run_invalid";
    let create_response = acceptance_response_for(
        "202 Accepted",
        invalid_run_id,
        false,
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runs/run_invalid",
            ),
        ],
    );
    let (server, _directory, credential_path) = prepared_run(vec![create_response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(&create_args(true), &environment);

    assert_invalid_response(&output);
    server.finish();

    let mut invalid_projection = run_body();
    invalid_projection["updatedAt"] = serde_json::json!("not-a-timestamp");
    let (output, server) = run_show_with_response(json_http_response("200 OK", invalid_projection));

    assert_invalid_response(&output);
    server.finish();
}

#[test]
fn run_failures_use_registered_outcomes_without_exposing_secrets() {
    let unauthenticated = run(&["run", "show", ORGANIZATION, RUN_ID, "--json"]);
    assert_eq!(unauthenticated.status.code(), Some(3));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&unauthenticated.stdout).unwrap()["outcome"],
        "unauthenticated"
    );
    assert_no_secret_output(&unauthenticated, &[TOKEN]);

    let cases = [
        (
            "403 Forbidden",
            serde_json::json!({
                "type": "https://api.scherzo.dev/problems/forbidden",
                "title": "Forbidden",
                "status": 403,
                "detail": "unique-response-capability-material"
            }),
            "forbidden",
        ),
        (
            "404 Not Found",
            serde_json::json!({
                "type": "https://api.scherzo.dev/problems/not-found",
                "title": "Not found",
                "status": 404
            }),
            "not_found",
        ),
    ];
    for (status, problem, expected) in cases {
        let (server, _directory, credential_path) =
            prepared_run(vec![problem_http_response(status, problem)]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &[
                "run",
                "show",
                ORGANIZATION,
                RUN_ID,
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );
        assert_eq!(output.status.code(), Some(1));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
            expected
        );
        assert_no_secret_output(&output, &[TOKEN, "unique-response-capability-material"]);
        server.finish();
    }

    let (conflict, _directory, credential_path) = prepared_run(vec![problem_http_response(
        "409 Conflict",
        serde_json::json!({
            "type": "https://api.scherzo.dev/problems/project-not-ready",
            "title": "Project not ready",
            "status": 409,
            "blockers": ["runner_pool_unassigned"]
        }),
    )]);
    let environment = deployment_environment(&conflict.api_url, &credential_path);
    let output = run_with_env(&create_args(true), &environment);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "conflict"
    );
    assert_no_secret_output(&output, &[TOKEN]);
    conflict.finish();

    let (malformed, _directory, credential_path) = prepared_run(vec![json_http_response(
        "200 OK",
        serde_json::json!({"state": "running"}),
    )]);
    let environment = deployment_environment(&malformed.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "invalid_response"
    );
    assert_no_secret_output(&output, &[TOKEN]);
    malformed.finish();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    drop(listener);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(&credential_path, &api_url, TOKEN, "2999-01-01T00:00:00Z");
    let environment = deployment_environment(&api_url, credential_path.to_str().unwrap());
    let output = run_with_env(
        &[
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(output.status.code(), Some(4));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "unreachable"
    );
    assert_no_secret_output(&output, &[TOKEN]);
}

#[test]
fn run_wait_bounds_transport_retries_and_emits_one_unavailable_result() {
    let (server, _directory, credential_path) = prepared_run(vec![Vec::new(), Vec::new()]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "run",
            "wait",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--timeout",
            "10s",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(4));
    assert!(output.stderr.is_empty());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "unreachable");
    assert_eq!(result["category"], "connection");
    assert_eq!(result["runId"], RUN_ID);
    assert_eq!(server.finish().len(), 2);
}

#[cfg(target_os = "linux")]
#[test]
fn timeout_and_signals_stop_only_the_local_run_wait() {
    let cases = [
        (None, Some(rustix::process::Signal::INT), 130, None),
        (None, Some(rustix::process::Signal::TERM), 143, None),
        (Some("1s"), None, 1, Some("timed_out")),
    ];

    for (timeout, signal, expected_exit, expected_outcome) in cases {
        let mut server =
            ScriptedServer::respond_with_paused_first_response(vec![json_http_response(
                "200 OK",
                run_body(),
            )]);
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        write_credential_fixture(
            &credential_path,
            &server.api_url,
            TOKEN,
            "2999-01-01T00:00:00Z",
        );
        let environment =
            deployment_environment(&server.api_url, credential_path.to_str().unwrap());
        let mut args = vec!["run", "wait", ORGANIZATION, RUN_ID, "--json"];
        if let Some(timeout) = timeout {
            args.extend(["--timeout", timeout]);
        }
        args.push("--allow-insecure-http");
        let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
        command
            .args(args)
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
        assert!(request.starts_with(&format!(
            "GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID} HTTP/1.1\r\n"
        )));

        if let Some(signal) = signal {
            rustix::process::kill_process(
                rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
                signal,
            )
            .unwrap();
        }
        let output = child.wait_with_output().unwrap();
        server.release_paused_response();

        assert_eq!(output.status.code(), Some(expected_exit));
        assert!(output.stderr.is_empty());
        match expected_outcome {
            Some(expected_outcome) => {
                let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
                assert_eq!(result["outcome"], expected_outcome);
                assert_eq!(result["organizationRef"], ORGANIZATION);
                assert_eq!(result["runId"], RUN_ID);
            }
            None => assert!(output.stdout.is_empty()),
        }
        assert_no_secret_output(&output, &[TOKEN]);
        assert!(server.finish().is_empty());
    }
}

#[cfg(target_os = "linux")]
#[test]
fn signalled_create_reports_unknown_commitment_without_exposing_credentials() {
    for (signal, expected_exit) in [
        (rustix::process::Signal::INT, 130),
        (rustix::process::Signal::TERM, 143),
    ] {
        let mut server =
            ScriptedServer::respond_with_paused_first_response(vec![acceptance_response(false)]);
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        write_credential_fixture(
            &credential_path,
            &server.api_url,
            TOKEN,
            "2999-01-01T00:00:00Z",
        );
        let environment =
            deployment_environment(&server.api_url, credential_path.to_str().unwrap());
        let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
        command
            .args(create_args(true))
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
        assert!(request.starts_with("POST /api/v1/organizations/"));

        rustix::process::kill_process(
            rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
            signal,
        )
        .unwrap();
        let output = child.wait_with_output().unwrap();
        server.release_paused_response();

        assert_eq!(output.status.code(), Some(expected_exit));
        assert!(output.stderr.is_empty());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "unknown");
        assert_eq!(result["commitment"], "unknown");
        assert_eq!(result["organizationRef"], ORGANIZATION);
        assert!(result.get("runId").is_none());
        assert_no_secret_output(&output, &[TOKEN]);
        assert!(server.finish().is_empty());
    }
}

#[cfg(target_os = "linux")]
#[test]
fn interruption_during_text_input_upload_never_claims_run_acceptance() {
    let input_bytes = b"private interrupted named input sentinel\n";
    let input_directory = tempfile::tempdir().unwrap();
    let input_path = input_directory.path().join("request.txt");
    fs::write(&input_path, input_bytes).unwrap();
    let mut storage =
        ScriptedServer::respond_with_paused_first_response(vec![http_response_with_headers(
            "204 No Content",
            None,
            &[],
            b"",
        )]);
    let signed_url = format!(
        "{}/private/request?signature=unique-interrupted-capability-sentinel",
        storage.api_url
    );
    let (server, _credential_directory, credential_path) = prepared_run(vec![
        create_input_set_response(input_bytes, false),
        upload_capability_response(input_bytes, &signed_url),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args(create_args_with_text_input(
            "request",
            input_path.to_str().unwrap(),
            true,
        ))
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
    let upload = storage.next_request();
    assert!(upload.starts_with("PUT "));
    assert!(!upload.contains("authorization:"));

    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    storage.release_paused_response();

    assert_eq!(output.status.code(), Some(130));
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            "unique-interrupted-capability-sentinel",
            std::str::from_utf8(input_bytes).unwrap(),
        ],
    );
    assert_eq!(server.finish().len(), 2);
    assert!(storage.finish().is_empty());
}
