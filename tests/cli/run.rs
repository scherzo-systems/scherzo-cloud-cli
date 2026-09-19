use super::*;

use base64::Engine as _;
use ring::digest::{SHA256, digest};

#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStringExt as _;
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
        "integrationContext": {
            "issueId": "issue-private-context-sentinel",
            "source": "linear"
        },
        "publication": null,
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

fn create_args_with_scalar_input<'a>(
    flag: &'a str,
    input_name: &'a str,
    input_source: &'a str,
    json: bool,
) -> Vec<&'a str> {
    let mut args: Vec<&'a str> = create_args(json);
    let insertion = args.len() - 1;
    args.insert(insertion, flag);
    args.insert(insertion + 1, input_name);
    args.insert(insertion + 2, input_source);
    args
}

fn create_args_with_text_input<'a>(
    input_name: &'a str,
    input_file: &'a str,
    json: bool,
) -> Vec<&'a str> {
    create_args_with_scalar_input("--input-text-file", input_name, input_file, json)
}

fn create_args_with_file_input<'a>(
    input_name: &'a str,
    media_type: &'a str,
    input_file: &'a str,
    json: bool,
) -> Vec<&'a str> {
    let mut args: Vec<&'a str> = create_args(json);
    let insertion = args.len() - 1;
    args.splice(
        insertion..insertion,
        ["--input-file", input_name, media_type, input_file],
    );
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

fn scalar_manifest_digest(bytes: &[u8], kind: &str) -> String {
    let canonical = format!(
        "{{\"inputs\":{{\"request\":{{\"kind\":\"{kind}\",\"sha256\":\"{}\",\"sizeBytes\":{}}}}},\"schemaVersion\":1}}",
        hex_digest(bytes),
        bytes.len()
    );
    hex_digest(canonical.as_bytes())
}

fn file_manifest_digest(bytes: &[u8], media_type: &str) -> String {
    let canonical = format!(
        "{{\"inputs\":{{\"request\":{{\"kind\":\"file\",\"mediaType\":{},\"sha256\":\"{}\",\"sizeBytes\":{}}}}},\"schemaVersion\":1}}",
        serde_json::to_string(media_type).unwrap(),
        hex_digest(bytes),
        bytes.len()
    );
    hex_digest(canonical.as_bytes())
}

fn scalar_input_set_body(
    bytes: &[u8],
    kind: &str,
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
                    "kind": kind,
                    "sizeBytes": bytes.len(),
                    "sha256": hex_digest(bytes)
                }
            }
        },
        "manifestDigest": {
            "algorithm": "sha256",
            "value": scalar_manifest_digest(bytes, kind)
        },
        "inputCount": 1,
        "attachmentCount": 0,
        "aggregateSizeBytes": bytes.len(),
        "state": state,
        "createdAt": "2026-08-24T01:00:00Z",
        "openDeadlineAt": "2999-08-25T01:00:00Z",
        "members": [{"memberId": "inputs/request", "uploadConfirmed": uploaded}],
        "replayed": replayed
    });
    if state == "sealed" {
        body["sealedAt"] = serde_json::json!("2026-08-24T01:02:00Z");
        body["sealedDeadlineAt"] = serde_json::json!("2026-08-25T01:02:00Z");
    }
    body
}

fn file_input_set_body(
    bytes: &[u8],
    media_type: &str,
    state: &str,
    uploaded: bool,
) -> serde_json::Value {
    let mut body = scalar_input_set_body(bytes, "file", state, uploaded, false);
    body["manifest"]["inputs"]["request"]["mediaType"] = serde_json::json!(media_type);
    body["manifestDigest"]["value"] = serde_json::json!(file_manifest_digest(bytes, media_type));
    body
}

fn create_scalar_input_set_response(bytes: &[u8], kind: &str, replayed: bool) -> Vec<u8> {
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
        &serde_json::to_vec(&scalar_input_set_body(bytes, kind, "open", false, replayed)).unwrap(),
    )
}

fn create_file_input_set_response(bytes: &[u8], media_type: &str) -> Vec<u8> {
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
        &serde_json::to_vec(&file_input_set_body(bytes, media_type, "open", false)).unwrap(),
    )
}

fn create_input_set_response(bytes: &[u8], replayed: bool) -> Vec<u8> {
    create_scalar_input_set_response(bytes, "text", replayed)
}

fn scalar_upload_capability_response(bytes: &[u8], media_type: &str, url: &str) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "capabilityExpiresAt": "2998-08-24T01:05:00Z",
            "members": [{
                "memberId": "inputs/request",
                "url": url,
                "requiredHeaders": {
                    "contentLength": bytes.len().to_string(),
                    "contentType": media_type,
                    "ifNoneMatch": "*",
                    "xAmzChecksumSha256": base64::engine::general_purpose::STANDARD.encode(sha256(bytes))
                }
            }]
        }))
        .unwrap(),
    )
}

fn upload_capability_response(bytes: &[u8], url: &str) -> Vec<u8> {
    scalar_upload_capability_response(bytes, "text/plain; charset=utf-8", url)
}

fn seal_file_input_set_response(bytes: &[u8], media_type: &str) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&file_input_set_body(bytes, media_type, "sealed", true)).unwrap(),
    )
}

fn seal_scalar_input_set_response(bytes: &[u8], kind: &str, replayed: bool) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&scalar_input_set_body(
            bytes, kind, "sealed", true, replayed,
        ))
        .unwrap(),
    )
}

fn seal_input_set_response(bytes: &[u8], replayed: bool) -> Vec<u8> {
    seal_scalar_input_set_response(bytes, "text", replayed)
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
fn run_create_sends_integration_context_from_regular_file_and_standard_input() {
    for source in ["file", "stdin"] {
        let context = br#"{"empty":"","issueId":"abc-123","source":"linear"}"#;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("context.json");
        fs::write(&path, context).unwrap();
        let operand = if source == "file" {
            path.to_str().unwrap()
        } else {
            "-"
        };
        let (server, _credentials, credential_path) =
            prepared_run(vec![acceptance_response(false)]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = create_args(true);
        let insertion = arguments.len() - 1;
        arguments.splice(
            insertion..insertion,
            ["--integration-context-file", operand],
        );

        let output = if source == "stdin" {
            run_with_stdin(&arguments, &environment, context)
        } else {
            run_with_env(&arguments, &environment)
        };

        assert!(
            output.status.success(),
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_no_secret_output(&output, &["abc-123"]);
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            request_body(&requests[0])["integrationContext"],
            serde_json::json!({"empty": "", "issueId": "abc-123", "source": "linear"})
        );
    }
}

#[test]
fn run_create_reads_named_text_and_json_from_standard_input() {
    for (flag, kind, input_bytes) in [
        ("--input-text-file", "text", b"stdin text".as_slice()),
        ("--input-json-file", "json", br#"{"stdin":true}"#.as_slice()),
    ] {
        let storage = OneShotServer::respond("204 No Content", None, b"");
        let signed_url = format!("{}/private/request?signature=stdin", storage.api_url);
        let (server, _credentials, credential_path) = prepared_run(vec![
            create_scalar_input_set_response(input_bytes, kind, false),
            scalar_upload_capability_response(
                input_bytes,
                if kind == "text" {
                    "text/plain; charset=utf-8"
                } else {
                    "application/json"
                },
                &signed_url,
            ),
            seal_scalar_input_set_response(input_bytes, kind, false),
            acceptance_response(false),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let arguments = create_args_with_scalar_input(flag, "request", "-", true);

        let output = run_with_stdin(&arguments, &environment, input_bytes);

        assert!(
            output.status.success(),
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(server.finish().len(), 4);
        assert_eq!(
            storage
                .finish()
                .split_once("\r\n\r\n")
                .unwrap()
                .1
                .as_bytes(),
            input_bytes
        );
    }
}

#[test]
fn competing_standard_input_claims_reject_before_cloud_access() {
    for input_flag in ["--input-text-file", "--input-json-file"] {
        let (server, _credentials, credential_path) = prepared_run(Vec::new());
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = create_args(true);
        let insertion = arguments.len() - 1;
        arguments.splice(
            insertion..insertion,
            [
                "--integration-context-file",
                "-",
                input_flag,
                "request",
                "-",
            ],
        );

        let output = run_with_env(&arguments, &environment);

        assert_eq!(output.status.code(), Some(1));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
            "invalid_input"
        );
        assert!(server.finish().is_empty());
    }
}

#[test]
fn invalid_integration_context_rejects_before_cloud_access() {
    let mut too_many = serde_json::Map::new();
    for index in 0..33 {
        too_many.insert(
            format!("key{index:02}"),
            serde_json::Value::String(String::new()),
        );
    }
    let invalid_documents = [
        br#"{"duplicate":"first","duplicate":"second"}"#.to_vec(),
        serde_json::to_vec(&too_many).unwrap(),
        serde_json::to_vec(&serde_json::json!({"": "value"})).unwrap(),
        serde_json::to_vec(&serde_json::json!({"key": "v".repeat(1025)})).unwrap(),
        br#"{"key":"\u0000"}"#.to_vec(),
    ];
    for document in invalid_documents {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("context.json");
        fs::write(&path, document).unwrap();
        let (server, _credentials, credential_path) = prepared_run(Vec::new());
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = create_args(true);
        let insertion = arguments.len() - 1;
        arguments.splice(
            insertion..insertion,
            ["--integration-context-file", path.to_str().unwrap()],
        );

        let output = run_with_env(&arguments, &environment);

        assert_eq!(output.status.code(), Some(1));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
            "invalid_input"
        );
        assert!(server.finish().is_empty());
    }
}

#[test]
fn invalid_integration_context_plain_output_redacts_caller_values() {
    const PRIVATE_VALUE: &str = "private-parser-value-sentinel";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("context.json");
    let encoded_object = serde_json::json!({"token": PRIVATE_VALUE}).to_string();
    fs::write(&path, serde_json::to_vec(&encoded_object).unwrap()).unwrap();
    let (server, _credentials, credential_path) = prepared_run(Vec::new());
    let environment = deployment_environment(&server.api_url, &credential_path);
    let mut arguments = create_args(false);
    let insertion = arguments.len() - 1;
    arguments.splice(
        insertion..insertion,
        ["--integration-context-file", path.to_str().unwrap()],
    );

    let output = run_with_env(&arguments, &environment);

    assert_eq!(output.status.code(), Some(1));
    assert_no_secret_output(&output, &[PRIVATE_VALUE]);
    assert!(server.finish().is_empty());
}

#[test]
fn oversized_integration_context_standard_input_rejects_before_cloud_access() {
    let (server, _credentials, credential_path) = prepared_run(Vec::new());
    let environment = deployment_environment(&server.api_url, &credential_path);
    let mut arguments = create_args(true);
    let insertion = arguments.len() - 1;
    arguments.splice(insertion..insertion, ["--integration-context-file", "-"]);
    let oversized_source = vec![b' '; 128 * 1024 + 1];

    let output = run_with_stdin(&arguments, &environment, &oversized_source);

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "invalid_input"
    );
    assert!(server.finish().is_empty());
}

#[test]
fn unreadable_integration_context_rejects_before_cloud_access() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing.json");
    let (server, _credentials, credential_path) = prepared_run(Vec::new());
    let environment = deployment_environment(&server.api_url, &credential_path);
    let mut arguments = create_args(true);
    let insertion = arguments.len() - 1;
    arguments.splice(
        insertion..insertion,
        ["--integration-context-file", missing.to_str().unwrap()],
    );

    let output = run_with_env(&arguments, &environment);

    assert_eq!(output.status.code(), Some(1));
    assert!(server.finish().is_empty());
}

#[test]
fn run_create_consumes_an_explicit_sealed_input_set_without_restaging() {
    let (server, _directory, credential_path) = prepared_run(vec![acceptance_response(false)]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let mut arguments = create_args(true);
    let insertion = arguments.len() - 1;
    arguments.splice(insertion..insertion, ["--input-set-id", INPUT_SET_ID]);

    let output = run_with_env(&arguments, &environment);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "accepted");
    assert_eq!(result["inputSetId"], INPUT_SET_ID);
    let request = server.finish().remove(0);
    assert!(request.contains("/runs HTTP/1.1"));
    assert_eq!(request_body(&request)["inputSetId"], INPUT_SET_ID);
}

#[test]
fn explicit_input_set_conflicts_with_acquired_inputs_before_cloud_access() {
    let output = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args([
            "run",
            "create",
            ORGANIZATION,
            "--project-id",
            PROJECT_ID,
            "--workflow-path",
            WORKFLOW_PATH,
            "--input-set-id",
            INPUT_SET_ID,
            "--input-text",
            "request",
            "private argument sentinel",
        ])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("private argument sentinel"));
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
fn run_create_stages_named_json_sources_without_rewriting_bytes() {
    let input_bytes = b"{\n  \"enabled\": true, \"ratio\": 1.00\n}\n";
    let input_directory = tempfile::tempdir().unwrap();
    let input_path = input_directory.path().join("request.json");
    fs::write(&input_path, input_bytes).unwrap();
    let input_path = input_path.to_str().unwrap();

    for (flag, source) in [
        ("--input-json", std::str::from_utf8(input_bytes).unwrap()),
        ("--input-json-file", input_path),
    ] {
        let storage = OneShotServer::respond("204 No Content", None, b"");
        let signed_url = format!(
            "{}/private/request?signature=unique-json-capability-sentinel",
            storage.api_url
        );
        let (server, _credential_directory, credential_path) = prepared_run(vec![
            create_scalar_input_set_response(input_bytes, "json", false),
            scalar_upload_capability_response(input_bytes, "application/json", &signed_url),
            seal_scalar_input_set_response(input_bytes, "json", false),
            acceptance_response(false),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &create_args_with_scalar_input(flag, "request", source, true),
            &environment,
        );

        assert!(
            output.status.success(),
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        assert_no_secret_output(
            &output,
            &[
                TOKEN,
                "unique-json-capability-sentinel",
                std::str::from_utf8(input_bytes).unwrap(),
            ],
        );
        let requests = server.finish();
        assert_eq!(
            request_body(&requests[0]),
            serde_json::json!({
                "projectId": PROJECT_ID,
                "schemaVersion": 1,
                "inputs": {
                    "request": {
                        "kind": "json",
                        "sizeBytes": input_bytes.len(),
                        "sha256": hex_digest(input_bytes)
                    }
                }
            })
        );
        assert_eq!(
            request_body(&requests[1]),
            serde_json::json!({"members": ["inputs/request"]})
        );
        assert_eq!(request_body(&requests[3])["inputSetId"], INPUT_SET_ID);

        let upload = storage.finish();
        assert_eq!(
            upload.split_once("\r\n\r\n").unwrap().1.as_bytes(),
            input_bytes
        );
        assert_eq!(header_value(&upload, "content-type"), "application/json");
        assert_eq!(
            header_value(&upload, "x-amz-checksum-sha256"),
            base64::engine::general_purpose::STANDARD.encode(sha256(input_bytes))
        );
    }
}

#[test]
fn run_create_stages_named_file_with_exact_media_type_and_bytes() {
    let input_bytes = *b"exact file bytes";
    let media_type = "application/octet-stream; version=1";
    let input_directory = tempfile::tempdir().unwrap();
    let input_path = input_directory.path().join("request.bin");
    fs::write(&input_path, input_bytes).unwrap();
    let input_path = input_path.to_str().unwrap();
    let storage = OneShotServer::respond("204 No Content", None, b"");
    let signed_url = format!(
        "{}/private/request?signature=unique-file-capability-sentinel",
        storage.api_url
    );
    let (server, _credential_directory, credential_path) = prepared_run(vec![
        create_file_input_set_response(&input_bytes, media_type),
        scalar_upload_capability_response(&input_bytes, media_type, &signed_url),
        seal_file_input_set_response(&input_bytes, media_type),
        acceptance_response(false),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &create_args_with_file_input("request", media_type, input_path, true),
        &environment,
    );

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    assert_no_secret_output(&output, &[TOKEN, "unique-file-capability-sentinel"]);
    let requests = server.finish();
    assert_eq!(
        request_body(&requests[0]),
        serde_json::json!({
            "projectId": PROJECT_ID,
            "schemaVersion": 1,
            "inputs": {
                "request": {
                    "kind": "file",
                    "mediaType": media_type,
                    "sizeBytes": input_bytes.len(),
                    "sha256": hex_digest(&input_bytes)
                }
            }
        })
    );
    assert_eq!(
        request_body(&requests[1]),
        serde_json::json!({"members": ["inputs/request"]})
    );
    assert_eq!(request_body(&requests[3])["inputSetId"], INPUT_SET_ID);

    let upload = storage.finish();
    assert_eq!(
        upload.split_once("\r\n\r\n").unwrap().1.as_bytes(),
        input_bytes
    );
    assert_eq!(header_value(&upload, "content-type"), media_type);
}

#[test]
fn run_create_stages_mixed_values_ordered_attachments_and_present_empty_collections() {
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("first.txt");
    let second_path = directory.path().join("second.bin");
    fs::write(&first_path, b"first attachment").unwrap();
    fs::write(&second_path, b"second attachment").unwrap();
    let json_bytes = b"{\"enabled\":true}";
    let values: [&[u8]; 4] = [b"", b"first attachment", b"second attachment", json_bytes];
    let media_types = [
        "text/plain; charset=utf-8",
        "text/plain",
        "application/octet-stream",
        "application/json",
    ];
    let storages = values
        .iter()
        .map(|_| OneShotServer::respond("204 No Content", None, b""))
        .collect::<Vec<_>>();
    let urls = storages
        .iter()
        .enumerate()
        .map(|(index, storage)| format!("{}/member-{index}?signature=mixed", storage.api_url))
        .collect::<Vec<_>>();
    let manifest = serde_json::json!({
        "schemaVersion": 1,
        "inputs": {
            "emptyText": {
                "kind": "text",
                "sizeBytes": 0,
                "sha256": hex_digest(b"")
            },
            "evidence": {
                "kind": "attachments",
                "items": [
                    {
                        "index": 0,
                        "displayName": null,
                        "mediaType": "text/plain",
                        "sizeBytes": values[1].len(),
                        "sha256": hex_digest(values[1])
                    },
                    {
                        "index": 1,
                        "displayName": null,
                        "mediaType": "application/octet-stream",
                        "sizeBytes": values[2].len(),
                        "sha256": hex_digest(values[2])
                    }
                ]
            },
            "nothing": {"kind": "attachments", "items": []},
            "settings": {
                "kind": "json",
                "sizeBytes": json_bytes.len(),
                "sha256": hex_digest(json_bytes)
            }
        }
    });
    let canonical = serde_json::to_vec(&manifest).unwrap();
    let member_ids = [
        "inputs/emptyText",
        "inputs/evidence/000000",
        "inputs/evidence/000001",
        "inputs/settings",
    ];
    let set_body = |state: &str, uploaded: bool| {
        let mut body = serde_json::json!({
            "id": INPUT_SET_ID,
            "organizationId": ORGANIZATION_ID,
            "projectId": PROJECT_ID,
            "boundsProfile": 1,
            "manifest": manifest.clone(),
            "manifestDigest": {"algorithm": "sha256", "value": hex_digest(&canonical)},
            "inputCount": 4,
            "attachmentCount": 2,
            "aggregateSizeBytes": values.iter().map(|value| value.len()).sum::<usize>(),
            "state": state,
            "createdAt": "2026-08-24T01:00:00Z",
            "openDeadlineAt": "2999-08-25T01:00:00Z",
            "members": member_ids.iter().map(|member| serde_json::json!({
                "memberId": member,
                "uploadConfirmed": uploaded
            })).collect::<Vec<_>>(),
            "replayed": false
        });
        if state == "sealed" {
            body["sealedAt"] = serde_json::json!("2026-08-24T01:02:00Z");
            body["sealedDeadlineAt"] = serde_json::json!("2026-08-25T01:02:00Z");
        }
        body
    };
    let create_response = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/run-input-sets/ris_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&set_body("open", false)).unwrap(),
    );
    let capability_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "capabilityExpiresAt": "2998-08-24T01:05:00Z",
            "members": member_ids.iter().enumerate().map(|(index, member)| serde_json::json!({
                "memberId": member,
                "url": urls[index],
                "requiredHeaders": {
                    "contentLength": values[index].len().to_string(),
                    "contentType": media_types[index],
                    "ifNoneMatch": "*",
                    "xAmzChecksumSha256": base64::engine::general_purpose::STANDARD.encode(sha256(values[index]))
                }
            })).collect::<Vec<_>>()
        }))
        .unwrap(),
    );
    let sealed_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&set_body("sealed", true)).unwrap(),
    );
    let (server, _credentials, credential_path) = prepared_run(vec![
        create_response,
        capability_response,
        sealed_response,
        acceptance_response(false),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let mut arguments = create_args(true);
    let insertion = arguments.len() - 1;
    arguments.splice(
        insertion..insertion,
        [
            "--input-text",
            "emptyText",
            "",
            "--input-json",
            "settings",
            std::str::from_utf8(json_bytes).unwrap(),
            "--input-attachment",
            "evidence",
            "text/plain",
            first_path.to_str().unwrap(),
            "--input-attachment",
            "evidence",
            "application/octet-stream",
            second_path.to_str().unwrap(),
            "--input-attachments-empty",
            "nothing",
        ],
    );

    let output = run_with_env(&arguments, &environment);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_no_secret_output(&output, &[TOKEN, "first attachment", "second attachment"]);
    let requests = server.finish();
    assert_eq!(request_body(&requests[0])["inputs"], manifest["inputs"]);
    assert_eq!(
        request_body(&requests[1]),
        serde_json::json!({"members": member_ids})
    );
    assert_eq!(request_body(&requests[3])["inputSetId"], INPUT_SET_ID);
    for (storage, expected) in storages.into_iter().zip(values) {
        assert_eq!(
            storage
                .finish()
                .split_once("\r\n\r\n")
                .unwrap()
                .1
                .as_bytes(),
            expected
        );
    }
}

fn retained_inputs_response(bytes: &[u8]) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
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
                "value": scalar_manifest_digest(bytes, "text")
            },
            "sealedAt": "2026-08-24T01:02:00Z",
            "contentExpiresAt": null,
            "inputCount": 1,
            "attachmentCount": 0,
            "aggregateSizeBytes": bytes.len(),
            "availability": "available"
        }))
        .unwrap(),
    )
}

fn download_capability_response(bytes: &[u8], url: &str) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "capabilityExpiresAt": "2998-08-24T01:05:00Z",
            "members": [{
                "memberId": "inputs/request",
                "attachmentIndex": null,
                "displayName": null,
                "mediaType": "text/plain; charset=utf-8",
                "sizeBytes": bytes.len(),
                "sha256": hex_digest(bytes),
                "url": url
            }]
        }))
        .unwrap(),
    )
}

fn retained_inputs_response_for_manifest(
    manifest: &serde_json::Value,
    input_count: usize,
    attachment_count: usize,
    aggregate_size_bytes: usize,
) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "manifest": manifest,
            "manifestDigest": {
                "algorithm": "sha256",
                "value": hex_digest(&serde_json::to_vec(manifest).unwrap())
            },
            "sealedAt": "2026-08-24T01:02:00Z",
            "contentExpiresAt": null,
            "inputCount": input_count,
            "attachmentCount": attachment_count,
            "aggregateSizeBytes": aggregate_size_bytes,
            "availability": "available"
        }))
        .unwrap(),
    )
}

fn download_capability_response_for_members(members: serde_json::Value) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "capabilityExpiresAt": "2998-08-24T01:05:00Z",
            "members": members
        }))
        .unwrap(),
    )
}

#[test]
fn explicit_input_set_flow_creates_open_then_uploads_seals_and_consumes() {
    let input_bytes = b"explicit single-use input\n";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("request.txt");
    fs::write(&path, input_bytes).unwrap();

    let (create_server, _credentials, credential_path) =
        prepared_run(vec![create_input_set_response(input_bytes, false)]);
    let environment = deployment_environment(&create_server.api_url, &credential_path);
    let create = run_with_env(
        &[
            "run",
            "input-set",
            "create",
            ORGANIZATION,
            "--project-id",
            PROJECT_ID,
            "--input-text-file",
            "request",
            path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(
        create.status.success(),
        "{}",
        String::from_utf8_lossy(&create.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&create.stdout).unwrap();
    assert_eq!(result["outcome"], "created");
    assert_eq!(result["inputSet"]["id"], INPUT_SET_ID);
    assert_eq!(result["inputSet"]["state"], "open");
    let requests = create_server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("/run-input-sets HTTP/1.1"));
    assert!(!requests[0].contains("upload-capabilities"));
    assert!(!requests[0].contains("/seal"));

    let storage = OneShotServer::respond("204 No Content", None, b"");
    let signed_url = format!("{}/object?signature=input-set-upload", storage.api_url);
    let open_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "open",
            false,
            false,
        ))
        .unwrap(),
    );
    let (upload_server, _credentials, credential_path) = prepared_run(vec![
        open_response,
        upload_capability_response(input_bytes, &signed_url),
    ]);
    let environment = deployment_environment(&upload_server.api_url, &credential_path);
    let upload = run_with_env(
        &[
            "run",
            "input-set",
            "upload",
            ORGANIZATION,
            INPUT_SET_ID,
            "--member-file",
            "inputs/request",
            path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(upload.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&upload.stdout).unwrap()["outcome"],
        "uploaded"
    );
    let requests = upload_server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains("/upload-capabilities HTTP/1.1"));
    storage.finish();

    let uploaded_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "open",
            true,
            false,
        ))
        .unwrap(),
    );
    let (seal_server, _credentials, credential_path) = prepared_run(vec![
        uploaded_response,
        seal_input_set_response(input_bytes, false),
    ]);
    let environment = deployment_environment(&seal_server.api_url, &credential_path);
    let seal = run_with_env(
        &[
            "run",
            "input-set",
            "seal",
            ORGANIZATION,
            INPUT_SET_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(seal.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&seal.stdout).unwrap()["outcome"],
        "sealed"
    );
    let requests = seal_server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains("/seal HTTP/1.1"));

    let (consume_server, _credentials, credential_path) =
        prepared_run(vec![acceptance_response(false)]);
    let environment = deployment_environment(&consume_server.api_url, &credential_path);
    let mut arguments = create_args(true);
    let insertion = arguments.len() - 1;
    arguments.splice(insertion..insertion, ["--input-set-id", INPUT_SET_ID]);
    let consume = run_with_env(&arguments, &environment);

    assert!(consume.status.success());
    let result: serde_json::Value = serde_json::from_slice(&consume.stdout).unwrap();
    assert_eq!(result["outcome"], "accepted");
    assert_eq!(result["inputSetId"], INPUT_SET_ID);
    let requests = consume_server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("/runs HTTP/1.1"));
    assert_eq!(request_body(&requests[0])["inputSetId"], INPUT_SET_ID);
}

#[test]
fn input_set_mutation_commands_upload_seal_and_delete_with_fresh_requests() {
    let input_bytes = b"selected single-use input\n";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("request.txt");
    fs::write(&path, input_bytes).unwrap();

    let storage = OneShotServer::respond("204 No Content", None, b"");
    let signed_url = format!("{}/object?signature=input-set-upload", storage.api_url);
    let open_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "open",
            false,
            false,
        ))
        .unwrap(),
    );
    let (upload_server, _credentials, credential_path) = prepared_run(vec![
        open_response,
        upload_capability_response(input_bytes, &signed_url),
    ]);
    let environment = deployment_environment(&upload_server.api_url, &credential_path);
    let upload = run_with_env(
        &[
            "run",
            "input-set",
            "upload",
            ORGANIZATION,
            INPUT_SET_ID,
            "--member-file",
            "inputs/request",
            path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(upload.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&upload.stdout).unwrap()["outcome"],
        "uploaded"
    );
    let requests = upload_server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains(&format!("/run-input-sets/{INPUT_SET_ID} HTTP/1.1")));
    assert!(requests[1].contains(&format!(
        "/run-input-sets/{INPUT_SET_ID}/upload-capabilities HTTP/1.1"
    )));
    storage.finish();
    assert_no_secret_output(&upload, &[TOKEN, "input-set-upload"]);

    let uploaded_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "open",
            true,
            false,
        ))
        .unwrap(),
    );
    let (seal_server, _credentials, credential_path) = prepared_run(vec![
        uploaded_response,
        seal_input_set_response(input_bytes, false),
    ]);
    let environment = deployment_environment(&seal_server.api_url, &credential_path);
    let seal = run_with_env(
        &[
            "run",
            "input-set",
            "seal",
            ORGANIZATION,
            INPUT_SET_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(seal.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&seal.stdout).unwrap()["outcome"],
        "sealed"
    );
    let requests = seal_server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains(&format!("/run-input-sets/{INPUT_SET_ID}/seal HTTP/1.1")));
    let seal_key = header_value(&requests[1], "idempotency-key");
    assert_eq!(seal_key.len(), 64);

    let unconfirmed = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args(["run", "input-set", "delete", ORGANIZATION, INPUT_SET_ID])
        .output()
        .unwrap();
    assert_eq!(unconfirmed.status.code(), Some(2));

    let response = http_response_with_headers(
        "204 No Content",
        None,
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        b"",
    );
    let (delete_server, _credentials, credential_path) = prepared_run(vec![response]);
    let environment = deployment_environment(&delete_server.api_url, &credential_path);
    let delete = run_with_env(
        &[
            "run",
            "input-set",
            "delete",
            ORGANIZATION,
            INPUT_SET_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(delete.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&delete.stdout).unwrap()["outcome"],
        "deleted"
    );
    let request = delete_server.finish().remove(0);
    assert!(request.starts_with(&format!(
        "DELETE /api/v1/organizations/{ORGANIZATION}/run-input-sets/{INPUT_SET_ID} HTTP/1.1"
    )));
    let delete_key = header_value(&request, "idempotency-key");
    assert_eq!(delete_key.len(), 64);
    assert_ne!(seal_key, delete_key);
}

#[test]
fn sealing_an_already_sealed_input_set_reports_a_lifecycle_conflict() {
    let input_bytes = b"already sealed";
    let sealed_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "sealed",
            true,
            false,
        ))
        .unwrap(),
    );
    let (server, _credentials, credential_path) = prepared_run(vec![sealed_response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "run",
            "input-set",
            "seal",
            ORGANIZATION,
            INPUT_SET_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["outcome"], "conflict");
    assert_eq!(receipt["inputSetId"], INPUT_SET_ID);
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains(&format!("/run-input-sets/{INPUT_SET_ID} HTTP/1.1")));
    assert!(!requests[0].contains("/seal HTTP/1.1"));
}

#[test]
fn input_set_upload_rejects_changed_member_bytes_before_capability_issue() {
    let expected = b"original immutable member";
    let changed = b"changed immutable member";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("request.txt");
    fs::write(&path, changed).unwrap();
    let open_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            expected, "text", "open", false, false,
        ))
        .unwrap(),
    );
    let (server, _credentials, credential_path) = prepared_run(vec![open_response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "run",
            "input-set",
            "upload",
            ORGANIZATION,
            INPUT_SET_ID,
            "--member-file",
            "inputs/request",
            path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "invalid_input"
    );
    assert_eq!(server.finish().len(), 1);
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            std::str::from_utf8(expected).unwrap(),
            std::str::from_utf8(changed).unwrap(),
        ],
    );
}

#[test]
fn input_set_upload_resumes_one_exact_attachment_member() {
    let uploaded_bytes = b"already accepted sibling";
    let selected_bytes = b"missing second attachment";
    let directory = tempfile::tempdir().unwrap();
    let selected_path = directory.path().join("second.bin");
    fs::write(&selected_path, selected_bytes).unwrap();
    let manifest = serde_json::json!({
        "schemaVersion": 1,
        "inputs": {
            "evidence": {
                "kind": "attachments",
                "items": [
                    {
                        "index": 0,
                        "displayName": "first.txt",
                        "mediaType": "text/plain",
                        "sizeBytes": uploaded_bytes.len(),
                        "sha256": hex_digest(uploaded_bytes)
                    },
                    {
                        "index": 1,
                        "displayName": "second.bin",
                        "mediaType": "application/octet-stream",
                        "sizeBytes": selected_bytes.len(),
                        "sha256": hex_digest(selected_bytes)
                    }
                ]
            }
        }
    });
    let set = serde_json::json!({
        "id": INPUT_SET_ID,
        "organizationId": ORGANIZATION_ID,
        "projectId": PROJECT_ID,
        "boundsProfile": 1,
        "manifest": manifest.clone(),
        "manifestDigest": {
            "algorithm": "sha256",
            "value": hex_digest(&serde_json::to_vec(&manifest).unwrap())
        },
        "inputCount": 1,
        "attachmentCount": 2,
        "aggregateSizeBytes": uploaded_bytes.len() + selected_bytes.len(),
        "state": "open",
        "createdAt": "2026-08-24T01:00:00Z",
        "openDeadlineAt": "2999-08-25T01:00:00Z",
        "members": [
            {"memberId": "inputs/evidence/000000", "uploadConfirmed": true},
            {"memberId": "inputs/evidence/000001", "uploadConfirmed": false}
        ],
        "replayed": false
    });
    let storage = OneShotServer::respond("204 No Content", None, b"");
    let signed_url = format!("{}/second?signature=resume", storage.api_url);
    let capabilities = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "capabilityExpiresAt": "2998-08-24T01:05:00Z",
            "members": [{
                "memberId": "inputs/evidence/000001",
                "url": signed_url,
                "requiredHeaders": {
                    "contentLength": selected_bytes.len().to_string(),
                    "contentType": "application/octet-stream",
                    "ifNoneMatch": "*",
                    "xAmzChecksumSha256": base64::engine::general_purpose::STANDARD.encode(sha256(selected_bytes))
                }
            }]
        }))
        .unwrap(),
    );
    let open_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&set).unwrap(),
    );
    let (server, _credentials, credential_path) = prepared_run(vec![open_response, capabilities]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "run",
            "input-set",
            "upload",
            ORGANIZATION,
            INPUT_SET_ID,
            "--member-file",
            "inputs/evidence/000001",
            selected_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "uploaded"
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        request_body(&requests[1]),
        serde_json::json!({"members": ["inputs/evidence/000001"]})
    );
    assert_eq!(
        storage
            .finish()
            .split_once("\r\n\r\n")
            .unwrap()
            .1
            .as_bytes(),
        selected_bytes
    );
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            "resume",
            std::str::from_utf8(selected_bytes).unwrap(),
        ],
    );
}

#[test]
fn input_set_and_retained_input_show_commands_emit_structural_json() {
    let input_bytes = b"show input";
    let input_set_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "open",
            false,
            false,
        ))
        .unwrap(),
    );
    let (input_set_server, _credentials, credential_path) = prepared_run(vec![input_set_response]);
    let environment = deployment_environment(&input_set_server.api_url, &credential_path);
    let input_set_output = run_with_env(
        &[
            "run",
            "input-set",
            "show",
            ORGANIZATION,
            INPUT_SET_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(input_set_output.status.success());
    let result: serde_json::Value = serde_json::from_slice(&input_set_output.stdout).unwrap();
    assert_eq!(result["outcome"], "found");
    assert_eq!(
        result["inputSet"]["manifest"]["inputs"]["request"]["kind"],
        "text"
    );
    input_set_server.finish();

    let (retained_server, _credentials, credential_path) =
        prepared_run(vec![retained_inputs_response(input_bytes)]);
    let environment = deployment_environment(&retained_server.api_url, &credential_path);
    let retained_output = run_with_env(
        &[
            "run",
            "inputs",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(retained_output.status.success());
    let result: serde_json::Value = serde_json::from_slice(&retained_output.stdout).unwrap();
    assert_eq!(result["outcome"], "found");
    assert_eq!(result["inventory"]["inputSetId"], INPUT_SET_ID);
    assert_eq!(
        result["inventory"]["manifest"]["inputs"]["request"]["kind"],
        "text"
    );
    assert_no_secret_output(&retained_output, &[TOKEN, "show input"]);
    retained_server.finish();
}

#[test]
fn run_input_http_decoders_reject_duplicate_members_before_using_success_responses() {
    let input_bytes = b"duplicate response member";
    let response_body = |response: Vec<u8>| {
        String::from_utf8(response)
            .unwrap()
            .split_once("\r\n\r\n")
            .unwrap()
            .1
            .to_owned()
    };
    let duplicate = |body: String, original: &str, replacement: &str| {
        let replaced = body.replacen(original, replacement, 1);
        assert_ne!(
            replaced, body,
            "fixture member should exist exactly as encoded"
        );
        replaced
    };

    let input_set_body = serde_json::to_string(&scalar_input_set_body(
        input_bytes,
        "text",
        "open",
        false,
        false,
    ))
    .unwrap();
    let input_set_body = duplicate(
        input_set_body,
        r#""state":"open""#,
        r#""state":"open","state":"sealed""#,
    );
    let (server, _credentials, credential_path) = prepared_run(vec![http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        input_set_body.as_bytes(),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "input-set",
            "show",
            ORGANIZATION,
            INPUT_SET_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_invalid_response(&output);
    assert_eq!(server.finish().len(), 1);

    let retained_body = duplicate(
        response_body(retained_inputs_response(input_bytes)),
        r#""kind":"text""#,
        r#""kind":"text","\u006bind":"text""#,
    );
    let (server, _credentials, credential_path) = prepared_run(vec![http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        retained_body.as_bytes(),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "inputs",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_invalid_response(&output);
    assert_eq!(server.finish().len(), 1);

    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("request.txt");
    fs::write(&source, input_bytes).unwrap();
    let upload_body = duplicate(
        response_body(upload_capability_response(
            input_bytes,
            "https://storage.invalid/object?signature=duplicate-upload",
        )),
        r#""contentType":"text/plain; charset=utf-8""#,
        r#""contentType":"text/plain; charset=utf-8","content\u0054ype":"text/plain; charset=utf-8""#,
    );
    let open_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "open",
            false,
            false,
        ))
        .unwrap(),
    );
    let (server, _credentials, credential_path) = prepared_run(vec![
        open_response,
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            upload_body.as_bytes(),
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "input-set",
            "upload",
            ORGANIZATION,
            INPUT_SET_ID,
            "--member-file",
            "inputs/request",
            source.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_invalid_response(&output);
    assert_eq!(server.finish().len(), 2);

    let download_body = duplicate(
        response_body(download_capability_response(
            input_bytes,
            "https://storage.invalid/object?signature=duplicate-download",
        )),
        r#""displayName":null"#,
        r#""displayName":null,"display\u004eame":null"#,
    );
    let destination = directory.path().join("duplicate-download");
    let (server, _credentials, credential_path) = prepared_run(vec![
        retained_inputs_response(input_bytes),
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            download_body.as_bytes(),
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "inputs",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            destination.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_invalid_response(&output);
    assert!(!destination.exists());
    assert_eq!(server.finish().len(), 2);
    assert_no_secret_output(&output, &[TOKEN, "duplicate-upload", "duplicate-download"]);
}

#[test]
fn retained_inputs_download_uses_inventory_bound_logical_paths() {
    let input_bytes = b"private retained input bytes\n";
    let storage = OneShotServer::respond("200 OK", Some("text/plain"), input_bytes);
    let signed_url = format!("{}/object?signature=retained-input", storage.api_url);
    let (download_server, _credentials, credential_path) = prepared_run(vec![
        retained_inputs_response(input_bytes),
        download_capability_response(input_bytes, &signed_url),
    ]);
    let environment = deployment_environment(&download_server.api_url, &credential_path);
    let destination_parent = tempfile::tempdir().unwrap();
    let destination = destination_parent.path().join("inputs-download");

    let output = run_with_env(
        &[
            "run",
            "inputs",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            destination.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read(destination.join("inputs/request")).unwrap(),
        input_bytes
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "downloaded");
    assert_eq!(result["inputSetId"], INPUT_SET_ID);
    assert_eq!(result["memberCount"], 1);
    let requests = download_server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains(&format!("/runs/{RUN_ID}/inputs HTTP/1.1")));
    assert_eq!(
        request_body(&requests[1]),
        serde_json::json!({"members": ["inputs/request"]})
    );
    let storage_request = storage.finish();
    assert!(storage_request.starts_with("GET /api/object?signature="));
    assert!(!storage_request.contains("authorization:"));
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            "retained-input",
            std::str::from_utf8(input_bytes).unwrap(),
        ],
    );
}

#[test]
fn retained_inputs_download_selects_exact_members_only() {
    let first = b"unselected private bytes";
    let second = b"selected private bytes";
    let manifest = serde_json::json!({
        "schemaVersion": 1,
        "inputs": {
            "first": {
                "kind": "text",
                "sizeBytes": first.len(),
                "sha256": hex_digest(first)
            },
            "second": {
                "kind": "text",
                "sizeBytes": second.len(),
                "sha256": hex_digest(second)
            }
        }
    });
    let storage = OneShotServer::respond("200 OK", Some("text/plain"), second);
    let signed_url = format!(
        "{}/second?signature=unique-selected-capability-sentinel",
        storage.api_url
    );
    let capabilities = download_capability_response_for_members(serde_json::json!([{
        "memberId": "inputs/second",
        "attachmentIndex": null,
        "displayName": null,
        "mediaType": "text/plain; charset=utf-8",
        "sizeBytes": second.len(),
        "sha256": hex_digest(second),
        "url": signed_url
    }]));
    let (server, _credentials, credential_path) = prepared_run(vec![
        retained_inputs_response_for_manifest(&manifest, 2, 0, first.len() + second.len()),
        capabilities,
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let destination_parent = tempfile::tempdir().unwrap();
    let destination = destination_parent.path().join("selected-inputs");

    let output = run_with_env(
        &[
            "run",
            "inputs",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            destination.to_str().unwrap(),
            "--member",
            "inputs/second",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fs::read(destination.join("inputs/second")).unwrap(), second);
    assert!(!destination.join("inputs/first").exists());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["memberCount"], 1);
    assert_eq!(receipt["totalSizeBytes"], second.len());
    let requests = server.finish();
    assert_eq!(
        request_body(&requests[1]),
        serde_json::json!({"members": ["inputs/second"]})
    );
    storage.finish();
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            "unique-selected-capability-sentinel",
            std::str::from_utf8(first).unwrap(),
            std::str::from_utf8(second).unwrap(),
        ],
    );
}

#[test]
fn retained_inputs_download_accepts_a_relative_destination() {
    let input_bytes = b"relative destination bytes";
    let storage = OneShotServer::respond("200 OK", Some("text/plain"), input_bytes);
    let signed_url = format!("{}/object?signature=relative", storage.api_url);
    let (server, _credentials, credential_path) = prepared_run(vec![
        retained_inputs_response(input_bytes),
        download_capability_response(input_bytes, &signed_url),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let current_directory = tempfile::tempdir().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args([
            "run",
            "inputs",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            "retained-inputs",
            "--json",
            "--allow-insecure-http",
        ])
        .current_dir(current_directory.path())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }

    let output = command.output().unwrap();

    assert!(output.status.success());
    assert_eq!(
        fs::read(
            current_directory
                .path()
                .join("retained-inputs/inputs/request")
        )
        .unwrap(),
        input_bytes
    );
    server.finish();
    storage.finish();
}

#[cfg(target_os = "linux")]
#[test]
fn retained_inputs_json_download_rejects_an_unrepresentable_destination_before_transfer() {
    let (server, _credentials, credential_path) = prepared_run(Vec::new());
    let environment = deployment_environment(&server.api_url, &credential_path);
    let destination_parent = tempfile::tempdir().unwrap();
    let destination = destination_parent
        .path()
        .join(std::ffi::OsString::from_vec(b"download-\xff".to_vec()));
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args([
            "run",
            "inputs",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
        ])
        .arg(&destination)
        .args(["--json", "--allow-insecure-http"])
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }

    let output = command.output().unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!destination.exists());
    assert!(server.finish().is_empty());
}

#[test]
fn retained_input_integrity_failure_leaves_no_destination() {
    let expected = b"expected retained bytes";
    let storage = OneShotServer::respond("200 OK", Some("text/plain"), b"different retained bytes");
    let signed_url = format!("{}/object?signature=integrity-mismatch", storage.api_url);
    let (server, _credentials, credential_path) = prepared_run(vec![
        retained_inputs_response(expected),
        download_capability_response(expected, &signed_url),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let destination_parent = tempfile::tempdir().unwrap();
    let destination = destination_parent.path().join("must-not-exist");

    let output = run_with_env(
        &[
            "run",
            "inputs",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            destination.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(!destination.exists());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "integrity_mismatch");
    assert_no_secret_output(&output, &[TOKEN, "integrity-mismatch"]);
    server.finish();
    storage.finish();
}

#[cfg(target_os = "linux")]
#[test]
fn interrupted_retained_input_download_removes_verified_private_staging() {
    let first = b"already downloaded private input\n";
    let second = b"paused private input\n";
    let manifest = serde_json::json!({
        "schemaVersion": 1,
        "inputs": {
            "first": {
                "kind": "text",
                "sizeBytes": first.len(),
                "sha256": hex_digest(first)
            },
            "second": {
                "kind": "text",
                "sizeBytes": second.len(),
                "sha256": hex_digest(second)
            }
        }
    });
    let first_storage = OneShotServer::respond("200 OK", Some("text/plain"), first);
    let mut second_storage =
        ScriptedServer::respond_with_paused_first_response(vec![http_response_with_headers(
            "200 OK",
            Some("text/plain"),
            &[],
            second,
        )]);
    let first_url = format!("{}/first?signature=first-private", first_storage.api_url);
    let second_url = format!(
        "{}/second?signature=interrupted-retained-input",
        second_storage.api_url
    );
    let capabilities = download_capability_response_for_members(serde_json::json!([
        {
            "memberId": "inputs/first",
            "attachmentIndex": null,
            "displayName": null,
            "mediaType": "text/plain; charset=utf-8",
            "sizeBytes": first.len(),
            "sha256": hex_digest(first),
            "url": first_url
        },
        {
            "memberId": "inputs/second",
            "attachmentIndex": null,
            "displayName": null,
            "mediaType": "text/plain; charset=utf-8",
            "sizeBytes": second.len(),
            "sha256": hex_digest(second),
            "url": second_url
        }
    ]));
    let (server, _credentials, credential_path) = prepared_run(vec![
        retained_inputs_response_for_manifest(&manifest, 2, 0, first.len() + second.len()),
        capabilities,
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let destination_parent = tempfile::tempdir().unwrap();
    let destination = destination_parent.path().join("must-not-be-committed");
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args([
            "run",
            "inputs",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            destination.to_str().unwrap(),
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
    let request = second_storage.next_request();
    assert!(request.starts_with("GET /api/second?signature="));

    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    second_storage.release_paused_response();

    assert_eq!(output.status.code(), Some(130));
    assert!(output.stderr.is_empty());
    assert!(!destination.exists());
    assert!(
        fs::read_dir(destination_parent.path())
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".scherzo-input-download-"))
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "interrupted");
    assert_eq!(result["runId"], RUN_ID);
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            "first-private",
            "interrupted-retained-input",
            std::str::from_utf8(first).unwrap(),
            std::str::from_utf8(second).unwrap(),
        ],
    );
    assert_eq!(server.finish().len(), 2);
    first_storage.finish();
    second_storage.finish();
}

#[test]
fn retained_input_deletion_requires_confirmation_and_sends_one_idempotent_request() {
    let unconfirmed = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args(["run", "inputs", "delete", ORGANIZATION, RUN_ID])
        .output()
        .unwrap();
    assert_eq!(unconfirmed.status.code(), Some(2));

    let response = http_response_with_headers(
        "204 No Content",
        None,
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        b"",
    );
    let (server, _credentials, credential_path) = prepared_run(vec![response]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "inputs",
            "delete",
            ORGANIZATION,
            RUN_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "deleted"
    );
    let request = server.finish().remove(0);
    assert!(request.starts_with(&format!(
        "DELETE /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID}/inputs HTTP/1.1"
    )));
    assert_eq!(header_value(&request, "idempotency-key").len(), 64);
}

#[test]
fn named_file_parameter_controls_stop_before_cloud_input_set_allocation() {
    let input_directory = tempfile::tempdir().unwrap();
    let input_path = input_directory.path().join("request.bin");
    fs::write(&input_path, b"private file input").unwrap();
    let input_path = input_path.to_str().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(&credential_path, &api_url, TOKEN, "2999-01-01T00:00:00Z");
    let environment = deployment_environment(&api_url, credential_path.to_str().unwrap());

    for control in ['\u{000b}', '\u{000c}', '\u{007f}'] {
        let media_type = format!("application/octet-stream;version=one{control}two");
        let output = run_with_env(
            &create_args_with_file_input("request", &media_type, input_path, true),
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        assert!(output.stderr.is_empty());
        let failure: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(failure["outcome"], "invalid_input");
        assert_no_secret_output(&output, &[TOKEN, "private file input"]);
    }
    assert!(matches!(
        listener.accept(),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    ));
}

#[test]
fn invalid_json_and_scalar_binding_conflicts_stop_before_cloud_access() {
    let input_directory = tempfile::tempdir().unwrap();
    let text_path = input_directory.path().join("request.txt");
    fs::write(&text_path, b"private text input").unwrap();
    let text_path = text_path.to_str().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(&credential_path, &api_url, TOKEN, "2999-01-01T00:00:00Z");
    let environment = deployment_environment(&api_url, credential_path.to_str().unwrap());

    let malformed = run_with_env(
        &create_args_with_scalar_input(
            "--input-json",
            "request",
            "{\"secret\":1,\"secret\":2}",
            true,
        ),
        &environment,
    );
    assert_eq!(malformed.status.code(), Some(1));
    assert_no_secret_output(&malformed, &[TOKEN, "secret"]);

    let malformed_file = run_with_env(
        &create_args_with_file_input("request", "not a media type", text_path, true),
        &environment,
    );
    assert_eq!(malformed_file.status.code(), Some(1));
    assert_no_secret_output(&malformed_file, &[TOKEN, "private text input"]);

    let mut conflicting = create_args_with_text_input("request", text_path, true);
    let insertion = conflicting.len() - 1;
    conflicting.splice(insertion..insertion, ["--input-json", "request", "null"]);
    let conflict = run_with_env(&conflicting, &environment);
    assert_eq!(conflict.status.code(), Some(1));
    assert_no_secret_output(&conflict, &[TOKEN, "private text input"]);
    assert!(matches!(
        listener.accept(),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    ));
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
    let directory = tempfile::tempdir().unwrap();
    let context_path = directory.path().join("context.json");
    fs::write(&context_path, br#"{"issueId":"retry-context"}"#).unwrap();
    let (server, _credential_directory, credential_path) =
        prepared_run(vec![Vec::new(), acceptance_response(true)]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let mut arguments = create_args(true);
    let insertion = arguments.len() - 1;
    arguments.splice(
        insertion..insertion,
        ["--integration-context-file", context_path.to_str().unwrap()],
    );

    let output = run_with_env(&arguments, &environment);

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
fn text_input_sequence_refreshes_authority_but_does_not_treat_network_failure_as_upload_evidence() {
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

    assert_eq!(output.status.code(), Some(4));
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["outcome"], "unreachable");
    assert_eq!(receipt["inputSetId"], INPUT_SET_ID);
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
    assert_eq!(requests.len(), 4);
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
            let issue_context = stdout
                .find("issueId: issue-private-context-sentinel")
                .expect("plain projection should expose the complete integration context");
            let source_context = stdout
                .find("source: linear")
                .expect("plain projection should expose the complete integration context");
            assert!(
                issue_context < source_context,
                "integration context should be sorted"
            );
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
fn signalled_input_set_create_reports_unknown_without_an_invented_id() {
    let input_text = "input set interrupted before its identifier is known";
    let input_bytes = input_text.as_bytes();
    let input_directory = tempfile::tempdir().unwrap();
    let input_path = input_directory.path().join("request.txt");
    fs::write(&input_path, input_bytes).unwrap();
    let mut server =
        ScriptedServer::respond_with_paused_first_response(vec![create_input_set_response(
            input_bytes,
            false,
        )]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment(&server.api_url, credential_path.to_str().unwrap());
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args([
            "run",
            "input-set",
            "create",
            ORGANIZATION,
            "--project-id",
            PROJECT_ID,
            "--input-text-file",
            "request",
            input_path.to_str().unwrap(),
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
    assert!(server.next_request().contains("/run-input-sets HTTP/1.1"));

    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    server.release_paused_response();

    assert_eq!(output.status.code(), Some(130));
    assert!(output.stderr.is_empty());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["outcome"], "unknown");
    assert_eq!(receipt["commitment"], "unknown");
    assert_eq!(receipt["organizationRef"], ORGANIZATION);
    assert_eq!(receipt["resourceKind"], "input set");
    assert!(receipt.get("resourceId").is_none());
    assert_no_secret_output(&output, &[TOKEN, input_text]);
    assert!(server.finish().is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn signalled_input_set_seal_reports_one_unknown_receipt() {
    let input_bytes = b"sealed input";
    let mut server = ScriptedServer::respond_with_paused_last_response(vec![
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            &serde_json::to_vec(&scalar_input_set_body(
                input_bytes,
                "text",
                "open",
                true,
                false,
            ))
            .unwrap(),
        ),
        seal_input_set_response(input_bytes, false),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment(&server.api_url, credential_path.to_str().unwrap());
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args([
            "run",
            "input-set",
            "seal",
            ORGANIZATION,
            INPUT_SET_ID,
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
    assert!(
        server
            .next_request()
            .contains(&format!("/run-input-sets/{INPUT_SET_ID} HTTP/1.1"))
    );
    assert!(
        server
            .next_request()
            .contains(&format!("/run-input-sets/{INPUT_SET_ID}/seal HTTP/1.1"))
    );

    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    server.release_paused_response();

    assert_eq!(output.status.code(), Some(130));
    assert!(output.stderr.is_empty());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["outcome"], "unknown");
    assert_eq!(receipt["commitment"], "unknown");
    assert_eq!(receipt["resourceKind"], "input set");
    assert_eq!(receipt["resourceId"], INPUT_SET_ID);
    assert!(server.finish().is_empty());
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
    assert!(output.stderr.is_empty());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "input_set_incomplete");
    assert_eq!(result["inputSetId"], INPUT_SET_ID);
    assert_eq!(result["organizationRef"], ORGANIZATION);
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
