use super::*;

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
    serde_json::json!({
        "id": RUN_ID,
        "organizationId": ORGANIZATION_ID,
        "projectId": PROJECT_ID,
        "displayName": "Release checks",
        "executionSpecId": EXECUTION_SPEC_ID,
        "state": "running",
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
            "promptPresent": true,
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
