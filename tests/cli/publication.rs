use super::*;

const TOKEN: &str = "unique-publication-access-token-sentinel";
const REFRESHED_TOKEN: &str = "unique-publication-refreshed-token-sentinel";
const ORGANIZATION: &str = "acme-research";
const ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
const PROJECT_ID: &str = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
const RUN_ID: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
const ARTIFACT_SET_ID: &str = "ats_01k0z6r1w8f4jy2m7q9v3x5abc";
const PUBLICATION_ID: &str = "pub_01k0z6r1w8f4jy2m7q9v3x5abc";
const PRINCIPAL_ID: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5abc";
const REPOSITORY_CONNECTION_ID: &str = "rpc_01k0z6r1w8f4jy2m7q9v3x5abc";
const EXPORT_NAME: &str = "changes";
const CALLER_KEY: &str = "caller-publication-key/unchanged";

fn prepared_publication(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, String) {
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

fn publication_body() -> serde_json::Value {
    serde_json::json!({
        "id": PUBLICATION_ID,
        "organizationId": ORGANIZATION_ID,
        "projectId": PROJECT_ID,
        "runId": RUN_ID,
        "artifactSetId": ARTIFACT_SET_ID,
        "exportName": EXPORT_NAME,
        "state": "queued",
        "version": 1,
        "artifact": {
            "artifactVersion": 1,
            "objectFormat": "sha1",
            "baseOid": "0123456789abcdef0123456789abcdef01234567",
            "headOid": "89abcdef0123456789abcdef0123456789abcdef",
            "treeOid": "fedcba9876543210fedcba9876543210fedcba98",
            "expiresAt": "2026-10-03T18:00:00Z"
        },
        "target": {
            "repositoryConnectionId": REPOSITORY_CONNECTION_ID,
            "providerRepositoryId": "123456",
            "fullName": "scherzo-systems/scherzo-cloud",
            "baseBranch": "main",
            "destinationBranch": format!("scherzo/{RUN_ID}/{EXPORT_NAME}")
        },
        "pullRequestMetadata": {
            "title": "Scherzo Cloud run: changes",
            "body": format!("Run: {RUN_ID}\nExport: {EXPORT_NAME}\n<!-- publication-marker -->")
        },
        "branch": null,
        "pullRequest": null,
        "outcome": null,
        "failure": null,
        "actorPrincipalId": PRINCIPAL_ID,
        "createdAt": "2026-09-03T18:00:00Z",
        "updatedAt": "2026-09-03T18:00:00Z",
        "startedAt": null,
        "terminalAt": null
    })
}

fn accepted_response(body: &serde_json::Value) -> Vec<u8> {
    publication_response(
        "202 Accepted",
        body,
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc/publications/pub_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
            ("Cache-Control", "private, no-store"),
        ],
    )
}

fn publication_response(
    status: &str,
    body: &serde_json::Value,
    headers: &[(&str, &str)],
) -> Vec<u8> {
    http_response_with_headers(
        status,
        Some("application/json"),
        headers,
        &serde_json::to_vec(body).unwrap(),
    )
}

fn create_args(json: bool, caller_key: bool) -> Vec<&'static str> {
    let mut args = vec![
        "publication",
        "create",
        ORGANIZATION,
        RUN_ID,
        "--export",
        EXPORT_NAME,
    ];
    if caller_key {
        args.extend(["--idempotency-key", CALLER_KEY]);
    }
    if json {
        args.push("--json");
    }
    args.push("--allow-insecure-http");
    args
}

fn assert_one_json_document(bytes: &[u8]) -> serde_json::Value {
    let mut documents =
        serde_json::Deserializer::from_slice(bytes).into_iter::<serde_json::Value>();
    let document = documents.next().unwrap().unwrap();
    assert!(
        documents.next().is_none(),
        "stdout should contain one JSON document"
    );
    document
}

fn assert_no_publication_secret(output: &Output, secrets: &[&str]) {
    let mut bytes = output.stdout.clone();
    bytes.extend_from_slice(&output.stderr);
    let text = String::from_utf8_lossy(&bytes);
    for secret in secrets {
        assert!(!text.contains(secret), "output exposed sentinel {secret:?}");
    }
}

#[test]
fn publication_create_sends_the_closed_request_and_renders_plain_and_json_receipts() {
    for json in [false, true] {
        let body = publication_body();
        let (server, _directory, credential_path) =
            prepared_publication(vec![accepted_response(&body)]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(&create_args(json, true), &environment);

        assert!(
            output.status.success(),
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        if json {
            assert_eq!(
                assert_one_json_document(&output.stdout),
                serde_json::json!({
                    "schemaVersion": 1,
                    "deployment": server.api_url,
                    "idempotencyKey": CALLER_KEY,
                    "publication": body
                })
            );
        } else {
            let stdout = String::from_utf8(output.stdout.clone()).unwrap();
            for field in [
                format!("publication: {PUBLICATION_ID}"),
                format!("run: {RUN_ID}"),
                format!("export: {EXPORT_NAME}"),
                "state: queued".to_owned(),
                "repository: scherzo-systems/scherzo-cloud".to_owned(),
                format!("destination branch: scherzo/{RUN_ID}/{EXPORT_NAME}"),
                format!("idempotency key: {CALLER_KEY}"),
            ] {
                assert!(
                    stdout.lines().any(|line| line == field),
                    "missing {field:?}: {stdout}"
                );
            }
        }
        assert_no_publication_secret(&output, &[TOKEN]);

        let request = server.finish().remove(0);
        assert!(request.starts_with(&format!(
            "POST /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID}/publications HTTP/1.1\r\n"
        )));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(request.split_once("\r\n\r\n").unwrap().1)
                .unwrap(),
            serde_json::json!({"exportName": EXPORT_NAME})
        );
        assert_eq!(header_value(&request, "idempotency-key"), CALLER_KEY);
        assert_eq!(
            header_value(&request, "accept"),
            "application/json, application/problem+json"
        );
        assert_eq!(header_value(&request, "content-type"), "application/json");
        assert_eq!(
            header_value(&request, "authorization"),
            format!("Bearer {TOKEN}")
        );
        assert!(!request.contains("targetBranch"));
        assert!(!request.contains("pullRequest"));
    }
}

#[test]
fn generated_key_is_reused_across_authentication_and_transport_retries() {
    let mut replayed = publication_body();
    replayed["state"] = serde_json::json!("running");
    replayed["version"] = serde_json::json!(2);
    replayed["updatedAt"] = serde_json::json!("2026-09-03T18:01:00Z");
    replayed["startedAt"] = serde_json::json!("2026-09-03T18:01:00Z");
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
                "refresh_token": "unique-publication-refreshed-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        Vec::new(),
        accepted_response(&replayed),
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

    let output = run_with_env(&create_args(true, false), &environment);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt = assert_one_json_document(&output.stdout);
    let effective_key = receipt["idempotencyKey"].as_str().unwrap();
    assert_eq!(effective_key.len(), 64);
    assert_eq!(receipt["publication"]["state"], "running");
    assert_no_publication_secret(&output, &[TOKEN, REFRESHED_TOKEN]);

    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    let api_requests = [&requests[0], &requests[2], &requests[3]];
    for request in api_requests {
        assert_eq!(header_value(request, "idempotency-key"), effective_key);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(request.split_once("\r\n\r\n").unwrap().1)
                .unwrap(),
            serde_json::json!({"exportName": EXPORT_NAME})
        );
    }
    assert_eq!(
        header_value(&requests[3], "authorization"),
        format!("Bearer {REFRESHED_TOKEN}")
    );
}

#[test]
fn publication_create_rejects_malformed_success_without_partial_success_output() {
    let body = publication_body();
    let mut unknown_field = body.clone();
    unknown_field["providerResponse"] = serde_json::json!("unique-provider-secret-sentinel");
    let mut invalid_lifecycle = body.clone();
    invalid_lifecycle["startedAt"] = serde_json::json!("2026-09-03T18:00:00Z");
    let cases = vec![
        publication_response(
            "200 OK",
            &body,
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                ("Cache-Control", "private, no-store"),
            ],
        ),
        publication_response(
            "202 Accepted",
            &body,
            &[
                ("Idempotency-Key", "different-key"),
                (
                    "Location",
                    "/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc/publications/pub_01k0z6r1w8f4jy2m7q9v3x5abc",
                ),
                ("Cache-Control", "private, no-store"),
            ],
        ),
        publication_response(
            "202 Accepted",
            &body,
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    "/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc/publications/pub_01k0z6r1w8f4jy2m7q9v3x5abd",
                ),
                ("Cache-Control", "private, no-store"),
            ],
        ),
        publication_response(
            "202 Accepted",
            &body,
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    "/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc/publications/pub_01k0z6r1w8f4jy2m7q9v3x5abc",
                ),
            ],
        ),
        accepted_response(&unknown_field),
        accepted_response(&invalid_lifecycle),
    ];

    for response in cases {
        let (server, _directory, credential_path) = prepared_publication(vec![response]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(&create_args(true, true), &environment);

        assert_eq!(output.status.code(), Some(1));
        assert!(output.stderr.is_empty());
        let result = assert_one_json_document(&output.stdout);
        assert_eq!(result["outcome"], "invalid_response");
        assert_eq!(result["idempotencyKey"], CALLER_KEY);
        assert!(result.get("publication").is_none());
        assert_no_publication_secret(
            &output,
            &[TOKEN, PUBLICATION_ID, "unique-provider-secret-sentinel"],
        );
        server.finish();
    }
}

#[test]
fn publication_failures_use_registered_exits_and_redact_response_details() {
    let unauthenticated = run(&create_args(true, true));
    assert_eq!(unauthenticated.status.code(), Some(3));
    assert_eq!(
        assert_one_json_document(&unauthenticated.stdout)["outcome"],
        "unauthenticated"
    );

    let signed_url = "https://storage.example.test/object?X-Amz-Signature=unique-signature";
    let (server, _directory, credential_path) = prepared_publication(vec![problem_http_response(
        "403 Forbidden",
        serde_json::json!({
            "type": "https://api.scherzo.dev/problems/forbidden",
            "title": "Forbidden",
            "status": 403,
            "detail": signed_url
        }),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let forbidden = run_with_env(&create_args(true, true), &environment);

    assert_eq!(forbidden.status.code(), Some(1));
    assert_eq!(
        assert_one_json_document(&forbidden.stdout)["outcome"],
        "forbidden"
    );
    assert_no_publication_secret(&forbidden, &[TOKEN, signed_url, "unique-signature"]);
    server.finish();

    let (server, _directory, credential_path) =
        prepared_publication(vec![http_response_with_headers(
            "503 Service Unavailable",
            Some("application/problem+json"),
            &[("Retry-After", "1")],
            &serde_json::to_vec(&serde_json::json!({
                "type": "https://api.scherzo.dev/problems/retryable-conflict",
                "title": "Retryable conflict",
                "status": 503
            }))
            .unwrap(),
        )]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let unavailable = run_with_env(&create_args(true, true), &environment);

    assert_eq!(unavailable.status.code(), Some(4));
    let result = assert_one_json_document(&unavailable.stdout);
    assert_eq!(result["outcome"], "unreachable");
    assert_eq!(result["category"], "server");
    assert_no_publication_secret(&unavailable, &[TOKEN]);
    server.finish();
}

#[test]
fn ambiguous_creation_preserves_the_generated_key_when_refresh_loses_authentication() {
    for json in [false, true] {
        let server = ScriptedServer::respond(vec![
            Vec::new(),
            problem_http_response(
                "401 Unauthorized",
                serde_json::json!({
                    "type": "https://api.scherzo.dev/problems/unauthorized",
                    "title": "Unauthorized",
                    "status": 401
                }),
            ),
            json_http_response(
                "400 Bad Request",
                serde_json::json!({"error": "invalid_grant"}),
            ),
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

        let output = run_with_env(&create_args(json, false), &environment);

        assert_eq!(output.status.code(), Some(3));
        let effective_key = if json {
            assert!(output.stderr.is_empty());
            let result = assert_one_json_document(&output.stdout);
            assert_eq!(result["outcome"], "unauthenticated");
            result["idempotencyKey"].as_str().unwrap().to_owned()
        } else {
            assert!(output.stdout.is_empty());
            String::from_utf8_lossy(&output.stderr)
                .lines()
                .find_map(|line| line.strip_prefix("idempotency key: "))
                .expect("plain authentication recovery should expose the effective key")
                .to_owned()
        };
        assert_eq!(effective_key.len(), 64);
        assert_no_publication_secret(&output, &[TOKEN, "unique-fixture-refresh-token"]);

        let requests = server.finish();
        assert_eq!(requests.len(), 3);
        for request in &requests[..2] {
            assert!(request.starts_with("POST /api/v1/organizations/"));
            assert_eq!(header_value(request, "idempotency-key"), effective_key);
        }
        assert!(requests[2].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    }
}

#[test]
fn post_dispatch_session_protocol_failure_emits_one_recovery_document() {
    let server = ScriptedServer::respond(vec![
        Vec::new(),
        problem_http_response(
            "401 Unauthorized",
            serde_json::json!({
                "type": "https://api.scherzo.dev/problems/unauthorized",
                "title": "Unauthorized",
                "status": 401
            }),
        ),
        json_http_response("200 OK", serde_json::json!({})),
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

    let output = run_with_env(&create_args(true, false), &environment);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let result = assert_one_json_document(&output.stdout);
    assert_eq!(result["outcome"], "unknown");
    assert_eq!(result["commitment"], "unknown");
    let effective_key = result["idempotencyKey"].as_str().unwrap();
    assert_eq!(effective_key.len(), 64);
    assert_no_publication_secret(&output, &[TOKEN, "unique-fixture-refresh-token"]);

    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert_eq!(header_value(&requests[0], "idempotency-key"), effective_key);
    assert_eq!(header_value(&requests[1], "idempotency-key"), effective_key);
    assert!(requests[2].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
}

#[cfg(target_os = "linux")]
#[test]
fn signal_during_session_acquisition_emits_no_false_publication_receipt() {
    let mut server = ScriptedServer::respond_with_paused_first_response(vec![json_http_response(
        "200 OK",
        serde_json::json!({
            "access_token": REFRESHED_TOKEN,
            "refresh_token": "unique-predispatch-refreshed-token",
            "token_type": "Bearer",
            "expires_in": 3600
        }),
    )]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2000-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args(create_args(true, false))
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
    let refresh = server.next_request();
    assert!(refresh.starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));

    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    server.release_paused_response();

    assert_eq!(output.status.code(), Some(130));
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
    assert_no_publication_secret(
        &output,
        &[TOKEN, REFRESHED_TOKEN, "unique-predispatch-refreshed-token"],
    );
    assert!(server.finish().is_empty());
}
