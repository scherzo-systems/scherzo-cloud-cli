use super::*;

const TOKEN: &str = "unique-organization-command-token-sentinel";

fn prepared_organization(
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
    write_credential_fixture(
        &credential_path,
        &server.api_url,
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

fn organization_body() -> serde_json::Value {
    serde_json::json!({
        "id": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
        "state": "active",
        "displayName": "Acme Research",
        "slug": "acme-research",
        "createdAt": "2026-07-22T20:32:00Z",
        "updatedAt": "2026-07-22T20:32:00Z",
        "future": { "accepted": true }
    })
}

fn organization_success(status: &str) -> Vec<u8> {
    if status == "201 Created" {
        http_response_with_headers(
            status,
            Some("application/json"),
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    "/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc",
                ),
            ],
            &serde_json::to_vec(&organization_body()).unwrap(),
        )
    } else {
        json_http_response(status, organization_body())
    }
}

fn update_success() -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&organization_body()).unwrap(),
    )
}

fn membership_success(items: serde_json::Value, next_cursor: Option<&str>) -> Vec<u8> {
    let mut page = serde_json::json!({ "items": items });
    if let Some(next_cursor) = next_cursor {
        page["nextCursor"] = serde_json::Value::String(next_cursor.to_owned());
    }
    json_http_response("200 OK", page)
}

fn organization_problem(status_text: &str, status: u16, problem_type: &str) -> Vec<u8> {
    problem_http_response(
        status_text,
        serde_json::json!({
            "type": problem_type,
            "title": "organization-problem-title-sentinel",
            "status": status,
            "detail": "organization-problem-detail-sentinel"
        }),
    )
}

fn response_with_detail(detail: &str) -> Vec<u8> {
    problem_http_response(
        "404 Not Found",
        serde_json::json!({
            "type": "https://api.scherzo.dev/problems/not-found",
            "title": "Private target",
            "status": 404,
            "detail": detail
        }),
    )
}

fn request_body(request: &str) -> &str {
    request
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .expect("request should contain a body separator")
}

#[test]
fn organization_help_is_contextual_and_side_effect_free() {
    let bare = run_with_env(
        &["organization"],
        &[("SCHERZO_CLOUD_API_URL", "partial-override-is-ignored")],
    );
    assert!(bare.status.success());
    assert!(bare.stderr.is_empty());
    let bare_help = String::from_utf8_lossy(&bare.stdout);
    assert!(bare_help.contains("Usage: scherzo-cloud organization [COMMAND]"));
    assert!(bare_help.contains("create   Create a Scherzo Cloud organization"));
    assert!(bare_help.contains("show     Show a Scherzo Cloud organization"));
    assert!(bare_help.contains("update   Update a Scherzo Cloud organization"));
    assert!(bare_help.contains("members  Manage Scherzo Cloud organization members"));

    let members = run_with_env(
        &["organization", "members"],
        &[("SCHERZO_CLOUD_API_URL", "partial-override-is-ignored")],
    );
    assert!(members.status.success());
    assert!(members.stderr.is_empty());
    assert!(
        String::from_utf8_lossy(&members.stdout)
            .contains("Usage: scherzo-cloud organization members [COMMAND]")
    );

    for (args, expected) in [
        (
            &["organization", "create", "--help"][..],
            "--display-name <DISPLAY_NAME>",
        ),
        (&["organization", "show", "--help"][..], "<ORGANIZATION>"),
        (
            &["organization", "update", "--help"][..],
            "--display-name <DISPLAY_NAME>|--slug <SLUG>",
        ),
        (
            &["organization", "members", "list", "--help"][..],
            "--limit <LIMIT>",
        ),
    ] {
        let output = run(args);
        assert!(output.status.success());
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains(expected));
        assert!(stdout.contains("--json"));
        assert!(stdout.contains("--allow-insecure-http"));
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn organization_has_no_alias_and_create_requires_a_display_name() {
    for args in [
        &["org", "show", "acme"][..],
        &["organization", "create", "--json"][..],
    ] {
        let output = run(args);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn human_create_has_exact_output_and_request_contract() {
    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![organization_success("201 Created")], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "create",
            "--display-name",
            "\u{2003}Acme Research\u{2003}",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!(
            concat!(
                "✓ Organization created.\n\n",
                "  Organization: org_01k0z6r1w8f4jy2m7q9v3x5abc\n",
                "  Name:         Acme Research\n",
                "  Slug:         acme-research\n",
                "  State:        active\n",
                "  Deployment:   {}\n"
            ),
            server.api_url
        )
    );
    assert!(output.stderr.is_empty());

    let request = server.finish().pop().unwrap();
    assert!(request.starts_with("POST /api/v1/organizations HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {TOKEN}")
    );
    assert_eq!(header_value(&request, "content-type"), "application/json");
    assert_eq!(
        header_value(&request, "accept"),
        "application/json, application/problem+json"
    );
    let key = header_value(&request, "idempotency-key");
    assert_eq!(key.len(), 64);
    assert!(
        key.bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&request)).unwrap(),
        serde_json::json!({"displayName": "\u{2003}Acme Research\u{2003}"})
    );
}

#[test]
fn json_create_reports_schema_one_and_supplied_slug() {
    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![organization_success("201 Created")], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "create",
            "--display-name",
            "Acme Research",
            "--slug",
            "acme-research",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "created",
            "organization": {
                "id": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
                "state": "active",
                "displayName": "Acme Research",
                "slug": "acme-research",
                "createdAt": "2026-07-22T20:32:00Z",
                "updatedAt": "2026-07-22T20:32:00Z"
            }
        })
    );
    assert!(output.stdout.ends_with(b"\n"));
    assert!(output.stderr.is_empty());
    let request = server.finish().pop().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&request)).unwrap(),
        serde_json::json!({
            "displayName": "Acme Research",
            "slug": "acme-research"
        })
    );
}

#[test]
fn create_retries_one_ambiguous_failure_with_the_same_complete_request() {
    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![Vec::new(), organization_success("201 Created")], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "create",
            "--display-name",
            "Acme Research",
            "--slug",
            "acme-research",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
}

#[test]
fn create_expected_outcomes_have_exact_json_and_exit_statuses() {
    let cases = [
        (
            organization_problem(
                "400 Bad Request",
                400,
                "https://api.scherzo.dev/problems/bad-request",
            ),
            "invalid_input",
            1,
            None,
        ),
        (
            organization_problem(
                "401 Unauthorized",
                401,
                "https://api.scherzo.dev/problems/unauthorized",
            ),
            "unauthenticated",
            3,
            None,
        ),
        (
            organization_problem(
                "403 Forbidden",
                403,
                "https://api.scherzo.dev/problems/forbidden",
            ),
            "forbidden",
            1,
            None,
        ),
        (
            organization_problem(
                "403 Forbidden",
                403,
                "https://api.scherzo.dev/problems/organization-creation-not-permitted",
            ),
            "creation_not_permitted",
            1,
            None,
        ),
        (
            organization_problem(
                "409 Conflict",
                409,
                "https://api.scherzo.dev/problems/slug-unavailable",
            ),
            "slug_unavailable",
            1,
            None,
        ),
        (
            organization_problem(
                "409 Conflict",
                409,
                "https://api.scherzo.dev/problems/quantity-limit-reached",
            ),
            "quantity_limit_reached",
            1,
            None,
        ),
        (
            organization_problem(
                "409 Conflict",
                409,
                "https://api.scherzo.dev/problems/idempotency-conflict",
            ),
            "idempotency_conflict",
            1,
            None,
        ),
        (
            http_response_with_headers(
                "429 Too Many Requests",
                Some("application/problem+json"),
                &[("Retry-After", "42")],
                &serde_json::to_vec(&serde_json::json!({
                    "type": "https://api.scherzo.dev/problems/rate-limit-exceeded",
                    "title": "organization-problem-title-sentinel",
                    "status": 429,
                    "detail": "organization-problem-detail-sentinel"
                }))
                .unwrap(),
            ),
            "rate_limited",
            4,
            Some(42),
        ),
        (
            http_response("503 Service Unavailable", None, &[]),
            "unreachable",
            4,
            None,
        ),
    ];

    for (response, expected_outcome, expected_status, retry_after) in cases {
        let (server, _directory, _path, credential_path) =
            prepared_organization(vec![response], TOKEN);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &[
                "organization",
                "create",
                "--display-name",
                "Acme Research",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(expected_status));
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["schemaVersion"], 1);
        assert_eq!(value["deployment"], server.api_url);
        assert_eq!(value["outcome"], expected_outcome);
        if let Some(retry_after) = retry_after {
            assert_eq!(value["retryAfter"], retry_after);
        } else {
            assert!(value.get("retryAfter").is_none());
        }
        if expected_outcome == "unreachable" {
            assert_eq!(value["category"], "server");
        } else {
            assert!(value.get("category").is_none());
        }
        assert!(value.get("title").is_none());
        assert!(value.get("detail").is_none());
        assert!(output.stderr.is_empty());
        let requests = server.finish();
        assert_eq!(requests.len(), 1, "explicit responses must not retry");
    }
}

#[test]
fn two_ambiguous_create_failures_report_an_unconfirmed_result() {
    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![Vec::new(), Vec::new()], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "create",
            "--display-name",
            "Acme Research",
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
    assert!(output.stderr.is_empty());
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn malformed_rate_limit_metadata_is_a_protocol_failure() {
    for retry_after in [None, Some("invalid"), Some("+42"), Some("0")] {
        let headers: Vec<(&str, &str)> = retry_after
            .map(|value| vec![("Retry-After", value)])
            .unwrap_or_default();
        let body = serde_json::to_vec(&serde_json::json!({
            "type": "https://api.scherzo.dev/problems/rate-limit-exceeded",
            "title": "rate-title-sentinel",
            "status": 429,
            "detail": "rate-detail-sentinel"
        }))
        .unwrap();
        let response = http_response_with_headers(
            "429 Too Many Requests",
            Some("application/problem+json"),
            &headers,
            &body,
        );
        let (server, _directory, _path, credential_path) =
            prepared_organization(vec![response], TOKEN);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &[
                "organization",
                "create",
                "--display-name",
                "Acme",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("invalid Retry-After"));
        assert!(!stderr.contains("rate-title-sentinel"));
        assert!(!stderr.contains("rate-detail-sentinel"));
        server.finish();
    }
}

#[test]
fn missing_and_expired_credentials_do_not_contact_the_api() {
    for expires_at in [None, Some("2000-01-01T00:00:00Z")] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        let directory = private_credential_directory();
        let credential_path = directory.path().join("credentials.json");
        if let Some(expires_at) = expires_at {
            write_credential_fixture(&credential_path, &api_url, TOKEN, expires_at);
        }
        let credential_path_string = credential_path.to_str().unwrap();
        let environment = deployment_environment(&api_url, credential_path_string);

        let output = run_with_env(
            &["organization", "create", "--display-name", "Acme", "--json"],
            &environment,
        );

        assert_eq!(output.status.code(), Some(3));
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
}

#[test]
fn unauthorized_removes_only_the_rejected_credential_and_forbidden_retains_it() {
    for (response, expected_status, expect_credential) in [
        (
            organization_problem(
                "401 Unauthorized",
                401,
                "https://api.scherzo.dev/problems/unauthorized",
            ),
            3,
            false,
        ),
        (
            http_response(
                "401 Unauthorized",
                Some("application/problem+json"),
                b"not-json",
            ),
            1,
            false,
        ),
        (
            organization_problem(
                "403 Forbidden",
                403,
                "https://api.scherzo.dev/problems/forbidden",
            ),
            1,
            true,
        ),
    ] {
        let (server, _directory, credential_path, credential_path_string) =
            prepared_organization(vec![response], TOKEN);
        let environment = deployment_environment(&server.api_url, &credential_path_string);

        let output = run_with_env(
            &[
                "organization",
                "show",
                "acme",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(expected_status));
        let stored: serde_json::Value =
            serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
        assert_eq!(
            !stored["credentials"].as_array().unwrap().is_empty(),
            expect_credential
        );
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!combined.contains(TOKEN));
        server.finish();
    }
}

#[test]
fn insecure_http_is_rejected_before_the_credential_is_transmitted() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let directory = private_credential_directory();
    let credential_path = directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &api_url,
        "unique-untransmitted-organization-token",
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment(&api_url, credential_path.to_str().unwrap());

    let output = run_with_env(&["organization", "show", "acme", "--json"], &environment);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("uses insecure HTTP"));
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
}

#[test]
fn show_success_preserves_schema_and_encodes_one_reference_segment() {
    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![organization_success("200 OK")], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "show",
            "org/ Mixed Case",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schemaVersion"], 1);
    assert_eq!(value["deployment"], server.api_url);
    assert_eq!(value["outcome"], "found");
    assert_eq!(
        value["organization"],
        serde_json::json!({
            "id": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
            "state": "active",
            "displayName": "Acme Research",
            "slug": "acme-research",
            "createdAt": "2026-07-22T20:32:00Z",
            "updatedAt": "2026-07-22T20:32:00Z"
        })
    );
    assert!(value["organization"].get("future").is_none());
    assert!(output.stdout.ends_with(b"\n"));
    assert!(output.stderr.is_empty());
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with("GET /api/v1/organizations/org%2F%20Mixed%20Case HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {TOKEN}")
    );
    assert!(!request.contains("idempotency-key:"));
}

#[test]
fn human_show_has_exact_success_output() {
    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![organization_success("200 OK")], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "show",
            "acme-research",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!(
            concat!(
                "✓ Organization found.\n\n",
                "  Organization: org_01k0z6r1w8f4jy2m7q9v3x5abc\n",
                "  Name:         Acme Research\n",
                "  Slug:         acme-research\n",
                "  State:        active\n",
                "  Deployment:   {}\n"
            ),
            server.api_url
        )
    );
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn a_rejected_request_does_not_remove_a_concurrently_replaced_credential() {
    let response = organization_problem(
        "401 Unauthorized",
        401,
        "https://api.scherzo.dev/problems/unauthorized",
    );
    let mut server = ScriptedServer::respond_with_paused_last_response(vec![response]);
    let api_url = server.api_url.clone();
    let directory = private_credential_directory();
    let credential_path = directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &api_url,
        "original-organization-token",
        "2999-01-01T00:00:00Z",
    );
    let credential_path_string = credential_path.to_str().unwrap().to_owned();
    let environment = deployment_environment(&api_url, &credential_path_string);

    let output = thread::scope(|scope| {
        let command = scope.spawn(|| {
            run_with_env(
                &[
                    "organization",
                    "show",
                    "acme",
                    "--json",
                    "--allow-insecure-http",
                ],
                &environment,
            )
        });
        let request = server.next_request();
        assert_eq!(
            header_value(&request, "authorization"),
            "Bearer original-organization-token"
        );
        fs::remove_file(&credential_path).unwrap();
        write_credential_fixture(
            &credential_path,
            &api_url,
            "replacement-organization-token",
            "2999-01-01T00:00:00Z",
        );
        server.release_paused_response();
        command.join().unwrap()
    });

    assert_eq!(output.status.code(), Some(3));
    assert!(output.stderr.is_empty());
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert_eq!(
        stored["credentials"][0]["accessToken"],
        "replacement-organization-token"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!combined.contains("original-organization-token"));
    assert!(!combined.contains("replacement-organization-token"));
    server.finish();
}

#[test]
fn show_expected_outcomes_have_exact_json_and_exit_statuses() {
    let cases = [
        (
            organization_problem(
                "400 Bad Request",
                400,
                "https://api.scherzo.dev/problems/bad-request",
            ),
            "invalid_input",
            1,
        ),
        (
            organization_problem(
                "401 Unauthorized",
                401,
                "https://api.scherzo.dev/problems/unauthorized",
            ),
            "unauthenticated",
            3,
        ),
        (
            organization_problem(
                "403 Forbidden",
                403,
                "https://api.scherzo.dev/problems/forbidden",
            ),
            "forbidden",
            1,
        ),
        (
            organization_problem(
                "404 Not Found",
                404,
                "https://api.scherzo.dev/problems/not-found",
            ),
            "not_found",
            1,
        ),
        (
            http_response("500 Internal Server Error", None, &[]),
            "unreachable",
            4,
        ),
    ];

    for (response, expected_outcome, expected_status) in cases {
        let (server, _directory, _path, credential_path) =
            prepared_organization(vec![response], TOKEN);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &[
                "organization",
                "show",
                "acme-research",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(expected_status));
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["schemaVersion"], 1);
        assert_eq!(value["deployment"], server.api_url);
        assert_eq!(value["outcome"], expected_outcome);
        if expected_outcome == "unreachable" {
            assert_eq!(value["category"], "server");
        }
        assert!(value.get("title").is_none());
        assert!(value.get("detail").is_none());
        assert!(output.stderr.is_empty());
        assert_eq!(server.finish().len(), 1);
    }

    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![Vec::new()], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let transport = run_with_env(
        &[
            "organization",
            "show",
            "acme-research",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(transport.status.code(), Some(4));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&transport.stdout).unwrap()["category"],
        "connection"
    );
    assert!(transport.stderr.is_empty());
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn private_not_found_outputs_are_identical_for_all_target_states() {
    for json in [false, true] {
        let responses = [
            "The target is inaccessible.",
            "The target is inactive.",
            "The target is absent.",
        ]
        .map(response_with_detail)
        .into_iter()
        .collect();
        let (server, _directory, _path, credential_path) = prepared_organization(responses, TOKEN);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut outputs = Vec::new();

        for _ in 0..3 {
            let mut args = vec![
                "organization",
                "show",
                "private-target",
                "--allow-insecure-http",
            ];
            if json {
                args.push("--json");
            }

            let output = run_with_env(&args, &environment);
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stderr.is_empty());
            if !json {
                assert_eq!(output.stdout, b"! Organization not found or unavailable.\n");
            }
            outputs.push(output.stdout);
        }
        assert_eq!(outputs[0], outputs[1]);
        assert_eq!(outputs[1], outputs[2]);
        assert_eq!(server.finish().len(), 3);
    }
}

#[test]
fn update_and_members_list_reject_invalid_cli_input_before_deployment_loading() {
    for args in [
        &["organization", "update", "acme"][..],
        &["organization", "members", "list", "acme", "--limit", "0"][..],
        &["organization", "members", "list", "acme", "--limit", "201"][..],
        &["organization", "members", "list", "acme", "--cursor", ""][..],
    ] {
        let output = run_with_env(
            args,
            &[("SCHERZO_CLOUD_API_URL", "partial-override-must-not-load")],
        );
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn update_accepts_each_patch_shape_and_preserves_the_request_contract() {
    let cases = [
        (
            &["--display-name", "\u{2003}Acme Labs\u{2003}"][..],
            serde_json::json!({"displayName": "\u{2003}Acme Labs\u{2003}"}),
        ),
        (
            &["--slug", "Server_Invalid"][..],
            serde_json::json!({"slug": "Server_Invalid"}),
        ),
        (
            &["--display-name", "Acme Labs", "--slug", "acme-labs"][..],
            serde_json::json!({"displayName": "Acme Labs", "slug": "acme-labs"}),
        ),
    ];

    for (profile_args, expected_body) in cases {
        let (server, _directory, _path, credential_path) =
            prepared_organization(vec![update_success()], TOKEN);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut args = vec![
            "organization",
            "update",
            "org/ Mixed Case",
            "--json",
            "--allow-insecure-http",
        ];
        args.extend_from_slice(profile_args);

        let output = run_with_env(&args, &environment);

        assert!(output.status.success());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["schemaVersion"], 1);
        assert_eq!(result["deployment"], server.api_url);
        assert_eq!(result["outcome"], "updated");
        let mut expected_organization = organization_body();
        expected_organization
            .as_object_mut()
            .unwrap()
            .remove("future");
        assert_eq!(result["organization"], expected_organization);
        assert!(output.stdout.ends_with(b"\n"));
        assert!(output.stderr.is_empty());

        let request = server.finish().pop().unwrap();
        assert!(
            request.starts_with("PATCH /api/v1/organizations/org%2F%20Mixed%20Case HTTP/1.1\r\n")
        );
        assert_eq!(
            header_value(&request, "authorization"),
            format!("Bearer {TOKEN}")
        );
        assert_eq!(
            header_value(&request, "content-type"),
            "application/merge-patch+json"
        );
        let key = header_value(&request, "idempotency-key");
        assert_eq!(key.len(), 64);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(request_body(&request)).unwrap(),
            expected_body
        );
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!combined.contains(TOKEN));
        assert!(!combined.contains(key));
    }
}

#[test]
fn human_update_has_exact_success_output() {
    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![update_success()], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "update",
            "acme-research",
            "--display-name",
            "Acme Research",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!(
            concat!(
                "✓ Organization updated.\n\n",
                "  Organization: org_01k0z6r1w8f4jy2m7q9v3x5abc\n",
                "  Name:         Acme Research\n",
                "  Slug:         acme-research\n",
                "  State:        active\n",
                "  Deployment:   {}\n"
            ),
            server.api_url
        )
    );
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn update_retries_one_ambiguous_failure_with_the_same_complete_request() {
    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![Vec::new(), update_success()], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "update",
            "acme-research",
            "--display-name",
            "Acme Labs",
            "--slug",
            "acme-labs",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
}

#[test]
fn update_expected_outcomes_have_exact_json_and_exit_statuses() {
    let cases = [
        (
            organization_problem(
                "400 Bad Request",
                400,
                "https://api.scherzo.dev/problems/bad-request",
            ),
            "invalid_input",
            1,
        ),
        (
            organization_problem(
                "401 Unauthorized",
                401,
                "https://api.scherzo.dev/problems/unauthorized",
            ),
            "unauthenticated",
            3,
        ),
        (
            organization_problem(
                "403 Forbidden",
                403,
                "https://api.scherzo.dev/problems/forbidden",
            ),
            "forbidden",
            1,
        ),
        (
            organization_problem(
                "404 Not Found",
                404,
                "https://api.scherzo.dev/problems/not-found",
            ),
            "not_found",
            1,
        ),
        (
            organization_problem(
                "409 Conflict",
                409,
                "https://api.scherzo.dev/problems/slug-unavailable",
            ),
            "slug_unavailable",
            1,
        ),
        (
            organization_problem(
                "409 Conflict",
                409,
                "https://api.scherzo.dev/problems/idempotency-conflict",
            ),
            "idempotency_conflict",
            1,
        ),
        (
            http_response("503 Service Unavailable", None, &[]),
            "unreachable",
            4,
        ),
    ];

    for (response, expected_outcome, expected_status) in cases {
        let (server, _directory, _path, credential_path) =
            prepared_organization(vec![response], TOKEN);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &[
                "organization",
                "update",
                "acme-research",
                "--slug",
                "acme-labs",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(expected_status));
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["schemaVersion"], 1);
        assert_eq!(value["deployment"], server.api_url);
        assert_eq!(value["outcome"], expected_outcome);
        if expected_outcome == "unreachable" {
            assert_eq!(value["category"], "server");
        } else {
            assert!(value.get("category").is_none());
        }
        assert!(value.get("title").is_none());
        assert!(value.get("detail").is_none());
        assert!(output.stderr.is_empty());
        assert_eq!(
            server.finish().len(),
            1,
            "explicit responses must not retry"
        );
    }
}

#[test]
fn update_transport_and_protocol_failures_have_closed_statuses() {
    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![Vec::new(), Vec::new()], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let transport = run_with_env(
        &[
            "organization",
            "update",
            "acme",
            "--slug",
            "acme-labs",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(transport.status.code(), Some(4));
    let value: serde_json::Value = serde_json::from_slice(&transport.stdout).unwrap();
    assert_eq!(value["outcome"], "unreachable");
    assert_eq!(value["category"], "connection");
    assert!(transport.stderr.is_empty());
    assert_eq!(server.finish().len(), 2);

    let response = json_http_response(
        "200 OK",
        serde_json::json!({"protocol-response-sentinel": true}),
    );
    let (server, _directory, _path, credential_path) = prepared_organization(vec![response], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let protocol = run_with_env(
        &[
            "organization",
            "update",
            "acme",
            "--slug",
            "acme-labs",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(protocol.status.code(), Some(1));
    assert!(protocol.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&protocol.stderr);
    assert!(stderr.contains("violates the public API contract"));
    assert!(!stderr.contains("protocol-response-sentinel"));
    assert!(!stderr.contains(TOKEN));
    server.finish();
}

#[test]
fn members_list_preserves_omitted_and_opaque_query_values() {
    let cases = [
        (&[][..], "/api/v1/organizations/acme%2Fresearch/memberships"),
        (
            &["--limit", "1"][..],
            "/api/v1/organizations/acme%2Fresearch/memberships?limit=1",
        ),
        (
            &["--limit", "200"][..],
            "/api/v1/organizations/acme%2Fresearch/memberships?limit=200",
        ),
        (
            &["--cursor", "opaque /+=?&"][..],
            "/api/v1/organizations/acme%2Fresearch/memberships?cursor=opaque+%2F%2B%3D%3F%26",
        ),
        (
            &["--limit", "42", "--cursor", "opaque /+=?&"][..],
            "/api/v1/organizations/acme%2Fresearch/memberships?limit=42&cursor=opaque+%2F%2B%3D%3F%26",
        ),
    ];

    for (query_args, expected_target) in cases {
        let (server, _directory, _path, credential_path) =
            prepared_organization(vec![membership_success(serde_json::json!([]), None)], TOKEN);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut args = vec![
            "organization",
            "members",
            "list",
            "acme/research",
            "--json",
            "--allow-insecure-http",
        ];
        args.extend_from_slice(query_args);

        let output = run_with_env(&args, &environment);

        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let request = server.finish().pop().unwrap();
        assert!(request.starts_with(&format!("GET {expected_target} HTTP/1.1\r\n")));
        assert_eq!(
            header_value(&request, "authorization"),
            format!("Bearer {TOKEN}")
        );
        assert!(!request.contains("idempotency-key:"));
    }
}

#[test]
fn json_members_list_emits_one_continued_page_exactly() {
    let items = serde_json::json!([
        {
            "id": "mem_owner",
            "principalId": "prn_human",
            "principalType": "human",
            "displayName": "Ada Lovelace",
            "role": "owner",
            "future": "member-response-sentinel"
        },
        {
            "id": "mem_service",
            "principalId": "prn_service",
            "principalType": "service",
            "role": "member"
        }
    ]);
    let cursor = "opaque continuation /+=?&";
    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![membership_success(items, Some(cursor))], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "members",
            "list",
            "acme",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "listed",
            "items": [
                {
                    "id": "mem_owner",
                    "principalId": "prn_human",
                    "principalType": "human",
                    "displayName": "Ada Lovelace",
                    "role": "owner"
                },
                {
                    "id": "mem_service",
                    "principalId": "prn_service",
                    "principalType": "service",
                    "role": "member"
                }
            ],
            "nextCursor": cursor
        })
    );
    assert!(output.stdout.ends_with(b"\n"));
    assert!(output.stderr.is_empty());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("member-response-sentinel"));
    assert_eq!(
        server.finish().len(),
        1,
        "nextCursor must not trigger pagination"
    );
}

#[test]
fn human_and_empty_members_pages_have_exact_output() {
    let items = serde_json::json!([
        {
            "id": "mem_owner",
            "principalId": "prn_human",
            "principalType": "human",
            "displayName": "Ada Lovelace",
            "role": "owner"
        },
        {
            "id": "mem_service",
            "principalId": "prn_service",
            "principalType": "service",
            "role": "member"
        }
    ]);
    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![membership_success(items, Some("next-page"))], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let human = run_with_env(
        &[
            "organization",
            "members",
            "list",
            "acme",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(human.status.success());
    assert_eq!(
        String::from_utf8(human.stdout).unwrap(),
        format!(
            concat!(
                "✓ Organization members listed.\n\n",
                "  Membership: mem_owner  Principal: prn_human  Type: human  Role: owner  Name: Ada Lovelace\n",
                "  Membership: mem_service  Principal: prn_service  Type: service  Role: member\n\n",
                "  Next cursor: next-page\n",
                "  Deployment: {}\n"
            ),
            server.api_url
        )
    );
    assert!(human.stderr.is_empty());
    server.finish();

    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![membership_success(serde_json::json!([]), None)], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let empty_human = run_with_env(
        &[
            "organization",
            "members",
            "list",
            "acme",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(empty_human.status.success());
    assert_eq!(
        String::from_utf8(empty_human.stdout).unwrap(),
        format!(
            "✓ Organization members listed.\n\n  Deployment: {}\n",
            server.api_url
        )
    );
    assert!(empty_human.stderr.is_empty());
    server.finish();

    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![membership_success(serde_json::json!([]), None)], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let empty = run_with_env(
        &[
            "organization",
            "members",
            "list",
            "acme",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(empty.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&empty.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "listed",
            "items": []
        })
    );
    assert!(empty.stderr.is_empty());
    server.finish();
}

#[test]
fn members_list_expected_outcomes_have_exact_json_and_exit_statuses() {
    let cases = [
        (
            organization_problem(
                "400 Bad Request",
                400,
                "https://api.scherzo.dev/problems/bad-request",
            ),
            "invalid_input",
            1,
        ),
        (
            organization_problem(
                "401 Unauthorized",
                401,
                "https://api.scherzo.dev/problems/unauthorized",
            ),
            "unauthenticated",
            3,
        ),
        (
            organization_problem(
                "403 Forbidden",
                403,
                "https://api.scherzo.dev/problems/forbidden",
            ),
            "forbidden",
            1,
        ),
        (
            organization_problem(
                "404 Not Found",
                404,
                "https://api.scherzo.dev/problems/not-found",
            ),
            "not_found",
            1,
        ),
        (
            http_response("500 Internal Server Error", None, &[]),
            "unreachable",
            4,
        ),
    ];

    for (response, expected_outcome, expected_status) in cases {
        let (server, _directory, _path, credential_path) =
            prepared_organization(vec![response], TOKEN);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &[
                "organization",
                "members",
                "list",
                "acme",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(expected_status));
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["schemaVersion"], 1);
        assert_eq!(value["deployment"], server.api_url);
        assert_eq!(value["outcome"], expected_outcome);
        if expected_outcome == "unreachable" {
            assert_eq!(value["category"], "server");
        } else {
            assert!(value.get("category").is_none());
        }
        assert!(value.get("title").is_none());
        assert!(value.get("detail").is_none());
        assert!(output.stderr.is_empty());
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn members_list_transport_and_protocol_failures_have_closed_statuses() {
    let (server, _directory, _path, credential_path) =
        prepared_organization(vec![Vec::new()], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let transport = run_with_env(
        &[
            "organization",
            "members",
            "list",
            "acme",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(transport.status.code(), Some(4));
    let value: serde_json::Value = serde_json::from_slice(&transport.stdout).unwrap();
    assert_eq!(value["outcome"], "unreachable");
    assert_eq!(value["category"], "connection");
    assert!(transport.stderr.is_empty());
    assert_eq!(server.finish().len(), 1, "reads must not retry");

    let response = json_http_response("200 OK", serde_json::json!({"items": [], "nextCursor": ""}));
    let (server, _directory, _path, credential_path) = prepared_organization(vec![response], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let protocol = run_with_env(
        &[
            "organization",
            "members",
            "list",
            "acme",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(protocol.status.code(), Some(1));
    assert!(protocol.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&protocol.stderr)
            .contains("organization membership cursor is empty")
    );
    assert!(!String::from_utf8_lossy(&protocol.stderr).contains(TOKEN));
    server.finish();
}

#[test]
fn members_list_rejects_explicit_null_optional_fields() {
    let malformed_pages = [
        serde_json::json!({"items": [], "nextCursor": null}),
        serde_json::json!({
            "items": [{
                "id": "mem_01k0z6r1w8f4jy2m7q9v3x5abc",
                "principalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
                "principalType": "human",
                "displayName": null,
                "role": "member"
            }]
        }),
    ];
    let mut statuses = Vec::new();
    let mut standard_outputs = Vec::new();

    for page in malformed_pages {
        let response = json_http_response("200 OK", page);
        let (server, _directory, _path, credential_path) =
            prepared_organization(vec![response], TOKEN);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &[
                "organization",
                "members",
                "list",
                "acme",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );
        statuses.push(output.status.code());
        standard_outputs.push(output.stdout);
        server.finish();
    }

    assert_eq!(statuses, [Some(1), Some(1)]);
    assert!(standard_outputs.iter().all(Vec::is_empty));
}

#[test]
fn update_and_members_list_report_missing_credentials_without_network_requests() {
    for args in [
        &[
            "organization",
            "update",
            "acme",
            "--slug",
            "acme-labs",
            "--json",
            "--allow-insecure-http",
        ][..],
        &[
            "organization",
            "members",
            "list",
            "acme",
            "--json",
            "--allow-insecure-http",
        ][..],
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        let directory = private_credential_directory();
        let credential_path = directory.path().join("credentials.json");
        let environment = deployment_environment(&api_url, credential_path.to_str().unwrap());

        let output = run_with_env(args, &environment);

        assert_eq!(output.status.code(), Some(3));
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
}

#[test]
fn update_and_members_list_keep_private_not_found_outputs_identical() {
    for command in ["update", "members"] {
        for json in [false, true] {
            let responses = [
                "The target is inaccessible.",
                "The target is inactive.",
                "The target is absent.",
            ]
            .map(response_with_detail)
            .into_iter()
            .collect();
            let (server, _directory, _path, credential_path) =
                prepared_organization(responses, TOKEN);
            let environment = deployment_environment(&server.api_url, &credential_path);
            let mut outputs = Vec::new();

            for _ in 0..3 {
                let mut args = if command == "update" {
                    vec![
                        "organization",
                        "update",
                        "private-target",
                        "--slug",
                        "still-private",
                        "--allow-insecure-http",
                    ]
                } else {
                    vec![
                        "organization",
                        "members",
                        "list",
                        "private-target",
                        "--allow-insecure-http",
                    ]
                };
                if json {
                    args.push("--json");
                }
                let output = run_with_env(&args, &environment);
                assert_eq!(output.status.code(), Some(1));
                assert!(output.stderr.is_empty());
                if !json {
                    assert_eq!(output.stdout, b"! Organization not found or unavailable.\n");
                }
                outputs.push(output.stdout);
            }
            assert_eq!(outputs[0], outputs[1]);
            assert_eq!(outputs[1], outputs[2]);
            assert_eq!(server.finish().len(), 3);
        }
    }
}
