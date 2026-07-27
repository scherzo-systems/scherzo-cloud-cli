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
    assert!(bare_help.contains("create  Create a Scherzo Cloud organization"));
    assert!(bare_help.contains("show    Show a Scherzo Cloud organization"));
    assert!(!bare_help.contains("update"));
    assert!(!bare_help.contains("members"));

    for (args, expected) in [
        (
            &["organization", "create", "--help"][..],
            "--display-name <DISPLAY_NAME>",
        ),
        (&["organization", "show", "--help"][..], "<ORGANIZATION>"),
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
            2,
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
            3,
            Some(42),
        ),
        (
            http_response("503 Service Unavailable", None, &[]),
            "unreachable",
            3,
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

    assert_eq!(output.status.code(), Some(3));
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
            2,
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

    assert_eq!(output.status.code(), Some(2));
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
            2,
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
            3,
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
    assert_eq!(transport.status.code(), Some(3));
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
