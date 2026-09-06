use super::*;

const TOKEN: &str = "unique-github-command-token-sentinel";
const ORGANIZATION: &str = "acme-research";
const SETUP_SESSION: &str = "ghs_01k0z6r1w8f4jy2m7q9v3x5abc";
const INSTALLATION: &str = "ghi_01k0z6r1w8f4jy2m7q9v3x5abc";

fn prepared_github(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, String) {
    let server = ScriptedServer::respond(responses);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let credential_path = credential_path.to_string_lossy().into_owned();
    (server, credential_directory, credential_path)
}

fn installation_body(state: &str) -> serde_json::Value {
    serde_json::json!({
        "id": INSTALLATION,
        "providerInstallationId": "713",
        "providerAccountId": "829",
        "providerAccountType": "Organization",
        "state": state,
        "createdAt": "2026-09-05T12:00:00Z",
        "updatedAt": "2026-09-05T12:01:00Z",
        "future": { "omitted": true }
    })
}

fn github_problem(status_text: &str, status: u16, problem_type: &str) -> Vec<u8> {
    problem_http_response(
        status_text,
        serde_json::json!({
            "type": problem_type,
            "title": "private-github-title-sentinel",
            "status": status,
            "detail": "private-github-proof-sentinel"
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
fn github_namespaces_print_help_without_loading_deployment() {
    for args in [
        &["github"][..],
        &["github", "setup"][..],
        &["github", "installation"][..],
        &["github", "repository"][..],
    ] {
        let output = run_with_env(
            args,
            &[("SCHERZO_CLOUD_API_URL", "partial-override-is-ignored")],
        );
        assert!(output.status.success());
        assert!(!output.stdout.is_empty());
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn setup_begin_and_complete_preserve_the_browser_handoff() {
    let setup_url =
        format!("https://github.example/apps/scherzo/installations/new?state={SETUP_SESSION}");
    let (server, _directory, credential_path) = prepared_github(vec![
        json_http_response(
            "201 Created",
            serde_json::json!({
                "id": SETUP_SESSION,
                "state": "pending",
                "expiresAt": "2026-09-05T12:10:00Z",
                "setupUrl": setup_url.clone(),
                "future": "setup-response-sentinel"
            }),
        ),
        json_http_response("200 OK", installation_body("active")),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let begin = run_with_env(
        &[
            "github",
            "setup",
            "begin",
            ORGANIZATION,
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(begin.status.success());
    let human = String::from_utf8(begin.stdout).unwrap();
    assert!(human.starts_with("✓ GitHub setup started.\n"));
    assert!(human.contains(&format!("  Setup session: {SETUP_SESSION}\n")));
    assert!(human.contains(&format!("  {setup_url}\n")));
    assert!(human.contains(&format!(
        "scherzo-cloud github setup complete {ORGANIZATION} {SETUP_SESSION} --provider-installation-id <INSTALLATION_ID>"
    )));
    assert!(!human.contains("setup-response-sentinel"));
    assert!(begin.stderr.is_empty());

    let complete = run_with_env(
        &[
            "github",
            "setup",
            "complete",
            ORGANIZATION,
            SETUP_SESSION,
            "--provider-installation-id",
            "713",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(complete.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&complete.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "organizationRef": ORGANIZATION,
            "outcome": "completed",
            "installation": {
                "id": INSTALLATION,
                "providerInstallationId": "713",
                "providerAccountId": "829",
                "providerAccountType": "Organization",
                "state": "active",
                "createdAt": "2026-09-05T12:00:00Z",
                "updatedAt": "2026-09-05T12:01:00Z"
            }
        })
    );
    assert!(complete.stdout.ends_with(b"\n"));
    assert!(complete.stderr.is_empty());

    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with(&format!(
        "POST /api/v1/organizations/{ORGANIZATION}/github/setup-sessions HTTP/1.1\r\n"
    )));
    assert!(request_body(&requests[0]).is_empty());
    assert!(requests[1].starts_with(&format!(
        "POST /api/v1/organizations/{ORGANIZATION}/github/setup-sessions/{SETUP_SESSION}/completion HTTP/1.1\r\n"
    )));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&requests[1])).unwrap(),
        serde_json::json!({"providerInstallationId": "713"})
    );
    for request in requests {
        assert_eq!(
            header_value(&request, "authorization"),
            format!("Bearer {TOKEN}")
        );
        assert!(!request.contains("idempotency-key:"));
    }
}

#[test]
fn installation_and_repository_lists_expose_current_provider_state() {
    let mut disconnected = installation_body("disconnected");
    disconnected["id"] = serde_json::Value::String("ghi_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned());
    disconnected["providerInstallationId"] = serde_json::Value::String("714".to_owned());
    disconnected["providerAccountType"] = serde_json::Value::String("User".to_owned());
    let (server, _directory, credential_path) = prepared_github(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "items": [installation_body("active"), disconnected],
                "future": "installation-list-sentinel"
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "installation": installation_body("active"),
                "items": [
                    {
                        "providerRepositoryId": "991",
                        "fullName": "acme/widgets",
                        "defaultBranch": "main",
                        "future": "repository-response-sentinel"
                    },
                    {
                        "providerRepositoryId": "992",
                        "fullName": "acme/services",
                        "defaultBranch": "release"
                    }
                ],
                "future": "repository-list-sentinel"
            }),
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let installations = run_with_env(
        &[
            "github",
            "installation",
            "list",
            "acme/research",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(installations.status.success());
    let installations_json: serde_json::Value =
        serde_json::from_slice(&installations.stdout).unwrap();
    assert_eq!(installations_json["outcome"], "listed");
    assert_eq!(installations_json["organizationRef"], "acme/research");
    assert_eq!(installations_json["items"][0]["state"], "active");
    assert_eq!(installations_json["items"][1]["state"], "disconnected");
    assert_eq!(
        installations_json["items"][1]["providerAccountType"],
        "User"
    );
    assert!(!String::from_utf8_lossy(&installations.stdout).contains("sentinel"));
    assert!(installations.stderr.is_empty());

    let repositories = run_with_env(
        &[
            "github",
            "repository",
            "list",
            "acme/research",
            INSTALLATION,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(repositories.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&repositories.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "organizationRef": "acme/research",
            "outcome": "listed",
            "installation": {
                "id": INSTALLATION,
                "providerInstallationId": "713",
                "providerAccountId": "829",
                "providerAccountType": "Organization",
                "state": "active",
                "createdAt": "2026-09-05T12:00:00Z",
                "updatedAt": "2026-09-05T12:01:00Z"
            },
            "items": [
                {
                    "providerRepositoryId": "991",
                    "fullName": "acme/widgets",
                    "defaultBranch": "main"
                },
                {
                    "providerRepositoryId": "992",
                    "fullName": "acme/services",
                    "defaultBranch": "release"
                }
            ]
        })
    );
    assert!(repositories.stderr.is_empty());

    let requests = server.finish();
    assert!(requests[0].starts_with(
        "GET /api/v1/organizations/acme%2Fresearch/github/installations HTTP/1.1\r\n"
    ));
    assert!(requests[1].starts_with(&format!(
        "GET /api/v1/organizations/acme%2Fresearch/github/installations/{INSTALLATION}/repositories HTTP/1.1\r\n"
    )));
}

#[test]
fn github_successes_reject_mismatched_response_subjects() {
    let mut mismatched_provider = installation_body("active");
    mismatched_provider["providerInstallationId"] = serde_json::Value::String("714".to_owned());

    let mismatched_binding_id = "ghi_01k0z6r1w8f4jy2m7q9v3x5abd";
    let mut mismatched_binding = installation_body("disconnected");
    mismatched_binding["id"] = serde_json::Value::String(mismatched_binding_id.to_owned());

    let mut mismatched_repository_binding = installation_body("active");
    mismatched_repository_binding["id"] =
        serde_json::Value::String(mismatched_binding_id.to_owned());

    let (server, _directory, credential_path) = prepared_github(vec![
        json_http_response("200 OK", mismatched_provider),
        json_http_response("200 OK", mismatched_binding),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "installation": mismatched_repository_binding,
                "items": []
            }),
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let commands = [
        vec![
            "github",
            "setup",
            "complete",
            ORGANIZATION,
            SETUP_SESSION,
            "--provider-installation-id",
            "713",
            "--json",
            "--allow-insecure-http",
        ],
        vec![
            "github",
            "installation",
            "disconnect",
            ORGANIZATION,
            INSTALLATION,
            "--json",
            "--allow-insecure-http",
        ],
        vec![
            "github",
            "repository",
            "list",
            ORGANIZATION,
            INSTALLATION,
            "--json",
            "--allow-insecure-http",
        ],
    ];

    let observed = commands
        .iter()
        .map(|args| {
            let output = run_with_env(args, &environment);
            let body: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            (
                output.status.code(),
                body["outcome"].as_str().unwrap().to_owned(),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(server.finish().len(), 3);
    assert_eq!(
        observed,
        vec![
            (Some(1), "invalid_response".to_owned()),
            (Some(1), "invalid_response".to_owned()),
            (Some(1), "invalid_response".to_owned()),
        ],
        "success responses must remain bound to the provider or binding ID in the request"
    );
}

#[test]
fn replayable_connection_mutations_retry_one_ambiguous_transport_failure() {
    let cases = [
        (
            &[
                "github",
                "setup",
                "complete",
                ORGANIZATION,
                SETUP_SESSION,
                "--provider-installation-id",
                "713",
                "--json",
                "--allow-insecure-http",
            ][..],
            "active",
            "completed",
            format!(
                "POST /api/v1/organizations/{ORGANIZATION}/github/setup-sessions/{SETUP_SESSION}/completion HTTP/1.1\r\n"
            ),
        ),
        (
            &[
                "github",
                "installation",
                "disconnect",
                ORGANIZATION,
                INSTALLATION,
                "--json",
                "--allow-insecure-http",
            ][..],
            "disconnected",
            "disconnected",
            format!(
                "DELETE /api/v1/organizations/{ORGANIZATION}/github/installations/{INSTALLATION} HTTP/1.1\r\n"
            ),
        ),
    ];

    for (args, state, expected_outcome, expected_request) in cases {
        let (server, _directory, credential_path) = prepared_github(vec![
            Vec::new(),
            json_http_response("200 OK", installation_body(state)),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(args, &environment);

        assert!(output.status.success());
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["outcome"], expected_outcome);
        assert_eq!(value["installation"]["state"], state);
        assert!(output.stderr.is_empty());
        let requests = server.finish();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
        assert!(requests[0].starts_with(&expected_request));
    }
}

#[test]
fn meaningful_api_failures_have_closed_json_and_exit_statuses() {
    let cases = [
        (
            github_problem(
                "400 Bad Request",
                400,
                "https://api.scherzo.dev/problems/bad-request",
            ),
            vec![
                "github",
                "setup",
                "complete",
                ORGANIZATION,
                SETUP_SESSION,
                "--provider-installation-id",
                "713",
            ],
            "invalid_input",
            1,
        ),
        (
            github_problem(
                "403 Forbidden",
                403,
                "https://api.scherzo.dev/problems/forbidden",
            ),
            vec!["github", "setup", "begin", ORGANIZATION],
            "forbidden",
            1,
        ),
        (
            github_problem(
                "404 Not Found",
                404,
                "https://api.scherzo.dev/problems/not-found",
            ),
            vec!["github", "installation", "list", ORGANIZATION],
            "not_found",
            1,
        ),
        (
            github_problem(
                "409 Conflict",
                409,
                "https://api.scherzo.dev/problems/source-connection-conflict",
            ),
            vec!["github", "repository", "list", ORGANIZATION, INSTALLATION],
            "source_connection_conflict",
            1,
        ),
        (
            http_response("503 Service Unavailable", None, &[]),
            vec![
                "github",
                "installation",
                "disconnect",
                ORGANIZATION,
                INSTALLATION,
            ],
            "unreachable",
            4,
        ),
    ];

    for (response, mut args, expected_outcome, expected_status) in cases {
        let (server, _directory, credential_path) = prepared_github(vec![response]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        args.extend(["--json", "--allow-insecure-http"]);

        let output = run_with_env(&args, &environment);

        assert_eq!(output.status.code(), Some(expected_status));
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["schemaVersion"], 1);
        assert_eq!(value["deployment"], server.api_url);
        assert_eq!(value["organizationRef"], ORGANIZATION);
        assert_eq!(value["outcome"], expected_outcome);
        if expected_outcome == "unreachable" {
            assert_eq!(value["category"], "server");
        } else {
            assert!(value.get("category").is_none());
        }
        assert!(value.get("title").is_none());
        assert!(value.get("detail").is_none());
        assert!(output.stderr.is_empty());
        let combined = String::from_utf8_lossy(&output.stdout);
        assert!(!combined.contains("private-github"));
        assert!(!combined.contains(TOKEN));
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn setup_conflict_is_actionable_on_standard_error_without_problem_prose() {
    let response = github_problem(
        "409 Conflict",
        409,
        "https://api.scherzo.dev/problems/source-connection-conflict",
    );
    let (server, _directory, credential_path) = prepared_github(vec![response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "github",
            "setup",
            "complete",
            ORGANIZATION,
            SETUP_SESSION,
            "--provider-installation-id",
            "713",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let diagnostic = String::from_utf8(output.stderr).unwrap();
    assert!(diagnostic.starts_with("error: "));
    assert!(diagnostic.contains("\n\n"));
    assert!(diagnostic.contains("scherzo-cloud github setup begin <ORGANIZATION>"));
    assert!(!diagnostic.contains("private-github"));
    assert!(!diagnostic.contains(TOKEN));
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn missing_human_credential_does_not_contact_the_github_api() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let directory = private_credential_directory();
    let credential_path = directory.path().join("credentials.json");
    let environment = deployment_environment(&api_url, credential_path.to_str().unwrap());

    let output = run_with_env(
        &["github", "installation", "list", ORGANIZATION, "--json"],
        &environment,
    );

    assert_eq!(output.status.code(), Some(3));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": api_url,
            "organizationRef": ORGANIZATION,
            "outcome": "unauthenticated"
        })
    );
    assert!(output.stderr.is_empty());
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
}

#[test]
fn oversized_github_success_response_is_rejected() {
    let repositories = (1..=1_024)
        .map(|provider_id| {
            serde_json::json!({
                "providerRepositoryId": provider_id.to_string(),
                "fullName": format!("acme/repo-{provider_id}"),
                "defaultBranch": "b".repeat(1_024)
            })
        })
        .collect::<Vec<_>>();
    let response_body = serde_json::to_vec(&serde_json::json!({
        "installation": installation_body("active"),
        "items": repositories
    }))
    .unwrap();
    assert!(response_body.len() > 1_024 * 1_024);
    let response = http_response("200 OK", Some("application/json"), &response_body);
    let (server, _directory, credential_path) = prepared_github(vec![response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "github",
            "repository",
            "list",
            ORGANIZATION,
            INSTALLATION,
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
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn rejected_github_access_token_is_refreshed_and_retried() {
    let rejected = github_problem(
        "401 Unauthorized",
        401,
        "https://api.scherzo.dev/problems/unauthorized",
    );
    let server = ScriptedServer::respond(vec![
        rejected,
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-refreshed-github-access-token",
                "refresh_token": "unique-refreshed-github-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({"items": [installation_body("active")]}),
        ),
    ]);
    let directory = private_credential_directory();
    let credential_path = directory.path().join("credentials.json");
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
            "github",
            "installation",
            "list",
            ORGANIZATION,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "listed"
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&requests[2], "authorization"),
        "Bearer unique-refreshed-github-access-token"
    );
}

#[test]
fn remote_insecure_setup_url_is_rejected_before_browser_handoff() {
    let unsafe_url = "http://github.example/private-setup-url-sentinel";
    let response = json_http_response(
        "201 Created",
        serde_json::json!({
            "id": SETUP_SESSION,
            "state": "pending",
            "expiresAt": "2026-09-05T12:10:00Z",
            "setupUrl": unsafe_url
        }),
    );
    let (server, _directory, credential_path) = prepared_github(vec![response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "github",
            "setup",
            "begin",
            ORGANIZATION,
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
    assert!(!String::from_utf8_lossy(&output.stdout).contains(unsafe_url));
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn malformed_success_is_a_redacted_protocol_failure() {
    let mut invalid = installation_body("active");
    invalid["providerInstallationId"] =
        serde_json::Value::String("private-invalid-provider-id-sentinel".to_owned());
    let (server, _directory, credential_path) =
        prepared_github(vec![json_http_response("200 OK", invalid)]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "github",
            "installation",
            "disconnect",
            ORGANIZATION,
            INSTALLATION,
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
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!combined.contains("private-invalid-provider-id-sentinel"));
    assert!(!combined.contains(TOKEN));
    server.finish();
}
