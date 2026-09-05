use super::*;

const TOKEN: &str = "unique-project-command-token-sentinel";
const ORGANIZATION: &str = "acme-research";
const ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
const MEMBERSHIP_ID: &str = "mem_01k0z6r1w8f4jy2m7q9v3x5abc";
const PROJECT_ID: &str = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
const INSTALLATION_ID: &str = "ghi_01k0z6r1w8f4jy2m7q9v3x5abc";
const CONNECTION_ID: &str = "rpc_01k0z6r1w8f4jy2m7q9v3x5abc";
const POOL_ID: &str = "rpl_01k0z6r1w8f4jy2m7q9v3x5abc";
const REPOSITORY_ID: &str = "123456789";
const RUN_ID: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";

fn prepared_project(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, String) {
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

fn repository_body(full_name: &str, default_branch: &str) -> serde_json::Value {
    serde_json::json!({
        "connectionId": CONNECTION_ID,
        "installationBindingId": INSTALLATION_ID,
        "providerRepositoryId": REPOSITORY_ID,
        "fullName": full_name,
        "defaultBranch": default_branch,
        "availability": "active"
    })
}

fn project_body(
    name: &str,
    pool: bool,
    repository: Option<serde_json::Value>,
) -> serde_json::Value {
    let blockers = match (pool, repository.is_some()) {
        (false, true) => serde_json::json!(["runner_pool_unassigned"]),
        (true, false) => serde_json::json!(["repository_detached"]),
        (false, false) => {
            serde_json::json!(["runner_pool_unassigned", "repository_detached"])
        }
        (true, true) => serde_json::json!([]),
    };
    serde_json::json!({
        "id": PROJECT_ID,
        "organizationId": ORGANIZATION_ID,
        "name": name,
        "runnerPool": pool.then(|| serde_json::json!({"id": POOL_ID, "name": "builders"})),
        "repository": repository,
        "executionReadiness": {
            "ready": blockers.as_array().unwrap().is_empty(),
            "blockers": blockers
        },
        "createdAt": "2026-09-05T12:00:00Z",
        "updatedAt": "2026-09-05T12:01:00Z"
    })
}

fn project_response(status: &str, project: serde_json::Value, create: bool) -> Vec<u8> {
    let mut headers = vec![("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)];
    if create {
        headers.push((
            "Location",
            "/v1/organizations/acme-research/projects/prj_01k0z6r1w8f4jy2m7q9v3x5abc",
        ));
    }
    http_response_with_headers(
        status,
        Some("application/json"),
        &headers,
        &serde_json::to_vec(&project).unwrap(),
    )
}

fn request_body(request: &str) -> serde_json::Value {
    serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
}

fn run_project(args: &[&str], server: &ScriptedServer, credential_path: &str) -> Output {
    let environment = deployment_environment(&server.api_url, credential_path);
    run_with_env(args, &environment)
}

fn assert_json_success(output: &Output, outcome: &str) -> serde_json::Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schemaVersion"], 1);
    assert_eq!(value["outcome"], outcome);
    value
}

#[test]
fn discovery_and_project_management_feed_an_inputless_cloud_run() {
    let membership_page = serde_json::json!({
        "items": [{
            "id": MEMBERSHIP_ID,
            "organizationId": ORGANIZATION_ID,
            "organizationState": "active",
            "organizationDisplayName": "Acme Research",
            "organizationSlug": ORGANIZATION,
            "role": "owner",
            "state": "active",
            "createdAt": "2026-09-05T11:00:00Z",
            "updatedAt": "2026-09-05T11:00:00Z"
        }]
    });
    let installation = serde_json::json!({
        "id": INSTALLATION_ID,
        "providerInstallationId": "87654321",
        "providerAccountId": "11223344",
        "providerAccountType": "Organization",
        "state": "active",
        "createdAt": "2026-09-05T11:00:00Z",
        "updatedAt": "2026-09-05T11:00:00Z"
    });
    let discovered_repository = serde_json::json!({
        "providerRepositoryId": REPOSITORY_ID,
        "fullName": "acme/widget",
        "defaultBranch": "main"
    });
    let initial = project_body(
        "widget",
        false,
        Some(repository_body("acme/widget", "release")),
    );
    let renamed = project_body(
        "widget-service",
        false,
        Some(repository_body("acme/widget", "release")),
    );
    let ready = project_body(
        "widget-service",
        true,
        Some(repository_body("acme/widget", "release")),
    );
    let rebound = project_body(
        "widget-service",
        true,
        Some(repository_body("acme/widget-next", "main")),
    );
    let branch_updated = project_body(
        "widget-service",
        true,
        Some(repository_body("acme/widget-next", "stable")),
    );
    let detached = project_body("widget-service", true, None);
    let unconfigured = project_body("widget-service", false, None);
    let acceptance = http_response_with_headers(
        "202 Accepted",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&serde_json::json!({"runId": RUN_ID, "replayed": false})).unwrap(),
    );
    let (server, _directory, credential_path) = prepared_project(vec![
        json_http_response("200 OK", membership_page),
        json_http_response(
            "200 OK",
            serde_json::json!({"items": [installation.clone()]}),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "installation": installation,
                "items": [discovered_repository]
            }),
        ),
        project_response("201 Created", initial.clone(), true),
        json_http_response(
            "200 OK",
            serde_json::json!({"items": [initial.clone()], "nextCursor": "next-project-page"}),
        ),
        json_http_response("200 OK", initial.clone()),
        project_response("200 OK", renamed.clone(), false),
        project_response("200 OK", ready.clone(), false),
        json_http_response("200 OK", repository_body("acme/widget", "release")),
        project_response("200 OK", rebound, false),
        project_response("200 OK", branch_updated, false),
        project_response("200 OK", detached, false),
        project_response("200 OK", unconfigured, false),
        acceptance,
    ]);

    let organizations = run_project(
        &["organization", "list", "--json", "--allow-insecure-http"],
        &server,
        &credential_path,
    );
    let organizations = assert_json_success(&organizations, "listed");
    assert_eq!(organizations["items"][0]["organizationSlug"], ORGANIZATION);

    let installations = run_project(
        &[
            "project",
            "repository",
            "installation",
            "list",
            ORGANIZATION,
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    let installations = assert_json_success(&installations, "listed");
    assert_eq!(installations["items"][0]["id"], INSTALLATION_ID);

    let repositories = run_project(
        &[
            "project",
            "repository",
            "list",
            ORGANIZATION,
            INSTALLATION_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    let repositories = assert_json_success(&repositories, "listed");
    assert_eq!(
        repositories["items"][0]["providerRepositoryId"],
        REPOSITORY_ID
    );
    assert_eq!(repositories["items"][0]["defaultBranch"], "main");

    let created = run_project(
        &[
            "project",
            "create",
            ORGANIZATION,
            "--name",
            "widget",
            "--installation-id",
            INSTALLATION_ID,
            "--repository-id",
            REPOSITORY_ID,
            "--default-branch",
            "release",
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    let created = assert_json_success(&created, "created");
    assert_eq!(created["project"]["id"], PROJECT_ID);
    assert_eq!(created["project"]["executionReadiness"]["ready"], false);
    assert_eq!(
        created["project"]["executionReadiness"]["blockers"],
        serde_json::json!(["runner_pool_unassigned"])
    );

    let listed = run_project(
        &[
            "project",
            "list",
            ORGANIZATION,
            "--limit",
            "1",
            "--cursor",
            "opaque /+=?&",
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    let listed = assert_json_success(&listed, "listed");
    assert_eq!(listed["items"][0]["id"], PROJECT_ID);
    assert_eq!(listed["nextCursor"], "next-project-page");

    let shown = run_project(
        &[
            "project",
            "show",
            ORGANIZATION,
            PROJECT_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    assert_eq!(
        assert_json_success(&shown, "found")["project"]["name"],
        "widget"
    );

    let renamed_output = run_project(
        &[
            "project",
            "rename",
            ORGANIZATION,
            PROJECT_ID,
            "--name",
            "widget-service",
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    assert_eq!(
        assert_json_success(&renamed_output, "renamed")["project"]["name"],
        "widget-service"
    );

    let pool_set = run_project(
        &[
            "project",
            "runner-pool",
            "set",
            ORGANIZATION,
            PROJECT_ID,
            POOL_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    let pool_set = assert_json_success(&pool_set, "runner_pool_set");
    assert_eq!(pool_set["project"]["runnerPool"]["id"], POOL_ID);
    assert_eq!(pool_set["project"]["executionReadiness"]["ready"], true);

    let repository = run_project(
        &[
            "project",
            "repository",
            "show",
            ORGANIZATION,
            PROJECT_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    assert_eq!(
        assert_json_success(&repository, "found")["repository"]["connectionId"],
        CONNECTION_ID
    );

    let repository_set = run_project(
        &[
            "project",
            "repository",
            "set",
            ORGANIZATION,
            PROJECT_ID,
            "--installation-id",
            INSTALLATION_ID,
            "--repository-id",
            REPOSITORY_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    assert_json_success(&repository_set, "repository_set");

    let repository_updated = run_project(
        &[
            "project",
            "repository",
            "update",
            ORGANIZATION,
            PROJECT_ID,
            "--default-branch",
            "stable",
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    let repository_updated = assert_json_success(&repository_updated, "repository_updated");
    assert_eq!(
        repository_updated["project"]["repository"]["defaultBranch"],
        "stable"
    );

    let repository_detached = run_project(
        &[
            "project",
            "repository",
            "detach",
            ORGANIZATION,
            PROJECT_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    let repository_detached = assert_json_success(&repository_detached, "repository_detached");
    assert_eq!(
        repository_detached["project"]["repository"],
        serde_json::Value::Null
    );

    let pool_removed = run_project(
        &[
            "project",
            "runner-pool",
            "remove",
            ORGANIZATION,
            PROJECT_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    let pool_removed = assert_json_success(&pool_removed, "runner_pool_removed");
    assert_eq!(
        pool_removed["project"]["executionReadiness"]["blockers"],
        serde_json::json!(["runner_pool_unassigned", "repository_detached"])
    );

    let run_created = run_project(
        &[
            "run",
            "create",
            ORGANIZATION,
            "--project-id",
            PROJECT_ID,
            "--workflow-path",
            "workflows/build.yaml",
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );
    assert_eq!(
        assert_json_success(&run_created, "accepted")["runId"],
        RUN_ID
    );

    let requests = server.finish();
    assert_eq!(requests.len(), 14);
    assert!(requests[0].starts_with("GET /api/v1/me/memberships HTTP/1.1\r\n"));
    assert!(requests[1].starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/github/installations HTTP/1.1\r\n"
    )));
    assert!(requests[2].starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/github/installations/{INSTALLATION_ID}/repositories HTTP/1.1\r\n"
    )));
    assert!(requests[3].starts_with(&format!(
        "POST /api/v1/organizations/{ORGANIZATION}/projects HTTP/1.1\r\n"
    )));
    assert_eq!(
        request_body(&requests[3]),
        serde_json::json!({
            "name": "widget",
            "repository": {
                "installationBindingId": INSTALLATION_ID,
                "providerRepositoryId": REPOSITORY_ID,
                "defaultBranch": "release"
            }
        })
    );
    assert!(requests[4].starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/projects?limit=1&cursor=opaque+%2F%2B%3D%3F%26 HTTP/1.1\r\n"
    )));
    assert!(requests[6].starts_with("PATCH "));
    assert_eq!(
        header_value(&requests[6], "content-type"),
        "application/merge-patch+json"
    );
    assert_eq!(
        request_body(&requests[6]),
        serde_json::json!({"name": "widget-service"})
    );
    assert!(requests[7].starts_with("PUT "));
    assert_eq!(
        request_body(&requests[7]),
        serde_json::json!({"runnerPoolId": POOL_ID})
    );
    assert_eq!(
        request_body(&requests[9]),
        serde_json::json!({
            "installationBindingId": INSTALLATION_ID,
            "providerRepositoryId": REPOSITORY_ID
        })
    );
    assert_eq!(
        header_value(&requests[10], "content-type"),
        "application/merge-patch+json"
    );
    assert_eq!(
        request_body(&requests[10]),
        serde_json::json!({"defaultBranch": "stable"})
    );
    assert!(requests[11].starts_with("DELETE "));
    assert!(requests[12].starts_with("DELETE "));
    assert_eq!(
        request_body(&requests[13]),
        serde_json::json!({
            "projectId": PROJECT_ID,
            "workflowPath": "workflows/build.yaml"
        })
    );
    for request in &requests {
        assert_eq!(
            header_value(request, "authorization"),
            format!("Bearer {TOKEN}")
        );
    }
    for request in [
        &requests[3],
        &requests[6],
        &requests[7],
        &requests[9],
        &requests[10],
        &requests[11],
        &requests[12],
        &requests[13],
    ] {
        let key = header_value(request, "idempotency-key");
        assert_eq!(key.len(), 64);
        assert!(key.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}

#[test]
fn project_create_retries_an_ambiguous_response_with_the_same_request_identity() {
    let project = project_body(
        "widget",
        false,
        Some(repository_body("acme/widget", "main")),
    );
    let (server, _directory, credential_path) = prepared_project(vec![
        Vec::new(),
        project_response("201 Created", project, true),
    ]);

    let output = run_project(
        &[
            "project",
            "create",
            ORGANIZATION,
            "--name",
            "widget",
            "--installation-id",
            INSTALLATION_ID,
            "--repository-id",
            REPOSITORY_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );

    assert_json_success(&output, "created");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
}

#[test]
fn project_create_refreshes_a_rejected_human_session_without_changing_the_request() {
    let refreshed_token = "unique-refreshed-project-command-token";
    let project = project_body(
        "widget",
        false,
        Some(repository_body("acme/widget", "main")),
    );
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
                "access_token": refreshed_token,
                "refresh_token": "unique-refreshed-project-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        project_response("201 Created", project, true),
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
            "project",
            "create",
            ORGANIZATION,
            "--name",
            "widget",
            "--installation-id",
            INSTALLATION_ID,
            "--repository-id",
            REPOSITORY_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_json_success(&output, "created");
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[2], "idempotency-key")
    );
    assert_eq!(request_body(&requests[0]), request_body(&requests[2]));
    assert_eq!(
        header_value(&requests[2], "authorization"),
        format!("Bearer {refreshed_token}")
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!combined.contains(TOKEN));
    assert!(!combined.contains(refreshed_token));
}

#[test]
fn project_create_rejects_success_without_required_identity_headers() {
    let project = project_body(
        "widget",
        false,
        Some(repository_body("acme/widget", "main")),
    );
    let responses = [
        json_http_response("201 Created", project.clone()),
        http_response_with_headers(
            "201 Created",
            Some("application/json"),
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    "/v1/organizations/acme-research/projects/prj_wrong",
                ),
            ],
            &serde_json::to_vec(&project).unwrap(),
        ),
    ];

    for response in responses {
        let (server, _directory, credential_path) = prepared_project(vec![response]);
        let output = run_project(
            &[
                "project",
                "create",
                ORGANIZATION,
                "--name",
                "widget",
                "--installation-id",
                INSTALLATION_ID,
                "--repository-id",
                REPOSITORY_ID,
                "--json",
                "--allow-insecure-http",
            ],
            &server,
            &credential_path,
        );

        assert_eq!(output.status.code(), Some(1));
        assert!(output.stderr.is_empty());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
            "invalid_response"
        );
        server.finish();
    }
}

#[test]
fn project_human_output_exposes_readiness_and_blockers() {
    let project = project_body(
        "widget",
        false,
        Some(repository_body("acme/widget", "main")),
    );
    let (server, _directory, credential_path) =
        prepared_project(vec![json_http_response("200 OK", project)]);

    let output = run_project(
        &[
            "project",
            "show",
            ORGANIZATION,
            PROJECT_ID,
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout).unwrap();
    for field in [
        format!("project: {PROJECT_ID}"),
        "name: widget".to_owned(),
        "runner pool: none".to_owned(),
        "repository: acme/widget".to_owned(),
        "default branch: main".to_owned(),
        "readiness: blocked".to_owned(),
        "blockers: runner_pool_unassigned".to_owned(),
    ] {
        assert!(
            stdout.lines().any(|line| line == field),
            "missing {field:?}"
        );
    }
    server.finish();
}

#[test]
fn project_api_errors_have_closed_json_outcomes_and_exit_codes() {
    let unsigned = run(&["project", "show", ORGANIZATION, PROJECT_ID, "--json"]);
    assert_eq!(unsigned.status.code(), Some(3));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&unsigned.stdout).unwrap()["outcome"],
        "unauthenticated"
    );
    assert!(unsigned.stderr.is_empty());

    let cases = [
        (
            vec![
                "project",
                "runner-pool",
                "set",
                ORGANIZATION,
                PROJECT_ID,
                POOL_ID,
            ],
            "403 Forbidden",
            "https://api.scherzo.dev/problems/forbidden",
            "forbidden",
            1,
        ),
        (
            vec!["project", "show", ORGANIZATION, PROJECT_ID],
            "404 Not Found",
            "https://api.scherzo.dev/problems/not-found",
            "not_found",
            1,
        ),
        (
            vec!["project", "repository", "show", ORGANIZATION, PROJECT_ID],
            "404 Not Found",
            "https://api.scherzo.dev/problems/repository-not-bound",
            "repository_not_bound",
            1,
        ),
        (
            vec![
                "project",
                "create",
                ORGANIZATION,
                "--name",
                "widget",
                "--installation-id",
                INSTALLATION_ID,
                "--repository-id",
                REPOSITORY_ID,
            ],
            "409 Conflict",
            "https://api.scherzo.dev/problems/project-name-unavailable",
            "name_unavailable",
            1,
        ),
        (
            vec![
                "project",
                "create",
                ORGANIZATION,
                "--name",
                "widget",
                "--installation-id",
                INSTALLATION_ID,
                "--repository-id",
                REPOSITORY_ID,
            ],
            "409 Conflict",
            "https://api.scherzo.dev/problems/source-connection-conflict",
            "source_conflict",
            1,
        ),
        (
            vec![
                "project",
                "create",
                ORGANIZATION,
                "--name",
                "widget",
                "--installation-id",
                INSTALLATION_ID,
                "--repository-id",
                REPOSITORY_ID,
            ],
            "429 Too Many Requests",
            "https://api.scherzo.dev/problems/rate-limit-exceeded",
            "rate_limited",
            4,
        ),
    ];

    for (mut args, status, problem_type, expected_outcome, expected_status) in cases {
        let problem = serde_json::json!({
            "type": problem_type,
            "title": "private-project-problem-title",
            "status": status.split_whitespace().next().unwrap().parse::<u16>().unwrap(),
            "detail": "private-project-problem-detail"
        });
        let response = if expected_outcome == "rate_limited" {
            http_response_with_headers(
                status,
                Some("application/problem+json"),
                &[("Retry-After", "42")],
                &serde_json::to_vec(&problem).unwrap(),
            )
        } else {
            problem_http_response(status, problem)
        };
        let (server, _directory, credential_path) = prepared_project(vec![response]);
        args.extend(["--json", "--allow-insecure-http"]);

        let output = run_project(&args, &server, &credential_path);

        assert_eq!(output.status.code(), Some(expected_status));
        assert!(output.stderr.is_empty());
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["outcome"], expected_outcome);
        if expected_outcome == "rate_limited" {
            assert_eq!(value["retryAfter"], 42);
        } else {
            assert!(value.get("retryAfter").is_none());
        }
        assert!(value.get("title").is_none());
        assert!(value.get("detail").is_none());
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(!text.contains("private-project-problem"));
        assert!(!text.contains(TOKEN));
        server.finish();
    }
}

#[test]
fn project_mutation_rejects_a_different_project_identity() {
    let mut other_project = project_body(
        "other-project",
        true,
        Some(repository_body("acme/other", "main")),
    );
    other_project["id"] = serde_json::json!("prj_01k0z6r1w8f4jy2m7q9v3x5abd");
    let (server, _directory, credential_path) =
        prepared_project(vec![project_response("200 OK", other_project, false)]);

    let output = run_project(
        &[
            "project",
            "runner-pool",
            "set",
            ORGANIZATION,
            PROJECT_ID,
            POOL_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "invalid_response"
    );
    server.finish();
}

#[test]
fn repository_update_reports_repository_not_bound() {
    let response = problem_http_response(
        "404 Not Found",
        serde_json::json!({
            "type": "https://api.scherzo.dev/problems/repository-not-bound",
            "title": "Repository not bound",
            "status": 404,
            "detail": "The project has no repository binding."
        }),
    );
    let (server, _directory, credential_path) = prepared_project(vec![response]);

    let output = run_project(
        &[
            "project",
            "repository",
            "update",
            ORGANIZATION,
            PROJECT_ID,
            "--default-branch",
            "main",
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "repository_not_bound"
    );
    server.finish();
}

#[test]
fn project_rate_limit_requires_a_positive_retry_after() {
    for retry_after in [None, Some("0"), Some("invalid")] {
        let body = serde_json::to_vec(&serde_json::json!({
            "type": "https://api.scherzo.dev/problems/rate-limit-exceeded",
            "title": "Rate limited",
            "status": 429
        }))
        .unwrap();
        let headers = retry_after
            .map(|value| vec![("Retry-After", value)])
            .unwrap_or_default();
        let response = http_response_with_headers(
            "429 Too Many Requests",
            Some("application/problem+json"),
            &headers,
            &body,
        );
        let (server, _directory, credential_path) = prepared_project(vec![response]);

        let output = run_project(
            &[
                "project",
                "create",
                ORGANIZATION,
                "--name",
                "widget",
                "--installation-id",
                INSTALLATION_ID,
                "--repository-id",
                REPOSITORY_ID,
                "--json",
                "--allow-insecure-http",
            ],
            &server,
            &credential_path,
        );

        assert_eq!(output.status.code(), Some(1));
        assert!(output.stderr.is_empty());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
            "invalid_response"
        );
        server.finish();
    }
}

#[test]
fn inconsistent_project_readiness_is_an_invalid_response() {
    let mut project = project_body(
        "widget",
        false,
        Some(repository_body("acme/widget", "main")),
    );
    project["executionReadiness"] = serde_json::json!({"ready": true, "blockers": []});
    let (server, _directory, credential_path) =
        prepared_project(vec![json_http_response("200 OK", project)]);

    let output = run_project(
        &[
            "project",
            "show",
            ORGANIZATION,
            PROJECT_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &server,
        &credential_path,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "invalid_response"
    );
    server.finish();
}
