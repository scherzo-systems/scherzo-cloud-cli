use super::*;

const TOKEN: &str = "unique-account-update-token-sentinel";

fn prepared_update(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, String) {
    let server = ScriptedServer::respond(responses);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let credential_path = credential_path.to_str().unwrap().to_owned();
    (server, credential_directory, credential_path)
}

fn updated_response(display_name: Option<&str>) -> Vec<u8> {
    let mut principal = serde_json::json!({
        "id": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
        "type": "human",
        "state": "active"
    });
    if let Some(display_name) = display_name {
        principal["displayName"] = serde_json::Value::String(display_name.to_owned());
    }
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&principal).unwrap(),
    )
}

fn update_problem(status_text: &str, status: u16, problem_type: &str) -> Vec<u8> {
    problem_http_response(
        status_text,
        serde_json::json!({
            "type": problem_type,
            "title": "account-update-problem-title-sentinel",
            "status": status,
            "detail": "account-update-problem-detail-sentinel"
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
fn account_update_sets_the_display_name_through_the_profile_patch() {
    let (server, _directory, credential_path) =
        prepared_update(vec![updated_response(Some("Countess of Lovelace"))]);
    let environment =
        deployment_environment_with_issuer(&server.api_url, &server.issuer, &credential_path);

    let output = run_with_env(
        &[
            "account",
            "update",
            "--display-name",
            "\u{2003}Countess of Lovelace\u{2003}",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!(
            concat!(
                "✓ Your account display name is set.\n\n",
                "  Display name: Countess of Lovelace\n",
                "  Principal:    prn_01k0z6r1w8f4jy2m7q9v3x5abc\n",
                "  Deployment:   {}\n"
            ),
            server.api_url
        )
    );
    assert!(output.stderr.is_empty());

    let request = server.finish().pop().unwrap();
    assert!(request.starts_with("PATCH /api/v1/me HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {TOKEN}")
    );
    assert_eq!(
        header_value(&request, "content-type"),
        "application/merge-patch+json"
    );
    assert_eq!(
        header_value(&request, "accept"),
        "application/json, application/problem+json"
    );
    let idempotency_key = header_value(&request, "idempotency-key");
    assert_eq!(idempotency_key.len(), 64);
    assert!(
        idempotency_key
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&request)).unwrap(),
        serde_json::json!({"displayName": "\u{2003}Countess of Lovelace\u{2003}"})
    );
}

#[test]
fn account_update_clears_the_display_name_with_an_explicit_null() {
    let (server, _directory, credential_path) = prepared_update(vec![updated_response(None)]);
    let environment =
        deployment_environment_with_issuer(&server.api_url, &server.issuer, &credential_path);

    let output = run_with_env(
        &[
            "account",
            "update",
            "--clear-display-name",
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
            "outcome": "cleared",
            "principal": {
                "id": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
                "type": "human",
                "state": "active"
            }
        })
    );
    assert!(output.stdout.ends_with(b"\n"));
    assert!(output.stderr.is_empty());
    let request = server.finish().pop().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&request)).unwrap(),
        serde_json::json!({"displayName": null})
    );
}

#[test]
fn an_equivalent_display_name_no_op_has_the_same_stable_result() {
    let response = updated_response(Some("Ada Lovelace"));
    let (server, _directory, credential_path) = prepared_update(vec![response.clone(), response]);
    let environment =
        deployment_environment_with_issuer(&server.api_url, &server.issuer, &credential_path);
    let args = [
        "account",
        "update",
        "--display-name",
        "Ada Lovelace",
        "--json",
        "--allow-insecure-http",
    ];

    let changed = run_with_env(&args, &environment);
    let unchanged = run_with_env(&args, &environment);

    assert!(changed.status.success());
    assert!(unchanged.status.success());
    assert_eq!(changed.stdout, unchanged.stdout);
    let result: serde_json::Value = serde_json::from_slice(&unchanged.stdout).unwrap();
    assert_eq!(result["outcome"], "set");
    assert_eq!(result["principal"]["displayName"], "Ada Lovelace");
    assert!(changed.stderr.is_empty());
    assert!(unchanged.stderr.is_empty());
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn an_ambiguous_update_retries_the_same_patch_and_request_identity() {
    let (server, _directory, credential_path) =
        prepared_update(vec![Vec::new(), updated_response(Some("Ada Lovelace"))]);
    let environment =
        deployment_environment_with_issuer(&server.api_url, &server.issuer, &credential_path);

    let output = run_with_env(
        &[
            "account",
            "update",
            "--display-name",
            "Ada Lovelace",
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
fn a_rejected_account_update_token_is_refreshed_and_retried() {
    let unauthorized = update_problem(
        "401 Unauthorized",
        401,
        "https://api.scherzo.dev/problems/unauthorized",
    );
    let (server, _directory, credential_path) = prepared_update(vec![
        unauthorized,
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-refreshed-account-update-token",
                "refresh_token": "unique-refreshed-account-update-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        updated_response(Some("Ada Lovelace")),
    ]);
    let environment =
        deployment_environment_with_issuer(&server.api_url, &server.issuer, &credential_path);

    let output = run_with_env(
        &[
            "account",
            "update",
            "--display-name",
            "Ada Lovelace",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].starts_with("PATCH /api/v1/me HTTP/1.1\r\n"));
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&requests[2], "authorization"),
        "Bearer unique-refreshed-account-update-token"
    );
}

#[test]
fn account_update_reports_meaningful_api_failures_without_problem_prose() {
    let cases = [
        (
            update_problem(
                "400 Bad Request",
                400,
                "https://api.scherzo.dev/problems/invalid-display-name",
            ),
            "invalid_display_name",
            1,
            None,
        ),
        (
            update_problem(
                "403 Forbidden",
                403,
                "https://api.scherzo.dev/problems/forbidden",
            ),
            "forbidden",
            1,
            None,
        ),
        (
            update_problem(
                "409 Conflict",
                409,
                "https://api.scherzo.dev/problems/idempotency-conflict",
            ),
            "idempotency_conflict",
            1,
            None,
        ),
        (
            update_problem(
                "413 Payload Too Large",
                413,
                "https://api.scherzo.dev/problems/request-body-too-large",
            ),
            "request_too_large",
            1,
            None,
        ),
        (
            update_problem(
                "415 Unsupported Media Type",
                415,
                "https://api.scherzo.dev/problems/unsupported-media-type",
            ),
            "unsupported_media_type",
            1,
            None,
        ),
        (
            http_response("500 Internal Server Error", None, &[]),
            "unreachable",
            4,
            Some("server"),
        ),
    ];

    for (response, expected_outcome, expected_status, expected_category) in cases {
        let (server, _directory, credential_path) = prepared_update(vec![response]);
        let environment =
            deployment_environment_with_issuer(&server.api_url, &server.issuer, &credential_path);
        let output = run_with_env(
            &[
                "account",
                "update",
                "--display-name",
                "Ada",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(expected_status));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["schemaVersion"], 1);
        assert_eq!(result["deployment"], server.api_url);
        assert_eq!(result["outcome"], expected_outcome);
        if let Some(category) = expected_category {
            assert_eq!(result["category"], category);
        } else {
            assert!(result.get("category").is_none());
        }
        assert!(result.get("title").is_none());
        assert!(result.get("detail").is_none());
        assert!(output.stderr.is_empty());
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn rejected_display_name_has_a_stable_human_remedy() {
    let response = update_problem(
        "400 Bad Request",
        400,
        "https://api.scherzo.dev/problems/invalid-display-name",
    );
    let (server, _directory, credential_path) = prepared_update(vec![response]);
    let environment =
        deployment_environment_with_issuer(&server.api_url, &server.issuer, &credential_path);

    let output = run_with_env(
        &[
            "account",
            "update",
            "--display-name",
            " ",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        output.stdout,
        b"! Your display name was rejected.\n\nUse a name that normalizes to 1 through 200 Unicode scalar values and contains no control characters.\n"
    );
    assert!(output.stderr.is_empty());
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn account_update_requires_exactly_one_display_name_operation() {
    for args in [
        &["account", "update"][..],
        &[
            "account",
            "update",
            "--display-name",
            "Ada",
            "--clear-display-name",
        ][..],
    ] {
        let output = run(args);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
    }
}
