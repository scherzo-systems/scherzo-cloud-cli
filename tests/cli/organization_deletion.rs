use super::*;

const ORGANIZATION_TOKEN: &str = "unique-organization-deletion-session-token";
const ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";

fn organization_schedule_response() -> Vec<u8> {
    http_response_with_headers(
        "202 Accepted",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "id": ORGANIZATION_ID,
            "kind": "organization",
            "state": "deletion_pending",
            "requestedAt": "2026-09-07T09:30:00Z",
            "deadline": "2026-10-07T09:30:00Z",
            "updatedAt": "2026-09-07T09:30:00Z"
        }))
        .unwrap(),
    )
}

fn organization_cancellation_response() -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "id": ORGANIZATION_ID,
            "kind": "organization",
            "state": "suspended",
            "updatedAt": "2026-09-08T10:45:00Z"
        }))
        .unwrap(),
    )
}

fn organization_lifecycle_problem(status: &str, code: u16, problem_type: &str) -> Vec<u8> {
    problem_http_response(
        status,
        serde_json::json!({
            "type": problem_type,
            "title": "organization-lifecycle-title-sentinel",
            "status": code,
            "detail": "organization-lifecycle-detail-sentinel"
        }),
    )
}

fn organization_cancellation_flow(terminal: Vec<u8>) -> Vec<Vec<u8>> {
    vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "unique-organization-cancellation-device-code",
                "user_code": "KEEP-ORG",
                "verification_uri": "https://auth.fixture.example/activate",
                "verification_uri_complete": "https://auth.fixture.example/activate?user_code=KEEP-ORG",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-organization-cancellation-proof",
                "token_type": "Bearer",
                "expires_in": 300
            }),
        ),
        terminal,
    ]
}

fn prepared_organization_deletion(
    responses: Vec<Vec<u8>>,
) -> (ScriptedServer, tempfile::TempDir, std::path::PathBuf) {
    let server = ScriptedServer::respond(responses);
    let directory = private_credential_directory();
    let credential_path = directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        ORGANIZATION_TOKEN,
        "2999-01-01T00:00:00Z",
    );
    (server, directory, credential_path)
}

#[test]
fn organization_deletion_request_returns_the_schedule_and_retains_the_account_session() {
    let (server, _directory, credential_path) =
        prepared_organization_deletion(vec![organization_schedule_response()]);
    let before = fs::read(&credential_path).unwrap();
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "organization",
            "deletion",
            "request",
            "acme-research",
            "--yes",
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
            "outcome": "scheduled",
            "organizationRef": "acme-research",
            "schedule": {
                "id": ORGANIZATION_ID,
                "kind": "organization",
                "state": "deletion_pending",
                "requestedAt": "2026-09-07T09:30:00Z",
                "deadline": "2026-10-07T09:30:00Z",
                "updatedAt": "2026-09-07T09:30:00Z"
            },
            "localCredential": "unchanged"
        })
    );
    assert!(output.stdout.ends_with(b"\n"));
    assert!(output.stderr.is_empty());
    assert_eq!(fs::read(&credential_path).unwrap(), before);

    let request = server.finish().pop().unwrap();
    assert!(request.starts_with("POST /api/v1/organizations/acme-research/deletion HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {ORGANIZATION_TOKEN}")
    );
    assert_eq!(request.split_once("\r\n\r\n").unwrap().1, "");
}

#[test]
fn organization_deletion_request_preserves_owner_and_private_target_failures() {
    for (status, code, problem_type, expected_outcome) in [
        (
            "403 Forbidden",
            403,
            "https://api.scherzo.dev/problems/forbidden",
            "forbidden",
        ),
        (
            "404 Not Found",
            404,
            "https://api.scherzo.dev/problems/not-found",
            "not_found",
        ),
        (
            "409 Conflict",
            409,
            "https://api.scherzo.dev/problems/lifecycle-transition-unavailable",
            "transition_unavailable",
        ),
    ] {
        let response = organization_lifecycle_problem(status, code, problem_type);
        let (server, _directory, credential_path) = prepared_organization_deletion(vec![response]);
        let environment = deployment_environment_with_issuer(
            &server.api_url,
            &server.issuer,
            credential_path.to_str().unwrap(),
        );

        let output = run_with_env(
            &[
                "organization",
                "deletion",
                "request",
                "private-target",
                "--yes",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], expected_outcome);
        assert_eq!(result["organizationRef"], "private-target");
        assert!(result.get("schedule").is_none());
        assert!(result.get("title").is_none());
        assert!(result.get("detail").is_none());
        assert!(output.stderr.is_empty());
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn organization_deletion_cancellation_uses_browser_proof_without_replacing_the_session() {
    let (server, _directory, credential_path) = prepared_organization_deletion(
        organization_cancellation_flow(organization_cancellation_response()),
    );
    let before = fs::read(&credential_path).unwrap();
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "organization",
            "deletion",
            "cancel",
            "acme-research",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    for expected in [
        "Cancel organization deletion",
        "open: https://auth.fixture.example/activate?user_code=KEEP-ORG",
        "code: KEEP-ORG",
        "✓ Organization deletion cancelled.",
        &format!("organization: {ORGANIZATION_ID}"),
        "state: suspended",
        "updated: 2026-09-08T10:45:00Z",
        "local credential: unchanged",
        "cancellation proof credential: not stored",
        &format!("deployment: {}", server.api_url),
    ] {
        assert!(
            stdout.contains(expected),
            "missing {expected:?} in {stdout:?}"
        );
    }
    assert_eq!(output.stderr, b"Waiting for authorization...\n");
    assert_eq!(fs::read(&credential_path).unwrap(), before);

    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        request_form(&requests[0]).get("scope").map(String::as_str),
        Some("openid profile email")
    );
    assert!(
        requests[2].starts_with("DELETE /api/v1/organizations/acme-research/deletion HTTP/1.1\r\n")
    );
    assert_eq!(
        header_value(&requests[2], "authorization"),
        "Bearer unique-organization-cancellation-proof"
    );
    let combined = format!("{stdout}{}", String::from_utf8_lossy(&output.stderr));
    assert!(!combined.contains("unique-organization-cancellation-proof"));
    assert!(!combined.contains(ORGANIZATION_TOKEN));
}

#[test]
fn organization_deletion_cancellation_does_not_weaken_owner_or_actor_proof() {
    let terminal = organization_lifecycle_problem(
        "403 Forbidden",
        403,
        "https://api.scherzo.dev/problems/reauthentication-required",
    );
    let (server, _directory, credential_path) =
        prepared_organization_deletion(organization_cancellation_flow(terminal));
    let before = fs::read(&credential_path).unwrap();
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "organization",
            "deletion",
            "cancel",
            "acme-research",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let events = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["operation"], "organization_deletion_cancellation");
    assert_eq!(events[1]["outcome"], "reauthentication_required");
    assert_eq!(events[1]["organizationRef"], "acme-research");
    assert_eq!(events[1]["localCredential"], "unchanged");
    assert_eq!(events[1]["proofCredentialStored"], false);
    assert!(output.stderr.is_empty());
    assert_eq!(fs::read(&credential_path).unwrap(), before);
    assert_eq!(server.finish().len(), 3);
}

#[test]
fn organization_deletion_rejects_invalid_targets_and_missing_confirmation_locally() {
    for args in [
        &[
            "organization",
            "deletion",
            "request",
            "acme/research",
            "--yes",
        ][..],
        &["organization", "deletion", "request", "acme-research"][..],
        &["organization", "deletion", "cancel", "acme/research"][..],
    ] {
        let output = run_with_env(
            args,
            &[("SCHERZO_CLOUD_API_URL", "partial-override-must-not-load")],
        );
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
    }
}
