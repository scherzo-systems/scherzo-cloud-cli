use std::fs::{OpenOptions, Permissions};
use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

use super::*;

const TOKEN: &str = "unique-invitation-command-token-sentinel";
const ORG_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
const PRINCIPAL_ID: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5abc";
const INVITATION_ID: &str = "inv_01k0z6r1w8f4jy2m7q9v3x5abc";
const MEMBERSHIP_ID: &str = "mem_01k0z6r1w8f4jy2m7q9v3x5abc";
const CAPABILITY: &str =
    "inv_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn invitation(state: &str, terminal_at: Option<&str>) -> serde_json::Value {
    let mut value = serde_json::json!({
        "id": INVITATION_ID,
        "organizationId": ORG_ID,
        "issuerPrincipalId": PRINCIPAL_ID,
        "targetKind": "principal",
        "state": state,
        "issuedAt": "2026-01-02T03:04:05Z",
        "expiresAt": "2026-01-09T03:04:05Z"
    });
    if state == "outstanding" {
        value["targetPrincipalId"] = PRINCIPAL_ID.into();
    }
    if let Some(terminal_at) = terminal_at {
        value["terminalAt"] = terminal_at.into();
    }
    value
}

fn prepared(
    responses: Vec<Vec<u8>>,
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
        TOKEN,
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

fn write_capability(path: &Path, contents: &str, mode: u32) {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)
        .expect("create capability file");
    // Creation modes are masked by the process umask; fixtures need the exact mode.
    file.set_permissions(Permissions::from_mode(mode))
        .expect("set capability file permissions");
    writeln!(file, "{contents}").expect("write capability file");
}

fn json_response_with_headers(
    status: &str,
    value: &serde_json::Value,
    headers: &[(&str, &str)],
) -> Vec<u8> {
    http_response_with_headers(
        status,
        Some("application/json"),
        headers,
        &serde_json::to_vec(value).unwrap(),
    )
}

fn problem_response_with_headers(
    status: &str,
    problem_type: &str,
    status_code: u16,
    headers: &[(&str, &str)],
) -> Vec<u8> {
    http_response_with_headers(
        status,
        Some("application/problem+json"),
        headers,
        &serde_json::to_vec(&serde_json::json!({
            "type": problem_type,
            "title": "Invitation request failed",
            "status": status_code
        }))
        .unwrap(),
    )
}

fn request_body(request: &str) -> &str {
    request.split_once("\r\n\r\n").unwrap().1
}

fn assert_authenticated(request: &str) {
    assert_eq!(
        header_value(request, "authorization"),
        format!("Bearer {TOKEN}")
    );
}

#[test]
fn organization_invitation_issue_sends_target_and_idempotency_key() {
    let location = format!("/v1/invitations/{INVITATION_ID}");
    let (server, _directory, _path, credential_path) = prepared(vec![json_response_with_headers(
        "201 Created",
        &invitation("outstanding", None),
        &[
            ("idempotency-key", ECHO_IDEMPOTENCY_KEY),
            ("location", &location),
        ],
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "invitations",
            "issue",
            ORG_ID,
            "--principal",
            PRINCIPAL_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"outcome\": \"issued\""));
    assert!(stdout.contains(INVITATION_ID));
    assert!(output.stderr.is_empty());
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "POST /api/v1/organizations/{ORG_ID}/invitations HTTP/1.1\r\n"
    )));
    assert_authenticated(&request);
    assert!(!header_value(&request, "idempotency-key").is_empty());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&request)).unwrap(),
        serde_json::json!({ "kind": "principal", "principalId": PRINCIPAL_ID })
    );
}

#[test]
fn organization_invitation_list_preserves_terminal_history_and_cursor() {
    let accepted = invitation("accepted", Some("2026-01-03T03:04:05Z"));
    let mut revoked = invitation("revoked", Some("2026-01-04T03:04:05Z"));
    revoked["id"] = "inv_01k0z6r1w8f4jy2m7q9v3x5abd".into();
    let mut expired = invitation("expired", Some("2026-01-09T03:04:05Z"));
    expired["id"] = "inv_01k0z6r1w8f4jy2m7q9v3x5abe".into();
    let (server, _directory, _path, credential_path) = prepared(vec![json_http_response(
        "200 OK",
        serde_json::json!({
            "items": [accepted, revoked, expired],
            "nextCursor": "page-two"
        }),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "invitations",
            "list",
            ORG_ID,
            "--limit",
            "3",
            "--cursor",
            "page-one",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"state\": \"accepted\""));
    assert!(stdout.contains("\"state\": \"revoked\""));
    assert!(stdout.contains("\"state\": \"expired\""));
    assert!(stdout.contains("\"nextCursor\": \"page-two\""));
    assert!(output.stderr.is_empty());
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "GET /api/v1/organizations/{ORG_ID}/invitations?limit=3&cursor=page-one HTTP/1.1\r\n"
    )));
    assert_authenticated(&request);
}

#[test]
fn invitation_inbox_and_preview_are_available_to_the_current_principal() {
    let (server, _directory, _path, credential_path) = prepared(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "items": [{
                    "id": INVITATION_ID,
                    "organizationId": ORG_ID,
                    "organizationDisplayName": "Example Organization",
                    "organizationSlug": "example",
                    "issuerPrincipalId": PRINCIPAL_ID,
                    "expiresAt": "2026-01-09T03:04:05Z"
                }]
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "id": INVITATION_ID,
                "organizationId": ORG_ID,
                "organizationDisplayName": "Example Organization",
                "organizationSlug": "example",
                "targetKind": "email",
                "expiresAt": "2026-01-09T03:04:05Z"
            }),
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let inbox = run_with_env(
        &["invitation", "list", "--allow-insecure-http"],
        &environment,
    );
    assert!(inbox.status.success(), "{inbox:?}");
    let stdout = String::from_utf8(inbox.stdout).unwrap();
    assert!(stdout.contains("Invitation inbox listed"));
    assert!(stdout.contains("Example Organization"));
    assert!(inbox.stderr.is_empty());

    let preview = run_with_env(
        &[
            "invitation",
            "preview",
            INVITATION_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(preview.status.success(), "{preview:?}");
    let stdout = String::from_utf8(preview.stdout).unwrap();
    assert!(stdout.contains("\"outcome\": \"previewed\""));
    assert!(stdout.contains("\"targetKind\": \"email\""));
    assert!(preview.stderr.is_empty());

    let requests = server.finish();
    assert!(requests[0].starts_with("GET /api/v1/me/invitations HTTP/1.1\r\n"));
    assert!(requests[1].starts_with(&format!(
        "POST /api/v1/invitations/{INVITATION_ID}/preview HTTP/1.1\r\n"
    )));
    assert_eq!(request_body(&requests[1]), "{}");
    requests
        .iter()
        .for_each(|request| assert_authenticated(request));
}

#[test]
fn invitation_accept_reads_a_protected_capability_file_without_printing_it() {
    let (server, capability_directory, _path, credential_path) =
        prepared(vec![json_response_with_headers(
            "200 OK",
            &serde_json::json!({
                "id": MEMBERSHIP_ID,
                "organizationId": ORG_ID,
                "principalId": PRINCIPAL_ID,
                "role": "member",
                "state": "active",
                "createdAt": "2026-01-02T03:04:05Z",
                "updatedAt": "2026-01-02T03:04:05Z"
            }),
            &[("idempotency-key", ECHO_IDEMPOTENCY_KEY)],
        )]);
    let capability_path = capability_directory.path().join("invitation.capability");
    write_capability(&capability_path, CAPABILITY, 0o600);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "invitation",
            "accept",
            INVITATION_ID,
            "--capability-file",
            capability_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stdout.contains("\"outcome\": \"accepted\""));
    assert!(!stdout.contains(CAPABILITY));
    assert!(!stderr.contains(CAPABILITY));
    let request = server.finish().pop().unwrap();
    assert_authenticated(&request);
    assert!(!header_value(&request, "idempotency-key").is_empty());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&request)).unwrap(),
        serde_json::json!({ "capability": CAPABILITY })
    );
}

#[test]
fn invitation_decline_and_owner_revoke_have_deterministic_success_output() {
    let (server, _directory, _path, credential_path) = prepared(vec![
        http_response_with_headers(
            "204 No Content",
            None,
            &[("idempotency-key", ECHO_IDEMPOTENCY_KEY)],
            &[],
        ),
        http_response_with_headers(
            "204 No Content",
            None,
            &[("idempotency-key", ECHO_IDEMPOTENCY_KEY)],
            &[],
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let decline = run_with_env(
        &[
            "invitation",
            "decline",
            INVITATION_ID,
            "--yes",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(decline.status.success(), "{decline:?}");
    assert!(
        String::from_utf8(decline.stdout)
            .unwrap()
            .contains("Invitation declined")
    );
    assert!(decline.stderr.is_empty());

    let revoke = run_with_env(
        &[
            "organization",
            "invitations",
            "revoke",
            ORG_ID,
            INVITATION_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(revoke.status.success(), "{revoke:?}");
    assert!(
        String::from_utf8(revoke.stdout)
            .unwrap()
            .contains("\"outcome\": \"revoked\"")
    );
    assert!(revoke.stderr.is_empty());

    let requests = server.finish();
    assert!(requests[0].starts_with(&format!(
        "POST /api/v1/invitations/{INVITATION_ID}/decline HTTP/1.1\r\n"
    )));
    assert!(requests[1].starts_with(&format!(
        "DELETE /api/v1/organizations/{ORG_ID}/invitations/{INVITATION_ID} HTTP/1.1\r\n"
    )));
    requests.iter().for_each(|request| {
        assert_authenticated(request);
        assert!(!header_value(request, "idempotency-key").is_empty());
    });
}

#[test]
fn capability_file_rejects_group_access_without_printing_the_secret() {
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        "http://127.0.0.1:9/api",
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let capability_path = credential_directory.path().join("unsafe.capability");
    write_capability(&capability_path, CAPABILITY, 0o640);
    let environment =
        deployment_environment("http://127.0.0.1:9/api", credential_path.to_str().unwrap());

    let output = run_with_env(
        &[
            "invitation",
            "preview",
            INVITATION_ID,
            "--capability-file",
            capability_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stdout.contains("\"outcome\": \"capability_source_unavailable\""));
    assert!(!stdout.contains(CAPABILITY));
    assert!(!stderr.contains(CAPABILITY));
}

#[test]
fn invitation_capability_must_match_the_path_invitation() {
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        "http://127.0.0.1:9/api",
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let capability_path = credential_directory.path().join("invitation.capability");
    write_capability(
        &capability_path,
        "inv_01k0z6r1w8f4jy2m7q9v3x5abd.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        0o600,
    );
    let environment =
        deployment_environment("http://127.0.0.1:9/api", credential_path.to_str().unwrap());

    let output = run_with_env(
        &[
            "invitation",
            "accept",
            INVITATION_ID,
            "--capability-file",
            capability_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stdout.contains("\"outcome\": \"invalid_capability\""));
    assert!(!stdout.contains("AAAAAAAA"));
    assert!(!stderr.contains("AAAAAAAA"));
}

#[test]
fn unavailable_invitation_returns_stable_json_and_exit_code() {
    let (server, _directory, _path, credential_path) = prepared(vec![problem_http_response(
        "409 Conflict",
        serde_json::json!({
            "type": "https://api.scherzo.dev/problems/invitation-unavailable",
            "title": "Invitation unavailable",
            "status": 409
        }),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "invitation",
            "preview",
            INVITATION_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("\"outcome\": \"invitation_unavailable\"")
    );
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn invitation_api_failures_use_stable_outcomes_and_exit_codes() {
    let (server, _directory, _path, credential_path) = prepared(vec![
        problem_response_with_headers(
            "403 Forbidden",
            "https://api.scherzo.dev/problems/forbidden",
            403,
            &[],
        ),
        problem_response_with_headers(
            "429 Too Many Requests",
            "https://api.scherzo.dev/problems/rate-limit-exceeded",
            429,
            &[("Retry-After", "17")],
        ),
        problem_response_with_headers(
            "409 Conflict",
            "https://api.scherzo.dev/problems/membership-limit-reached",
            409,
            &[],
        ),
        problem_response_with_headers(
            "409 Conflict",
            "https://api.scherzo.dev/problems/idempotency-conflict",
            409,
            &[],
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let cases = [
        (
            vec![
                "organization",
                "invitations",
                "list",
                ORG_ID,
                "--json",
                "--allow-insecure-http",
            ],
            1,
            "forbidden",
            None,
        ),
        (
            vec![
                "organization",
                "invitations",
                "issue",
                ORG_ID,
                "--email",
                "teammate@example.com",
                "--json",
                "--allow-insecure-http",
            ],
            4,
            "rate_limited",
            Some(17),
        ),
        (
            vec![
                "invitation",
                "accept",
                INVITATION_ID,
                "--json",
                "--allow-insecure-http",
            ],
            1,
            "membership_limit_reached",
            None,
        ),
        (
            vec![
                "invitation",
                "decline",
                INVITATION_ID,
                "--yes",
                "--json",
                "--allow-insecure-http",
            ],
            1,
            "idempotency_conflict",
            None,
        ),
    ];

    for (arguments, exit_code, outcome, retry_after) in cases {
        let output = run_with_env(&arguments, &environment);
        assert_eq!(output.status.code(), Some(exit_code), "{output:?}");
        assert!(output.stderr.is_empty());
        let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(document["schemaVersion"], 1);
        assert_eq!(document["outcome"], outcome);
        if let Some(retry_after) = retry_after {
            assert_eq!(document["retryAfter"], retry_after);
        }
    }

    assert_eq!(server.finish().len(), 4);
}
