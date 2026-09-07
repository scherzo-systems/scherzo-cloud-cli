use super::*;

const TOKEN: &str = "unique-organization-audit-token-sentinel";

fn audit_page(next_cursor: Option<&str>) -> serde_json::Value {
    let mut page = serde_json::json!({
        "items": [
            {
                "id": "aud_01k0z6r1w8f4jy2m7q9v3x5abc",
                "occurredAt": "2026-09-05T12:00:00Z",
                "retention": {
                    "identifier": "identity-tenancy-production-730d-v1",
                    "retainUntil": "2028-09-04T12:00:00Z"
                },
                "detailsStatus": "details_available",
                "actor": {
                    "kind": "principal",
                    "principalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abc"
                },
                "delegatingPrincipalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abd",
                "action": "project.repository_bound",
                "subject": {
                    "kind": "project",
                    "id": "prj_01k0z6r1w8f4jy2m7q9v3x5abc"
                },
                "changes": [
                    {
                        "field": "repository_connection_id",
                        "after": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc"
                    }
                ],
                "future": { "omittedFromStableOutput": true }
            },
            {
                "id": "aud_01k0z6r1w8f4jy2m7q9v3x5abd",
                "occurredAt": "2026-09-05T12:01:00Z",
                "retention": {
                    "identifier": "identity-tenancy-production-730d-v1",
                    "retainUntil": "2028-09-04T12:01:00Z"
                },
                "detailsStatus": "details_unavailable"
            }
        ],
        "warnings": [
            {
                "recordId": "aud_01k0z6r1w8f4jy2m7q9v3x5abd",
                "reason": "unknown_action"
            }
        ],
        "futurePageField": true
    });
    if let Some(next_cursor) = next_cursor {
        page["nextCursor"] = serde_json::Value::String(next_cursor.to_owned());
    }
    page
}

fn audit_success(next_cursor: Option<&str>) -> Vec<u8> {
    json_http_response("200 OK", audit_page(next_cursor))
}

#[test]
fn organization_audit_list_preserves_one_privacy_safe_page_as_stable_json() {
    let cursor = "next page /+=";
    let (server, _directory, _path, credential_path) =
        organization::prepared_organization(vec![audit_success(Some(cursor))], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "audit",
            "list",
            "acme-research",
            "--limit",
            "100",
            "--cursor",
            "opaque /+=?&",
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
                    "id": "aud_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "occurredAt": "2026-09-05T12:00:00Z",
                    "retention": {
                        "identifier": "identity-tenancy-production-730d-v1",
                        "retainUntil": "2028-09-04T12:00:00Z"
                    },
                    "detailsStatus": "details_available",
                    "actor": {
                        "kind": "principal",
                        "principalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abc"
                    },
                    "delegatingPrincipalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abd",
                    "action": "project.repository_bound",
                    "subject": {
                        "kind": "project",
                        "id": "prj_01k0z6r1w8f4jy2m7q9v3x5abc"
                    },
                    "changes": [
                        {
                            "field": "repository_connection_id",
                            "after": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc"
                        }
                    ]
                },
                {
                    "id": "aud_01k0z6r1w8f4jy2m7q9v3x5abd",
                    "occurredAt": "2026-09-05T12:01:00Z",
                    "retention": {
                        "identifier": "identity-tenancy-production-730d-v1",
                        "retainUntil": "2028-09-04T12:01:00Z"
                    },
                    "detailsStatus": "details_unavailable"
                }
            ],
            "nextCursor": cursor,
            "warnings": [
                {
                    "recordId": "aud_01k0z6r1w8f4jy2m7q9v3x5abd",
                    "reason": "unknown_action"
                }
            ]
        })
    );
    assert!(output.stdout.ends_with(b"\n"));
    assert!(output.stderr.is_empty());

    let requests = server.finish();
    assert_eq!(requests.len(), 1, "the CLI must return exactly one page");
    assert!(requests[0].starts_with(
        "GET /api/v1/organizations/acme-research/audit-records?limit=100&cursor=opaque+%2F%2B%3D%3F%26 HTTP/1.1\r\n"
    ));
    assert_eq!(
        header_value(&requests[0], "authorization"),
        format!("Bearer {TOKEN}")
    );
    assert!(!requests[0].contains("idempotency-key:"));
}

#[test]
fn human_audit_list_identifies_available_details_without_inventing_unavailable_details() {
    let (server, _directory, _path, credential_path) =
        organization::prepared_organization(vec![audit_success(None)], TOKEN);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "audit",
            "list",
            "acme-research",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    for expected in [
        "time: 2026-09-05T12:00:00Z",
        "actor: principal prn_01k0z6r1w8f4jy2m7q9v3x5abc",
        "action: project.repository_bound",
        "target: project prj_01k0z6r1w8f4jy2m7q9v3x5abc",
        "retention: identity-tenancy-production-730d-v1 · retain until: 2028-09-04T12:00:00Z",
        "warning: aud_01k0z6r1w8f4jy2m7q9v3x5abd · reason: unknown_action",
    ] {
        assert!(stdout.lines().any(|line| line == expected));
    }
    let unavailable = stdout
        .split("record: aud_01k0z6r1w8f4jy2m7q9v3x5abd")
        .nth(1)
        .expect("unavailable record should be rendered");
    assert!(unavailable.contains("details: unavailable"));
    assert!(!unavailable.lines().any(|line| {
        line.starts_with("actor: ") || line.starts_with("action: ") || line.starts_with("target: ")
    }));
    assert!(output.stderr.is_empty());
    server.finish();
}

#[test]
fn organization_audit_api_failures_have_closed_json_and_registered_statuses() {
    let cases = [
        (
            organization::organization_problem(
                "400 Bad Request",
                400,
                "https://api.scherzo.dev/problems/bad-request",
            ),
            "invalid_input",
            1,
        ),
        (
            organization::organization_problem(
                "401 Unauthorized",
                401,
                "https://api.scherzo.dev/problems/unauthorized",
            ),
            "unauthenticated",
            3,
        ),
        (
            organization::organization_problem(
                "403 Forbidden",
                403,
                "https://api.scherzo.dev/problems/forbidden",
            ),
            "forbidden",
            1,
        ),
        (
            organization::organization_problem(
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
            organization::prepared_organization_refresh(vec![response], TOKEN);
        let environment =
            deployment_environment_with_issuer(&server.api_url, &server.issuer, &credential_path);
        let output = run_with_env(
            &[
                "organization",
                "audit",
                "list",
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
        } else {
            assert!(value.get("category").is_none());
        }
        assert!(value.get("title").is_none());
        assert!(value.get("detail").is_none());
        assert!(output.stderr.is_empty());
        assert_eq!(
            server.finish().len(),
            if expected_outcome == "unauthenticated" {
                2
            } else {
                1
            }
        );
    }
}

#[test]
fn organization_audit_rejects_invalid_filters_before_loading_deployment() {
    for args in [
        &["organization", "audit", "list", "acme/research"][..],
        &["organization", "audit", "list", "acme", "--limit", "0"][..],
        &["organization", "audit", "list", "acme", "--limit", "101"][..],
        &["organization", "audit", "list", "acme", "--cursor", ""][..],
    ] {
        let output = run_with_env(
            args,
            &[("SCHERZO_CLOUD_API_URL", "partial-override-must-not-load")],
        );
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
    }
}
