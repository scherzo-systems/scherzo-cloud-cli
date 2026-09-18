use super::*;

const HUMAN_TOKEN: &str = "delegation-human-token";
const HUMAN_PRINCIPAL_ID: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5abc";
const SERVICE_PRINCIPAL_ID: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5abd";
const DELEGATION_ID: &str = "dlg_01k0z6r1w8f4jy2m7q9v3x5abc";
const SERVICE_API_KEY: &str =
    "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const PROPOSED_AT: &str = "2026-01-02T03:04:05Z";
const ACCEPTED_AT: &str = "2026-01-03T03:04:05Z";
const ENDED_AT: &str = "2026-01-04T03:04:05Z";

fn delegation(state: &str) -> serde_json::Value {
    let mut value = serde_json::json!({
        "id": DELEGATION_ID,
        "humanPrincipalId": HUMAN_PRINCIPAL_ID,
        "servicePrincipalId": SERVICE_PRINCIPAL_ID,
        "state": state,
        "proposedAt": PROPOSED_AT
    });
    if matches!(state, "active" | "ended") {
        value["acceptedAt"] = ACCEPTED_AT.into();
    }
    if state == "ended" {
        value["endedAt"] = ENDED_AT.into();
        value["terminalReason"] = "participant_ended".into();
    }
    value
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

fn problem_response(status: &str, problem_type: &str, status_code: u16) -> Vec<u8> {
    problem_http_response(
        status,
        serde_json::json!({
            "type": problem_type,
            "title": "Delegation request rejected",
            "status": status_code
        }),
    )
}

fn prepared(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, String, String) {
    let server = ScriptedServer::respond(responses);
    let directory = private_credential_directory();
    let human_path = directory.path().join("human.json");
    write_credential_fixture(
        &human_path,
        &server.api_url,
        HUMAN_TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let service_path = directory.path().join("service.key");
    fs::write(&service_path, format!("{SERVICE_API_KEY}\n")).unwrap();
    fs::set_permissions(&service_path, Permissions::from_mode(0o600)).unwrap();
    (
        server,
        directory,
        human_path.to_string_lossy().into_owned(),
        service_path.to_string_lossy().into_owned(),
    )
}

fn assert_authorization(request: &str, credential: &str) {
    assert_eq!(
        header_value(request, "authorization"),
        format!("Bearer {credential}")
    );
}

#[test]
fn delegation_workflow_preserves_actor_separation_history_and_pagination() {
    let location = format!("/v1/delegations/{DELEGATION_ID}");
    let (server, _directory, human_credentials, service_key) = prepared(vec![
        json_response_with_headers(
            "201 Created",
            &delegation("pending"),
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                ("Location", &location),
            ],
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "items": [delegation("pending")],
                "nextCursor": "delegations-page-two"
            }),
        ),
        json_http_response("200 OK", delegation("pending")),
        json_response_with_headers(
            "200 OK",
            &delegation("active"),
            &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        ),
        http_response_with_headers(
            "204 No Content",
            None,
            &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
            &[],
        ),
        json_http_response("200 OK", delegation("ended")),
    ]);
    let environment = deployment_environment(&server.api_url, &human_credentials);

    let proposed = run_with_env(
        &[
            "delegation",
            "propose",
            SERVICE_PRINCIPAL_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(proposed.status.success(), "{proposed:?}");
    let proposed_json: serde_json::Value = serde_json::from_slice(&proposed.stdout).unwrap();
    assert_eq!(proposed_json["outcome"], "proposed");
    assert_eq!(proposed_json["delegation"]["state"], "pending");
    assert!(proposed.stderr.is_empty());

    let listed = run_with_env(
        &[
            "delegation",
            "list",
            "--limit",
            "1",
            "--cursor",
            "delegations-page-one",
            "--service-api-key-file",
            &service_key,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(listed.status.success(), "{listed:?}");
    let listed_json: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(listed_json["outcome"], "listed");
    assert_eq!(listed_json["nextCursor"], "delegations-page-two");
    assert_eq!(listed_json["items"][0]["id"], DELEGATION_ID);
    assert!(listed.stderr.is_empty());

    let shown = run_with_env(
        &[
            "delegation",
            "show",
            DELEGATION_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(shown.status.success(), "{shown:?}");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&shown.stdout).unwrap()["outcome"],
        "shown"
    );
    assert!(shown.stderr.is_empty());

    let accepted = run_with_env(
        &[
            "delegation",
            "accept",
            DELEGATION_ID,
            "--service-api-key-file",
            &service_key,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(accepted.status.success(), "{accepted:?}");
    let accepted_json: serde_json::Value = serde_json::from_slice(&accepted.stdout).unwrap();
    assert_eq!(accepted_json["outcome"], "accepted");
    assert_eq!(accepted_json["delegation"]["state"], "active");
    assert!(accepted.stderr.is_empty());

    let ended = run_with_env(
        &[
            "delegation",
            "end",
            DELEGATION_ID,
            "--yes",
            "--service-api-key-file",
            &service_key,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(ended.status.success(), "{ended:?}");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&ended.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "ended",
            "delegationId": DELEGATION_ID
        })
    );
    assert!(ended.stderr.is_empty());

    let shown_ended = run_with_env(
        &[
            "delegation",
            "show",
            DELEGATION_ID,
            "--service-api-key-file",
            &service_key,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(shown_ended.status.success(), "{shown_ended:?}");
    let shown_ended_json: serde_json::Value = serde_json::from_slice(&shown_ended.stdout).unwrap();
    assert_eq!(shown_ended_json["delegation"]["state"], "ended");
    assert_eq!(
        shown_ended_json["delegation"]["terminalReason"],
        "participant_ended"
    );
    assert!(shown_ended.stderr.is_empty());

    let requests = server.finish();
    assert!(requests[0].starts_with("POST /api/v1/delegations HTTP/1.1\r\n"));
    assert_authorization(&requests[0], HUMAN_TOKEN);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(requests[0].split_once("\r\n\r\n").unwrap().1)
            .unwrap(),
        serde_json::json!({"servicePrincipalId": SERVICE_PRINCIPAL_ID})
    );
    assert!(!header_value(&requests[0], "idempotency-key").is_empty());

    assert!(requests[1].starts_with(
        "GET /api/v1/me/delegations?limit=1&cursor=delegations-page-one HTTP/1.1\r\n"
    ));
    assert_authorization(&requests[1], SERVICE_API_KEY);

    assert!(requests[2].starts_with(&format!(
        "GET /api/v1/delegations/{DELEGATION_ID} HTTP/1.1\r\n"
    )));
    assert_authorization(&requests[2], HUMAN_TOKEN);

    assert!(requests[3].starts_with(&format!(
        "POST /api/v1/delegations/{DELEGATION_ID}/accept HTTP/1.1\r\n"
    )));
    assert_authorization(&requests[3], SERVICE_API_KEY);
    assert!(!header_value(&requests[3], "idempotency-key").is_empty());
    assert_eq!(requests[3].split_once("\r\n\r\n").unwrap().1, "");

    assert!(requests[4].starts_with(&format!(
        "DELETE /api/v1/delegations/{DELEGATION_ID} HTTP/1.1\r\n"
    )));
    assert_authorization(&requests[4], SERVICE_API_KEY);
    assert!(!header_value(&requests[4], "idempotency-key").is_empty());

    assert!(requests[5].starts_with(&format!(
        "GET /api/v1/delegations/{DELEGATION_ID} HTTP/1.1\r\n"
    )));
    assert_authorization(&requests[5], SERVICE_API_KEY);
}

#[test]
fn acceptance_requires_explicit_service_authentication_and_proposal_rejects_it() {
    let accept = run(&["delegation", "accept", DELEGATION_ID]);
    assert_eq!(accept.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&accept.stderr).contains("--service-api-key-file <PATH|->"));

    let propose = run(&[
        "delegation",
        "propose",
        SERVICE_PRINCIPAL_ID,
        "--service-api-key-file",
        "service.key",
    ]);
    assert_eq!(propose.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&propose.stderr).contains("unexpected argument"));
}

#[test]
fn ended_and_inaccessible_delegations_have_stable_failures() {
    let (server, _directory, human_credentials, service_key) = prepared(vec![
        problem_response(
            "409 Conflict",
            "https://api.scherzo.dev/problems/delegation-transition-unavailable",
            409,
        ),
        problem_response(
            "409 Conflict",
            "https://api.scherzo.dev/problems/delegation-transition-unavailable",
            409,
        ),
        problem_response(
            "404 Not Found",
            "https://api.scherzo.dev/problems/not-found",
            404,
        ),
        http_response_with_headers(
            "503 Service Unavailable",
            Some("application/problem+json"),
            &[("Retry-After", "3")],
            &serde_json::to_vec(&serde_json::json!({
                "type": "https://api.scherzo.dev/problems/retryable-conflict",
                "title": "Retryable conflict",
                "status": 503
            }))
            .unwrap(),
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &human_credentials);

    let ended = run_with_env(
        &[
            "delegation",
            "accept",
            DELEGATION_ID,
            "--service-api-key-file",
            &service_key,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(ended.status.code(), Some(1), "{ended:?}");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&ended.stdout).unwrap()["outcome"],
        "delegation_transition_unavailable"
    );
    assert!(ended.stderr.is_empty());

    let end_ended = run_with_env(
        &[
            "delegation",
            "end",
            DELEGATION_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(end_ended.status.code(), Some(1), "{end_ended:?}");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&end_ended.stdout).unwrap()["outcome"],
        "delegation_transition_unavailable"
    );
    assert!(end_ended.stderr.is_empty());

    let inaccessible = run_with_env(
        &[
            "delegation",
            "show",
            DELEGATION_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(inaccessible.status.code(), Some(1), "{inaccessible:?}");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&inaccessible.stdout).unwrap()["outcome"],
        "not_found"
    );
    assert!(inaccessible.stderr.is_empty());

    let retryable = run_with_env(
        &[
            "delegation",
            "propose",
            SERVICE_PRINCIPAL_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(retryable.status.code(), Some(4), "{retryable:?}");
    let retryable_json: serde_json::Value = serde_json::from_slice(&retryable.stdout).unwrap();
    assert_eq!(retryable_json["outcome"], "retryable_conflict");
    assert_eq!(retryable_json["retryAfter"], 3);
    assert!(retryable.stderr.is_empty());

    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert!(requests[1].starts_with(&format!(
        "DELETE /api/v1/delegations/{DELEGATION_ID} HTTP/1.1\r\n"
    )));
    assert_authorization(&requests[1], HUMAN_TOKEN);
}
