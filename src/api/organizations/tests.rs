use std::time::Duration;

use super::*;
use crate::api::HttpTransportPolicy;
use crate::api::http_util::MAX_RESPONSE_BODY_BYTES;
use crate::api::test_support::ScriptedHttpServer;

const TOKEN: &str = "organization-unit-test-token-sentinel";
const KEY: &str = "organization-unit-test-idempotency-key";

fn http_client() -> HttpClient {
    HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).expect("HTTP client should build")
}

fn response(
    status: &str,
    content_type: Option<&str>,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Vec<u8> {
    let mut response = format!("HTTP/1.1 {status}\r\nConnection: close\r\n");
    if let Some(content_type) = content_type {
        response.push_str(&format!("Content-Type: {content_type}\r\n"));
    }
    for (name, value) in headers {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    let mut response = response.into_bytes();
    response.extend_from_slice(body);
    response
}

fn organization_body() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "id": "org_fixture",
        "state": "active",
        "displayName": "Acme Research",
        "slug": "acme-research",
        "createdAt": "2026-07-22T20:32:00Z",
        "updatedAt": "2026-07-22T20:32:00Z",
        "future": { "accepted": true }
    }))
    .expect("organization fixture should serialize")
}

fn success(status: &str) -> Vec<u8> {
    let headers = match status {
        "201 Created" => vec![
            ("Idempotency-Key", KEY),
            ("Location", "/v1/organizations/org_fixture"),
        ],
        "200 OK" => vec![("Idempotency-Key", KEY)],
        _ => Vec::new(),
    };
    response(
        status,
        Some(JSON_MEDIA_TYPE),
        &headers,
        &organization_body(),
    )
}

fn interrupted_success(status: &str) -> Vec<u8> {
    let mut response = success(status);
    response.pop();
    response
}

fn problem_response(
    status_text: &str,
    status: u16,
    problem_type: &str,
    headers: &[(&str, &str)],
) -> Vec<u8> {
    let body = serde_json::to_vec(&serde_json::json!({
        "type": problem_type,
        "title": "problem-title-sentinel",
        "status": status,
        "detail": "problem-detail-sentinel"
    }))
    .expect("problem fixture should serialize");
    response(status_text, Some(PROBLEM_MEDIA_TYPE), headers, &body)
}

fn body(request: &str) -> &str {
    request
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .expect("request should contain a body separator")
}

fn header_value<'a>(request: &'a str, name: &str) -> &'a str {
    request
        .lines()
        .find_map(|line| {
            let (actual, value) = line.split_once(':')?;
            actual
                .eq_ignore_ascii_case(name)
                .then_some(value.trim().trim_end_matches('\r'))
        })
        .expect("request should contain expected header")
}

fn send_fixture_shutdown(api_url: &str) {
    let address = api_url
        .strip_prefix("http://")
        .unwrap()
        .trim_end_matches("/api/");
    if let Ok(mut stream) = std::net::TcpStream::connect(address) {
        let _ = std::io::Write::write_all(
            &mut stream,
            b"GET /fixture-shutdown HTTP/1.1\r\nHost: fixture\r\nConnection: close\r\n\r\n",
        );
    }
}

#[test]
fn create_retries_failures_before_and_during_a_created_response() {
    for first_response in [Vec::new(), interrupted_success("201 Created")] {
        let server =
            ScriptedHttpServer::respond_in_sequence(vec![first_response, success("201 Created")]);

        let outcome = create_organization(
            &http_client(),
            &server.api_url,
            TOKEN,
            KEY,
            "\u{2003}Acme Research\u{2003}",
            Some("acme-research"),
        )
        .expect("create should succeed after one ambiguous failure");

        assert!(matches!(outcome, CreateOrganizationOutcome::Created(_)));
        let requests = server.finish();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
        assert!(requests[0].starts_with("POST /api/v1/organizations HTTP/1.1\r\n"));
        assert_eq!(
            header_value(&requests[0], "authorization"),
            format!("Bearer {TOKEN}")
        );
        assert_eq!(header_value(&requests[0], "idempotency-key"), KEY);
        assert_eq!(header_value(&requests[0], "content-type"), JSON_MEDIA_TYPE);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body(&requests[0])).unwrap(),
            serde_json::json!({
                "displayName": "\u{2003}Acme Research\u{2003}",
                "slug": "acme-research"
            })
        );
    }
}

#[test]
fn create_omits_an_absent_slug_and_does_not_retry_explicit_failures() {
    let response = problem_response("400 Bad Request", 400, BAD_REQUEST, &[]);
    let server = ScriptedHttpServer::respond(response);

    let outcome = create_organization(&http_client(), &server.api_url, TOKEN, KEY, "Acme", None)
        .expect("contracted failure should decode");

    assert_eq!(
        outcome,
        CreateOrganizationOutcome::Common(CommonOrganizationFailure::InvalidInput)
    );
    let request = server.finish_one();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(body(&request)).unwrap(),
        serde_json::json!({"displayName": "Acme"})
    );
}

#[test]
fn create_classifies_its_operation_specific_failures() {
    let cases = [
        (
            "403 Forbidden",
            403,
            CREATION_NOT_PERMITTED,
            CreateOrganizationOutcome::CreationNotPermitted,
        ),
        (
            "403 Forbidden",
            403,
            FORBIDDEN,
            CreateOrganizationOutcome::Common(CommonOrganizationFailure::Forbidden),
        ),
        (
            "409 Conflict",
            409,
            SLUG_UNAVAILABLE,
            CreateOrganizationOutcome::SlugUnavailable,
        ),
        (
            "409 Conflict",
            409,
            QUANTITY_LIMIT_REACHED,
            CreateOrganizationOutcome::QuantityLimitReached,
        ),
        (
            "409 Conflict",
            409,
            IDEMPOTENCY_CONFLICT,
            CreateOrganizationOutcome::IdempotencyConflict,
        ),
    ];

    for (status_text, status, problem_type, expected) in cases {
        let server =
            ScriptedHttpServer::respond(problem_response(status_text, status, problem_type, &[]));
        let outcome =
            create_organization(&http_client(), &server.api_url, TOKEN, KEY, "Acme", None)
                .expect("contracted create failure should decode");
        assert_eq!(outcome, expected);
        server.finish_one();
    }
}

#[test]
fn rate_limit_requires_matching_problem_and_positive_retry_after() {
    let valid = ScriptedHttpServer::respond(problem_response(
        "429 Too Many Requests",
        429,
        RATE_LIMITED,
        &[("Retry-After", "42")],
    ));
    let outcome = create_organization(&http_client(), &valid.api_url, TOKEN, KEY, "Acme", None)
        .expect("valid rate limit should decode");
    assert_eq!(
        outcome,
        CreateOrganizationOutcome::RateLimited { retry_after: 42 }
    );
    valid.finish_one();

    for headers in [
        Vec::new(),
        vec![("Retry-After", "not-a-number")],
        vec![("Retry-After", "+42")],
        vec![("Retry-After", "0")],
    ] {
        let server = ScriptedHttpServer::respond(problem_response(
            "429 Too Many Requests",
            429,
            RATE_LIMITED,
            &headers,
        ));
        let error = create_organization(&http_client(), &server.api_url, TOKEN, KEY, "Acme", None)
            .expect_err("invalid Retry-After should be a protocol failure");
        assert!(error.to_string().contains("invalid Retry-After"));
        server.finish_one();
    }
}

#[test]
fn show_encodes_the_reference_as_one_path_segment_and_uses_one_attempt() {
    let server = ScriptedHttpServer::respond(success("200 OK"));
    let outcome = get_organization(&http_client(), &server.api_url, TOKEN, "org/ Mixed Case")
        .expect("show should succeed");

    assert!(matches!(outcome, GetOrganizationOutcome::Found(_)));
    let request = server.finish_one();
    assert!(request.starts_with("GET /api/v1/organizations/org%2F%20Mixed%20Case HTTP/1.1\r\n"));
    assert!(!request.contains("idempotency-key:"));
}

#[test]
fn review_dot_segment_reference_is_sent_as_one_literal_segment() {
    let server =
        ScriptedHttpServer::respond(problem_response("400 Bad Request", 400, BAD_REQUEST, &[]));

    let outcome = get_organization(&http_client(), &server.api_url, TOKEN, "..")
        .expect("server-invalid reference should reach the organization endpoint");

    assert_eq!(
        outcome,
        GetOrganizationOutcome::Common(CommonOrganizationFailure::InvalidInput)
    );
    let request = server.finish_one();
    assert!(
        request.starts_with("GET /api/v1/organizations/%2E%2E HTTP/1.1\r\n"),
        "request unexpectedly reinterpreted the opaque reference: {request:?}"
    );
}

#[test]
fn show_preserves_private_failure_classification() {
    let cases = [
        (
            "400 Bad Request",
            400,
            BAD_REQUEST,
            GetOrganizationOutcome::Common(CommonOrganizationFailure::InvalidInput),
        ),
        (
            "401 Unauthorized",
            401,
            UNAUTHORIZED,
            GetOrganizationOutcome::Common(CommonOrganizationFailure::Unauthenticated),
        ),
        (
            "403 Forbidden",
            403,
            FORBIDDEN,
            GetOrganizationOutcome::Common(CommonOrganizationFailure::Forbidden),
        ),
        (
            "404 Not Found",
            404,
            NOT_FOUND,
            GetOrganizationOutcome::NotFound,
        ),
    ];

    for (status_text, status, problem_type, expected) in cases {
        let server =
            ScriptedHttpServer::respond(problem_response(status_text, status, problem_type, &[]));
        let outcome = get_organization(&http_client(), &server.api_url, TOKEN, "private-target")
            .expect("contracted show failure should decode");
        assert_eq!(outcome, expected);
        server.finish_one();
    }
}

#[test]
fn update_retries_failures_before_and_during_an_updated_response() {
    for first_response in [Vec::new(), interrupted_success("200 OK")] {
        let server =
            ScriptedHttpServer::respond_in_sequence(vec![first_response, success("200 OK")]);

        let outcome = update_organization(
            &http_client(),
            &server.api_url,
            TOKEN,
            "acme/research",
            KEY,
            Some("Acme Labs"),
            None,
        )
        .expect("update should succeed after one ambiguous failure");

        assert!(matches!(outcome, UpdateOrganizationOutcome::Updated(_)));
        let requests = server.finish();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
        assert!(
            requests[0].starts_with("PATCH /api/v1/organizations/acme%2Fresearch HTTP/1.1\r\n")
        );
        assert_eq!(
            header_value(&requests[0], "authorization"),
            format!("Bearer {TOKEN}")
        );
        assert_eq!(header_value(&requests[0], "idempotency-key"), KEY);
        assert_eq!(
            header_value(&requests[0], "content-type"),
            MERGE_PATCH_MEDIA_TYPE
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body(&requests[0])).unwrap(),
            serde_json::json!({"displayName": "Acme Labs"})
        );
    }
}

#[test]
fn mutation_attempt_budget_includes_interrupted_success_responses() {
    let interrupted = interrupted_success("201 Created");
    let server = ScriptedHttpServer::respond_in_sequence(vec![
        interrupted.clone(),
        interrupted,
        success("201 Created"),
    ]);

    let outcome = create_organization(&http_client(), &server.api_url, TOKEN, KEY, "Acme", None)
        .expect("an exhausted request budget should be an expected outcome");

    assert_eq!(
        outcome,
        CreateOrganizationOutcome::Common(CommonOrganizationFailure::Unreachable(
            UnreachableCategory::Connection
        ))
    );
    send_fixture_shutdown(&server.api_url);
    let requests = server.finish();
    assert_eq!(requests.len(), MUTATION_ATTEMPTS + 1);
    assert_eq!(requests[0], requests[1]);
    assert!(
        requests[2].starts_with("GET /fixture-shutdown HTTP/1.1\r\n"),
        "the mutation exceeded its two-attempt budget"
    );
}

#[test]
fn membership_list_preserves_optional_query_and_decodes_models() {
    let page = serde_json::to_vec(&serde_json::json!({
        "items": [{
            "id": "mem_fixture",
            "principalId": "prn_fixture",
            "principalType": "human",
            "displayName": "Ada",
            "role": "owner",
            "future": true
        }],
        "nextCursor": "opaque cursor"
    }))
    .unwrap();
    let server = ScriptedHttpServer::respond(response("200 OK", Some(JSON_MEDIA_TYPE), &[], &page));

    let outcome = list_organization_memberships(
        &http_client(),
        &server.api_url,
        TOKEN,
        "acme/research",
        Some(200),
        Some("opaque cursor"),
    )
    .expect("membership page should decode");

    let ListOrganizationMembershipsOutcome::Listed(page) = outcome else {
        panic!("expected listed page");
    };
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.next_cursor.as_deref(), Some("opaque cursor"));
    let request = server.finish_one();
    assert!(request.starts_with(
        "GET /api/v1/organizations/acme%2Fresearch/memberships?limit=200&cursor=opaque+cursor HTTP/1.1\r\n"
    ));
}

#[test]
fn update_serializes_each_requested_merge_patch_shape() {
    for (display_name, slug, expected) in [
        (
            Some("Acme Labs"),
            None,
            serde_json::json!({"displayName": "Acme Labs"}),
        ),
        (
            None,
            Some("acme-labs"),
            serde_json::json!({"slug": "acme-labs"}),
        ),
        (
            Some("Acme Labs"),
            Some("acme-labs"),
            serde_json::json!({"displayName": "Acme Labs", "slug": "acme-labs"}),
        ),
    ] {
        let server = ScriptedHttpServer::respond(success("200 OK"));
        let outcome = update_organization(
            &http_client(),
            &server.api_url,
            TOKEN,
            "acme",
            KEY,
            display_name,
            slug,
        )
        .expect("update should succeed");
        assert!(matches!(outcome, UpdateOrganizationOutcome::Updated(_)));
        let request = server.finish_one();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body(&request)).unwrap(),
            expected
        );
    }
}

#[test]
fn update_and_membership_list_classify_only_their_contracted_failures() {
    let update_cases = [
        (
            "400 Bad Request",
            400,
            BAD_REQUEST,
            UpdateOrganizationOutcome::Common(CommonOrganizationFailure::InvalidInput),
        ),
        (
            "401 Unauthorized",
            401,
            UNAUTHORIZED,
            UpdateOrganizationOutcome::Common(CommonOrganizationFailure::Unauthenticated),
        ),
        (
            "403 Forbidden",
            403,
            FORBIDDEN,
            UpdateOrganizationOutcome::Common(CommonOrganizationFailure::Forbidden),
        ),
        (
            "404 Not Found",
            404,
            NOT_FOUND,
            UpdateOrganizationOutcome::NotFound,
        ),
        (
            "409 Conflict",
            409,
            SLUG_UNAVAILABLE,
            UpdateOrganizationOutcome::SlugUnavailable,
        ),
        (
            "409 Conflict",
            409,
            IDEMPOTENCY_CONFLICT,
            UpdateOrganizationOutcome::IdempotencyConflict,
        ),
    ];
    for (status_text, status, problem_type, expected) in update_cases {
        let server =
            ScriptedHttpServer::respond(problem_response(status_text, status, problem_type, &[]));
        let outcome = update_organization(
            &http_client(),
            &server.api_url,
            TOKEN,
            "acme",
            KEY,
            Some("Acme"),
            None,
        )
        .expect("contracted update failure should decode");
        assert_eq!(outcome, expected);
        server.finish_one();
    }

    let list_cases = [
        (
            "400 Bad Request",
            400,
            BAD_REQUEST,
            ListOrganizationMembershipsOutcome::Common(CommonOrganizationFailure::InvalidInput),
        ),
        (
            "401 Unauthorized",
            401,
            UNAUTHORIZED,
            ListOrganizationMembershipsOutcome::Common(CommonOrganizationFailure::Unauthenticated),
        ),
        (
            "403 Forbidden",
            403,
            FORBIDDEN,
            ListOrganizationMembershipsOutcome::Common(CommonOrganizationFailure::Forbidden),
        ),
        (
            "404 Not Found",
            404,
            NOT_FOUND,
            ListOrganizationMembershipsOutcome::NotFound,
        ),
    ];
    for (status_text, status, problem_type, expected) in list_cases {
        let server =
            ScriptedHttpServer::respond(problem_response(status_text, status, problem_type, &[]));
        let outcome = list_organization_memberships(
            &http_client(),
            &server.api_url,
            TOKEN,
            "acme",
            None,
            None,
        )
        .expect("contracted membership-list failure should decode");
        assert_eq!(outcome, expected);
        server.finish_one();
    }
}

#[test]
fn membership_list_omits_absent_query_parameters() {
    let page = serde_json::to_vec(&serde_json::json!({"items": []})).unwrap();
    let server = ScriptedHttpServer::respond(response("200 OK", Some(JSON_MEDIA_TYPE), &[], &page));

    let outcome =
        list_organization_memberships(&http_client(), &server.api_url, TOKEN, "acme", None, None)
            .expect("empty membership page should decode");

    assert_eq!(
        outcome,
        ListOrganizationMembershipsOutcome::Listed(OrganizationMembershipPage {
            items: Vec::new(),
            next_cursor: None,
        })
    );
    let request = server.finish_one();
    assert!(request.starts_with("GET /api/v1/organizations/acme/memberships HTTP/1.1\r\n"));
}

#[test]
fn malformed_media_and_mismatched_problem_status_are_protocol_failures() {
    let invalid_media = ScriptedHttpServer::respond(response(
        "200 OK",
        Some("text/plain"),
        &[],
        &organization_body(),
    ));
    let error = get_organization(&http_client(), &invalid_media.api_url, TOKEN, "acme")
        .expect_err("invalid success media type should fail");
    assert!(error.to_string().contains("Content-Type"));
    invalid_media.finish_one();

    let mismatched_body = serde_json::to_vec(&serde_json::json!({
        "type": NOT_FOUND,
        "title": "mismatched-status-title-sentinel",
        "status": 403
    }))
    .unwrap();
    let mismatched = ScriptedHttpServer::respond(response(
        "404 Not Found",
        Some(PROBLEM_MEDIA_TYPE),
        &[],
        &mismatched_body,
    ));
    let error = get_organization(&http_client(), &mismatched.api_url, TOKEN, "acme")
        .expect_err("mismatched problem status should fail");
    assert!(error.to_string().contains("does not match"));
    assert!(
        !error
            .to_string()
            .contains("mismatched-status-title-sentinel")
    );
    mismatched.finish_one();
}

#[test]
fn changed_operation_problem_is_a_protocol_failure_without_leaking_prose() {
    let server = ScriptedHttpServer::respond(problem_response(
        "409 Conflict",
        409,
        QUANTITY_LIMIT_REACHED,
        &[],
    ));

    let error = update_organization(
        &http_client(),
        &server.api_url,
        TOKEN,
        "acme",
        KEY,
        None,
        Some("new-slug"),
    )
    .expect_err("create-only problem should not be accepted for update");

    let formatted = error.to_string();
    assert!(formatted.contains("unrecognized problem type"));
    for secret in [
        TOKEN,
        KEY,
        "problem-title-sentinel",
        "problem-detail-sentinel",
    ] {
        assert!(!formatted.contains(secret));
    }
    server.finish_one();
}

#[test]
fn responses_are_bounded_and_success_models_are_validated() {
    let oversized = vec![b'x'; MAX_RESPONSE_BODY_BYTES + 1];
    let server =
        ScriptedHttpServer::respond(response("200 OK", Some(JSON_MEDIA_TYPE), &[], &oversized));
    let error = get_organization(&http_client(), &server.api_url, TOKEN, "acme")
        .expect_err("oversized body should fail");
    assert!(error.to_string().contains("exceeds 1 MiB"));
    server.finish_one();

    for body in [
        br#"{"id":"","state":"active","displayName":"Acme","slug":"acme","createdAt":"c","updatedAt":"u"}"#.as_slice(),
        br#"{"id":"org_fixture","state":"future","displayName":"Acme","slug":"acme","createdAt":"c","updatedAt":"u"}"#.as_slice(),
    ] {
        let server = ScriptedHttpServer::respond(response(
            "200 OK",
            Some(JSON_MEDIA_TYPE),
            &[],
            body,
        ));
        get_organization(&http_client(), &server.api_url, TOKEN, "acme")
            .expect_err("invalid success representation should fail");
        server.finish_one();
    }
}

#[test]
fn mutation_successes_require_their_response_identity_headers() {
    for headers in [
        vec![("Location", "/v1/organizations/org_fixture")],
        vec![
            ("Idempotency-Key", "different-key"),
            ("Location", "/v1/organizations/org_fixture"),
        ],
    ] {
        let server = ScriptedHttpServer::respond(response(
            "201 Created",
            Some(JSON_MEDIA_TYPE),
            &headers,
            &organization_body(),
        ));
        let error = create_organization(&http_client(), &server.api_url, TOKEN, KEY, "Acme", None)
            .expect_err("missing or changed response identity must fail");
        assert!(error.to_string().contains("Idempotency-Key"));
        server.finish_one();
    }

    for location in [None, Some("/v1/organizations/a-different-organization")] {
        let mut headers = vec![("Idempotency-Key", KEY)];
        if let Some(location) = location {
            headers.push(("Location", location));
        }
        let server = ScriptedHttpServer::respond(response(
            "201 Created",
            Some(JSON_MEDIA_TYPE),
            &headers,
            &organization_body(),
        ));
        let error = create_organization(&http_client(), &server.api_url, TOKEN, KEY, "Acme", None)
            .expect_err("missing or changed Location must fail");
        assert!(error.to_string().contains("Location"));
        server.finish_one();
    }

    let missing_update_identity = ScriptedHttpServer::respond(response(
        "200 OK",
        Some(JSON_MEDIA_TYPE),
        &[],
        &organization_body(),
    ));
    let error = update_organization(
        &http_client(),
        &missing_update_identity.api_url,
        TOKEN,
        "acme",
        KEY,
        Some("Acme"),
        None,
    )
    .expect_err("update response identity is required");
    assert!(error.to_string().contains("Idempotency-Key"));
    missing_update_identity.finish_one();
}

#[test]
fn interrupted_success_with_mismatched_identity_is_not_retried() {
    let mut interrupted = response(
        "201 Created",
        Some(JSON_MEDIA_TYPE),
        &[
            ("Idempotency-Key", "different-key"),
            ("Location", "/v1/organizations/org_fixture"),
        ],
        &organization_body(),
    );
    interrupted.pop();
    let server = ScriptedHttpServer::respond_in_sequence(vec![interrupted, success("201 Created")]);

    let result = create_organization(&http_client(), &server.api_url, TOKEN, KEY, "Acme", None);

    send_fixture_shutdown(&server.api_url);
    let requests = server.finish();
    assert!(
        requests[1].starts_with("GET /fixture-shutdown HTTP/1.1\r\n"),
        "the mutation retried after the successful response echoed another request identity"
    );
    let error = result.expect_err("a mismatched response identity is a protocol failure");
    assert!(error.to_string().contains("Idempotency-Key"));
}

#[test]
fn redirects_and_malformed_unauthorized_responses_are_protocol_failures() {
    let redirect = ScriptedHttpServer::respond(response(
        "302 Found",
        None,
        &[("Location", "http://127.0.0.1:1/credential-target")],
        &[],
    ));
    let error = get_organization(&http_client(), &redirect.api_url, TOKEN, "acme")
        .expect_err("redirect should fail");
    assert!(error.to_string().contains("redirect responses"));
    redirect.finish_one();

    let unauthorized = ScriptedHttpServer::respond(response(
        "401 Unauthorized",
        Some(PROBLEM_MEDIA_TYPE),
        &[],
        b"not-json",
    ));
    let error = get_organization(&http_client(), &unauthorized.api_url, TOKEN, "acme")
        .expect_err("malformed unauthorized response should fail");
    assert!(error.credential_rejected());
    assert!(!error.to_string().contains(TOKEN));
    unauthorized.finish_one();

    let interrupted = ScriptedHttpServer::respond(
        b"HTTP/1.1 401 Unauthorized\r\nConnection: close\r\nContent-Type: application/problem+json\r\nContent-Length: 100\r\n\r\n{".to_vec(),
    );
    let error = create_organization(
        &http_client(),
        &interrupted.api_url,
        TOKEN,
        KEY,
        "Acme",
        None,
    )
    .expect_err("interrupted unauthorized mutation response should fail");
    assert!(error.credential_rejected());
    assert!(error.to_string().contains("could not be read"));
    assert!(!error.to_string().contains(TOKEN));
    interrupted.finish_one();
}

#[test]
fn unauthorized_status_survives_the_body_deadline() {
    let Err(error) = classify_response_deadline(Operation::Get, StatusCode::UNAUTHORIZED) else {
        panic!("a timed-out unauthorized body is a protocol failure");
    };

    assert!(error.credential_rejected());
    assert!(!error.to_string().contains(TOKEN));
}

#[test]
fn deadlines_cover_headers_and_reads_do_not_retry() {
    let mut server = ScriptedHttpServer::respond_when_released(success("200 OK"));

    let outcome = get_organization_with_timeout(
        &http_client(),
        &server.api_url,
        TOKEN,
        "acme",
        Duration::from_millis(25),
    )
    .expect("timeout is an expected outcome");

    assert_eq!(
        outcome,
        GetOrganizationOutcome::Common(CommonOrganizationFailure::Unreachable(
            UnreachableCategory::Timeout
        ))
    );
    server.release_response();
    server.finish_one();
}

#[test]
fn explicit_server_status_with_interrupted_body_is_not_retried() {
    let interrupted =
        b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 1\r\n\r\n"
            .to_vec();
    let server = ScriptedHttpServer::respond_in_sequence(vec![interrupted, success("200 OK")]);

    let outcome = update_organization(
        &http_client(),
        &server.api_url,
        TOKEN,
        "acme",
        KEY,
        Some("Acme"),
        None,
    )
    .expect("the explicit server response should be classified");

    send_fixture_shutdown(&server.api_url);
    let requests = server.finish();

    assert!(
        requests[1].starts_with("GET /fixture-shutdown HTTP/1.1\r\n"),
        "the mutation sent a second request after receiving HTTP 503"
    );
    assert_eq!(
        outcome,
        UpdateOrganizationOutcome::Common(CommonOrganizationFailure::Unreachable(
            UnreachableCategory::Server
        ))
    );
}

#[test]
fn explicit_server_failures_are_not_retried() {
    let server = ScriptedHttpServer::respond(response("503 Service Unavailable", None, &[], &[]));

    let outcome = create_organization(&http_client(), &server.api_url, TOKEN, KEY, "Acme", None)
        .expect("server failure should be an expected outcome");

    assert_eq!(
        outcome,
        CreateOrganizationOutcome::Common(CommonOrganizationFailure::Unreachable(
            UnreachableCategory::Server
        ))
    );
    server.finish_one();
}
