use std::io::{self, Write as _};
use std::net::TcpListener;
use std::thread;

use super::*;
use crate::api::HttpTransportPolicy;
use crate::api::http_util::MAX_RESPONSE_BODY_BYTES;
use crate::api::test_support::{ScriptedHttpServer, read_request};

type TestServer = ScriptedHttpServer;

fn response(status: &str, content_type: Option<&str>, body: &[u8]) -> Vec<u8> {
    let content_type = content_type
        .map(|value| format!("Content-Type: {value}\r\n"))
        .unwrap_or_default();
    let mut response = format!(
        "HTTP/1.1 {status}\r\nConnection: close\r\n{content_type}Content-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    response
}

fn http_client() -> HttpClient {
    HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).expect("HTTP client should build")
}

fn problem(status: u16, problem_type: &str, actions: Option<serde_json::Value>) -> Vec<u8> {
    let mut value = serde_json::json!({
        "type": problem_type,
        "title": "Fixture problem",
        "status": status
    });
    if let Some(actions) = actions {
        value["actions"] = actions;
    }
    serde_json::to_vec(&value).unwrap()
}

#[test]
fn authenticated_response_preserves_opaque_actions_and_ignores_additive_fields() {
    let actions = serde_json::json!([
        {
            "id": "future.action",
            "kind": "future-kind",
            "guide": "https://guarded.invalid/guide",
            "command": "do-not-execute",
            "unknown": { "nested": true }
        },
        "an-action-shape-the-cli-does-not-know"
    ]);
    let body = serde_json::to_vec(&serde_json::json!({
        "principal": {
            "id": "prn_fixture",
            "type": "human",
            "state": "active",
            "displayName": "Ada",
            "future": { "nested": true }
        },
        "actions": actions,
        "futureEnvelope": true
    }))
    .unwrap();
    let server = TestServer::respond(response(
        "200 OK",
        Some("application/json; charset=utf-8"),
        &body,
    ));

    let outcome = get_current_principal(
        &http_client(),
        &server.api_url,
        Some("synthetic-access-token"),
    )
    .unwrap();

    assert_eq!(
        outcome,
        CurrentPrincipalOutcome::Authenticated(AuthenticatedPrincipal {
            principal: HumanPrincipal {
                id: "prn_fixture".to_owned(),
                display_name: Some("Ada".to_owned()),
            },
            actions: Some(actions.as_array().unwrap().to_owned()),
        })
    );
    let request = server.finish_one();
    assert!(request.starts_with("GET /api/v1/me HTTP/1.1\r\n"));
    assert!(request.contains("authorization: Bearer synthetic-access-token\r\n"));
    assert!(request.contains("accept: application/json, application/problem+json\r\n"));
}

#[test]
fn service_principal_response_is_not_accepted_as_a_human_credential() {
    let body = br#"{"principal":{"id":"prn_service","type":"service","state":"active"}}"#;
    let server = TestServer::respond(response("200 OK", Some("application/json"), body));

    let error = get_current_principal(
        &http_client(),
        &server.api_url,
        Some("synthetic-access-token"),
    )
    .unwrap_err();

    assert!(!error.is_local());
    assert!(!error.credential_rejected());
    assert_eq!(
        error.to_string(),
        "current-principal response violates the public API contract: the principal type is not human"
    );
    server.finish_one();
}

#[test]
fn signup_actions_are_preserved_as_opaque_values_or_omitted() {
    let actions = serde_json::json!([
        {
            "id": "future.action",
            "kind": "future-kind",
            "guide": "https://elsewhere.invalid/guide",
            "unknown": { "nested": true }
        },
        "an-action-shape-the-cli-does-not-know"
    ]);
    for expected_actions in [Some(actions), None] {
        let body = problem(403, PRINCIPAL_NOT_PROVISIONED, expected_actions.clone());
        let server =
            TestServer::respond(response("403 Forbidden", Some(PROBLEM_MEDIA_TYPE), &body));

        let outcome = get_current_principal(&http_client(), &server.api_url, None).unwrap();

        assert_eq!(
            outcome,
            CurrentPrincipalOutcome::SignupRequired {
                actions: expected_actions.and_then(|value| value.as_array().cloned())
            }
        );
        let request = server.finish_one();
        assert!(!request.contains("authorization:"));
    }
}

#[test]
fn recognized_http_failures_map_to_closed_status_categories() {
    for (status, content_type, body, expected) in [
        (
            "401 Unauthorized",
            Some(PROBLEM_MEDIA_TYPE),
            problem(401, "https://api.scherzo.dev/problems/unauthorized", None),
            CurrentPrincipalOutcome::Unauthenticated,
        ),
        (
            "429 Too Many Requests",
            None,
            Vec::new(),
            CurrentPrincipalOutcome::Unreachable(UnreachableCategory::RateLimited),
        ),
        (
            "503 Service Unavailable",
            None,
            Vec::new(),
            CurrentPrincipalOutcome::Unreachable(UnreachableCategory::Server),
        ),
    ] {
        let server = TestServer::respond(response(status, content_type, &body));

        let outcome = get_current_principal(&http_client(), &server.api_url, None).unwrap();

        assert_eq!(outcome, expected);
        server.finish_one();
    }
}

#[test]
fn malformed_or_unexpected_responses_are_protocol_failures() {
    let cases = [
        (
            "200 OK",
            Some("text/plain"),
            br#"{"id":"prn_fixture","type":"human","state":"active"}"#.as_slice(),
        ),
        ("200 OK", Some(JSON_MEDIA_TYPE), b"not-json".as_slice()),
        (
            "403 Forbidden",
            Some(PROBLEM_MEDIA_TYPE),
            br#"{"type":"https://example.invalid/different","title":"No","status":403}"#.as_slice(),
        ),
        (
            "404 Not Found",
            Some(PROBLEM_MEDIA_TYPE),
            br#"{"type":"about:blank","title":"Missing","status":404}"#.as_slice(),
        ),
    ];

    for (status, content_type, body) in cases {
        let server = TestServer::respond(response(status, content_type, body));

        let error = get_current_principal(&http_client(), &server.api_url, None).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("violates the public API contract")
        );
        server.finish_one();
    }
}

#[test]
fn malformed_unauthorized_response_still_marks_the_credential_rejected() {
    let body = br#"{"secret":"unique-response-secret"}"#;
    let server = TestServer::respond(response("401 Unauthorized", Some(PROBLEM_MEDIA_TYPE), body));

    let error = get_current_principal(&http_client(), &server.api_url, Some("synthetic-token"))
        .unwrap_err();

    assert!(error.credential_rejected());
    assert!(!error.to_string().contains("unique-response-secret"));
    server.finish_one();
}

#[test]
fn redirect_is_returned_as_a_protocol_failure_without_being_followed() {
    let response = b"HTTP/1.1 302 Found\r\nConnection: close\r\nLocation: http://127.0.0.1:1/escaped\r\nContent-Length: 0\r\n\r\n".to_vec();
    let server = TestServer::respond(response);

    let error = get_current_principal(&http_client(), &server.api_url, Some("synthetic-token"))
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("redirect responses are not permitted")
    );
    server.finish_one();
}

#[test]
fn response_body_is_bounded_before_any_status_is_reported() {
    let body = vec![b'x'; MAX_RESPONSE_BODY_BYTES + 1];
    for status in ["200 OK", "503 Service Unavailable"] {
        let mut raw = format!(
            "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Type: application/json\r\n\r\n"
        )
        .into_bytes();
        raw.extend_from_slice(&body);
        let server = TestServer::respond(raw);

        let error = get_current_principal(&http_client(), &server.api_url, None).unwrap_err();

        assert!(error.to_string().contains("exceeds 1 MiB"));
        server.finish_one();
    }
}

#[test]
fn request_deadline_maps_to_timeout() {
    let body = br#"{"id":"prn_fixture","type":"human","state":"active"}"#;
    let mut server =
        TestServer::respond_when_released(response("200 OK", Some(JSON_MEDIA_TYPE), body));

    let outcome = get_current_principal_with_timeout(
        &http_client(),
        &server.api_url,
        None,
        Duration::from_millis(25),
    )
    .unwrap();

    assert_eq!(
        outcome,
        CurrentPrincipalOutcome::Unreachable(UnreachableCategory::Timeout)
    );
    server.release_response();
    server.finish_one();
}

#[test]
fn request_deadline_bounds_the_complete_streaming_response() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    // The peer sends headers and then remains pending, isolating the production
    // body deadline without using a sleep as test coordination.
    let (release_response, response_release) = std::sync::mpsc::sync_channel(0);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\n\r\n",
            )
            .unwrap();
        stream.flush().unwrap();
        response_release
            .recv()
            .expect("streaming response should be released");
        let _ = stream.write_all(b"{}");
    });

    let outcome = get_current_principal_with_timeout(
        &http_client(),
        &format!("http://{address}"),
        None,
        Duration::from_millis(60),
    )
    .unwrap();

    assert_eq!(
        outcome,
        CurrentPrincipalOutcome::Unreachable(UnreachableCategory::Timeout)
    );
    release_response
        .send(())
        .expect("streaming response should be released");
    server.join().unwrap();
}

#[test]
fn connection_closed_before_response_maps_to_connection() {
    let server = TestServer::respond(Vec::new());

    let outcome = get_current_principal_with_timeout(
        &http_client(),
        &server.api_url,
        None,
        Duration::from_secs(2),
    )
    .unwrap();

    assert_eq!(
        outcome,
        CurrentPrincipalOutcome::Unreachable(UnreachableCategory::Connection)
    );
    server.finish_one();
}

#[test]
fn failed_tls_handshake_maps_to_tls() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
    });

    let outcome = get_current_principal_with_timeout(
        &http_client(),
        &format!("https://{address}"),
        None,
        Duration::from_millis(500),
    )
    .unwrap();

    assert_eq!(
        outcome,
        CurrentPrincipalOutcome::Unreachable(UnreachableCategory::Tls)
    );
    server.join().unwrap();
}

#[test]
fn transport_error_classifier_uses_the_closed_vocabulary() {
    for (message, expected) in [
        ("DNS lookup failed", UnreachableCategory::Dns),
        ("invalid peer certificate", UnreachableCategory::Tls),
        ("connection refused", UnreachableCategory::Connection),
    ] {
        let error = io::Error::other(message);
        assert_eq!(classify_error_chain(&error), expected);
    }
    let timeout = io::Error::from(io::ErrorKind::TimedOut);
    assert_eq!(classify_error_chain(&timeout), UnreachableCategory::Timeout);
}
