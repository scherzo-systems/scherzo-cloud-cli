use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};

use super::*;
use crate::api::HttpTransportPolicy;
use crate::api::http_util::MAX_RESPONSE_BODY_BYTES;

struct TestServer {
    api_url: String,
    request: Receiver<String>,
    thread: JoinHandle<()>,
}

impl TestServer {
    fn respond(response: Vec<u8>) -> Self {
        Self::respond_after(Duration::ZERO, response)
    }

    fn respond_after(delay: Duration, response: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("fixture listener should bind");
        let address = listener.local_addr().unwrap();
        let (sender, request) = mpsc::sync_channel(1);
        let thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("fixture request should arrive");
            let request_bytes = read_request(&mut stream);
            sender
                .send(String::from_utf8(request_bytes).expect("request should be text"))
                .unwrap();
            thread::sleep(delay);
            let _ = stream.write_all(&response);
        });

        Self {
            api_url: format!("http://{address}/api/"),
            request,
            thread,
        }
    }

    fn finish(self) -> String {
        let request = self
            .request
            .recv_timeout(Duration::from_secs(2))
            .expect("fixture should capture one request");
        self.thread.join().expect("fixture server should stop");
        request
    }
}

fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream
            .read(&mut buffer)
            .expect("request should be readable");
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        assert!(
            request.len() < 16 * 1024,
            "fixture request is unexpectedly large"
        );
    }
    request
}

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
fn authenticated_response_ignores_additive_principal_fields() {
    let body = br#"{"id":"prn_fixture","type":"human","state":"active","displayName":"Ada","future":{"nested":true}}"#;
    let server = TestServer::respond(response(
        "200 OK",
        Some("application/json; charset=utf-8"),
        body,
    ));

    let outcome = get_current_principal(
        &http_client(),
        &server.api_url,
        Some("synthetic-access-token"),
    )
    .unwrap();

    assert_eq!(
        outcome,
        CurrentPrincipalOutcome::Authenticated(HumanPrincipal {
            id: "prn_fixture".to_owned(),
            display_name: Some("Ada".to_owned()),
        })
    );
    let request = server.finish();
    assert!(request.starts_with("GET /api/v1/me HTTP/1.1\r\n"));
    assert!(request.contains("authorization: Bearer synthetic-access-token\r\n"));
    assert!(request.contains("accept: application/json, application/problem+json\r\n"));
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
        let request = server.finish();
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
        server.finish();
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
        server.finish();
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
    server.finish();
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
    server.finish();
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
        server.finish();
    }
}

#[test]
fn request_deadline_maps_to_timeout() {
    let body = br#"{"id":"prn_fixture","type":"human","state":"active"}"#;
    let server = TestServer::respond_after(
        Duration::from_millis(150),
        response("200 OK", Some(JSON_MEDIA_TYPE), body),
    );

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
    server.finish();
}

#[test]
fn request_deadline_bounds_the_complete_streaming_response() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\n\r\n",
            )
            .unwrap();
        for _ in 0..8 {
            if stream.write_all(b" ").is_err() {
                break;
            }
            if stream.flush().is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
    });
    let started = std::time::Instant::now();

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
    assert!(started.elapsed() < Duration::from_millis(150));
    server.join().unwrap();
}

#[test]
fn refused_connection_maps_to_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let outcome = get_current_principal_with_timeout(
        &http_client(),
        &format!("http://{address}"),
        None,
        Duration::from_millis(250),
    )
    .unwrap();

    assert_eq!(
        outcome,
        CurrentPrincipalOutcome::Unreachable(UnreachableCategory::Connection)
    );
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
