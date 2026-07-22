use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::*;
use crate::api::http_util::MAX_RESPONSE_BODY_BYTES;

struct ScriptedServer {
    issuer: String,
    requests: Receiver<String>,
    thread: JoinHandle<()>,
    expected_requests: usize,
}

impl ScriptedServer {
    fn new(responses: Vec<Vec<u8>>) -> Self {
        let expected_requests = responses.len();
        let listener = TcpListener::bind("127.0.0.1:0").expect("fixture listener should bind");
        let address = listener.local_addr().unwrap();
        let (sender, requests) = mpsc::channel();
        let thread = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("fixture request should arrive");
                let request = read_request(&mut stream);
                sender
                    .send(String::from_utf8(request).expect("request should be text"))
                    .unwrap();
                let _ = stream.write_all(&response);
            }
        });

        Self {
            issuer: format!("http://{address}/tenant/"),
            requests,
            thread,
            expected_requests,
        }
    }

    fn deployment(&self) -> Deployment {
        Deployment::for_test("http://api.fixture.example".to_owned(), self.issuer.clone())
    }

    fn finish(self) -> Vec<String> {
        let requests = (0..self.expected_requests)
            .map(|_| {
                self.requests
                    .recv_timeout(Duration::from_secs(2))
                    .expect("fixture should capture request")
            })
            .collect();
        self.thread.join().expect("fixture server should stop");
        requests
    }
}

fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    let mut expected_length = None;
    loop {
        let read = stream
            .read(&mut buffer)
            .expect("request should be readable");
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        if let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
            let body_start = header_end + 4;
            let length = *expected_length.get_or_insert_with(|| {
                String::from_utf8_lossy(&request[..body_start])
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|value| value.parse::<usize>().ok())
                    })
                    .unwrap_or_default()
            });
            if request.len() >= body_start + length {
                break;
            }
        }
        assert!(request.len() < 128 * 1024);
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

fn json_response(status: &str, value: serde_json::Value) -> Vec<u8> {
    response(
        status,
        Some("application/json; charset=utf-8"),
        &serde_json::to_vec(&value).unwrap(),
    )
}

fn keep_alive_json_response(status: &str, value: serde_json::Value) -> Vec<u8> {
    let body = serde_json::to_vec(&value).unwrap();
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(&body);
    response
}

fn request_form(request: &str) -> std::collections::HashMap<String, String> {
    let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
    url::form_urlencoded::parse(body.as_bytes())
        .into_owned()
        .collect()
}

#[test]
fn device_authorization_requests_the_exact_client_audience_and_scopes() {
    let server = ScriptedServer::new(vec![json_response(
        "200 OK",
        serde_json::json!({
            "device_code": "unique-private-device-code",
            "user_code": "ABCD-EFGH",
            "verification_uri": "https://auth.fixture.example/activate",
            "verification_uri_complete": "https://auth.fixture.example/activate?user_code=ABCD-EFGH",
            "expires_in": 600,
            "interval": 2
        }),
    )]);
    let deployment = server.deployment();
    let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).unwrap();

    let authorization = authorize(&client, &deployment).unwrap();

    assert_eq!(authorization.device_code(), "unique-private-device-code");
    assert_eq!(authorization.user_code(), "ABCD-EFGH");
    assert_eq!(authorization.expires_in(), Duration::from_secs(600));
    assert_eq!(authorization.interval(), Duration::from_secs(2));
    let debug = format!("{authorization:?}");
    assert!(!debug.contains("unique-private-device-code"));
    assert!(!debug.contains("ABCD-EFGH"));
    assert!(!debug.contains("auth.fixture.example"));

    let requests = server.finish();
    assert!(requests[0].starts_with("POST /tenant/oauth/device/code HTTP/1.1\r\n"));
    let form = request_form(&requests[0]);
    assert_eq!(form.get("client_id").unwrap(), "fixture-public-client");
    assert_eq!(form.get("audience").unwrap(), "https://api.fixture.example");
    assert_eq!(form.get("scope").unwrap(), "openid profile email");
    assert!(!form.contains_key("refresh_token"));
    assert!(!form.contains_key("offline_access"));
}

#[test]
fn device_authorization_defaults_the_poll_interval_and_allows_missing_complete_uri() {
    let authorization = decode_device_authorization(
        &serde_json::to_vec(&serde_json::json!({
            "device_code": "private",
            "user_code": "VISIBLE",
            "verification_uri": "https://auth.fixture.example/activate",
            "expires_in": 60
        }))
        .unwrap(),
        HttpTransportPolicy::HttpsOnly,
    )
    .unwrap();

    assert_eq!(authorization.interval(), DEFAULT_POLL_INTERVAL);
    assert_eq!(authorization.verification_uri_complete(), None);
}

#[test]
fn insecure_activation_urls_require_the_invocation_opt_in() {
    let body = serde_json::to_vec(&serde_json::json!({
        "device_code": "private",
        "user_code": "VISIBLE",
        "verification_uri": "http://auth.fixture.example/activate",
        "expires_in": 60
    }))
    .unwrap();

    assert!(matches!(
        decode_device_authorization(&body, HttpTransportPolicy::HttpsOnly),
        Err(AuthorizationError::Protocol { .. })
    ));
    assert!(decode_device_authorization(&body, HttpTransportPolicy::AllowInsecureHttp).is_ok());
}

#[test]
fn token_polling_handles_standard_device_grant_outcomes() {
    let responses = [
        ("authorization_pending", "400 Bad Request"),
        ("slow_down", "429 Too Many Requests"),
        ("access_denied", "403 Forbidden"),
        ("expired_token", "400 Bad Request"),
    ]
    .map(|(error, status)| json_response(status, serde_json::json!({ "error": error })))
    .to_vec();
    let server = ScriptedServer::new(responses);
    let deployment = server.deployment();
    let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).unwrap();

    assert!(matches!(
        poll_token(&client, &deployment, "private-device-code").unwrap(),
        TokenPoll::Pending
    ));
    assert!(matches!(
        poll_token(&client, &deployment, "private-device-code").unwrap(),
        TokenPoll::SlowDown
    ));
    assert!(matches!(
        poll_token(&client, &deployment, "private-device-code").unwrap(),
        TokenPoll::Denied
    ));
    assert!(matches!(
        poll_token(&client, &deployment, "private-device-code").unwrap(),
        TokenPoll::Expired
    ));

    for request in server.finish() {
        assert!(request.starts_with("POST /tenant/oauth/token HTTP/1.1\r\n"));
        let form = request_form(&request);
        assert_eq!(
            form.get("grant_type").unwrap(),
            "urn:ietf:params:oauth:grant-type:device_code"
        );
        assert_eq!(form.get("device_code").unwrap(), "private-device-code");
        assert_eq!(form.get("client_id").unwrap(), "fixture-public-client");
    }
}

#[test]
fn token_polling_reuses_one_http_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut requests = Vec::new();
        for error in ["authorization_pending", "access_denied"] {
            requests.push(String::from_utf8(read_request(&mut stream)).unwrap());
            stream
                .write_all(&keep_alive_json_response(
                    "400 Bad Request",
                    serde_json::json!({ "error": error }),
                ))
                .unwrap();
        }
        requests
    });
    let deployment = Deployment::for_test(
        "http://api.fixture.example".to_owned(),
        format!("http://{address}/tenant/"),
    );
    let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).unwrap();

    assert!(matches!(
        poll_token(&client, &deployment, "private-device-code").unwrap(),
        TokenPoll::Pending
    ));
    assert!(matches!(
        poll_token(&client, &deployment, "private-device-code").unwrap(),
        TokenPoll::Denied
    ));

    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.starts_with("POST /tenant/oauth/token HTTP/1.1\r\n"))
    );
}

#[test]
fn issued_token_is_validated_and_refresh_token_is_ignored() {
    let server = ScriptedServer::new(vec![json_response(
        "200 OK",
        serde_json::json!({
            "access_token": "unique-issued-access-token",
            "token_type": "bearer",
            "expires_in": 300,
            "refresh_token": "unique-ignored-refresh-token"
        }),
    )]);
    let deployment = server.deployment();
    let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).unwrap();

    let TokenPoll::Issued(token) = poll_token(&client, &deployment, "private-device-code").unwrap()
    else {
        panic!("expected an issued token");
    };

    assert_eq!(token.access_token(), "unique-issued-access-token");
    assert_eq!(token.expires_in(), Duration::from_secs(300));
    let debug = format!("{token:?}");
    assert!(!debug.contains("unique-issued-access-token"));
    assert!(!debug.contains("unique-ignored-refresh-token"));
    server.finish();
}

#[test]
fn invalid_device_and_token_payloads_are_protocol_errors() {
    for body in [
        serde_json::json!({
            "device_code": "",
            "user_code": "VISIBLE",
            "verification_uri": "https://auth.fixture.example/activate",
            "expires_in": 60
        }),
        serde_json::json!({
            "device_code": "private",
            "user_code": "",
            "verification_uri": "https://auth.fixture.example/activate",
            "expires_in": 60
        }),
        serde_json::json!({
            "device_code": "private",
            "user_code": "VISIBLE",
            "verification_uri": "javascript:alert(1)",
            "expires_in": 60
        }),
        serde_json::json!({
            "device_code": "private",
            "user_code": "VISIBLE",
            "verification_uri": "https://auth.fixture.example/activate",
            "expires_in": 0
        }),
        serde_json::json!({
            "device_code": "private",
            "user_code": "VISIBLE",
            "verification_uri": "https://auth.fixture.example/activate",
            "expires_in": 60,
            "interval": 0
        }),
    ] {
        assert!(matches!(
            decode_device_authorization(
                &serde_json::to_vec(&body).unwrap(),
                HttpTransportPolicy::HttpsOnly,
            ),
            Err(AuthorizationError::Protocol { .. })
        ));
    }

    let boundary_token = "x".repeat(MAX_ACCESS_TOKEN_BYTES);
    assert!(
        decode_issued_token(
            &serde_json::to_vec(&serde_json::json!({
                "access_token": boundary_token,
                "token_type": "Bearer",
                "expires_in": 60
            }))
            .unwrap()
        )
        .is_ok()
    );

    for body in [
        serde_json::json!({
            "access_token": "",
            "token_type": "Bearer",
            "expires_in": 60
        }),
        serde_json::json!({
            "access_token": "token",
            "token_type": "MAC",
            "expires_in": 60
        }),
        serde_json::json!({
            "access_token": "token",
            "token_type": "Bearer",
            "expires_in": 0
        }),
        serde_json::json!({
            "access_token": "x".repeat(MAX_ACCESS_TOKEN_BYTES + 1),
            "token_type": "Bearer",
            "expires_in": 60
        }),
    ] {
        assert!(matches!(
            decode_issued_token(&serde_json::to_vec(&body).unwrap()),
            Err(AuthorizationError::Protocol { .. })
        ));
    }
}

#[test]
fn oauth_request_deadline_bounds_the_complete_exchange() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        thread::sleep(Duration::from_millis(100));
        let _ = stream.write_all(&json_response("200 OK", serde_json::json!({})));
    });

    let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).unwrap();
    let Err(error) = post_form_with_timeout(
        &client,
        Url::parse(&format!("http://{address}/oauth/token")).unwrap(),
        &[],
        Duration::from_millis(20),
    ) else {
        panic!("slow OAuth request should time out");
    };

    assert!(matches!(
        error,
        AuthorizationError::Unreachable(UnreachableCategory::Timeout)
    ));
    server.join().unwrap();
}

#[test]
fn redirects_oversized_responses_and_temporary_failures_are_classified() {
    let oversized = format!(
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        MAX_RESPONSE_BODY_BYTES + 1
    )
    .into_bytes();
    let redirect = b"HTTP/1.1 302 Found\r\nConnection: close\r\nLocation: http://127.0.0.1:1/escaped\r\nContent-Length: 0\r\n\r\n".to_vec();
    let server = ScriptedServer::new(vec![
        redirect,
        oversized,
        response("429 Too Many Requests", None, &[]),
        response("503 Service Unavailable", None, &[]),
    ]);
    let deployment = server.deployment();
    let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).unwrap();

    assert!(matches!(
        authorize(&client, &deployment),
        Err(AuthorizationError::Protocol { .. })
    ));
    assert!(matches!(
        authorize(&client, &deployment),
        Err(AuthorizationError::Protocol { .. })
    ));
    assert!(matches!(
        authorize(&client, &deployment),
        Err(AuthorizationError::Unreachable(
            UnreachableCategory::RateLimited
        ))
    ));
    assert!(matches!(
        authorize(&client, &deployment),
        Err(AuthorizationError::Unreachable(UnreachableCategory::Server))
    ));
    server.finish();
}
