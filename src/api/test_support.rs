use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_REQUEST_BYTES: usize = 128 * 1024;

pub(crate) struct ScriptedHttpServer {
    pub(crate) api_url: String,
    requests: Receiver<String>,
    thread: JoinHandle<()>,
    response_count: usize,
    response_release: Option<SyncSender<()>>,
}

impl ScriptedHttpServer {
    pub(crate) fn respond(response: Vec<u8>) -> Self {
        Self::start(vec![ScriptedResponse::immediate(response)], None)
    }

    // Deadline tests keep the response pending until the production timer has
    // classified the request, then release the fixture through this channel.
    pub(crate) fn respond_when_released(response: Vec<u8>) -> Self {
        let (release, released) = mpsc::sync_channel(0);
        Self::start(
            vec![ScriptedResponse {
                release: Some(released),
                response,
            }],
            Some(release),
        )
    }

    pub(crate) fn respond_in_sequence(responses: Vec<Vec<u8>>) -> Self {
        Self::start(
            responses
                .into_iter()
                .map(ScriptedResponse::immediate)
                .collect(),
            None,
        )
    }

    fn start(responses: Vec<ScriptedResponse>, response_release: Option<SyncSender<()>>) -> Self {
        let response_count = responses.len();
        let listener = TcpListener::bind("127.0.0.1:0").expect("fixture listener should bind");
        let address = listener.local_addr().expect("fixture address should exist");
        let (sender, requests) = mpsc::channel();
        let thread = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("fixture request should arrive");
                let request = read_request(&mut stream);
                sender
                    .send(String::from_utf8(request).expect("request should be text"))
                    .expect("fixture request receiver should remain available");
                if let Some(release) = response.release {
                    release
                        .recv()
                        .expect("controlled fixture response should be released");
                }
                let _ = stream.write_all(&response.response);
            }
        });

        Self {
            api_url: format!("http://{address}/api/"),
            requests,
            thread,
            response_count,
            response_release,
        }
    }

    pub(crate) fn release_response(&mut self) {
        self.response_release
            .take()
            .expect("fixture should have a controlled response")
            .send(())
            .expect("controlled fixture response should be released");
    }

    pub(crate) fn finish_one(self) -> String {
        let mut requests = self.finish();
        assert_eq!(requests.len(), 1, "fixture should capture one request");
        requests.remove(0)
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "wall time only bounds the external HTTP fixture's readiness messages"
    )]
    pub(crate) fn finish(self) -> Vec<String> {
        assert!(
            self.response_release.is_none(),
            "controlled fixture response should be released before finishing"
        );
        let requests = (0..self.response_count)
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

struct ScriptedResponse {
    release: Option<Receiver<()>>,
    response: Vec<u8>,
}

impl ScriptedResponse {
    fn immediate(response: Vec<u8>) -> Self {
        Self {
            release: None,
            response,
        }
    }
}

pub(crate) fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("fixture read timeout should be configurable");
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
        assert!(
            request.len() <= MAX_REQUEST_BYTES,
            "fixture request is unexpectedly large"
        );

        if let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
            let body_start = header_end + 4;
            let length = *expected_length.get_or_insert_with(|| {
                String::from_utf8_lossy(&request[..body_start])
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or_default()
            });
            assert!(
                body_start.saturating_add(length) <= MAX_REQUEST_BYTES,
                "fixture request declares an unexpectedly large body"
            );
            if request.len() >= body_start + length {
                request.truncate(body_start + length);
                break;
            }
        }
    }

    request
}

#[cfg(test)]
mod tests {
    use std::net::TcpStream;

    use super::*;

    #[test]
    fn captures_ordered_complete_request_bodies() {
        let server = ScriptedHttpServer::respond_in_sequence(vec![
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n".to_vec(),
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n".to_vec(),
        ]);

        for body in ["first body", "second body is longer"] {
            let address = server
                .api_url
                .strip_prefix("http://")
                .and_then(|value| value.strip_suffix("/api/"))
                .expect("fixture URL should have the expected shape");
            let mut stream = TcpStream::connect(address).expect("fixture should accept a request");
            write!(
                stream,
                "POST / HTTP/1.1\r\nHost: {address}\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .expect("fixture request should be writable");
            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .expect("fixture response should be readable");
            assert!(response.starts_with(b"HTTP/1.1 204 No Content"));
        }

        let requests = server.finish();
        assert!(requests[0].ends_with("first body"));
        assert!(requests[1].ends_with("second body is longer"));
    }
}
