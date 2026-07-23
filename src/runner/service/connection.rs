use std::fmt;
use std::time::Duration;

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode, header};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use crate::runner::service::Sleeper;
use crate::runner::service::config::Config;
use crate::runner_protocol::{
    CloudFrame, RunnerEnvelope, RunnerFrame, decode_cloud_frame, encode_runner_frame,
};

const SUBPROTOCOL: &str = "scherzo.runner.v1";
const MAX_INBOUND_MESSAGE_BYTES: usize = 65_536;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const WELCOME_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

type FrameWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

pub(crate) trait FrameSource: Send + Sync {
    fn public_id(&self, prefix: &str) -> String;
    fn utc_timestamp(&self) -> Result<String, ConnectionError>;
}

pub(crate) struct SystemFrameSource;

impl FrameSource for SystemFrameSource {
    fn public_id(&self, prefix: &str) -> String {
        format!(
            "{prefix}{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        )
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "SystemFrameSource is the production boundary for wall-clock timestamps"
    )]
    fn utc_timestamp(&self) -> Result<String, ConnectionError> {
        time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|_| {
                ConnectionError::terminal(
                    ConnectionProgress::unacknowledged(),
                    "format current timestamp",
                )
            })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct OpeningHello<'a> {
    pub(crate) boot_id: &'a str,
    pub(crate) encoded: &'a [u8],
    pub(crate) message_id: &'a str,
    pub(crate) sequence: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConnectionProgress {
    pub(crate) opening_acknowledged: bool,
    pub(crate) handshake_completed: bool,
}

impl ConnectionProgress {
    pub(crate) const fn unacknowledged() -> Self {
        Self {
            opening_acknowledged: false,
            handshake_completed: false,
        }
    }
}

// FailureKind is the normative runner ending classification: retryable endings
// re-enter bounded backoff while terminal endings stop the runner service
// because retrying identical transport state cannot succeed.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum FailureKind {
    Retryable,
    Terminal,
}

#[derive(Debug)]
pub(crate) struct ConnectionError {
    pub(crate) progress: ConnectionProgress,
    kind: FailureKind,
    cause: &'static str,
}

impl ConnectionError {
    pub(crate) const fn terminal(progress: ConnectionProgress, cause: &'static str) -> Self {
        Self {
            progress,
            kind: FailureKind::Terminal,
            cause,
        }
    }

    const fn retryable(progress: ConnectionProgress, cause: &'static str) -> Self {
        Self {
            progress,
            kind: FailureKind::Retryable,
            cause,
        }
    }

    pub(crate) const fn is_terminal(&self) -> bool {
        matches!(self.kind, FailureKind::Terminal)
    }

    pub(crate) const fn cause(&self) -> &'static str {
        self.cause
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runner gateway connection failed: {}",
            self.cause
        )
    }
}

impl std::error::Error for ConnectionError {}

// close_outcome classifies a received close frame by status code only; close
// reasons are diagnostics, never contract.
fn close_outcome(
    progress: ConnectionProgress,
    close: Option<CloseFrame>,
) -> Result<ConnectionProgress, ConnectionError> {
    match close.map(|close| close.code) {
        Some(CloseCode::Policy) => Err(ConnectionError::terminal(
            progress,
            "gateway closed connection with policy violation",
        )),
        Some(CloseCode::Unsupported) => Err(ConnectionError::terminal(
            progress,
            "gateway attributed unsupported frames to this runner",
        )),
        Some(CloseCode::Size) => Err(ConnectionError::terminal(
            progress,
            "gateway attributed oversized frames to this runner",
        )),
        _ => Ok(progress),
    }
}

// close_locally best-effort announces why the runner is abandoning the
// connection; failures to deliver the close frame are deliberately ignored.
async fn close_locally(
    writer: &mut FrameWriter,
    sleeper: &dyn Sleeper,
    code: CloseCode,
    reason: &'static str,
) {
    let close = Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    }));
    tokio::select! {
        biased;
        _ = writer.send(close) => {}
        _ = sleeper.sleep(CLOSE_TIMEOUT) => {}
    }
}

// protocol_violation closes locally with status 1002 and reports a retryable
// ending: a misbehaving gateway is indistinguishable from a transient fault.
async fn protocol_violation(
    writer: &mut FrameWriter,
    sleeper: &dyn Sleeper,
    progress: ConnectionProgress,
    cause: &'static str,
) -> ConnectionError {
    close_locally(writer, sleeper, CloseCode::Protocol, cause).await;
    ConnectionError::retryable(progress, cause)
}

pub(crate) async fn run(
    config: &Config,
    frame_source: &dyn FrameSource,
    sleeper: &dyn Sleeper,
    opening: OpeningHello<'_>,
    next_sequence: &mut u64,
) -> Result<ConnectionProgress, ConnectionError> {
    crate::tls::install_provider();
    let unacknowledged = ConnectionProgress::unacknowledged();
    let mut request = config
        .endpoint()
        .as_str()
        .into_client_request()
        .map_err(|_| ConnectionError::terminal(unacknowledged, "build gateway request"))?;
    let authorization =
        HeaderValue::from_str(&format!("Bearer {}", config.credential().bearer_value()))
            .map_err(|_| ConnectionError::terminal(unacknowledged, "build authorization header"))?;
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, authorization);
    request.headers_mut().insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(SUBPROTOCOL),
    );
    let socket_config = WebSocketConfig::default()
        .max_message_size(Some(MAX_INBOUND_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_INBOUND_MESSAGE_BYTES));
    let connect = connect_async_with_config(request, Some(socket_config), false);
    tokio::pin!(connect);
    let connection = tokio::select! {
        biased;
        result = &mut connect => Some(result),
        _ = sleeper.sleep(CONNECT_TIMEOUT) => None,
    };
    let (socket, response) = match connection {
        Some(Ok(established)) => established,
        Some(Err(WebSocketError::Http(response))) => {
            return Err(match response.status() {
                StatusCode::UNAUTHORIZED => ConnectionError::terminal(
                    unacknowledged,
                    "runner gateway rejected the credential",
                ),
                StatusCode::BAD_REQUEST => ConnectionError::terminal(
                    unacknowledged,
                    "runner gateway rejected the connection request",
                ),
                _ => ConnectionError::retryable(
                    unacknowledged,
                    "runner gateway returned an HTTP error",
                ),
            });
        }
        Some(Err(_)) => {
            return Err(ConnectionError::retryable(
                unacknowledged,
                "connect to runner gateway",
            ));
        }
        None => {
            return Err(ConnectionError::retryable(
                unacknowledged,
                "runner gateway connect timeout",
            ));
        }
    };
    if response
        .headers()
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        != Some(SUBPROTOCOL)
    {
        return Err(ConnectionError::terminal(
            unacknowledged,
            "runner gateway did not select the required subprotocol",
        ));
    }
    let opening_hello = std::str::from_utf8(opening.encoded)
        .map_err(|_| ConnectionError::terminal(unacknowledged, "encode opening hello as UTF-8"))?;
    let (mut writer, mut reader) = socket.split();
    writer
        .send(Message::Text(opening_hello.into()))
        .await
        .map_err(|_| ConnectionError::retryable(unacknowledged, "send opening hello"))?;

    let mut welcome_timer = sleeper.sleep(WELCOME_TIMEOUT);
    let mut inbound_silence_timeout = None;
    let mut progress = ConnectionProgress::unacknowledged();
    let mut pending_effect_acknowledgement: Option<(String, String, u64)> = None;

    loop {
        let message = if let Some(timeout) = inbound_silence_timeout {
            tokio::select! {
                biased;
                message = reader.next() => message,
                _ = sleeper.sleep(timeout) => {
                    close_locally(
                        &mut writer,
                        sleeper,
                        CloseCode::Away,
                        "gateway liveness timeout",
                    ).await;
                    return Err(ConnectionError::retryable(
                        progress,
                        "gateway liveness timeout",
                    ));
                }
            }
        } else {
            tokio::select! {
                biased;
                message = reader.next() => message,
                _ = &mut welcome_timer => {
                    return Err(ConnectionError::retryable(
                        progress,
                        "gateway welcome timeout",
                    ));
                }
            }
        };
        let Some(message) = message else {
            return Ok(progress);
        };
        let message = match message {
            Ok(message) => message,
            Err(WebSocketError::Capacity(_)) => {
                return Err(protocol_violation(
                    &mut writer,
                    sleeper,
                    progress,
                    "oversized gateway frame",
                )
                .await);
            }
            Err(_) => {
                return Err(ConnectionError::retryable(progress, "read gateway frame"));
            }
        };
        match message {
            Message::Text(text) => {
                let Ok(frame) = decode_cloud_frame(text.as_bytes()) else {
                    return Err(protocol_violation(
                        &mut writer,
                        sleeper,
                        progress,
                        "undecodable gateway frame",
                    )
                    .await);
                };
                match frame {
                    CloudFrame::Welcome {
                        ping_interval_seconds,
                        pong_timeout_seconds,
                        ..
                    } if inbound_silence_timeout.is_none() => {
                        let _ = ping_interval_seconds;
                        inbound_silence_timeout = Some(Duration::from_secs(pong_timeout_seconds));
                    }
                    CloudFrame::ObservationAck {
                        acknowledged_message_id,
                        acknowledged_sequence,
                        ..
                    } if acknowledged_message_id == opening.message_id
                        && acknowledged_sequence == opening.sequence =>
                    {
                        progress.opening_acknowledged = true;
                    }
                    CloudFrame::ObservationAck {
                        acknowledged_message_id,
                        acknowledged_sequence,
                        ..
                    } => {
                        let Some((effect_id, message_id, sequence)) =
                            pending_effect_acknowledgement.clone()
                        else {
                            return Err(protocol_violation(
                                &mut writer,
                                sleeper,
                                progress,
                                "unexpected observation acknowledgement",
                            )
                            .await);
                        };
                        if acknowledged_message_id != message_id
                            || acknowledged_sequence != sequence
                        {
                            return Err(protocol_violation(
                                &mut writer,
                                sleeper,
                                progress,
                                "mismatched effect acknowledgement",
                            )
                            .await);
                        }
                        pending_effect_acknowledgement = None;
                        eprintln!(
                            "runner effect received: {effect_id} (execution not implemented)"
                        );
                    }
                    CloudFrame::AssignmentOffer { effect_id, .. }
                        if progress.handshake_completed
                            && pending_effect_acknowledgement.is_none() =>
                    {
                        let sequence = *next_sequence;
                        *next_sequence = next_sequence.checked_add(1).ok_or_else(|| {
                            ConnectionError::terminal(
                                progress,
                                "runner observation sequence overflow",
                            )
                        })?;
                        let message_id = frame_source.public_id("rmsg_");
                        let frame = RunnerFrame::EffectAcknowledged {
                            envelope: RunnerEnvelope {
                                message_id: message_id.clone(),
                                runner_id: config.credential().runner_id().to_owned(),
                                boot_id: opening.boot_id.to_owned(),
                                sequence,
                                sent_at: frame_source.utc_timestamp().map_err(|_| {
                                    ConnectionError::terminal(
                                        progress,
                                        "format effect acknowledgement timestamp",
                                    )
                                })?,
                            },
                            effect_id: effect_id.clone(),
                        };
                        let encoded = encode_runner_frame(&frame).map_err(|_| {
                            ConnectionError::terminal(progress, "encode effect acknowledgement")
                        })?;
                        let encoded = std::str::from_utf8(&encoded).map_err(|_| {
                            ConnectionError::terminal(
                                progress,
                                "encode effect acknowledgement as UTF-8",
                            )
                        })?;
                        writer
                            .send(Message::Text(encoded.into()))
                            .await
                            .map_err(|_| {
                                ConnectionError::retryable(progress, "send effect acknowledgement")
                            })?;
                        pending_effect_acknowledgement = Some((effect_id, message_id, sequence));
                    }
                    _ => {
                        return Err(protocol_violation(
                            &mut writer,
                            sleeper,
                            progress,
                            "unexpected gateway frame",
                        )
                        .await);
                    }
                }
                if inbound_silence_timeout.is_some() && progress.opening_acknowledged {
                    progress.handshake_completed = true;
                }
            }
            Message::Ping(_) => {
                writer
                    .flush()
                    .await
                    .map_err(|_| ConnectionError::retryable(progress, "flush runner pong"))?;
            }
            Message::Pong(_) => {}
            Message::Close(close) => return close_outcome(progress, close),
            Message::Binary(_) => {
                return Err(protocol_violation(
                    &mut writer,
                    sleeper,
                    progress,
                    "binary gateway frame",
                )
                .await);
            }
            Message::Frame(_) => {
                return Err(protocol_violation(
                    &mut writer,
                    sleeper,
                    progress,
                    "unexpected raw gateway frame",
                )
                .await);
            }
        }
    }
}

pub(crate) fn opening_hello(
    frame_source: &dyn FrameSource,
    runner_id: &str,
    boot_id: &str,
    message_id: String,
    sequence: u64,
    runner_version: &str,
) -> Result<Vec<u8>, ConnectionError> {
    let sent_at = frame_source.utc_timestamp().map_err(|_| {
        ConnectionError::terminal(
            ConnectionProgress::unacknowledged(),
            "format opening hello timestamp",
        )
    })?;
    encode_runner_frame(&RunnerFrame::Hello {
        envelope: RunnerEnvelope {
            message_id,
            runner_id: runner_id.to_owned(),
            boot_id: boot_id.to_owned(),
            sequence,
            sent_at,
        },
        runner_version: runner_version.to_owned(),
        max_concurrent_runs: 1,
    })
    .map_err(|_| {
        ConnectionError::terminal(ConnectionProgress::unacknowledged(), "encode opening hello")
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
    use tokio_tungstenite::tungstenite::http::{HeaderValue, header};
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    use super::{
        ConnectionError, ConnectionProgress, FrameSource, OpeningHello, close_outcome,
        opening_hello, run,
    };
    use crate::runner::credential::{Credential, test_credential};
    use crate::runner::service::Sleeper;
    use crate::runner::service::config::Config;
    use crate::runner::service::test_support::{
        accept_fixture_socket, assignment_offer, controlled_sleeper, deterministic_frame_source,
        effect_acknowledgement, expect_close_frame, expect_opening_hello, fixture_listener,
        observation_acknowledgement, sleep_request, welcome,
    };

    const CREDENTIAL: &str =
        "rnr_01k0z6r1w8f4jy2m7q9v3x5abd.abcdefghijklmnopqrstuvwxyzABCDEFG-012345678";
    const BOOT_ID: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe";
    const OPENING_MESSAGE_ID: &str = "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc";

    #[allow(clippy::result_large_err)] // Required by tungstenite's handshake callback type.
    #[tokio::test]
    async fn authenticates_and_completes_hello_and_ping_pong() {
        let (listener, endpoint) = fixture_listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fixture connection");
            let mut socket = accept_hdr_async(stream, |request: &Request, mut response: Response| {
                assert_eq!(
                    request.headers().get(header::AUTHORIZATION).and_then(|value| value.to_str().ok()),
                    Some("Bearer rnr_01k0z6r1w8f4jy2m7q9v3x5abd.abcdefghijklmnopqrstuvwxyzABCDEFG-012345678"),
                );
                assert_eq!(
                    request.headers().get(header::SEC_WEBSOCKET_PROTOCOL).and_then(|value| value.to_str().ok()),
                    Some("scherzo.runner.v1"),
                );
                response.headers_mut().insert(
                    header::SEC_WEBSOCKET_PROTOCOL,
                    HeaderValue::from_static("scherzo.runner.v1"),
                );
                Ok(response)
            })
            .await
            .expect("accept WebSocket fixture");
            let Some(Ok(Message::Text(hello))) = socket.next().await else {
                panic!("fixture did not receive opening hello");
            };
            let hello: serde_json::Value =
                serde_json::from_str(&hello).expect("decode opening hello");
            assert_eq!(hello["messageId"], OPENING_MESSAGE_ID);
            socket.send(welcome()).await.expect("send welcome");
            socket
                .send(observation_acknowledgement(OPENING_MESSAGE_ID, 1))
                .await
                .expect("send opening acknowledgement");
            socket
                .send(Message::Ping(Vec::new().into()))
                .await
                .expect("send Ping");
            let Some(Ok(Message::Pong(_))) = socket.next().await else {
                panic!("fixture did not receive matching Pong");
            };
            socket
                .send(assignment_offer())
                .await
                .expect("send assignment offer");
            let effect_acknowledgement = effect_acknowledgement(&mut socket).await;
            assert_eq!(effect_acknowledgement["type"], "effect_acknowledged");
            assert_eq!(
                effect_acknowledgement["messageId"],
                "rmsg_00000000000000000000000001"
            );
            assert_eq!(effect_acknowledgement["sentAt"], "2026-07-23T00:00:00Z");
            assert_eq!(
                effect_acknowledgement["payload"]["effectId"],
                "eff_01k0z6r1w8f4jy2m7q9v3x5abg"
            );
            let acknowledgement_message_id = effect_acknowledgement["messageId"]
                .as_str()
                .expect("effect acknowledgement message ID");
            socket
                .send(Message::Text(
                    json!({
                        "protocolVersion": 1,
                        "direction": "cloud_to_runner",
                        "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abf",
                        "sentAt": "2026-07-23T00:00:03Z",
                        "type": "observation_ack",
                        "payloadVersion": 1,
                        "payload": {
                            "acknowledgedMessageId": acknowledgement_message_id,
                            "acknowledgedSequence": 2
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("send effect acknowledgement response");
            socket.close(None).await.expect("close fixture socket");
        });

        let directory = TempDir::new().expect("create credential directory");
        let path = directory.path().join("runner.credential");
        fs::write(&path, CREDENTIAL).expect("write credential");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("set credential mode");
        let config = Config::new(
            &endpoint,
            Credential::load(&path).expect("load credential"),
            true,
        )
        .expect("configure loopback gateway");
        let frame_source = deterministic_frame_source();
        let (sleeper, _sleep_requests) = controlled_sleeper();
        let opening = test_opening(&config, frame_source.as_ref());
        let mut next_sequence = 2;
        let outcome = run_test_connection(
            &config,
            frame_source.as_ref(),
            sleeper.as_ref(),
            &opening,
            &mut next_sequence,
        )
        .await
        .expect("run fixture connection");
        assert!(outcome.opening_acknowledged);
        assert!(outcome.handshake_completed);
        assert_eq!(next_sequence, 3);
        server.await.expect("join fixture server");
    }

    #[tokio::test]
    async fn rejects_a_connection_that_never_sends_welcome() {
        let (listener, endpoint) = fixture_listener().await;
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        let server = tokio::spawn(async move {
            let mut socket = accept_fixture_socket(&listener).await;
            expect_opening_hello(&mut socket).await;
            let release = sleep_request(&mut sleep_requests, Duration::from_secs(5)).await;
            release.release();
            std::future::pending::<()>().await;
        });
        let (error, next_sequence) =
            run_failing_fixture_connection(&endpoint, sleeper.as_ref()).await;
        assert_eq!(error.cause(), "gateway welcome timeout");
        assert!(!error.is_terminal());
        assert!(!error.progress.opening_acknowledged);
        assert!(!error.progress.handshake_completed);
        assert_eq!(next_sequence, 2);

        abort_fixture_server(server).await;
    }

    #[tokio::test]
    async fn rejects_inbound_silence_after_handshake() {
        let (listener, endpoint) = fixture_listener().await;
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        let server = tokio::spawn(async move {
            let mut socket = accept_fixture_socket(&listener).await;
            expect_opening_hello(&mut socket).await;

            let welcome_timer = sleep_request(&mut sleep_requests, Duration::from_secs(5)).await;
            socket.send(welcome()).await.expect("send welcome");

            let silence_timer = sleep_request(&mut sleep_requests, Duration::from_secs(2)).await;
            drop(welcome_timer);
            socket
                .send(observation_acknowledgement(OPENING_MESSAGE_ID, 1))
                .await
                .expect("send opening acknowledgement");

            let release_silence = sleep_request(&mut sleep_requests, Duration::from_secs(2)).await;
            drop(silence_timer);
            release_silence.release();
            std::future::pending::<()>().await;
        });
        let (error, next_sequence) =
            run_failing_fixture_connection(&endpoint, sleeper.as_ref()).await;
        assert_eq!(error.cause(), "gateway liveness timeout");
        assert!(error.progress.opening_acknowledged);
        assert!(error.progress.handshake_completed);
        assert_eq!(next_sequence, 2);

        abort_fixture_server(server).await;
    }

    #[tokio::test]
    async fn rejects_a_connection_that_never_completes_the_upgrade() {
        let (listener, endpoint) = fixture_listener().await;
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.expect("accept fixture connection");
            let release = sleep_request(&mut sleep_requests, Duration::from_secs(10)).await;
            release.release();
            std::future::pending::<()>().await;
        });

        let (error, next_sequence) =
            run_failing_fixture_connection(&endpoint, sleeper.as_ref()).await;
        assert_eq!(error.cause(), "runner gateway connect timeout");
        assert!(!error.is_terminal());
        assert_eq!(next_sequence, 2);

        abort_fixture_server(server).await;
    }

    #[tokio::test]
    async fn terminates_when_the_gateway_rejects_the_credential() {
        let (listener, endpoint) = fixture_listener().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept fixture connection");
            let mut buffer = [0_u8; 4096];
            let _ = stream.read(&mut buffer).await;
            stream
                .write_all(b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n")
                .await
                .expect("write 401 response");
        });

        let error = run_fixture_connection(&endpoint)
            .await
            .expect_err("unauthorized connection succeeded");
        assert!(error.is_terminal());
        assert_eq!(error.cause(), "runner gateway rejected the credential");
        server.await.expect("join fixture server");
    }

    #[tokio::test]
    async fn treats_an_oversized_cloud_frame_as_a_protocol_violation() {
        let (listener, endpoint) = fixture_listener().await;
        let server = tokio::spawn(async move {
            let mut socket = accept_fixture_socket(&listener).await;
            expect_opening_hello(&mut socket).await;
            socket
                .send(Message::Text("x".repeat(65_537).into()))
                .await
                .expect("send oversized frame");
            while let Some(Ok(_)) = socket.next().await {}
        });

        let error = run_fixture_connection(&endpoint)
            .await
            .expect_err("oversized frame accepted");
        assert!(!error.is_terminal());
        assert_eq!(error.cause(), "oversized gateway frame");
        server.await.expect("join fixture server");
    }

    #[tokio::test]
    async fn closes_locally_with_going_away_on_inbound_silence() {
        let (listener, endpoint) = fixture_listener().await;
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        let server = tokio::spawn(async move {
            let mut socket = accept_fixture_socket(&listener).await;
            expect_opening_hello(&mut socket).await;
            socket.send(welcome()).await.expect("send welcome");
            let release = sleep_request(&mut sleep_requests, Duration::from_secs(2)).await;
            release.release();
            let close = expect_close_frame(&mut socket).await;
            assert_eq!(close.code, CloseCode::Away);
            assert_eq!(&*close.reason, "gateway liveness timeout");
        });

        let (error, _) = run_failing_fixture_connection(&endpoint, sleeper.as_ref()).await;
        assert!(!error.is_terminal());
        assert_eq!(error.cause(), "gateway liveness timeout");
        server.await.expect("join fixture server");
    }

    #[tokio::test]
    async fn closes_locally_with_protocol_error_on_an_undecodable_frame() {
        let (listener, endpoint) = fixture_listener().await;
        let server = tokio::spawn(async move {
            let mut socket = accept_fixture_socket(&listener).await;
            expect_opening_hello(&mut socket).await;
            socket
                .send(Message::Text("not valid JSON".into()))
                .await
                .expect("send undecodable frame");
            let close = expect_close_frame(&mut socket).await;
            assert_eq!(close.code, CloseCode::Protocol);
        });

        let error = run_fixture_connection(&endpoint)
            .await
            .expect_err("undecodable frame accepted");
        assert!(!error.is_terminal());
        assert_eq!(error.cause(), "undecodable gateway frame");
        server.await.expect("join fixture server");
    }

    #[test]
    fn classifies_received_close_statuses() {
        for (code, cause) in [
            (
                CloseCode::Policy,
                "gateway closed connection with policy violation",
            ),
            (
                CloseCode::Unsupported,
                "gateway attributed unsupported frames to this runner",
            ),
            (
                CloseCode::Size,
                "gateway attributed oversized frames to this runner",
            ),
        ] {
            let error = close_outcome(
                ConnectionProgress::unacknowledged(),
                Some(CloseFrame {
                    code,
                    reason: "".into(),
                }),
            )
            .expect_err("terminal close status succeeded");
            assert!(error.is_terminal());
            assert_eq!(error.cause(), cause);
        }
        for close in [
            None,
            Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "".into(),
            }),
            Some(CloseFrame {
                code: CloseCode::Away,
                reason: "superseded runner session".into(),
            }),
            Some(CloseFrame {
                code: CloseCode::Error,
                reason: "".into(),
            }),
        ] {
            assert!(close_outcome(ConnectionProgress::unacknowledged(), close).is_ok());
        }
    }

    async fn run_fixture_connection(endpoint: &str) -> Result<ConnectionProgress, ConnectionError> {
        let config = test_config(endpoint);
        let frame_source = deterministic_frame_source();
        let (sleeper, _sleep_requests) = controlled_sleeper();
        let opening = test_opening(&config, frame_source.as_ref());
        let mut next_sequence = 2;
        run_test_connection(
            &config,
            frame_source.as_ref(),
            sleeper.as_ref(),
            &opening,
            &mut next_sequence,
        )
        .await
    }

    async fn run_failing_fixture_connection(
        endpoint: &str,
        sleeper: &dyn Sleeper,
    ) -> (ConnectionError, u64) {
        let config = test_config(endpoint);
        let frame_source = deterministic_frame_source();
        let opening = test_opening(&config, frame_source.as_ref());
        let mut next_sequence = 2;
        let error = run_test_connection(
            &config,
            frame_source.as_ref(),
            sleeper,
            &opening,
            &mut next_sequence,
        )
        .await
        .expect_err("fixture connection unexpectedly succeeded");
        (error, next_sequence)
    }

    async fn run_test_connection(
        config: &Config,
        frame_source: &dyn FrameSource,
        sleeper: &dyn Sleeper,
        opening: &[u8],
        next_sequence: &mut u64,
    ) -> Result<ConnectionProgress, ConnectionError> {
        run(
            config,
            frame_source,
            sleeper,
            OpeningHello {
                boot_id: BOOT_ID,
                encoded: opening,
                message_id: OPENING_MESSAGE_ID,
                sequence: 1,
            },
            next_sequence,
        )
        .await
    }

    async fn abort_fixture_server(server: tokio::task::JoinHandle<()>) {
        server.abort();
        assert!(
            server
                .await
                .expect_err("fixture server should be aborted")
                .is_cancelled()
        );
    }

    fn test_config(endpoint: &str) -> Config {
        Config::new(endpoint, test_credential(), true).expect("configure gateway")
    }

    fn test_opening(config: &Config, frame_source: &dyn FrameSource) -> Vec<u8> {
        opening_hello(
            frame_source,
            config.credential().runner_id(),
            BOOT_ID,
            OPENING_MESSAGE_ID.to_owned(),
            1,
            "0.2.0",
        )
        .expect("encode opening hello")
    }
}
