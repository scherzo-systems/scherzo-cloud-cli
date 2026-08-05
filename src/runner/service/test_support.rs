use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderValue, header};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};

use crate::runner::service::assignment::{WallClockHealth, WallClockHealthFailure};
use crate::runner::service::connection::{ConnectionError, FrameSource, run_established};
use crate::runner::service::{ConnectionAttempt, ConnectionFuture, Connector, Sleeper};

pub(crate) type FixtureSocket = WebSocketStream<TcpStream>;

const TEST_WATCHDOG: Duration = Duration::from_secs(10);

#[expect(
    clippy::disallowed_methods,
    reason = "real time is allowed only to keep a broken test from hanging"
)]
pub(crate) async fn with_watchdog<F>(future: F) -> Result<F::Output, tokio::time::error::Elapsed>
where
    F: Future,
{
    tokio::time::timeout(TEST_WATCHDOG, future).await
}

struct DeterministicFrameSource {
    next: AtomicU64,
}

impl FrameSource for DeterministicFrameSource {
    fn public_id(&self, prefix: &str) -> String {
        let value = u128::from(self.next.fetch_add(1, Ordering::Relaxed) + 1);
        format!(
            "{prefix}{}",
            ulid::Ulid::from(value).to_string().to_ascii_lowercase()
        )
    }

    fn utc_timestamp(&self) -> Result<String, ConnectionError> {
        Ok("2026-07-23T00:00:00Z".to_owned())
    }
}

pub(crate) fn deterministic_frame_source() -> Arc<dyn FrameSource> {
    Arc::new(DeterministicFrameSource {
        next: AtomicU64::new(0),
    })
}

struct HealthyWallClock;

impl WallClockHealth for HealthyWallClock {
    fn uncertainty(&self) -> Result<Duration, WallClockHealthFailure> {
        Ok(Duration::ZERO)
    }
}

pub(crate) fn healthy_wall_clock() -> Arc<dyn WallClockHealth> {
    Arc::new(HealthyWallClock)
}

#[derive(Clone, Default)]
pub(crate) struct DeterminismTranscript {
    events: Arc<Mutex<Vec<String>>>,
}

impl DeterminismTranscript {
    pub(crate) fn record(&self, event: String) {
        self.events
            .lock()
            .expect("determinism transcript mutex poisoned")
            .push(event);
    }

    pub(crate) fn snapshot(&self) -> Vec<String> {
        self.events
            .lock()
            .expect("determinism transcript mutex poisoned")
            .clone()
    }
}

pub(crate) struct SleepRelease {
    notification: Arc<Notify>,
    duration: Duration,
    transcript: Option<DeterminismTranscript>,
}

impl SleepRelease {
    pub(crate) fn release(self) {
        if let Some(transcript) = &self.transcript {
            transcript.record(format!("sleep.released:{}ms", self.duration.as_millis()));
        }
        self.notification.notify_one();
    }
}

struct ControlledSleeper {
    requests: mpsc::UnboundedSender<(Duration, SleepRelease)>,
    transcript: Option<DeterminismTranscript>,
}

impl Sleeper for ControlledSleeper {
    fn sleep(&self, duration: Duration) -> super::SleepFuture<'_> {
        let requests = self.requests.clone();
        Box::pin(async move {
            let notification = Arc::new(Notify::new());
            if let Some(transcript) = &self.transcript {
                transcript.record(format!("sleep.requested:{}ms", duration.as_millis()));
            }
            requests
                .send((
                    duration,
                    SleepRelease {
                        notification: Arc::clone(&notification),
                        duration,
                        transcript: self.transcript.clone(),
                    },
                ))
                .expect("controlled sleep receiver should remain open");
            notification.notified().await;
        })
    }
}

pub(crate) fn controlled_sleeper() -> (
    Arc<dyn Sleeper>,
    mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
) {
    controlled_sleeper_with_optional_transcript(None)
}

pub(crate) fn controlled_sleeper_with_transcript(
    transcript: DeterminismTranscript,
) -> (
    Arc<dyn Sleeper>,
    mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
) {
    controlled_sleeper_with_optional_transcript(Some(transcript))
}

fn controlled_sleeper_with_optional_transcript(
    transcript: Option<DeterminismTranscript>,
) -> (
    Arc<dyn Sleeper>,
    mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
) {
    let (requests, receiver) = mpsc::unbounded_channel();
    (
        Arc::new(ControlledSleeper {
            requests,
            transcript,
        }),
        receiver,
    )
}

struct ScriptedDuplexState {
    pending_pong: Mutex<Option<Message>>,
}

pub(crate) struct ScriptedInbound {
    sender: mpsc::UnboundedSender<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    transcript: DeterminismTranscript,
}

impl ScriptedInbound {
    pub(crate) fn send(&self, message: Message) {
        self.transcript
            .record(format!("inbound.queued:{}", describe_message(&message)));
        self.sender
            .send(Ok(message))
            .expect("scripted inbound reader should remain open");
    }
}

pub(crate) struct ScriptedReader {
    receiver: mpsc::UnboundedReceiver<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    state: Arc<ScriptedDuplexState>,
    transcript: DeterminismTranscript,
}

impl Stream for ScriptedReader {
    type Item = Result<Message, tokio_tungstenite::tungstenite::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = std::task::ready!(Pin::new(&mut self.receiver).poll_recv(context));
        if let Some(Ok(message)) = &item {
            self.transcript
                .record(format!("inbound.read:{}", describe_message(message)));
            if let Message::Ping(payload) = message {
                *self
                    .state
                    .pending_pong
                    .lock()
                    .expect("scripted duplex mutex poisoned") =
                    Some(Message::Pong(payload.clone()));
            }
        }
        Poll::Ready(item)
    }
}

pub(crate) struct ScriptedWriter {
    outbound: mpsc::UnboundedSender<Message>,
    state: Arc<ScriptedDuplexState>,
    transcript: DeterminismTranscript,
}

impl ScriptedWriter {
    fn record(&self, message: Message) {
        self.transcript
            .record(format!("outbound:{}", describe_message(&message)));
        self.outbound
            .send(message)
            .expect("scripted outbound receiver should remain open");
    }
}

impl Sink<Message> for ScriptedWriter {
    type Error = tokio_tungstenite::tungstenite::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        self.record(item);
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let pending_pong = self
            .state
            .pending_pong
            .lock()
            .expect("scripted duplex mutex poisoned")
            .take();
        if let Some(pong) = pending_pong {
            self.record(pong);
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

pub(crate) fn scripted_duplex(
    transcript: DeterminismTranscript,
) -> (
    ScriptedInbound,
    ScriptedReader,
    ScriptedWriter,
    mpsc::UnboundedReceiver<Message>,
) {
    let (inbound, receiver) = mpsc::unbounded_channel();
    let (outbound, outbound_receiver) = mpsc::unbounded_channel();
    let state = Arc::new(ScriptedDuplexState {
        pending_pong: Mutex::new(None),
    });
    (
        ScriptedInbound {
            sender: inbound,
            transcript: transcript.clone(),
        },
        ScriptedReader {
            receiver,
            state: Arc::clone(&state),
            transcript: transcript.clone(),
        },
        ScriptedWriter {
            outbound,
            state,
            transcript,
        },
        outbound_receiver,
    )
}

pub(crate) struct ScriptedConnection {
    pub(crate) inbound: ScriptedInbound,
    pub(crate) outbound: mpsc::UnboundedReceiver<Message>,
}

pub(crate) struct ScriptedConnector {
    attempts: mpsc::UnboundedSender<ScriptedConnection>,
    next_attempt: AtomicU64,
    transcript: DeterminismTranscript,
}

impl Connector for ScriptedConnector {
    fn connect<'a>(&'a self, attempt: ConnectionAttempt<'a>) -> ConnectionFuture<'a> {
        Box::pin(async move {
            let attempt_number = self.next_attempt.fetch_add(1, Ordering::Relaxed) + 1;
            self.transcript
                .record(format!("connection.attempt:{attempt_number}"));
            let (inbound, reader, writer, outbound) = scripted_duplex(self.transcript.clone());
            self.attempts
                .send(ScriptedConnection { inbound, outbound })
                .expect("scripted connection receiver should remain open");
            let outcome = run_established(
                attempt.dependencies,
                attempt.opening,
                attempt.next_sequence,
                reader,
                writer,
            )
            .await;
            self.transcript
                .record(describe_connection_outcome(&outcome));
            outcome
        })
    }
}

pub(crate) fn scripted_connector(
    transcript: DeterminismTranscript,
) -> (
    ScriptedConnector,
    mpsc::UnboundedReceiver<ScriptedConnection>,
) {
    let (attempts, receiver) = mpsc::unbounded_channel();
    (
        ScriptedConnector {
            attempts,
            next_attempt: AtomicU64::new(0),
            transcript,
        },
        receiver,
    )
}

fn describe_connection_outcome(
    outcome: &Result<super::connection::ConnectionProgress, ConnectionError>,
) -> String {
    match outcome {
        Ok(progress) => format!(
            "connection.outcome:closed:opening_acknowledged={}:handshake_completed={}",
            progress.opening_acknowledged, progress.handshake_completed
        ),
        Err(error) => format!(
            "connection.outcome:{}:{}:opening_acknowledged={}:handshake_completed={}",
            if error.is_terminal() {
                "terminal"
            } else {
                "retryable"
            },
            error.cause(),
            error.progress.opening_acknowledged,
            error.progress.handshake_completed,
        ),
    }
}

fn describe_message(message: &Message) -> String {
    match message {
        Message::Text(text) => format!("text:{text}"),
        Message::Binary(payload) => format!("binary:{payload:?}"),
        Message::Ping(payload) => format!("ping:{payload:?}"),
        Message::Pong(payload) => format!("pong:{payload:?}"),
        Message::Close(Some(close)) => format!("close:{}:{}", close.code, close.reason),
        Message::Close(None) => "close:none".to_owned(),
        Message::Frame(frame) => format!("frame:{frame:?}"),
    }
}

pub(crate) async fn sleep_request(
    requests: &mut mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
    expected: Duration,
) -> SleepRelease {
    loop {
        let (duration, release) = requests
            .recv()
            .await
            .expect("controlled sleeper closed before requesting the expected timer");
        if duration == expected {
            return release;
        }
    }
}

pub(crate) async fn fixture_listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture listener");
    let endpoint = format!(
        "ws://{}/v1/connect",
        listener.local_addr().expect("fixture address")
    );
    (listener, endpoint)
}

#[allow(
    clippy::result_large_err,
    reason = "tungstenite's handshake callback requires its large error type"
)]
pub(crate) async fn accept_fixture_socket(listener: &TcpListener) -> FixtureSocket {
    accept_fixture_socket_with_headers(listener).await.0
}

#[allow(
    clippy::result_large_err,
    reason = "tungstenite's handshake callback requires its large error type"
)]
pub(crate) async fn accept_fixture_socket_with_headers(
    listener: &TcpListener,
) -> (FixtureSocket, HeaderMap) {
    let (stream, _) = listener.accept().await.expect("accept fixture connection");
    let (captured, capture) = std::sync::mpsc::sync_channel(1);
    let socket = accept_hdr_async(stream, |request: &Request, mut response: Response| {
        captured
            .send(request.headers().clone())
            .expect("capture fixture upgrade headers");
        response.headers_mut().insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("scherzo.runner.v1"),
        );
        Ok(response)
    })
    .await
    .expect("accept WebSocket fixture");
    (
        socket,
        capture
            .try_recv()
            .expect("fixture upgrade headers should be captured"),
    )
}

pub(crate) fn welcome() -> Message {
    Message::Text(
        json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abc",
            "sentAt": "2026-07-23T00:00:00Z",
            "type": "welcome",
            "payloadVersion": 1,
            "payload": {
                "sessionId": "rsn_01k0z6r1w8f4jy2m7q9v3x5abc",
                "pingIntervalSeconds": 1,
                "pongTimeoutSeconds": 2,
                "leasePolicy": {
                    "schemaVersion": 1,
                    "maxClockUncertaintyMilliseconds": 1000,
                    "forceStopAndReapBudgetMilliseconds": 5000,
                    "terminalReportDeliveryBudgetMilliseconds": 5000,
                    "startDeliveryBudgetMilliseconds": 5000,
                    "renewalDeliveryBudgetMilliseconds": 5000,
                    "leaseDurationMilliseconds": 30000,
                    "fencingMarginMilliseconds": 11000
                }
            }
        })
        .to_string()
        .into(),
    )
}

pub(crate) fn observation_acknowledgement(message_id: &str, sequence: u64) -> Message {
    observation_acknowledgement_with_cloud_id(
        "cmsg_01k0z6r1w8f4jy2m7q9v3x5abd",
        "2026-07-23T00:00:01Z",
        message_id,
        sequence,
    )
}

pub(crate) fn effect_observation_acknowledgement(message_id: &str, sequence: u64) -> Message {
    observation_acknowledgement_with_cloud_id(
        "cmsg_01k0z6r1w8f4jy2m7q9v3x5abf",
        "2026-07-23T00:00:03Z",
        message_id,
        sequence,
    )
}

fn observation_acknowledgement_with_cloud_id(
    cloud_message_id: &str,
    sent_at: &str,
    acknowledged_message_id: &str,
    acknowledged_sequence: u64,
) -> Message {
    Message::Text(
        json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": cloud_message_id,
            "sentAt": sent_at,
            "type": "observation_ack",
            "payloadVersion": 1,
            "payload": {
                "acknowledgedMessageId": acknowledged_message_id,
                "acknowledgedSequence": acknowledged_sequence
            }
        })
        .to_string()
        .into(),
    )
}

pub(crate) fn assignment_offer() -> Message {
    Message::Text(
        json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abe",
            "sentAt": "2026-07-23T00:00:02Z",
            "type": "assignment_offer",
            "payloadVersion": 1,
            "payload": {
                "effectId": "eff_01k0z6r1w8f4jy2m7q9v3x5abg",
                "assignmentId": "asn_01k0z6r1w8f4jy2m7q9v3x5abh",
                "runId": "run_01k0z6r1w8f4jy2m7q9v3x5abj",
                "executionSpec": {
                    "executionSpecId": "xsp_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "schemaVersion": 1,
                    "registeredWorkflowId": "wfl_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "executionLimits": {
                        "maximumParallelSteps": 1,
                        "cancellationGraceSeconds": 1
                    }
                },
                "offerExpiresAt": "2026-07-23T01:00:00Z"
            }
        })
        .to_string()
        .into(),
    )
}

pub(crate) async fn effect_acknowledgement(socket: &mut FixtureSocket) -> Value {
    let Some(Ok(Message::Text(message))) = socket.next().await else {
        panic!("fixture did not receive effect acknowledgement");
    };
    serde_json::from_str(&message).expect("decode effect acknowledgement")
}

pub(crate) async fn offer_assignment_after_handshake(
    socket: &mut FixtureSocket,
    opening_message_id: &str,
    opening_sequence: u64,
) -> Value {
    socket.send(welcome()).await.expect("send welcome");
    socket
        .send(observation_acknowledgement(
            opening_message_id,
            opening_sequence,
        ))
        .await
        .expect("send opening acknowledgement");
    socket
        .send(assignment_offer())
        .await
        .expect("send assignment offer");
    effect_acknowledgement(socket).await
}

pub(crate) async fn expect_opening_hello(socket: &mut FixtureSocket) {
    let Some(Ok(Message::Text(_))) = socket.next().await else {
        panic!("fixture did not receive opening hello");
    };
}

pub(crate) async fn accept_opened_fixture_socket(listener: &TcpListener) -> FixtureSocket {
    let mut socket = accept_fixture_socket(listener).await;
    expect_opening_hello(&mut socket).await;
    socket
}

pub(crate) async fn expect_close_frame(socket: &mut FixtureSocket) -> CloseFrame {
    loop {
        match socket.next().await {
            Some(Ok(Message::Close(Some(frame)))) => break frame,
            Some(Ok(Message::Close(None))) => panic!("fixture received a close without a body"),
            Some(Ok(_)) => continue,
            other => panic!("fixture did not receive close: {other:?}"),
        }
    }
}
