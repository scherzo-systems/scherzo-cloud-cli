use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::{HeaderValue, header};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};

use crate::runner::service::Sleeper;
use crate::runner::service::connection::{ConnectionError, FrameSource};

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

pub(crate) struct SleepRelease {
    notification: Arc<Notify>,
}

impl SleepRelease {
    pub(crate) fn release(self) {
        self.notification.notify_one();
    }
}

struct ControlledSleeper {
    requests: mpsc::UnboundedSender<(Duration, SleepRelease)>,
}

impl Sleeper for ControlledSleeper {
    fn sleep(&self, duration: Duration) -> super::SleepFuture<'_> {
        let requests = self.requests.clone();
        Box::pin(async move {
            let notification = Arc::new(Notify::new());
            requests
                .send((
                    duration,
                    SleepRelease {
                        notification: Arc::clone(&notification),
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
    let (requests, receiver) = mpsc::unbounded_channel();
    (Arc::new(ControlledSleeper { requests }), receiver)
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

// Required by tungstenite's handshake callback type.
#[allow(clippy::result_large_err)]
pub(crate) async fn accept_fixture_socket(listener: &TcpListener) -> FixtureSocket {
    let (stream, _) = listener.accept().await.expect("accept fixture connection");
    accept_hdr_async(stream, |_: &Request, mut response: Response| {
        response.headers_mut().insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("scherzo.runner.v1"),
        );
        Ok(response)
    })
    .await
    .expect("accept WebSocket fixture")
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
                "pongTimeoutSeconds": 2
            }
        })
        .to_string()
        .into(),
    )
}

pub(crate) fn observation_acknowledgement(message_id: &str, sequence: u64) -> Message {
    Message::Text(
        json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abd",
            "sentAt": "2026-07-23T00:00:01Z",
            "type": "observation_ack",
            "payloadVersion": 1,
            "payload": {
                "acknowledgedMessageId": message_id,
                "acknowledgedSequence": sequence
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
                "leaseExpiresAt": "2026-07-23T01:00:00Z"
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

pub(crate) async fn expect_opening_hello(socket: &mut FixtureSocket) {
    let Some(Ok(Message::Text(_))) = socket.next().await else {
        panic!("fixture did not receive opening hello");
    };
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
