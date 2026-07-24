#![deny(clippy::disallowed_methods)]

mod backoff;
mod config;
mod connection;
#[cfg(test)]
mod determinism_spike;
#[cfg(test)]
mod test_support;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub(crate) use config::Config;

use backoff::Backoff;
use connection::{
    ConnectionError, ConnectionProgress, FrameSource, OpeningHello, SystemFrameSource,
    opening_hello,
};

type SleepFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub(crate) trait Sleeper: Send + Sync {
    fn sleep(&self, duration: std::time::Duration) -> SleepFuture<'_>;
}

struct TokioSleeper;

impl Sleeper for TokioSleeper {
    #[expect(
        clippy::disallowed_methods,
        reason = "TokioSleeper is the production boundary for wall-clock sleeps"
    )]
    fn sleep(&self, duration: std::time::Duration) -> SleepFuture<'_> {
        Box::pin(tokio::time::sleep(duration))
    }
}

#[derive(Debug)]
pub(crate) enum ServiceError {
    BuildRuntime,
    Connection(ConnectionError),
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BuildRuntime => formatter.write_str("start runner service"),
            Self::Connection(error) => {
                write!(formatter, "runner service stopped unexpectedly: {error}")
            }
        }
    }
}

impl std::error::Error for ServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BuildRuntime => None,
            Self::Connection(error) => Some(error),
        }
    }
}

pub(crate) fn run(config: Config) -> Result<(), ServiceError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| ServiceError::BuildRuntime)?;
    runtime.block_on(run_until_cancelled(config))
}

async fn run_until_cancelled(config: Config) -> Result<(), ServiceError> {
    run_until_cancelled_with_dependencies(
        config,
        Arc::new(SystemFrameSource),
        Arc::new(TokioSleeper),
    )
    .await
}

async fn run_until_cancelled_with_dependencies(
    config: Config,
    frame_source: Arc<dyn FrameSource>,
    sleeper: Arc<dyn Sleeper>,
) -> Result<(), ServiceError> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| ServiceError::BuildRuntime)?;
    let boot_id = frame_source.public_id("rbt_");
    let mut opening_sequence = 1;
    let mut sequence = opening_sequence;
    let mut opening_message_id = frame_source.public_id("rmsg_");
    let mut opening = opening_hello(
        frame_source.as_ref(),
        config.credential().runner_id(),
        &boot_id,
        opening_message_id.clone(),
        opening_sequence,
        env!("CARGO_PKG_VERSION"),
    )
    .map_err(ServiceError::Connection)?;
    sequence = sequence
        .checked_add(1)
        .ok_or_else(|| ServiceError::Connection(sequence_overflow()))?;
    let mut backoff = Backoff::new();

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = terminate.recv() => return Ok(()),
            result = connection::run(
                &config,
                frame_source.as_ref(),
                sleeper.as_ref(),
                OpeningHello {
                    boot_id: &boot_id,
                    encoded: &opening,
                    message_id: &opening_message_id,
                    sequence: opening_sequence,
                },
                &mut sequence,
            ) => {
                let (progress, cause) = match result {
                    Ok(progress) => (progress, "gateway closed connection"),
                    Err(error) if error.is_terminal() => return Err(ServiceError::Connection(error)),
                    Err(error) => (error.progress, error.cause()),
                };
                if progress.handshake_completed {
                    backoff.reset();
                }
                if progress.opening_acknowledged {
                    opening_message_id = frame_source.public_id("rmsg_");
                    opening_sequence = sequence;
                    opening = opening_hello(
                        frame_source.as_ref(),
                        config.credential().runner_id(),
                        &boot_id,
                        opening_message_id.clone(),
                        opening_sequence,
                        env!("CARGO_PKG_VERSION"),
                    ).map_err(ServiceError::Connection)?;
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| ServiceError::Connection(sequence_overflow()))?;
                }
                let delay = backoff.next_delay();
                eprintln!(
                    "runner gateway disconnected ({cause}); retrying in {} seconds",
                    delay.as_secs()
                );
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => return Ok(()),
                    _ = terminate.recv() => return Ok(()),
                    _ = sleeper.sleep(delay) => {}
                }
            }
        }
    }
}

const fn sequence_overflow() -> ConnectionError {
    ConnectionError::terminal(
        ConnectionProgress::unacknowledged(),
        "runner sequence overflow",
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    use super::sequence_overflow;
    use super::test_support::{
        accept_fixture_socket, assignment_offer, controlled_sleeper, deterministic_frame_source,
        effect_acknowledgement, expect_opening_hello, fixture_listener,
        observation_acknowledgement, welcome, with_watchdog,
    };
    use super::{Config, ServiceError, run_until_cancelled_with_dependencies};
    use crate::runner::credential::test_credential;

    #[test]
    fn reports_connection_failure_cause() {
        let error = ServiceError::Connection(sequence_overflow());
        assert_eq!(
            error.to_string(),
            "runner service stopped unexpectedly: runner gateway connection failed: runner sequence overflow"
        );
    }

    #[tokio::test]
    async fn exits_when_the_gateway_closes_with_policy_violation() {
        let (listener, endpoint) = fixture_listener().await;
        let server = tokio::spawn(async move {
            let mut socket = accept_fixture_socket(&listener).await;
            expect_opening_hello(&mut socket).await;
            socket
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Policy,
                    reason: "invalid runner observation".into(),
                })))
                .await
                .expect("send policy close");
            while let Some(Ok(_)) = socket.next().await {}
        });

        let config = Config::new(&endpoint, test_credential(), true).expect("configure gateway");
        let (sleeper, _sleep_requests) = controlled_sleeper();
        let error = with_watchdog(run_until_cancelled_with_dependencies(
            config,
            deterministic_frame_source(),
            sleeper,
        ))
        .await
        .expect("runner retried a terminal policy close")
        .expect_err("policy close did not stop the service");
        assert_eq!(
            error.to_string(),
            "runner service stopped unexpectedly: runner gateway connection failed: \
             gateway closed connection with policy violation"
        );
        server.await.expect("fixture server failed");
    }

    #[tokio::test]
    async fn replaces_an_acknowledged_opening_after_a_connection_error() {
        let (listener, endpoint) = fixture_listener().await;
        let (failure_sent, failure_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut socket = accept_fixture_socket(&listener).await;
            let Some(Ok(Message::Text(first_hello))) = socket.next().await else {
                panic!("fixture did not receive first opening hello");
            };
            let first_hello: serde_json::Value =
                serde_json::from_str(&first_hello).expect("decode first opening hello");
            let first_message_id = first_hello["messageId"]
                .as_str()
                .expect("first opening message ID")
                .to_owned();
            assert_eq!(first_message_id, "rmsg_00000000000000000000000002");
            assert_eq!(first_hello["bootId"], "rbt_00000000000000000000000001");
            assert_eq!(first_hello["sentAt"], "2026-07-23T00:00:00Z");
            let first_sequence = first_hello["sequence"]
                .as_u64()
                .expect("first opening sequence");
            assert_eq!(first_sequence, 1);

            socket.send(welcome()).await.expect("send welcome");
            socket
                .send(observation_acknowledgement(
                    &first_message_id,
                    first_sequence,
                ))
                .await
                .expect("send opening acknowledgement");
            socket
                .send(assignment_offer())
                .await
                .expect("send assignment offer");
            let effect_acknowledgement = effect_acknowledgement(&mut socket).await;
            assert_eq!(effect_acknowledgement["sequence"], 2);

            socket
                .send(Message::Text("not valid JSON".into()))
                .await
                .expect("send invalid frame");
            failure_sent
                .send(())
                .expect("report first connection failure");
            drop(socket);

            let mut socket = accept_fixture_socket(&listener).await;
            let Some(Ok(Message::Text(second_hello))) = socket.next().await else {
                panic!("fixture did not receive replacement opening hello");
            };
            let second_hello: serde_json::Value =
                serde_json::from_str(&second_hello).expect("decode replacement opening hello");
            assert_eq!(second_hello["messageId"], "rmsg_00000000000000000000000004");
            assert_ne!(second_hello["messageId"], first_message_id);
            assert_eq!(second_hello["sequence"], 3);
            assert_eq!(second_hello["sentAt"], "2026-07-23T00:00:00Z");
        });

        let config = Config::new(&endpoint, test_credential(), true).expect("configure gateway");
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        let service = tokio::spawn(run_until_cancelled_with_dependencies(
            config,
            deterministic_frame_source(),
            sleeper,
        ));
        with_watchdog(failure_received)
            .await
            .expect("first connection did not reach its failure")
            .expect("fixture server dropped failure signal");
        let release_sleep = loop {
            let (delay, release_sleep) = with_watchdog(sleep_requests.recv())
                .await
                .expect("runner did not request a timer")
                .expect("controlled sleeper closed");
            if delay < Duration::from_secs(1) {
                break release_sleep;
            }
            release_sleep.release();
        };
        release_sleep.release();
        let server_result = with_watchdog(server).await;
        service.abort();
        assert!(
            service
                .await
                .expect_err("service task should be aborted")
                .is_cancelled()
        );
        server_result
            .expect("runner did not reconnect")
            .expect("fixture server failed");
    }
}
