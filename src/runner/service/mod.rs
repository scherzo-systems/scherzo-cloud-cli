mod assignment;
mod backoff;
mod config;
mod connection;
#[cfg(test)]
mod conversation;
#[cfg(test)]
mod determinism_spike;
mod execution;
#[cfg(test)]
mod test_support;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use opentelemetry::KeyValue;

pub(crate) use config::{AssignmentConfig, Config};

use crate::execution::workflow::cancellation::MAXIMUM_CANCELLATION_GRACE;
use crate::runner::telemetry::{self, Event, Outcome, Recorder};
use assignment::{AssignmentManager, SystemWallClockHealth, WallClockHealth};
use backoff::Backoff;
use connection::{
    ActiveEffectEvent, ConnectionCause, ConnectionDependencies, ConnectionError,
    ConnectionProgress, FailureKind, FrameSource, OpeningHello, SystemFrameSource, opening_hello,
    record_progress,
};

type SleepFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
type ConnectionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ConnectionProgress, ConnectionError>> + Send + 'a>>;
type ShutdownFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

const SHUTDOWN_DELIVERY_AND_CLEANUP_RESERVE: std::time::Duration =
    std::time::Duration::from_secs(15);
const SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(
    MAXIMUM_CANCELLATION_GRACE.as_secs() + SHUTDOWN_DELIVERY_AND_CLEANUP_RESERVE.as_secs(),
);

pub(crate) trait Sleeper: Send + Sync {
    fn now(&self) -> std::time::Instant;
    fn sleep(&self, duration: std::time::Duration) -> SleepFuture<'_>;
}

trait Shutdown: Send {
    fn wait(&mut self) -> ShutdownFuture<'_>;
}

struct ProcessShutdown {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

impl ProcessShutdown {
    fn new() -> Result<Self, ServiceError> {
        let interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .map_err(|_| ServiceError::BuildRuntime)?;
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|_| ServiceError::BuildRuntime)?;
        Ok(Self {
            interrupt,
            terminate,
        })
    }
}

impl Shutdown for ProcessShutdown {
    fn wait(&mut self) -> ShutdownFuture<'_> {
        Box::pin(async {
            tokio::select! {
                _ = self.interrupt.recv() => {}
                _ = self.terminate.recv() => {}
            }
        })
    }
}

struct TokioSleeper;

impl Sleeper for TokioSleeper {
    #[expect(
        clippy::disallowed_methods,
        reason = "TokioSleeper is the production boundary for monotonic runner time"
    )]
    fn now(&self) -> std::time::Instant {
        std::time::Instant::now()
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "TokioSleeper is the production boundary for wall-clock sleeps"
    )]
    fn sleep(&self, duration: std::time::Duration) -> SleepFuture<'_> {
        Box::pin(tokio::time::sleep(duration))
    }
}

struct ConnectionAttempt<'a> {
    dependencies: ConnectionDependencies<'a>,
    opening: OpeningHello<'a>,
    next_sequence: &'a mut u64,
}

trait Connector: Send + Sync {
    fn connect<'a>(&'a self, attempt: ConnectionAttempt<'a>) -> ConnectionFuture<'a>;
}

struct WebSocketConnector;

impl Connector for WebSocketConnector {
    fn connect<'a>(&'a self, attempt: ConnectionAttempt<'a>) -> ConnectionFuture<'a> {
        Box::pin(connection::run(
            attempt.dependencies,
            attempt.opening,
            attempt.next_sequence,
        ))
    }
}

struct ConnectionLoopDependencies {
    config: Config,
    frame_source: Arc<dyn FrameSource>,
    sleeper: Arc<dyn Sleeper>,
    recorder: Arc<Recorder>,
    wall_clock: Arc<dyn WallClockHealth>,
    boot_id: String,
}

impl ConnectionLoopDependencies {
    fn new(
        config: Config,
        frame_source: Arc<dyn FrameSource>,
        sleeper: Arc<dyn Sleeper>,
        recorder: Arc<Recorder>,
        wall_clock: Arc<dyn WallClockHealth>,
        boot_id: String,
    ) -> Self {
        Self {
            config,
            frame_source,
            sleeper,
            recorder,
            wall_clock,
            boot_id,
        }
    }
}

#[derive(Debug)]
pub(crate) enum ServiceError {
    BuildRuntime,
    AssignmentShutdown,
    ShutdownForced,
    ShutdownDeadlineExceeded,
    Connection(ConnectionError),
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BuildRuntime => formatter.write_str("start runner service"),
            Self::AssignmentShutdown => formatter.write_str("prepare runner assignment shutdown"),
            Self::ShutdownForced => {
                formatter.write_str("runner shutdown forced by repeated signal")
            }
            Self::ShutdownDeadlineExceeded => {
                formatter.write_str("runner graceful shutdown deadline exceeded")
            }
            Self::Connection(error) => {
                write!(formatter, "runner service stopped unexpectedly: {error}")
            }
        }
    }
}

impl std::error::Error for ServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BuildRuntime
            | Self::AssignmentShutdown
            | Self::ShutdownForced
            | Self::ShutdownDeadlineExceeded => None,
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
    let frame_source: Arc<dyn FrameSource> = Arc::new(SystemFrameSource);
    let sleeper: Arc<dyn Sleeper> = Arc::new(TokioSleeper);
    let boot_id = frame_source.public_id("rbt_");
    let recorder = Recorder::stderr(&boot_id);
    let wall_clock: Arc<dyn WallClockHealth> = Arc::new(SystemWallClockHealth);
    let mut shutdown = ProcessShutdown::new()?;
    run_service_loop(
        config,
        frame_source,
        sleeper,
        recorder,
        wall_clock,
        boot_id,
        &mut shutdown,
    )
    .await
}

#[cfg(test)]
async fn run_until_cancelled_with_dependencies(
    config: Config,
    frame_source: Arc<dyn FrameSource>,
    sleeper: Arc<dyn Sleeper>,
    recorder: Arc<Recorder>,
    wall_clock: Arc<dyn WallClockHealth>,
    mut shutdown: Box<dyn Shutdown>,
) -> Result<(), ServiceError> {
    let boot_id = frame_source.public_id("rbt_");
    run_service_loop(
        config,
        frame_source,
        sleeper,
        recorder,
        wall_clock,
        boot_id,
        shutdown.as_mut(),
    )
    .await
}

async fn run_service_loop(
    config: Config,
    frame_source: Arc<dyn FrameSource>,
    sleeper: Arc<dyn Sleeper>,
    recorder: Arc<Recorder>,
    wall_clock: Arc<dyn WallClockHealth>,
    boot_id: String,
    shutdown: &mut dyn Shutdown,
) -> Result<(), ServiceError> {
    run_connection_loop(
        ConnectionLoopDependencies::new(
            config,
            frame_source,
            sleeper,
            recorder,
            wall_clock,
            boot_id,
        ),
        &WebSocketConnector,
        Backoff::new(),
        shutdown,
    )
    .await
}

async fn run_connection_loop(
    dependencies: ConnectionLoopDependencies,
    connector: &dyn Connector,
    mut backoff: Backoff,
    shutdown: &mut dyn Shutdown,
) -> Result<(), ServiceError> {
    let ConnectionLoopDependencies {
        config,
        frame_source,
        sleeper,
        recorder,
        wall_clock,
        boot_id,
    } = dependencies;
    let assignment_manager = Mutex::new(AssignmentManager::new_with_sleeper(
        &config,
        boot_id.clone(),
        wall_clock,
        Arc::clone(&sleeper),
    ));
    let mut opening_sequence = 1;
    let mut sequence = opening_sequence;
    let mut opening_message_id = frame_source.public_id("rmsg_");
    let mut opening = opening_hello(
        frame_source.as_ref(),
        config.credential().runner_id(),
        &boot_id,
        opening_message_id.clone(),
        opening_sequence,
        crate::build_info::VERSION,
    )
    .map_err(ServiceError::Connection)?;
    sequence = sequence
        .checked_add(1)
        .ok_or_else(|| ServiceError::Connection(sequence_overflow()))?;
    let mut attempt = 1_u64;
    let mut shutting_down = false;
    let mut shutdown_deadline: Option<Pin<Box<dyn Future<Output = ()> + Send>>> = None;

    loop {
        if shutting_down
            && assignment_manager
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .shutdown_complete()
        {
            return Ok(());
        }
        let connection_event = connection_event(&recorder, &config, &boot_id, attempt);
        let active_effect_event = ActiveEffectEvent::new();
        let result = {
            let connection = connector.connect(ConnectionAttempt {
                dependencies: ConnectionDependencies::new(
                    &config,
                    frame_source.as_ref(),
                    sleeper.as_ref(),
                    &recorder,
                    &connection_event,
                    &active_effect_event,
                    &assignment_manager,
                    attempt,
                ),
                opening: OpeningHello {
                    boot_id: &boot_id,
                    encoded: &opening,
                    message_id: &opening_message_id,
                    sequence: opening_sequence,
                },
                next_sequence: &mut sequence,
            });
            tokio::pin!(connection);
            loop {
                if shutting_down
                    && assignment_manager
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .shutdown_complete()
                {
                    cancel_attempt(&connection_event, &active_effect_event);
                    return Ok(());
                }
                let notification = assignment_manager
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .notification();
                let notified = notification.notified();
                tokio::pin!(notified);
                if shutting_down {
                    let Some(deadline) = shutdown_deadline.as_mut() else {
                        return Err(ServiceError::AssignmentShutdown);
                    };
                    tokio::select! {
                        biased;
                        _ = shutdown.wait() => {
                            cancel_attempt(&connection_event, &active_effect_event);
                            return Err(ServiceError::ShutdownForced);
                        }
                        result = &mut connection => break result,
                        () = &mut notified => continue,
                        () = deadline => {
                            cancel_attempt(&connection_event, &active_effect_event);
                            return Err(ServiceError::ShutdownDeadlineExceeded);
                        }
                    }
                } else {
                    tokio::select! {
                        biased;
                        _ = shutdown.wait() => {
                            assignment_manager
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .begin_shutdown()
                                .map_err(|_| ServiceError::AssignmentShutdown)?;
                            shutting_down = true;
                            let deadline_sleeper = Arc::clone(&sleeper);
                            shutdown_deadline = Some(Box::pin(async move {
                                deadline_sleeper.sleep(SHUTDOWN_TIMEOUT).await;
                            }));
                        }
                        result = &mut connection => break result,
                    }
                }
            }
        };
        let (progress, cause, kind) = match result {
            Ok(progress) => (
                progress,
                ConnectionCause::GatewayClosedConnection,
                FailureKind::Retryable,
            ),
            Err(error) if error.is_terminal() => {
                finish_connection_event(
                    &connection_event,
                    error.progress,
                    error.kind(),
                    error.connection_cause(),
                    None,
                    Outcome::Failure,
                );
                return Err(ServiceError::Connection(error));
            }
            Err(error) => (error.progress, error.connection_cause(), error.kind()),
        };
        if progress.handshake_completed {
            backoff.reset();
        }
        let delay = backoff.next_delay();
        let outcome = if cause.is_timeout() {
            Outcome::Timeout
        } else {
            Outcome::Disconnected
        };
        finish_connection_event(
            &connection_event,
            progress,
            kind,
            cause,
            Some(delay),
            outcome,
        );
        if progress.opening_acknowledged {
            opening_message_id = frame_source.public_id("rmsg_");
            opening_sequence = sequence;
            opening = opening_hello(
                frame_source.as_ref(),
                config.credential().runner_id(),
                &boot_id,
                opening_message_id.clone(),
                opening_sequence,
                crate::build_info::VERSION,
            )
            .map_err(ServiceError::Connection)?;
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| ServiceError::Connection(sequence_overflow()))?;
        }
        attempt = attempt.saturating_add(1);
        if shutting_down {
            let notification = assignment_manager
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .notification();
            let Some(deadline) = shutdown_deadline.as_mut() else {
                return Err(ServiceError::AssignmentShutdown);
            };
            tokio::select! {
                biased;
                () = shutdown.wait() => return Err(ServiceError::ShutdownForced),
                () = deadline => return Err(ServiceError::ShutdownDeadlineExceeded),
                () = notification.notified() => {}
                () = sleeper.sleep(delay) => {}
            }
        } else {
            tokio::select! {
                _ = shutdown.wait() => {
                    assignment_manager
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .begin_shutdown()
                        .map_err(|_| ServiceError::AssignmentShutdown)?;
                    shutting_down = true;
                    let deadline_sleeper = Arc::clone(&sleeper);
                    shutdown_deadline = Some(Box::pin(async move {
                        deadline_sleeper.sleep(SHUTDOWN_TIMEOUT).await;
                    }));
                }
                _ = sleeper.sleep(delay) => {}
            }
        }
    }
}

fn connection_event(recorder: &Recorder, config: &Config, boot_id: &str, attempt: u64) -> Event {
    let mut attributes = vec![
        KeyValue::new(
            telemetry::attribute::RUNNER_ID,
            config.credential().runner_id().to_owned(),
        ),
        KeyValue::new(telemetry::attribute::RUNNER_BOOT_ID, boot_id.to_owned()),
        KeyValue::new(
            telemetry::attribute::RUNNER_VERSION,
            crate::build_info::VERSION,
        ),
        KeyValue::new(
            telemetry::attribute::CONNECTION_ATTEMPT,
            telemetry::integer(attempt),
        ),
    ];
    if let Some(address) = config.endpoint().host_str() {
        attributes.push(KeyValue::new(
            telemetry::attribute::SERVER_ADDRESS,
            address.to_owned(),
        ));
    }
    if let Some(port) = config.endpoint().port_or_known_default() {
        attributes.push(KeyValue::new(
            telemetry::attribute::SERVER_PORT,
            i64::from(port),
        ));
    }
    let event = recorder.start("runner.gateway_connection", attributes);
    record_progress(&event, ConnectionProgress::unacknowledged());
    event
}

fn cancel_attempt(connection_event: &Event, active_effect_event: &ActiveEffectEvent) {
    active_effect_event.finish(Outcome::Cancelled, None);
    connection_event.finish(Outcome::Cancelled);
}

fn finish_connection_event(
    event: &Event,
    progress: ConnectionProgress,
    kind: FailureKind,
    cause: ConnectionCause,
    backoff: Option<std::time::Duration>,
    outcome: Outcome,
) {
    record_progress(event, progress);
    event.set(KeyValue::new(
        telemetry::attribute::FAILURE_KIND,
        kind.as_str(),
    ));
    event.set(KeyValue::new(
        telemetry::attribute::ERROR_TYPE,
        cause.error_type(),
    ));
    if let Some(backoff) = backoff {
        event.set(KeyValue::new(
            telemetry::attribute::BACKOFF_MS,
            telemetry::integer_u128(backoff.as_millis()),
        ));
    }
    event.finish(outcome);
}

const fn sequence_overflow() -> ConnectionError {
    ConnectionError::terminal(
        ConnectionProgress::unacknowledged(),
        ConnectionCause::RunnerSequenceOverflow,
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::Notify;
    use tokio_tungstenite::tungstenite::Message;

    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    use super::sequence_overflow;
    use super::test_support::{
        FixtureSocket, SleepRelease, accept_fixture_socket, accept_fixture_socket_with_headers,
        accept_opened_fixture_socket, assignment_offer, controlled_shutdown, controlled_sleeper,
        deterministic_frame_source, effect_acknowledgement, effect_observation_acknowledgement,
        expect_close_frame, expect_opening_hello, fixture_listener, healthy_wall_clock,
        observation_acknowledgement, offer_assignment_after_handshake, sleep_request,
        terminal_observation_acknowledgement, welcome, with_watchdog,
    };
    use super::{
        AssignmentConfig, Config, SHUTDOWN_TIMEOUT, ServiceError, Sleeper,
        run_until_cancelled_with_dependencies,
    };
    use crate::runner::credential::test_credential;
    use crate::runner::telemetry::{TestCapture, test_recorder};

    #[test]
    fn shutdown_timeout_accommodates_maximum_cancellation_grace() {
        assert_eq!(
            SHUTDOWN_TIMEOUT,
            super::MAXIMUM_CANCELLATION_GRACE + Duration::from_secs(15)
        );
    }

    #[test]
    fn reports_connection_failure_cause() {
        let error = ServiceError::Connection(sequence_overflow());
        assert_eq!(
            error.to_string(),
            "runner service stopped unexpectedly: runner gateway connection failed: runner sequence overflow"
        );
    }

    fn accepted_assignment_config(endpoint: &str) -> (tempfile::TempDir, Config) {
        let temporary = tempfile::tempdir().expect("create service fixture root");
        let source = temporary.path().join("source");
        let work = temporary.path().join("work");
        fs::create_dir(&source).expect("create workflow source");
        fs::create_dir(&work).expect("create runner work root");
        fs::write(
            source.join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        )
        .expect("write workflow fixture");
        let assignment = AssignmentConfig::new(
            "wfl_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            &source,
            Path::new("workflow.yaml"),
            &work,
        )
        .expect("configure assignment");
        let config =
            Config::new(endpoint, test_credential(), true, assignment).expect("configure gateway");
        (temporary, config)
    }

    async fn accept_assignment_interruption(
        listener: &tokio::net::TcpListener,
        accepted_sent: tokio::sync::oneshot::Sender<()>,
    ) -> (FixtureSocket, serde_json::Value, serde_json::Value) {
        let mut socket = accept_opened_fixture_socket(listener).await;
        let effect =
            offer_assignment_after_handshake(&mut socket, "rmsg_00000000000000000000000002", 1)
                .await;
        socket
            .send(effect_observation_acknowledgement(
                effect["messageId"].as_str().unwrap(),
                effect["sequence"].as_u64().unwrap(),
            ))
            .await
            .expect("acknowledge offer receipt");
        let Some(Ok(Message::Text(accepted))) = socket.next().await else {
            panic!("fixture did not receive assignment acceptance");
        };
        let accepted: serde_json::Value =
            serde_json::from_str(&accepted).expect("decode assignment acceptance");
        assert_eq!(accepted["type"], "assignment_accepted");
        accepted_sent
            .send(())
            .expect("report assignment acceptance");

        let Some(Ok(Message::Text(interrupted))) = socket.next().await else {
            panic!("fixture did not receive shutdown interruption");
        };
        let interrupted: serde_json::Value =
            serde_json::from_str(&interrupted).expect("decode shutdown interruption");
        assert_eq!(interrupted["type"], "assignment_interrupted");
        assert_eq!(interrupted["payload"]["reason"], "graceful_shutdown");
        (socket, accepted, interrupted)
    }

    fn spawn_fixture_service(
        endpoint: &str,
        sleeper: Arc<dyn Sleeper>,
    ) -> (
        tokio::task::JoinHandle<Result<(), ServiceError>>,
        TestCapture,
        Arc<Notify>,
    ) {
        let config = Config::fixture(endpoint, test_credential(), true).expect("configure gateway");
        spawn_configured_service(config, sleeper)
    }

    fn spawn_configured_service(
        config: Config,
        sleeper: Arc<dyn Sleeper>,
    ) -> (
        tokio::task::JoinHandle<Result<(), ServiceError>>,
        TestCapture,
        Arc<Notify>,
    ) {
        let (recorder, capture) = test_recorder("rbt_00000000000000000000000001");
        let (shutdown, shutdown_trigger) = controlled_shutdown();
        let service = tokio::spawn(run_until_cancelled_with_dependencies(
            config,
            deterministic_frame_source(),
            sleeper,
            recorder,
            healthy_wall_clock(),
            shutdown,
        ));
        (service, capture, shutdown_trigger)
    }

    struct AcceptedAssignmentService {
        _temporary: tempfile::TempDir,
        task: tokio::task::JoinHandle<Result<(), ServiceError>>,
        shutdown_trigger: Arc<Notify>,
        _sleep_requests: tokio::sync::mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
    }

    fn spawn_accepted_assignment_service(endpoint: &str) -> AcceptedAssignmentService {
        let (sleeper, sleep_requests) = controlled_sleeper();
        let (temporary, config) = accepted_assignment_config(endpoint);
        let (task, _capture, shutdown_trigger) = spawn_configured_service(config, sleeper);
        AcceptedAssignmentService {
            _temporary: temporary,
            task,
            shutdown_trigger,
            _sleep_requests: sleep_requests,
        }
    }

    async fn begin_accepted_assignment_shutdown(
        endpoint: &str,
        accepted_received: tokio::sync::oneshot::Receiver<()>,
    ) -> AcceptedAssignmentService {
        let service = spawn_accepted_assignment_service(endpoint);
        with_watchdog(accepted_received)
            .await
            .expect("runner did not accept assignment")
            .expect("fixture dropped acceptance signal");
        service.shutdown_trigger.notify_one();
        service
    }

    async fn backoff_request(
        requests: &mut tokio::sync::mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
    ) -> (Duration, SleepRelease) {
        loop {
            let (delay, release) = with_watchdog(requests.recv())
                .await
                .expect("runner did not request backoff")
                .expect("controlled sleeper closed");
            if delay < Duration::from_secs(1) {
                return (delay, release);
            }
            drop(release);
        }
    }

    async fn abort_service(service: tokio::task::JoinHandle<Result<(), ServiceError>>) {
        service.abort();
        assert!(
            service
                .await
                .expect_err("service task should be aborted")
                .is_cancelled()
        );
    }

    fn assert_attempt_event_pair(
        capture: &TestCapture,
        effect_outcome: &str,
        effect_error_type: Option<&str>,
    ) -> serde_json::Map<String, serde_json::Value> {
        assert_eq!(capture.events().len(), 2);
        let effect = capture.event("runner.effect_acknowledgement");
        assert_eq!(effect["scherzo.outcome"], effect_outcome);
        assert_eq!(
            effect.get("error.type").and_then(serde_json::Value::as_str),
            effect_error_type,
        );
        assert_eq!(capture.span_count("runner.effect_acknowledgement"), 1);
        assert_eq!(capture.span_count("runner.gateway_connection"), 1);
        capture.event("runner.gateway_connection")
    }

    #[tokio::test]
    async fn exits_when_the_gateway_closes_with_policy_violation() {
        let (listener, endpoint) = fixture_listener().await;
        let (headers_sent, headers_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, headers) = accept_fixture_socket_with_headers(&listener).await;
            headers_sent
                .send(headers)
                .expect("send captured upgrade headers");
            expect_opening_hello(&mut socket).await;
            socket
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Policy,
                    reason: "PEER-CLOSE-REASON-MUST-NOT-LEAK".into(),
                })))
                .await
                .expect("send policy close");
            while let Some(Ok(_)) = socket.next().await {}
        });

        let endpoint = format!("{endpoint}?secret=URL-QUERY-MUST-NOT-LEAK");
        let config =
            Config::fixture(&endpoint, test_credential(), true).expect("configure gateway");
        let (sleeper, _sleep_requests) = controlled_sleeper();
        let (recorder, capture) = test_recorder("rbt_00000000000000000000000001");
        let (shutdown, _shutdown_trigger) = controlled_shutdown();
        let error = with_watchdog(run_until_cancelled_with_dependencies(
            config,
            deterministic_frame_source(),
            sleeper,
            recorder,
            healthy_wall_clock(),
            shutdown,
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
        let headers = headers_received
            .await
            .expect("receive captured upgrade headers");

        let events = capture.events();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event["event.name"], "runner.gateway_connection");
        assert_eq!(event["scherzo.connection.attempt"], 1);
        assert_eq!(event["scherzo.connection.failure_kind"], "terminal");
        assert_eq!(event["error.type"], "gateway_policy_violation");
        assert_eq!(event["scherzo.outcome"], "failure");
        assert_eq!(event["scherzo.cloud.text_frames_received"], 0);
        assert_eq!(event["scherzo.runner.text_frames_sent"], 1);
        assert!(event.get("scherzo.connection.backoff_ms").is_none());
        let encoded = serde_json::to_string(event).expect("encode terminal connection event");
        for sentinel in [
            "URL-QUERY-MUST-NOT-LEAK",
            "PEER-CLOSE-REASON-MUST-NOT-LEAK",
            "abcdefghijklmnopqrstuvwxyzABCDEFG-012345678",
        ] {
            assert!(!encoded.contains(sentinel));
        }
        assert_eq!(capture.span_count("runner.gateway_connection"), 1);
        let expected_traceparent = format!(
            "00-{}-{}-01",
            event["trace_id"].as_str().expect("connection trace ID"),
            event["span_id"].as_str().expect("connection span ID"),
        );
        assert_eq!(
            headers
                .get("traceparent")
                .and_then(|value| value.to_str().ok()),
            Some(expected_traceparent.as_str())
        );
        assert!(headers.get("tracestate").is_none());
        assert!(headers.get("baggage").is_none());
    }

    #[tokio::test]
    async fn projects_resolved_build_version_across_initial_and_reconnect_telemetry() {
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
            assert_eq!(
                first_hello["payload"]["runnerVersion"],
                crate::build_info::VERSION
            );
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
            assert_eq!(second_hello["messageId"], "rmsg_00000000000000000000000005");
            assert_ne!(second_hello["messageId"], first_message_id);
            assert_eq!(second_hello["sequence"], 4);
            assert_eq!(second_hello["sentAt"], "2026-07-23T00:00:00Z");
            assert_eq!(
                second_hello["payload"]["runnerVersion"],
                crate::build_info::VERSION
            );
        });

        let (sleeper, mut sleep_requests) = controlled_sleeper();
        let (service, capture, _shutdown_trigger) = spawn_fixture_service(&endpoint, sleeper);
        with_watchdog(failure_received)
            .await
            .expect("first connection did not reach its failure")
            .expect("fixture server dropped failure signal");
        let (backoff_delay, release_sleep) = backoff_request(&mut sleep_requests).await;
        let records = capture.records();
        assert!(records.iter().all(|record| {
            record["service.version"] == crate::build_info::VERSION
                && record
                    .get("scherzo.runner.version")
                    .is_none_or(|version| version == crate::build_info::VERSION)
        }));
        let events = capture.events();
        let connection_events: Vec<_> = events
            .iter()
            .filter(|event| event["event.name"] == "runner.gateway_connection")
            .collect();
        assert_eq!(connection_events.len(), 1);
        let event = connection_events[0];
        assert_eq!(event["scherzo.connection.failure_kind"], "retryable");
        assert_eq!(event["scherzo.runner.version"], crate::build_info::VERSION);
        assert_eq!(event["error.type"], "undecodable_gateway_frame");
        assert_eq!(event["scherzo.outcome"], "disconnected");
        assert_eq!(
            event["scherzo.connection.backoff_ms"],
            i64::try_from(backoff_delay.as_millis()).expect("fixture backoff fits i64")
        );
        assert_eq!(event["scherzo.runner.opening_acknowledged"], true);
        assert_eq!(event["scherzo.runner.handshake_completed"], true);
        assert_eq!(event["scherzo.cloud.text_frames_received"], 3);
        assert_eq!(event["scherzo.runner.text_frames_sent"], 3);
        assert_eq!(event["scherzo.runner.effects_received"], 1);
        assert_eq!(event["scherzo.runner.effect_acknowledgements_confirmed"], 0);
        assert!(
            !serde_json::to_string(event)
                .expect("encode retryable connection event")
                .contains("not valid JSON")
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event.name"] == "runner.effect_acknowledgement")
                .count(),
            1
        );
        assert_eq!(capture.span_count("runner.gateway_connection"), 1);
        release_sleep.release();
        let server_result = with_watchdog(server).await;
        abort_service(service).await;
        server_result
            .expect("runner did not reconnect")
            .expect("fixture server failed");
    }

    #[tokio::test]
    async fn records_a_normal_gateway_close_once_before_backoff() {
        let (listener, endpoint) = fixture_listener().await;
        let server = tokio::spawn(async move {
            let mut socket = accept_fixture_socket(&listener).await;
            expect_opening_hello(&mut socket).await;
            socket
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: "normal fixture close".into(),
                })))
                .await
                .expect("send normal close");
            while let Some(Ok(_)) = socket.next().await {}
        });

        let (sleeper, mut sleep_requests) = controlled_sleeper();
        let (service, capture, _shutdown_trigger) = spawn_fixture_service(&endpoint, sleeper);
        let (_delay, release_backoff) = backoff_request(&mut sleep_requests).await;

        let events = capture.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event.name"], "runner.gateway_connection");
        assert_eq!(events[0]["error.type"], "gateway_closed_connection");
        assert_eq!(events[0]["scherzo.connection.failure_kind"], "retryable");
        assert_eq!(events[0]["scherzo.outcome"], "disconnected");
        assert!(events[0].get("scherzo.connection.backoff_ms").is_some());
        assert_eq!(capture.span_count("runner.gateway_connection"), 1);

        abort_service(service).await;
        drop(release_backoff);
        with_watchdog(server)
            .await
            .expect("fixture server did not close")
            .expect("fixture server failed");
    }

    #[tokio::test]
    async fn records_a_connection_timeout_once_with_backoff() {
        let (listener, endpoint) = fixture_listener().await;
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.expect("accept fixture connection");
            std::future::pending::<()>().await;
        });
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        let (service, capture, _shutdown_trigger) = spawn_fixture_service(&endpoint, sleeper);

        sleep_request(&mut sleep_requests, Duration::from_secs(10))
            .await
            .release();
        let (_delay, release_backoff) = backoff_request(&mut sleep_requests).await;

        let events = capture.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event.name"], "runner.gateway_connection");
        assert_eq!(events[0]["error.type"], "connect_timeout");
        assert_eq!(events[0]["scherzo.connection.failure_kind"], "retryable");
        assert_eq!(events[0]["scherzo.outcome"], "timeout");
        assert!(events[0].get("scherzo.connection.backoff_ms").is_some());
        assert_eq!(capture.span_count("runner.gateway_connection"), 1);

        abort_service(service).await;
        drop(release_backoff);
        server.abort();
        assert!(
            server
                .await
                .expect_err("fixture server should be aborted")
                .is_cancelled()
        );
    }

    #[tokio::test]
    async fn classifies_pending_effect_event_timeout() {
        let (listener, endpoint) = fixture_listener().await;
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        let server = tokio::spawn(async move {
            let mut socket = accept_opened_fixture_socket(&listener).await;

            let welcome_timer = sleep_request(&mut sleep_requests, Duration::from_secs(5)).await;
            socket.send(welcome()).await.expect("send welcome");
            let first_liveness_timer =
                sleep_request(&mut sleep_requests, Duration::from_secs(2)).await;
            drop(welcome_timer);
            socket
                .send(observation_acknowledgement(
                    "rmsg_00000000000000000000000002",
                    1,
                ))
                .await
                .expect("send opening acknowledgement");
            let second_liveness_timer =
                sleep_request(&mut sleep_requests, Duration::from_secs(2)).await;
            drop(first_liveness_timer);
            socket
                .send(assignment_offer())
                .await
                .expect("send assignment offer");
            let _acknowledgement = effect_acknowledgement(&mut socket).await;
            let pending_liveness_timer =
                sleep_request(&mut sleep_requests, Duration::from_secs(2)).await;
            drop(second_liveness_timer);
            pending_liveness_timer.release();

            let close = expect_close_frame(&mut socket).await;
            assert_eq!(close.code, CloseCode::Away);
            backoff_request(&mut sleep_requests).await.1
        });

        let (service, capture, _shutdown_trigger) = spawn_fixture_service(&endpoint, sleeper);
        let release_backoff = with_watchdog(server)
            .await
            .expect("runner did not time out the pending acknowledgement")
            .expect("fixture server failed");

        let connection =
            assert_attempt_event_pair(&capture, "timeout", Some("gateway_liveness_timeout"));
        assert_eq!(connection["scherzo.outcome"], "timeout");
        assert_eq!(connection["error.type"], "gateway_liveness_timeout");

        abort_service(service).await;
        drop(release_backoff);
    }

    #[tokio::test]
    async fn shutdown_reports_and_waits_for_an_accepted_assignment_interruption() {
        let (listener, endpoint) = fixture_listener().await;
        let (accepted_sent, accepted_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, accepted, interrupted) =
                accept_assignment_interruption(&listener, accepted_sent).await;
            socket
                .send(observation_acknowledgement(
                    accepted["messageId"].as_str().unwrap(),
                    accepted["sequence"].as_u64().unwrap(),
                ))
                .await
                .expect("acknowledge assignment acceptance");
            socket
                .send(terminal_observation_acknowledgement(
                    interrupted["messageId"].as_str().unwrap(),
                    interrupted["sequence"].as_u64().unwrap(),
                ))
                .await
                .expect("acknowledge shutdown interruption");
            while let Some(Ok(_)) = socket.next().await {}
        });

        let service = begin_accepted_assignment_shutdown(&endpoint, accepted_received).await;

        with_watchdog(service.task)
            .await
            .expect("runner ignored shutdown acknowledgement")
            .expect("runner service task failed")
            .expect("runner service returned an error");
        with_watchdog(server)
            .await
            .expect("gateway fixture did not observe service exit")
            .expect("gateway fixture failed");
    }

    #[tokio::test]
    async fn second_shutdown_signal_forces_exit_without_another_observation() {
        let (listener, endpoint) = fixture_listener().await;
        let (accepted_sent, accepted_received) = tokio::sync::oneshot::channel();
        let (interrupted_sent, interrupted_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _accepted, _interrupted) =
                accept_assignment_interruption(&listener, accepted_sent).await;
            interrupted_sent
                .send(())
                .expect("report shutdown interruption");

            while let Some(frame) = socket.next().await {
                match frame {
                    Ok(Message::Text(_)) => {
                        panic!("repeated shutdown signal produced another observation");
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });

        let service = begin_accepted_assignment_shutdown(&endpoint, accepted_received).await;
        with_watchdog(interrupted_received)
            .await
            .expect("runner did not begin graceful shutdown")
            .expect("fixture dropped interruption signal");
        service.shutdown_trigger.notify_one();

        let error = with_watchdog(service.task)
            .await
            .expect("runner ignored repeated shutdown signal")
            .expect("runner service task failed")
            .expect_err("runner treated a repeated shutdown signal as successful exit");
        assert!(matches!(error, ServiceError::ShutdownForced));
        with_watchdog(server)
            .await
            .expect("gateway fixture did not observe forced exit")
            .expect("gateway fixture failed");
    }

    #[tokio::test]
    async fn classifies_pending_effect_event_cancellation() {
        let (listener, endpoint) = fixture_listener().await;
        let (pending_sent, pending_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut socket = accept_opened_fixture_socket(&listener).await;
            let _acknowledgement =
                offer_assignment_after_handshake(&mut socket, "rmsg_00000000000000000000000002", 1)
                    .await;
            pending_sent
                .send(())
                .expect("report pending effect acknowledgement");
            while let Some(Ok(_)) = socket.next().await {}
        });

        let (sleeper, _sleep_requests) = controlled_sleeper();
        let (service, capture, shutdown_trigger) = spawn_fixture_service(&endpoint, sleeper);
        with_watchdog(pending_received)
            .await
            .expect("effect acknowledgement did not become pending")
            .expect("fixture server dropped pending signal");

        shutdown_trigger.notify_one();
        with_watchdog(service)
            .await
            .expect("runner service ignored termination")
            .expect("runner service task failed")
            .expect("runner service returned an error");
        with_watchdog(server)
            .await
            .expect("fixture server did not observe shutdown")
            .expect("fixture server failed");

        let connection = assert_attempt_event_pair(&capture, "cancelled", None);
        assert_eq!(connection["scherzo.outcome"], "cancelled");
        assert_eq!(connection["scherzo.runner.opening_acknowledged"], true);
        assert_eq!(connection["scherzo.runner.handshake_completed"], true);
        assert!(connection.get("error.type").is_none());
        assert!(connection.get("scherzo.connection.failure_kind").is_none());
        assert!(connection.get("scherzo.connection.backoff_ms").is_none());
        assert_eq!(capture.span_count("runner.effect_acknowledgement"), 1);
        assert_eq!(capture.span_count("runner.gateway_connection"), 1);
    }
}
