mod artifact_delivery;
mod assignment;
mod backoff;
mod config;
mod connection;
mod control;
#[cfg(test)]
mod conversation;
#[cfg(test)]
mod determinism_spike;
mod execution;
mod lease_clock;
mod source;
#[cfg(test)]
mod test_support;
mod workspace;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use opentelemetry::KeyValue;

#[cfg(test)]
pub(crate) use config::AssignmentConfig;
pub(crate) use config::Config;

use crate::execution::workflow::cancellation::MAXIMUM_CANCELLATION_GRACE;
use crate::runner::control_protocol::{ConnectionFailure, ControlError};
use crate::runner::telemetry::{self, Event, Outcome, Recorder};
use assignment::AssignmentManager;
use backoff::Backoff;
use connection::{
    ActiveEffectEvent, ConnectionCause, ConnectionDependencies, ConnectionError,
    ConnectionProgress, FailureKind, FrameSource, OpeningHello, SystemFrameSource, opening_hello,
    record_progress,
};
use control::{ControlServer, ControlServerError, LiveStatus, ReloadRequest};
use lease_clock::{LeaseClock, LeaseClockError};

type SleepFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
type ConnectionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ConnectionProgress, ConnectionError>> + Send + 'a>>;
type ShutdownFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

struct Sequence {
    next: Mutex<u64>,
    emission: tokio::sync::Mutex<()>,
}

impl Sequence {
    const fn new(next: u64) -> Self {
        Self {
            next: Mutex::new(next),
            emission: tokio::sync::Mutex::const_new(()),
        }
    }

    fn next(&self) -> Result<u64, ConnectionError> {
        let mut next = self
            .next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let value = *next;
        *next = next.checked_add(1).ok_or_else(sequence_overflow)?;
        Ok(value)
    }

    fn peek(&self) -> u64 {
        *self
            .next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    async fn lock_emission(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.emission.lock().await
    }
}

const SHUTDOWN_DELIVERY_RESERVE: std::time::Duration = std::time::Duration::from_secs(10);
const SHUTDOWN_CLEANUP_RESERVE: std::time::Duration = std::time::Duration::from_secs(5);
const SHUTDOWN_CLEANUP_START_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(
    MAXIMUM_CANCELLATION_GRACE.as_secs() + SHUTDOWN_DELIVERY_RESERVE.as_secs(),
);
const SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(
    SHUTDOWN_CLEANUP_START_TIMEOUT.as_secs() + SHUTDOWN_CLEANUP_RESERVE.as_secs(),
);

pub(crate) trait Sleeper: Send + Sync {
    #[allow(
        dead_code,
        reason = "deterministic transport tests inspect their logical sleep clock"
    )]
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
    next_sequence: &'a Sequence,
}

struct PromotedAttempt {
    config: Config,
    opening: Vec<u8>,
    opening_message_id: String,
    opening_sequence: u64,
    attempt: u64,
    connection: connection::CandidateConnection,
    connection_event: Event,
    active_effect_event: ActiveEffectEvent,
}

enum ActiveAttemptResult {
    Finished(Result<ConnectionProgress, ConnectionError>),
    Promoted(Box<PromotedAttempt>),
}

enum ActiveEvent {
    ManualReload(ReloadRequest),
    StartupReload,
    AssignmentNotification,
    Shutdown,
    Finished(Result<ConnectionProgress, ConnectionError>),
}

enum ConnectedReloadResult {
    Continue,
    Promoted(Box<PromotedAttempt>),
    Shutdown,
    Finished(Result<ConnectionProgress, ConnectionError>),
}

struct PreparedReload {
    state_access: crate::runner::enrollment::RunnerStateAccess,
    expected_runner_id: String,
    expected_current_credential_id: String,
    candidate: PromotedAttempt,
}

struct ReloadDependencies {
    boot_id: String,
    frame_source: Arc<dyn FrameSource>,
    sleeper: Arc<dyn Sleeper>,
    recorder: Arc<Recorder>,
    assignment_manager: Arc<Mutex<AssignmentManager>>,
    live_status: Arc<LiveStatus>,
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
    lease_clock: LeaseClock,
    boot_id: String,
}

impl ConnectionLoopDependencies {
    fn new(
        config: Config,
        frame_source: Arc<dyn FrameSource>,
        sleeper: Arc<dyn Sleeper>,
        recorder: Arc<Recorder>,
        lease_clock: LeaseClock,
        boot_id: String,
    ) -> Self {
        Self {
            config,
            frame_source,
            sleeper,
            recorder,
            lease_clock,
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
    Control(ControlServerError),
    LeaseClock(LeaseClockError),
    WorkRootInUse,
    WorkRootIsolation,
    WorkspaceCleanupFailed,
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
            Self::Control(error) => write!(formatter, "start runner local control: {error}"),
            Self::LeaseClock(error) => write!(formatter, "runner lease clock failed: {error}"),
            Self::WorkRootInUse => formatter.write_str("runner work root is already in use"),
            Self::WorkRootIsolation => {
                formatter.write_str("runner work-root isolation could not be established")
            }
            Self::WorkspaceCleanupFailed => formatter.write_str("runner workspace cleanup failed"),
        }
    }
}

impl std::error::Error for ServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BuildRuntime
            | Self::AssignmentShutdown
            | Self::ShutdownForced
            | Self::ShutdownDeadlineExceeded
            | Self::WorkRootInUse
            | Self::WorkRootIsolation
            | Self::WorkspaceCleanupFailed => None,
            Self::Connection(error) => Some(error),
            Self::Control(error) => Some(error),
            Self::LeaseClock(error) => Some(error),
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
    let lease_clock = LeaseClock::system().map_err(ServiceError::LeaseClock)?;
    let mut shutdown = ProcessShutdown::new()?;
    run_service_loop(
        config,
        frame_source,
        sleeper,
        recorder,
        lease_clock,
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
    mut shutdown: Box<dyn Shutdown>,
) -> Result<(), ServiceError> {
    let boot_id = frame_source.public_id("rbt_");
    let lease_clock = LeaseClock::system().map_err(ServiceError::LeaseClock)?;
    run_service_loop(
        config,
        frame_source,
        sleeper,
        recorder,
        lease_clock,
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
    lease_clock: LeaseClock,
    boot_id: String,
    shutdown: &mut dyn Shutdown,
) -> Result<(), ServiceError> {
    run_connection_loop(
        ConnectionLoopDependencies::new(
            config,
            frame_source,
            sleeper,
            recorder,
            lease_clock,
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
    backoff: Backoff,
    shutdown: &mut dyn Shutdown,
) -> Result<(), ServiceError> {
    let config_lifetime = dependencies.config.clone();
    let work_root = workspace::WorkRootLease::acquire(
        config_lifetime.assignment().work_root(),
        &dependencies.boot_id,
    )
    .map_err(work_root_service_error)?;
    let result = run_connection_loop_with_work_root(
        dependencies,
        connector,
        backoff,
        shutdown,
        Arc::clone(&work_root),
    )
    .await;
    if result.is_err() {
        work_root.cancel_cleanup();
    }
    result
}

async fn run_connection_loop_with_work_root(
    dependencies: ConnectionLoopDependencies,
    connector: &dyn Connector,
    mut backoff: Backoff,
    shutdown: &mut dyn Shutdown,
    work_root: Arc<workspace::WorkRootLease>,
) -> Result<(), ServiceError> {
    let ConnectionLoopDependencies {
        mut config,
        frame_source,
        sleeper,
        recorder,
        lease_clock,
        boot_id,
    } = dependencies;
    let assignment_manager = Arc::new(Mutex::new(
        AssignmentManager::new_with_sleeper_and_work_root(
            &config,
            boot_id.clone(),
            lease_clock,
            Arc::clone(&sleeper),
            Arc::clone(&work_root),
            Arc::clone(&recorder),
        ),
    ));
    let live_status = Arc::new(LiveStatus::new(
        boot_id.clone(),
        config.credential().credential_id().to_owned(),
        config
            .startup_pending()
            .map(|pending| pending.credential_id.clone()),
    ));
    let reload_dependencies = ReloadDependencies {
        boot_id: boot_id.clone(),
        frame_source: Arc::clone(&frame_source),
        sleeper: Arc::clone(&sleeper),
        recorder: Arc::clone(&recorder),
        assignment_manager: Arc::clone(&assignment_manager),
        live_status: Arc::clone(&live_status),
    };
    let (reload_sender, mut reload_requests) = tokio::sync::mpsc::channel::<ReloadRequest>(1);
    let _control_server = config
        .control_socket_path()
        .map(|path| {
            ControlServer::bind(
                path,
                Arc::clone(&live_status),
                Arc::clone(&assignment_manager),
                reload_sender,
            )
        })
        .transpose()
        .map_err(ServiceError::Control)?;
    let mut opening_sequence = 1;
    let sequence = Sequence::new(2);
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
    let mut attempt = 1_u64;
    let mut startup_reload_pending = config.startup_pending().is_some();
    let mut promoted_attempt: Option<PromotedAttempt> = None;
    let mut shutting_down = false;
    let mut shutdown_deadline: Option<Pin<Box<dyn Future<Output = ()> + Send>>> = None;

    loop {
        if assignment_manager
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cleanup_failure_ready_to_exit()
        {
            return Err(ServiceError::WorkspaceCleanupFailed);
        }
        if assignment_manager
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lease_clock_failure_ready_to_exit()
        {
            return Err(ServiceError::LeaseClock(LeaseClockError::ClockUnavailable));
        }
        if shutting_down
            && assignment_manager
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .shutdown_complete()
        {
            let Some(deadline) = shutdown_deadline.as_mut() else {
                return Err(ServiceError::AssignmentShutdown);
            };
            return finish_shutdown_cleanup(&work_root, shutdown, deadline).await;
        }
        if shutting_down
            && assignment_manager
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .shutdown_waiting_only_for_cleanup()
        {
            let Some(deadline) = shutdown_deadline.as_mut() else {
                return Err(ServiceError::AssignmentShutdown);
            };
            wait_for_shutdown_progress(&assignment_manager, &work_root, shutdown, deadline, None)
                .await?;
            continue;
        }
        live_status.connecting();
        let promoted = promoted_attempt.take();
        let (connection_event, active_effect_event, candidate) = match promoted {
            Some(promoted) => (
                promoted.connection_event,
                promoted.active_effect_event,
                Some(promoted.connection),
            ),
            None => (
                connection_event(&recorder, &config, &boot_id, attempt),
                ActiveEffectEvent::new(),
                None,
            ),
        };
        let result = {
            let dependencies = ConnectionDependencies::new(
                &config,
                frame_source.as_ref(),
                sleeper.as_ref(),
                &recorder,
                &connection_event,
                &active_effect_event,
                &assignment_manager,
                attempt,
            )
            .with_live_status(&live_status);
            let opening_frame = OpeningHello {
                boot_id: &boot_id,
                encoded: &opening,
                message_id: &opening_message_id,
                sequence: opening_sequence,
            };
            let connection: ConnectionFuture<'_> = match candidate {
                Some(candidate) => Box::pin(connection::run_promoted(
                    dependencies,
                    opening_frame,
                    &sequence,
                    candidate,
                )),
                None => connector.connect(ConnectionAttempt {
                    dependencies,
                    opening: opening_frame,
                    next_sequence: &sequence,
                }),
            };
            tokio::pin!(connection);
            loop {
                if assignment_manager
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .cleanup_failure_ready_to_exit()
                {
                    cancel_attempt(&connection_event, &active_effect_event);
                    return Err(ServiceError::WorkspaceCleanupFailed);
                }
                if shutting_down
                    && assignment_manager
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .shutdown_complete()
                {
                    cancel_attempt(&connection_event, &active_effect_event);
                    let Some(deadline) = shutdown_deadline.as_mut() else {
                        return Err(ServiceError::AssignmentShutdown);
                    };
                    return finish_shutdown_cleanup(&work_root, shutdown, deadline).await;
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
                        result = &mut connection => {
                            break ActiveAttemptResult::Finished(result);
                        }
                        () = &mut notified => continue,
                        () = deadline => {
                            cancel_attempt(&connection_event, &active_effect_event);
                            return Err(ServiceError::ShutdownDeadlineExceeded);
                        }
                    }
                } else {
                    let event = tokio::select! {
                        biased;
                        Some(request) = reload_requests.recv() => {
                            ActiveEvent::ManualReload(request)
                        }
                        () = live_status.wait_until_connected(), if startup_reload_pending => {
                            ActiveEvent::StartupReload
                        }
                        _ = shutdown.wait() => ActiveEvent::Shutdown,
                        () = &mut notified => ActiveEvent::AssignmentNotification,
                        result = &mut connection => ActiveEvent::Finished(result),
                    };
                    match event {
                        event @ (ActiveEvent::ManualReload(_) | ActiveEvent::StartupReload) => {
                            let (request, startup) = match event {
                                ActiveEvent::ManualReload(request) => (Some(request), false),
                                ActiveEvent::StartupReload => (None, true),
                                ActiveEvent::AssignmentNotification
                                | ActiveEvent::Shutdown
                                | ActiveEvent::Finished(_) => unreachable!(),
                            };
                            if startup {
                                startup_reload_pending = false;
                            }
                            match reload_dependencies
                                .perform_while_connected(
                                    &config,
                                    &sequence,
                                    attempt.saturating_add(1),
                                    request,
                                    &mut connection,
                                    shutdown,
                                )
                                .await
                            {
                                ConnectedReloadResult::Continue => {}
                                ConnectedReloadResult::Promoted(promoted) => {
                                    cancel_attempt(&connection_event, &active_effect_event);
                                    startup_reload_pending = false;
                                    break ActiveAttemptResult::Promoted(promoted);
                                }
                                ConnectedReloadResult::Shutdown => {
                                    shutdown_deadline = Some(begin_shutdown(
                                        &live_status,
                                        &assignment_manager,
                                        &sleeper,
                                    )?);
                                    shutting_down = true;
                                }
                                ConnectedReloadResult::Finished(result) => {
                                    break ActiveAttemptResult::Finished(result);
                                }
                            }
                        }
                        ActiveEvent::AssignmentNotification => continue,
                        ActiveEvent::Shutdown => {
                            shutdown_deadline =
                                Some(begin_shutdown(&live_status, &assignment_manager, &sleeper)?);
                            shutting_down = true;
                        }
                        ActiveEvent::Finished(result) => {
                            break ActiveAttemptResult::Finished(result);
                        }
                    }
                }
            }
        };
        let result = match result {
            ActiveAttemptResult::Finished(result) => result,
            ActiveAttemptResult::Promoted(promoted) => {
                install_promoted_attempt(
                    *promoted,
                    &mut config,
                    &mut opening,
                    &mut opening_message_id,
                    &mut opening_sequence,
                    &mut attempt,
                    &mut promoted_attempt,
                );
                continue;
            }
        };
        let (progress, cause, kind) = match result {
            Ok(progress) => (
                progress,
                ConnectionCause::GatewayClosedConnection,
                FailureKind::Retryable,
            ),
            Err(error) if error.is_terminal() => {
                if error.kind() == FailureKind::TerminalAuthentication && startup_reload_pending {
                    startup_reload_pending = false;
                    if let Some(promoted) = reload_dependencies
                        .perform(&config, &sequence, attempt.saturating_add(1), None)
                        .await
                    {
                        finish_connection_event(
                            &connection_event,
                            error.progress,
                            error.kind(),
                            error.connection_cause(),
                            None,
                            Outcome::Failure,
                        );
                        install_promoted_attempt(
                            promoted,
                            &mut config,
                            &mut opening,
                            &mut opening_message_id,
                            &mut opening_sequence,
                            &mut attempt,
                            &mut promoted_attempt,
                        );
                        continue;
                    }
                }
                match error.kind() {
                    FailureKind::TerminalAuthentication => live_status.authentication_failed(),
                    FailureKind::TerminalProtocol => live_status.protocol_failed(),
                    FailureKind::Retryable => {}
                }
                finish_connection_event(
                    &connection_event,
                    error.progress,
                    error.kind(),
                    error.connection_cause(),
                    None,
                    Outcome::Failure,
                );
                if error.kind() != FailureKind::TerminalAuthentication {
                    return Err(ServiceError::Connection(error));
                }
                loop {
                    tokio::select! {
                        Some(request) = reload_requests.recv() => {
                            if let Some(promoted) = reload_dependencies
                                .perform(
                                    &config,
                                    &sequence,
                                    attempt.saturating_add(1),
                                    Some(request),
                                )
                                .await
                            {
                                install_promoted_attempt(
                                    promoted,
                                    &mut config,
                                    &mut opening,
                                    &mut opening_message_id,
                                    &mut opening_sequence,
                                    &mut attempt,
                                    &mut promoted_attempt,
                                );
                                break;
                            }
                        }
                        _ = shutdown.wait() => {
                            shutdown_deadline = Some(begin_shutdown(
                                &live_status,
                                &assignment_manager,
                                &sleeper,
                            )?);
                            shutting_down = true;
                            break;
                        }
                    }
                }
                continue;
            }
            Err(error) => (error.progress, error.connection_cause(), error.kind()),
        };
        if progress.handshake_completed {
            backoff.reset();
        }
        live_status.backing_off(connection_failure(cause));
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
            opening_sequence = sequence.next().map_err(ServiceError::Connection)?;
            opening = opening_hello(
                frame_source.as_ref(),
                config.credential().runner_id(),
                &boot_id,
                opening_message_id.clone(),
                opening_sequence,
                crate::build_info::VERSION,
            )
            .map_err(ServiceError::Connection)?;
        }
        attempt = attempt.saturating_add(1);
        if shutting_down {
            let Some(deadline) = shutdown_deadline.as_mut() else {
                return Err(ServiceError::AssignmentShutdown);
            };
            wait_for_shutdown_progress(
                &assignment_manager,
                &work_root,
                shutdown,
                deadline,
                Some(sleeper.sleep(delay)),
            )
            .await?;
        } else {
            tokio::select! {
                Some(request) = reload_requests.recv() => {
                    if let Some(promoted) = reload_dependencies
                        .perform(
                            &config,
                            &sequence,
                            attempt.saturating_add(1),
                            Some(request),
                        )
                        .await
                    {
                        // Both connected and backoff reload entry points install the
                        // same prepared transport through the shared state transition.
                        // jscpd:ignore-start
                        install_promoted_attempt(
                            promoted,
                            &mut config,
                            &mut opening,
                            &mut opening_message_id,
                            &mut opening_sequence,
                            &mut attempt,
                            &mut promoted_attempt,
                        );
                        // jscpd:ignore-end
                        continue;
                    }
                }
                _ = shutdown.wait() => {
                    shutdown_deadline = Some(begin_shutdown(
                        &live_status,
                        &assignment_manager,
                        &sleeper,
                    )?);
                    shutting_down = true;
                }
                _ = sleeper.sleep(delay) => {}
            }
        }
    }
}

async fn wait_for_shutdown_progress(
    assignment_manager: &Mutex<AssignmentManager>,
    work_root: &workspace::WorkRootLease,
    shutdown: &mut dyn Shutdown,
    deadline: &mut Pin<Box<dyn Future<Output = ()> + Send>>,
    delay: Option<SleepFuture<'_>>,
) -> Result<(), ServiceError> {
    let notification = assignment_manager
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .notification();
    let optional_delay = async move {
        match delay {
            Some(delay) => delay.await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(optional_delay);
    tokio::select! {
        biased;
        () = shutdown.wait() => {
            work_root.cancel_cleanup();
            Err(ServiceError::ShutdownForced)
        }
        () = deadline.as_mut() => {
            work_root.cancel_cleanup();
            Err(ServiceError::ShutdownDeadlineExceeded)
        }
        () = notification.notified() => Ok(()),
        () = &mut optional_delay => Ok(()),
    }
}

async fn finish_shutdown_cleanup(
    work_root: &workspace::WorkRootLease,
    shutdown: &mut dyn Shutdown,
    deadline: &mut Pin<Box<dyn Future<Output = ()> + Send>>,
) -> Result<(), ServiceError> {
    let pending = work_root.release_boot_root_pending();
    let completion = pending.wait_async();
    tokio::pin!(completion);
    tokio::select! {
        biased;
        () = shutdown.wait() => {
            work_root.cancel_cleanup();
            Err(ServiceError::ShutdownForced)
        }
        result = &mut completion => match result {
            workspace::CleanupResult::Released => Ok(()),
            workspace::CleanupResult::Quarantined(_) | workspace::CleanupResult::Preempted => {
                Err(ServiceError::WorkspaceCleanupFailed)
            }
        },
        () = deadline.as_mut() => {
            work_root.cancel_cleanup();
            Err(ServiceError::ShutdownDeadlineExceeded)
        }
    }
}

fn begin_shutdown(
    live_status: &LiveStatus,
    assignment_manager: &Mutex<AssignmentManager>,
    sleeper: &Arc<dyn Sleeper>,
) -> Result<Pin<Box<dyn Future<Output = ()> + Send>>, ServiceError> {
    live_status.stopping();
    assignment_manager
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .begin_shutdown()
        .map_err(|_| ServiceError::AssignmentShutdown)?;
    let deadline_sleeper = Arc::clone(sleeper);
    Ok(Box::pin(async move {
        deadline_sleeper.sleep(SHUTDOWN_TIMEOUT).await;
    }))
}

fn install_promoted_attempt(
    promoted: PromotedAttempt,
    config: &mut Config,
    opening: &mut Vec<u8>,
    opening_message_id: &mut String,
    opening_sequence: &mut u64,
    attempt: &mut u64,
    promoted_attempt: &mut Option<PromotedAttempt>,
) {
    *config = promoted.config.clone();
    opening.clone_from(&promoted.opening);
    opening_message_id.clone_from(&promoted.opening_message_id);
    *opening_sequence = promoted.opening_sequence;
    *attempt = promoted.attempt;
    *promoted_attempt = Some(promoted);
}

impl ReloadDependencies {
    async fn perform(
        &self,
        config: &Config,
        sequence: &Sequence,
        attempt: u64,
        mut request: Option<ReloadRequest>,
    ) -> Option<PromotedAttempt> {
        let result = self.reload_candidate(config, sequence, attempt).await;
        respond_to_reload(&mut request, &result);
        result.ok()
    }

    async fn perform_while_connected<F>(
        &self,
        config: &Config,
        sequence: &Sequence,
        attempt: u64,
        request: Option<ReloadRequest>,
        connection: &mut F,
        shutdown: &mut dyn Shutdown,
    ) -> ConnectedReloadResult
    where
        F: Future<Output = Result<ConnectionProgress, ConnectionError>> + Unpin,
    {
        let mut request = request;
        if !self.live_status.is_connected() {
            tokio::select! {
                biased;
                () = self.live_status.wait_until_connected() => {}
                result = &mut *connection => {
                    let failure = Err(ControlError::PendingConnectionFailed);
                    respond_to_reload(&mut request, &failure);
                    return ConnectedReloadResult::Finished(result);
                }
                _ = shutdown.wait() => {
                    let failure = Err(ControlError::PendingConnectionFailed);
                    respond_to_reload(&mut request, &failure);
                    return ConnectedReloadResult::Shutdown;
                }
            }
        }
        let preparation = self.prepare_candidate(config, sequence, attempt);
        tokio::pin!(preparation);
        let prepared = tokio::select! {
            biased;
            result = &mut preparation => result,
            result = &mut *connection => {
                let failure = Err(ControlError::PendingConnectionFailed);
                respond_to_reload(&mut request, &failure);
                return ConnectedReloadResult::Finished(result);
            }
            _ = shutdown.wait() => {
                let failure = Err(ControlError::PendingConnectionFailed);
                respond_to_reload(&mut request, &failure);
                return ConnectedReloadResult::Shutdown;
            }
        };
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                let failure = Err(error);
                respond_to_reload(&mut request, &failure);
                return ConnectedReloadResult::Continue;
            }
        };
        match self.commit_candidate(prepared).await {
            Ok(promoted) => {
                let success = Ok(promoted);
                respond_to_reload(&mut request, &success);
                let Ok(promoted) = success else {
                    unreachable!("successful reload result changed before installation")
                };
                ConnectedReloadResult::Promoted(Box::new(promoted))
            }
            Err(error) => {
                let recovery = self
                    .prepare_connection(config.clone(), sequence, attempt.saturating_add(1))
                    .await;
                let failure = Err(error);
                respond_to_reload(&mut request, &failure);
                match recovery {
                    Ok(recovered) => ConnectedReloadResult::Promoted(Box::new(recovered)),
                    Err(_) => ConnectedReloadResult::Continue,
                }
            }
        }
    }

    async fn reload_candidate(
        &self,
        config: &Config,
        sequence: &Sequence,
        attempt: u64,
    ) -> Result<PromotedAttempt, ControlError> {
        let prepared = self.prepare_candidate(config, sequence, attempt).await?;
        self.commit_candidate(prepared).await
    }

    async fn prepare_candidate(
        &self,
        config: &Config,
        sequence: &Sequence,
        attempt: u64,
    ) -> Result<PreparedReload, ControlError> {
        let state_access = config
            .state_access()
            .cloned()
            .ok_or(ControlError::NoPendingCredential)?;
        let expected_runner_id = config.credential().runner_id().to_owned();
        let load_access = state_access.clone();
        let load_runner_id = expected_runner_id.clone();
        let pending =
            tokio::task::spawn_blocking(move || load_access.load_pending(&load_runner_id))
                .await
                .map_err(|_| ControlError::StateUpdateFailed)?
                .map_err(control_state_error)?;
        let Some(pending) = pending else {
            self.live_status.pending(None);
            return Err(ControlError::NoPendingCredential);
        };
        if pending.runner_id != expected_runner_id {
            return Err(ControlError::PendingRegistrationMismatch);
        }
        self.live_status
            .pending(Some(pending.credential_id.clone()));
        let pending_config = config
            .with_pending_credential(pending)
            .map_err(|_| ControlError::StateUpdateFailed)?;
        let candidate = self
            .prepare_connection(pending_config, sequence, attempt)
            .await?;
        Ok(PreparedReload {
            state_access,
            expected_runner_id,
            expected_current_credential_id: config.credential().credential_id().to_owned(),
            candidate,
        })
    }

    async fn prepare_connection(
        &self,
        config: Config,
        sequence: &Sequence,
        attempt: u64,
    ) -> Result<PromotedAttempt, ControlError> {
        let connection_event = connection_event(&self.recorder, &config, &self.boot_id, attempt);
        let active_effect_event = ActiveEffectEvent::new();
        let dependencies = ConnectionDependencies::new(
            &config,
            self.frame_source.as_ref(),
            self.sleeper.as_ref(),
            &self.recorder,
            &connection_event,
            &active_effect_event,
            &self.assignment_manager,
            attempt,
        );
        let transport = connection::connect_candidate_transport(dependencies)
            .await
            .map_err(|error| candidate_control_error(&connection_event, error))?;
        let emission = sequence.lock_emission().await;
        let opening_sequence = sequence
            .next()
            .map_err(|error| candidate_control_error(&connection_event, error))?;
        let opening_message_id = self.frame_source.public_id("rmsg_");
        let opening = opening_hello(
            self.frame_source.as_ref(),
            config.credential().runner_id(),
            &self.boot_id,
            opening_message_id.clone(),
            opening_sequence,
            crate::build_info::VERSION,
        )
        .map_err(|error| candidate_control_error(&connection_event, error))?;
        let candidate = connection::authenticate_candidate(
            dependencies,
            transport,
            OpeningHello {
                boot_id: &self.boot_id,
                encoded: &opening,
                message_id: &opening_message_id,
                sequence: opening_sequence,
            },
        )
        .await
        .map_err(|error| candidate_control_error(&connection_event, error))?;
        drop(emission);
        Ok(PromotedAttempt {
            config,
            opening,
            opening_message_id,
            opening_sequence,
            attempt,
            connection: candidate,
            connection_event,
            active_effect_event,
        })
    }

    async fn commit_candidate(
        &self,
        prepared: PreparedReload,
    ) -> Result<PromotedAttempt, ControlError> {
        let state_access = prepared.state_access.clone();
        let expected_runner_id = prepared.expected_runner_id.clone();
        let expected_current_credential_id = prepared.expected_current_credential_id.clone();
        let expected_pending_credential_id = prepared
            .candidate
            .config
            .credential()
            .credential_id()
            .to_owned();
        let promotion = tokio::task::spawn_blocking(move || {
            state_access.promote(
                &expected_runner_id,
                &expected_current_credential_id,
                &expected_pending_credential_id,
            )
        })
        .await
        .map_err(|_| ControlError::StateUpdateFailed)
        .and_then(|result| result.map_err(control_state_error));
        if let Err(error) = promotion {
            prepared.candidate.connection_event.finish(Outcome::Failure);
            return Err(error);
        }
        self.live_status.promoted(
            prepared
                .candidate
                .config
                .credential()
                .credential_id()
                .to_owned(),
        );
        Ok(prepared.candidate)
    }
}

fn candidate_control_error(connection_event: &Event, error: ConnectionError) -> ControlError {
    let category = match error.kind() {
        FailureKind::TerminalAuthentication => ControlError::PendingAuthenticationFailed,
        FailureKind::TerminalProtocol => ControlError::PendingProtocolFailed,
        FailureKind::Retryable => ControlError::PendingConnectionFailed,
    };
    finish_connection_event(
        connection_event,
        error.progress,
        error.kind(),
        error.connection_cause(),
        None,
        if error.connection_cause().is_timeout() {
            Outcome::Timeout
        } else {
            Outcome::Failure
        },
    );
    category
}

fn respond_to_reload(
    request: &mut Option<ReloadRequest>,
    result: &Result<PromotedAttempt, ControlError>,
) {
    if let Some(request) = request.take() {
        let response = result
            .as_ref()
            .map(|promoted| promoted.config.credential().credential_id().to_owned())
            .map_err(|error| *error);
        let _ = request.response.send(response);
    }
}

const fn control_state_error(error: crate::runner::enrollment::ReloadStateError) -> ControlError {
    match error {
        crate::runner::enrollment::ReloadStateError::RegistrationMismatch => {
            ControlError::PendingRegistrationMismatch
        }
        crate::runner::enrollment::ReloadStateError::StateUpdate => ControlError::StateUpdateFailed,
    }
}

const fn connection_failure(cause: ConnectionCause) -> ConnectionFailure {
    match cause {
        ConnectionCause::CredentialRejected => ConnectionFailure::Authentication,
        ConnectionCause::GatewayRateLimited => ConnectionFailure::RateLimited,
        ConnectionCause::GatewayUnavailable | ConnectionCause::GatewayHttpError => {
            ConnectionFailure::CloudUnavailable
        }
        ConnectionCause::GatewayPolicyViolation
        | ConnectionCause::GatewayUnsupportedFrames
        | ConnectionCause::GatewayOversizedFrames
        | ConnectionCause::RequiredSubprotocolNotSelected
        | ConnectionCause::OversizedGatewayFrame
        | ConnectionCause::UndecodableGatewayFrame
        | ConnectionCause::UnexpectedObservationAcknowledgement
        | ConnectionCause::MismatchedEffectAcknowledgement
        | ConnectionCause::UnexpectedGatewayFrame
        | ConnectionCause::BinaryGatewayFrame
        | ConnectionCause::UnexpectedRawGatewayFrame
        | ConnectionCause::InvalidExecutionLeasePolicy
        | ConnectionCause::ChangedExecutionLeasePolicy
        | ConnectionCause::ConflictingAssignmentOffer
        | ConnectionCause::RunnerLeaseClockFailure => ConnectionFailure::Protocol,
        _ => ConnectionFailure::Network,
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

const fn work_root_service_error(error: workspace::WorkRootError) -> ServiceError {
    match error {
        workspace::WorkRootError::WorkRootInUse => ServiceError::WorkRootInUse,
        workspace::WorkRootError::UnsafeWorkRoot
        | workspace::WorkRootError::AmbiguousOwnedRoot
        | workspace::WorkRootError::StaleRootCleanupFailed
        | workspace::WorkRootError::CreateBootRoot => ServiceError::WorkRootIsolation,
    }
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
    use std::io;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use base64::Engine as _;
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::handshake::server::{
        Request as HandshakeRequest, Response as HandshakeResponse,
    };
    use tokio_tungstenite::tungstenite::http::{HeaderValue, header};

    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    use super::sequence_overflow;
    use super::test_support::{
        ControlledShutdownTrigger, FixtureSocket, SleepRelease, accept_fixture_socket,
        accept_fixture_socket_with_headers, accept_opened_fixture_socket, assignment_offer,
        controlled_shutdown, controlled_sleeper, deterministic_frame_source,
        effect_acknowledgement, effect_observation_acknowledgement, expect_close_frame,
        expect_opening_hello, fixture_lease_clock, fixture_listener, observation_acknowledgement,
        offer_assignment_after_handshake, scripted_connector, sleep_request,
        terminal_observation_acknowledgement, welcome, with_watchdog,
    };
    use super::workspace::{
        CleanupCancellation, CleanupSleeper, TreeRemover, WorkRootHook, WorkRootLease,
        WorkspaceFilesystem,
    };
    use super::{
        AssignmentConfig, Config, ConnectionLoopDependencies, LiveStatus, ReloadDependencies,
        ReloadRequest, SHUTDOWN_TIMEOUT, Sequence, ServiceError, Sleeper, TokioSleeper,
        run_connection_loop_with_work_root, run_until_cancelled_with_dependencies,
    };
    use crate::runner::control_protocol::{ConnectionState, ControlError, Operation, Response};
    use crate::runner::credential::test_credential;
    use crate::runner::service::assignment::AssignmentManager;
    use crate::runner::telemetry::{TestCapture, test_recorder};

    #[test]
    fn shutdown_timeout_accommodates_maximum_cancellation_grace() {
        assert_eq!(
            SHUTDOWN_TIMEOUT,
            super::MAXIMUM_CANCELLATION_GRACE + Duration::from_secs(15)
        );
    }

    struct FailingBootRemover {
        calls: AtomicUsize,
    }

    impl TreeRemover for FailingBootRemover {
        fn remove_tree(&self, _path: &Path) -> io::Result<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(io::Error::other("injected boot cleanup failure"))
        }
    }

    struct BlockingCleanupSleeper {
        started: tokio::sync::mpsc::UnboundedSender<()>,
        cancelled: tokio::sync::mpsc::UnboundedSender<()>,
    }

    impl CleanupSleeper for BlockingCleanupSleeper {
        fn sleep(&self, _duration: Duration, cancellation: &CleanupCancellation) -> bool {
            let _ = self.started.send(());
            let result = cancellation.wait(Duration::from_secs(60));
            let _ = self.cancelled.send(());
            result
        }
    }

    struct NoopWorkRootHook;

    impl WorkRootHook for NoopWorkRootHook {
        fn before_child_enumeration(&self) {}
    }

    #[tokio::test]
    async fn second_signal_preempts_final_boot_root_retry() {
        let config = Config::fixture(
            "ws://127.0.0.1:1/v1/runner/connect",
            test_credential(),
            true,
        )
        .unwrap();
        let frame_source = deterministic_frame_source();
        let boot_id = frame_source.public_id("rbt_");
        let (recorder, _capture) = test_recorder(&boot_id);
        let (sleeper, _sleep_requests) = controlled_sleeper();
        let dependencies = ConnectionLoopDependencies::new(
            config.clone(),
            frame_source,
            sleeper,
            recorder,
            fixture_lease_clock(),
            boot_id.clone(),
        );
        let remover = Arc::new(FailingBootRemover {
            calls: AtomicUsize::new(0),
        });
        let (started, mut cleanup_started) = tokio::sync::mpsc::unbounded_channel();
        let (cancelled, mut cleanup_cancelled) = tokio::sync::mpsc::unbounded_channel();
        let work_root = WorkRootLease::acquire_with(
            config.assignment().work_root(),
            &boot_id,
            WorkspaceFilesystem::injected(
                remover.clone(),
                Arc::new(BlockingCleanupSleeper { started, cancelled }),
                Arc::new(NoopWorkRootHook),
            ),
        )
        .unwrap();
        let boot_path = work_root.boot_path().to_owned();
        let (connector, _attempts) = scripted_connector(Default::default());
        let (mut shutdown, shutdown_trigger) = controlled_shutdown();
        shutdown_trigger.notify_one();
        let service = run_connection_loop_with_work_root(
            dependencies,
            &connector,
            super::Backoff::with_fixed_unit(1.0),
            shutdown.as_mut(),
            work_root,
        );
        let force = async {
            cleanup_started
                .recv()
                .await
                .expect("boot cleanup did not reach its retry wait");
            shutdown_trigger.notify_one();
            cleanup_cancelled
                .recv()
                .await
                .expect("forced shutdown did not cancel boot cleanup");
        };

        let (result, ()) = tokio::join!(service, force);

        assert!(matches!(result, Err(ServiceError::ShutdownForced)));
        assert_eq!(remover.calls.load(Ordering::Relaxed), 1);
        assert!(boot_path.exists());
    }

    #[test]
    fn reports_connection_failure_cause() {
        let error = ServiceError::Connection(sequence_overflow());
        assert_eq!(
            error.to_string(),
            "runner service stopped unexpectedly: runner gateway connection failed: runner sequence overflow"
        );
    }

    fn current_credential_secret() -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0_u8; 32])
    }

    fn pending_credential_secret() -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([2_u8; 32])
    }

    #[allow(
        clippy::result_large_err,
        reason = "tungstenite's handshake callback requires its large error type"
    )]
    async fn accept_fixture_stream(stream: tokio::net::TcpStream) -> FixtureSocket {
        accept_hdr_async(
            stream,
            |_request: &HandshakeRequest, mut response: HandshakeResponse| {
                response.headers_mut().insert(
                    header::SEC_WEBSOCKET_PROTOCOL,
                    HeaderValue::from_static("scherzo.runner.v1"),
                );
                Ok(response)
            },
        )
        .await
        .unwrap()
    }

    struct RotationFixture {
        _root: tempfile::TempDir,
        _runtime: tempfile::TempDir,
        config: Config,
        config_path: PathBuf,
        state_path: PathBuf,
    }

    impl RotationFixture {
        fn new(endpoint: &str) -> Self {
            let root = tempfile::tempdir().unwrap();
            let runtime = tempfile::tempdir_in("/tmp").unwrap();
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(runtime.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let state_directory = root.path().join("state");
            let source = root.path().join("source");
            let work = root.path().join("work");
            fs::create_dir(&state_directory).unwrap();
            fs::set_permissions(&state_directory, fs::Permissions::from_mode(0o700)).unwrap();
            fs::create_dir(&source).unwrap();
            fs::create_dir(&work).unwrap();
            fs::set_permissions(&work, fs::Permissions::from_mode(0o700)).unwrap();
            fs::write(
                source.join("workflow.yaml"),
                "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
            )
            .unwrap();
            let state_path = state_directory.join("runner.json");
            let connection_endpoint = endpoint.to_owned();
            fs::write(
                &state_path,
                serde_json::to_vec_pretty(&serde_json::json!({
                    "schemaVersion": 1,
                    "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "connectionUrl": connection_endpoint,
                    "currentCredential": {
                        "id": "rrc_01k0z6r1w8f4jy2m7q9v3x5abc",
                        "secret": current_credential_secret(),
                        "activationId": "rna_01k0z6r1w8f4jy2m7q9v3x5abc",
                        "enrolledAt": "2026-08-06T12:00:00Z"
                    },
                    "pendingCredential": {
                        "id": "rrc_01k0z6r1w8f4jy2m7q9v3x5abd",
                        "secret": pending_credential_secret(),
                        "activationId": "rna_01k0z6r1w8f4jy2m7q9v3x5abd",
                        "enrolledAt": "2026-08-06T13:00:00Z"
                    },
                    "updatedAt": "2026-08-06T13:00:00Z"
                }))
                .unwrap(),
            )
            .unwrap();
            fs::set_permissions(&state_path, fs::Permissions::from_mode(0o600)).unwrap();
            let config_path = root.path().join("config.json");
            fs::write(
                &config_path,
                serde_json::to_vec_pretty(&serde_json::json!({
                    "schemaVersion": 1,
                    "deploymentMode": "development",
                    "runnerStatePath": state_path,
                    "controlSocketPath": runtime.path().join("runner.sock"),
                    "workRoot": work
                }))
                .unwrap(),
            )
            .unwrap();
            crate::runner::enrollment::load_runner_service_configuration(&config_path).unwrap();
            let config = Config::load(&config_path)
                .unwrap()
                .with_materialized_source_fixture(source, PathBuf::from("workflow.yaml"));
            Self {
                _root: root,
                _runtime: runtime,
                config,
                config_path,
                state_path,
            }
        }
        fn without_pending(endpoint: &str) -> Self {
            let mut fixture = Self::new(endpoint);
            let mut state: serde_json::Value =
                serde_json::from_slice(&fs::read(&fixture.state_path).unwrap()).unwrap();
            state.as_object_mut().unwrap().remove("pendingCredential");
            fs::write(
                &fixture.state_path,
                serde_json::to_vec_pretty(&state).unwrap(),
            )
            .unwrap();
            fs::set_permissions(&fixture.state_path, fs::Permissions::from_mode(0o600)).unwrap();
            fixture.config = Config::load(&fixture.config_path)
                .unwrap()
                .with_materialized_source_fixture(
                    fixture._root.path().join("source"),
                    PathBuf::from("workflow.yaml"),
                );
            fixture
        }

        fn stage_pending(&self) {
            let mut state: serde_json::Value =
                serde_json::from_slice(&fs::read(&self.state_path).unwrap()).unwrap();
            state["pendingCredential"] = serde_json::json!({
                "id": "rrc_01k0z6r1w8f4jy2m7q9v3x5abd",
                "secret": pending_credential_secret(),
                "activationId": "rna_01k0z6r1w8f4jy2m7q9v3x5abd",
                "enrolledAt": "2026-08-06T13:00:00Z"
            });
            fs::write(&self.state_path, serde_json::to_vec_pretty(&state).unwrap()).unwrap();
            fs::set_permissions(&self.state_path, fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    async fn reload_fixture(
        config: &Config,
    ) -> (
        Option<super::PromotedAttempt>,
        Result<String, ControlError>,
        TestCapture,
    ) {
        let frame_source = deterministic_frame_source();
        let (recorder, capture) = test_recorder("rbt_01k0z6r1w8f4jy2m7q9v3x5abe");
        let assignments = Arc::new(std::sync::Mutex::new(AssignmentManager::new_with_sleeper(
            config,
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
            fixture_lease_clock(),
            Arc::new(TokioSleeper),
            Arc::clone(&recorder),
        )));
        let status = Arc::new(LiveStatus::new(
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
            config.credential().credential_id().to_owned(),
            config
                .startup_pending()
                .map(|pending| pending.credential_id.clone()),
        ));
        let reload = ReloadDependencies {
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
            frame_source,
            sleeper: Arc::new(TokioSleeper),
            recorder,
            assignment_manager: assignments,
            live_status: status,
        };
        let (response, receive) = tokio::sync::oneshot::channel();
        let promoted = reload
            .perform(
                config,
                &Sequence::new(2),
                2,
                Some(ReloadRequest { response }),
            )
            .await;
        (promoted, receive.await.unwrap(), capture)
    }

    async fn request_control(socket_path: PathBuf, operation: Operation) -> Response {
        tokio::task::spawn_blocking(move || {
            crate::runner::control_client::request(&socket_path, operation)
        })
        .await
        .unwrap()
        .unwrap()
    }

    async fn request_staged_reload(fixture: &RotationFixture, socket_path: PathBuf) -> Response {
        fixture.stage_pending();
        request_control(socket_path, Operation::ReloadCredential).await
    }

    async fn accept_rotation_connections(
        listener: &tokio::net::TcpListener,
        current_ready: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> (FixtureSocket, FixtureSocket) {
        let (mut current, current_headers) = accept_fixture_socket_with_headers(listener).await;
        assert!(
            current_headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("Bearer rrc_01k0z6r1w8f4jy2m7q9v3x5abc."))
        );
        let Some(Ok(Message::Text(current_hello))) = current.next().await else {
            panic!("current connection omitted hello");
        };
        let current_hello: serde_json::Value = serde_json::from_str(&current_hello).unwrap();
        current.send(welcome()).await.unwrap();
        current
            .send(observation_acknowledgement(
                current_hello["messageId"].as_str().unwrap(),
                current_hello["sequence"].as_u64().unwrap(),
            ))
            .await
            .unwrap();
        if let Some(current_ready) = current_ready {
            current_ready.send(()).unwrap();
        }

        let (mut pending, pending_headers) = accept_fixture_socket_with_headers(listener).await;
        assert!(
            pending_headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("Bearer rrc_01k0z6r1w8f4jy2m7q9v3x5abd."))
        );
        let Some(Ok(Message::Text(pending_hello))) = pending.next().await else {
            panic!("pending connection omitted hello");
        };
        let pending_hello: serde_json::Value = serde_json::from_str(&pending_hello).unwrap();
        assert_eq!(pending_hello["bootId"], current_hello["bootId"]);
        pending.send(welcome()).await.unwrap();
        pending
            .send(observation_acknowledgement(
                pending_hello["messageId"].as_str().unwrap(),
                pending_hello["sequence"].as_u64().unwrap(),
            ))
            .await
            .unwrap();
        (current, pending)
    }

    #[tokio::test]
    async fn reload_promotes_a_valid_pending_credential_without_changing_boot() {
        let (listener, endpoint) = fixture_listener().await;
        let fixture = RotationFixture::new(&endpoint);
        let server = tokio::spawn(async move {
            let (mut socket, headers) = accept_fixture_socket_with_headers(&listener).await;
            let expected_authorization = format!(
                "Bearer rrc_01k0z6r1w8f4jy2m7q9v3x5abd.{}",
                pending_credential_secret()
            );
            assert_eq!(
                headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok()),
                Some(expected_authorization.as_str())
            );
            let Some(Ok(Message::Text(hello))) = socket.next().await else {
                panic!("pending connection omitted hello");
            };
            let hello: serde_json::Value = serde_json::from_str(&hello).unwrap();
            assert_eq!(hello["bootId"], "rbt_01k0z6r1w8f4jy2m7q9v3x5abe");
            socket.send(welcome()).await.unwrap();
        });

        let (promoted, response, capture) = reload_fixture(&fixture.config).await;
        assert_eq!(response, Ok("rrc_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned()));
        let promoted = promoted.unwrap();
        assert_eq!(
            promoted.config.credential().credential_id(),
            "rrc_01k0z6r1w8f4jy2m7q9v3x5abd"
        );
        let state: serde_json::Value =
            serde_json::from_slice(&fs::read(&fixture.state_path).unwrap()).unwrap();
        assert_eq!(
            state["currentCredential"]["id"],
            "rrc_01k0z6r1w8f4jy2m7q9v3x5abd"
        );
        assert!(state.get("pendingCredential").is_none());
        let telemetry = serde_json::to_string(&capture.records()).unwrap();
        for secret in [current_credential_secret(), pending_credential_secret()] {
            assert!(!telemetry.contains(&secret));
        }
        drop(promoted);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn control_socket_reload_promotes_the_live_same_boot_connection() {
        let (listener, endpoint) = fixture_listener().await;
        let fixture = RotationFixture::without_pending(&endpoint);
        let socket_path = fixture.config.control_socket_path().unwrap().to_owned();
        let (current_sent, current_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (current, mut pending) =
                accept_rotation_connections(&listener, Some(current_sent)).await;
            while pending.next().await.is_some() {}
            drop(current);
        });
        let (service, _capture, shutdown_trigger) =
            spawn_configured_service(fixture.config.clone(), Arc::new(TokioSleeper));
        with_watchdog(current_received).await.unwrap().unwrap();
        assert_eq!(
            request_staged_reload(&fixture, socket_path).await,
            Response::Reloaded {
                credential_id: "rrc_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned()
            }
        );
        let state: serde_json::Value =
            serde_json::from_slice(&fs::read(&fixture.state_path).unwrap()).unwrap();
        assert!(state.get("pendingCredential").is_none());
        shutdown_trigger.notify_one();
        with_watchdog(service).await.unwrap().unwrap().unwrap();
        with_watchdog(server).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn live_reload_retains_the_boot_and_accepted_assignment_manager() {
        let (listener, endpoint) = fixture_listener().await;
        let fixture = RotationFixture::without_pending(&endpoint);
        let socket_path = fixture.config.control_socket_path().unwrap().to_owned();
        let (accepted_sent, accepted_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut current = accept_fixture_socket(&listener).await;
            let Some(Ok(Message::Text(current_hello))) = current.next().await else {
                panic!("current connection omitted hello");
            };
            let current_hello: serde_json::Value = serde_json::from_str(&current_hello).unwrap();
            current.send(welcome()).await.unwrap();
            current
                .send(observation_acknowledgement(
                    current_hello["messageId"].as_str().unwrap(),
                    current_hello["sequence"].as_u64().unwrap(),
                ))
                .await
                .unwrap();
            current.send(assignment_offer()).await.unwrap();
            loop {
                let Some(Ok(Message::Text(frame))) = current.next().await else {
                    panic!("current connection closed before assignment acceptance");
                };
                let frame: serde_json::Value = serde_json::from_str(&frame).unwrap();
                if frame["type"] == "assignment_accepted" {
                    accepted_sent.send(()).unwrap();
                    break;
                }
            }

            let mut pending = accept_fixture_socket(&listener).await;
            let Some(Ok(Message::Text(pending_hello))) = pending.next().await else {
                panic!("pending connection omitted hello");
            };
            let pending_hello: serde_json::Value = serde_json::from_str(&pending_hello).unwrap();
            assert_eq!(pending_hello["bootId"], current_hello["bootId"]);
            pending.send(welcome()).await.unwrap();
            pending
                .send(observation_acknowledgement(
                    pending_hello["messageId"].as_str().unwrap(),
                    pending_hello["sequence"].as_u64().unwrap(),
                ))
                .await
                .unwrap();
            while let Some(Ok(Message::Text(frame))) = pending.next().await {
                let frame: serde_json::Value = serde_json::from_str(&frame).unwrap();
                if matches!(
                    frame["type"].as_str(),
                    Some("assignment_accepted" | "assignment_interrupted")
                ) {
                    pending
                        .send(observation_acknowledgement(
                            frame["messageId"].as_str().unwrap(),
                            frame["sequence"].as_u64().unwrap(),
                        ))
                        .await
                        .unwrap();
                }
                if frame["type"] == "assignment_interrupted" {
                    break;
                }
            }
            drop(current);
        });
        let (service, _capture, shutdown_trigger) =
            spawn_configured_service(fixture.config.clone(), Arc::new(TokioSleeper));
        with_watchdog(accepted_received).await.unwrap().unwrap();
        let Response::Status(before) =
            request_control(socket_path.clone(), Operation::Status).await
        else {
            panic!("control status was not available before reload");
        };
        assert_eq!(before.assignment_counts.accepted, 1);

        assert_eq!(
            request_staged_reload(&fixture, socket_path.clone()).await,
            Response::Reloaded {
                credential_id: "rrc_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned()
            }
        );
        let Response::Status(after) = request_control(socket_path, Operation::Status).await else {
            panic!("control status was not available after reload");
        };
        assert_eq!(after.boot_id, before.boot_id);
        assert_eq!(after.assignment_counts, before.assignment_counts);
        assert_eq!(after.assignment_counts.accepted, 1);

        shutdown_trigger.notify_one();
        with_watchdog(service).await.unwrap().unwrap().unwrap();
        with_watchdog(server).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn terminal_authentication_remains_locally_inspectable() {
        let (listener, endpoint) = fixture_listener().await;
        let fixture = RotationFixture::without_pending(&endpoint);
        let socket_path = fixture.config.control_socket_path().unwrap().to_owned();
        let (rejected_sent, rejected_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut current, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = current.read(&mut request).await.unwrap();
            current
                .write_all(b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
            rejected_sent.send(()).unwrap();

            let (mut pending, headers) = accept_fixture_socket_with_headers(&listener).await;
            assert!(
                headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| {
                        value.starts_with("Bearer rrc_01k0z6r1w8f4jy2m7q9v3x5abd.")
                    })
            );
            let Some(Ok(Message::Text(hello))) = pending.next().await else {
                panic!("pending connection omitted hello");
            };
            let hello: serde_json::Value = serde_json::from_str(&hello).unwrap();
            pending.send(welcome()).await.unwrap();
            pending
                .send(observation_acknowledgement(
                    hello["messageId"].as_str().unwrap(),
                    hello["sequence"].as_u64().unwrap(),
                ))
                .await
                .unwrap();
            while pending.next().await.is_some() {}
        });
        let (service, _capture, shutdown_trigger) =
            spawn_configured_service(fixture.config.clone(), Arc::new(TokioSleeper));
        with_watchdog(rejected_received).await.unwrap().unwrap();

        with_watchdog(async {
            loop {
                let response = request_control(socket_path.clone(), Operation::Status).await;
                if let Response::Status(status) = response
                    && status.connection_state == ConnectionState::AuthenticationFailed
                {
                    break;
                }
                TokioSleeper.sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            request_staged_reload(&fixture, socket_path).await,
            Response::Reloaded {
                credential_id: "rrc_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned()
            }
        );

        shutdown_trigger.notify_one();
        with_watchdog(service).await.unwrap().unwrap().unwrap();
        with_watchdog(server).await.unwrap().unwrap();
    }

    // These integration proofs intentionally exercise different interleavings
    // against the same closed handshake; keeping each scenario explicit makes
    // its ordering contract auditable.
    // jscpd:ignore-start
    #[tokio::test]
    async fn pending_handshake_preserves_contiguous_current_boot_sequences() {
        let (listener, endpoint) = fixture_listener().await;
        let mut fixture = RotationFixture::without_pending(&endpoint);
        fixture.config = fixture.config.clone().with_materialized_source_fixture(
            fixture._root.path().join("source"),
            PathBuf::from("missing-workflow-fixture.yaml"),
        );
        let socket_path = fixture.config.control_socket_path().unwrap().to_owned();
        let (current_sent, current_received) = tokio::sync::oneshot::channel();
        let (sequences_sent, sequences_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut current = accept_fixture_socket(&listener).await;
            let Some(Ok(Message::Text(current_hello))) = current.next().await else {
                panic!("current connection omitted hello");
            };
            let current_hello: serde_json::Value = serde_json::from_str(&current_hello).unwrap();
            current.send(welcome()).await.unwrap();
            current
                .send(observation_acknowledgement(
                    current_hello["messageId"].as_str().unwrap(),
                    current_hello["sequence"].as_u64().unwrap(),
                ))
                .await
                .unwrap();
            current_sent.send(()).unwrap();

            let (pending_stream, _) = listener.accept().await.unwrap();
            current.send(assignment_offer()).await.unwrap();
            let Some(Ok(Message::Text(current_observation))) = current.next().await else {
                panic!("current connection omitted assignment observation");
            };
            let current_observation: serde_json::Value =
                serde_json::from_str(&current_observation).unwrap();
            let latest_current_sequence = current_observation["sequence"].as_u64().unwrap();

            let mut pending = accept_fixture_stream(pending_stream).await;
            let Some(Ok(Message::Text(pending_hello))) = pending.next().await else {
                panic!("pending connection omitted hello");
            };
            let pending_hello: serde_json::Value = serde_json::from_str(&pending_hello).unwrap();
            sequences_sent
                .send((
                    latest_current_sequence,
                    pending_hello["sequence"].as_u64().unwrap(),
                ))
                .unwrap();
            pending
                .send(Message::Text("invalid-pending-frame".into()))
                .await
                .unwrap();
            while current.next().await.is_some() {}
        });
        let (service, _capture, shutdown_trigger) =
            spawn_configured_service(fixture.config.clone(), Arc::new(TokioSleeper));
        with_watchdog(current_received).await.unwrap().unwrap();
        assert_eq!(
            request_staged_reload(&fixture, socket_path).await,
            Response::Error(ControlError::PendingProtocolFailed)
        );
        let (latest_current_sequence, pending_hello_sequence) =
            with_watchdog(sequences_received).await.unwrap().unwrap();
        assert!(
            pending_hello_sequence > latest_current_sequence,
            "pending hello sequence {pending_hello_sequence} regressed behind current sequence {latest_current_sequence}"
        );

        shutdown_trigger.notify_one();
        with_watchdog(service).await.unwrap().unwrap().unwrap();
        with_watchdog(server).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn pending_handshake_keeps_the_current_transport_live() {
        let (listener, endpoint) = fixture_listener().await;
        let fixture = RotationFixture::without_pending(&endpoint);
        let socket_path = fixture.config.control_socket_path().unwrap().to_owned();
        let (current_sent, current_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut current = accept_fixture_socket(&listener).await;
            let Some(Ok(Message::Text(current_hello))) = current.next().await else {
                panic!("current connection omitted hello");
            };
            let current_hello: serde_json::Value = serde_json::from_str(&current_hello).unwrap();
            current.send(welcome()).await.unwrap();
            current
                .send(observation_acknowledgement(
                    current_hello["messageId"].as_str().unwrap(),
                    current_hello["sequence"].as_u64().unwrap(),
                ))
                .await
                .unwrap();
            current_sent.send(()).unwrap();

            let mut pending = accept_fixture_socket(&listener).await;
            expect_opening_hello(&mut pending).await;
            current
                .send(Message::Ping(vec![1, 2, 3].into()))
                .await
                .unwrap();
            loop {
                match with_watchdog(current.next()).await.unwrap() {
                    Some(Ok(Message::Pong(payload))) => {
                        assert_eq!(payload.as_ref(), &[1, 2, 3]);
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => panic!("current connection failed: {error}"),
                    None => panic!("current connection closed during pending handshake"),
                }
            }
            pending
                .send(Message::Text("invalid-pending-frame".into()))
                .await
                .unwrap();
            while current.next().await.is_some() {}
        });
        let (service, _capture, shutdown_trigger) =
            spawn_configured_service(fixture.config.clone(), Arc::new(TokioSleeper));
        with_watchdog(current_received).await.unwrap().unwrap();
        assert_eq!(
            request_staged_reload(&fixture, socket_path).await,
            Response::Error(ControlError::PendingProtocolFailed)
        );
        shutdown_trigger.notify_one();
        with_watchdog(service).await.unwrap().unwrap().unwrap();
        with_watchdog(server).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn startup_establishes_current_before_promoting_pending_in_the_same_boot() {
        let (listener, endpoint) = fixture_listener().await;
        let fixture = RotationFixture::new(&endpoint);
        let state_path = fixture.state_path.clone();
        let (promoted_sent, promoted_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (current, mut pending) = accept_rotation_connections(&listener, None).await;
            promoted_sent.send(()).unwrap();
            while pending.next().await.is_some() {}
            drop(current);
        });
        let (service, _capture, shutdown_trigger) =
            spawn_configured_service(fixture.config, Arc::new(TokioSleeper));
        with_watchdog(promoted_received).await.unwrap().unwrap();
        loop {
            let state: serde_json::Value =
                serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
            if state.get("pendingCredential").is_none() {
                break;
            }
            TokioSleeper.sleep(Duration::from_millis(1)).await;
        }
        shutdown_trigger.notify_one();
        with_watchdog(service).await.unwrap().unwrap().unwrap();
        with_watchdog(server).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn reload_state_write_failure_preserves_current_and_pending_state() {
        let (listener, endpoint) = fixture_listener().await;
        let fixture = RotationFixture::new(&endpoint);
        let original = fs::read(&fixture.state_path).unwrap();
        let state_path = fixture.state_path.clone();
        let hard_link = state_path.with_extension("hard-link");
        let server = tokio::spawn(async move {
            let mut socket = accept_fixture_socket(&listener).await;
            expect_opening_hello(&mut socket).await;
            fs::hard_link(&state_path, &hard_link).unwrap();
            socket.send(welcome()).await.unwrap();
            hard_link
        });

        let (promoted, response, _capture) = reload_fixture(&fixture.config).await;
        assert!(promoted.is_none());
        assert_eq!(response, Err(ControlError::StateUpdateFailed));
        assert_eq!(fs::read(&fixture.state_path).unwrap(), original);
        let hard_link = server.await.unwrap();
        fs::remove_file(hard_link).unwrap();
    }

    #[tokio::test]
    async fn reload_state_write_failure_restores_current_transport() {
        let (listener, endpoint) = fixture_listener().await;
        let fixture = RotationFixture::without_pending(&endpoint);
        fixture.stage_pending();
        let staged_state = fs::read(&fixture.state_path).unwrap();
        let state_path = fixture.state_path.clone();
        let hard_link = state_path.with_extension("hard-link");
        let socket_path = fixture.config.control_socket_path().unwrap().to_owned();
        let (current_sent, current_received) = tokio::sync::oneshot::channel();
        let (recovered_sent, recovered_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut current, _) = accept_fixture_socket_with_headers(&listener).await;
            let Some(Ok(Message::Text(current_hello))) = current.next().await else {
                panic!("current connection omitted hello");
            };
            let current_hello: serde_json::Value = serde_json::from_str(&current_hello).unwrap();
            current.send(welcome()).await.unwrap();
            current
                .send(observation_acknowledgement(
                    current_hello["messageId"].as_str().unwrap(),
                    current_hello["sequence"].as_u64().unwrap(),
                ))
                .await
                .unwrap();
            current_sent.send(()).unwrap();

            let (mut pending, pending_headers) =
                accept_fixture_socket_with_headers(&listener).await;
            assert!(
                pending_headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| {
                        value.starts_with("Bearer rrc_01k0z6r1w8f4jy2m7q9v3x5abd.")
                    })
            );
            let Some(Ok(Message::Text(pending_hello))) = pending.next().await else {
                panic!("pending connection omitted hello");
            };
            let pending_hello: serde_json::Value = serde_json::from_str(&pending_hello).unwrap();
            fs::hard_link(&state_path, &hard_link).unwrap();
            pending.send(welcome()).await.unwrap();
            pending
                .send(observation_acknowledgement(
                    pending_hello["messageId"].as_str().unwrap(),
                    pending_hello["sequence"].as_u64().unwrap(),
                ))
                .await
                .unwrap();
            drop(current);

            let (mut recovered, recovered_headers) =
                accept_fixture_socket_with_headers(&listener).await;
            assert!(
                recovered_headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| {
                        value.starts_with("Bearer rrc_01k0z6r1w8f4jy2m7q9v3x5abc.")
                    })
            );
            let Some(Ok(Message::Text(recovered_hello))) = recovered.next().await else {
                panic!("recovered current connection omitted hello");
            };
            let recovered_hello: serde_json::Value =
                serde_json::from_str(&recovered_hello).unwrap();
            assert_eq!(recovered_hello["bootId"], pending_hello["bootId"]);
            assert!(
                recovered_hello["sequence"].as_u64().unwrap()
                    > pending_hello["sequence"].as_u64().unwrap()
            );
            recovered.send(welcome()).await.unwrap();
            recovered
                .send(observation_acknowledgement(
                    recovered_hello["messageId"].as_str().unwrap(),
                    recovered_hello["sequence"].as_u64().unwrap(),
                ))
                .await
                .unwrap();
            recovered_sent.send(()).unwrap();
            while recovered.next().await.is_some() {}
            hard_link
        });
        let (service, _capture, shutdown_trigger) =
            spawn_configured_service(fixture.config.clone(), Arc::new(TokioSleeper));
        with_watchdog(current_received).await.unwrap().unwrap();
        assert_eq!(
            request_control(socket_path, Operation::ReloadCredential).await,
            Response::Error(ControlError::StateUpdateFailed)
        );
        with_watchdog(recovered_received).await.unwrap().unwrap();
        assert_eq!(fs::read(&fixture.state_path).unwrap(), staged_state);

        shutdown_trigger.notify_one();
        with_watchdog(service).await.unwrap().unwrap().unwrap();
        let hard_link = with_watchdog(server).await.unwrap().unwrap();
        fs::remove_file(hard_link).unwrap();
    }

    // jscpd:ignore-end
    #[tokio::test]
    async fn reload_authentication_failure_preserves_pending_state() {
        let (listener, endpoint) = fixture_listener().await;
        let fixture = RotationFixture::new(&endpoint);
        let original = fs::read(&fixture.state_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
        });

        let (promoted, response, capture) = reload_fixture(&fixture.config).await;
        assert!(promoted.is_none());
        assert_eq!(response, Err(ControlError::PendingAuthenticationFailed));
        assert_eq!(fs::read(&fixture.state_path).unwrap(), original);
        let telemetry = serde_json::to_string(&capture.records()).unwrap();
        assert!(!telemetry.contains(&pending_credential_secret()));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reload_network_failure_preserves_pending_state() {
        let fixture = RotationFixture::new("ws://127.0.0.1:1/v1/runner/connect");
        let original = fs::read(&fixture.state_path).unwrap();
        let (promoted, response, capture) = reload_fixture(&fixture.config).await;
        assert!(promoted.is_none());
        assert_eq!(response, Err(ControlError::PendingConnectionFailed));
        assert_eq!(fs::read(&fixture.state_path).unwrap(), original);
        let telemetry = serde_json::to_string(&capture.records()).unwrap();
        assert!(!telemetry.contains(&pending_credential_secret()));
    }

    #[tokio::test]
    async fn reload_protocol_failure_preserves_pending_state_without_raw_diagnostics() {
        let (listener, endpoint) = fixture_listener().await;
        let fixture = RotationFixture::new(&endpoint);
        let original = fs::read(&fixture.state_path).unwrap();
        let server = tokio::spawn(async move {
            let mut socket = accept_fixture_socket(&listener).await;
            expect_opening_hello(&mut socket).await;
            socket
                .send(Message::Text(
                    "RAW-PROTOCOL-DIAGNOSTIC-MUST-NOT-LEAK".into(),
                ))
                .await
                .unwrap();
        });

        let (promoted, response, capture) = reload_fixture(&fixture.config).await;
        assert!(promoted.is_none());
        assert_eq!(response, Err(ControlError::PendingProtocolFailed));
        assert_eq!(fs::read(&fixture.state_path).unwrap(), original);
        assert!(
            !serde_json::to_string(&capture.records())
                .unwrap()
                .contains("RAW-PROTOCOL-DIAGNOSTIC-MUST-NOT-LEAK")
        );
        server.await.unwrap();
    }

    fn accepted_assignment_config(endpoint: &str) -> (tempfile::TempDir, Config) {
        let temporary = tempfile::tempdir().expect("create service fixture root");
        let source = temporary.path().join("source");
        let work = temporary.path().join("work");
        fs::create_dir(&source).expect("create materialized source fixture");
        fs::create_dir(&work).expect("create runner work root");
        fs::set_permissions(&work, fs::Permissions::from_mode(0o700))
            .expect("make runner work root private");
        fs::write(
            source.join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        )
        .expect("write materialized workflow fixture");
        let assignment = AssignmentConfig::new(&work).expect("configure assignment");
        let config = Config::new(endpoint, test_credential(), true, assignment)
            .expect("configure gateway")
            .with_materialized_source_fixture(source, PathBuf::from("workflow.yaml"));
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
        ControlledShutdownTrigger,
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
        ControlledShutdownTrigger,
    ) {
        let (recorder, capture) = test_recorder("rbt_00000000000000000000000001");
        let (shutdown, shutdown_trigger) = controlled_shutdown();
        let service = tokio::spawn(run_until_cancelled_with_dependencies(
            config,
            deterministic_frame_source(),
            sleeper,
            recorder,
            shutdown,
        ));
        (service, capture, shutdown_trigger)
    }

    struct AcceptedAssignmentService {
        _temporary: tempfile::TempDir,
        task: tokio::task::JoinHandle<Result<(), ServiceError>>,
        shutdown_trigger: ControlledShutdownTrigger,
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
        assert_eq!(
            event["scherzo.connection.failure_kind"],
            "terminal_protocol"
        );
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
        assert_eq!(event["error.type"], "read_gateway_frame");
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
