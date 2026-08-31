use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use super::assignment::AssignmentManager;
use super::backoff::Backoff;
use super::connection::{
    ActiveEffectEvent, ConnectionDependencies, ConnectionError, ConnectionProgress, FrameSource,
    OpeningHello, opening_hello, run_established,
};
use super::test_support::{
    ControlledShutdownTrigger, DeterminismTranscript, ScriptedConnection, ScriptedConnector,
    ScriptedInbound, ScriptedReader, ScriptedWriter, SleepRelease, assignment_offer,
    assignment_prepare, controlled_shutdown, controlled_sleeper_with_transcript,
    deterministic_frame_source, effect_observation_acknowledgement, fixture_lease_clock,
    observation_acknowledgement, scripted_connector, scripted_duplex, sleep_request, welcome,
    with_watchdog,
};
use super::workspace::{
    CleanupCancellation, CleanupSleeper, TreeRemover, WorkRootHook, WorkRootLease,
    WorkspaceFilesystem,
};
use super::{
    Config, ConnectionLoopDependencies, Connector, ServiceError, Shutdown, Sleeper,
    run_connection_loop, run_connection_loop_with_work_root,
};
use crate::runner::credential::test_credential;
use crate::runner::telemetry::test_recorder;

const ESTABLISHED_REPETITIONS: usize = 1_000;
const RECONNECT_REPETITIONS: usize = 100;
const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abd";
const BOOT_ID: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe";
const OPENING_MESSAGE_ID: &str = "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc";
const EFFECT_ID: &str = "eff_01k0z6r1w8f4jy2m7q9v3x5abg";
const PREPARE_EFFECT_ID: &str = "eff_01k0z6r1w8f4jy2m7q9v3x5abh";

#[tokio::test]
async fn established_connection_has_a_deterministic_transcript() {
    let mut expected = None;
    for repetition in 0..ESTABLISHED_REPETITIONS {
        let actual = with_watchdog(run_assignment_scenario())
            .await
            .expect("deterministic assignment scenario timed out");
        assert_stable_transcript(&mut expected, actual, repetition);
    }
}

#[tokio::test]
async fn welcome_and_silence_boundaries_have_deterministic_transcripts() {
    let mut expected = None;
    for repetition in 0..ESTABLISHED_REPETITIONS {
        let actual = with_watchdog(run_timeout_boundary_scenarios())
            .await
            .expect("deterministic timeout scenarios timed out");
        assert_stable_transcript(&mut expected, actual, repetition);
    }
}

#[tokio::test]
async fn reconnect_backoff_reset_and_cancellation_have_a_deterministic_transcript() {
    let mut expected = None;
    for repetition in 0..RECONNECT_REPETITIONS {
        let actual = with_watchdog(run_reconnect_scenario())
            .await
            .expect("deterministic reconnect scenario timed out");
        assert_stable_transcript(&mut expected, actual, repetition);
    }
}

struct FailingTreeRemover;

impl TreeRemover for FailingTreeRemover {
    fn remove_tree(&self, _path: &Path) -> io::Result<()> {
        Err(io::Error::other("injected cleanup failure"))
    }
}

struct ImmediateCleanupSleeper;

impl CleanupSleeper for ImmediateCleanupSleeper {
    fn sleep(&self, _duration: Duration, cancellation: &CleanupCancellation) -> bool {
        !cancellation.is_cancelled()
    }
}

struct TrackingSleeper {
    inner: Arc<dyn Sleeper>,
    next_id: AtomicUsize,
    created: mpsc::UnboundedSender<(usize, Duration)>,
    polled: mpsc::UnboundedSender<usize>,
}

impl Sleeper for TrackingSleeper {
    fn now(&self) -> std::time::Instant {
        self.inner.now()
    }

    fn sleep(&self, duration: Duration) -> super::SleepFuture<'_> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let _ = self.created.send((id, duration));
        let polled = self.polled.clone();
        let mut inner = self.inner.sleep(duration);
        Box::pin(std::future::poll_fn(move |context| {
            let _ = polled.send(id);
            inner.as_mut().poll(context)
        }))
    }
}

struct TrackingSleeperFixture {
    sleeper: Arc<dyn Sleeper>,
    creations: mpsc::UnboundedReceiver<(usize, Duration)>,
    polls: mpsc::UnboundedReceiver<usize>,
}

fn tracking_sleeper(inner: Arc<dyn Sleeper>) -> TrackingSleeperFixture {
    let (created, creations) = mpsc::unbounded_channel();
    let (polled, polls) = mpsc::unbounded_channel();
    TrackingSleeperFixture {
        sleeper: Arc::new(TrackingSleeper {
            inner,
            next_id: AtomicUsize::new(0),
            created,
            polled,
        }),
        creations,
        polls,
    }
}

struct GatedFirstRemoval {
    calls: AtomicUsize,
    started: tokio::sync::mpsc::UnboundedSender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl TreeRemover for GatedFirstRemoval {
    fn remove_tree(&self, path: &Path) -> io::Result<()> {
        if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
            let _ = self.started.send(());
            self.release
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .recv()
                .map_err(|_| io::Error::other("cleanup gate closed"))?;
        }
        std::fs::remove_dir_all(path)
    }
}

struct NoopWorkRootHook;

impl WorkRootHook for NoopWorkRootHook {
    fn before_child_enumeration(&self) {}
}

fn injected_work_root(
    dependencies: &ConnectionLoopDependencies,
    remover: Arc<dyn TreeRemover>,
) -> Arc<WorkRootLease> {
    WorkRootLease::acquire_with(
        dependencies.config.assignment().work_root(),
        &dependencies.boot_id,
        WorkspaceFilesystem::injected(
            remover,
            Arc::new(ImmediateCleanupSleeper),
            Arc::new(NoopWorkRootHook),
        ),
    )
    .unwrap()
}

struct CleanupServiceFixture {
    dependencies: ConnectionLoopDependencies,
    connector: ScriptedConnector,
    attempts: mpsc::UnboundedReceiver<ScriptedConnection>,
    shutdown: Box<dyn Shutdown>,
    shutdown_trigger: ControlledShutdownTrigger,
    sleep_requests: mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
    work_root: Arc<WorkRootLease>,
}

fn cleanup_service_fixture(remover: Arc<dyn TreeRemover>) -> CleanupServiceFixture {
    let transcript = DeterminismTranscript::default();
    let (sleeper, sleep_requests) = controlled_sleeper_with_transcript(transcript.clone());
    cleanup_service_fixture_with_sleeper(remover, transcript, sleeper, sleep_requests)
}

fn cleanup_service_fixture_with_sleeper(
    remover: Arc<dyn TreeRemover>,
    transcript: DeterminismTranscript,
    sleeper: Arc<dyn Sleeper>,
    sleep_requests: mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
) -> CleanupServiceFixture {
    let (connector, attempts) = scripted_connector(transcript);
    let (shutdown, shutdown_trigger) = controlled_shutdown();
    let dependencies = deterministic_connection_loop_dependencies(sleeper);
    let work_root = injected_work_root(&dependencies, remover);
    CleanupServiceFixture {
        dependencies,
        connector,
        attempts,
        shutdown,
        shutdown_trigger,
        sleep_requests,
        work_root,
    }
}

async fn run_cleanup_service(
    dependencies: ConnectionLoopDependencies,
    connector: &ScriptedConnector,
    shutdown: &mut dyn Shutdown,
    work_root: Arc<WorkRootLease>,
) -> Result<(), ServiceError> {
    run_connection_loop_with_work_root(
        dependencies,
        connector,
        Backoff::with_fixed_unit(1.0),
        shutdown,
        work_root,
    )
    .await
}

async fn offer_rejected_assignment(
    attempts: &mut mpsc::UnboundedReceiver<ScriptedConnection>,
    sleep_requests: &mut mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
) -> ScriptedConnection {
    let mut connection = next_connection(attempts).await;
    let hello = next_hello(&mut connection).await;
    complete_handshake(&connection, sleep_requests, &hello).await;
    connection.inbound.send(scripted_assignment_offer());
    let effect_acknowledgement = next_outbound(&mut connection.outbound).await;
    assert_eq!(
        decode_text(&effect_acknowledgement, "effect acknowledgement")["type"],
        "effect_acknowledged"
    );
    let preparing = next_outbound(&mut connection.outbound).await;
    assert_eq!(
        decode_text(&preparing, "assignment preparation acknowledgement")["type"],
        "assignment_preparing"
    );
    connection.inbound.send(assignment_prepare());
    let prepare_acknowledgement = next_outbound(&mut connection.outbound).await;
    assert_eq!(
        decode_text(&prepare_acknowledgement, "prepare effect acknowledgement")["type"],
        "effect_acknowledged"
    );
    for (preparation_sequence, phase) in [
        (1, "source_materialization"),
        (2, "input_download"),
        (3, "workflow_admission"),
    ] {
        let progress = next_outbound(&mut connection.outbound).await;
        let progress = decode_text(&progress, "assignment preparation progress");
        assert_eq!(progress["type"], "assignment_preparation_progress");
        assert_eq!(
            progress["payload"]["preparationSequence"],
            preparation_sequence
        );
        assert_eq!(progress["payload"]["phase"], phase);
    }
    let rejection = next_outbound(&mut connection.outbound).await;
    assert_eq!(
        decode_text(&rejection, "assignment rejection")["type"],
        "assignment_rejected"
    );
    connection
}

#[tokio::test]
async fn cleanup_completion_does_not_restart_the_inbound_silence_timer() {
    let (started, mut cleanup_started) = tokio::sync::mpsc::unbounded_channel();
    let (release_cleanup, release) = std::sync::mpsc::channel();
    let transcript = DeterminismTranscript::default();
    let (controlled, sleep_requests) = controlled_sleeper_with_transcript(transcript.clone());
    let TrackingSleeperFixture {
        sleeper: tracked,
        creations: mut timer_creations,
        polls: mut timer_polls,
    } = tracking_sleeper(controlled);
    let mut fixture = cleanup_service_fixture_with_sleeper(
        Arc::new(GatedFirstRemoval {
            calls: AtomicUsize::new(0),
            started,
            release: Mutex::new(release),
        }),
        transcript,
        tracked,
        sleep_requests,
    );
    // The recovery and failure cases keep separate peer futures because their post-rejection
    // synchronization and service termination authority are intentionally different.
    // Keep shutdown pending: this scenario ends by cancelling the service future after the
    // assertion rather than exercising the independent graceful-shutdown protocol.
    let _shutdown_lifetime = fixture.shutdown_trigger;
    // jscpd:ignore-start
    let service = run_cleanup_service(
        fixture.dependencies,
        &fixture.connector,
        fixture.shutdown.as_mut(),
        fixture.work_root,
    );
    let peer = async {
        let _connection =
            offer_rejected_assignment(&mut fixture.attempts, &mut fixture.sleep_requests).await;
        // jscpd:ignore-end
        cleanup_started
            .recv()
            .await
            .expect("cleanup did not reach the injected removal gate");
        let silence_timer =
            sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2)).await;
        let mut silence_timer_id = None;
        while let Ok((id, duration)) = timer_creations.try_recv() {
            if duration == Duration::from_secs(2) {
                silence_timer_id = Some(id);
            }
        }
        let silence_timer_id = silence_timer_id.expect("inbound silence timer was not created");
        while timer_polls.try_recv().is_ok() {}
        release_cleanup.send(()).unwrap();
        let repolled = with_watchdog(timer_polls.recv())
            .await
            .expect("cleanup completion did not repoll the established connection")
            .expect("tracking sleeper closed before connection repoll");
        assert_eq!(
            repolled, silence_timer_id,
            "local cleanup completion replaced the gateway silence timer"
        );
        drop(silence_timer);
    };
    with_watchdog(async {
        tokio::pin!(service);
        tokio::pin!(peer);
        tokio::select! {
            biased;
            result = &mut service => {
                panic!("runner service stopped before cleanup recovery completed: {result:?}")
            }
            () = &mut peer => {}
        }
    })
    .await
    .expect("cleanup recovery scenario timed out");
}

#[tokio::test]
async fn cleanup_failure_ends_established_service_without_gateway_close() {
    let mut fixture = cleanup_service_fixture(Arc::new(FailingTreeRemover));
    let (finished, service_finished) = tokio::sync::oneshot::channel();
    // Keep this distinct async ownership boundary beside the recovered case above.
    // jscpd:ignore-start
    let service = async {
        let result = run_cleanup_service(
            fixture.dependencies,
            &fixture.connector,
            fixture.shutdown.as_mut(),
            fixture.work_root,
        )
        .await;
        let _ = finished.send(());
        result
    };
    let peer = async {
        let _connection =
            offer_rejected_assignment(&mut fixture.attempts, &mut fixture.sleep_requests).await;
        service_finished
            .await
            .expect("service dropped without reporting cleanup failure");
        // jscpd:ignore-end
    };
    let result = with_watchdog(async {
        let (result, ()) = tokio::join!(service, peer);
        result
    })
    .await
    .expect("cleanup failure did not wake and terminate the established service");
    assert!(matches!(result, Err(ServiceError::WorkspaceCleanupFailed)));
}

#[tokio::test]
async fn terminal_gateway_close_stops_without_backoff() {
    let transcript = with_watchdog(run_terminal_close_scenario())
        .await
        .expect("deterministic terminal-close scenario timed out");
    assert_eq!(
        transcript
            .iter()
            .filter(|event| event.starts_with("connection.attempt:"))
            .count(),
        1
    );
    assert!(
        transcript
            .iter()
            .any(|event| event.starts_with("connection.outcome:terminal:"))
    );
    assert!(
        transcript
            .iter()
            .all(|event| !event.starts_with("sleep.requested:1000ms"))
    );
}

async fn run_assignment_scenario() -> Vec<String> {
    let transcript = DeterminismTranscript::default();
    let mut fixture = established_fixture(&transcript);
    let mut next_sequence = 2;
    let connection = fixture
        .runtime
        .run(&mut next_sequence, fixture.reader, fixture.writer);
    let peer = async {
        let hello = next_outbound(&mut fixture.outbound).await;
        let hello = decode_text(&hello, "opening event");
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["messageId"], OPENING_MESSAGE_ID);

        let welcome_timer =
            sleep_request(&mut fixture.sleep_requests, Duration::from_secs(5)).await;
        fixture.inbound.send(welcome());
        let first_silence_timer =
            sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2)).await;
        fixture
            .inbound
            .send(observation_acknowledgement(OPENING_MESSAGE_ID, 1));
        let second_silence_timer =
            sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2)).await;
        fixture.inbound.send(scripted_assignment_offer());

        let acknowledgement = next_outbound(&mut fixture.outbound).await;
        let acknowledgement = assert_effect_acknowledgement(
            &acknowledgement,
            "rmsg_00000000000000000000000001",
            BOOT_ID,
            2,
        );
        let acknowledgement_message_id = acknowledgement["messageId"]
            .as_str()
            .expect("effect acknowledgement message ID");

        let assignment_silence_timer =
            sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2)).await;
        fixture.inbound.send(effect_observation_acknowledgement(
            acknowledgement_message_id,
            2,
        ));
        let preparing = next_outbound(&mut fixture.outbound).await;
        let preparing = decode_text(&preparing, "assignment preparation acknowledgement");
        assert_eq!(preparing["type"], "assignment_preparing");
        assert_eq!(preparing["sequence"], 3);
        let preparing_silence_timer =
            sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2)).await;
        fixture.inbound.send(effect_observation_acknowledgement(
            preparing["messageId"]
                .as_str()
                .expect("assignment preparation acknowledgement message ID"),
            3,
        ));

        let prepare_effect_silence_timer =
            sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2)).await;
        fixture.inbound.send(assignment_prepare());
        let prepare_acknowledgement = next_outbound(&mut fixture.outbound).await;
        let prepare_acknowledgement =
            decode_text(&prepare_acknowledgement, "prepare effect acknowledgement");
        assert_eq!(prepare_acknowledgement["type"], "effect_acknowledged");
        assert_eq!(prepare_acknowledgement["sequence"], 4);
        assert_eq!(
            prepare_acknowledgement["payload"]["effectId"],
            PREPARE_EFFECT_ID
        );
        let prepare_acknowledgement_silence_timer =
            sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2)).await;
        fixture.inbound.send(effect_observation_acknowledgement(
            prepare_acknowledgement["messageId"]
                .as_str()
                .expect("prepare effect acknowledgement message ID"),
            4,
        ));

        let mut progress_silence_timers = Vec::new();
        for (sequence, preparation_sequence, phase) in [
            (5, 1, "source_materialization"),
            (6, 2, "input_download"),
            (7, 3, "workflow_admission"),
        ] {
            let progress = next_outbound(&mut fixture.outbound).await;
            let progress = decode_text(&progress, "assignment preparation progress");
            assert_eq!(progress["type"], "assignment_preparation_progress");
            assert_eq!(progress["sequence"], sequence);
            assert_eq!(
                progress["payload"]["preparationSequence"],
                preparation_sequence
            );
            assert_eq!(progress["payload"]["phase"], phase);
            let progress_silence_timer =
                sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2)).await;
            fixture.inbound.send(effect_observation_acknowledgement(
                progress["messageId"]
                    .as_str()
                    .expect("assignment preparation progress message ID"),
                sequence,
            ));
            progress_silence_timers.push(progress_silence_timer);
        }

        let semantic = next_outbound(&mut fixture.outbound).await;
        let semantic = decode_text(&semantic, "semantic assignment response");
        assert_eq!(semantic["type"], "assignment_rejected");
        assert_eq!(semantic["sequence"], 8);
        assert_eq!(
            semantic["payload"]["decline"]["reason"],
            "workflow_source_invalid"
        );
        let semantic_silence_timer =
            sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2)).await;
        fixture.inbound.send(effect_observation_acknowledgement(
            semantic["messageId"]
                .as_str()
                .expect("semantic assignment response message ID"),
            8,
        ));
        let stress_silence_timer =
            sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2)).await;
        fixture
            .inbound
            .send(Message::Ping(b"scripted-boundary".to_vec().into()));
        stress_silence_timer.release();
        let pong = next_outbound(&mut fixture.outbound).await;
        assert_eq!(pong, Message::Pong(b"scripted-boundary".to_vec().into()));

        let final_silence_timer =
            sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2)).await;
        fixture.inbound.send(gateway_close(
            CloseCode::Normal,
            "assignment script complete",
        ));

        drop(welcome_timer);
        drop(first_silence_timer);
        drop(second_silence_timer);
        drop(assignment_silence_timer);
        drop(preparing_silence_timer);
        drop(prepare_effect_silence_timer);
        drop(prepare_acknowledgement_silence_timer);
        drop(progress_silence_timers);
        drop(semantic_silence_timer);
        drop(final_silence_timer);
    };

    let (outcome, ()) = tokio::join!(connection, peer);
    let outcome = outcome.expect("run deterministic established connection");
    assert!(outcome.opening_acknowledged);
    assert!(outcome.handshake_completed);
    assert_eq!(next_sequence, 9);
    transcript.record(
        "scenario.outcome:gateway-close:opening_acknowledged=true:handshake_completed=true"
            .to_owned(),
    );
    let events = transcript.snapshot();
    assert_assignment_acknowledgement_order(&events);
    events
}

async fn run_timeout_boundary_scenarios() -> Vec<String> {
    let transcript = DeterminismTranscript::default();
    run_timeout_scenario(&transcript, TimeoutScenario::Welcome).await;
    run_timeout_scenario(&transcript, TimeoutScenario::InboundSilence).await;
    transcript.snapshot()
}

#[derive(Clone, Copy)]
enum TimeoutScenario {
    Welcome,
    InboundSilence,
}

async fn run_timeout_scenario(transcript: &DeterminismTranscript, scenario: TimeoutScenario) {
    let (name, expected_cause) = match scenario {
        TimeoutScenario::Welcome => ("welcome-timeout", "gateway welcome timeout"),
        TimeoutScenario::InboundSilence => ("inbound-silence-boundary", "gateway liveness timeout"),
    };
    transcript.record(format!("scenario.start:{name}"));
    let mut fixture = established_fixture(transcript);
    let mut next_sequence = 2;
    let connection = fixture
        .runtime
        .run(&mut next_sequence, fixture.reader, fixture.writer);
    let peer = async {
        let _hello = next_outbound(&mut fixture.outbound).await;
        match scenario {
            TimeoutScenario::Welcome => {
                sleep_request(&mut fixture.sleep_requests, Duration::from_secs(5))
                    .await
                    .release();
            }
            TimeoutScenario::InboundSilence => {
                let welcome_timer =
                    sleep_request(&mut fixture.sleep_requests, Duration::from_secs(5)).await;
                fixture.inbound.send(welcome());
                welcome_timer.release();

                let first_silence_timer =
                    sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2)).await;
                fixture
                    .inbound
                    .send(observation_acknowledgement(OPENING_MESSAGE_ID, 1));
                first_silence_timer.release();

                sleep_request(&mut fixture.sleep_requests, Duration::from_secs(2))
                    .await
                    .release();
                let local_close = next_outbound(&mut fixture.outbound).await;
                assert_eq!(
                    local_close,
                    Message::Close(Some(CloseFrame {
                        code: CloseCode::Away,
                        reason: "gateway liveness timeout".into(),
                    }))
                );
            }
        }
    };
    let (outcome, ()) = tokio::join!(connection, peer);
    let error = outcome.expect_err("released timeout did not fail the connection");
    assert!(!error.is_terminal());
    assert_eq!(error.cause(), expected_cause);
    assert_eq!(
        error.progress.opening_acknowledged,
        matches!(scenario, TimeoutScenario::InboundSilence)
    );
    assert_eq!(
        error.progress.handshake_completed,
        matches!(scenario, TimeoutScenario::InboundSilence)
    );
    assert_eq!(next_sequence, 2);
    if matches!(scenario, TimeoutScenario::Welcome) {
        assert!(
            fixture.outbound.try_recv().is_err(),
            "welcome timeout sent a frame"
        );
    }
    transcript.record(format!("scenario.outcome:retryable:{expected_cause}"));
}

async fn run_reconnect_scenario() -> Vec<String> {
    let transcript = DeterminismTranscript::default();
    let (sleeper, mut sleep_requests) = controlled_sleeper_with_transcript(transcript.clone());
    let (connector, mut attempts) = scripted_connector(transcript.clone());
    let (mut shutdown, cancel) = controlled_shutdown();
    let (finished, service_finished) = tokio::sync::oneshot::channel();
    let service = async {
        let result =
            run_deterministic_connection_loop(sleeper, &connector, shutdown.as_mut()).await;
        let _ = finished.send(());
        result
    };
    let peer = async {
        let mut first = next_connection(&mut attempts).await;
        let first_hello = next_hello(&mut first).await;
        assert_hello(
            &first_hello,
            "rmsg_00000000000000000000000002",
            "rbt_00000000000000000000000001",
            1,
        );
        let first_welcome_timer = sleep_request(&mut sleep_requests, Duration::from_secs(5)).await;
        first.inbound.send(gateway_close(
            CloseCode::Normal,
            "retry before opening acknowledgement",
        ));
        drop(first_welcome_timer);
        release_backoff(&mut sleep_requests, &transcript, Duration::from_secs(1)).await;

        let mut second = next_connection(&mut attempts).await;
        let second_hello = next_hello(&mut second).await;
        assert_eq!(
            second_hello, first_hello,
            "unacknowledged hello was not replayed"
        );
        complete_handshake(&second, &mut sleep_requests, &second_hello).await;
        second.inbound.send(scripted_assignment_offer());
        let effect_acknowledgement = next_outbound(&mut second.outbound).await;
        assert_effect_acknowledgement(
            &effect_acknowledgement,
            "rmsg_00000000000000000000000003",
            "rbt_00000000000000000000000001",
            2,
        );
        let pending_effect_timer = sleep_request(&mut sleep_requests, Duration::from_secs(2)).await;
        second.inbound.send(gateway_close(
            CloseCode::Normal,
            "disconnect before effect acknowledgement confirmation",
        ));
        drop(pending_effect_timer);
        release_backoff(&mut sleep_requests, &transcript, Duration::from_secs(1)).await;

        let mut third = next_connection(&mut attempts).await;
        let third_hello = next_hello(&mut third).await;
        assert_hello(
            &third_hello,
            "rmsg_00000000000000000000000005",
            "rbt_00000000000000000000000001",
            4,
        );
        assert_ne!(third_hello, second_hello, "acknowledged hello was replayed");
        let third_welcome_timer = sleep_request(&mut sleep_requests, Duration::from_secs(5)).await;
        third.inbound.send(gateway_close(
            CloseCode::Normal,
            "first failure after reset",
        ));
        drop(third_welcome_timer);
        release_backoff(&mut sleep_requests, &transcript, Duration::from_secs(2)).await;

        let mut fourth = next_connection(&mut attempts).await;
        let fourth_hello = next_hello(&mut fourth).await;
        assert_eq!(
            fourth_hello, third_hello,
            "unacknowledged hello was not replayed"
        );
        let fourth_welcome_timer = sleep_request(&mut sleep_requests, Duration::from_secs(5)).await;
        fourth.inbound.send(gateway_close(
            CloseCode::Normal,
            "second failure after reset",
        ));
        drop(fourth_welcome_timer);
        release_backoff(&mut sleep_requests, &transcript, Duration::from_secs(4)).await;

        let mut fifth = next_connection(&mut attempts).await;
        let fifth_hello = next_hello(&mut fifth).await;
        assert_eq!(
            fifth_hello, third_hello,
            "unacknowledged hello was not replayed"
        );
        complete_handshake(&fifth, &mut sleep_requests, &fifth_hello).await;
        fifth.inbound.send(gateway_close(
            CloseCode::Normal,
            "completed handshake resets backoff",
        ));
        let final_backoff =
            request_backoff(&mut sleep_requests, &transcript, Duration::from_secs(1)).await;
        transcript.record("cancellation.released".to_owned());
        cancel.notify_one();
        drop(final_backoff);

        let mut sixth = next_connection(&mut attempts).await;
        let sixth_hello = next_hello(&mut sixth).await;
        assert_hello(
            &sixth_hello,
            "rmsg_00000000000000000000000006",
            "rbt_00000000000000000000000001",
            5,
        );
        complete_handshake(&sixth, &mut sleep_requests, &sixth_hello).await;
        let replayed_preparing = next_outbound(&mut sixth.outbound).await;
        assert_eq!(
            decode_text(&replayed_preparing, "preparation replay after reconnect")["type"],
            "assignment_preparing"
        );
        sixth.inbound.send(assignment_prepare());
        let prepare_acknowledgement = next_outbound(&mut sixth.outbound).await;
        assert_eq!(
            decode_text(&prepare_acknowledgement, "prepare effect acknowledgement")["type"],
            "effect_acknowledged"
        );
        for (preparation_sequence, phase) in [
            (1, "source_materialization"),
            (2, "input_download"),
            (3, "workflow_admission"),
        ] {
            let progress = next_outbound(&mut sixth.outbound).await;
            let progress = decode_text(&progress, "assignment preparation progress");
            assert_eq!(progress["type"], "assignment_preparation_progress");
            assert_eq!(
                progress["payload"]["preparationSequence"],
                preparation_sequence
            );
            assert_eq!(progress["payload"]["phase"], phase);
        }
        let rejection = next_outbound(&mut sixth.outbound).await;
        assert_eq!(
            decode_text(&rejection, "shutdown preparation rejection")["type"],
            "assignment_rejected"
        );
        service_finished
            .await
            .expect("service dropped before shutdown replay completed");
    };

    let (service_result, ()) = tokio::join!(service, peer);
    service_result.expect("scripted cancellation should stop the runner cleanly");
    transcript.record("service.outcome:cancelled".to_owned());
    let events = transcript.snapshot();
    assert_eq!(
        requested_backoff_durations(&events),
        vec![1_000, 1_000, 2_000, 4_000, 1_000]
    );
    assert_eq!(
        released_backoff_durations(&events),
        vec![1_000, 1_000, 2_000, 4_000]
    );
    events
}

async fn run_terminal_close_scenario() -> Vec<String> {
    let transcript = DeterminismTranscript::default();
    let (sleeper, mut sleep_requests) = controlled_sleeper_with_transcript(transcript.clone());
    let (connector, mut attempts) = scripted_connector(transcript.clone());
    let (mut shutdown, _shutdown_trigger) = controlled_shutdown();
    let service = run_deterministic_connection_loop(sleeper, &connector, shutdown.as_mut());
    let peer = async {
        let mut attempt = next_connection(&mut attempts).await;
        let hello = next_hello(&mut attempt).await;
        assert_hello(
            &hello,
            "rmsg_00000000000000000000000002",
            "rbt_00000000000000000000000001",
            1,
        );
        let welcome_timer = sleep_request(&mut sleep_requests, Duration::from_secs(5)).await;
        attempt.inbound.send(gateway_close(
            CloseCode::Policy,
            "gateway rejected runner transport integrity",
        ));
        drop(welcome_timer);
    };
    let (service_result, ()) = tokio::join!(service, peer);
    let error = service_result.expect_err("terminal policy close entered reconnect backoff");
    assert_eq!(
        error.to_string(),
        "runner service stopped unexpectedly: runner gateway connection failed: \
         gateway closed connection with policy violation"
    );
    transcript.record("service.outcome:terminal".to_owned());
    transcript.snapshot()
}

async fn request_backoff(
    sleep_requests: &mut mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
    transcript: &DeterminismTranscript,
    duration: Duration,
) -> SleepRelease {
    let release = sleep_request(sleep_requests, duration).await;
    transcript.record(format!("backoff.requested:{}ms", duration.as_millis()));
    release
}

async fn release_backoff(
    sleep_requests: &mut mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
    transcript: &DeterminismTranscript,
    duration: Duration,
) {
    let release = request_backoff(sleep_requests, transcript, duration).await;
    transcript.record(format!("backoff.released:{}ms", duration.as_millis()));
    release.release();
}

async fn complete_handshake(
    connection: &ScriptedConnection,
    sleep_requests: &mut mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
    hello: &Value,
) {
    let welcome_timer = sleep_request(sleep_requests, Duration::from_secs(5)).await;
    connection.inbound.send(welcome());
    let first_silence_timer = sleep_request(sleep_requests, Duration::from_secs(2)).await;
    connection.inbound.send(observation_acknowledgement(
        hello["messageId"].as_str().expect("opening message ID"),
        hello["sequence"].as_u64().expect("opening sequence"),
    ));
    let second_silence_timer = sleep_request(sleep_requests, Duration::from_secs(2)).await;
    drop(welcome_timer);
    drop(first_silence_timer);
    drop(second_silence_timer);
}

struct EstablishedRuntime {
    config: Config,
    frame_source: Arc<dyn FrameSource>,
    sleeper: Arc<dyn Sleeper>,
    opening: Vec<u8>,
}

impl EstablishedRuntime {
    async fn run(
        &self,
        next_sequence: &mut u64,
        reader: ScriptedReader,
        writer: ScriptedWriter,
    ) -> Result<ConnectionProgress, ConnectionError> {
        let (recorder, _capture) = test_recorder(BOOT_ID);
        let connection_event = recorder.start("runner.fixture_connection", []);
        let active_effect_event = ActiveEffectEvent::new();
        let assignment_manager = Mutex::new(AssignmentManager::new(
            &self.config,
            BOOT_ID.to_owned(),
            fixture_lease_clock(),
        ));
        let result = run_established(
            ConnectionDependencies::new(
                &self.config,
                self.frame_source.as_ref(),
                self.sleeper.as_ref(),
                &recorder,
                &connection_event,
                &active_effect_event,
                &assignment_manager,
                1,
            ),
            opening_frame(&self.opening),
            next_sequence,
            reader,
            writer,
        )
        .await;
        assignment_manager
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .settle_cleanup();
        result
    }
}

struct EstablishedFixture {
    runtime: EstablishedRuntime,
    sleep_requests: mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
    inbound: ScriptedInbound,
    reader: ScriptedReader,
    writer: ScriptedWriter,
    outbound: mpsc::UnboundedReceiver<Message>,
}

fn established_fixture(transcript: &DeterminismTranscript) -> EstablishedFixture {
    let config = deterministic_config();
    let frame_source = deterministic_frame_source();
    let opening = opening_hello(
        frame_source.as_ref(),
        config.credential().runner_id(),
        BOOT_ID,
        OPENING_MESSAGE_ID.to_owned(),
        1,
        "0.2.0",
    )
    .expect("encode deterministic opening hello");
    let (sleeper, sleep_requests) = controlled_sleeper_with_transcript(transcript.clone());
    let (inbound, reader, writer, outbound) = scripted_duplex(transcript.clone());
    EstablishedFixture {
        runtime: EstablishedRuntime {
            config,
            frame_source,
            sleeper,
            opening,
        },
        sleep_requests,
        inbound,
        reader,
        writer,
        outbound,
    }
}

fn deterministic_config() -> Config {
    Config::fixture(
        "ws://127.0.0.1:1/v1/runner/connect",
        test_credential(),
        true,
    )
    .expect("configure deterministic gateway")
}

fn deterministic_connection_loop_dependencies(
    sleeper: Arc<dyn Sleeper>,
) -> ConnectionLoopDependencies {
    let frame_source = deterministic_frame_source();
    let boot_id = frame_source.public_id("rbt_");
    let (recorder, _capture) = test_recorder(&boot_id);
    ConnectionLoopDependencies::new(
        deterministic_config(),
        frame_source,
        sleeper,
        recorder,
        fixture_lease_clock(),
        boot_id,
    )
}

async fn run_deterministic_connection_loop(
    sleeper: Arc<dyn Sleeper>,
    connector: &dyn Connector,
    shutdown: &mut dyn Shutdown,
) -> Result<(), ServiceError> {
    run_connection_loop(
        deterministic_connection_loop_dependencies(sleeper),
        connector,
        Backoff::with_fixed_unit(1.0),
        shutdown,
    )
    .await
}

fn opening_frame(encoded: &[u8]) -> OpeningHello<'_> {
    OpeningHello {
        boot_id: BOOT_ID,
        encoded,
        message_id: OPENING_MESSAGE_ID,
        sequence: 1,
    }
}

async fn next_connection(
    attempts: &mut mpsc::UnboundedReceiver<ScriptedConnection>,
) -> ScriptedConnection {
    attempts
        .recv()
        .await
        .expect("runner service stopped before the expected connection attempt")
}

async fn next_hello(connection: &mut ScriptedConnection) -> Value {
    let message = next_outbound(&mut connection.outbound).await;
    let hello = decode_text(&message, "opening hello");
    assert_eq!(hello["type"], "hello");
    hello
}

async fn next_outbound(outbound: &mut mpsc::UnboundedReceiver<Message>) -> Message {
    outbound
        .recv()
        .await
        .expect("scripted writer closed before the expected event")
}

fn assert_hello(hello: &Value, message_id: &str, boot_id: &str, sequence: u64) {
    assert_eq!(hello["messageId"], message_id);
    assert_eq!(hello["runnerId"], RUNNER_ID);
    assert_eq!(hello["bootId"], boot_id);
    assert_eq!(hello["sequence"], sequence);
    assert_eq!(hello["sentAt"], "2026-07-23T00:00:00Z");
}

fn scripted_assignment_offer() -> Message {
    let offer = assignment_offer();
    let decoded = decode_text(&offer, "assignment offer");
    assert_eq!(decoded["protocolVersion"], 1);
    assert_eq!(decoded["direction"], "cloud_to_runner");
    assert_eq!(decoded["messageId"], "cmsg_01k0z6r1w8f4jy2m7q9v3x5abe");
    assert_eq!(decoded["sentAt"], "2026-07-23T00:00:02Z");
    assert_eq!(decoded["type"], "assignment_offer");
    assert_eq!(decoded["payloadVersion"], 1);
    assert_eq!(decoded["payload"]["effectId"], EFFECT_ID);
    assert_eq!(
        decoded["payload"]["assignmentId"],
        "asn_01k0z6r1w8f4jy2m7q9v3x5abh"
    );
    assert_eq!(
        decoded["payload"]["runId"],
        "run_01k0z6r1w8f4jy2m7q9v3x5abj"
    );
    offer
}

fn assert_effect_acknowledgement(
    message: &Message,
    message_id: &str,
    boot_id: &str,
    sequence: u64,
) -> Value {
    let acknowledgement = decode_text(message, "effect acknowledgement");
    assert_eq!(
        acknowledgement,
        json!({
            "protocolVersion": 1,
            "direction": "runner_to_cloud",
            "messageId": message_id,
            "runnerId": RUNNER_ID,
            "bootId": boot_id,
            "sequence": sequence,
            "sentAt": "2026-07-23T00:00:00Z",
            "type": "effect_acknowledged",
            "payloadVersion": 1,
            "payload": {
                "effectId": EFFECT_ID
            }
        })
    );
    acknowledgement
}

fn decode_text(message: &Message, description: &str) -> Value {
    let Message::Text(text) = message else {
        panic!("{description} was not text");
    };
    serde_json::from_str(text).unwrap_or_else(|error| panic!("decode {description}: {error}"))
}

fn gateway_close(code: CloseCode, reason: &'static str) -> Message {
    Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    }))
}

fn assert_assignment_acknowledgement_order(events: &[String]) {
    let assignment = event_position(
        events,
        0,
        "inbound.read:text:",
        "\"type\":\"assignment_offer\"",
    );
    let acknowledgement = event_position(
        events,
        assignment + 1,
        "outbound:text:",
        "\"type\":\"effect_acknowledged\"",
    );
    let confirmation = event_position(
        events,
        acknowledgement + 1,
        "inbound.read:text:",
        "rmsg_00000000000000000000000001",
    );
    assert!(assignment < acknowledgement && acknowledgement < confirmation);
}

fn event_position(events: &[String], start: usize, prefix: &str, required_fragment: &str) -> usize {
    events
        .iter()
        .enumerate()
        .skip(start)
        .find_map(|(index, event)| {
            (event.starts_with(prefix) && event.contains(required_fragment)).then_some(index)
        })
        .unwrap_or_else(|| {
            panic!("transcript omitted event {prefix} containing {required_fragment}: {events:#?}")
        })
}

fn requested_backoff_durations(events: &[String]) -> Vec<u64> {
    transcript_durations(events, "backoff.requested:")
}

fn released_backoff_durations(events: &[String]) -> Vec<u64> {
    transcript_durations(events, "backoff.released:")
}

fn transcript_durations(events: &[String], prefix: &str) -> Vec<u64> {
    events
        .iter()
        .filter_map(|event| {
            event
                .strip_prefix(prefix)
                .and_then(|duration| duration.strip_suffix("ms"))
                .and_then(|duration| duration.parse().ok())
        })
        .collect()
}

fn assert_stable_transcript(
    expected: &mut Option<Vec<String>>,
    actual: Vec<String>,
    repetition: usize,
) {
    if let Some(expected) = expected {
        assert_eq!(
            &actual,
            expected,
            "runner transcript diverged at repetition {}\nfirst: {expected:#?}\nactual: {actual:#?}",
            repetition + 1,
        );
    } else {
        *expected = Some(actual);
    }
}
