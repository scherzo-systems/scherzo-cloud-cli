use std::future::{Future, pending, ready};
use std::num::NonZeroU64;
use std::ops::Add;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot, watch};

use super::*;

const TEST_WATCHDOG: Duration = Duration::from_secs(10);
const VALIDATION_DEADLINE: Duration = Duration::from_secs(5);
const FEEDBACK_LIMIT: u64 = 8 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TestInstant(Duration);

impl Add<Duration> for TestInstant {
    type Output = Self;

    fn add(self, duration: Duration) -> Self::Output {
        Self(self.0 + duration)
    }
}

#[derive(Clone, Copy)]
struct NeverClock;

impl CoordinatorClock for NeverClock {
    type Instant = TestInstant;

    fn now(&mut self) -> Self::Instant {
        TestInstant(Duration::ZERO)
    }

    fn wait_until(&self, _deadline: Self::Instant) -> impl Future<Output = ()> + Send {
        pending()
    }
}

#[derive(Clone)]
struct InlineWorker {
    starts: Arc<AtomicUsize>,
}

impl InlineWorker {
    fn new() -> Self {
        Self {
            starts: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn starts(&self) -> usize {
        self.starts.load(Ordering::SeqCst)
    }
}

impl ResultValidationWorker for InlineWorker {
    type Running = ReadyValidation;

    fn start(&self, request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            serde_json::from_slice::<Value>(&request.canonical_json).unwrap(),
            *request.candidate
        );
        Ok(ReadyValidation {
            decision: Some(evaluate_candidate(
                &request.schema,
                &request.candidate,
                request.maximum_feedback_bytes.get(),
            )),
        })
    }
}

struct ReadyValidation {
    decision: Option<Result<ValidationWorkerDecision, ()>>,
}

impl RunningResultValidation for ReadyValidation {
    fn wait(&mut self) -> impl Future<Output = Result<ValidationWorkerDecision, ()>> + Send {
        ready(self.decision.take().unwrap())
    }

    fn request_stop(&mut self) {}

    fn quiesce(self) -> impl Future<Output = ()> + Send {
        ready(())
    }
}

#[derive(Clone)]
struct ControlledClock {
    deadline_registrations: mpsc::UnboundedSender<TestInstant>,
    expired: watch::Receiver<bool>,
}

struct ClockControl {
    deadline_registrations: mpsc::UnboundedReceiver<TestInstant>,
    expired: watch::Sender<bool>,
}

impl ControlledClock {
    fn new() -> (Self, ClockControl) {
        let (deadline_registrations, registrations) = mpsc::unbounded_channel();
        let (expired, expiration) = watch::channel(false);
        (
            Self {
                deadline_registrations,
                expired: expiration,
            },
            ClockControl {
                deadline_registrations: registrations,
                expired,
            },
        )
    }
}

impl CoordinatorClock for ControlledClock {
    type Instant = TestInstant;

    fn now(&mut self) -> Self::Instant {
        TestInstant(Duration::from_secs(100))
    }

    fn wait_until(&self, deadline: Self::Instant) -> impl Future<Output = ()> + Send {
        let registrations = self.deadline_registrations.clone();
        let mut expired = self.expired.clone();
        async move {
            let _ = registrations.send(deadline);
            while !*expired.borrow_and_update() {
                if expired.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}

#[derive(Clone)]
struct BlockedWorker {
    started: mpsc::UnboundedSender<BlockedWorkerControl>,
}

struct BlockedWorkerControl {
    decision: Option<oneshot::Sender<Result<ValidationWorkerDecision, ()>>>,
    stopped: oneshot::Receiver<()>,
    quiesce: Option<oneshot::Sender<()>>,
}

impl BlockedWorkerControl {
    async fn wait_until_stopped(&mut self) {
        (&mut self.stopped).await.unwrap();
    }

    fn report_decision(&mut self, decision: Result<ValidationWorkerDecision, ()>) {
        self.decision.take().unwrap().send(decision).unwrap();
    }

    fn report_quiescence(mut self) {
        self.quiesce.take().unwrap().send(()).unwrap();
    }
}

struct BlockedValidation {
    decision: oneshot::Receiver<Result<ValidationWorkerDecision, ()>>,
    stop: Option<oneshot::Sender<()>>,
    quiesced: oneshot::Receiver<()>,
}

fn blocked_worker() -> (BlockedWorker, mpsc::UnboundedReceiver<BlockedWorkerControl>) {
    let (started, controls) = mpsc::unbounded_channel();
    (BlockedWorker { started }, controls)
}

#[derive(Clone)]
struct CancellingWorker {
    cancellation: CancellationSource,
    blocked: BlockedWorker,
}

impl ResultValidationWorker for CancellingWorker {
    type Running = BlockedValidation;

    fn start(&self, request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        assert!(
            self.cancellation
                .request_cancellation(CancellationReason::UserRequest)
        );
        self.blocked.start(request)
    }
}

impl ResultValidationWorker for BlockedWorker {
    type Running = BlockedValidation;

    fn start(&self, _request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        let (decision_guard, decision) = oneshot::channel();
        let (stop, stopped) = oneshot::channel();
        let (quiesce, quiesced) = oneshot::channel();
        self.started
            .send(BlockedWorkerControl {
                decision: Some(decision_guard),
                stopped,
                quiesce: Some(quiesce),
            })
            .map_err(|_| ())?;
        Ok(BlockedValidation {
            decision,
            stop: Some(stop),
            quiesced,
        })
    }
}

impl RunningResultValidation for BlockedValidation {
    async fn wait(&mut self) -> Result<ValidationWorkerDecision, ()> {
        (&mut self.decision).await.map_err(|_| ())?
    }

    fn request_stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }

    async fn quiesce(mut self) {
        let _ = (&mut self.quiesced).await;
    }
}

#[tokio::test]
async fn candidates_are_bounded_rejected_and_corrected_through_one_validator() {
    let schema = retained_schema(json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "count": {"type": "integer", "minimum": 1},
            "name": {"type": "string", "minLength": 3}
        },
        "required": ["count", "name"]
    }));
    let worker = InlineWorker::new();
    let validator = validator(schema, 128, NeverClock, worker.clone());
    let cancellation = CancellationSource::new();

    let oversized = validator
        .validate(Arc::new(json!({"name": "x".repeat(256)})), &cancellation)
        .await;
    assert_eq!(
        oversized,
        ResultValidationOutcome::Decided(ResultValidationDecision::Rejected {
            feedback: Arc::from("Result rejected: canonical JSON exceeds the 128-byte limit.\n")
        })
    );
    assert_eq!(
        worker.starts(),
        0,
        "size rejection must precede schema work"
    );

    let invalid_candidate = Arc::new(json!({"count": 0, "extra": true, "name": "x"}));
    let first_rejection = validator
        .validate(Arc::clone(&invalid_candidate), &cancellation)
        .await;
    let second_rejection = validator.validate(invalid_candidate, &cancellation).await;
    assert_eq!(first_rejection, second_rejection);
    let ResultValidationOutcome::Decided(ResultValidationDecision::Rejected { feedback }) =
        first_rejection
    else {
        panic!("schema-invalid candidate must be rejected");
    };
    assert_eq!(
        feedback.as_ref(),
        concat!(
            "Result rejected by the workflow schema:\n",
            "1. instance /count violates `minimum` at schema /properties/count/minimum\n",
            "2. instance /name violates `minLength` at schema /properties/name/minLength\n",
            "3. instance $ violates `additionalProperties` at schema /additionalProperties\n",
        )
    );

    let accepted = validator
        .validate(Arc::new(json!({"name": "Ada", "count": 1})), &cancellation)
        .await;
    let ResultValidationOutcome::Decided(ResultValidationDecision::Valid(accepted)) = accepted
    else {
        panic!("corrected candidate must be accepted");
    };
    assert_eq!(accepted.value(), &json!({"count": 1, "name": "Ada"}));
    assert_eq!(accepted.canonical_json(), br#"{"count":1,"name":"Ada"}"#);
    assert_eq!(worker.starts(), 3);
}

#[tokio::test]
async fn worker_candidate_limit_accepts_four_mib_and_exact_cap_then_rejects_excess() {
    let schema = retained_schema(json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object"
    }));
    let worker = InlineWorker::new();
    let validator = validator(
        schema,
        MAXIMUM_AGENT_RESULT_BYTES + 1,
        NeverClock,
        worker.clone(),
    );
    let cancellation = CancellationSource::new();
    let json_overhead = u64::try_from(br#"{"payload":""}"#.len()).unwrap();
    let candidate = |bytes: u64| {
        Arc::new(json!({
            "payload": "x".repeat(usize::try_from(bytes - json_overhead).unwrap())
        }))
    };

    for bytes in [4 * 1024 * 1024, MAXIMUM_AGENT_RESULT_BYTES] {
        let accepted = validator.validate(candidate(bytes), &cancellation).await;
        let ResultValidationOutcome::Decided(ResultValidationDecision::Valid(accepted)) = accepted
        else {
            panic!("the four-MiB candidate and exact worker cap must be admitted");
        };
        assert_eq!(
            u64::try_from(accepted.canonical_json().len()).unwrap(),
            bytes
        );
    }

    let oversized = validator
        .validate(candidate(MAXIMUM_AGENT_RESULT_BYTES + 1), &cancellation)
        .await;
    assert_eq!(
        oversized,
        ResultValidationOutcome::Decided(ResultValidationDecision::Rejected {
            feedback: Arc::from(format!(
                "Result rejected: canonical JSON exceeds the {MAXIMUM_AGENT_RESULT_BYTES}-byte limit.\n"
            ))
        })
    );
    assert_eq!(worker.starts(), 2);
}

#[tokio::test]
async fn rejection_feedback_stops_at_sixteen_failures_and_the_byte_bound() {
    let properties = (0..24)
        .map(|index| (format!("p{index:02}"), json!({"type": "integer"})))
        .collect();
    let candidate = Arc::new(Value::Object(
        (0..24)
            .map(|index| (format!("p{index:02}"), json!("wrong")))
            .collect(),
    ));
    let schema = retained_schema(json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "properties": Value::Object(properties)
    }));
    let validator = validator(schema, 4096, NeverClock, InlineWorker::new());
    let cancellation = CancellationSource::new();

    let first = validator
        .validate(Arc::clone(&candidate), &cancellation)
        .await;
    let second = validator.validate(candidate, &cancellation).await;
    assert_eq!(first, second);
    let ResultValidationOutcome::Decided(ResultValidationDecision::Rejected { feedback }) = first
    else {
        panic!("invalid fixture must be rejected");
    };
    assert!(feedback.len() <= usize::try_from(FEEDBACK_LIMIT).unwrap());
    assert_eq!(feedback.lines().skip(1).count(), MAXIMUM_REPORTED_FAILURES);
    assert!(feedback.contains("16. instance /p15"));
    assert!(!feedback.contains("/p16"));
}

#[tokio::test]
async fn rejection_feedback_truncates_at_a_utf8_boundary() {
    let property = "é".repeat(5_000);
    let schema = retained_schema(json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "properties": Value::Object(
            [(property.clone(), json!({"type": "integer"}))]
                .into_iter()
                .collect()
        )
    }));
    let validator = validator(schema, 1024 * 1024, NeverClock, InlineWorker::new());
    let cancellation = CancellationSource::new();
    let candidate = Arc::new(Value::Object(
        [(property, json!("wrong"))].into_iter().collect(),
    ));

    let first = validator
        .validate(Arc::clone(&candidate), &cancellation)
        .await;
    let second = validator.validate(candidate, &cancellation).await;
    assert_eq!(first, second);
    let ResultValidationOutcome::Decided(ResultValidationDecision::Rejected { feedback }) = first
    else {
        panic!("invalid fixture must be rejected");
    };
    assert!(feedback.len() <= usize::try_from(FEEDBACK_LIMIT).unwrap());
    assert!(feedback.len() >= usize::try_from(FEEDBACK_LIMIT).unwrap() - 3);
    assert!(feedback.starts_with("Result rejected by the workflow schema:\n1. instance /"));
}

#[tokio::test]
async fn cancellation_requested_during_worker_start_preempts_validation() {
    with_watchdog(async {
        let (clock, mut clock_control) = ControlledClock::new();
        let (blocked, mut worker_controls) = blocked_worker();
        let cancellation = CancellationSource::new();
        let validator = Arc::new(validator(
            retained_schema(json!({"$schema": JSON_SCHEMA_DIALECT})),
            1024,
            clock,
            CancellingWorker {
                cancellation: cancellation.clone(),
                blocked,
            },
        ));
        let validation = {
            let validator = Arc::clone(&validator);
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                validator
                    .validate(Arc::new(json!({"candidate": true})), &cancellation)
                    .await
            })
        };

        let mut worker_control = worker_controls.recv().await.unwrap();
        #[derive(Debug, Eq, PartialEq)]
        enum FirstSignal {
            WorkerStopped,
            DeadlineRegistered,
        }
        let first_signal = tokio::select! {
            biased;
            stopped = &mut worker_control.stopped => {
                stopped.unwrap();
                FirstSignal::WorkerStopped
            }
            registration = clock_control.deadline_registrations.recv() => {
                assert_eq!(
                    registration,
                    Some(TestInstant(Duration::from_secs(105)))
                );
                FirstSignal::DeadlineRegistered
            }
        };

        if first_signal == FirstSignal::DeadlineRegistered {
            clock_control.expired.send_replace(true);
            worker_control.wait_until_stopped().await;
        }
        worker_control.report_quiescence();
        assert_eq!(
            validation.await.unwrap(),
            ResultValidationOutcome::Cancelled {
                reason: CancellationReason::UserRequest
            }
        );
        assert_eq!(
            first_signal,
            FirstSignal::WorkerStopped,
            "cancellation must stop validation before its deadline is polled"
        );
    })
    .await;
}

#[tokio::test]
async fn a_decision_completed_after_deadline_exhaustion_is_not_accepted() {
    with_watchdog(async {
        let (clock, mut clock_control) = ControlledClock::new();
        let (worker, mut worker_controls) = blocked_worker();
        let validator = Arc::new(validator(
            retained_schema(json!({"$schema": JSON_SCHEMA_DIALECT})),
            1024,
            clock,
            worker,
        ));
        let cancellation = CancellationSource::new();
        let validation = {
            let validator = Arc::clone(&validator);
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                validator
                    .validate(Arc::new(json!({"candidate": true})), &cancellation)
                    .await
            })
        };

        let mut worker_control = worker_controls.recv().await.unwrap();
        assert_eq!(
            clock_control.deadline_registrations.recv().await,
            Some(TestInstant(Duration::from_secs(105)))
        );
        clock_control.expired.send_replace(true);
        worker_control.report_decision(Ok(ValidationWorkerDecision::Valid));
        worker_control.report_quiescence();

        assert_eq!(
            validation.await.unwrap(),
            ResultValidationOutcome::Decided(ResultValidationDecision::Fatal(
                ResultValidationFatal::LimitExceeded {
                    deadline: positive_duration(VALIDATION_DEADLINE)
                }
            ))
        );
    })
    .await;
}

#[tokio::test]
async fn deadline_exhaustion_stops_and_quiesces_the_worker_before_returning() {
    with_watchdog(async {
        let (clock, mut clock_control) = ControlledClock::new();
        let (worker, mut worker_controls) = blocked_worker();
        let validator = Arc::new(validator(
            retained_schema(json!({"$schema": JSON_SCHEMA_DIALECT})),
            1024,
            clock,
            worker,
        ));
        let cancellation = CancellationSource::new();
        let validation = {
            let validator = Arc::clone(&validator);
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                validator
                    .validate(Arc::new(json!({"candidate": true})), &cancellation)
                    .await
            })
        };

        let mut worker_control = worker_controls.recv().await.unwrap();
        assert_eq!(
            clock_control.deadline_registrations.recv().await,
            Some(TestInstant(Duration::from_secs(105)))
        );
        clock_control.expired.send_replace(true);
        worker_control.wait_until_stopped().await;
        assert!(
            !validation.is_finished(),
            "fatal exhaustion must wait for worker quiescence"
        );
        worker_control.report_quiescence();
        assert_eq!(
            validation.await.unwrap(),
            ResultValidationOutcome::Decided(ResultValidationDecision::Fatal(
                ResultValidationFatal::LimitExceeded {
                    deadline: positive_duration(VALIDATION_DEADLINE)
                }
            ))
        );
    })
    .await;
}

#[tokio::test]
async fn cancellation_preempts_blocked_validation_and_waits_for_quiescence() {
    with_watchdog(async {
        let (clock, mut clock_control) = ControlledClock::new();
        let (worker, mut worker_controls) = blocked_worker();
        let validator = Arc::new(validator(
            retained_schema(json!({"$schema": JSON_SCHEMA_DIALECT})),
            1024,
            clock,
            worker,
        ));
        let cancellation = CancellationSource::new();
        let validation = {
            let validator = Arc::clone(&validator);
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                validator
                    .validate(Arc::new(json!({"candidate": true})), &cancellation)
                    .await
            })
        };

        let mut worker_control = worker_controls.recv().await.unwrap();
        assert_eq!(
            clock_control.deadline_registrations.recv().await,
            Some(TestInstant(Duration::from_secs(105)))
        );
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        worker_control.wait_until_stopped().await;
        clock_control.expired.send_replace(true);
        assert!(
            !validation.is_finished(),
            "cancellation must wait for worker quiescence"
        );
        worker_control.report_quiescence();
        assert_eq!(
            validation.await.unwrap(),
            ResultValidationOutcome::Cancelled {
                reason: CancellationReason::UserRequest
            }
        );
    })
    .await;
}

#[test]
fn internal_worker_uses_the_supported_validator_and_bounded_feedback() {
    let schema = serde_json::to_vec(&json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "string",
        "pattern": "^(a+)+$"
    }))
    .unwrap();
    let candidate = br#""aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!""#;
    let mut request = Vec::new();
    write_frame(&mut request, &schema);
    write_frame(&mut request, candidate);
    request.extend_from_slice(&FEEDBACK_LIMIT.to_be_bytes());
    let mut response = Vec::new();

    run_internal_worker_io(&mut request.as_slice(), &mut response).unwrap();

    assert!(matches!(
        serde_json::from_slice::<WorkerResponse>(&response).unwrap(),
        WorkerResponse::Rejected { feedback }
            if feedback.contains("violates `pattern`") && feedback.len() <= 8 * 1024
    ));
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is allowed only as an anti-hang watchdog, not a behavior assertion"
)]
async fn with_watchdog<Output>(future: impl Future<Output = Output>) -> Output {
    match tokio::time::timeout(TEST_WATCHDOG, future).await {
        Ok(output) => output,
        Err(_) => panic!("result validation test watchdog expired"),
    }
}

fn retained_schema(document: Value) -> RetainedResultSchema {
    let bytes = Arc::<[u8]>::from(serde_json::to_vec(&document).unwrap());
    RetainedResultSchema::compile(bytes, Arc::new(document)).unwrap()
}

fn validator<Clock, Worker>(
    schema: RetainedResultSchema,
    maximum_candidate_bytes: u64,
    clock: Clock,
    worker: Worker,
) -> AuthoritativeResultValidator<Clock, Worker>
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
{
    AuthoritativeResultValidator::new(
        schema,
        NonZeroU64::new(maximum_candidate_bytes).unwrap(),
        NonZeroU64::new(FEEDBACK_LIMIT).unwrap(),
        positive_duration(VALIDATION_DEADLINE),
        clock,
        worker,
    )
}

fn positive_duration(duration: Duration) -> PositiveDuration {
    PositiveDuration::new(duration).unwrap()
}

fn write_frame(destination: &mut Vec<u8>, bytes: &[u8]) {
    destination.extend_from_slice(&u64::try_from(bytes.len()).unwrap().to_be_bytes());
    destination.extend_from_slice(bytes);
}
