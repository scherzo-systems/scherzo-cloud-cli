use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use opentelemetry::propagation::{Injector, TextMapPropagator};
use opentelemetry::trace::{SpanKind, Status, TraceContextExt, Tracer, TracerProvider};
use opentelemetry::{Context, KeyValue, Value as AttributeValue};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use serde_json::{Map, Number, Value};

mod otlp;
pub(crate) mod attribute {
    pub(crate) const BACKOFF_MS: &str = "scherzo.connection.backoff_ms";
    pub(crate) const CONNECTION_ATTEMPT: &str = "scherzo.connection.attempt";
    pub(crate) const EFFECT_ACKNOWLEDGEMENTS_CONFIRMED: &str =
        "scherzo.runner.effect_acknowledgements_confirmed";
    pub(crate) const EFFECT_ID: &str = "scherzo.effect.id";
    pub(crate) const EFFECTS_RECEIVED: &str = "scherzo.runner.effects_received";
    pub(crate) const ERROR_TYPE: &str = "error.type";
    pub(crate) const FAILURE_KIND: &str = "scherzo.connection.failure_kind";
    pub(crate) const HANDSHAKE_COMPLETED: &str = "scherzo.runner.handshake_completed";
    pub(crate) const OPENING_ACKNOWLEDGED: &str = "scherzo.runner.opening_acknowledged";
    pub(crate) const RUN_ID: &str = "scherzo.run.id";
    pub(crate) const RUNNER_BOOT_ID: &str = "scherzo.runner.boot_id";
    pub(crate) const RUNNER_ID: &str = "scherzo.runner.id";
    pub(crate) const RUNNER_SEQUENCE: &str = "scherzo.runner.sequence";
    pub(crate) const RUNNER_TEXT_FRAMES_SENT: &str = "scherzo.runner.text_frames_sent";
    pub(crate) const RUNNER_VERSION: &str = "scherzo.runner.version";
    pub(crate) const CLOUD_TEXT_FRAMES_RECEIVED: &str = "scherzo.cloud.text_frames_received";
    pub(crate) const SERVER_ADDRESS: &str = "server.address";
    pub(crate) const SERVER_PORT: &str = "server.port";
    pub(crate) const ASSIGNMENT_ID: &str = "scherzo.assignment.id";
    pub(crate) const PROTOCOL_ACKNOWLEDGED_MESSAGE_ID: &str =
        "scherzo.protocol.acknowledged_message_id";
    pub(crate) const PROTOCOL_ACKNOWLEDGED_SEQUENCE: &str =
        "scherzo.protocol.acknowledged_sequence";
    pub(crate) const PROTOCOL_CLOSE_CODE: &str = "scherzo.protocol.close_code";
    pub(crate) const PROTOCOL_CLOSE_INITIATOR: &str = "scherzo.protocol.close_initiator";
    pub(crate) const PROTOCOL_DIRECTION: &str = "scherzo.protocol.direction";
    pub(crate) const PROTOCOL_EVENT: &str = "scherzo.protocol.event";
    pub(crate) const PROTOCOL_FRAME_KIND: &str = "scherzo.protocol.frame_kind";
    pub(crate) const PROTOCOL_FRAME_TYPE: &str = "scherzo.protocol.frame_type";
    pub(crate) const PROTOCOL_LEASE_EXPIRES_AT: &str = "scherzo.protocol.lease_expires_at";
    pub(crate) const PROTOCOL_MESSAGE_ID: &str = "scherzo.protocol.message_id";
    pub(crate) const PROTOCOL_ORDER: &str = "scherzo.protocol.order";
    pub(crate) const PROTOCOL_PAYLOAD_VERSION: &str = "scherzo.protocol.payload_version";
    pub(crate) const PROTOCOL_PING_INTERVAL_SECONDS: &str =
        "scherzo.protocol.ping_interval_seconds";
    pub(crate) const PROTOCOL_PONG_TIMEOUT_SECONDS: &str = "scherzo.protocol.pong_timeout_seconds";
    pub(crate) const PROTOCOL_SENT_AT: &str = "scherzo.protocol.sent_at";
    pub(crate) const PROTOCOL_TIMER: &str = "scherzo.protocol.timer";
    pub(crate) const PROTOCOL_VERSION: &str = "scherzo.protocol.version";
    pub(crate) const RUNNER_MAX_CONCURRENT_RUNS: &str = "scherzo.runner.max_concurrent_runs";
    pub(crate) const RUNNER_SESSION_ID: &str = "scherzo.runner.session_id";
}

const EVENT_NAME: &str = "event.name";
const MAIN: &str = "scherzo.main";
const SCHEMA_VERSION: &str = "scherzo.event.schema_version";
const OUTCOME: &str = "scherzo.outcome";
const SERVICE_NAME: &str = "service.name";
const SERVICE_VERSION: &str = "service.version";
const SERVICE_INSTANCE_ID: &str = "service.instance.id";
const DROPPED_COUNT: &str = "scherzo.telemetry.dropped_count";
const DURATION_MS: &str = "duration_ms";
const TRACE_ID: &str = "trace_id";
const SPAN_ID: &str = "span_id";
const EVENT_QUEUE_CAPACITY: usize = 128;
const EVENT_QUEUE_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
    Success,
    Failure,
    Cancelled,
    Timeout,
    Disconnected,
}

impl Outcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::Disconnected => "disconnected",
        }
    }

    const fn is_error(self) -> bool {
        matches!(self, Self::Failure | Self::Timeout)
    }
}

pub(crate) trait EventWriter: Send + Sync {
    fn write(&self, event: &Map<String, Value>) -> io::Result<()>;
}

struct QueuedRecord {
    bytes: Vec<u8>,
    represented_dropped_count: u64,
}

enum QueueMessage {
    Record(QueuedRecord),
    Drain(SyncSender<()>),
}

struct QueuedWriter {
    sender: Option<SyncSender<QueueMessage>>,
}

impl QueuedWriter {
    fn stderr(dropped_count: Arc<AtomicU64>) -> Self {
        let (sender, receiver) = sync_channel::<QueueMessage>(EVENT_QUEUE_CAPACITY);
        let worker = thread::Builder::new()
            .name("scherzo-runner-events".to_owned())
            .spawn(move || {
                let mut stderr = FramedWriter::new(io::stderr());
                while let Ok(message) = receiver.recv() {
                    match message {
                        QueueMessage::Record(record) => {
                            if stderr.write_record(&record.bytes).is_err() {
                                dropped_count.fetch_add(
                                    record.represented_dropped_count.saturating_add(1),
                                    Ordering::AcqRel,
                                );
                            }
                        }
                        QueueMessage::Drain(completed) => {
                            let _ = completed.send(());
                            break;
                        }
                    }
                }
            });
        Self {
            sender: worker.ok().map(|_| sender),
        }
    }

    #[cfg(test)]
    fn from_sender(sender: SyncSender<QueueMessage>) -> Self {
        Self {
            sender: Some(sender),
        }
    }
}

impl EventWriter for QueuedWriter {
    fn write(&self, event: &Map<String, Value>) -> io::Result<()> {
        let mut bytes = serde_json::to_vec(event)?;
        bytes.push(b'\n');
        let record = QueuedRecord {
            bytes,
            represented_dropped_count: event
                .get(DROPPED_COUNT)
                .and_then(Value::as_u64)
                .unwrap_or(0),
        };
        let Some(sender) = &self.sender else {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "runner event writer unavailable",
            ));
        };
        sender
            .try_send(QueueMessage::Record(record))
            .map_err(|error| match error {
                TrySendError::Full(_) => {
                    io::Error::new(io::ErrorKind::WouldBlock, "runner event queue is full")
                }
                TrySendError::Disconnected(_) => {
                    io::Error::new(io::ErrorKind::BrokenPipe, "runner event writer stopped")
                }
            })
    }
}

impl Drop for QueuedWriter {
    fn drop(&mut self) {
        let Some(sender) = self.sender.take() else {
            return;
        };
        let (completed, completion) = sync_channel(0);
        if sender.try_send(QueueMessage::Drain(completed)).is_ok() {
            let _ = completion.recv_timeout(EVENT_QUEUE_DRAIN_TIMEOUT);
        }
    }
}

struct FramedWriter<W> {
    output: W,
    separator_required: bool,
}

impl<W: Write> FramedWriter<W> {
    fn new(output: W) -> Self {
        Self {
            output,
            separator_required: false,
        }
    }

    fn write_record(&mut self, record: &[u8]) -> io::Result<()> {
        if self.separator_required {
            self.output.write_all(b"\n")?;
            self.separator_required = false;
        }
        if let Err(error) = self.output.write_all(record) {
            self.separator_required = true;
            return Err(error);
        }
        Ok(())
    }
}

pub(crate) struct Recorder {
    provider: SdkTracerProvider,
    tracer: SdkTracer,
    writer: Arc<dyn EventWriter>,
    service_version: Arc<str>,
    service_instance_id: Arc<str>,
    dropped_count: Arc<AtomicU64>,
}

impl fmt::Debug for Recorder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Recorder { .. }")
    }
}

impl Recorder {
    pub(crate) fn stderr(service_instance_id: &str) -> Arc<Self> {
        let service_version = crate::build_info::VERSION;
        let dropped_count = Arc::new(AtomicU64::new(0));
        let writer: Arc<dyn EventWriter> =
            Arc::new(QueuedWriter::stderr(Arc::clone(&dropped_count)));
        let mut provider = SdkTracerProvider::builder();
        if let Some(processor) = otlp::configured_processor(Arc::clone(&writer)) {
            provider = provider.with_span_processor(processor);
        }
        let resource = runner_resource(service_version, service_instance_id);
        let provider = provider.with_resource(resource).build();
        Arc::new(Self::with_dropped_count(
            provider,
            writer,
            service_version,
            service_instance_id,
            dropped_count,
        ))
    }

    #[cfg(test)]
    fn new(
        provider: SdkTracerProvider,
        writer: Arc<dyn EventWriter>,
        service_version: &str,
        service_instance_id: &str,
    ) -> Self {
        Self::with_dropped_count(
            provider,
            writer,
            service_version,
            service_instance_id,
            Arc::new(AtomicU64::new(0)),
        )
    }

    fn with_dropped_count(
        provider: SdkTracerProvider,
        writer: Arc<dyn EventWriter>,
        service_version: &str,
        service_instance_id: &str,
        dropped_count: Arc<AtomicU64>,
    ) -> Self {
        let tracer = provider.tracer("scherzo-runner");
        Self {
            provider,
            tracer,
            writer,
            service_version: Arc::from(service_version),
            service_instance_id: Arc::from(service_instance_id),
            dropped_count,
        }
    }

    fn common_attributes(&self, main: bool) -> [KeyValue; 5] {
        [
            KeyValue::new(MAIN, main),
            KeyValue::new(SCHEMA_VERSION, 1_i64),
            KeyValue::new(SERVICE_NAME, "scherzo-runner"),
            KeyValue::new(SERVICE_VERSION, Arc::clone(&self.service_version)),
            KeyValue::new(SERVICE_INSTANCE_ID, Arc::clone(&self.service_instance_id)),
        ]
    }

    pub(crate) fn start(
        &self,
        name: &'static str,
        attributes: impl IntoIterator<Item = KeyValue>,
    ) -> Event {
        let mut fields = BTreeMap::new();
        let mut span_attributes = Vec::new();
        for attribute in self.common_attributes(true).into_iter().chain(attributes) {
            if let Some(value) = json_value(&attribute.value) {
                fields.insert(attribute.key.as_str().to_owned(), value);
                span_attributes.push(attribute);
            }
        }
        fields.insert(EVENT_NAME.to_owned(), Value::String(name.to_owned()));

        let span = self
            .tracer
            .span_builder(name)
            .with_kind(SpanKind::Internal)
            .with_attributes(span_attributes)
            .start_with_context(&self.tracer, &Context::new());
        Event {
            inner: Arc::new(EventInner {
                context: Context::new().with_span(span),
                writer: Arc::clone(&self.writer),
                dropped_count: Arc::clone(&self.dropped_count),
                started_at: crate::timing::monotonic_now(),
                state: Mutex::new(EventState {
                    fields,
                    completed: false,
                }),
                name,
            }),
        }
    }

    // record writes one instantaneous, reviewed diagnostic without creating a
    // main event or span. Protocol call sites pass decoded fields only; this
    // interface must never receive credentials, raw frames, or peer reasons.
    pub(crate) fn record(
        &self,
        name: &'static str,
        attributes: impl IntoIterator<Item = KeyValue>,
    ) {
        let mut fields = Map::new();
        for attribute in self.common_attributes(false).into_iter().chain(attributes) {
            if let Some(value) = json_value(&attribute.value) {
                fields.insert(attribute.key.as_str().to_owned(), value);
            }
        }
        fields.insert(EVENT_NAME.to_owned(), Value::String(name.to_owned()));
        if self.writer.write(&fields).is_err() {
            self.dropped_count.fetch_add(1, Ordering::AcqRel);
        }
    }
}

fn runner_resource(service_version: &str, service_instance_id: &str) -> Resource {
    Resource::builder_empty()
        .with_attributes([
            KeyValue::new(SERVICE_NAME, "scherzo-runner"),
            KeyValue::new(SERVICE_VERSION, service_version.to_owned()),
            KeyValue::new(SERVICE_INSTANCE_ID, service_instance_id.to_owned()),
        ])
        .build()
}

#[derive(Clone)]
pub(crate) struct Event {
    inner: Arc<EventInner>,
}

struct EventInner {
    context: Context,
    writer: Arc<dyn EventWriter>,
    dropped_count: Arc<AtomicU64>,
    started_at: Instant,
    state: Mutex<EventState>,
    name: &'static str,
}

struct EventState {
    fields: BTreeMap<String, Value>,
    completed: bool,
}

impl fmt::Debug for Event {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Event")
            .field("name", &self.inner.name)
            .finish_non_exhaustive()
    }
}

impl Event {
    pub(crate) fn inject_trace_context(&self, injector: &mut dyn Injector) {
        TraceContextPropagator::new().inject_context(&self.inner.context, injector);
    }

    pub(crate) fn set(&self, attribute: KeyValue) {
        let Some(value) = json_value(&attribute.value) else {
            return;
        };
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.completed {
            return;
        }
        state
            .fields
            .insert(attribute.key.as_str().to_owned(), value);
        self.inner.context.span().set_attribute(attribute);
    }

    pub(crate) fn finish(&self, outcome: Outcome) {
        let (event, prior_dropped_count) = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.completed {
                return;
            }
            state.completed = true;

            let outcome_attribute = KeyValue::new(OUTCOME, outcome.as_str());
            state.fields.insert(
                OUTCOME.to_owned(),
                Value::String(outcome.as_str().to_owned()),
            );
            let duration_ms =
                integer_u128(crate::timing::elapsed(self.inner.started_at).as_millis());
            state
                .fields
                .insert(DURATION_MS.to_owned(), Value::Number(duration_ms.into()));
            let span = self.inner.context.span();
            let prior_dropped_count = self.inner.dropped_count.swap(0, Ordering::AcqRel);
            if prior_dropped_count > 0 {
                let count = integer(prior_dropped_count);
                state
                    .fields
                    .insert(DROPPED_COUNT.to_owned(), Value::Number(count.into()));
                span.set_attribute(KeyValue::new(DROPPED_COUNT, count));
            }
            span.set_attribute(outcome_attribute);
            if outcome.is_error() {
                span.set_status(Status::error(outcome.as_str()));
            }
            let span_context = span.span_context();
            if span_context.is_valid() {
                state.fields.insert(
                    TRACE_ID.to_owned(),
                    Value::String(span_context.trace_id().to_string()),
                );
                state.fields.insert(
                    SPAN_ID.to_owned(),
                    Value::String(span_context.span_id().to_string()),
                );
            }
            span.end();

            let event = state
                .fields
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Map<_, _>>();
            (event, prior_dropped_count)
        };

        if self.inner.writer.write(&event).is_err() {
            self.inner
                .dropped_count
                .fetch_add(prior_dropped_count.saturating_add(1), Ordering::AcqRel);
        }
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        let _ = self
            .provider
            .shutdown_with_timeout(otlp::MAX_SHUTDOWN_TIMEOUT);
    }
}

fn json_value(value: &AttributeValue) -> Option<Value> {
    match value {
        AttributeValue::Bool(value) => Some(Value::Bool(*value)),
        AttributeValue::I64(value) => Some(Value::Number((*value).into())),
        AttributeValue::F64(value) => Number::from_f64(*value).map(Value::Number),
        AttributeValue::String(value) => Some(Value::String(value.as_str().to_owned())),
        AttributeValue::Array(_) => None,
        _ => None,
    }
}

pub(crate) fn integer(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(crate) fn integer_u128(value: u128) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct TestCapture {
    events: Arc<Mutex<Vec<Map<String, Value>>>>,
    spans: Arc<Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>,
}

#[cfg(test)]
impl TestCapture {
    pub(crate) fn records(&self) -> Vec<Map<String, Value>> {
        self.events
            .lock()
            .expect("event capture mutex poisoned")
            .clone()
    }

    pub(crate) fn events(&self) -> Vec<Map<String, Value>> {
        self.records()
            .into_iter()
            .filter(|event| event.get(MAIN).and_then(Value::as_bool) == Some(true))
            .collect()
    }

    pub(crate) fn spans(&self) -> Vec<opentelemetry_sdk::trace::SpanData> {
        self.spans
            .lock()
            .expect("span capture mutex poisoned")
            .clone()
    }

    pub(crate) fn event(&self, name: &str) -> Map<String, Value> {
        self.events()
            .into_iter()
            .find(|event| event[EVENT_NAME] == name)
            .expect("captured event should exist")
    }

    pub(crate) fn span_count(&self, name: &str) -> usize {
        self.spans
            .lock()
            .expect("span capture mutex poisoned")
            .iter()
            .filter(|span| &*span.name == name)
            .count()
    }
}

#[cfg(test)]
impl EventWriter for TestCapture {
    fn write(&self, event: &Map<String, Value>) -> io::Result<()> {
        self.events
            .lock()
            .expect("event capture mutex poisoned")
            .push(event.clone());
        Ok(())
    }
}

#[cfg(test)]
impl opentelemetry_sdk::trace::SpanExporter for TestCapture {
    async fn export(
        &self,
        spans: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.spans
            .lock()
            .expect("span capture mutex poisoned")
            .extend(spans);
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn test_recorder(service_instance_id: &str) -> (Arc<Recorder>, TestCapture) {
    let capture = TestCapture::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(capture.clone())
        .build();
    let recorder = Recorder::new(
        provider,
        Arc::new(capture.clone()),
        crate::build_info::VERSION,
        service_instance_id,
    );
    (Arc::new(recorder), capture)
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::sync_channel;
    use std::thread;

    use opentelemetry::KeyValue;
    use serde_json::Value;

    use super::{
        DROPPED_COUNT, EventWriter, FramedWriter, Outcome, QueuedWriter, Recorder, TestCapture,
        test_recorder,
    };

    #[test]
    fn projects_scalar_attributes_to_one_json_event_and_span() {
        let (recorder, capture) = test_recorder("rbt_fixture");
        let event = recorder.start(
            "runner.fixture",
            [
                KeyValue::new("fixture.bool", true),
                KeyValue::new("fixture.integer", 42_i64),
                KeyValue::new("fixture.float", 1.5_f64),
                KeyValue::new("fixture.string", "line one\nline two"),
            ],
        );
        event.finish(Outcome::Success);
        event.finish(Outcome::Failure);
        drop(event);

        let events = capture.events();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event["event.name"], "runner.fixture");
        assert_eq!(event["fixture.bool"], true);
        assert_eq!(event["fixture.integer"], 42);
        assert_eq!(event["fixture.float"], 1.5);
        assert_eq!(event["fixture.string"], "line one\nline two");
        assert_eq!(event["scherzo.outcome"], "success");
        assert!(event["duration_ms"].is_i64());
        assert!(event["trace_id"].is_string());
        assert!(event["span_id"].is_string());
        let encoded = serde_json::to_string(event).expect("encode captured event");
        assert!(!encoded.contains('\n'));

        let spans = capture.spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(&*spans[0].name, "runner.fixture");
        assert_eq!(spans[0].status, opentelemetry::trace::Status::Unset);
        let span_attribute = |name: &str| {
            spans[0]
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == name)
                .map(|attribute| &attribute.value)
        };
        assert_eq!(
            span_attribute("scherzo.main"),
            Some(&opentelemetry::Value::Bool(true))
        );
        assert_eq!(
            span_attribute("service.name"),
            Some(&opentelemetry::Value::String("scherzo-runner".into()))
        );
        assert_eq!(
            span_attribute("fixture.integer"),
            Some(&opentelemetry::Value::I64(42))
        );
        assert_eq!(
            span_attribute("scherzo.outcome"),
            Some(&opentelemetry::Value::String("success".into()))
        );
    }

    #[test]
    fn concurrent_enrichment_is_complete_and_duplicate_finish_is_ignored() {
        let (recorder, capture) = test_recorder("rbt_fixture");
        let event = recorder.start("runner.concurrent", []);
        let mut workers = Vec::new();
        for index in 0..20 {
            let event = event.clone();
            workers.push(thread::spawn(move || {
                event.set(KeyValue::new(
                    format!("fixture.field_{index}"),
                    index as i64,
                ));
            }));
        }
        for worker in workers {
            worker.join().expect("join event worker");
        }
        event.finish(Outcome::Failure);
        event.finish(Outcome::Success);

        let events = capture.events();
        assert_eq!(events.len(), 1);
        for index in 0..20 {
            assert_eq!(events[0][&format!("fixture.field_{index}")], index as i64);
        }
        assert_eq!(events[0]["scherzo.outcome"], "failure");
        let spans = capture.spans();
        assert_eq!(spans.len(), 1);
        assert!(matches!(
            spans[0].status,
            opentelemetry::trace::Status::Error { .. }
        ));
    }

    #[test]
    fn writer_failure_is_nonfatal_and_reported_by_the_next_event() {
        #[derive(Debug)]
        struct FailOnceWriter {
            failed: AtomicBool,
            captured: TestCapture,
        }

        impl EventWriter for FailOnceWriter {
            fn write(&self, event: &serde_json::Map<String, Value>) -> std::io::Result<()> {
                if !self.failed.swap(true, Ordering::AcqRel) {
                    return Err(std::io::Error::other("synthetic writer failure"));
                }
                self.captured.write(event)
            }
        }

        let captured = TestCapture::default();
        let writer = Arc::new(FailOnceWriter {
            failed: AtomicBool::new(false),
            captured: captured.clone(),
        });
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let recorder = Recorder::new(provider, writer, "0.2.0-test", "rbt_fixture");
        let dropped = recorder.start("runner.dropped", []);
        let recovered = recorder.start("runner.recovered", []);
        dropped.finish(Outcome::Failure);
        recovered.finish(Outcome::Success);

        let events = captured.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0][DROPPED_COUNT], 1);
    }

    #[test]
    fn a_full_output_queue_drops_without_waiting() {
        let (sender, _receiver) = sync_channel(1);
        let writer = QueuedWriter::from_sender(sender);
        let event = serde_json::Map::new();

        writer.write(&event).expect("fill event queue");
        let error = writer
            .write(&event)
            .expect_err("full queue should drop event");

        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn a_partial_write_is_terminated_before_the_next_record() {
        #[derive(Default)]
        struct PartialThenRecover {
            calls: usize,
            output: Vec<u8>,
        }

        impl Write for PartialThenRecover {
            fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
                self.calls += 1;
                match self.calls {
                    1 => {
                        let written = buffer.len().min(4);
                        self.output.extend_from_slice(&buffer[..written]);
                        Ok(written)
                    }
                    2 => Err(io::Error::other("synthetic partial write failure")),
                    _ => {
                        self.output.extend_from_slice(buffer);
                        Ok(buffer.len())
                    }
                }
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut writer = FramedWriter::new(PartialThenRecover::default());
        writer
            .write_record(b"{\"broken\":true}\n")
            .expect_err("first record should fail after a partial write");
        writer
            .write_record(b"{\"event.name\":\"runner.recovered\"}\n")
            .expect("write recovery record");

        let lines: Vec<_> = writer.output.output.split(|byte| *byte == b'\n').collect();
        assert_eq!(lines.len(), 3);
        assert!(serde_json::from_slice::<Value>(lines[0]).is_err());
        let recovered: Value = serde_json::from_slice(lines[1]).expect("decode recovery record");
        assert_eq!(recovered["event.name"], "runner.recovered");
    }

    #[test]
    fn debug_output_does_not_include_service_or_attribute_values() {
        let (recorder, _capture) = test_recorder("INSTANCE-MUST-NOT-APPEAR");
        let event = recorder.start(
            "runner.redacted",
            [KeyValue::new("fixture.secret", "SECRET-MUST-NOT-APPEAR")],
        );

        let recorder_debug = format!("{recorder:?}");
        let event_debug = format!("{event:?}");
        assert!(!recorder_debug.contains("INSTANCE-MUST-NOT-APPEAR"));
        assert!(!event_debug.contains("SECRET-MUST-NOT-APPEAR"));
        assert_eq!(recorder_debug, "Recorder { .. }");
        assert!(event_debug.contains("runner.redacted"));
    }
}
