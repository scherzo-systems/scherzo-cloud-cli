use std::ffi::OsString;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{Span, SpanData, SpanProcessor};
use prost::Message as _;
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use url::Url;

use super::EventWriter;

const SDK_DISABLED: &str = "OTEL_SDK_DISABLED";
const TRACES_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
const ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const TRACES_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL";
const PROTOCOL: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";
const TRACES_HEADERS: &str = "OTEL_EXPORTER_OTLP_TRACES_HEADERS";
const HEADERS: &str = "OTEL_EXPORTER_OTLP_HEADERS";
const TRACES_TIMEOUT: &str = "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT";
const TIMEOUT: &str = "OTEL_EXPORTER_OTLP_TIMEOUT";
const HTTP_PROTOBUF: &str = "http/protobuf";

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_REQUEST_TIMEOUT_MS: u64 = 30_000;
const EXPORT_QUEUE_CAPACITY: usize = 128;
const MAX_EXPORT_BATCH_SIZE: usize = 32;
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_HEADER_COUNT: usize = 64;
const MAX_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(250);

const DIAGNOSTIC_NAME: &str = "runner.telemetry_export";
const DIAGNOSTIC_NAME_FIELD: &str = "diagnostic.name";
const DIAGNOSTIC_CLASSIFICATION_FIELD: &str = "diagnostic.classification";

pub(super) fn configured_processor(writer: Arc<dyn EventWriter>) -> Option<ExportSpanProcessor> {
    let diagnostics = DiagnosticReporter::new(writer);
    let settings = match ExportSettings::from_lookup(|name| std::env::var_os(name)) {
        SettingsOutcome::Disabled => return None,
        SettingsOutcome::Invalid(classification) => {
            diagnostics.report(classification);
            return None;
        }
        SettingsOutcome::Enabled(settings) => settings,
    };

    match OtlpHttpExporter::new(settings) {
        Ok(exporter) => Some(ExportSpanProcessor::new(exporter, diagnostics)),
        Err(()) => {
            diagnostics.report(DiagnosticClassification::ExporterUnavailable);
            None
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiagnosticClassification {
    PrivacyInvalid,
    EndpointInvalid,
    ProtocolUnsupported,
    HeadersInvalid,
    TimeoutInvalid,
    ExporterUnavailable,
    RequestFailed,
    QueueSaturated,
    ShutdownTimeout,
}

impl DiagnosticClassification {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PrivacyInvalid => "privacy_invalid",
            Self::EndpointInvalid => "endpoint_invalid",
            Self::ProtocolUnsupported => "protocol_unsupported",
            Self::HeadersInvalid => "headers_invalid",
            Self::TimeoutInvalid => "timeout_invalid",
            Self::ExporterUnavailable => "exporter_unavailable",
            Self::RequestFailed => "request_failed",
            Self::QueueSaturated => "queue_saturated",
            Self::ShutdownTimeout => "shutdown_timeout",
        }
    }

    const fn bit(self) -> u64 {
        1 << self as u8
    }
}

#[derive(Clone)]
struct DiagnosticReporter {
    writer: Arc<dyn EventWriter>,
    reported: Arc<AtomicU64>,
}

impl fmt::Debug for DiagnosticReporter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DiagnosticReporter { .. }")
    }
}

impl DiagnosticReporter {
    fn new(writer: Arc<dyn EventWriter>) -> Self {
        Self {
            writer,
            reported: Arc::new(AtomicU64::new(0)),
        }
    }

    fn report(&self, classification: DiagnosticClassification) {
        let bit = classification.bit();
        if self.reported.fetch_or(bit, Ordering::AcqRel) & bit != 0 {
            return;
        }
        let diagnostic = serde_json::Map::from_iter([
            (
                DIAGNOSTIC_NAME_FIELD.to_owned(),
                serde_json::Value::String(DIAGNOSTIC_NAME.to_owned()),
            ),
            (
                DIAGNOSTIC_CLASSIFICATION_FIELD.to_owned(),
                serde_json::Value::String(classification.as_str().to_owned()),
            ),
        ]);
        let _ = self.writer.write(&diagnostic);
    }
}

#[derive(Debug)]
enum SettingsOutcome {
    Disabled,
    Invalid(DiagnosticClassification),
    Enabled(ExportSettings),
}

struct ExportSettings {
    endpoint: Url,
    headers: HeaderMap,
    timeout: Duration,
}

impl fmt::Debug for ExportSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExportSettings { .. }")
    }
}

impl ExportSettings {
    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<OsString>) -> SettingsOutcome {
        match unicode_value(&mut lookup, SDK_DISABLED) {
            EnvValue::Absent => {}
            EnvValue::Invalid => {
                return SettingsOutcome::Invalid(DiagnosticClassification::PrivacyInvalid);
            }
            EnvValue::Present(value) if value.is_empty() || value.eq_ignore_ascii_case("false") => {
            }
            EnvValue::Present(value) if value.eq_ignore_ascii_case("true") => {
                return SettingsOutcome::Disabled;
            }
            EnvValue::Present(_) => {
                return SettingsOutcome::Invalid(DiagnosticClassification::PrivacyInvalid);
            }
        }

        let endpoint = match selected_value(&mut lookup, TRACES_ENDPOINT, ENDPOINT) {
            Ok(None) => return SettingsOutcome::Disabled,
            Ok(Some((value, signal_specific))) => match export_endpoint(&value, signal_specific) {
                Some(endpoint) => endpoint,
                None => {
                    return SettingsOutcome::Invalid(DiagnosticClassification::EndpointInvalid);
                }
            },
            Err(()) => {
                return SettingsOutcome::Invalid(DiagnosticClassification::EndpointInvalid);
            }
        };

        match selected_value(&mut lookup, TRACES_PROTOCOL, PROTOCOL) {
            Ok(None) => {}
            Ok(Some((value, _))) if value == HTTP_PROTOBUF => {}
            Ok(Some(_)) | Err(()) => {
                return SettingsOutcome::Invalid(DiagnosticClassification::ProtocolUnsupported);
            }
        }

        let headers = match selected_value(&mut lookup, TRACES_HEADERS, HEADERS) {
            Ok(None) => HeaderMap::new(),
            Ok(Some((value, _))) => match parse_headers(&value) {
                Some(headers) => headers,
                None => {
                    return SettingsOutcome::Invalid(DiagnosticClassification::HeadersInvalid);
                }
            },
            Err(()) => {
                return SettingsOutcome::Invalid(DiagnosticClassification::HeadersInvalid);
            }
        };

        let timeout = match selected_value(&mut lookup, TRACES_TIMEOUT, TIMEOUT) {
            Ok(None) => DEFAULT_REQUEST_TIMEOUT,
            Ok(Some((value, _))) => match parse_timeout(&value) {
                Some(timeout) => timeout,
                None => {
                    return SettingsOutcome::Invalid(DiagnosticClassification::TimeoutInvalid);
                }
            },
            Err(()) => {
                return SettingsOutcome::Invalid(DiagnosticClassification::TimeoutInvalid);
            }
        };

        SettingsOutcome::Enabled(Self {
            endpoint,
            headers,
            timeout,
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
enum EnvValue {
    Absent,
    Invalid,
    Present(String),
}

fn unicode_value(lookup: &mut impl FnMut(&str) -> Option<OsString>, name: &str) -> EnvValue {
    match lookup(name) {
        None => EnvValue::Absent,
        Some(value) => match value.into_string() {
            Ok(value) => EnvValue::Present(value),
            Err(_) => EnvValue::Invalid,
        },
    }
}

fn selected_value(
    lookup: &mut impl FnMut(&str) -> Option<OsString>,
    preferred: &str,
    fallback: &str,
) -> Result<Option<(String, bool)>, ()> {
    match unicode_value(lookup, preferred) {
        EnvValue::Present(value) if !value.is_empty() => return Ok(Some((value, true))),
        EnvValue::Invalid => return Err(()),
        EnvValue::Absent | EnvValue::Present(_) => {}
    }
    match unicode_value(lookup, fallback) {
        EnvValue::Present(value) if !value.is_empty() => Ok(Some((value, false))),
        EnvValue::Absent | EnvValue::Present(_) => Ok(None),
        EnvValue::Invalid => Err(()),
    }
}

fn export_endpoint(value: &str, signal_specific: bool) -> Option<Url> {
    let mut endpoint = Url::parse(value).ok()?;
    if endpoint.username() != ""
        || endpoint.password().is_some()
        || endpoint.host_str().is_none()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return None;
    }
    match endpoint.scheme() {
        "https" => {}
        "http" if crate::runner::is_loopback(&endpoint) => {}
        _ => return None,
    }
    if !signal_specific {
        endpoint
            .path_segments_mut()
            .ok()?
            .pop_if_empty()
            .push("v1")
            .push("traces");
    }
    Some(endpoint)
}

fn parse_timeout(value: &str) -> Option<Duration> {
    let milliseconds = value.parse::<u64>().ok()?;
    (1..=MAX_REQUEST_TIMEOUT_MS)
        .contains(&milliseconds)
        .then(|| Duration::from_millis(milliseconds))
}

fn parse_headers(value: &str) -> Option<HeaderMap> {
    if value.len() > MAX_HEADER_BYTES {
        return None;
    }
    let mut headers = HeaderMap::new();
    if value.is_empty() {
        return Some(headers);
    }
    for (index, entry) in value.split(',').enumerate() {
        if index >= MAX_HEADER_COUNT {
            return None;
        }
        let (name, value) = entry.trim().split_once('=')?;
        let name = HeaderName::from_bytes(name.trim().as_bytes()).ok()?;
        let decoded = percent_decode(value.trim())?;
        let value = HeaderValue::from_str(&decoded).ok()?;
        headers.insert(name, value);
    }
    Some(headers)
}

fn percent_decode(value: &str) -> Option<String> {
    let input = value.as_bytes();
    let mut decoded = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        if input[index] == b'%' {
            let upper = *input.get(index + 1)?;
            let lower = *input.get(index + 2)?;
            decoded.push(hex_digit(upper)? << 4 | hex_digit(lower)?);
            index += 3;
        } else {
            decoded.push(input[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

const fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

trait BatchExporter: Send + Sync {
    fn set_resource(&mut self, resource: &Resource);
    fn export(&mut self, spans: Vec<SpanData>) -> Result<(), ()>;
}

struct OtlpHttpExporter {
    client: Client,
    endpoint: Url,
    headers: HeaderMap,
    resource: opentelemetry_proto::transform::common::tonic::ResourceAttributesWithSchema,
}

impl fmt::Debug for OtlpHttpExporter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OtlpHttpExporter { .. }")
    }
}

impl OtlpHttpExporter {
    fn new(settings: ExportSettings) -> Result<Self, ()> {
        crate::tls::install_provider();
        let client = Client::builder()
            .connect_timeout(settings.timeout)
            .timeout(settings.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()
            .map_err(|_| ())?;
        Ok(Self {
            client,
            endpoint: settings.endpoint,
            headers: settings.headers,
            resource: Default::default(),
        })
    }
}

impl BatchExporter for OtlpHttpExporter {
    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.into();
    }

    fn export(&mut self, spans: Vec<SpanData>) -> Result<(), ()> {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::transform::trace::tonic::group_spans_by_resource_and_scope;

        let request = ExportTraceServiceRequest {
            resource_spans: group_spans_by_resource_and_scope(spans, &self.resource),
        };
        let mut headers = self.headers.clone();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-protobuf"),
        );
        let response = self
            .client
            .post(self.endpoint.clone())
            .headers(headers)
            .body(request.encode_to_vec())
            .send()
            .map_err(|_| ())?;
        response.status().is_success().then_some(()).ok_or(())
    }
}

enum ProcessorMessage {
    Span(Box<SpanData>),
    Flush(SyncSender<OTelSdkResult>),
    Shutdown(SyncSender<OTelSdkResult>),
}

pub(super) struct ExportSpanProcessor {
    exporter: Option<Box<dyn BatchExporter>>,
    diagnostics: DiagnosticReporter,
    sender: Option<SyncSender<ProcessorMessage>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    shutdown: AtomicBool,
    queue_capacity: usize,
}

impl fmt::Debug for ExportSpanProcessor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExportSpanProcessor { .. }")
    }
}

impl ExportSpanProcessor {
    fn new(exporter: impl BatchExporter + 'static, diagnostics: DiagnosticReporter) -> Self {
        Self::with_queue_capacity(exporter, diagnostics, EXPORT_QUEUE_CAPACITY)
    }

    fn with_queue_capacity(
        exporter: impl BatchExporter + 'static,
        diagnostics: DiagnosticReporter,
        queue_capacity: usize,
    ) -> Self {
        Self {
            exporter: Some(Box::new(exporter)),
            diagnostics,
            sender: None,
            worker: Mutex::new(None),
            shutdown: AtomicBool::new(false),
            queue_capacity,
        }
    }

    fn send_control(
        &self,
        message: impl FnOnce(SyncSender<OTelSdkResult>) -> ProcessorMessage,
        timeout: Duration,
    ) -> OTelSdkResult {
        let Some(sender) = &self.sender else {
            return Ok(());
        };
        let (completion, completed) = sync_channel(1);
        sender
            .try_send(message(completion))
            .map_err(|error| match error {
                TrySendError::Full(_) => OTelSdkError::InternalFailure(
                    "runner telemetry export control queue is full".to_owned(),
                ),
                TrySendError::Disconnected(_) => OTelSdkError::AlreadyShutdown,
            })?;
        completed
            .recv_timeout(timeout)
            .map_err(|error| match error {
                std::sync::mpsc::RecvTimeoutError::Timeout => OTelSdkError::Timeout(timeout),
                std::sync::mpsc::RecvTimeoutError::Disconnected => OTelSdkError::AlreadyShutdown,
            })?
    }
}

impl SpanProcessor for ExportSpanProcessor {
    fn on_start(&self, _span: &mut Span, _context: &opentelemetry::Context) {}

    fn on_end(&self, span: SpanData) {
        let Some(sender) = &self.sender else {
            return;
        };
        match sender.try_send(ProcessorMessage::Span(Box::new(span))) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.diagnostics
                    .report(DiagnosticClassification::QueueSaturated);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.diagnostics
                    .report(DiagnosticClassification::RequestFailed);
            }
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.send_control(ProcessorMessage::Flush, MAX_SHUTDOWN_TIMEOUT)
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let timeout = timeout.min(MAX_SHUTDOWN_TIMEOUT);
        let result = self.send_control(ProcessorMessage::Shutdown, timeout);
        if result.is_err() {
            self.diagnostics
                .report(DiagnosticClassification::ShutdownTimeout);
            return result;
        }
        if let Some(worker) = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = worker.join();
        }
        Ok(())
    }

    fn set_resource(&mut self, resource: &Resource) {
        let Some(mut exporter) = self.exporter.take() else {
            return;
        };
        exporter.set_resource(resource);
        let (sender, receiver) = sync_channel(self.queue_capacity);
        let diagnostics = self.diagnostics.clone();
        match thread::Builder::new()
            .name("scherzo-runner-otlp".to_owned())
            .spawn(move || export_worker(exporter, receiver, &diagnostics))
        {
            Ok(worker) => {
                self.sender = Some(sender);
                *self
                    .worker
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
            }
            Err(_) => self
                .diagnostics
                .report(DiagnosticClassification::ExporterUnavailable),
        }
    }
}

fn export_worker(
    mut exporter: Box<dyn BatchExporter>,
    receiver: Receiver<ProcessorMessage>,
    diagnostics: &DiagnosticReporter,
) {
    while let Ok(message) = receiver.recv() {
        match message {
            ProcessorMessage::Span(span) => {
                let mut spans = vec![*span];
                let mut control = None;
                while spans.len() < MAX_EXPORT_BATCH_SIZE {
                    match receiver.try_recv() {
                        Ok(ProcessorMessage::Span(span)) => spans.push(*span),
                        Ok(message) => {
                            control = Some(message);
                            break;
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => break,
                    }
                }
                if exporter.export(spans).is_err() {
                    diagnostics.report(DiagnosticClassification::RequestFailed);
                }
                if control.is_some_and(finish_control) {
                    break;
                }
            }
            message => {
                if finish_control(message) {
                    break;
                }
            }
        }
    }
}

fn finish_control(message: ProcessorMessage) -> bool {
    match message {
        ProcessorMessage::Flush(completion) => {
            let _ = completion.send(Ok(()));
            false
        }
        ProcessorMessage::Shutdown(completion) => {
            let _ = completion.send(Ok(()));
            true
        }
        ProcessorMessage::Span(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Write as _;
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use opentelemetry::KeyValue;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use prost::Message as _;

    use super::*;
    use crate::runner::telemetry::{Outcome, Recorder, TestCapture};

    fn outcome(values: &[(&str, &str)]) -> SettingsOutcome {
        let values = values
            .iter()
            .map(|(name, value)| ((*name).to_owned(), OsString::from(value)))
            .collect::<HashMap<_, _>>();
        ExportSettings::from_lookup(|name| values.get(name).cloned())
    }

    fn enabled(values: &[(&str, &str)]) -> ExportSettings {
        match outcome(values) {
            SettingsOutcome::Enabled(settings) => settings,
            other => panic!("expected enabled export settings, got {other:?}"),
        }
    }

    #[test]
    fn privacy_veto_and_absent_endpoint_disable_export() {
        assert!(matches!(outcome(&[]), SettingsOutcome::Disabled));
        assert!(matches!(
            outcome(&[
                (SDK_DISABLED, "true"),
                (TRACES_ENDPOINT, "https://collector.test/v1/traces")
            ]),
            SettingsOutcome::Disabled
        ));
        assert!(matches!(
            outcome(&[
                (SDK_DISABLED, "TrUe"),
                (TRACES_ENDPOINT, "https://collector.test/v1/traces")
            ]),
            SettingsOutcome::Disabled
        ));
        assert!(matches!(
            outcome(&[
                (SDK_DISABLED, "false"),
                (TRACES_ENDPOINT, "https://collector.test/v1/traces")
            ]),
            SettingsOutcome::Enabled(_)
        ));
        assert!(matches!(
            outcome(&[
                (SDK_DISABLED, ""),
                (TRACES_ENDPOINT, "https://collector.test/v1/traces")
            ]),
            SettingsOutcome::Enabled(_)
        ));
        assert!(matches!(
            outcome(&[
                (SDK_DISABLED, "yes"),
                (TRACES_ENDPOINT, "https://collector.test/v1/traces")
            ]),
            SettingsOutcome::Invalid(DiagnosticClassification::PrivacyInvalid)
        ));
    }

    #[test]
    fn empty_signal_specific_settings_fall_back_to_generic_values() {
        let settings = enabled(&[
            (TRACES_ENDPOINT, "https://collector.example.test/v1/traces"),
            (TRACES_HEADERS, ""),
            (HEADERS, "authorization=Bearer%20fixture"),
            (TRACES_TIMEOUT, ""),
            (TIMEOUT, "250"),
        ]);

        assert_eq!(
            settings.headers["authorization"],
            HeaderValue::from_static("Bearer fixture")
        );
        assert_eq!(settings.timeout, Duration::from_millis(250));
    }

    #[test]
    fn endpoint_precedence_and_transport_policy_are_closed() {
        let settings = enabled(&[
            (TRACES_ENDPOINT, "http://127.0.0.1:4318/custom/traces"),
            (ENDPOINT, "https://generic.example.test/base"),
        ]);
        assert_eq!(
            settings.endpoint.as_str(),
            "http://127.0.0.1:4318/custom/traces"
        );

        let generic = enabled(&[(ENDPOINT, "https://collector.example.test/base%20path/")]);
        assert_eq!(
            generic.endpoint.as_str(),
            "https://collector.example.test/base%20path/v1/traces"
        );

        for endpoint in [
            "http://collector.example.test/v1/traces",
            "http://localhost.example.test/v1/traces",
            "ftp://collector.example.test/v1/traces",
            "https://user@collector.example.test/v1/traces",
            "https://collector.example.test/v1/traces?tenant=secret",
            "https://collector.example.test/v1/traces#secret",
        ] {
            assert!(matches!(
                outcome(&[(TRACES_ENDPOINT, endpoint)]),
                SettingsOutcome::Invalid(DiagnosticClassification::EndpointInvalid)
            ));
        }
        assert!(matches!(
            outcome(&[(TRACES_ENDPOINT, "http://[::1]:4318/v1/traces")]),
            SettingsOutcome::Enabled(_)
        ));
    }

    #[test]
    fn protocol_headers_and_timeout_use_standard_precedence() {
        let settings = enabled(&[
            (TRACES_ENDPOINT, "https://collector.example.test/v1/traces"),
            (TRACES_PROTOCOL, HTTP_PROTOBUF),
            (PROTOCOL, "grpc"),
            (TRACES_HEADERS, "authorization=Bearer%20fixture,x-plus=a+b"),
            (HEADERS, "ignored=generic"),
            (TRACES_TIMEOUT, "250"),
            (TIMEOUT, "1000"),
        ]);
        assert_eq!(settings.timeout, Duration::from_millis(250));
        assert_eq!(
            settings.headers["authorization"],
            HeaderValue::from_static("Bearer fixture")
        );
        assert_eq!(settings.headers["x-plus"], HeaderValue::from_static("a+b"));
        assert!(settings.headers.get("ignored").is_none());

        for (name, value, classification) in [
            (
                TRACES_PROTOCOL,
                "grpc",
                DiagnosticClassification::ProtocolUnsupported,
            ),
            (
                TRACES_TIMEOUT,
                "0",
                DiagnosticClassification::TimeoutInvalid,
            ),
            (
                TRACES_TIMEOUT,
                "30001",
                DiagnosticClassification::TimeoutInvalid,
            ),
            (
                TRACES_HEADERS,
                "authorization=%zz",
                DiagnosticClassification::HeadersInvalid,
            ),
        ] {
            assert!(matches!(
                outcome(&[
                    (TRACES_ENDPOINT, "https://collector.example.test/v1/traces"),
                    (name, value),
                ]),
                SettingsOutcome::Invalid(actual) if actual == classification
            ));
        }
    }

    struct CapturedRequest {
        head: String,
        body: Vec<u8>,
    }

    struct OtlpReceiver {
        endpoint: String,
        stop: mpsc::SyncSender<()>,
        worker: thread::JoinHandle<Vec<CapturedRequest>>,
    }

    impl OtlpReceiver {
        fn start(status: &'static str) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind OTLP receiver");
            listener
                .set_nonblocking(true)
                .expect("make OTLP receiver nonblocking");
            let address = listener.local_addr().expect("OTLP receiver address");
            let (stop, stopped) = mpsc::sync_channel(1);
            let worker = thread::spawn(move || {
                let mut requests = Vec::new();
                loop {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let request = crate::api::test_support::read_request(&mut stream);
                            let body_start = request
                                .windows(4)
                                .position(|part| part == b"\r\n\r\n")
                                .map(|index| index + 4)
                                .expect("OTLP request should have headers");
                            requests.push(CapturedRequest {
                                head: String::from_utf8(request[..body_start].to_vec())
                                    .expect("OTLP request headers should be text"),
                                body: request[body_start..].to_vec(),
                            });
                            let response = format!(
                                "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
                            );
                            let _ = stream.write_all(response.as_bytes());
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            if stopped.recv_timeout(Duration::from_millis(2)).is_ok() {
                                break;
                            }
                        }
                        Err(error) => panic!("accept OTLP request: {error}"),
                    }
                }
                requests
            });
            Self {
                endpoint: format!("http://{address}/v1/traces"),
                stop,
                worker,
            }
        }

        fn finish(self) -> Vec<CapturedRequest> {
            self.stop.send(()).expect("stop OTLP receiver");
            self.worker.join().expect("join OTLP receiver")
        }
    }

    fn resource() -> Resource {
        Resource::builder_empty()
            .with_attributes([
                KeyValue::new("service.name", "scherzo-runner"),
                KeyValue::new("service.version", "0.2.0-test"),
                KeyValue::new("service.instance.id", "rbt_fixture"),
            ])
            .build()
    }

    fn recorder_with_exporter(
        exporter: impl BatchExporter + 'static,
        writer: TestCapture,
        queue_capacity: usize,
    ) -> Arc<Recorder> {
        let writer: Arc<dyn EventWriter> = Arc::new(writer);
        let diagnostics = DiagnosticReporter::new(Arc::clone(&writer));
        let processor =
            ExportSpanProcessor::with_queue_capacity(exporter, diagnostics, queue_capacity);
        let provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .with_resource(resource())
            .build();
        Arc::new(Recorder::new(provider, writer, "0.2.0-test", "rbt_fixture"))
    }

    fn proto_value(value: &opentelemetry_proto::tonic::common::v1::AnyValue) -> serde_json::Value {
        match value.value.as_ref().expect("OTLP value should exist") {
            any_value::Value::StringValue(value) => serde_json::Value::String(value.clone()),
            any_value::Value::BoolValue(value) => serde_json::Value::Bool(*value),
            any_value::Value::IntValue(value) => serde_json::Value::Number((*value).into()),
            any_value::Value::DoubleValue(value) => serde_json::Number::from_f64(*value)
                .map(serde_json::Value::Number)
                .expect("finite OTLP double"),
            other => panic!("unexpected OTLP attribute value: {other:?}"),
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn exports_canonical_events_with_exact_resource_and_local_identifiers() {
        let receiver = OtlpReceiver::start("200 OK");
        let settings = enabled(&[
            (TRACES_ENDPOINT, &receiver.endpoint),
            (TRACES_HEADERS, "authorization=Bearer%20HEADER-SENTINEL"),
            (TRACES_TIMEOUT, "1000"),
        ]);
        let exporter = OtlpHttpExporter::new(settings).expect("build OTLP exporter");
        let capture = TestCapture::default();
        let recorder = recorder_with_exporter(exporter, capture.clone(), EXPORT_QUEUE_CAPACITY);

        let connection = recorder.start(
            "runner.gateway_connection",
            [KeyValue::new("scherzo.runner.id", "rnr_fixture")],
        );
        connection.finish(Outcome::Disconnected);
        let effect = recorder.start(
            "runner.effect_acknowledgement",
            [KeyValue::new("scherzo.effect.id", "eff_fixture")],
        );
        effect.finish(Outcome::Success);
        let local_events = capture.events();
        drop(recorder);

        let requests = receiver.finish();
        assert!(!requests.is_empty());
        let mut resource_attributes = None;
        let mut spans = HashMap::new();
        for request in requests {
            assert!(request.head.starts_with("POST /v1/traces HTTP/1.1\r\n"));
            assert!(
                request
                    .head
                    .to_ascii_lowercase()
                    .contains("content-type: application/x-protobuf\r\n")
            );
            assert!(
                request
                    .head
                    .contains("authorization: Bearer HEADER-SENTINEL\r\n")
            );
            let export = ExportTraceServiceRequest::decode(request.body.as_slice())
                .expect("decode OTLP trace request");
            for resource_spans in export.resource_spans {
                let attributes = resource_spans
                    .resource
                    .expect("OTLP resource should exist")
                    .attributes
                    .into_iter()
                    .map(|attribute| {
                        (
                            attribute.key,
                            proto_value(attribute.value.as_ref().expect("resource value")),
                        )
                    })
                    .collect::<HashMap<_, _>>();
                resource_attributes.get_or_insert(attributes);
                for scope in resource_spans.scope_spans {
                    for span in scope.spans {
                        spans.insert(span.name.clone(), span);
                    }
                }
            }
        }

        assert_eq!(
            resource_attributes.expect("captured OTLP resource"),
            HashMap::from([
                (
                    "service.name".to_owned(),
                    serde_json::json!("scherzo-runner")
                ),
                (
                    "service.version".to_owned(),
                    serde_json::json!("0.2.0-test")
                ),
                (
                    "service.instance.id".to_owned(),
                    serde_json::json!("rbt_fixture")
                ),
            ])
        );
        assert_eq!(spans.len(), 2);
        for local in local_events {
            let name = local["event.name"].as_str().expect("local event name");
            let span = &spans[name];
            assert_eq!(hex(&span.trace_id), local["trace_id"]);
            assert_eq!(hex(&span.span_id), local["span_id"]);
            assert!(span.parent_span_id.is_empty());
            let attributes = span
                .attributes
                .iter()
                .map(|attribute| {
                    (
                        attribute.key.as_str(),
                        proto_value(attribute.value.as_ref().expect("span value")),
                    )
                })
                .collect::<HashMap<_, _>>();
            for (key, value) in &local {
                if !matches!(
                    key.as_str(),
                    "event.name" | "duration_ms" | "trace_id" | "span_id"
                ) {
                    assert_eq!(attributes.get(key.as_str()), Some(value), "attribute {key}");
                }
            }
        }
        assert_ne!(
            spans["runner.gateway_connection"].trace_id,
            spans["runner.effect_acknowledgement"].trace_id
        );
    }

    #[test]
    fn rejecting_receiver_reports_no_endpoint_header_or_response_details() {
        let receiver = OtlpReceiver::start("503 UNIQUE-RESPONSE-SENTINEL");
        let endpoint_sentinel = receiver.endpoint.clone();
        let settings = enabled(&[
            (TRACES_ENDPOINT, &endpoint_sentinel),
            (TRACES_HEADERS, "x-owned-secret=UNIQUE-HEADER-SENTINEL"),
            (TRACES_TIMEOUT, "1000"),
        ]);
        let exporter = OtlpHttpExporter::new(settings).expect("build OTLP exporter");
        let capture = TestCapture::default();
        let recorder = recorder_with_exporter(exporter, capture.clone(), EXPORT_QUEUE_CAPACITY);
        recorder
            .start("runner.gateway_connection", [])
            .finish(Outcome::Failure);
        drop(recorder);

        let requests = receiver.finish();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0]
                .head
                .contains("x-owned-secret: UNIQUE-HEADER-SENTINEL\r\n")
        );
        let local_output = serde_json::to_string(&capture.events()).expect("encode local output");
        assert!(local_output.contains("request_failed"));
        for sentinel in [
            "UNIQUE-HEADER-SENTINEL",
            "UNIQUE-RESPONSE-SENTINEL",
            endpoint_sentinel.as_str(),
        ] {
            assert!(!local_output.contains(sentinel));
        }
    }

    #[derive(Debug)]
    struct RejectingExporter;

    impl BatchExporter for RejectingExporter {
        fn set_resource(&mut self, _resource: &Resource) {}

        fn export(&mut self, _spans: Vec<SpanData>) -> Result<(), ()> {
            Err(())
        }
    }

    #[test]
    fn exporter_rejection_is_nonfatal_and_diagnostic_is_closed_and_bounded() {
        let capture = TestCapture::default();
        let recorder =
            recorder_with_exporter(RejectingExporter, capture.clone(), EXPORT_QUEUE_CAPACITY);
        for _ in 0..3 {
            recorder
                .start("runner.gateway_connection", [])
                .finish(Outcome::Failure);
        }
        drop(recorder);

        let events = capture.events();
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event.get(DIAGNOSTIC_CLASSIFICATION_FIELD)
                        == Some(&serde_json::json!("request_failed"))
                })
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.get("event.name").is_some())
                .count(),
            3
        );
        let diagnostics = events
            .iter()
            .filter(|event| event.get(DIAGNOSTIC_NAME_FIELD).is_some())
            .collect::<Vec<_>>();
        assert_eq!(
            diagnostics,
            vec![&serde_json::Map::from_iter([
                (
                    DIAGNOSTIC_NAME_FIELD.to_owned(),
                    serde_json::json!(DIAGNOSTIC_NAME),
                ),
                (
                    DIAGNOSTIC_CLASSIFICATION_FIELD.to_owned(),
                    serde_json::json!("request_failed"),
                ),
            ])]
        );
    }

    #[derive(Debug)]
    struct StalledExporter {
        started: mpsc::SyncSender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl BatchExporter for StalledExporter {
        fn set_resource(&mut self, _resource: &Resource) {}

        fn export(&mut self, _spans: Vec<SpanData>) -> Result<(), ()> {
            let _ = self.started.try_send(());
            let _ = self.release.lock().expect("stall release mutex").recv();
            Ok(())
        }
    }

    fn stalled_recorder(
        queue_capacity: usize,
    ) -> (
        Arc<Recorder>,
        TestCapture,
        mpsc::Receiver<()>,
        mpsc::SyncSender<()>,
    ) {
        let (started, export_started) = mpsc::sync_channel(1);
        let (release, released) = mpsc::sync_channel(1);
        let capture = TestCapture::default();
        let recorder = recorder_with_exporter(
            StalledExporter {
                started,
                release: Mutex::new(released),
            },
            capture.clone(),
            queue_capacity,
        );
        (recorder, capture, export_started, release)
    }

    #[test]
    fn stalled_export_has_bounded_shutdown() {
        let (recorder, capture, export_started, release) = stalled_recorder(EXPORT_QUEUE_CAPACITY);
        recorder
            .start("runner.gateway_connection", [])
            .finish(Outcome::Disconnected);
        export_started
            .recv_timeout(Duration::from_secs(1))
            .expect("export should stall");

        let before = Instant::now();
        drop(recorder);
        assert!(before.elapsed() < Duration::from_secs(1));
        assert!(capture.events().iter().any(|event| {
            event.get(DIAGNOSTIC_CLASSIFICATION_FIELD)
                == Some(&serde_json::json!("shutdown_timeout"))
        }));
        release.send(()).expect("release stalled export");
    }

    #[test]
    fn saturated_export_queue_drops_without_blocking_runner_events() {
        let (recorder, capture, export_started, release) = stalled_recorder(1);
        recorder
            .start("runner.gateway_connection", [])
            .finish(Outcome::Disconnected);
        export_started
            .recv_timeout(Duration::from_secs(1))
            .expect("first export should stall");
        recorder
            .start("runner.effect_acknowledgement", [])
            .finish(Outcome::Success);

        let before = Instant::now();
        recorder
            .start("runner.effect_acknowledgement", [])
            .finish(Outcome::Success);
        assert!(before.elapsed() < Duration::from_secs(1));
        assert!(capture.events().iter().any(|event| {
            event.get(DIAGNOSTIC_CLASSIFICATION_FIELD)
                == Some(&serde_json::json!("queue_saturated"))
        }));

        release.send(()).expect("release first export");
        export_started
            .recv_timeout(Duration::from_secs(1))
            .expect("queued export should begin");
        release.send(()).expect("release queued export");
        drop(recorder);
        assert_eq!(
            capture
                .events()
                .iter()
                .filter(|event| event.get("event.name").is_some())
                .count(),
            3
        );
    }
}
