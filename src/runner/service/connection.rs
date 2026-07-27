use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use opentelemetry::KeyValue;
use opentelemetry::propagation::Injector;
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{
    HeaderMap, HeaderName, HeaderValue, StatusCode, header,
};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};

use crate::runner::service::Sleeper;
use crate::runner::service::config::Config;
use crate::runner::telemetry::{self, Event, Outcome, Recorder};
use crate::runner_protocol::{
    CloudFrame, RunnerEnvelope, RunnerFrame, decode_cloud_frame, encode_runner_frame,
};

const SUBPROTOCOL: &str = "scherzo.runner.v1";
const MAX_INBOUND_MESSAGE_BYTES: usize = 65_536;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const WELCOME_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

struct WebSocketTraceContextInjector<'a>(&'a mut HeaderMap);

impl Injector for WebSocketTraceContextInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if value.is_empty() || !matches!(key, "traceparent" | "tracestate") {
            return;
        }
        let Ok(name) = HeaderName::from_bytes(key.as_bytes()) else {
            return;
        };
        let Ok(value) = HeaderValue::from_str(&value) else {
            return;
        };
        self.0.insert(name, value);
    }
}

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
                    ConnectionCause::FormatCurrentTimestamp,
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
    pub(crate) cloud_text_frames_received: u64,
    pub(crate) runner_text_frames_sent: u64,
    pub(crate) effects_received: u64,
    pub(crate) effect_acknowledgements_confirmed: u64,
}

impl ConnectionProgress {
    pub(crate) const fn unacknowledged() -> Self {
        Self {
            opening_acknowledged: false,
            handshake_completed: false,
            cloud_text_frames_received: 0,
            runner_text_frames_sent: 0,
            effects_received: 0,
            effect_acknowledgements_confirmed: 0,
        }
    }

    fn incremented(self, value: u64) -> Result<u64, ConnectionError> {
        value.checked_add(1).ok_or_else(|| {
            ConnectionError::terminal(self, ConnectionCause::ConnectionCounterOverflow)
        })
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

impl FailureKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Terminal => "terminal",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ConnectionCause {
    FormatCurrentTimestamp,
    GatewayPolicyViolation,
    GatewayUnsupportedFrames,
    GatewayOversizedFrames,
    BuildGatewayRequest,
    BuildAuthorizationHeader,
    CredentialRejected,
    ConnectionRequestRejected,
    GatewayHttpError,
    ConnectGateway,
    ConnectTimeout,
    RequiredSubprotocolNotSelected,
    EncodeOpeningHelloUtf8,
    SendOpeningHello,
    GatewayLivenessTimeout,
    GatewayWelcomeTimeout,
    OversizedGatewayFrame,
    ReadGatewayFrame,
    UndecodableGatewayFrame,
    UnexpectedObservationAcknowledgement,
    MismatchedEffectAcknowledgement,
    ObservationSequenceOverflow,
    FormatEffectAcknowledgementTimestamp,
    CalculateLeaseRemaining,
    EncodeEffectAcknowledgement,
    EncodeEffectAcknowledgementUtf8,
    SendEffectAcknowledgement,
    UnexpectedGatewayFrame,
    FlushRunnerPong,
    BinaryGatewayFrame,
    UnexpectedRawGatewayFrame,
    FormatOpeningHelloTimestamp,
    EncodeOpeningHello,
    RunnerSequenceOverflow,
    GatewayClosedConnection,
    ConnectionCounterOverflow,
    EffectAcknowledgementUnconfirmed,
}

impl ConnectionCause {
    pub(crate) const fn message(self) -> &'static str {
        match self {
            Self::FormatCurrentTimestamp => "format current timestamp",
            Self::GatewayPolicyViolation => "gateway closed connection with policy violation",
            Self::GatewayUnsupportedFrames => {
                "gateway attributed unsupported frames to this runner"
            }
            Self::GatewayOversizedFrames => "gateway attributed oversized frames to this runner",
            Self::BuildGatewayRequest => "build gateway request",
            Self::BuildAuthorizationHeader => "build authorization header",
            Self::CredentialRejected => "runner gateway rejected the credential",
            Self::ConnectionRequestRejected => "runner gateway rejected the connection request",
            Self::GatewayHttpError => "runner gateway returned an HTTP error",
            Self::ConnectGateway => "connect to runner gateway",
            Self::ConnectTimeout => "runner gateway connect timeout",
            Self::RequiredSubprotocolNotSelected => {
                "runner gateway did not select the required subprotocol"
            }
            Self::EncodeOpeningHelloUtf8 => "encode opening hello as UTF-8",
            Self::SendOpeningHello => "send opening hello",
            Self::GatewayLivenessTimeout => "gateway liveness timeout",
            Self::GatewayWelcomeTimeout => "gateway welcome timeout",
            Self::OversizedGatewayFrame => "oversized gateway frame",
            Self::ReadGatewayFrame => "read gateway frame",
            Self::UndecodableGatewayFrame => "undecodable gateway frame",
            Self::UnexpectedObservationAcknowledgement => "unexpected observation acknowledgement",
            Self::MismatchedEffectAcknowledgement => "mismatched effect acknowledgement",
            Self::ObservationSequenceOverflow => "runner observation sequence overflow",
            Self::FormatEffectAcknowledgementTimestamp => "format effect acknowledgement timestamp",
            Self::CalculateLeaseRemaining => "calculate effect lease remaining",
            Self::EncodeEffectAcknowledgement => "encode effect acknowledgement",
            Self::EncodeEffectAcknowledgementUtf8 => "encode effect acknowledgement as UTF-8",
            Self::SendEffectAcknowledgement => "send effect acknowledgement",
            Self::UnexpectedGatewayFrame => "unexpected gateway frame",
            Self::FlushRunnerPong => "flush runner pong",
            Self::BinaryGatewayFrame => "binary gateway frame",
            Self::UnexpectedRawGatewayFrame => "unexpected raw gateway frame",
            Self::FormatOpeningHelloTimestamp => "format opening hello timestamp",
            Self::EncodeOpeningHello => "encode opening hello",
            Self::RunnerSequenceOverflow => "runner sequence overflow",
            Self::GatewayClosedConnection => "gateway closed connection",
            Self::ConnectionCounterOverflow => "runner connection counter overflow",
            Self::EffectAcknowledgementUnconfirmed => {
                "effect acknowledgement confirmation not received"
            }
        }
    }

    pub(crate) const fn error_type(self) -> &'static str {
        match self {
            Self::FormatCurrentTimestamp => "format_current_timestamp",
            Self::GatewayPolicyViolation => "gateway_policy_violation",
            Self::GatewayUnsupportedFrames => "gateway_unsupported_frames",
            Self::GatewayOversizedFrames => "gateway_oversized_frames",
            Self::BuildGatewayRequest => "build_gateway_request",
            Self::BuildAuthorizationHeader => "build_authorization_header",
            Self::CredentialRejected => "credential_rejected",
            Self::ConnectionRequestRejected => "connection_request_rejected",
            Self::GatewayHttpError => "gateway_http_error",
            Self::ConnectGateway => "connect_gateway",
            Self::ConnectTimeout => "connect_timeout",
            Self::RequiredSubprotocolNotSelected => "required_subprotocol_not_selected",
            Self::EncodeOpeningHelloUtf8 => "encode_opening_hello_utf8",
            Self::SendOpeningHello => "send_opening_hello",
            Self::GatewayLivenessTimeout => "gateway_liveness_timeout",
            Self::GatewayWelcomeTimeout => "gateway_welcome_timeout",
            Self::OversizedGatewayFrame => "oversized_gateway_frame",
            Self::ReadGatewayFrame => "read_gateway_frame",
            Self::UndecodableGatewayFrame => "undecodable_gateway_frame",
            Self::UnexpectedObservationAcknowledgement => "unexpected_observation_acknowledgement",
            Self::MismatchedEffectAcknowledgement => "mismatched_effect_acknowledgement",
            Self::ObservationSequenceOverflow => "observation_sequence_overflow",
            Self::FormatEffectAcknowledgementTimestamp => "format_effect_acknowledgement_timestamp",
            Self::CalculateLeaseRemaining => "calculate_lease_remaining",
            Self::EncodeEffectAcknowledgement => "encode_effect_acknowledgement",
            Self::EncodeEffectAcknowledgementUtf8 => "encode_effect_acknowledgement_utf8",
            Self::SendEffectAcknowledgement => "send_effect_acknowledgement",
            Self::UnexpectedGatewayFrame => "unexpected_gateway_frame",
            Self::FlushRunnerPong => "flush_runner_pong",
            Self::BinaryGatewayFrame => "binary_gateway_frame",
            Self::UnexpectedRawGatewayFrame => "unexpected_raw_gateway_frame",
            Self::FormatOpeningHelloTimestamp => "format_opening_hello_timestamp",
            Self::EncodeOpeningHello => "encode_opening_hello",
            Self::RunnerSequenceOverflow => "runner_sequence_overflow",
            Self::GatewayClosedConnection => "gateway_closed_connection",
            Self::ConnectionCounterOverflow => "connection_counter_overflow",
            Self::EffectAcknowledgementUnconfirmed => "effect_acknowledgement_unconfirmed",
        }
    }

    pub(crate) const fn is_timeout(self) -> bool {
        matches!(
            self,
            Self::ConnectTimeout | Self::GatewayWelcomeTimeout | Self::GatewayLivenessTimeout
        )
    }
}

#[derive(Debug)]
pub(crate) struct ConnectionError {
    pub(crate) progress: ConnectionProgress,
    kind: FailureKind,
    cause: ConnectionCause,
}

impl ConnectionError {
    pub(crate) const fn terminal(progress: ConnectionProgress, cause: ConnectionCause) -> Self {
        Self {
            progress,
            kind: FailureKind::Terminal,
            cause,
        }
    }

    const fn retryable(progress: ConnectionProgress, cause: ConnectionCause) -> Self {
        Self {
            progress,
            kind: FailureKind::Retryable,
            cause,
        }
    }

    pub(crate) const fn is_terminal(&self) -> bool {
        matches!(self.kind, FailureKind::Terminal)
    }

    pub(crate) const fn kind(&self) -> FailureKind {
        self.kind
    }

    pub(crate) const fn connection_cause(&self) -> ConnectionCause {
        self.cause
    }

    #[cfg(test)]
    pub(crate) const fn cause(&self) -> &'static str {
        self.cause.message()
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runner gateway connection failed: {}",
            self.cause.message()
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
            ConnectionCause::GatewayPolicyViolation,
        )),
        Some(CloseCode::Unsupported) => Err(ConnectionError::terminal(
            progress,
            ConnectionCause::GatewayUnsupportedFrames,
        )),
        Some(CloseCode::Size) => Err(ConnectionError::terminal(
            progress,
            ConnectionCause::GatewayOversizedFrames,
        )),
        _ => Ok(progress),
    }
}

// close_locally best-effort announces why the runner is abandoning the
// connection; failures to deliver the close frame are deliberately ignored.
async fn close_locally<W>(
    writer: &mut W,
    sleeper: &dyn Sleeper,
    code: CloseCode,
    reason: &'static str,
) where
    W: Sink<Message, Error = WebSocketError> + Unpin,
{
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
async fn protocol_violation<W>(
    writer: &mut W,
    sleeper: &dyn Sleeper,
    progress: ConnectionProgress,
    cause: ConnectionCause,
) -> ConnectionError
where
    W: Sink<Message, Error = WebSocketError> + Unpin,
{
    close_locally(writer, sleeper, CloseCode::Protocol, cause.message()).await;
    ConnectionError::retryable(progress, cause)
}

pub(crate) async fn run(
    dependencies: ConnectionDependencies<'_>,
    opening: OpeningHello<'_>,
    next_sequence: &mut u64,
) -> Result<ConnectionProgress, ConnectionError> {
    let config = dependencies.config;
    let sleeper = dependencies.sleeper;
    let active_effect_event = dependencies.active_effect_event;
    crate::tls::install_provider();
    let unacknowledged = ConnectionProgress::unacknowledged();
    let mut request = config
        .endpoint()
        .as_str()
        .into_client_request()
        .map_err(|_| {
            ConnectionError::terminal(unacknowledged, ConnectionCause::BuildGatewayRequest)
        })?;
    let authorization =
        HeaderValue::from_str(&format!("Bearer {}", config.credential().bearer_value())).map_err(
            |_| {
                ConnectionError::terminal(unacknowledged, ConnectionCause::BuildAuthorizationHeader)
            },
        )?;
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, authorization);
    request.headers_mut().insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(SUBPROTOCOL),
    );
    dependencies
        .connection_event
        .inject_trace_context(&mut WebSocketTraceContextInjector(request.headers_mut()));
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
                StatusCode::UNAUTHORIZED => {
                    ConnectionError::terminal(unacknowledged, ConnectionCause::CredentialRejected)
                }
                StatusCode::BAD_REQUEST => ConnectionError::terminal(
                    unacknowledged,
                    ConnectionCause::ConnectionRequestRejected,
                ),
                _ => ConnectionError::retryable(unacknowledged, ConnectionCause::GatewayHttpError),
            });
        }
        Some(Err(_)) => {
            return Err(ConnectionError::retryable(
                unacknowledged,
                ConnectionCause::ConnectGateway,
            ));
        }
        None => {
            return Err(ConnectionError::retryable(
                unacknowledged,
                ConnectionCause::ConnectTimeout,
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
            ConnectionCause::RequiredSubprotocolNotSelected,
        ));
    }
    let (writer, reader) = socket.split();
    let result = run_established(dependencies, opening, next_sequence, reader, writer).await;
    active_effect_event.finish_connection_end(&result);
    result
}

struct PendingEffectAcknowledgement {
    message_id: String,
    sequence: u64,
}

pub(super) struct ActiveEffectEvent {
    event: Mutex<Option<Event>>,
}

impl ActiveEffectEvent {
    pub(super) fn new() -> Self {
        Self {
            event: Mutex::new(None),
        }
    }

    fn start(&self, event: Event) {
        let previous = self
            .event
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(event);
        if let Some(previous) = previous {
            previous.set(KeyValue::new(
                telemetry::attribute::ERROR_TYPE,
                ConnectionCause::EffectAcknowledgementUnconfirmed.error_type(),
            ));
            previous.finish(Outcome::Disconnected);
        }
    }

    pub(super) fn finish(&self, outcome: Outcome, cause: Option<ConnectionCause>) {
        let event = self
            .event
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(event) = event {
            if let Some(cause) = cause {
                event.set(KeyValue::new(
                    telemetry::attribute::ERROR_TYPE,
                    cause.error_type(),
                ));
            }
            event.finish(outcome);
        }
    }

    fn finish_connection_end(&self, result: &Result<ConnectionProgress, ConnectionError>) {
        match result {
            Err(error) if error.connection_cause().is_timeout() => {
                self.finish(Outcome::Timeout, Some(error.connection_cause()))
            }
            _ => self.finish(
                Outcome::Disconnected,
                Some(ConnectionCause::EffectAcknowledgementUnconfirmed),
            ),
        }
    }
}

fn finish_effect_failure(event: &Event, cause: ConnectionCause) {
    event.set(KeyValue::new(
        telemetry::attribute::ERROR_TYPE,
        cause.error_type(),
    ));
    event.finish(Outcome::Failure);
}

pub(super) fn record_progress(event: &Event, progress: ConnectionProgress) {
    for attribute in [
        KeyValue::new(
            telemetry::attribute::OPENING_ACKNOWLEDGED,
            progress.opening_acknowledged,
        ),
        KeyValue::new(
            telemetry::attribute::HANDSHAKE_COMPLETED,
            progress.handshake_completed,
        ),
        KeyValue::new(
            telemetry::attribute::CLOUD_TEXT_FRAMES_RECEIVED,
            telemetry::integer(progress.cloud_text_frames_received),
        ),
        KeyValue::new(
            telemetry::attribute::RUNNER_TEXT_FRAMES_SENT,
            telemetry::integer(progress.runner_text_frames_sent),
        ),
        KeyValue::new(
            telemetry::attribute::EFFECTS_RECEIVED,
            telemetry::integer(progress.effects_received),
        ),
        KeyValue::new(
            telemetry::attribute::EFFECT_ACKNOWLEDGEMENTS_CONFIRMED,
            telemetry::integer(progress.effect_acknowledgements_confirmed),
        ),
    ] {
        event.set(attribute);
    }
}

fn lease_remaining_ms(lease_expires_at: &str, now: &str) -> Option<i64> {
    let format = &time::format_description::well_known::Rfc3339;
    let lease = time::OffsetDateTime::parse(lease_expires_at, format).ok()?;
    let now = time::OffsetDateTime::parse(now, format).ok()?;
    i64::try_from((lease - now).whole_milliseconds().max(0)).ok()
}

#[derive(Clone, Copy)]
pub(super) struct ConnectionDependencies<'a> {
    config: &'a Config,
    frame_source: &'a dyn FrameSource,
    sleeper: &'a dyn Sleeper,
    recorder: &'a Recorder,
    connection_event: &'a Event,
    active_effect_event: &'a ActiveEffectEvent,
}

impl<'a> ConnectionDependencies<'a> {
    pub(super) fn new(
        config: &'a Config,
        frame_source: &'a dyn FrameSource,
        sleeper: &'a dyn Sleeper,
        recorder: &'a Recorder,
        connection_event: &'a Event,
        active_effect_event: &'a ActiveEffectEvent,
    ) -> Self {
        Self {
            config,
            frame_source,
            sleeper,
            recorder,
            connection_event,
            active_effect_event,
        }
    }
}

pub(super) async fn run_established<R, W>(
    dependencies: ConnectionDependencies<'_>,
    opening: OpeningHello<'_>,
    next_sequence: &mut u64,
    mut reader: R,
    mut writer: W,
) -> Result<ConnectionProgress, ConnectionError>
where
    R: Stream<Item = Result<Message, WebSocketError>> + Unpin,
    W: Sink<Message, Error = WebSocketError> + Unpin,
{
    let ConnectionDependencies {
        config,
        frame_source,
        sleeper,
        recorder,
        connection_event,
        active_effect_event,
    } = dependencies;
    let unacknowledged = ConnectionProgress::unacknowledged();
    let opening_hello = std::str::from_utf8(opening.encoded).map_err(|_| {
        ConnectionError::terminal(unacknowledged, ConnectionCause::EncodeOpeningHelloUtf8)
    })?;
    writer
        .send(Message::Text(opening_hello.into()))
        .await
        .map_err(|_| {
            ConnectionError::retryable(unacknowledged, ConnectionCause::SendOpeningHello)
        })?;

    let mut welcome_timer = sleeper.sleep(WELCOME_TIMEOUT);
    let mut inbound_silence_timeout = None;
    let mut progress = ConnectionProgress::unacknowledged();
    progress.runner_text_frames_sent = progress.incremented(progress.runner_text_frames_sent)?;
    record_progress(connection_event, progress);
    let mut pending_effect_acknowledgement: Option<PendingEffectAcknowledgement> = None;

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
                        ConnectionCause::GatewayLivenessTimeout.message(),
                    ).await;
                    return Err(ConnectionError::retryable(
                        progress,
                        ConnectionCause::GatewayLivenessTimeout,
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
                        ConnectionCause::GatewayWelcomeTimeout,
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
                    ConnectionCause::OversizedGatewayFrame,
                )
                .await);
            }
            Err(_) => {
                return Err(ConnectionError::retryable(
                    progress,
                    ConnectionCause::ReadGatewayFrame,
                ));
            }
        };
        match message {
            Message::Text(text) => {
                let Ok(frame) = decode_cloud_frame(text.as_bytes()) else {
                    return Err(protocol_violation(
                        &mut writer,
                        sleeper,
                        progress,
                        ConnectionCause::UndecodableGatewayFrame,
                    )
                    .await);
                };
                progress.cloud_text_frames_received =
                    progress.incremented(progress.cloud_text_frames_received)?;
                record_progress(connection_event, progress);
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
                        record_progress(connection_event, progress);
                    }
                    CloudFrame::ObservationAck {
                        acknowledged_message_id,
                        acknowledged_sequence,
                        ..
                    } => {
                        let Some(pending) = pending_effect_acknowledgement.as_ref() else {
                            return Err(protocol_violation(
                                &mut writer,
                                sleeper,
                                progress,
                                ConnectionCause::UnexpectedObservationAcknowledgement,
                            )
                            .await);
                        };
                        if acknowledged_message_id != pending.message_id
                            || acknowledged_sequence != pending.sequence
                        {
                            return Err(protocol_violation(
                                &mut writer,
                                sleeper,
                                progress,
                                ConnectionCause::MismatchedEffectAcknowledgement,
                            )
                            .await);
                        }
                        progress.effect_acknowledgements_confirmed = match progress
                            .incremented(progress.effect_acknowledgements_confirmed)
                        {
                            Ok(count) => count,
                            Err(error) => {
                                if pending_effect_acknowledgement.take().is_some() {
                                    active_effect_event.finish(
                                        Outcome::Failure,
                                        Some(ConnectionCause::ConnectionCounterOverflow),
                                    );
                                }
                                return Err(error);
                            }
                        };
                        record_progress(connection_event, progress);
                        if pending_effect_acknowledgement.take().is_some() {
                            active_effect_event.finish(Outcome::Success, None);
                        }
                    }
                    CloudFrame::AssignmentOffer {
                        effect_id,
                        assignment_id,
                        run_id,
                        lease_expires_at,
                        ..
                    } if progress.handshake_completed
                        && pending_effect_acknowledgement.is_none() =>
                    {
                        let sequence = *next_sequence;
                        let event = recorder.start(
                            "runner.effect_acknowledgement",
                            [
                                KeyValue::new(telemetry::attribute::EFFECT_ID, effect_id.clone()),
                                KeyValue::new(telemetry::attribute::ASSIGNMENT_ID, assignment_id),
                                KeyValue::new(telemetry::attribute::RUN_ID, run_id),
                                KeyValue::new(
                                    telemetry::attribute::RUNNER_ID,
                                    config.credential().runner_id().to_owned(),
                                ),
                                KeyValue::new(
                                    telemetry::attribute::RUNNER_BOOT_ID,
                                    opening.boot_id.to_owned(),
                                ),
                                KeyValue::new(
                                    telemetry::attribute::RUNNER_SEQUENCE,
                                    telemetry::integer(sequence),
                                ),
                            ],
                        );
                        active_effect_event.start(event.clone());
                        progress.effects_received =
                            match progress.incremented(progress.effects_received) {
                                Ok(count) => count,
                                Err(error) => {
                                    finish_effect_failure(
                                        &event,
                                        ConnectionCause::ConnectionCounterOverflow,
                                    );
                                    return Err(error);
                                }
                            };
                        record_progress(connection_event, progress);
                        let Some(incremented_sequence) = next_sequence.checked_add(1) else {
                            finish_effect_failure(
                                &event,
                                ConnectionCause::ObservationSequenceOverflow,
                            );
                            return Err(ConnectionError::terminal(
                                progress,
                                ConnectionCause::ObservationSequenceOverflow,
                            ));
                        };
                        *next_sequence = incremented_sequence;
                        let message_id = frame_source.public_id("rmsg_");
                        let sent_at = match frame_source.utc_timestamp() {
                            Ok(timestamp) => timestamp,
                            Err(_) => {
                                finish_effect_failure(
                                    &event,
                                    ConnectionCause::FormatEffectAcknowledgementTimestamp,
                                );
                                return Err(ConnectionError::terminal(
                                    progress,
                                    ConnectionCause::FormatEffectAcknowledgementTimestamp,
                                ));
                            }
                        };
                        let Some(lease_remaining_ms) =
                            lease_remaining_ms(&lease_expires_at, &sent_at)
                        else {
                            finish_effect_failure(&event, ConnectionCause::CalculateLeaseRemaining);
                            return Err(ConnectionError::terminal(
                                progress,
                                ConnectionCause::CalculateLeaseRemaining,
                            ));
                        };
                        event.set(KeyValue::new(
                            telemetry::attribute::LEASE_REMAINING_MS,
                            lease_remaining_ms,
                        ));
                        let frame = RunnerFrame::EffectAcknowledged {
                            envelope: RunnerEnvelope {
                                message_id: message_id.clone(),
                                runner_id: config.credential().runner_id().to_owned(),
                                boot_id: opening.boot_id.to_owned(),
                                sequence,
                                sent_at,
                            },
                            effect_id,
                        };
                        let encoded = match encode_runner_frame(&frame) {
                            Ok(encoded) => encoded,
                            Err(_) => {
                                finish_effect_failure(
                                    &event,
                                    ConnectionCause::EncodeEffectAcknowledgement,
                                );
                                return Err(ConnectionError::terminal(
                                    progress,
                                    ConnectionCause::EncodeEffectAcknowledgement,
                                ));
                            }
                        };
                        let encoded = match std::str::from_utf8(&encoded) {
                            Ok(encoded) => encoded,
                            Err(_) => {
                                finish_effect_failure(
                                    &event,
                                    ConnectionCause::EncodeEffectAcknowledgementUtf8,
                                );
                                return Err(ConnectionError::terminal(
                                    progress,
                                    ConnectionCause::EncodeEffectAcknowledgementUtf8,
                                ));
                            }
                        };
                        if writer.send(Message::Text(encoded.into())).await.is_err() {
                            finish_effect_failure(
                                &event,
                                ConnectionCause::SendEffectAcknowledgement,
                            );
                            return Err(ConnectionError::retryable(
                                progress,
                                ConnectionCause::SendEffectAcknowledgement,
                            ));
                        }
                        progress.runner_text_frames_sent =
                            progress.incremented(progress.runner_text_frames_sent)?;
                        record_progress(connection_event, progress);
                        pending_effect_acknowledgement = Some(PendingEffectAcknowledgement {
                            message_id,
                            sequence,
                        });
                    }
                    _ => {
                        return Err(protocol_violation(
                            &mut writer,
                            sleeper,
                            progress,
                            ConnectionCause::UnexpectedGatewayFrame,
                        )
                        .await);
                    }
                }
                if inbound_silence_timeout.is_some()
                    && progress.opening_acknowledged
                    && !progress.handshake_completed
                {
                    progress.handshake_completed = true;
                    record_progress(connection_event, progress);
                }
            }
            Message::Ping(_) => {
                writer.flush().await.map_err(|_| {
                    ConnectionError::retryable(progress, ConnectionCause::FlushRunnerPong)
                })?;
            }
            Message::Pong(_) => {}
            Message::Close(close) => return close_outcome(progress, close),
            Message::Binary(_) => {
                return Err(protocol_violation(
                    &mut writer,
                    sleeper,
                    progress,
                    ConnectionCause::BinaryGatewayFrame,
                )
                .await);
            }
            Message::Frame(_) => {
                return Err(protocol_violation(
                    &mut writer,
                    sleeper,
                    progress,
                    ConnectionCause::UnexpectedRawGatewayFrame,
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
            ConnectionCause::FormatOpeningHelloTimestamp,
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
        ConnectionError::terminal(
            ConnectionProgress::unacknowledged(),
            ConnectionCause::EncodeOpeningHello,
        )
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures_util::{Sink, SinkExt, StreamExt};
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Notify;
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::Error as WebSocketError;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
    use tokio_tungstenite::tungstenite::http::{HeaderValue, header};
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    use super::{
        ActiveEffectEvent, ConnectionCause, ConnectionDependencies, ConnectionError,
        ConnectionProgress, FrameSource, OpeningHello, close_outcome, opening_hello, run,
        run_established,
    };
    use crate::runner::credential::{Credential, test_credential};
    use crate::runner::service::Sleeper;
    use crate::runner::service::config::Config;
    use crate::runner::service::test_support::{
        accept_fixture_socket, accept_opened_fixture_socket, assignment_offer, controlled_sleeper,
        deterministic_frame_source, effect_acknowledgement, expect_close_frame,
        expect_opening_hello, fixture_listener, observation_acknowledgement,
        offer_assignment_after_handshake, sleep_request, welcome, with_watchdog,
    };
    use crate::runner::telemetry::{Outcome, TestCapture, test_recorder};

    const CREDENTIAL: &str =
        "rnr_01k0z6r1w8f4jy2m7q9v3x5abd.abcdefghijklmnopqrstuvwxyzABCDEFG-012345678";
    const BOOT_ID: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe";
    const OPENING_MESSAGE_ID: &str = "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc";

    struct BackpressuredEffectWriter {
        sent: usize,
        blocked: Arc<Notify>,
    }

    impl Sink<Message> for BackpressuredEffectWriter {
        type Error = WebSocketError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.sent == 0 {
                Poll::Ready(Ok(()))
            } else {
                self.blocked.notify_one();
                Poll::Pending
            }
        }

        fn start_send(mut self: Pin<&mut Self>, _message: Message) -> Result<(), Self::Error> {
            self.sent += 1;
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn connection_causes_have_stable_unique_safe_slugs() {
        let causes = [
            (
                ConnectionCause::FormatCurrentTimestamp,
                "format current timestamp",
                "format_current_timestamp",
            ),
            (
                ConnectionCause::GatewayPolicyViolation,
                "gateway closed connection with policy violation",
                "gateway_policy_violation",
            ),
            (
                ConnectionCause::GatewayUnsupportedFrames,
                "gateway attributed unsupported frames to this runner",
                "gateway_unsupported_frames",
            ),
            (
                ConnectionCause::GatewayOversizedFrames,
                "gateway attributed oversized frames to this runner",
                "gateway_oversized_frames",
            ),
            (
                ConnectionCause::BuildGatewayRequest,
                "build gateway request",
                "build_gateway_request",
            ),
            (
                ConnectionCause::BuildAuthorizationHeader,
                "build authorization header",
                "build_authorization_header",
            ),
            (
                ConnectionCause::CredentialRejected,
                "runner gateway rejected the credential",
                "credential_rejected",
            ),
            (
                ConnectionCause::ConnectionRequestRejected,
                "runner gateway rejected the connection request",
                "connection_request_rejected",
            ),
            (
                ConnectionCause::GatewayHttpError,
                "runner gateway returned an HTTP error",
                "gateway_http_error",
            ),
            (
                ConnectionCause::ConnectGateway,
                "connect to runner gateway",
                "connect_gateway",
            ),
            (
                ConnectionCause::ConnectTimeout,
                "runner gateway connect timeout",
                "connect_timeout",
            ),
            (
                ConnectionCause::RequiredSubprotocolNotSelected,
                "runner gateway did not select the required subprotocol",
                "required_subprotocol_not_selected",
            ),
            (
                ConnectionCause::EncodeOpeningHelloUtf8,
                "encode opening hello as UTF-8",
                "encode_opening_hello_utf8",
            ),
            (
                ConnectionCause::SendOpeningHello,
                "send opening hello",
                "send_opening_hello",
            ),
            (
                ConnectionCause::GatewayLivenessTimeout,
                "gateway liveness timeout",
                "gateway_liveness_timeout",
            ),
            (
                ConnectionCause::GatewayWelcomeTimeout,
                "gateway welcome timeout",
                "gateway_welcome_timeout",
            ),
            (
                ConnectionCause::OversizedGatewayFrame,
                "oversized gateway frame",
                "oversized_gateway_frame",
            ),
            (
                ConnectionCause::ReadGatewayFrame,
                "read gateway frame",
                "read_gateway_frame",
            ),
            (
                ConnectionCause::UndecodableGatewayFrame,
                "undecodable gateway frame",
                "undecodable_gateway_frame",
            ),
            (
                ConnectionCause::UnexpectedObservationAcknowledgement,
                "unexpected observation acknowledgement",
                "unexpected_observation_acknowledgement",
            ),
            (
                ConnectionCause::MismatchedEffectAcknowledgement,
                "mismatched effect acknowledgement",
                "mismatched_effect_acknowledgement",
            ),
            (
                ConnectionCause::ObservationSequenceOverflow,
                "runner observation sequence overflow",
                "observation_sequence_overflow",
            ),
            (
                ConnectionCause::FormatEffectAcknowledgementTimestamp,
                "format effect acknowledgement timestamp",
                "format_effect_acknowledgement_timestamp",
            ),
            (
                ConnectionCause::CalculateLeaseRemaining,
                "calculate effect lease remaining",
                "calculate_lease_remaining",
            ),
            (
                ConnectionCause::EncodeEffectAcknowledgement,
                "encode effect acknowledgement",
                "encode_effect_acknowledgement",
            ),
            (
                ConnectionCause::EncodeEffectAcknowledgementUtf8,
                "encode effect acknowledgement as UTF-8",
                "encode_effect_acknowledgement_utf8",
            ),
            (
                ConnectionCause::SendEffectAcknowledgement,
                "send effect acknowledgement",
                "send_effect_acknowledgement",
            ),
            (
                ConnectionCause::UnexpectedGatewayFrame,
                "unexpected gateway frame",
                "unexpected_gateway_frame",
            ),
            (
                ConnectionCause::FlushRunnerPong,
                "flush runner pong",
                "flush_runner_pong",
            ),
            (
                ConnectionCause::BinaryGatewayFrame,
                "binary gateway frame",
                "binary_gateway_frame",
            ),
            (
                ConnectionCause::UnexpectedRawGatewayFrame,
                "unexpected raw gateway frame",
                "unexpected_raw_gateway_frame",
            ),
            (
                ConnectionCause::FormatOpeningHelloTimestamp,
                "format opening hello timestamp",
                "format_opening_hello_timestamp",
            ),
            (
                ConnectionCause::EncodeOpeningHello,
                "encode opening hello",
                "encode_opening_hello",
            ),
            (
                ConnectionCause::RunnerSequenceOverflow,
                "runner sequence overflow",
                "runner_sequence_overflow",
            ),
            (
                ConnectionCause::GatewayClosedConnection,
                "gateway closed connection",
                "gateway_closed_connection",
            ),
            (
                ConnectionCause::ConnectionCounterOverflow,
                "runner connection counter overflow",
                "connection_counter_overflow",
            ),
            (
                ConnectionCause::EffectAcknowledgementUnconfirmed,
                "effect acknowledgement confirmation not received",
                "effect_acknowledgement_unconfirmed",
            ),
        ];
        let mut slugs = std::collections::HashSet::new();
        for (cause, message, slug) in causes {
            assert_eq!(cause.message(), message);
            assert_eq!(cause.error_type(), slug);
            assert!(
                slug.bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            );
            assert!(slugs.insert(slug), "duplicate cause slug {slug}");
        }
    }

    #[test]
    fn connection_counter_overflow_is_terminal_and_classified() {
        let progress = ConnectionProgress {
            cloud_text_frames_received: u64::MAX,
            ..ConnectionProgress::unacknowledged()
        };
        let error = progress
            .incremented(progress.cloud_text_frames_received)
            .expect_err("overflowed connection counter");

        assert!(error.is_terminal());
        assert_eq!(error.cause(), "runner connection counter overflow");
        assert_eq!(
            error.connection_cause().error_type(),
            "connection_counter_overflow"
        );
    }

    #[allow(
        clippy::result_large_err,
        reason = "tungstenite's handshake callback requires its large error type"
    )]
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
        let (outcome, capture, next_sequence) =
            run_configured_fixture_connection_with_capture(&config).await;
        let outcome = outcome.expect("run fixture connection");
        assert!(outcome.opening_acknowledged);
        assert!(outcome.handshake_completed);
        assert_eq!(outcome.cloud_text_frames_received, 4);
        assert_eq!(outcome.runner_text_frames_sent, 2);
        assert_eq!(outcome.effects_received, 1);
        assert_eq!(outcome.effect_acknowledgements_confirmed, 1);
        assert_eq!(next_sequence, 3);
        server.await.expect("join fixture server");

        let events = capture.events();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event["event.name"], "runner.effect_acknowledgement");
        assert_eq!(event["scherzo.effect.id"], "eff_01k0z6r1w8f4jy2m7q9v3x5abg");
        assert_eq!(
            event["scherzo.assignment.id"],
            "asn_01k0z6r1w8f4jy2m7q9v3x5abh"
        );
        assert_eq!(event["scherzo.run.id"], "run_01k0z6r1w8f4jy2m7q9v3x5abj");
        assert_eq!(event["scherzo.runner.boot_id"], BOOT_ID);
        assert_eq!(event["scherzo.runner.sequence"], 2);
        assert_eq!(event["scherzo.delivery.lease_remaining_ms"], 3_600_000);
        assert_eq!(event["scherzo.outcome"], "success");
        let encoded = serde_json::to_string(event).expect("encode effect event");
        assert!(!encoded.contains("runner.run"));
        assert!(!encoded.contains("accepted"));
        assert!(!encoded.contains("executed"));
        assert!(!encoded.contains("abcdefghijklmnopqrstuvwxyzABCDEFG-012345678"));
        assert_eq!(capture.span_count("runner.effect_acknowledgement"), 1);
    }

    #[tokio::test]
    async fn disconnects_an_effect_event_before_transport_confirmation() {
        let (listener, endpoint) = fixture_listener().await;
        let server = tokio::spawn(async move {
            let mut socket = accept_fixture_socket(&listener).await;
            expect_opening_hello(&mut socket).await;
            let _acknowledgement =
                offer_assignment_after_handshake(&mut socket, OPENING_MESSAGE_ID, 1).await;
            socket
                .close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: "PEER-CLOSE-REASON-MUST-NOT-LEAK".into(),
                }))
                .await
                .expect("close fixture socket");
        });

        let config = test_config(&endpoint);
        let (outcome, capture, _next_sequence) =
            run_configured_fixture_connection_with_capture(&config).await;
        let outcome = outcome.expect("close established fixture connection");
        assert_eq!(outcome.effects_received, 1);
        assert_eq!(outcome.effect_acknowledgements_confirmed, 0);
        server.await.expect("join fixture server");

        let events = capture.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event.name"], "runner.effect_acknowledgement");
        assert_eq!(events[0]["scherzo.outcome"], "disconnected");
        assert_eq!(
            events[0]["error.type"],
            "effect_acknowledgement_unconfirmed"
        );
        assert!(
            !serde_json::to_string(&events[0])
                .expect("encode disconnected effect event")
                .contains("PEER-CLOSE-REASON-MUST-NOT-LEAK")
        );
        assert_eq!(capture.span_count("runner.effect_acknowledgement"), 1);
    }

    #[tokio::test]
    async fn keeps_effect_event_across_cancellation_while_send_is_pending() {
        let config = test_config("wss://gateway.example.test/v1/connect");
        let frame_source = deterministic_frame_source();
        let opening = test_opening(&config, frame_source.as_ref());
        let (sleeper, _sleep_requests) = controlled_sleeper();
        let (recorder, capture) = test_recorder(BOOT_ID);
        let connection_event = recorder.start("runner.gateway_connection", []);
        let active_effect_event = ActiveEffectEvent::new();
        let blocked = Arc::new(Notify::new());
        let reader = futures_util::stream::iter([
            Ok(welcome()),
            Ok(observation_acknowledgement(OPENING_MESSAGE_ID, 1)),
            Ok(assignment_offer()),
        ]);
        let writer = BackpressuredEffectWriter {
            sent: 0,
            blocked: Arc::clone(&blocked),
        };
        let mut next_sequence = 2;
        let mut connection = Box::pin(run_established(
            ConnectionDependencies::new(
                &config,
                frame_source.as_ref(),
                sleeper.as_ref(),
                &recorder,
                &connection_event,
                &active_effect_event,
            ),
            OpeningHello {
                boot_id: BOOT_ID,
                encoded: &opening,
                message_id: OPENING_MESSAGE_ID,
                sequence: 1,
            },
            &mut next_sequence,
            reader,
            writer,
        ));

        with_watchdog(async {
            tokio::select! {
                result = &mut connection => panic!("pending send completed unexpectedly: {result:?}"),
                _ = blocked.notified() => {}
            }
        })
        .await
        .expect("effect acknowledgement send was not attempted");
        drop(connection);
        active_effect_event.finish(Outcome::Cancelled, None);
        connection_event.finish(Outcome::Cancelled);

        let events = capture.events();
        assert_eq!(events.len(), 2);
        let effect = capture.event("runner.effect_acknowledgement");
        assert_eq!(effect["scherzo.outcome"], "cancelled");
        assert!(effect.get("error.type").is_none());
        assert_eq!(capture.span_count("runner.effect_acknowledgement"), 1);
        assert_eq!(capture.span_count("runner.gateway_connection"), 1);
    }

    #[tokio::test]
    async fn rejects_a_connection_that_never_sends_welcome() {
        let (listener, endpoint) = fixture_listener().await;
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        let server = tokio::spawn(async move {
            let _socket = accept_opened_fixture_socket(&listener).await;
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

    async fn run_configured_fixture_connection_with_capture(
        config: &Config,
    ) -> (
        Result<ConnectionProgress, ConnectionError>,
        TestCapture,
        u64,
    ) {
        let frame_source = deterministic_frame_source();
        let (sleeper, _sleep_requests) = controlled_sleeper();
        let opening = test_opening(config, frame_source.as_ref());
        let mut next_sequence = 2;
        let (result, capture) = run_test_connection_with_capture(
            config,
            frame_source.as_ref(),
            sleeper.as_ref(),
            &opening,
            &mut next_sequence,
        )
        .await;
        (result, capture, next_sequence)
    }

    async fn run_test_connection(
        config: &Config,
        frame_source: &dyn FrameSource,
        sleeper: &dyn Sleeper,
        opening: &[u8],
        next_sequence: &mut u64,
    ) -> Result<ConnectionProgress, ConnectionError> {
        run_test_connection_with_capture(config, frame_source, sleeper, opening, next_sequence)
            .await
            .0
    }

    async fn run_test_connection_with_capture(
        config: &Config,
        frame_source: &dyn FrameSource,
        sleeper: &dyn Sleeper,
        opening: &[u8],
        next_sequence: &mut u64,
    ) -> (Result<ConnectionProgress, ConnectionError>, TestCapture) {
        let (recorder, capture) = test_recorder(BOOT_ID);
        let connection_event = recorder.start("runner.fixture_connection", []);
        let active_effect_event = ActiveEffectEvent::new();
        let result = run(
            ConnectionDependencies::new(
                config,
                frame_source,
                sleeper,
                &recorder,
                &connection_event,
                &active_effect_event,
            ),
            OpeningHello {
                boot_id: BOOT_ID,
                encoded: opening,
                message_id: OPENING_MESSAGE_ID,
                sequence: 1,
            },
            next_sequence,
        )
        .await;
        (result, capture)
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
