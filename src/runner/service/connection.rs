use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use opentelemetry::KeyValue;
use opentelemetry::propagation::Injector;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{
    HeaderMap, HeaderName, HeaderValue, StatusCode, header,
};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use crate::runner::service::assignment::{
    AssignmentManager, AssignmentManagerFailure, AssignmentOffer, AssignmentRenewal,
    AssignmentStart, PendingAssignmentObservation, WelcomePolicyFailure,
};
use crate::runner::service::config::Config;
use crate::runner::service::control::LiveStatus;
use crate::runner::service::{Sequence, Sleeper};
use crate::runner::telemetry::{self, Event, Outcome, Recorder};
use crate::runner_protocol::{
    CloudFrame, MAXIMUM_ENCODED_FRAME_BYTES, RunnerEnvelope, RunnerFrame, decode_cloud_frame,
    encode_runner_frame,
};

const SUBPROTOCOL: &str = "scherzo.runner.v1";
const MAX_INBOUND_MESSAGE_BYTES: usize = MAXIMUM_ENCODED_FRAME_BYTES;
const MAX_OUTBOUND_MESSAGE_BYTES: usize = MAXIMUM_ENCODED_FRAME_BYTES;
const OBSERVATION_WINDOW: usize = 32;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const WELCOME_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const RUNNER_PROTOCOL_EVENT_NAME: &str = "runner.gateway_protocol";

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

struct ProtocolLog<'a> {
    recorder: &'a Recorder,
    runner_id: &'a str,
    boot_id: &'a str,
    connection_attempt: u64,
    session_id: Option<String>,
    order: u64,
}

impl<'a> ProtocolLog<'a> {
    fn new(
        recorder: &'a Recorder,
        runner_id: &'a str,
        boot_id: &'a str,
        connection_attempt: u64,
    ) -> Self {
        Self {
            recorder,
            runner_id,
            boot_id,
            connection_attempt,
            session_id: None,
            order: 0,
        }
    }

    fn opening_hello(&mut self, opening: OpeningHello<'_>) {
        // The opening was encoded and schema-validated by this process. Keep
        // only its reviewed timestamp, never the raw frame or decode prose.
        let sent_at = serde_json::from_slice::<serde_json::Value>(opening.encoded)
            .ok()
            .and_then(|frame| frame.get("sentAt")?.as_str().map(str::to_owned));
        let mut attributes = protocol_text_attributes(
            "runner_to_cloud",
            "hello",
            opening.message_id,
            sent_at.as_deref(),
        );
        attributes.extend([
            KeyValue::new(
                telemetry::attribute::RUNNER_SEQUENCE,
                telemetry::integer(opening.sequence),
            ),
            KeyValue::new(
                telemetry::attribute::RUNNER_VERSION,
                crate::build_info::VERSION,
            ),
            KeyValue::new(telemetry::attribute::RUNNER_MAX_CONCURRENT_RUNS, 1_i64),
        ]);
        self.emit(attributes);
    }

    fn runner_text(&mut self, frame: &RunnerFrame) {
        let (envelope, frame_type, effect_id, assignment_id) = match frame {
            RunnerFrame::EffectAcknowledged {
                envelope,
                effect_id,
            } => (envelope, "effect_acknowledged", Some(effect_id), None),
            RunnerFrame::AssignmentAccepted {
                envelope,
                effect_id,
                assignment_id,
                ..
            } => (
                envelope,
                "assignment_accepted",
                Some(effect_id),
                Some(assignment_id),
            ),
            RunnerFrame::AssignmentRejected {
                envelope,
                effect_id,
                assignment_id,
                ..
            } => (
                envelope,
                "assignment_rejected",
                Some(effect_id),
                Some(assignment_id),
            ),
            RunnerFrame::AssignmentInterrupted {
                envelope,
                assignment_id,
                ..
            } => (
                envelope,
                "assignment_interrupted",
                None,
                Some(assignment_id),
            ),
            RunnerFrame::ExecutionLeaseRenewalRequested {
                envelope,
                assignment_id,
                ..
            } => (
                envelope,
                "execution_lease_renewal_requested",
                None,
                Some(assignment_id),
            ),
            RunnerFrame::ExecutionStarted {
                envelope,
                assignment_id,
                ..
            } => (envelope, "execution_started", None, Some(assignment_id)),
            RunnerFrame::ExecutionTransition {
                envelope,
                assignment_id,
                ..
            } => (envelope, "execution_transition", None, Some(assignment_id)),
            RunnerFrame::ExecutionFinished {
                envelope,
                assignment_id,
                ..
            } => (envelope, "execution_finished", None, Some(assignment_id)),
            RunnerFrame::ExecutionInterrupted {
                envelope,
                assignment_id,
                ..
            } => (envelope, "execution_interrupted", None, Some(assignment_id)),
            RunnerFrame::ExecutionAborted {
                envelope,
                assignment_id,
                ..
            } => (envelope, "execution_aborted", None, Some(assignment_id)),
            RunnerFrame::Hello { .. } => return,
        };
        let mut attributes = protocol_text_attributes(
            "runner_to_cloud",
            frame_type,
            &envelope.message_id,
            Some(&envelope.sent_at),
        );
        attributes.push(KeyValue::new(
            telemetry::attribute::RUNNER_SEQUENCE,
            telemetry::integer(envelope.sequence),
        ));
        if let Some(effect_id) = effect_id {
            attributes.push(KeyValue::new(
                telemetry::attribute::EFFECT_ID,
                effect_id.clone(),
            ));
        }
        if let Some(assignment_id) = assignment_id {
            attributes.push(KeyValue::new(
                telemetry::attribute::ASSIGNMENT_ID,
                assignment_id.clone(),
            ));
        }
        self.emit(attributes);
    }

    fn cloud_text(&mut self, frame: &CloudFrame) {
        let (envelope, frame_type, details) = match frame {
            CloudFrame::Welcome {
                envelope,
                session_id,
                ping_interval_seconds,
                pong_timeout_seconds,
                lease_policy: _,
            } => {
                self.session_id = Some(session_id.clone());
                (
                    envelope,
                    "welcome",
                    vec![
                        KeyValue::new(
                            telemetry::attribute::PROTOCOL_PING_INTERVAL_SECONDS,
                            telemetry::integer(*ping_interval_seconds),
                        ),
                        KeyValue::new(
                            telemetry::attribute::PROTOCOL_PONG_TIMEOUT_SECONDS,
                            telemetry::integer(*pong_timeout_seconds),
                        ),
                    ],
                )
            }
            CloudFrame::ObservationAck {
                envelope,
                acknowledged_message_id,
                acknowledged_sequence,
            } => (
                envelope,
                "observation_ack",
                vec![
                    KeyValue::new(
                        telemetry::attribute::PROTOCOL_ACKNOWLEDGED_MESSAGE_ID,
                        acknowledged_message_id.clone(),
                    ),
                    KeyValue::new(
                        telemetry::attribute::PROTOCOL_ACKNOWLEDGED_SEQUENCE,
                        telemetry::integer(*acknowledged_sequence),
                    ),
                ],
            ),
            CloudFrame::AssignmentOffer {
                envelope,
                effect_id,
                assignment_id,
                run_id,
                ..
            } => (
                envelope,
                "assignment_offer",
                vec![
                    KeyValue::new(telemetry::attribute::EFFECT_ID, effect_id.clone()),
                    KeyValue::new(telemetry::attribute::ASSIGNMENT_ID, assignment_id.clone()),
                    KeyValue::new(telemetry::attribute::RUN_ID, run_id.clone()),
                ],
            ),
            CloudFrame::AssignmentStart {
                envelope,
                effect_id,
                assignment_id,
                run_id,
                lease,
                ..
            } => (
                envelope,
                "assignment_start",
                leased_assignment_effect_attributes(
                    effect_id,
                    assignment_id,
                    run_id,
                    &lease.expires_at,
                ),
            ),
            CloudFrame::AssignmentLeaseRenewed {
                envelope,
                effect_id,
                assignment_id,
                run_id,
                lease,
                ..
            } => (
                envelope,
                "assignment_lease_renewed",
                leased_assignment_effect_attributes(
                    effect_id,
                    assignment_id,
                    run_id,
                    &lease.expires_at,
                ),
            ),
            CloudFrame::AssignmentRelease {
                envelope,
                effect_id,
                assignment_id,
                run_id,
                ..
            } => (
                envelope,
                "assignment_release",
                vec![
                    KeyValue::new(telemetry::attribute::EFFECT_ID, effect_id.clone()),
                    KeyValue::new(telemetry::attribute::ASSIGNMENT_ID, assignment_id.clone()),
                    KeyValue::new(telemetry::attribute::RUN_ID, run_id.clone()),
                ],
            ),
        };
        let mut attributes = protocol_text_attributes(
            "cloud_to_runner",
            frame_type,
            &envelope.message_id,
            Some(&envelope.sent_at),
        );
        attributes.extend(details);
        self.emit(attributes);
    }

    fn control(&mut self, direction: &'static str, kind: &'static str) {
        self.emit([
            KeyValue::new(telemetry::attribute::PROTOCOL_EVENT, "frame"),
            KeyValue::new(telemetry::attribute::PROTOCOL_DIRECTION, direction),
            KeyValue::new(telemetry::attribute::PROTOCOL_FRAME_KIND, kind),
        ]);
    }

    fn timer_expired(&mut self, timer: &'static str) {
        self.emit([
            KeyValue::new(telemetry::attribute::PROTOCOL_EVENT, "timer_expired"),
            KeyValue::new(telemetry::attribute::PROTOCOL_TIMER, timer),
        ]);
    }

    fn transport_ended(&mut self) {
        self.emit([KeyValue::new(
            telemetry::attribute::PROTOCOL_EVENT,
            "transport_ended",
        )]);
    }

    fn read_failed(&mut self) {
        self.emit([KeyValue::new(
            telemetry::attribute::PROTOCOL_EVENT,
            "read_failed",
        )]);
    }

    fn close(&mut self, direction: &'static str, initiator: &'static str, code: Option<u16>) {
        self.close_record("frame", direction, initiator, code);
    }

    fn close_write_failed(&mut self, direction: &'static str, initiator: &'static str, code: u16) {
        self.close_record("write_failed", direction, initiator, Some(code));
    }

    fn close_record(
        &mut self,
        event: &'static str,
        direction: &'static str,
        initiator: &'static str,
        code: Option<u16>,
    ) {
        let mut attributes = vec![
            KeyValue::new(telemetry::attribute::PROTOCOL_EVENT, event),
            KeyValue::new(telemetry::attribute::PROTOCOL_DIRECTION, direction),
            KeyValue::new(telemetry::attribute::PROTOCOL_FRAME_KIND, "close"),
            KeyValue::new(telemetry::attribute::PROTOCOL_CLOSE_INITIATOR, initiator),
        ];
        if let Some(code) = code {
            attributes.push(KeyValue::new(
                telemetry::attribute::PROTOCOL_CLOSE_CODE,
                i64::from(code),
            ));
        }
        self.emit(attributes);
    }

    fn emit(&mut self, attributes: impl IntoIterator<Item = KeyValue>) {
        self.order = self.order.saturating_add(1);
        let mut common = vec![
            KeyValue::new(telemetry::attribute::RUNNER_ID, self.runner_id.to_owned()),
            KeyValue::new(
                telemetry::attribute::RUNNER_BOOT_ID,
                self.boot_id.to_owned(),
            ),
            KeyValue::new(
                telemetry::attribute::CONNECTION_ATTEMPT,
                telemetry::integer(self.connection_attempt),
            ),
            KeyValue::new(
                telemetry::attribute::PROTOCOL_ORDER,
                telemetry::integer(self.order),
            ),
        ];
        if let Some(session_id) = &self.session_id {
            common.push(KeyValue::new(
                telemetry::attribute::RUNNER_SESSION_ID,
                session_id.clone(),
            ));
        }
        common.extend(attributes);
        self.recorder.record(RUNNER_PROTOCOL_EVENT_NAME, common);
    }
}

fn leased_assignment_effect_attributes(
    effect_id: &str,
    assignment_id: &str,
    run_id: &str,
    lease_expires_at: &str,
) -> Vec<KeyValue> {
    vec![
        KeyValue::new(telemetry::attribute::EFFECT_ID, effect_id.to_owned()),
        KeyValue::new(
            telemetry::attribute::ASSIGNMENT_ID,
            assignment_id.to_owned(),
        ),
        KeyValue::new(telemetry::attribute::RUN_ID, run_id.to_owned()),
        KeyValue::new(
            telemetry::attribute::PROTOCOL_LEASE_EXPIRES_AT,
            lease_expires_at.to_owned(),
        ),
    ]
}

fn protocol_text_attributes(
    direction: &'static str,
    frame_type: &'static str,
    message_id: &str,
    sent_at: Option<&str>,
) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new(telemetry::attribute::PROTOCOL_EVENT, "frame"),
        KeyValue::new(telemetry::attribute::PROTOCOL_DIRECTION, direction),
        KeyValue::new(telemetry::attribute::PROTOCOL_FRAME_KIND, "text"),
        KeyValue::new(telemetry::attribute::PROTOCOL_FRAME_TYPE, frame_type),
        KeyValue::new(telemetry::attribute::PROTOCOL_VERSION, 1_i64),
        KeyValue::new(telemetry::attribute::PROTOCOL_PAYLOAD_VERSION, 1_i64),
        KeyValue::new(
            telemetry::attribute::PROTOCOL_MESSAGE_ID,
            message_id.to_owned(),
        ),
    ];
    if let Some(sent_at) = sent_at {
        attributes.push(KeyValue::new(
            telemetry::attribute::PROTOCOL_SENT_AT,
            sent_at.to_owned(),
        ));
    }
    attributes
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
    TerminalAuthentication,
    TerminalProtocol,
}

impl FailureKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::TerminalAuthentication => "terminal_authentication",
            Self::TerminalProtocol => "terminal_protocol",
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
    GatewayRateLimited,
    GatewayUnavailable,
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
    InvalidExecutionLeasePolicy,
    ChangedExecutionLeasePolicy,
    ConflictingAssignmentOffer,
    AssignmentDecisionCapacity,
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
            Self::GatewayRateLimited => "runner gateway rate limited the connection request",
            Self::GatewayUnavailable => "runner gateway is unavailable",
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
            Self::InvalidExecutionLeasePolicy => "invalid execution lease policy",
            Self::ChangedExecutionLeasePolicy => {
                "execution lease policy changed within runner boot"
            }
            Self::ConflictingAssignmentOffer => "conflicting assignment offer",
            Self::AssignmentDecisionCapacity => "assignment decision capacity exhausted",
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
            Self::GatewayRateLimited => "gateway_rate_limited",
            Self::GatewayUnavailable => "gateway_unavailable",
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
            Self::InvalidExecutionLeasePolicy => "invalid_execution_lease_policy",
            Self::ChangedExecutionLeasePolicy => "changed_execution_lease_policy",
            Self::ConflictingAssignmentOffer => "conflicting_assignment_offer",
            Self::AssignmentDecisionCapacity => "assignment_decision_capacity",
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
            kind: FailureKind::TerminalProtocol,
            cause,
        }
    }

    const fn terminal_authentication(progress: ConnectionProgress, cause: ConnectionCause) -> Self {
        Self {
            progress,
            kind: FailureKind::TerminalAuthentication,
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
        !matches!(self.kind, FailureKind::Retryable)
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
    protocol: &mut ProtocolLog<'_>,
    code: CloseCode,
    reason: &'static str,
) where
    W: Sink<Message, Error = WebSocketError> + Unpin,
{
    let close = Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    }));
    let send_result = tokio::select! {
        biased;
        result = writer.send(close) => Some(result),
        _ = sleeper.sleep(CLOSE_TIMEOUT) => None,
    };
    match send_result {
        Some(Ok(())) => protocol.close("runner_to_cloud", "runner", Some(u16::from(code))),
        Some(Err(_)) => {
            protocol.close_write_failed("runner_to_cloud", "runner", u16::from(code));
        }
        None => protocol.timer_expired("close"),
    }
}

// protocol_violation closes locally with status 1002 and reports a terminal
// protocol ending. Retrying unchanged software or configuration cannot make a
// malformed Cloud protocol stream safe.
async fn protocol_violation<W>(
    writer: &mut W,
    sleeper: &dyn Sleeper,
    protocol: &mut ProtocolLog<'_>,
    progress: ConnectionProgress,
    cause: ConnectionCause,
) -> ConnectionError
where
    W: Sink<Message, Error = WebSocketError> + Unpin,
{
    close_locally(
        writer,
        sleeper,
        protocol,
        CloseCode::Protocol,
        cause.message(),
    )
    .await;
    ConnectionError::terminal(progress, cause)
}

type RunnerSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(super) struct CandidateTransport(RunnerSocket);

pub(super) struct CandidateConnection {
    socket: RunnerSocket,
    progress: ConnectionProgress,
    inbound_silence_timeout: Duration,
    protocol_session_id: String,
    protocol_order: u64,
}

pub(crate) async fn run(
    dependencies: ConnectionDependencies<'_>,
    opening: OpeningHello<'_>,
    next_sequence: &Sequence,
) -> Result<ConnectionProgress, ConnectionError> {
    let active_effect_event = dependencies.active_effect_event;
    let socket = connect_gateway(dependencies).await?;
    let (writer, reader) = socket.split();
    let result = run_established_shared(dependencies, opening, next_sequence, reader, writer).await;
    active_effect_event.finish_connection_end(&result);
    result
}

async fn connect_gateway(
    dependencies: ConnectionDependencies<'_>,
) -> Result<RunnerSocket, ConnectionError> {
    let config = dependencies.config;
    let sleeper = dependencies.sleeper;
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
                StatusCode::UNAUTHORIZED => ConnectionError::terminal_authentication(
                    unacknowledged,
                    ConnectionCause::CredentialRejected,
                ),
                StatusCode::BAD_REQUEST => ConnectionError::terminal(
                    unacknowledged,
                    ConnectionCause::ConnectionRequestRejected,
                ),
                StatusCode::TOO_MANY_REQUESTS => {
                    ConnectionError::retryable(unacknowledged, ConnectionCause::GatewayRateLimited)
                }
                status if status.is_server_error() => {
                    ConnectionError::retryable(unacknowledged, ConnectionCause::GatewayUnavailable)
                }
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
    Ok(socket)
}

pub(super) async fn connect_candidate_transport(
    dependencies: ConnectionDependencies<'_>,
) -> Result<CandidateTransport, ConnectionError> {
    connect_gateway(dependencies).await.map(CandidateTransport)
}

pub(super) async fn authenticate_candidate(
    dependencies: ConnectionDependencies<'_>,
    transport: CandidateTransport,
    opening: OpeningHello<'_>,
) -> Result<CandidateConnection, ConnectionError> {
    let CandidateTransport(mut socket) = transport;
    let mut protocol = ProtocolLog::new(
        dependencies.recorder,
        dependencies.config.credential().runner_id(),
        opening.boot_id,
        dependencies.connection_attempt,
    );
    let unacknowledged = ConnectionProgress::unacknowledged();
    let hello = std::str::from_utf8(opening.encoded).map_err(|_| {
        ConnectionError::terminal(unacknowledged, ConnectionCause::EncodeOpeningHelloUtf8)
    })?;
    socket
        .send(Message::Text(hello.into()))
        .await
        .map_err(|_| {
            ConnectionError::retryable(unacknowledged, ConnectionCause::SendOpeningHello)
        })?;
    protocol.opening_hello(opening);
    let mut progress = ConnectionProgress::unacknowledged();
    progress.runner_text_frames_sent = 1;
    record_progress(dependencies.connection_event, progress);
    let welcome_timeout = dependencies.sleeper.sleep(WELCOME_TIMEOUT);
    tokio::pin!(welcome_timeout);
    // A candidate must stop at valid welcome and return its live socket without
    // entering assignment transport before protected-state promotion. This
    // bounded pre-welcome parser therefore repeats only the common base-frame
    // handling rather than reusing the effect-capable established loop.
    // jscpd:ignore-start
    loop {
        let message = tokio::select! {
            biased;
            message = socket.next() => message,
            () = &mut welcome_timeout => {
                protocol.timer_expired("welcome");
                return Err(ConnectionError::retryable(
                    progress,
                    ConnectionCause::GatewayWelcomeTimeout,
                ));
            }
        };
        let Some(message) = message else {
            return Err(ConnectionError::retryable(
                progress,
                ConnectionCause::ReadGatewayFrame,
            ));
        };
        let message = match message {
            Ok(message) => message,
            Err(WebSocketError::Capacity(_)) => {
                return Err(ConnectionError::terminal(
                    progress,
                    ConnectionCause::OversizedGatewayFrame,
                ));
            }
            Err(_) => {
                return Err(ConnectionError::retryable(
                    progress,
                    ConnectionCause::ReadGatewayFrame,
                ));
            }
        };
        let frame = match message {
            Message::Text(text) => {
                let frame = decode_cloud_frame(text.as_bytes()).map_err(|_| {
                    ConnectionError::terminal(progress, ConnectionCause::UndecodableGatewayFrame)
                })?;
                protocol.cloud_text(&frame);
                progress.cloud_text_frames_received =
                    progress.incremented(progress.cloud_text_frames_received)?;
                record_progress(dependencies.connection_event, progress);
                frame
            }
            Message::Ping(_) => {
                protocol.control("cloud_to_runner", "ping");
                socket.flush().await.map_err(|_| {
                    ConnectionError::retryable(progress, ConnectionCause::FlushRunnerPong)
                })?;
                protocol.control("runner_to_cloud", "pong");
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(close) => {
                return match close_outcome(progress, close) {
                    Ok(progress) => Err(ConnectionError::retryable(
                        progress,
                        ConnectionCause::GatewayClosedConnection,
                    )),
                    Err(error) => Err(error),
                };
            }
            Message::Binary(_) | Message::Frame(_) => {
                return Err(ConnectionError::terminal(
                    progress,
                    ConnectionCause::UnexpectedRawGatewayFrame,
                ));
            }
        };
        match frame {
            CloudFrame::Welcome {
                session_id,
                pong_timeout_seconds,
                lease_policy,
                ..
            } => {
                let policy = dependencies
                    .assignment_manager
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .retain_lease_policy(&lease_policy);
                let cause = match policy {
                    Ok(()) => None,
                    Err(WelcomePolicyFailure::Invalid) => {
                        Some(ConnectionCause::InvalidExecutionLeasePolicy)
                    }
                    Err(WelcomePolicyFailure::Changed) => {
                        Some(ConnectionCause::ChangedExecutionLeasePolicy)
                    }
                };
                if let Some(cause) = cause {
                    return Err(ConnectionError::terminal(progress, cause));
                }
                return Ok(CandidateConnection {
                    socket,
                    progress,
                    inbound_silence_timeout: Duration::from_secs(pong_timeout_seconds),
                    protocol_session_id: session_id,
                    protocol_order: protocol.order,
                });
            }
            CloudFrame::ObservationAck {
                acknowledged_message_id,
                acknowledged_sequence,
                ..
            } if acknowledged_message_id == opening.message_id
                && acknowledged_sequence == opening.sequence =>
            {
                progress.opening_acknowledged = true;
                record_progress(dependencies.connection_event, progress);
            }
            _ => {
                return Err(ConnectionError::terminal(
                    progress,
                    ConnectionCause::UnexpectedGatewayFrame,
                ));
            }
        }
    }
    // jscpd:ignore-end
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingObservationKind {
    EffectReceipt,
    AssignmentObservation { id: u64 },
}

struct PendingObservation {
    message_id: String,
    sequence: u64,
    kind: PendingObservationKind,
}

struct ObservationTransport<'a> {
    assignment_manager: &'a Mutex<AssignmentManager>,
}

impl Drop for ObservationTransport<'_> {
    fn drop(&mut self) {
        self.assignment_manager
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .finish_transport();
    }
}

enum AssignmentManagerEffect {
    Offer(AssignmentOffer),
    Start(AssignmentStart),
    Renewal(AssignmentRenewal),
    Release {
        assignment_id: String,
        run_id: String,
        attempt_id: String,
        reason: String,
    },
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

#[derive(Clone, Copy)]
pub(super) struct ConnectionDependencies<'a> {
    config: &'a Config,
    frame_source: &'a dyn FrameSource,
    sleeper: &'a dyn Sleeper,
    recorder: &'a Recorder,
    connection_event: &'a Event,
    active_effect_event: &'a ActiveEffectEvent,
    assignment_manager: &'a Mutex<AssignmentManager>,
    connection_attempt: u64,
    live_status: Option<&'a LiveStatus>,
}

impl<'a> ConnectionDependencies<'a> {
    #[expect(
        clippy::too_many_arguments,
        reason = "the connection adapter receives explicit service and attempt-scoped boundaries"
    )]
    pub(super) fn new(
        config: &'a Config,
        frame_source: &'a dyn FrameSource,
        sleeper: &'a dyn Sleeper,
        recorder: &'a Recorder,
        connection_event: &'a Event,
        active_effect_event: &'a ActiveEffectEvent,
        assignment_manager: &'a Mutex<AssignmentManager>,
        connection_attempt: u64,
    ) -> Self {
        Self {
            config,
            frame_source,
            sleeper,
            recorder,
            connection_event,
            active_effect_event,
            assignment_manager,
            connection_attempt,
            live_status: None,
        }
    }

    pub(super) fn with_live_status(mut self, live_status: &'a LiveStatus) -> Self {
        self.live_status = Some(live_status);
        self
    }
}

// Deterministic transcript fixtures retain a plain mutable counter while
// production shares one sequence allocator across candidate connections.
// jscpd:ignore-start
#[cfg(test)]
pub(super) async fn run_established<R, W>(
    dependencies: ConnectionDependencies<'_>,
    opening: OpeningHello<'_>,
    next_sequence: &mut u64,
    reader: R,
    writer: W,
) -> Result<ConnectionProgress, ConnectionError>
where
    R: Stream<Item = Result<Message, WebSocketError>> + Unpin,
    W: Sink<Message, Error = WebSocketError> + Unpin,
{
    let sequence = Sequence::new(*next_sequence);
    let result = run_established_shared(dependencies, opening, &sequence, reader, writer).await;
    *next_sequence = sequence.peek();
    result
}
// jscpd:ignore-end

pub(super) async fn run_established_shared<R, W>(
    dependencies: ConnectionDependencies<'_>,
    opening: OpeningHello<'_>,
    next_sequence: &Sequence,
    reader: R,
    writer: W,
) -> Result<ConnectionProgress, ConnectionError>
where
    R: Stream<Item = Result<Message, WebSocketError>> + Unpin,
    W: Sink<Message, Error = WebSocketError> + Unpin,
{
    run_established_inner(dependencies, opening, next_sequence, reader, writer, None).await
}

pub(super) async fn run_promoted(
    dependencies: ConnectionDependencies<'_>,
    opening: OpeningHello<'_>,
    next_sequence: &Sequence,
    candidate: CandidateConnection,
) -> Result<ConnectionProgress, ConnectionError> {
    let active_effect_event = dependencies.active_effect_event;
    let CandidateConnection {
        socket,
        progress,
        inbound_silence_timeout,
        protocol_session_id,
        protocol_order,
    } = candidate;
    let (writer, reader) = socket.split();
    let result = run_established_inner(
        dependencies,
        opening,
        next_sequence,
        reader,
        writer,
        Some((
            progress,
            inbound_silence_timeout,
            protocol_session_id,
            protocol_order,
        )),
    )
    .await;
    active_effect_event.finish_connection_end(&result);
    result
}

async fn run_established_inner<R, W>(
    dependencies: ConnectionDependencies<'_>,
    opening: OpeningHello<'_>,
    next_sequence: &Sequence,
    mut reader: R,
    mut writer: W,
    resumed: Option<(ConnectionProgress, Duration, String, u64)>,
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
        assignment_manager,
        connection_attempt,
        live_status,
    } = dependencies;
    let _observation_transport = ObservationTransport { assignment_manager };
    let mut protocol = ProtocolLog::new(
        recorder,
        config.credential().runner_id(),
        opening.boot_id,
        connection_attempt,
    );
    let resumed_transport = resumed.map(|(progress, timeout, session_id, order)| {
        protocol.session_id = Some(session_id);
        protocol.order = order;
        (progress, timeout)
    });
    let unacknowledged = ConnectionProgress::unacknowledged();
    let mut welcome_timer = sleeper.sleep(WELCOME_TIMEOUT);
    let (mut progress, mut inbound_silence_timeout) =
        if let Some((mut progress, timeout)) = resumed_transport {
            if progress.opening_acknowledged {
                progress.handshake_completed = true;
                record_progress(connection_event, progress);
                if let Some(status) = live_status {
                    status.connected(frame_source.utc_timestamp().ok());
                }
            }
            (progress, Some(timeout))
        } else {
            let opening_hello = std::str::from_utf8(opening.encoded).map_err(|_| {
                ConnectionError::terminal(unacknowledged, ConnectionCause::EncodeOpeningHelloUtf8)
            })?;
            writer
                .send(Message::Text(opening_hello.into()))
                .await
                .map_err(|_| {
                    ConnectionError::retryable(unacknowledged, ConnectionCause::SendOpeningHello)
                })?;
            protocol.opening_hello(opening);
            let mut progress = ConnectionProgress::unacknowledged();
            progress.runner_text_frames_sent =
                progress.incremented(progress.runner_text_frames_sent)?;
            record_progress(connection_event, progress);
            (progress, None)
        };
    let mut in_flight = VecDeque::<PendingObservation>::new();
    let mut buffered_effect: Option<CloudFrame> = None;
    let assignment_notification = assignment_manager
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .notification();

    loop {
        if progress.handshake_completed && in_flight.len() < OBSERVATION_WINDOW {
            if let Some(effect) = buffered_effect.take() {
                let pending = send_effect_receipt(
                    &mut writer,
                    config,
                    frame_source,
                    opening.boot_id,
                    next_sequence,
                    &mut protocol,
                    &mut progress,
                    connection_event,
                    active_effect_event,
                    assignment_manager,
                    recorder,
                    effect,
                )
                .await?;
                in_flight.push_back(pending);
                continue;
            }

            let in_flight_ids: BTreeSet<_> = in_flight
                .iter()
                .filter_map(|pending| match pending.kind {
                    PendingObservationKind::AssignmentObservation { id } => Some(id),
                    PendingObservationKind::EffectReceipt => None,
                })
                .collect();
            let available = OBSERVATION_WINDOW - in_flight.len();
            let pending = assignment_manager
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pending_observations(&in_flight_ids, available);
            if !pending.is_empty() {
                for observation in pending {
                    let pending = send_assignment_observation(
                        &mut writer,
                        config,
                        frame_source,
                        opening.boot_id,
                        next_sequence,
                        &mut protocol,
                        &mut progress,
                        connection_event,
                        assignment_manager,
                        observation,
                    )
                    .await?;
                    in_flight.push_back(pending);
                }
                continue;
            }
        }

        let notified = assignment_notification.notified();
        tokio::pin!(notified);
        let message = if let Some(timeout) = inbound_silence_timeout {
            tokio::select! {
                biased;
                message = reader.next() => message,
                () = &mut notified => continue,
                _ = sleeper.sleep(timeout) => {
                    protocol.timer_expired("inbound_silence");
                    close_locally(
                        &mut writer,
                        sleeper,
                        &mut protocol,
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
                () = &mut notified => continue,
                _ = &mut welcome_timer => {
                    protocol.timer_expired("welcome");
                    return Err(ConnectionError::retryable(
                        progress,
                        ConnectionCause::GatewayWelcomeTimeout,
                    ));
                }
            }
        };
        let Some(message) = message else {
            protocol.transport_ended();
            return Ok(progress);
        };
        let message = match message {
            Ok(message) => message,
            Err(WebSocketError::Capacity(_)) => {
                return Err(protocol_violation(
                    &mut writer,
                    sleeper,
                    &mut protocol,
                    progress,
                    ConnectionCause::OversizedGatewayFrame,
                )
                .await);
            }
            Err(_) => {
                protocol.read_failed();
                return Err(ConnectionError::retryable(
                    progress,
                    ConnectionCause::ReadGatewayFrame,
                ));
            }
        };
        let frame = match message {
            Message::Text(text) => {
                let Ok(frame) = decode_cloud_frame(text.as_bytes()) else {
                    return Err(protocol_violation(
                        &mut writer,
                        sleeper,
                        &mut protocol,
                        progress,
                        ConnectionCause::UndecodableGatewayFrame,
                    )
                    .await);
                };
                protocol.cloud_text(&frame);
                progress.cloud_text_frames_received =
                    progress.incremented(progress.cloud_text_frames_received)?;
                record_progress(connection_event, progress);
                frame
            }
            Message::Ping(_) => {
                protocol.control("cloud_to_runner", "ping");
                writer.flush().await.map_err(|_| {
                    ConnectionError::retryable(progress, ConnectionCause::FlushRunnerPong)
                })?;
                protocol.control("runner_to_cloud", "pong");
                continue;
            }
            Message::Pong(_) => {
                protocol.control("cloud_to_runner", "pong");
                continue;
            }
            Message::Close(close) => {
                protocol.close(
                    "cloud_to_runner",
                    "gateway",
                    close.as_ref().map(|frame| u16::from(frame.code)),
                );
                return close_outcome(progress, close);
            }
            Message::Binary(_) => {
                return Err(protocol_violation(
                    &mut writer,
                    sleeper,
                    &mut protocol,
                    progress,
                    ConnectionCause::BinaryGatewayFrame,
                )
                .await);
            }
            Message::Frame(_) => {
                return Err(protocol_violation(
                    &mut writer,
                    sleeper,
                    &mut protocol,
                    progress,
                    ConnectionCause::UnexpectedRawGatewayFrame,
                )
                .await);
            }
        };

        match frame {
            CloudFrame::Welcome {
                ping_interval_seconds,
                pong_timeout_seconds,
                lease_policy,
                ..
            } if inbound_silence_timeout.is_none() => {
                let policy_result = assignment_manager
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .retain_lease_policy(&lease_policy);
                let cause = match policy_result {
                    Ok(()) => None,
                    Err(WelcomePolicyFailure::Invalid) => {
                        Some(ConnectionCause::InvalidExecutionLeasePolicy)
                    }
                    Err(WelcomePolicyFailure::Changed) => {
                        Some(ConnectionCause::ChangedExecutionLeasePolicy)
                    }
                };
                if let Some(cause) = cause {
                    return Err(protocol_violation(
                        &mut writer,
                        sleeper,
                        &mut protocol,
                        progress,
                        cause,
                    )
                    .await);
                }
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
                let Some(pending) = in_flight.front() else {
                    return Err(protocol_violation(
                        &mut writer,
                        sleeper,
                        &mut protocol,
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
                        &mut protocol,
                        progress,
                        ConnectionCause::MismatchedEffectAcknowledgement,
                    )
                    .await);
                }
                let kind = pending.kind;
                in_flight.pop_front();
                match kind {
                    PendingObservationKind::EffectReceipt => {
                        progress.effect_acknowledgements_confirmed = match progress
                            .incremented(progress.effect_acknowledgements_confirmed)
                        {
                            Ok(count) => count,
                            Err(error) => {
                                active_effect_event.finish(
                                    Outcome::Failure,
                                    Some(ConnectionCause::ConnectionCounterOverflow),
                                );
                                return Err(error);
                            }
                        };
                        record_progress(connection_event, progress);
                        active_effect_event.finish(Outcome::Success, None);
                    }
                    PendingObservationKind::AssignmentObservation { id } => {
                        assignment_manager
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .acknowledge_observation(id);
                    }
                }
            }
            effect @ CloudFrame::AssignmentOffer { .. }
            | effect @ CloudFrame::AssignmentStart { .. }
            | effect @ CloudFrame::AssignmentLeaseRenewed { .. }
            | effect @ CloudFrame::AssignmentRelease { .. }
                if progress.handshake_completed && buffered_effect.is_none() =>
            {
                buffered_effect = Some(effect);
            }
            _ => {
                return Err(protocol_violation(
                    &mut writer,
                    sleeper,
                    &mut protocol,
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
            if let Some(status) = live_status {
                status.connected(frame_source.utc_timestamp().ok());
            }
        }
    }
}

// Effect receipts and semantic observations have different telemetry and state effects;
// explicit sender boundaries are clearer than one mode-switched transport operation.
// jscpd:ignore-start
#[expect(
    clippy::too_many_arguments,
    reason = "effect receipt binds transport, telemetry, and service-scope assignment state"
)]
async fn send_effect_receipt<W>(
    writer: &mut W,
    config: &Config,
    frame_source: &dyn FrameSource,
    boot_id: &str,
    next_sequence: &Sequence,
    protocol: &mut ProtocolLog<'_>,
    progress: &mut ConnectionProgress,
    connection_event: &Event,
    active_effect_event: &ActiveEffectEvent,
    assignment_manager: &Mutex<AssignmentManager>,
    recorder: &Recorder,
    effect: CloudFrame,
) -> Result<PendingObservation, ConnectionError>
where
    W: Sink<Message, Error = WebSocketError> + Unpin,
{
    // jscpd:ignore-end
    let (effect_id, assignment_id, run_id, manager_effect) = match effect {
        CloudFrame::AssignmentOffer {
            effect_id,
            assignment_id,
            run_id,
            attempt_id,
            execution_spec,
            ..
        } => {
            let offer = AssignmentOffer {
                effect_id: effect_id.clone(),
                assignment_id: assignment_id.clone(),
                run_id: run_id.clone(),
                attempt_id,
                execution_spec: *execution_spec,
            };
            (
                effect_id,
                assignment_id,
                run_id,
                AssignmentManagerEffect::Offer(offer),
            )
        }
        CloudFrame::AssignmentStart {
            effect_id,
            assignment_id,
            run_id,
            attempt_id,
            execution_spec_id,
            lease,
            ..
        } => {
            let start = AssignmentStart {
                effect_id: effect_id.clone(),
                assignment_id: assignment_id.clone(),
                run_id: run_id.clone(),
                attempt_id,
                execution_spec_id,
                lease,
            };
            (
                effect_id,
                assignment_id,
                run_id,
                AssignmentManagerEffect::Start(start),
            )
        }
        CloudFrame::AssignmentLeaseRenewed {
            effect_id,
            assignment_id,
            run_id,
            attempt_id,
            lease,
            ..
        } => {
            let renewal = AssignmentRenewal {
                effect_id: effect_id.clone(),
                assignment_id: assignment_id.clone(),
                run_id: run_id.clone(),
                attempt_id,
                lease,
            };
            (
                effect_id,
                assignment_id,
                run_id,
                AssignmentManagerEffect::Renewal(renewal),
            )
        }
        CloudFrame::AssignmentRelease {
            effect_id,
            assignment_id,
            run_id,
            attempt_id,
            reason,
            ..
        } => {
            let release_assignment_id = assignment_id.clone();
            let release_run_id = run_id.clone();
            (
                effect_id,
                assignment_id,
                run_id,
                AssignmentManagerEffect::Release {
                    assignment_id: release_assignment_id,
                    run_id: release_run_id,
                    attempt_id,
                    reason,
                },
            )
        }
        _ => {
            return Err(ConnectionError::terminal(
                *progress,
                ConnectionCause::UnexpectedGatewayFrame,
            ));
        }
    };

    let emission = next_sequence.lock_emission().await;
    let sequence = next_sequence.peek();
    let event = recorder.start(
        "runner.effect_acknowledgement",
        [
            KeyValue::new(telemetry::attribute::EFFECT_ID, effect_id.clone()),
            KeyValue::new(telemetry::attribute::ASSIGNMENT_ID, assignment_id.clone()),
            KeyValue::new(telemetry::attribute::RUN_ID, run_id),
            KeyValue::new(
                telemetry::attribute::RUNNER_ID,
                config.credential().runner_id().to_owned(),
            ),
            KeyValue::new(telemetry::attribute::RUNNER_BOOT_ID, boot_id.to_owned()),
            KeyValue::new(
                telemetry::attribute::RUNNER_SEQUENCE,
                telemetry::integer(sequence),
            ),
        ],
    );
    active_effect_event.start(event.clone());
    progress.effects_received = match progress.incremented(progress.effects_received) {
        Ok(count) => count,
        Err(error) => {
            finish_effect_failure(&event, ConnectionCause::ConnectionCounterOverflow);
            return Err(error);
        }
    };
    record_progress(connection_event, *progress);
    let envelope = match next_envelope(config, frame_source, boot_id, next_sequence, progress) {
        Ok(envelope) => envelope,
        Err(error) => {
            finish_effect_failure(&event, error.connection_cause());
            return Err(error);
        }
    };
    let frame = RunnerFrame::EffectAcknowledged {
        envelope,
        effect_id,
    };
    let pending = send_runner_frame(
        writer,
        protocol,
        progress,
        connection_event,
        &event,
        frame,
        PendingObservationKind::EffectReceipt,
    )
    .await?;
    drop(emission);

    let manager_result = {
        let mut manager = assignment_manager
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match manager_effect {
            AssignmentManagerEffect::Offer(offer) => manager.handle_offer(offer).map(|_| None),
            AssignmentManagerEffect::Start(start) => manager.handle_start(start),
            AssignmentManagerEffect::Renewal(renewal) => {
                manager.handle_renewal(renewal).map(|_| None)
            }
            AssignmentManagerEffect::Release {
                assignment_id,
                run_id,
                attempt_id,
                reason,
            } => manager
                .handle_release(&assignment_id, &run_id, &attempt_id, &reason)
                .map(|_| None),
        }
    };
    match manager_result {
        Ok(Some(job)) => job.spawn(),
        Ok(None) => {}
        Err(failure) => {
            let cause = match failure {
                AssignmentManagerFailure::ConflictingOffer => {
                    ConnectionCause::ConflictingAssignmentOffer
                }
                AssignmentManagerFailure::DecisionCapacity => {
                    ConnectionCause::AssignmentDecisionCapacity
                }
            };
            return Err(ConnectionError::retryable(*progress, cause));
        }
    }
    Ok(pending)
}

fn next_envelope(
    config: &Config,
    frame_source: &dyn FrameSource,
    boot_id: &str,
    next_sequence: &Sequence,
    progress: &ConnectionProgress,
) -> Result<RunnerEnvelope, ConnectionError> {
    let sequence = next_sequence.next().map_err(|_| {
        ConnectionError::terminal(*progress, ConnectionCause::ObservationSequenceOverflow)
    })?;
    let sent_at = frame_source.utc_timestamp().map_err(|_| {
        ConnectionError::terminal(
            *progress,
            ConnectionCause::FormatEffectAcknowledgementTimestamp,
        )
    })?;
    Ok(RunnerEnvelope {
        message_id: frame_source.public_id("rmsg_"),
        runner_id: config.credential().runner_id().to_owned(),
        boot_id: boot_id.to_owned(),
        sequence,
        sent_at,
    })
}

// This boundary intentionally remains separate from effect-receipt delivery above.
// jscpd:ignore-start
#[expect(
    clippy::too_many_arguments,
    reason = "semantic observation delivery owns the complete transport envelope"
)]
async fn send_assignment_observation<W>(
    writer: &mut W,
    config: &Config,
    frame_source: &dyn FrameSource,
    boot_id: &str,
    next_sequence: &Sequence,
    protocol: &mut ProtocolLog<'_>,
    progress: &mut ConnectionProgress,
    connection_event: &Event,
    assignment_manager: &Mutex<AssignmentManager>,
    pending: PendingAssignmentObservation,
) -> Result<PendingObservation, ConnectionError>
where
    W: Sink<Message, Error = WebSocketError> + Unpin,
{
    // jscpd:ignore-end
    let emission = next_sequence.lock_emission().await;
    let envelope = next_envelope(config, frame_source, boot_id, next_sequence, progress)?;
    let sequence = envelope.sequence;
    let message_id = envelope.message_id.clone();
    let frame = pending.observation.runner_frame(envelope);
    let encoded = encode_runner_frame(&frame).map_err(|_| {
        ConnectionError::terminal(*progress, ConnectionCause::EncodeEffectAcknowledgement)
    })?;
    if encoded.len() > MAX_OUTBOUND_MESSAGE_BYTES {
        return Err(ConnectionError::terminal(
            *progress,
            ConnectionCause::EncodeEffectAcknowledgement,
        ));
    }
    let encoded = std::str::from_utf8(&encoded).map_err(|_| {
        ConnectionError::terminal(*progress, ConnectionCause::EncodeEffectAcknowledgementUtf8)
    })?;
    writer
        .send(Message::Text(encoded.into()))
        .await
        .map_err(|_| {
            ConnectionError::retryable(*progress, ConnectionCause::SendEffectAcknowledgement)
        })?;
    drop(emission);
    protocol.runner_text(&frame);
    progress.runner_text_frames_sent = progress.incremented(progress.runner_text_frames_sent)?;
    record_progress(connection_event, *progress);
    assignment_manager
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .mark_observation_encoded(pending.id);
    Ok(PendingObservation {
        message_id,
        sequence,
        kind: PendingObservationKind::AssignmentObservation { id: pending.id },
    })
}

async fn send_runner_frame<W>(
    writer: &mut W,
    protocol: &mut ProtocolLog<'_>,
    progress: &mut ConnectionProgress,
    connection_event: &Event,
    event: &Event,
    frame: RunnerFrame,
    kind: PendingObservationKind,
) -> Result<PendingObservation, ConnectionError>
where
    W: Sink<Message, Error = WebSocketError> + Unpin,
{
    let envelope = match &frame {
        RunnerFrame::EffectAcknowledged { envelope, .. } => envelope,
        _ => unreachable!("effect sender received a semantic frame"),
    };
    let message_id = envelope.message_id.clone();
    let sequence = envelope.sequence;
    let encoded = encode_runner_frame(&frame).map_err(|_| {
        finish_effect_failure(event, ConnectionCause::EncodeEffectAcknowledgement);
        ConnectionError::terminal(*progress, ConnectionCause::EncodeEffectAcknowledgement)
    })?;
    if encoded.len() > MAX_OUTBOUND_MESSAGE_BYTES {
        finish_effect_failure(event, ConnectionCause::EncodeEffectAcknowledgement);
        return Err(ConnectionError::terminal(
            *progress,
            ConnectionCause::EncodeEffectAcknowledgement,
        ));
    }
    let encoded = std::str::from_utf8(&encoded).map_err(|_| {
        finish_effect_failure(event, ConnectionCause::EncodeEffectAcknowledgementUtf8);
        ConnectionError::terminal(*progress, ConnectionCause::EncodeEffectAcknowledgementUtf8)
    })?;
    if writer.send(Message::Text(encoded.into())).await.is_err() {
        finish_effect_failure(event, ConnectionCause::SendEffectAcknowledgement);
        return Err(ConnectionError::retryable(
            *progress,
            ConnectionCause::SendEffectAcknowledgement,
        ));
    }
    protocol.runner_text(&frame);
    progress.runner_text_frames_sent = progress.incremented(progress.runner_text_frames_sent)?;
    record_progress(connection_event, *progress);
    Ok(PendingObservation {
        message_id,
        sequence,
        kind,
    })
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
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures_util::{Sink, SinkExt, StreamExt};
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::{Notify, mpsc};
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::Error as WebSocketError;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
    use tokio_tungstenite::tungstenite::http::{HeaderValue, header};
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    use super::{
        ActiveEffectEvent, ConnectionCause, ConnectionDependencies, ConnectionError,
        ConnectionProgress, FailureKind, FrameSource, OBSERVATION_WINDOW, OpeningHello,
        ProtocolLog, RUNNER_PROTOCOL_EVENT_NAME, close_locally, close_outcome, opening_hello, run,
        run_established,
    };
    use crate::runner::credential::test_credential;
    use crate::runner::service::assignment::AssignmentManager;
    use crate::runner::service::config::Config;
    use crate::runner::service::test_support::{
        DeterminismTranscript, SleepRelease, accept_fixture_socket, accept_opened_fixture_socket,
        assignment_offer, controlled_sleeper, deterministic_frame_source, effect_acknowledgement,
        expect_close_frame, expect_opening_hello, fixture_listener, healthy_wall_clock,
        observation_acknowledgement, offer_assignment_after_handshake, scripted_duplex,
        sleep_request, welcome, with_watchdog,
    };
    use crate::runner::service::{Sequence, Sleeper};
    use crate::runner::telemetry::{Event, Outcome, Recorder, TestCapture, test_recorder};

    const BOOT_ID: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe";
    const OPENING_MESSAGE_ID: &str = "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc";

    struct EstablishedTestContext {
        config: Config,
        frame_source: Arc<dyn FrameSource>,
        sleeper: Arc<dyn Sleeper>,
        recorder: Arc<Recorder>,
        capture: TestCapture,
        connection_event: Event,
        active_effect_event: ActiveEffectEvent,
        assignment_manager: Mutex<AssignmentManager>,
        _sleep_requests: mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
        opening: Vec<u8>,
    }

    impl EstablishedTestContext {
        fn new() -> Self {
            let config = test_config("wss://gateway.example.test/v1/runner/connect");
            let frame_source = deterministic_frame_source();
            let opening = test_opening(&config, frame_source.as_ref());
            let (sleeper, sleep_requests) = controlled_sleeper();
            let (recorder, capture) = test_recorder(BOOT_ID);
            let connection_event = recorder.start("runner.gateway_connection", []);
            let assignment_manager = Mutex::new(AssignmentManager::new(
                &config,
                BOOT_ID.to_owned(),
                healthy_wall_clock(),
            ));
            Self {
                config,
                frame_source,
                sleeper,
                recorder,
                capture,
                connection_event,
                active_effect_event: ActiveEffectEvent::new(),
                assignment_manager,
                _sleep_requests: sleep_requests,
                opening,
            }
        }

        fn dependencies(&self) -> ConnectionDependencies<'_> {
            ConnectionDependencies::new(
                &self.config,
                self.frame_source.as_ref(),
                self.sleeper.as_ref(),
                self.recorder.as_ref(),
                &self.connection_event,
                &self.active_effect_event,
                &self.assignment_manager,
                1,
            )
        }

        fn opening(&self) -> OpeningHello<'_> {
            OpeningHello {
                boot_id: BOOT_ID,
                encoded: &self.opening,
                message_id: OPENING_MESSAGE_ID,
                sequence: 1,
            }
        }
    }

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

    #[tokio::test]
    async fn fills_exact_observation_window_and_refills_one_fifo_slot() {
        let context = EstablishedTestContext::new();
        context
            .assignment_manager
            .lock()
            .unwrap()
            .enqueue_fixture_transitions(40);
        let (inbound, reader, writer, mut outbound) =
            scripted_duplex(DeterminismTranscript::default());
        let mut next_sequence = 2;
        let established = run_established(
            context.dependencies(),
            context.opening(),
            &mut next_sequence,
            reader,
            writer,
        );
        let peer = async {
            let hello = with_watchdog(outbound.recv())
                .await
                .expect("opening hello timed out")
                .expect("opening hello missing");
            assert!(matches!(hello, Message::Text(_)));
            inbound.send(welcome());
            inbound.send(observation_acknowledgement(OPENING_MESSAGE_ID, 1));

            let mut window = Vec::new();
            for _ in 0..OBSERVATION_WINDOW {
                let message = with_watchdog(outbound.recv())
                    .await
                    .expect("observation window fill timed out")
                    .expect("observation window ended early");
                let Message::Text(text) = message else {
                    panic!("observation window contained a control frame");
                };
                window.push(
                    serde_json::from_str::<serde_json::Value>(&text)
                        .expect("decode window observation"),
                );
            }
            assert_eq!(window[0]["sequence"], 2);
            assert_eq!(window[31]["sequence"], 33);
            assert!(outbound.try_recv().is_err(), "window exceeded 32 frames");

            inbound.send(observation_acknowledgement(
                window[0]["messageId"].as_str().expect("window message ID"),
                2,
            ));
            let refill = with_watchdog(outbound.recv())
                .await
                .expect("one-slot refill timed out")
                .expect("one-slot refill missing");
            let Message::Text(refill) = refill else {
                panic!("one-slot refill was not text");
            };
            let refill: serde_json::Value =
                serde_json::from_str(&refill).expect("decode one-slot refill");
            assert_eq!(refill["sequence"], 34);
            assert_eq!(refill["payload"]["executionEventSequence"], 33);
            inbound.send(Message::Close(None));
        };

        let (result, ()) = with_watchdog(async { tokio::join!(established, peer) })
            .await
            .expect("window fixture timed out");
        result.expect("window fixture connection failed");
        assert_eq!(next_sequence, 35);
    }

    #[tokio::test]
    async fn reconnect_replays_only_unacknowledged_semantics_with_fresh_identity() {
        let context = EstablishedTestContext::new();
        context
            .assignment_manager
            .lock()
            .unwrap()
            .enqueue_fixture_transitions(2);
        let mut next_sequence = 2;

        let (first_inbound, first_reader, first_writer, mut first_outbound) =
            scripted_duplex(DeterminismTranscript::default());
        let first_connection = run_established(
            context.dependencies(),
            context.opening(),
            &mut next_sequence,
            first_reader,
            first_writer,
        );
        let first_peer = async {
            first_outbound.recv().await.expect("first opening hello");
            first_inbound.send(welcome());
            first_inbound.send(observation_acknowledgement(OPENING_MESSAGE_ID, 1));
            let first: serde_json::Value = serde_json::from_str(
                first_outbound
                    .recv()
                    .await
                    .expect("first observation")
                    .to_text()
                    .unwrap(),
            )
            .unwrap();
            let second: serde_json::Value = serde_json::from_str(
                first_outbound
                    .recv()
                    .await
                    .expect("second observation")
                    .to_text()
                    .unwrap(),
            )
            .unwrap();
            first_inbound.send(observation_acknowledgement(
                first["messageId"].as_str().unwrap(),
                first["sequence"].as_u64().unwrap(),
            ));
            first_inbound.send(Message::Close(None));
            second
        };
        let (first_result, original) =
            with_watchdog(async { tokio::join!(first_connection, first_peer) })
                .await
                .expect("first connection timed out");
        first_result.expect("first connection failed");
        assert_eq!(original["payload"]["executionEventSequence"], 2);
        assert_eq!(next_sequence, 4);

        let (second_inbound, second_reader, second_writer, mut second_outbound) =
            scripted_duplex(DeterminismTranscript::default());
        let second_connection = run_established(
            context.dependencies(),
            context.opening(),
            &mut next_sequence,
            second_reader,
            second_writer,
        );
        let second_peer = async {
            second_outbound
                .recv()
                .await
                .expect("replacement opening hello");
            second_inbound.send(welcome());
            second_inbound.send(observation_acknowledgement(OPENING_MESSAGE_ID, 1));
            let replay: serde_json::Value = serde_json::from_str(
                second_outbound
                    .recv()
                    .await
                    .expect("replayed observation")
                    .to_text()
                    .unwrap(),
            )
            .unwrap();
            second_inbound.send(Message::Close(None));
            replay
        };
        let (second_result, replay) =
            with_watchdog(async { tokio::join!(second_connection, second_peer) })
                .await
                .expect("replacement connection timed out");
        second_result.expect("replacement connection failed");

        assert_eq!(replay["payload"], original["payload"]);
        assert_ne!(replay["messageId"], original["messageId"]);
        assert_eq!(replay["sequence"], 4);
        assert_eq!(next_sequence, 5);
    }

    #[tokio::test]
    async fn failed_close_send_is_not_logged_as_a_frame() {
        let (sleeper, _sleep_requests) = controlled_sleeper();
        let (recorder, capture) = test_recorder(BOOT_ID);
        let mut protocol =
            ProtocolLog::new(&recorder, "rnr_01k0z6r1w8f4jy2m7q9v3x5abd", BOOT_ID, 1);
        let mut writer = futures_util::sink::unfold((), |(), _: Message| {
            std::future::ready(Err(WebSocketError::ConnectionClosed))
        });

        close_locally(
            &mut writer,
            sleeper.as_ref(),
            &mut protocol,
            CloseCode::Protocol,
            "safe close reason",
        )
        .await;

        let frames: Vec<_> = capture
            .records()
            .into_iter()
            .filter(|record| record["scherzo.protocol.event"] == "frame")
            .collect();
        assert!(
            frames.is_empty(),
            "failed close send was logged as a frame: {frames:?}"
        );
    }

    #[tokio::test]
    async fn transport_read_failure_is_not_logged_as_a_close_frame() {
        let context = EstablishedTestContext::new();
        let reader = futures_util::stream::iter([Err(WebSocketError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "transport reset sentinel",
        )))]);
        let writer = BackpressuredEffectWriter {
            sent: 0,
            blocked: Arc::new(Notify::new()),
        };
        let mut next_sequence = 2;

        run_established(
            context.dependencies(),
            context.opening(),
            &mut next_sequence,
            reader,
            writer,
        )
        .await
        .expect_err("transport read failure unexpectedly succeeded");

        let protocol: Vec<_> = context
            .capture
            .records()
            .into_iter()
            .filter(|record| record["event.name"] == RUNNER_PROTOCOL_EVENT_NAME)
            .collect();
        assert_eq!(protocol.len(), 2);
        assert_eq!(protocol[1]["scherzo.protocol.order"], 2);
        assert_eq!(protocol[1]["scherzo.protocol.event"], "read_failed");
        for frame_field in [
            "scherzo.protocol.direction",
            "scherzo.protocol.frame_kind",
            "scherzo.protocol.close_initiator",
        ] {
            assert!(
                protocol[1].get(frame_field).is_none(),
                "transport read failure was logged with frame metadata: {:?}",
                protocol[1]
            );
        }
        assert!(
            !serde_json::to_string(&protocol)
                .expect("encode protocol records")
                .contains("transport reset sentinel")
        );
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
            (
                ConnectionCause::InvalidExecutionLeasePolicy,
                "invalid execution lease policy",
                "invalid_execution_lease_policy",
            ),
            (
                ConnectionCause::ChangedExecutionLeasePolicy,
                "execution lease policy changed within runner boot",
                "changed_execution_lease_policy",
            ),
            (
                ConnectionCause::ConflictingAssignmentOffer,
                "conflicting assignment offer",
                "conflicting_assignment_offer",
            ),
            (
                ConnectionCause::AssignmentDecisionCapacity,
                "assignment decision capacity exhausted",
                "assignment_decision_capacity",
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
                    Some("Bearer rrc_01k0z6r1w8f4jy2m7q9v3x5abd.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
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
            socket
                .send(Message::Text(
                    json!({
                        "protocolVersion": 1,
                        "direction": "cloud_to_runner",
                        "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abp",
                        "sentAt": "2026-07-23T00:00:04Z",
                        "type": "assignment_release",
                        "payloadVersion": 1,
                        "payload": {
                            "effectId": "eff_01k0z6r1w8f4jy2m7q9v3x5abj",
                            "assignmentId": "asn_01k0z6r1w8f4jy2m7q9v3x5abn",
                            "runId": "run_01k0z6r1w8f4jy2m7q9v3x5abp",
                            "attemptId": "atm_01k0z6r1w8f4jy2m7q9v3x5abc",
                            "reason": "stale_or_invalid_acceptance"
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("send assignment release while semantic response is pending");
            let Some(Ok(Message::Text(response))) = socket.next().await else {
                panic!("fixture did not receive semantic assignment response");
            };
            let response: serde_json::Value =
                serde_json::from_str(&response).expect("decode semantic assignment response");
            assert_eq!(response["type"], "assignment_rejected");
            assert_eq!(
                response["payload"]["decline"]["reason"],
                "workflow_mapping_unavailable"
            );
            socket
                .send(observation_acknowledgement(
                    response["messageId"]
                        .as_str()
                        .expect("semantic response message ID"),
                    3,
                ))
                .await
                .expect("send semantic response acknowledgement");
            let Some(Ok(Message::Text(release_acknowledgement))) = socket.next().await else {
                panic!("fixture did not receive the queued release acknowledgement");
            };
            let release_acknowledgement: serde_json::Value =
                serde_json::from_str(&release_acknowledgement)
                    .expect("decode release acknowledgement");
            assert_eq!(release_acknowledgement["type"], "effect_acknowledged");
            assert_eq!(
                release_acknowledgement["payload"]["effectId"],
                "eff_01k0z6r1w8f4jy2m7q9v3x5abj"
            );
            socket
                .send(observation_acknowledgement(
                    release_acknowledgement["messageId"]
                        .as_str()
                        .expect("release acknowledgement message ID"),
                    4,
                ))
                .await
                .expect("send release acknowledgement response");
            socket.close(None).await.expect("close fixture socket");
        });

        let config = Config::fixture(&endpoint, test_credential(), true)
            .expect("configure loopback gateway");
        let (outcome, capture, next_sequence) =
            run_configured_fixture_connection_with_capture(&config).await;
        let outcome = outcome.expect("run fixture connection");
        assert!(outcome.opening_acknowledged);
        assert!(outcome.handshake_completed);
        assert_eq!(outcome.cloud_text_frames_received, 7);
        assert_eq!(outcome.runner_text_frames_sent, 4);
        assert_eq!(outcome.effects_received, 2);
        assert_eq!(outcome.effect_acknowledgements_confirmed, 2);
        assert_eq!(next_sequence, 5);
        server.await.expect("join fixture server");

        let events = capture.events();
        assert_eq!(events.len(), 2);
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
        assert_eq!(event["scherzo.outcome"], "success");
        let encoded = serde_json::to_string(event).expect("encode effect event");
        assert!(!encoded.contains("runner.run"));
        assert!(!encoded.contains("accepted"));
        assert!(!encoded.contains("executed"));
        assert!(!encoded.contains("abcdefghijklmnopqrstuvwxyzABCDEFG-012345678"));
        assert_eq!(
            events[1]["scherzo.effect.id"],
            "eff_01k0z6r1w8f4jy2m7q9v3x5abj"
        );
        assert_eq!(events[1]["scherzo.runner.sequence"], 4);
        assert_eq!(events[1]["scherzo.outcome"], "success");
        assert_eq!(capture.span_count("runner.effect_acknowledgement"), 2);

        let protocol: Vec<_> = capture
            .records()
            .into_iter()
            .filter(|record| record["event.name"] == RUNNER_PROTOCOL_EVENT_NAME)
            .collect();
        let expected = [
            ("text", Some("hello")),
            ("text", Some("welcome")),
            ("text", Some("observation_ack")),
            ("ping", None),
            ("pong", None),
            ("text", Some("assignment_offer")),
            ("text", Some("effect_acknowledged")),
            ("text", Some("assignment_rejected")),
            ("text", Some("observation_ack")),
            ("text", Some("assignment_release")),
            ("text", Some("effect_acknowledged")),
            ("text", Some("observation_ack")),
            ("text", Some("observation_ack")),
            ("close", None),
        ];
        assert_eq!(protocol.len(), expected.len());
        for (index, (record, (kind, frame_type))) in protocol.iter().zip(expected).enumerate() {
            assert_eq!(record["scherzo.main"], false);
            assert_eq!(record["scherzo.protocol.order"], index + 1);
            assert_eq!(record["scherzo.protocol.frame_kind"], kind);
            assert_eq!(
                record
                    .get("scherzo.protocol.frame_type")
                    .and_then(serde_json::Value::as_str),
                frame_type,
            );
            assert_eq!(record["scherzo.connection.attempt"], 1);
            assert_eq!(record["scherzo.runner.id"], config.credential().runner_id());
            assert_eq!(record["scherzo.runner.boot_id"], BOOT_ID);
        }
        assert_eq!(
            protocol[1]["scherzo.runner.session_id"],
            "rsn_01k0z6r1w8f4jy2m7q9v3x5abc"
        );
        assert_eq!(
            protocol[5]["scherzo.effect.id"],
            "eff_01k0z6r1w8f4jy2m7q9v3x5abg"
        );
        assert_eq!(protocol[6]["scherzo.runner.sequence"], 2);
        assert_eq!(
            protocol[0]["scherzo.runner.version"],
            crate::build_info::VERSION
        );
        let protocol_json = serde_json::to_string(&protocol).expect("encode protocol records");
        for forbidden in [
            "abcdefghijklmnopqrstuvwxyzABCDEFG-012345678",
            "Authorization",
            "PEER-CLOSE-REASON-MUST-NOT-LEAK",
        ] {
            assert!(!protocol_json.contains(forbidden));
        }
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
        let context = EstablishedTestContext::new();
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
            context.dependencies(),
            context.opening(),
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
        context.active_effect_event.finish(Outcome::Cancelled, None);
        context.connection_event.finish(Outcome::Cancelled);

        let events = context.capture.events();
        assert_eq!(events.len(), 2);
        let effect = context.capture.event("runner.effect_acknowledgement");
        assert_eq!(effect["scherzo.outcome"], "cancelled");
        assert!(effect.get("error.type").is_none());
        assert_eq!(
            context.capture.span_count("runner.effect_acknowledgement"),
            1
        );
        assert_eq!(context.capture.span_count("runner.gateway_connection"), 1);
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
        let (error, next_sequence, capture) =
            run_failing_fixture_connection(&endpoint, sleeper.as_ref()).await;
        assert_eq!(error.cause(), "gateway welcome timeout");
        assert!(!error.is_terminal());
        assert!(!error.progress.opening_acknowledged);
        assert!(!error.progress.handshake_completed);
        assert_eq!(next_sequence, 2);
        let protocol: Vec<_> = capture
            .records()
            .into_iter()
            .filter(|record| record["event.name"] == RUNNER_PROTOCOL_EVENT_NAME)
            .collect();
        assert_eq!(protocol.len(), 2);
        assert_eq!(protocol[0]["scherzo.protocol.frame_type"], "hello");
        assert_eq!(protocol[1]["scherzo.protocol.event"], "timer_expired");
        assert_eq!(protocol[1]["scherzo.protocol.timer"], "welcome");

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
        let (error, next_sequence, capture) =
            run_failing_fixture_connection(&endpoint, sleeper.as_ref()).await;
        assert_eq!(error.cause(), "gateway liveness timeout");
        assert!(error.progress.opening_acknowledged);
        assert!(error.progress.handshake_completed);
        assert_eq!(next_sequence, 2);
        let timer = capture
            .records()
            .into_iter()
            .find(|record| record.get("scherzo.protocol.event") == Some(&json!("timer_expired")))
            .expect("runner liveness timer record");
        assert_eq!(timer["scherzo.protocol.timer"], "inbound_silence");

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

        let (error, next_sequence, _capture) =
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
        assert_eq!(error.kind(), FailureKind::TerminalAuthentication);
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
        assert!(error.is_terminal());
        assert_eq!(error.kind(), FailureKind::TerminalProtocol);
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

        let (error, _, _capture) =
            run_failing_fixture_connection(&endpoint, sleeper.as_ref()).await;
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
        assert!(error.is_terminal());
        assert_eq!(error.kind(), FailureKind::TerminalProtocol);
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
    ) -> (ConnectionError, u64, TestCapture) {
        let config = test_config(endpoint);
        let frame_source = deterministic_frame_source();
        let opening = test_opening(&config, frame_source.as_ref());
        let mut next_sequence = 2;
        let (result, capture) = run_test_connection_with_capture(
            &config,
            frame_source.as_ref(),
            sleeper,
            &opening,
            &mut next_sequence,
        )
        .await;
        let error = result.expect_err("fixture connection unexpectedly succeeded");
        (error, next_sequence, capture)
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
        let assignment_manager = Mutex::new(AssignmentManager::new(
            config,
            BOOT_ID.to_owned(),
            healthy_wall_clock(),
        ));
        let sequence = Sequence::new(*next_sequence);
        let result = run(
            ConnectionDependencies::new(
                config,
                frame_source,
                sleeper,
                &recorder,
                &connection_event,
                &active_effect_event,
                &assignment_manager,
                1,
            ),
            OpeningHello {
                boot_id: BOOT_ID,
                encoded: opening,
                message_id: OPENING_MESSAGE_ID,
                sequence: 1,
            },
            &sequence,
        )
        .await;
        *next_sequence = sequence.peek();
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
        Config::fixture(endpoint, test_credential(), true).expect("configure gateway")
    }

    fn test_opening(config: &Config, frame_source: &dyn FrameSource) -> Vec<u8> {
        opening_hello(
            frame_source,
            config.credential().runner_id(),
            BOOT_ID,
            OPENING_MESSAGE_ID.to_owned(),
            1,
            crate::build_info::VERSION,
        )
        .expect("encode opening hello")
    }
}
