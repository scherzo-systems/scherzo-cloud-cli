use std::{fmt, sync::OnceLock};

use jsonschema::Validator;
use serde_json::{Value, json};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};

// cargo-typify emits public declarations; keep its output reproducible and contain
// the binary crate's visibility exception to this generated module.
#[allow(
    dead_code,
    unreachable_pub,
    clippy::unwrap_used,
    clippy::large_enum_variant,
    clippy::enum_variant_names,
    reason = "cargo-typify emits reusable schema definitions, public types, enum names, variant sizes, and infallible static regex initialization"
)]
pub(crate) mod generated;

const PROTOCOL_SCHEMA: &str = include_str!("schema/runner-protocol-v1.schema.json");
pub(crate) const MAXIMUM_ENCODED_FRAME_BYTES: usize = 65_536;
const PROTOCOL_VERSION: i64 = 1;
const PAYLOAD_VERSION: i64 = 1;
const RUNNER_TO_CLOUD: &str = "runner_to_cloud";
const CLOUD_TO_RUNNER: &str = "cloud_to_runner";

static PROTOCOL_VALIDATOR: OnceLock<Result<Validator, ()>> = OnceLock::new();

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RunnerEnvelope {
    pub(crate) message_id: String,
    pub(crate) runner_id: String,
    pub(crate) boot_id: String,
    pub(crate) sequence: u64,
    pub(crate) sent_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RunnerFrame {
    Hello {
        envelope: RunnerEnvelope,
        runner_version: String,
    },
    EffectAcknowledged {
        envelope: RunnerEnvelope,
        effect_id: String,
    },
    AssignmentAccepted {
        envelope: RunnerEnvelope,
        effect_id: String,
        assignment_id: String,
        offered_execution_spec_id: String,
    },
    AssignmentRejected {
        envelope: RunnerEnvelope,
        effect_id: String,
        assignment_id: String,
        decline: AssignmentDecline,
    },
    AssignmentInterrupted {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        reason: String,
    },
    ExecutionLeaseRenewalRequested {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        current_lease_sequence: u64,
    },
    ExecutionStarted {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
    },
    ExecutionTransition {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        execution_event_sequence: u64,
        workflow_event: Value,
    },
    ExecutionFinished {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        final_execution_event_sequence: u64,
        outcome: Value,
        artifact_delivery: Value,
    },
    ExecutionInterrupted {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        final_execution_event_sequence: u64,
        reason: String,
        terminal_outcome: Value,
        artifact_delivery: Value,
    },
    ExecutionAborted {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        last_execution_event_sequence: u64,
        reason: String,
    },
    ArtifactCarrierRegister {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        portable_owner_path: String,
        media_type: String,
        size_bytes: u64,
        sha256: String,
        idempotency_key: String,
    },
    ArtifactCarrierConfirm {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        artifact_set_id: String,
        carrier_id: String,
    },
    ArtifactResultRegister {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        size_bytes: u64,
        sha256: String,
    },
    ArtifactResultConfirm {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        artifact_set_id: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RunnerUnableReason {
    ExecutionEnvironmentUnavailable,
    SourceServiceUnavailable,
    WorkflowEnvironmentUnsupported,
}

impl RunnerUnableReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionEnvironmentUnavailable => "execution_environment_unavailable",
            Self::SourceServiceUnavailable => "source_service_unavailable",
            Self::WorkflowEnvironmentUnsupported => "workflow_environment_unsupported",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionSpecInvalidReason {
    UnsupportedSchemaVersion,
    InvalidExecutionLimits,
    InvalidSourceProjection,
    UnsupportedSourceObjectFormat,
    SourceCommitMismatch,
    SourceCheckoutDirty,
    WorkflowSourceDigestMismatch,
    WorkflowSourceInvalid,
    WorkflowContractInvalid,
    WorkflowAdmissionInvalid,
}

impl ExecutionSpecInvalidReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedSchemaVersion => "unsupported_schema_version",
            Self::InvalidExecutionLimits => "invalid_execution_limits",
            Self::InvalidSourceProjection => "invalid_source_projection",
            Self::UnsupportedSourceObjectFormat => "unsupported_source_object_format",
            Self::SourceCommitMismatch => "source_commit_mismatch",
            Self::SourceCheckoutDirty => "source_checkout_dirty",
            Self::WorkflowSourceDigestMismatch => "workflow_source_digest_mismatch",
            Self::WorkflowSourceInvalid => "workflow_source_invalid",
            Self::WorkflowContractInvalid => "workflow_contract_invalid",
            Self::WorkflowAdmissionInvalid => "workflow_admission_invalid",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignmentDecline {
    CapacityUnavailable,
    RunnerUnable(RunnerUnableReason),
    ExecutionSpecInvalid(ExecutionSpecInvalidReason),
}

impl AssignmentDecline {
    pub(crate) const fn protocol_type_and_reason(self) -> (&'static str, Option<&'static str>) {
        match self {
            Self::CapacityUnavailable => ("capacity_unavailable", None),
            Self::RunnerUnable(reason) => ("runner_unable", Some(reason.as_str())),
            Self::ExecutionSpecInvalid(reason) => ("execution_spec_invalid", Some(reason.as_str())),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionLimitsV1RunnerProjection {
    pub(crate) maximum_parallel_steps: u64,
    pub(crate) cancellation_grace_seconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowSourceClosureDigestV1RunnerProjection {
    pub(crate) algorithm: String,
    pub(crate) value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionSourceV1RunnerProjection {
    pub(crate) repository_connection_id: String,
    pub(crate) object_format: String,
    pub(crate) commit_oid: String,
    pub(crate) workflow_path: String,
    pub(crate) workflow_source_closure_digest: WorkflowSourceClosureDigestV1RunnerProjection,
    pub(crate) checkout_credential_reference: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionSpecV1RunnerProjection {
    pub(crate) execution_spec_id: String,
    pub(crate) schema_version: u64,
    pub(crate) execution_limits: ExecutionLimitsV1RunnerProjection,
    pub(crate) source: ExecutionSourceV1RunnerProjection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionLeasePolicy {
    pub(crate) schema_version: u64,
    pub(crate) max_clock_uncertainty_milliseconds: i64,
    pub(crate) force_stop_and_reap_budget_milliseconds: i64,
    pub(crate) terminal_report_delivery_budget_milliseconds: i64,
    pub(crate) start_delivery_budget_milliseconds: i64,
    pub(crate) renewal_delivery_budget_milliseconds: i64,
    pub(crate) lease_duration_milliseconds: u64,
    pub(crate) fencing_margin_milliseconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionLeaseGrant {
    pub(crate) sequence: u64,
    pub(crate) expires_at: String,
    pub(crate) runner_stop_before: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CloudEnvelope {
    pub(crate) message_id: String,
    pub(crate) sent_at: String,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ArtifactUploadCapability {
    pub(crate) url: String,
    pub(crate) content_length: String,
    pub(crate) content_type: String,
    pub(crate) if_none_match: String,
    pub(crate) checksum_sha256: String,
    pub(crate) expires_at: String,
}

impl fmt::Debug for ArtifactUploadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ArtifactUploadCapability(<redacted>)")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactRegistrationOutcome {
    Succeeded {
        artifact_set_id: String,
        carrier_id: String,
        upload_capability: ArtifactUploadCapability,
    },
    Retryable,
    Failed {
        code: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactRegistrationResponse {
    pub(crate) request_message_id: String,
    pub(crate) outcome: ArtifactRegistrationOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactConfirmationOutcome {
    Confirmed {
        artifact_set_id: String,
        carrier_id: String,
    },
    Absent {
        artifact_set_id: String,
        carrier_id: String,
        upload_capability: ArtifactUploadCapability,
    },
    Retryable {
        artifact_set_id: String,
        carrier_id: String,
    },
    Failed {
        artifact_set_id: String,
        carrier_id: String,
        code: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactConfirmationResponse {
    pub(crate) request_message_id: String,
    pub(crate) outcome: ArtifactConfirmationOutcome,
}

// Result freeze responses remain distinct from carrier responses because their
// deadline participates in a separate result-finalization state machine.
// jscpd:ignore-start
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactResultRegistrationOutcome {
    Succeeded {
        artifact_set_id: String,
        finalization_deadline: String,
        upload_capability: ArtifactUploadCapability,
    },
    Retryable,
    Failed {
        code: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactResultRegistrationResponse {
    pub(crate) request_message_id: String,
    pub(crate) outcome: ArtifactResultRegistrationOutcome,
}
// jscpd:ignore-end

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactResultConfirmationOutcome {
    Confirmed {
        artifact_set_id: String,
    },
    Absent {
        artifact_set_id: String,
        upload_capability: ArtifactUploadCapability,
    },
    Retryable {
        artifact_set_id: String,
    },
    Failed {
        artifact_set_id: String,
        phase: String,
        code: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactResultConfirmationResponse {
    pub(crate) request_message_id: String,
    pub(crate) outcome: ArtifactResultConfirmationOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CloudFrame {
    Welcome {
        envelope: CloudEnvelope,
        session_id: String,
        ping_interval_seconds: u64,
        pong_timeout_seconds: u64,
        lease_policy: ExecutionLeasePolicy,
    },
    ObservationAck {
        envelope: CloudEnvelope,
        acknowledged_message_id: String,
        acknowledged_sequence: u64,
    },
    ArtifactCarrierRegistration {
        envelope: CloudEnvelope,
        response: ArtifactRegistrationResponse,
    },
    ArtifactCarrierConfirmation {
        envelope: CloudEnvelope,
        response: ArtifactConfirmationResponse,
    },
    ArtifactResultRegistration {
        envelope: CloudEnvelope,
        response: ArtifactResultRegistrationResponse,
    },
    ArtifactResultConfirmation {
        envelope: CloudEnvelope,
        response: ArtifactResultConfirmationResponse,
    },
    AssignmentOffer {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        project_id: String,
        attempt_id: String,
        execution_spec: Box<ExecutionSpecV1RunnerProjection>,
        offer_expires_at: String,
    },
    AssignmentStart {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        attempt_id: String,
        execution_spec_id: String,
        lease: ExecutionLeaseGrant,
    },
    AssignmentLeaseRenewed {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        attempt_id: String,
        lease: ExecutionLeaseGrant,
    },
    AssignmentRelease {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        attempt_id: String,
        reason: String,
    },
}

#[derive(Debug)]
pub(crate) enum DecodeError {
    InvalidJson,
    InvalidFrame(&'static str),
    RunnerDirectedFrame,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson => formatter.write_str("runner protocol frame is not valid JSON"),
            Self::InvalidFrame(field) => {
                write!(formatter, "runner protocol frame has an invalid {field}")
            }
            Self::RunnerDirectedFrame => {
                formatter.write_str("runner protocol frame has runner-to-cloud direction")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

#[derive(Debug)]
pub(crate) enum EncodeError {
    InvalidFrame(&'static str),
    Serialization,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFrame(field) => {
                write!(formatter, "runner protocol frame has an invalid {field}")
            }
            Self::Serialization => formatter.write_str("encode runner protocol frame"),
        }
    }
}

impl std::error::Error for EncodeError {}

pub(crate) fn decode_cloud_frame(bytes: &[u8]) -> Result<CloudFrame, DecodeError> {
    match decode_frame(bytes)? {
        ValidatedFrame::Cloud(frame) => Ok(*frame),
        ValidatedFrame::Runner => Err(DecodeError::RunnerDirectedFrame),
    }
}

pub(crate) fn encode_runner_frame(frame: &RunnerFrame) -> Result<Vec<u8>, EncodeError> {
    let value = match frame {
        RunnerFrame::Hello {
            envelope,
            runner_version,
        } => runner_frame_value(
            envelope,
            "hello",
            json!({ "runnerVersion": runner_version }),
        ),
        RunnerFrame::EffectAcknowledged {
            envelope,
            effect_id,
        } => runner_frame_value(
            envelope,
            "effect_acknowledged",
            json!({ "effectId": effect_id }),
        ),
        RunnerFrame::AssignmentAccepted {
            envelope,
            effect_id,
            assignment_id,
            offered_execution_spec_id,
        } => runner_frame_value(
            envelope,
            "assignment_accepted",
            json!({
                "effectId": effect_id,
                "assignmentId": assignment_id,
                "offeredExecutionSpecId": offered_execution_spec_id,
            }),
        ),
        RunnerFrame::AssignmentRejected {
            envelope,
            effect_id,
            assignment_id,
            decline,
        } => {
            let (decline_type, decline_reason) = decline.protocol_type_and_reason();
            let decline = match decline_reason {
                Some(reason) => json!({
                    "type": decline_type,
                    "reason": reason,
                }),
                None => json!({ "type": decline_type }),
            };
            runner_frame_value(
                envelope,
                "assignment_rejected",
                json!({
                    "effectId": effect_id,
                    "assignmentId": assignment_id,
                    "decline": decline,
                }),
            )
        }
        RunnerFrame::AssignmentInterrupted {
            envelope,
            assignment_id,
            attempt_id,
            reason,
        } => runner_frame_value(
            envelope,
            "assignment_interrupted",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "reason": reason,
            }),
        ),
        RunnerFrame::ExecutionLeaseRenewalRequested {
            envelope,
            assignment_id,
            attempt_id,
            current_lease_sequence,
        } => runner_frame_value(
            envelope,
            "execution_lease_renewal_requested",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "currentLeaseSequence": current_lease_sequence,
            }),
        ),
        RunnerFrame::ExecutionStarted {
            envelope,
            assignment_id,
            attempt_id,
        } => runner_frame_value(
            envelope,
            "execution_started",
            json!({ "assignmentId": assignment_id, "attemptId": attempt_id }),
        ),
        RunnerFrame::ExecutionTransition {
            envelope,
            assignment_id,
            attempt_id,
            execution_event_sequence,
            workflow_event,
        } => runner_frame_value(
            envelope,
            "execution_transition",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "executionEventSequence": execution_event_sequence,
                "workflowEvent": workflow_event,
            }),
        ),
        RunnerFrame::ExecutionFinished {
            envelope,
            assignment_id,
            attempt_id,
            final_execution_event_sequence,
            outcome,
            artifact_delivery,
        } => runner_frame_value(
            envelope,
            "execution_finished",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "finalExecutionEventSequence": final_execution_event_sequence,
                "outcome": outcome,
                "artifactDelivery": artifact_delivery,
            }),
        ),
        RunnerFrame::ExecutionInterrupted {
            envelope,
            assignment_id,
            attempt_id,
            final_execution_event_sequence,
            reason,
            terminal_outcome,
            artifact_delivery,
        } => runner_frame_value(
            envelope,
            "execution_interrupted",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "finalExecutionEventSequence": final_execution_event_sequence,
                "reason": reason,
                "terminalOutcome": terminal_outcome,
                "artifactDelivery": artifact_delivery,
            }),
        ),
        RunnerFrame::ExecutionAborted {
            envelope,
            assignment_id,
            attempt_id,
            last_execution_event_sequence,
            reason,
        } => runner_frame_value(
            envelope,
            "execution_aborted",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "lastExecutionEventSequence": last_execution_event_sequence,
                "reason": reason,
            }),
        ),
        RunnerFrame::ArtifactCarrierRegister {
            envelope,
            assignment_id,
            attempt_id,
            portable_owner_path,
            media_type,
            size_bytes,
            sha256,
            idempotency_key,
        } => runner_frame_value(
            envelope,
            "artifact_carrier_register",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "portableOwnerPath": portable_owner_path,
                "mediaType": media_type,
                "sizeBytes": size_bytes,
                "sha256": sha256,
                "idempotencyKey": idempotency_key,
            }),
        ),
        RunnerFrame::ArtifactCarrierConfirm {
            envelope,
            assignment_id,
            attempt_id,
            artifact_set_id,
            carrier_id,
        } => runner_frame_value(
            envelope,
            "artifact_carrier_confirm",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "artifactSetId": artifact_set_id,
                "carrierId": carrier_id,
            }),
        ),
        RunnerFrame::ArtifactResultRegister {
            envelope,
            assignment_id,
            attempt_id,
            size_bytes,
            sha256,
        } => runner_frame_value(
            envelope,
            "artifact_result_register",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "sizeBytes": size_bytes,
                "sha256": sha256,
            }),
        ),
        RunnerFrame::ArtifactResultConfirm {
            envelope,
            assignment_id,
            attempt_id,
            artifact_set_id,
        } => runner_frame_value(
            envelope,
            "artifact_result_confirm",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "artifactSetId": artifact_set_id,
            }),
        ),
    };
    let encoded = serde_json::to_vec(&value).map_err(|_| EncodeError::Serialization)?;

    match decode_frame(&encoded) {
        Ok(ValidatedFrame::Runner) => Ok(encoded),
        Ok(ValidatedFrame::Cloud(_)) => Err(EncodeError::InvalidFrame("direction")),
        Err(DecodeError::InvalidFrame(field)) => Err(EncodeError::InvalidFrame(field)),
        Err(DecodeError::InvalidJson | DecodeError::RunnerDirectedFrame) => {
            Err(EncodeError::Serialization)
        }
    }
}

fn runner_frame_value(envelope: &RunnerEnvelope, frame_type: &str, payload: Value) -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "direction": RUNNER_TO_CLOUD,
        "messageId": envelope.message_id,
        "runnerId": envelope.runner_id,
        "bootId": envelope.boot_id,
        "sequence": envelope.sequence,
        "sentAt": envelope.sent_at,
        "type": frame_type,
        "payloadVersion": PAYLOAD_VERSION,
        "payload": payload,
    })
}

enum ValidatedFrame {
    Runner,
    Cloud(Box<CloudFrame>),
}

fn cloud(frame: CloudFrame) -> ValidatedFrame {
    ValidatedFrame::Cloud(Box::new(frame))
}

// cargo-typify generates distinct cloud structs with the same envelope fields.
// This macro keeps their validation and projection on one shared path without
// introducing wrappers around generated types.
macro_rules! validated_runner_frame {
    ($frame:expr) => {
        validate_runner_frame(
            &$frame.protocol_version,
            &$frame.payload_version,
            &$frame.direction,
            $frame.sent_at,
        )
    };
}

macro_rules! validated_cloud_envelope {
    ($frame:expr) => {
        cloud_envelope(
            &$frame.protocol_version,
            &$frame.payload_version,
            &$frame.direction,
            $frame.message_id,
            $frame.sent_at,
        )
    };
}

fn decode_frame(bytes: &[u8]) -> Result<ValidatedFrame, DecodeError> {
    let value: Value = serde_json::from_slice(bytes).map_err(|_| DecodeError::InvalidJson)?;
    validate_protocol_schema(&value)?;
    validate_closed_shape(&value)?;
    let generated = serde_json::from_value(value).map_err(|_| DecodeError::InvalidJson)?;

    match generated {
        generated::RunnerProtocolVersion1::RunnerHello(frame) => validate_runner_frame(
            &frame.protocol_version,
            &frame.payload_version,
            &frame.direction,
            frame.sent_at,
        ),
        generated::RunnerProtocolVersion1::RunnerEffectAcknowledged(frame) => {
            validate_runner_frame(
                &frame.protocol_version,
                &frame.payload_version,
                &frame.direction,
                frame.sent_at,
            )
        }
        generated::RunnerProtocolVersion1::RunnerAssignmentAccepted(frame) => {
            validate_runner_frame(
                &frame.protocol_version,
                &frame.payload_version,
                &frame.direction,
                frame.sent_at,
            )
        }
        generated::RunnerProtocolVersion1::RunnerAssignmentRejected(frame) => {
            validate_runner_frame(
                &frame.protocol_version,
                &frame.payload_version,
                &frame.direction,
                frame.sent_at,
            )
        }
        generated::RunnerProtocolVersion1::RunnerAssignmentInterrupted(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerExecutionLeaseRenewalRequested(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerExecutionStarted(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerExecutionTransition(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerExecutionFinished(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerExecutionInterrupted(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerExecutionAborted(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerArtifactCarrierRegister(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerArtifactCarrierConfirm(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerArtifactResultRegister(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerArtifactResultConfirm(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::CloudWelcome(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let ping_interval_seconds = frame.payload.ping_interval_seconds.get();
            let pong_timeout_seconds = u64::try_from(frame.payload.pong_timeout_seconds)
                .map_err(|_| DecodeError::InvalidFrame("pongTimeoutSeconds"))?;
            if pong_timeout_seconds < ping_interval_seconds.saturating_mul(2) {
                return Err(DecodeError::InvalidFrame("pongTimeoutSeconds"));
            }
            let session_id = frame.payload.session_id.to_string();
            let policy = frame.payload.lease_policy;
            let schema_version = policy
                .schema_version
                .as_u64()
                .ok_or(DecodeError::InvalidFrame("schemaVersion"))?;
            Ok(cloud(CloudFrame::Welcome {
                envelope,
                session_id,
                ping_interval_seconds,
                pong_timeout_seconds,
                lease_policy: ExecutionLeasePolicy {
                    schema_version,
                    max_clock_uncertainty_milliseconds: policy.max_clock_uncertainty_milliseconds.0,
                    force_stop_and_reap_budget_milliseconds: policy
                        .force_stop_and_reap_budget_milliseconds
                        .0,
                    terminal_report_delivery_budget_milliseconds: policy
                        .terminal_report_delivery_budget_milliseconds
                        .0,
                    start_delivery_budget_milliseconds: policy.start_delivery_budget_milliseconds.0,
                    renewal_delivery_budget_milliseconds: policy
                        .renewal_delivery_budget_milliseconds
                        .0,
                    lease_duration_milliseconds: policy.lease_duration_milliseconds.0.get(),
                    fencing_margin_milliseconds: policy.fencing_margin_milliseconds.0.get(),
                },
            }))
        }
        generated::RunnerProtocolVersion1::CloudObservationAck(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            Ok(cloud(CloudFrame::ObservationAck {
                envelope,
                acknowledged_message_id: frame.payload.acknowledged_message_id.to_string(),
                acknowledged_sequence: frame.payload.acknowledged_sequence.0.get(),
            }))
        }
        generated::RunnerProtocolVersion1::CloudArtifactCarrierRegistration(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let response = artifact_registration_response(frame.payload)?;
            Ok(cloud(CloudFrame::ArtifactCarrierRegistration {
                envelope,
                response,
            }))
        }
        generated::RunnerProtocolVersion1::CloudArtifactCarrierConfirmation(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let response = artifact_confirmation_response(frame.payload)?;
            Ok(cloud(CloudFrame::ArtifactCarrierConfirmation {
                envelope,
                response,
            }))
        }
        generated::RunnerProtocolVersion1::CloudArtifactResultRegistration(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let response = artifact_result_registration_response(frame.payload)?;
            Ok(cloud(CloudFrame::ArtifactResultRegistration {
                envelope,
                response,
            }))
        }
        generated::RunnerProtocolVersion1::CloudArtifactResultConfirmation(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let response = artifact_result_confirmation_response(frame.payload)?;
            Ok(cloud(CloudFrame::ArtifactResultConfirmation {
                envelope,
                response,
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentOffer(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let offer_expires_at = validate_timestamp(&frame.payload.offer_expires_at)?;
            let execution_spec = frame.payload.execution_spec;
            let schema_version = execution_spec
                .schema_version
                .as_u64()
                .ok_or(DecodeError::InvalidFrame("schemaVersion"))?;
            let maximum_parallel_steps =
                u64::try_from(execution_spec.execution_limits.maximum_parallel_steps.0)
                    .map_err(|_| DecodeError::InvalidFrame("maximumParallelSteps"))?;
            let cancellation_grace_seconds =
                u64::try_from(execution_spec.execution_limits.cancellation_grace_seconds.0)
                    .map_err(|_| DecodeError::InvalidFrame("cancellationGraceSeconds"))?;
            let source = ExecutionSourceV1RunnerProjection {
                repository_connection_id: execution_spec
                    .source
                    .repository_connection_id
                    .to_string(),
                object_format: execution_spec.source.object_format.to_string(),
                commit_oid: execution_spec.source.commit_oid.to_string(),
                workflow_path: execution_spec.source.workflow_path.to_string(),
                workflow_source_closure_digest: WorkflowSourceClosureDigestV1RunnerProjection {
                    algorithm: execution_spec
                        .source
                        .workflow_source_closure_digest
                        .algorithm
                        .to_string(),
                    value: execution_spec
                        .source
                        .workflow_source_closure_digest
                        .value
                        .to_string(),
                },
                checkout_credential_reference: execution_spec
                    .source
                    .checkout_credential_reference
                    .to_string(),
            };
            Ok(cloud(CloudFrame::AssignmentOffer {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                project_id: frame.payload.project_id.to_string(),
                attempt_id: frame.payload.attempt_id.to_string(),
                execution_spec: Box::new(ExecutionSpecV1RunnerProjection {
                    execution_spec_id: execution_spec.execution_spec_id.to_string(),
                    schema_version,
                    execution_limits: ExecutionLimitsV1RunnerProjection {
                        maximum_parallel_steps,
                        cancellation_grace_seconds,
                    },
                    source,
                }),
                offer_expires_at,
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentStart(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let lease = frame.payload.lease;
            Ok(cloud(CloudFrame::AssignmentStart {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                attempt_id: frame.payload.attempt_id.to_string(),
                execution_spec_id: frame.payload.execution_spec_id.to_string(),
                lease: ExecutionLeaseGrant {
                    sequence: lease.lease_sequence.get(),
                    expires_at: validate_timestamp(&lease.lease_expires_at)?,
                    runner_stop_before: validate_timestamp(&lease.runner_stop_before)?,
                },
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentLeaseRenewed(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let lease = frame.payload.lease;
            let sequence = u64::try_from(lease.lease_sequence)
                .map_err(|_| DecodeError::InvalidFrame("leaseSequence"))?;
            Ok(cloud(CloudFrame::AssignmentLeaseRenewed {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                attempt_id: frame.payload.attempt_id.to_string(),
                lease: ExecutionLeaseGrant {
                    sequence,
                    expires_at: validate_timestamp(&lease.lease_expires_at)?,
                    runner_stop_before: validate_timestamp(&lease.runner_stop_before)?,
                },
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentRelease(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            Ok(cloud(CloudFrame::AssignmentRelease {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                attempt_id: frame.payload.attempt_id.to_string(),
                reason: frame.payload.reason.to_string(),
            }))
        }
    }
}

fn artifact_registration_response(
    payload: generated::CloudArtifactCarrierRegistrationPayload,
) -> Result<ArtifactRegistrationResponse, DecodeError> {
    use generated::CloudArtifactCarrierRegistrationPayload as Payload;

    let (request_message_id, outcome) = match payload {
        Payload::Succeeded {
            artifact_set_id,
            carrier_id,
            request_message_id,
            upload_capability,
        } => (
            request_message_id.to_string(),
            ArtifactRegistrationOutcome::Succeeded {
                artifact_set_id: artifact_set_id.to_string(),
                carrier_id: carrier_id.to_string(),
                upload_capability: artifact_upload_capability(upload_capability)?,
            },
        ),
        Payload::Retryable { request_message_id } => (
            request_message_id.to_string(),
            ArtifactRegistrationOutcome::Retryable,
        ),
        Payload::Failed {
            code,
            request_message_id,
        } => (
            request_message_id.to_string(),
            ArtifactRegistrationOutcome::Failed {
                code: code.to_string(),
            },
        ),
    };
    Ok(ArtifactRegistrationResponse {
        request_message_id,
        outcome,
    })
}

fn artifact_confirmation_response(
    payload: generated::CloudArtifactCarrierConfirmationPayload,
) -> Result<ArtifactConfirmationResponse, DecodeError> {
    use generated::CloudArtifactCarrierConfirmationPayload as Payload;

    let (request_message_id, outcome) = match payload {
        Payload::Confirmed {
            artifact_set_id,
            carrier_id,
            request_message_id,
        } => (
            request_message_id.to_string(),
            ArtifactConfirmationOutcome::Confirmed {
                artifact_set_id: artifact_set_id.to_string(),
                carrier_id: carrier_id.to_string(),
            },
        ),
        Payload::Absent {
            artifact_set_id,
            carrier_id,
            request_message_id,
            upload_capability,
        } => (
            request_message_id.to_string(),
            ArtifactConfirmationOutcome::Absent {
                artifact_set_id: artifact_set_id.to_string(),
                carrier_id: carrier_id.to_string(),
                upload_capability: artifact_upload_capability(upload_capability)?,
            },
        ),
        Payload::Retryable {
            artifact_set_id,
            carrier_id,
            request_message_id,
        } => (
            request_message_id.to_string(),
            ArtifactConfirmationOutcome::Retryable {
                artifact_set_id: artifact_set_id.to_string(),
                carrier_id: carrier_id.to_string(),
            },
        ),
        Payload::Failed {
            artifact_set_id,
            carrier_id,
            code,
            request_message_id,
        } => (
            request_message_id.to_string(),
            ArtifactConfirmationOutcome::Failed {
                artifact_set_id: artifact_set_id.to_string(),
                carrier_id: carrier_id.to_string(),
                code: code.to_string(),
            },
        ),
    };
    Ok(ArtifactConfirmationResponse {
        request_message_id,
        outcome,
    })
}

fn artifact_result_registration_response(
    payload: generated::CloudArtifactResultRegistrationPayload,
) -> Result<ArtifactResultRegistrationResponse, DecodeError> {
    use generated::CloudArtifactResultRegistrationPayload as Payload;

    let (request_message_id, outcome) = match payload {
        Payload::Succeeded {
            artifact_set_id,
            finalization_deadline,
            request_message_id,
            upload_capability,
        } => (
            request_message_id.to_string(),
            ArtifactResultRegistrationOutcome::Succeeded {
                artifact_set_id: artifact_set_id.to_string(),
                finalization_deadline: validate_timestamp(&finalization_deadline)?,
                upload_capability: artifact_upload_capability(upload_capability)?,
            },
        ),
        Payload::Retryable { request_message_id } => (
            request_message_id.to_string(),
            ArtifactResultRegistrationOutcome::Retryable,
        ),
        Payload::Failed {
            code,
            request_message_id,
        } => (
            request_message_id.to_string(),
            ArtifactResultRegistrationOutcome::Failed {
                code: code.to_string(),
            },
        ),
    };
    Ok(ArtifactResultRegistrationResponse {
        request_message_id,
        outcome,
    })
}

fn artifact_result_confirmation_response(
    payload: generated::CloudArtifactResultConfirmationPayload,
) -> Result<ArtifactResultConfirmationResponse, DecodeError> {
    let value = serde_json::to_value(payload)
        .map_err(|_| DecodeError::InvalidFrame("artifact result confirmation"))?;
    let field = |name| {
        value
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or(DecodeError::InvalidFrame("artifact result confirmation"))
    };
    let request_message_id = field("requestMessageId")?;
    let artifact_set_id = field("artifactSetId")?;
    let outcome = match field("outcome")?.as_str() {
        "confirmed" => ArtifactResultConfirmationOutcome::Confirmed { artifact_set_id },
        "absent" => {
            let capability = value
                .get("uploadCapability")
                .cloned()
                .ok_or(DecodeError::InvalidFrame("artifact upload capability"))?;
            let capability = serde_json::from_value::<generated::UploadCapability>(capability)
                .map_err(|_| DecodeError::InvalidFrame("artifact upload capability"))?;
            ArtifactResultConfirmationOutcome::Absent {
                artifact_set_id,
                upload_capability: artifact_upload_capability(capability)?,
            }
        }
        "retryable" => ArtifactResultConfirmationOutcome::Retryable { artifact_set_id },
        "failed" => ArtifactResultConfirmationOutcome::Failed {
            artifact_set_id,
            phase: field("phase")?,
            code: field("code")?,
        },
        _ => return Err(DecodeError::InvalidFrame("artifact result confirmation")),
    };
    Ok(ArtifactResultConfirmationResponse {
        request_message_id,
        outcome,
    })
}

fn artifact_upload_capability(
    capability: generated::UploadCapability,
) -> Result<ArtifactUploadCapability, DecodeError> {
    let expires_at = validate_timestamp(&capability.expires_at)?;
    let if_none_match = capability
        .headers
        .if_none_match
        .as_str()
        .ok_or(DecodeError::InvalidFrame("If-None-Match"))?
        .to_owned();
    Ok(ArtifactUploadCapability {
        url: capability.url,
        content_length: capability.headers.content_length.to_string(),
        content_type: capability.headers.content_type.to_string(),
        if_none_match,
        checksum_sha256: capability.headers.x_amz_checksum_sha256.to_string(),
        expires_at,
    })
}

fn validate_protocol_schema(value: &Value) -> Result<(), DecodeError> {
    let validator = PROTOCOL_VALIDATOR
        .get_or_init(|| {
            let schema = serde_json::from_str::<Value>(PROTOCOL_SCHEMA).map_err(|_| ())?;
            jsonschema::draft202012::new(&schema).map_err(|_| ())
        })
        .as_ref()
        .map_err(|_| DecodeError::InvalidFrame("schema"))?;
    if validator.is_valid(value) {
        Ok(())
    } else {
        Err(DecodeError::InvalidFrame("schema"))
    }
}

fn validate_closed_shape(value: &Value) -> Result<(), DecodeError> {
    let object = value
        .as_object()
        .ok_or(DecodeError::InvalidFrame("envelope"))?;
    let direction = object
        .get("direction")
        .and_then(Value::as_str)
        .ok_or(DecodeError::InvalidFrame("direction"))?;
    let frame_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or(DecodeError::InvalidFrame("type"))?;
    let envelope_keys: &[&str] = if direction == RUNNER_TO_CLOUD {
        &[
            "protocolVersion",
            "direction",
            "messageId",
            "runnerId",
            "bootId",
            "sequence",
            "sentAt",
            "type",
            "payloadVersion",
            "payload",
        ]
    } else {
        &[
            "protocolVersion",
            "direction",
            "messageId",
            "sentAt",
            "type",
            "payloadVersion",
            "payload",
        ]
    };
    if object.len() != envelope_keys.len()
        || !envelope_keys.iter().all(|key| object.contains_key(*key))
    {
        return Err(DecodeError::InvalidFrame("envelope"));
    }
    let payload = object
        .get("payload")
        .and_then(Value::as_object)
        .ok_or(DecodeError::InvalidFrame("payload"))?;
    let payload_keys: &[&str] = match frame_type {
        "hello" => &["runnerVersion"],
        "effect_acknowledged" => &["effectId"],
        "assignment_accepted" => &["effectId", "assignmentId", "offeredExecutionSpecId"],
        "assignment_rejected" => &["effectId", "assignmentId", "decline"],
        "assignment_interrupted" => &["assignmentId", "attemptId", "reason"],
        "execution_lease_renewal_requested" => {
            &["assignmentId", "attemptId", "currentLeaseSequence"]
        }
        "execution_started" => &["assignmentId", "attemptId"],
        "execution_transition" => &[
            "assignmentId",
            "attemptId",
            "executionEventSequence",
            "workflowEvent",
        ],
        "execution_finished" => &[
            "assignmentId",
            "attemptId",
            "finalExecutionEventSequence",
            "outcome",
            "artifactDelivery",
        ],
        "execution_interrupted" => &[
            "assignmentId",
            "attemptId",
            "finalExecutionEventSequence",
            "reason",
            "terminalOutcome",
            "artifactDelivery",
        ],
        "execution_aborted" => &[
            "assignmentId",
            "attemptId",
            "lastExecutionEventSequence",
            "reason",
        ],
        "welcome" => &[
            "sessionId",
            "pingIntervalSeconds",
            "pongTimeoutSeconds",
            "leasePolicy",
        ],
        "observation_ack" => &["acknowledgedMessageId", "acknowledgedSequence"],
        "artifact_carrier_register" => &[
            "assignmentId",
            "attemptId",
            "portableOwnerPath",
            "mediaType",
            "sizeBytes",
            "sha256",
            "idempotencyKey",
        ],
        "artifact_carrier_confirm" => &["assignmentId", "attemptId", "artifactSetId", "carrierId"],
        "artifact_result_register" => &["assignmentId", "attemptId", "sizeBytes", "sha256"],
        "artifact_result_confirm" => &["assignmentId", "attemptId", "artifactSetId"],
        "artifact_carrier_registration"
        | "artifact_carrier_confirmation"
        | "artifact_result_registration"
        | "artifact_result_confirmation" => return Ok(()),
        "assignment_offer" => &[
            "effectId",
            "assignmentId",
            "runId",
            "projectId",
            "attemptId",
            "executionSpec",
            "offerExpiresAt",
        ],
        "assignment_start" => &[
            "effectId",
            "assignmentId",
            "runId",
            "attemptId",
            "executionSpecId",
            "lease",
        ],
        "assignment_lease_renewed" => &["effectId", "assignmentId", "runId", "attemptId", "lease"],
        "assignment_release" => &["effectId", "assignmentId", "runId", "attemptId", "reason"],
        _ => return Err(DecodeError::InvalidFrame("type")),
    };
    if payload.len() != payload_keys.len()
        || !payload_keys.iter().all(|key| payload.contains_key(*key))
    {
        return Err(DecodeError::InvalidFrame("payload"));
    }
    Ok(())
}

fn validate_runner_frame(
    protocol_version: &Value,
    payload_version: &Value,
    direction: &Value,
    sent_at: generated::UtcTimestamp,
) -> Result<ValidatedFrame, DecodeError> {
    validate_constants(
        protocol_version,
        payload_version,
        direction,
        RUNNER_TO_CLOUD,
    )?;
    validate_timestamp(&sent_at)?;
    Ok(ValidatedFrame::Runner)
}

fn cloud_envelope(
    protocol_version: &Value,
    payload_version: &Value,
    direction: &Value,
    message_id: generated::CloudMessageId,
    sent_at: generated::UtcTimestamp,
) -> Result<CloudEnvelope, DecodeError> {
    validate_constants(
        protocol_version,
        payload_version,
        direction,
        CLOUD_TO_RUNNER,
    )?;
    Ok(CloudEnvelope {
        message_id: message_id.to_string(),
        sent_at: validate_timestamp(&sent_at)?,
    })
}

fn validate_constants(
    protocol_version: &Value,
    payload_version: &Value,
    direction: &Value,
    expected_direction: &str,
) -> Result<(), DecodeError> {
    if protocol_version.as_i64() != Some(PROTOCOL_VERSION) {
        return Err(DecodeError::InvalidFrame("protocolVersion"));
    }
    if payload_version.as_i64() != Some(PAYLOAD_VERSION) {
        return Err(DecodeError::InvalidFrame("payloadVersion"));
    }
    if direction.as_str() != Some(expected_direction) {
        return Err(DecodeError::InvalidFrame("direction"));
    }
    Ok(())
}

fn validate_timestamp(timestamp: &generated::UtcTimestamp) -> Result<String, DecodeError> {
    let value = timestamp.to_string();
    let parsed =
        OffsetDateTime::parse(&value, &Rfc3339).map_err(|_| DecodeError::InvalidFrame("sentAt"))?;
    if parsed.offset() != UtcOffset::UTC || !value.ends_with('Z') {
        return Err(DecodeError::InvalidFrame("sentAt"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_FIXTURES: &[&[u8]] = &[
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-hello.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-effect-acknowledged.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-welcome.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-observation-ack.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-fresh-hello.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-assignment-accepted.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-assignment-rejected.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-assignment-interrupted.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-assignment-start.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-execution-lease-renewal-requested.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-execution-started.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-execution-transition.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-execution-transition-agent-failure.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-execution-transition-output-failure.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-execution-finished.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-execution-finished-delivery-failure.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-execution-finished-delivery-internal-failure.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-execution-interrupted.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-execution-aborted.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-assignment-lease-renewed.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-assignment-release.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-artifact-carrier-register.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-artifact-carrier-confirm.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-artifact-carrier-registration.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-artifact-carrier-confirmation.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-artifact-result-register.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/runner-artifact-result-confirm.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-artifact-result-registration.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-artifact-result-confirmation.json"
        )),
    ];

    const INVALID_FIXTURES: &[&[u8]] = &[
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/unknown-type.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/wrong-direction.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/unsupported-protocol-version.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/extra-envelope-field.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/extra-payload-field.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/invalid-runner-id.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/sequence-zero.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/non-utc-timestamp.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/execution-finished-zero-sequence.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/execution-interrupted-reason-mismatch.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/execution-aborted-inconsistent-zero.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/delivery-code-phase-mismatch.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/invalid/delivery-open-diagnostic.json"
        )),
    ];

    #[test]
    fn generated_types_and_handwritten_validation_accept_every_valid_fixture() {
        for (index, fixture) in VALID_FIXTURES.iter().enumerate() {
            let parsed = serde_json::from_slice::<generated::RunnerProtocolVersion1>(fixture);
            assert!(
                parsed.is_ok(),
                "generated types rejected valid fixture {index}: {}",
                parsed.unwrap_err()
            );
            assert!(
                decode_frame(fixture).is_ok(),
                "validation rejected valid fixture {index}"
            );
        }
    }

    #[test]
    fn generated_types_and_handwritten_validation_reject_every_invalid_fixture() {
        for (index, fixture) in INVALID_FIXTURES.iter().enumerate() {
            let result = serde_json::from_slice::<generated::RunnerProtocolVersion1>(fixture)
                .ok()
                .and_then(|frame| decode_frame(fixture).ok().map(|_| frame));
            assert!(result.is_none(), "invalid fixture {index} was accepted");
        }
    }

    #[test]
    fn decode_cloud_frame_rejects_runner_directed_frames() {
        assert!(matches!(
            decode_cloud_frame(VALID_FIXTURES[0]),
            Err(DecodeError::RunnerDirectedFrame)
        ));
    }

    #[test]
    fn encode_runner_frame_round_trips_through_generated_validation() {
        let frame = RunnerFrame::Hello {
            envelope: RunnerEnvelope {
                message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
                boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
                sequence: 1,
                sent_at: "2026-07-23T00:00:00Z".to_owned(),
            },
            runner_version: "0.2.0".to_owned(),
        };

        let encoded = encode_runner_frame(&frame).unwrap();
        assert!(matches!(decode_frame(&encoded), Ok(ValidatedFrame::Runner)));
    }

    #[test]
    fn assignment_decisions_encode_only_the_closed_wire_vocabulary() {
        const EFFECT_ID: &str = "eff_01k0z6r1w8f4jy2m7q9v3x5abg";
        const ASSIGNMENT_ID: &str = "asn_01k0z6r1w8f4jy2m7q9v3x5abh";
        const EXECUTION_SPEC_ID: &str = "xsp_01k0z6r1w8f4jy2m7q9v3x5abj";
        let envelope = || RunnerEnvelope {
            message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
            sequence: 1,
            sent_at: "2026-07-23T00:00:00Z".to_owned(),
        };
        let rejected = |decline| RunnerFrame::AssignmentRejected {
            envelope: envelope(),
            effect_id: EFFECT_ID.to_owned(),
            assignment_id: ASSIGNMENT_ID.to_owned(),
            decline,
        };
        let rejected_payload = |decline| {
            json!({
                "effectId": EFFECT_ID,
                "assignmentId": ASSIGNMENT_ID,
                "decline": decline,
            })
        };
        let cases = [
            (
                RunnerFrame::AssignmentAccepted {
                    envelope: envelope(),
                    effect_id: EFFECT_ID.to_owned(),
                    assignment_id: ASSIGNMENT_ID.to_owned(),
                    offered_execution_spec_id: EXECUTION_SPEC_ID.to_owned(),
                },
                json!({
                    "effectId": EFFECT_ID,
                    "assignmentId": ASSIGNMENT_ID,
                    "offeredExecutionSpecId": EXECUTION_SPEC_ID,
                }),
            ),
            (
                rejected(AssignmentDecline::CapacityUnavailable),
                rejected_payload(json!({ "type": "capacity_unavailable" })),
            ),
            (
                rejected(AssignmentDecline::RunnerUnable(
                    RunnerUnableReason::SourceServiceUnavailable,
                )),
                rejected_payload(json!({
                    "type": "runner_unable",
                    "reason": "source_service_unavailable",
                })),
            ),
            (
                rejected(AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::InvalidExecutionLimits,
                )),
                rejected_payload(json!({
                    "type": "execution_spec_invalid",
                    "reason": "invalid_execution_limits",
                })),
            ),
        ];

        for (index, (frame, expected_payload)) in cases.into_iter().enumerate() {
            let encoded = encode_runner_frame(&frame)
                .unwrap_or_else(|error| panic!("assignment decision {index}: {error}"));
            let value: Value = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(value["payload"], expected_payload);
        }
    }

    #[test]
    fn unsupported_lease_policy_version_is_not_normalized_to_v1() {
        let mut welcome: Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-welcome.json"
        )))
        .unwrap();
        welcome["payload"]["leasePolicy"]["schemaVersion"] = json!(2);
        let encoded = serde_json::to_vec(&welcome).unwrap();

        match decode_cloud_frame(&encoded) {
            Err(_) => {}
            Ok(CloudFrame::Welcome { lease_policy, .. }) => {
                assert_eq!(lease_policy.schema_version, 2);
            }
            Ok(_) => panic!("welcome decoded as another frame type"),
        }
    }

    #[test]
    fn zero_execution_limit_reaches_semantic_admission() {
        let mut offer: Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer.json"
        )))
        .unwrap();
        offer["payload"]["executionSpec"]["executionLimits"]["maximumParallelSteps"] = json!(0);
        let encoded = serde_json::to_vec(&offer).unwrap();

        let decoded = decode_cloud_frame(&encoded)
            .expect("invalid execution limits must receive a semantic rejection");

        assert!(matches!(
            decoded,
            CloudFrame::AssignmentOffer { execution_spec, .. }
                if execution_spec.execution_limits.maximum_parallel_steps == 0
        ));
    }

    #[test]
    fn malformed_source_values_reach_closed_semantic_admission() {
        let mut offer: Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer.json"
        )))
        .unwrap();
        offer["payload"]["executionSpec"]["source"]["commitOid"] = json!("not-an-oid");
        offer["payload"]["executionSpec"]["source"]["objectFormat"] = json!("sha256");
        let encoded = serde_json::to_vec(&offer).unwrap();

        let decoded = decode_cloud_frame(&encoded)
            .expect("malformed immutable source must receive a semantic rejection");
        assert!(matches!(
            decoded,
            CloudFrame::AssignmentOffer { execution_spec, .. }
                if execution_spec.source.commit_oid == "not-an-oid"
                    && execution_spec.source.object_format == "sha256"
        ));
    }

    #[test]
    fn decode_cloud_frame_rejects_invalid_welcome_timing_pair() {
        let bytes = br#"{
          "protocolVersion": 1,
          "direction": "cloud_to_runner",
          "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abh",
          "sentAt": "2026-07-23T00:00:00Z",
          "type": "welcome",
          "payloadVersion": 1,
          "payload": {
            "sessionId": "rsn_01k0z6r1w8f4jy2m7q9v3x5abj",
            "pingIntervalSeconds": 10,
            "pongTimeoutSeconds": 19,
            "leasePolicy": {
              "schemaVersion": 1,
              "maxClockUncertaintyMilliseconds": 1000,
              "forceStopAndReapBudgetMilliseconds": 5000,
              "terminalReportDeliveryBudgetMilliseconds": 5000,
              "startDeliveryBudgetMilliseconds": 5000,
              "renewalDeliveryBudgetMilliseconds": 5000,
              "leaseDurationMilliseconds": 320000,
              "fencingMarginMilliseconds": 11000
            }
          }
        }"#;

        assert!(matches!(
            decode_cloud_frame(bytes),
            Err(DecodeError::InvalidFrame("pongTimeoutSeconds"))
        ));
    }
}
