use std::fmt;

use serde_json::{Value, json};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};

// cargo-typify emits public declarations; keep its output reproducible and contain
// the binary crate's visibility exception to this generated module.
#[allow(
    dead_code,
    unreachable_pub,
    clippy::unwrap_used,
    reason = "cargo-typify emits reusable schema definitions, public types, and infallible static regex initialization"
)]
pub(crate) mod generated;

const PROTOCOL_VERSION: i64 = 1;
const PAYLOAD_VERSION: i64 = 1;
const RUNNER_TO_CLOUD: &str = "runner_to_cloud";
const CLOUD_TO_RUNNER: &str = "cloud_to_runner";

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
        max_concurrent_runs: u64,
    },
    EffectAcknowledged {
        envelope: RunnerEnvelope,
        effect_id: String,
    },
    AssignmentRejected {
        envelope: RunnerEnvelope,
        effect_id: String,
        assignment_id: String,
        decline_type: String,
        decline_reason: Option<String>,
    },
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
pub(crate) struct CloudEnvelope {
    pub(crate) message_id: String,
    pub(crate) sent_at: String,
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
    AssignmentOffer {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        execution_spec_id: String,
        registered_workflow_id: String,
        offer_expires_at: String,
    },
    AssignmentStart {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        execution_spec_id: String,
        lease_expires_at: String,
    },
    AssignmentRelease {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
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
        ValidatedFrame::Cloud(frame) => Ok(frame),
        ValidatedFrame::Runner => Err(DecodeError::RunnerDirectedFrame),
    }
}

pub(crate) fn encode_runner_frame(frame: &RunnerFrame) -> Result<Vec<u8>, EncodeError> {
    let value = match frame {
        RunnerFrame::Hello {
            envelope,
            runner_version,
            max_concurrent_runs,
        } => runner_frame_value(
            envelope,
            "hello",
            json!({
                "runnerVersion": runner_version,
                "maxConcurrentRuns": max_concurrent_runs,
            }),
        ),
        RunnerFrame::EffectAcknowledged {
            envelope,
            effect_id,
        } => runner_frame_value(
            envelope,
            "effect_acknowledged",
            json!({ "effectId": effect_id }),
        ),
        RunnerFrame::AssignmentRejected {
            envelope,
            effect_id,
            assignment_id,
            decline_type,
            decline_reason,
        } => {
            let mut decline = json!({ "type": decline_type });
            if let Some(reason) = decline_reason {
                decline["reason"] = json!(reason);
            }
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
    Cloud(CloudFrame),
}

// cargo-typify generates distinct cloud structs with the same envelope fields.
// This macro keeps their validation and projection on one shared path without
// introducing wrappers around generated types.
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
            validate_runner_frame(
                &frame.protocol_version,
                &frame.payload_version,
                &frame.direction,
                frame.sent_at,
            )
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
            Ok(ValidatedFrame::Cloud(CloudFrame::Welcome {
                envelope,
                session_id,
                ping_interval_seconds,
                pong_timeout_seconds,
                lease_policy: ExecutionLeasePolicy {
                    schema_version: 1,
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
            Ok(ValidatedFrame::Cloud(CloudFrame::ObservationAck {
                envelope,
                acknowledged_message_id: frame.payload.acknowledged_message_id.to_string(),
                acknowledged_sequence: frame.payload.acknowledged_sequence.0.get(),
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentOffer(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let offer_expires_at = validate_timestamp(&frame.payload.offer_expires_at)?;
            Ok(ValidatedFrame::Cloud(CloudFrame::AssignmentOffer {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                execution_spec_id: frame.payload.execution_spec.execution_spec_id.to_string(),
                registered_workflow_id: frame
                    .payload
                    .execution_spec
                    .registered_workflow_id
                    .to_string(),
                offer_expires_at,
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentStart(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let lease_expires_at = validate_timestamp(&frame.payload.lease.lease_expires_at)?;
            Ok(ValidatedFrame::Cloud(CloudFrame::AssignmentStart {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                execution_spec_id: frame.payload.execution_spec_id.to_string(),
                lease_expires_at,
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentRelease(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            Ok(ValidatedFrame::Cloud(CloudFrame::AssignmentRelease {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                reason: frame.payload.reason.to_string(),
            }))
        }
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
        "hello" => &["runnerVersion", "maxConcurrentRuns"],
        "effect_acknowledged" => &["effectId"],
        "assignment_accepted" => &["effectId", "assignmentId", "offeredExecutionSpecId"],
        "assignment_rejected" => &["effectId", "assignmentId", "decline"],
        "assignment_interrupted" => &["assignmentId", "attemptId", "reason"],
        "welcome" => &[
            "sessionId",
            "pingIntervalSeconds",
            "pongTimeoutSeconds",
            "leasePolicy",
        ],
        "observation_ack" => &["acknowledgedMessageId", "acknowledgedSequence"],
        "assignment_offer" => &[
            "effectId",
            "assignmentId",
            "runId",
            "executionSpec",
            "offerExpiresAt",
        ],
        "assignment_start" => &[
            "effectId",
            "assignmentId",
            "runId",
            "executionSpecId",
            "lease",
        ],
        "assignment_release" => &["effectId", "assignmentId", "runId", "reason"],
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
            "/tests/fixtures/runner-protocol/v1/valid/cloud-assignment-release.json"
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
    ];

    #[test]
    fn generated_types_and_handwritten_validation_accept_every_valid_fixture() {
        for fixture in VALID_FIXTURES {
            let parsed = serde_json::from_slice::<generated::RunnerProtocolVersion1>(fixture);
            assert!(parsed.is_ok(), "generated types rejected valid fixture");
            assert!(
                decode_frame(fixture).is_ok(),
                "validation rejected valid fixture"
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
            max_concurrent_runs: 1,
        };

        let encoded = encode_runner_frame(&frame).unwrap();
        assert!(matches!(decode_frame(&encoded), Ok(ValidatedFrame::Runner)));
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
              "leaseDurationMilliseconds": 30000,
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
