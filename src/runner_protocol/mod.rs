use std::fmt;

use serde_json::{Value, json};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};

// Generated protocol data transfer objects stay private to this boundary.
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
        lease_expires_at: String,
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

fn decode_frame(bytes: &[u8]) -> Result<ValidatedFrame, DecodeError> {
    let generated = serde_json::from_slice(bytes).map_err(|_| DecodeError::InvalidJson)?;

    match generated {
        generated::RunnerProtocolVersion1::RunnerHello(frame) => {
            validate_runner_frame(
                RunnerFrameMetadata {
                    protocol_version: &frame.protocol_version,
                    payload_version: &frame.payload_version,
                    direction: &frame.direction,
                    frame_type: &frame.type_,
                    expected_type: "hello",
                },
                frame.sent_at,
            )?;
            Ok(ValidatedFrame::Runner)
        }
        generated::RunnerProtocolVersion1::RunnerEffectAcknowledged(frame) => {
            validate_runner_frame(
                RunnerFrameMetadata {
                    protocol_version: &frame.protocol_version,
                    payload_version: &frame.payload_version,
                    direction: &frame.direction,
                    frame_type: &frame.type_,
                    expected_type: "effect_acknowledged",
                },
                frame.sent_at,
            )?;
            Ok(ValidatedFrame::Runner)
        }
        generated::RunnerProtocolVersion1::CloudWelcome(frame) => {
            validate_constants(
                &frame.protocol_version,
                &frame.payload_version,
                &frame.direction,
                &frame.type_,
                CLOUD_TO_RUNNER,
                "welcome",
            )?;
            let envelope = cloud_envelope(frame.message_id, frame.sent_at)?;
            let ping_interval_seconds = frame.payload.ping_interval_seconds.get();
            let pong_timeout_seconds = u64::try_from(frame.payload.pong_timeout_seconds)
                .map_err(|_| DecodeError::InvalidFrame("pongTimeoutSeconds"))?;
            if pong_timeout_seconds < ping_interval_seconds.saturating_mul(2) {
                return Err(DecodeError::InvalidFrame("pongTimeoutSeconds"));
            }
            Ok(ValidatedFrame::Cloud(CloudFrame::Welcome {
                envelope,
                session_id: frame.payload.session_id.to_string(),
                ping_interval_seconds,
                pong_timeout_seconds,
            }))
        }
        generated::RunnerProtocolVersion1::CloudObservationAck(frame) => {
            validate_constants(
                &frame.protocol_version,
                &frame.payload_version,
                &frame.direction,
                &frame.type_,
                CLOUD_TO_RUNNER,
                "observation_ack",
            )?;
            let envelope = cloud_envelope(frame.message_id, frame.sent_at)?;
            Ok(ValidatedFrame::Cloud(CloudFrame::ObservationAck {
                envelope,
                acknowledged_message_id: frame.payload.acknowledged_message_id.to_string(),
                acknowledged_sequence: frame.payload.acknowledged_sequence.0.get(),
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentOffer(frame) => {
            validate_constants(
                &frame.protocol_version,
                &frame.payload_version,
                &frame.direction,
                &frame.type_,
                CLOUD_TO_RUNNER,
                "assignment_offer",
            )?;
            let envelope = cloud_envelope(frame.message_id, frame.sent_at)?;
            let lease_expires_at = validate_timestamp(&frame.payload.lease_expires_at)?;
            Ok(ValidatedFrame::Cloud(CloudFrame::AssignmentOffer {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                lease_expires_at,
            }))
        }
    }
}

struct RunnerFrameMetadata<'a> {
    protocol_version: &'a Value,
    payload_version: &'a Value,
    direction: &'a Value,
    frame_type: &'a Value,
    expected_type: &'a str,
}

fn validate_runner_frame(
    metadata: RunnerFrameMetadata<'_>,
    sent_at: generated::UtcTimestamp,
) -> Result<(), DecodeError> {
    validate_constants(
        metadata.protocol_version,
        metadata.payload_version,
        metadata.direction,
        metadata.frame_type,
        RUNNER_TO_CLOUD,
        metadata.expected_type,
    )?;
    validate_timestamp(&sent_at)?;
    Ok(())
}

fn cloud_envelope(
    message_id: generated::CloudMessageId,
    sent_at: generated::UtcTimestamp,
) -> Result<CloudEnvelope, DecodeError> {
    Ok(CloudEnvelope {
        message_id: message_id.to_string(),
        sent_at: validate_timestamp(&sent_at)?,
    })
}

fn validate_constants(
    protocol_version: &Value,
    payload_version: &Value,
    direction: &Value,
    frame_type: &Value,
    expected_direction: &str,
    expected_type: &str,
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
    if frame_type.as_str() != Some(expected_type) {
        return Err(DecodeError::InvalidFrame("type"));
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
        for fixture in INVALID_FIXTURES {
            let result = serde_json::from_slice::<generated::RunnerProtocolVersion1>(fixture)
                .ok()
                .and_then(|frame| decode_frame(fixture).ok().map(|_| frame));
            assert!(result.is_none(), "invalid fixture was accepted");
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
            "pongTimeoutSeconds": 19
          }
        }"#;

        assert!(matches!(
            decode_cloud_frame(bytes),
            Err(DecodeError::InvalidFrame("pongTimeoutSeconds"))
        ));
    }
}
