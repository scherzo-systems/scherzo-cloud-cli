use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};

pub(crate) const REQUEST_LIMIT: usize = 4_096;
pub(crate) const RESPONSE_LIMIT: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Operation {
    Status,
    ReloadCredential,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ControlError {
    InvalidRequest,
    UnsupportedVersion,
    NoPendingCredential,
    PendingRegistrationMismatch,
    PendingAuthenticationFailed,
    PendingProtocolFailed,
    PendingConnectionFailed,
    StateUpdateFailed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProcessState {
    Running,
    Stopping,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConnectionState {
    Connecting,
    Connected,
    BackingOff,
    AuthenticationFailed,
    ProtocolFailed,
    Stopping,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConnectionFailure {
    Network,
    RateLimited,
    CloudUnavailable,
    Authentication,
    Protocol,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct AssignmentCounts {
    pub(crate) preparing: u64,
    pub(crate) accepted: u64,
    pub(crate) running: u64,
    pub(crate) finishing: u64,
    pub(crate) reporting: u64,
}

impl AssignmentCounts {
    pub(crate) fn total(self) -> Option<u64> {
        self.preparing
            .checked_add(self.accepted)?
            .checked_add(self.running)?
            .checked_add(self.finishing)?
            .checked_add(self.reporting)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct StatusSnapshot {
    pub(crate) process_state: ProcessState,
    pub(crate) boot_id: String,
    pub(crate) uptime_milliseconds: u64,
    pub(crate) connection_state: ConnectionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_connected_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) current_credential_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pending_credential_id: Option<String>,
    pub(crate) assignment_counts: AssignmentCounts,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_connection_failure: Option<ConnectionFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Response {
    Status(StatusSnapshot),
    Reloaded { credential_id: String },
    Error(ControlError),
}

#[derive(Serialize)]
#[serde(untagged)]
enum EncodedResponse<'a> {
    Status {
        #[serde(rename = "schemaVersion")]
        schema_version: u8,
        outcome: &'static str,
        status: &'a StatusSnapshot,
    },
    Reloaded {
        #[serde(rename = "schemaVersion")]
        schema_version: u8,
        outcome: &'static str,
        #[serde(rename = "credentialId")]
        credential_id: &'a str,
    },
    Error {
        #[serde(rename = "schemaVersion")]
        schema_version: u8,
        outcome: &'static str,
        error: ControlError,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DecodedResponse {
    schema_version: u8,
    outcome: String,
    #[serde(default)]
    status: Option<StatusSnapshot>,
    #[serde(default)]
    credential_id: Option<String>,
    #[serde(default)]
    error: Option<ControlError>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ProtocolFailure {
    Invalid,
    Oversized,
}

impl fmt::Display for ProtocolFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Runner Serve control protocol is invalid")
    }
}

impl std::error::Error for ProtocolFailure {}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<Operation, ControlError> {
    if bytes.len() > REQUEST_LIMIT
        || bytes.last() != Some(&b'\n')
        || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return Err(ControlError::InvalidRequest);
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes[..bytes.len() - 1])
        .map_err(|_| ControlError::InvalidRequest)?;
    let object = value.as_object().ok_or(ControlError::InvalidRequest)?;
    let keys = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if keys != BTreeSet::from(["operation", "schemaVersion"]) {
        return Err(ControlError::InvalidRequest);
    }
    let version = object
        .get("schemaVersion")
        .and_then(serde_json::Value::as_u64)
        .ok_or(ControlError::InvalidRequest)?;
    if version != 1 {
        return Err(ControlError::UnsupportedVersion);
    }
    match object.get("operation").and_then(serde_json::Value::as_str) {
        Some("status") => Ok(Operation::Status),
        Some("reload_credential") => Ok(Operation::ReloadCredential),
        _ => Err(ControlError::InvalidRequest),
    }
}

pub(crate) fn encode_request(operation: Operation) -> Vec<u8> {
    let operation = match operation {
        Operation::Status => "status",
        Operation::ReloadCredential => "reload_credential",
    };
    let mut request = format!(r#"{{"schemaVersion":1,"operation":"{operation}"}}"#).into_bytes();
    request.push(b'\n');
    request
}

pub(crate) fn encode_response(response: &Response) -> Result<Vec<u8>, ProtocolFailure> {
    let value = match response {
        Response::Status(status) => EncodedResponse::Status {
            schema_version: 1,
            outcome: "ok",
            status,
        },
        Response::Reloaded { credential_id } => EncodedResponse::Reloaded {
            schema_version: 1,
            outcome: "reloaded",
            credential_id,
        },
        Response::Error(error) => EncodedResponse::Error {
            schema_version: 1,
            outcome: "error",
            error: *error,
        },
    };
    let mut bytes = serde_json::to_vec(&value).map_err(|_| ProtocolFailure::Invalid)?;
    bytes.push(b'\n');
    if bytes.len() > RESPONSE_LIMIT {
        return Err(ProtocolFailure::Oversized);
    }
    Ok(bytes)
}

pub(crate) fn decode_response(bytes: &[u8]) -> Result<Response, ProtocolFailure> {
    if bytes.len() > RESPONSE_LIMIT
        || bytes.last() != Some(&b'\n')
        || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return Err(ProtocolFailure::Invalid);
    }
    let response: DecodedResponse =
        serde_json::from_slice(&bytes[..bytes.len() - 1]).map_err(|_| ProtocolFailure::Invalid)?;
    if response.schema_version != 1 {
        return Err(ProtocolFailure::Invalid);
    }
    match (
        response.outcome.as_str(),
        response.status,
        response.credential_id,
        response.error,
    ) {
        ("ok", Some(status), None, None) if valid_status(&status) => Ok(Response::Status(status)),
        ("reloaded", None, Some(credential_id), None)
            if super::validation::valid_typed_id(&credential_id, "rrc_") =>
        {
            Ok(Response::Reloaded { credential_id })
        }
        ("error", None, None, Some(error)) => Ok(Response::Error(error)),
        _ => Err(ProtocolFailure::Invalid),
    }
}

fn valid_status(status: &StatusSnapshot) -> bool {
    super::validation::valid_typed_id(&status.boot_id, "rbt_")
        && status
            .current_credential_id
            .as_deref()
            .is_none_or(|value| super::validation::valid_typed_id(value, "rrc_"))
        && status
            .pending_credential_id
            .as_deref()
            .is_none_or(|value| super::validation::valid_typed_id(value, "rrc_"))
        && status
            .assignment_counts
            .total()
            .is_some_and(|total| total <= 1)
        && status
            .last_connected_at
            .as_deref()
            .is_none_or(valid_timestamp)
}

fn valid_timestamp(value: &str) -> bool {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_is_closed_and_lf_delimited() {
        assert_eq!(
            decode_request(b"{\"schemaVersion\":1,\"operation\":\"status\"}\n"),
            Ok(Operation::Status)
        );
        assert_eq!(
            decode_request(b"{\"schemaVersion\":2,\"operation\":\"status\"}\n"),
            Err(ControlError::UnsupportedVersion)
        );
        for request in [
            b"{\"schemaVersion\":1,\"operation\":\"other\"}\n".as_slice(),
            b"{\"schemaVersion\":1,\"operation\":\"status\",\"path\":\"/tmp/x\"}\n",
            b"{\"schemaVersion\":1,\"operation\":\"status\"}",
            b"{}\n",
        ] {
            assert_eq!(decode_request(request), Err(ControlError::InvalidRequest));
        }
    }

    #[test]
    fn request_and_response_bounds_include_the_lf_delimiter() {
        let mut request = encode_request(Operation::Status);
        request.splice(
            request.len() - 1..request.len() - 1,
            std::iter::repeat_n(b' ', REQUEST_LIMIT - request.len()),
        );
        assert_eq!(request.len(), REQUEST_LIMIT);
        assert_eq!(decode_request(&request), Ok(Operation::Status));
        request.insert(request.len() - 1, b' ');
        assert_eq!(decode_request(&request), Err(ControlError::InvalidRequest));

        let response = Response::Status(StatusSnapshot {
            process_state: ProcessState::Running,
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            uptime_milliseconds: 0,
            connection_state: ConnectionState::Connecting,
            last_connected_at: None,
            current_credential_id: None,
            pending_credential_id: None,
            assignment_counts: AssignmentCounts::default(),
            last_connection_failure: None,
        });
        let mut encoded = encode_response(&response).unwrap();
        encoded.splice(
            encoded.len() - 1..encoded.len() - 1,
            std::iter::repeat_n(b' ', RESPONSE_LIMIT - encoded.len()),
        );
        assert_eq!(encoded.len(), RESPONSE_LIMIT);
        assert_eq!(decode_response(&encoded), Ok(response));
        encoded.insert(encoded.len() - 1, b' ');
        assert_eq!(decode_response(&encoded), Err(ProtocolFailure::Invalid));
    }

    #[test]
    fn canonical_schema_accepts_only_valid_control_fixtures() {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let schema: serde_json::Value = serde_json::from_slice(
            &std::fs::read(manifest.join("schemas/runner-control-v1.schema.json")).unwrap(),
        )
        .unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let fixtures = manifest.join("tests/fixtures/runner-control/v1");
        for (kind, expected) in [("valid", true), ("invalid", false)] {
            for entry in std::fs::read_dir(fixtures.join(kind)).unwrap() {
                let path = entry.unwrap().path();
                let value: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                assert_eq!(validator.is_valid(&value), expected, "{}", path.display());
            }
        }
    }

    #[test]
    fn status_round_trips_with_exact_idle_counts() {
        let response = Response::Status(StatusSnapshot {
            process_state: ProcessState::Running,
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            uptime_milliseconds: 10,
            connection_state: ConnectionState::Connecting,
            last_connected_at: None,
            current_credential_id: Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
            pending_credential_id: None,
            assignment_counts: AssignmentCounts::default(),
            last_connection_failure: None,
        });
        let encoded = encode_response(&response).unwrap();
        assert_eq!(decode_response(&encoded).unwrap(), response);
        assert_eq!(encoded.iter().filter(|byte| **byte == b'\n').count(), 1);
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            value["status"]["assignmentCounts"]
                .as_object()
                .unwrap()
                .len(),
            5
        );
    }
}
