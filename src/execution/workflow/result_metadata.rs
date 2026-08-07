use std::collections::{BTreeMap, BTreeSet};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::de::{Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::publication::{
    DiagnosticStreamV1, ExportV1, FailureCodeV1, FailurePhaseV1, FailureV1, StepReasonV1,
    WorkflowOutcomeV1, WorkflowResultV1, WorkflowStepStateV1, WorkflowStepV1,
};
use super::schema_common::{
    is_canonical_absolute_path, is_canonical_relative_path, is_identifier, is_lowercase_hex,
    utc_timestamp,
};

pub(crate) const MAXIMUM_RESULT_JSON_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_PARALLEL_STEPS: usize = 256;
const MAXIMUM_STEPS: usize = 256;
const MAXIMUM_EXPORTS: usize = 4_096;
const MAXIMUM_CARRIERS: usize = 2_048;
const MAXIMUM_RETAINED_BYTES_PER_STREAM: u64 = 65_536;
const SHA256_ALGORITHM: &str = "sha256";
const BASE64_ENCODING: &str = "base64";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResultMetadataError;

pub(crate) fn decode(bytes: &[u8]) -> Result<WorkflowResultV1, ResultMetadataError> {
    if bytes.starts_with(&[0xef, 0xbb, 0xbf]) || !bytes.ends_with(b"\n") {
        return Err(ResultMetadataError);
    }
    let unique = serde_json::from_slice::<UniqueValue>(bytes).map_err(|_| ResultMetadataError)?;
    let result =
        serde_json::from_value::<WorkflowResultV1>(unique.0).map_err(|_| ResultMetadataError)?;
    validate(&result)?;
    Ok(result)
}

pub(crate) fn validate(result: &WorkflowResultV1) -> Result<(), ResultMetadataError> {
    if result.schema_version != 1
        || result.attempt_number == 0
        || result.workflow.provenance.kind != "local"
        || !is_canonical_relative_path(&result.workflow.path)
        || !is_canonical_absolute_path(&result.workflow.provenance.source_root)
        || result.workflow.digest.algorithm != SHA256_ALGORITHM
        || !is_lowercase_hex(&result.workflow.digest.value, 64)
        || !is_canonical_absolute_path(&result.execution.execution_root)
        || !(1..=MAXIMUM_PARALLEL_STEPS).contains(&result.execution.maximum_parallel_steps)
        || parse_timestamp(&result.execution.started_at).is_none()
        || parse_timestamp(&result.execution.finished_at).is_none()
        || result.command_output_policy.encoding != BASE64_ENCODING
        || result
            .command_output_policy
            .maximum_retained_bytes_per_stream
            != MAXIMUM_RETAINED_BYTES_PER_STREAM
        || result.steps.is_empty()
        || result.steps.len() > MAXIMUM_STEPS
        || result.exports.len() > MAXIMUM_EXPORTS
    {
        return Err(ResultMetadataError);
    }

    validate_outcome(result)?;
    validate_steps(&result.steps)?;
    validate_exports(&result.exports)
}

fn validate_outcome(result: &WorkflowResultV1) -> Result<(), ResultMetadataError> {
    let valid = match result.outcome {
        WorkflowOutcomeV1::Succeeded => {
            result.primary_failure.is_none() && result.cancellation.is_none()
        }
        WorkflowOutcomeV1::Failed => result.primary_failure.is_some(),
        WorkflowOutcomeV1::Cancelled => {
            result.primary_failure.is_none() && result.cancellation.is_some()
        }
    };
    if !valid {
        return Err(ResultMetadataError);
    }

    if let Some(cancellation) = &result.cancellation
        && parse_timestamp(&cancellation.force_stop_deadline).is_none()
    {
        return Err(ResultMetadataError);
    }

    let Some(primary) = &result.primary_failure else {
        return Ok(());
    };
    let failure = FailureV1 {
        phase: primary.phase,
        cause: primary.cause.clone(),
    };
    validate_failure(&failure)?;
    result
        .steps
        .iter()
        .any(|step| {
            step.id == primary.step
                && step.state == WorkflowStepStateV1::Failed
                && step.failure.as_ref() == Some(&failure)
        })
        .then_some(())
        .ok_or(ResultMetadataError)
}

fn validate_steps(steps: &[WorkflowStepV1]) -> Result<(), ResultMetadataError> {
    let mut ids = BTreeSet::new();
    for step in steps {
        if !is_identifier(&step.id)
            || !ids.insert(&step.id)
            || !matches!(step.kind.as_str(), "cmd" | "agent")
        {
            return Err(ResultMetadataError);
        }
        if let Some(failure) = &step.failure {
            validate_failure(failure)?;
        }
        match (&step.started_at, step.duration_milliseconds) {
            (Some(started_at), Some(_)) if parse_timestamp(started_at).is_some() => {}
            (None, None) => {}
            _ => return Err(ResultMetadataError),
        }
        let exact_fields = match step.state {
            WorkflowStepStateV1::Succeeded => {
                step.failure.is_none() && step.dependency.is_none() && step.reason.is_none()
            }
            WorkflowStepStateV1::Failed => {
                step.failure.is_some() && step.dependency.is_none() && step.reason.is_none()
            }
            WorkflowStepStateV1::Blocked => {
                step.failure.is_none()
                    && step.dependency.as_deref().is_some_and(is_identifier)
                    && step.reason.is_none()
            }
            WorkflowStepStateV1::NotRun => {
                step.failure.is_none()
                    && step.dependency.is_none()
                    && step.reason == Some(StepReasonV1::FailureStop)
            }
            WorkflowStepStateV1::Cancelled => {
                step.failure.is_none()
                    && step.dependency.is_none()
                    && matches!(
                        step.reason,
                        Some(
                            StepReasonV1::UserRequest
                                | StepReasonV1::TerminationRequest
                                | StepReasonV1::CallerOutputFailure
                                | StepReasonV1::RunnerShutdown
                                | StepReasonV1::ExecutionLeaseExpired
                        )
                    )
            }
        };
        if !exact_fields
            || (step.kind == "agent" && step.command_output.is_some())
            || step.command_output.as_ref().is_some_and(|output| {
                !valid_stream(&output.stdout) || !valid_stream(&output.stderr)
            })
        {
            return Err(ResultMetadataError);
        }
    }
    Ok(())
}

fn validate_failure(failure: &FailureV1) -> Result<(), ResultMetadataError> {
    let cause = &failure.cause;
    let valid = if is_input_failure_code(cause.code) {
        let valid_input = if cause.code == FailureCodeV1::InputInvalidName {
            cause.collection_index.is_none()
                && cause
                    .input
                    .as_deref()
                    .is_some_and(|input| !is_identifier(input))
        } else {
            cause.input.as_deref().is_none_or(is_identifier)
        };
        failure.phase == FailurePhaseV1::Start
            && cause.output.is_none()
            && cause.exit_code.is_none()
            && valid_input
    } else if is_output_failure_code(cause.code) {
        failure.phase == FailurePhaseV1::OutputCapture
            && cause.input.is_none()
            && cause.collection_index.is_none()
            && cause.exit_code.is_none()
            && cause.output.as_deref().is_some_and(is_identifier)
    } else if cause.code == FailureCodeV1::CommandExit {
        failure.phase == FailurePhaseV1::Execution
            && cause.input.is_none()
            && cause.collection_index.is_none()
            && cause.output.is_none()
            && cause.exit_code != Some(0)
    } else {
        cause.input.is_none()
            && cause.collection_index.is_none()
            && cause.output.is_none()
            && cause.exit_code.is_none()
            && simple_failure_phase(cause.code, failure.phase)
    };
    valid.then_some(()).ok_or(ResultMetadataError)
}

pub(crate) fn is_input_failure_code(code: FailureCodeV1) -> bool {
    matches!(
        code,
        FailureCodeV1::InputInvalidName
            | FailureCodeV1::InputValueCountLimit
            | FailureCodeV1::InputValueSizeLimit
            | FailureCodeV1::InputTotalSizeLimit
            | FailureCodeV1::InputCollectionOrdinalLimit
            | FailureCodeV1::InputTypeMismatch
            | FailureCodeV1::InputSourceUnavailable
            | FailureCodeV1::InputStagingUnavailable
            | FailureCodeV1::InputLiveLimit
    )
}

pub(crate) fn is_output_failure_code(code: FailureCodeV1) -> bool {
    matches!(
        code,
        FailureCodeV1::OutputPathAbsolute
            | FailureCodeV1::OutputPathEscape
            | FailureCodeV1::OutputPathEmpty
            | FailureCodeV1::OutputMissing
            | FailureCodeV1::OutputSymbolicLink
            | FailureCodeV1::OutputParentNotDirectory
            | FailureCodeV1::OutputNotRegularFile
            | FailureCodeV1::OutputSourceUnavailable
            | FailureCodeV1::CapturedFileCountLimit
            | FailureCodeV1::CapturedFileSizeLimit
            | FailureCodeV1::CapturedTotalSizeLimit
            | FailureCodeV1::OutputStagingUnavailable
    )
}

fn simple_failure_phase(code: FailureCodeV1, phase: FailurePhaseV1) -> bool {
    match code {
        FailureCodeV1::StepUnavailable => {
            matches!(phase, FailurePhaseV1::Start | FailurePhaseV1::OutputCapture)
        }
        FailureCodeV1::HarnessStartFailed
        | FailureCodeV1::HarnessInputTooLarge
        | FailureCodeV1::HarnessFailed
        | FailureCodeV1::HarnessProtocolFailed
        | FailureCodeV1::MissingResponse
        | FailureCodeV1::MissingResult
        | FailureCodeV1::ResultValidationLimitExceeded
        | FailureCodeV1::CapturedValueTooLarge
        | FailureCodeV1::ResultSettlementFailed => {
            matches!(phase, FailurePhaseV1::Start | FailurePhaseV1::Execution)
        }
        FailureCodeV1::CommandWaitFailed => phase == FailurePhaseV1::Execution,
        FailureCodeV1::OutputUnsupported | FailureCodeV1::CaptureTaskUnavailable => {
            phase == FailurePhaseV1::OutputCapture
        }
        FailureCodeV1::CommandExit
        | FailureCodeV1::InputInvalidName
        | FailureCodeV1::InputValueCountLimit
        | FailureCodeV1::InputValueSizeLimit
        | FailureCodeV1::InputTotalSizeLimit
        | FailureCodeV1::InputCollectionOrdinalLimit
        | FailureCodeV1::InputTypeMismatch
        | FailureCodeV1::InputSourceUnavailable
        | FailureCodeV1::InputStagingUnavailable
        | FailureCodeV1::InputLiveLimit
        | FailureCodeV1::OutputPathAbsolute
        | FailureCodeV1::OutputPathEscape
        | FailureCodeV1::OutputPathEmpty
        | FailureCodeV1::OutputMissing
        | FailureCodeV1::OutputSymbolicLink
        | FailureCodeV1::OutputParentNotDirectory
        | FailureCodeV1::OutputNotRegularFile
        | FailureCodeV1::OutputSourceUnavailable
        | FailureCodeV1::CapturedFileCountLimit
        | FailureCodeV1::CapturedFileSizeLimit
        | FailureCodeV1::CapturedTotalSizeLimit
        | FailureCodeV1::OutputStagingUnavailable => false,
        FailureCodeV1::PreparationTaskUnavailable
        | FailureCodeV1::InputsUnavailable
        | FailureCodeV1::OutputsUnsupported
        | FailureCodeV1::AgentRuntimeUnavailable
        | FailureCodeV1::AgentStepUnavailable
        | FailureCodeV1::AgentAdmissionUnavailable
        | FailureCodeV1::AgentInputsUnavailable
        | FailureCodeV1::AgentInputMissingUpstream
        | FailureCodeV1::AgentInputTypeMismatch
        | FailureCodeV1::AgentSourceUnavailable
        | FailureCodeV1::AgentSourceTextInvalid
        | FailureCodeV1::AgentResultSchemaUnavailable
        | FailureCodeV1::AgentValueModeInvalid
        | FailureCodeV1::AgentAttachmentCountLimit
        | FailureCodeV1::AgentAttachmentBytesLimit
        | FailureCodeV1::ArtifactStagingMismatch
        | FailureCodeV1::AgentStagingMismatch
        | FailureCodeV1::AgentInputStagingUnavailable
        | FailureCodeV1::ExecutionRootRebound
        | FailureCodeV1::WorkingDirectoryUnavailable
        | FailureCodeV1::WorkingDirectoryEscape
        | FailureCodeV1::WorkingDirectoryNotDirectory
        | FailureCodeV1::CommandArgvInvalid
        | FailureCodeV1::CommandPathUnconfigured
        | FailureCodeV1::ExecutableNotFound
        | FailureCodeV1::ExecutableUnavailable
        | FailureCodeV1::CommandLaunchNotFound
        | FailureCodeV1::CommandLaunchPermissionDenied
        | FailureCodeV1::CommandLaunchInvalidInput
        | FailureCodeV1::CommandLaunchFailed => phase == FailurePhaseV1::Start,
    }
}

fn valid_stream(stream: &DiagnosticStreamV1) -> bool {
    if stream.encoding != BASE64_ENCODING
        || stream.retained_bytes > MAXIMUM_RETAINED_BYTES_PER_STREAM
        || stream.truncated != (stream.discarded_bytes != 0)
        || (stream.discarded_bytes != 0
            && stream.retained_bytes != MAXIMUM_RETAINED_BYTES_PER_STREAM)
    {
        return false;
    }
    BASE64_STANDARD.decode(&stream.data).is_ok_and(|bytes| {
        u64::try_from(bytes.len()) == Ok(stream.retained_bytes)
            && BASE64_STANDARD.encode(bytes) == stream.data
    })
}

fn validate_exports(exports: &BTreeMap<String, ExportV1>) -> Result<(), ResultMetadataError> {
    let mut groups = BTreeMap::<&str, Vec<(usize, &ExportV1)>>::new();
    for (index, (name, export)) in exports.iter().enumerate() {
        if !is_identifier(name) {
            return Err(ResultMetadataError);
        }
        let ExportV1::Available {
            kind,
            media_type,
            path,
            size_bytes: _,
            digest,
        } = export
        else {
            continue;
        };
        if !valid_export_kind(kind, media_type)
            || digest.algorithm != SHA256_ALGORITHM
            || !is_lowercase_hex(&digest.value, 64)
            || parse_carrier_ordinal(path).is_none()
        {
            return Err(ResultMetadataError);
        }
        groups.entry(path).or_default().push((index + 1, export));
    }
    if groups.len() > MAXIMUM_CARRIERS {
        return Err(ResultMetadataError);
    }

    for (path, members) in groups {
        let owner = members
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .min()
            .ok_or(ResultMetadataError)?;
        if parse_carrier_ordinal(path) != Some(owner)
            || members
                .iter()
                .any(|(_, metadata)| *metadata != members[0].1)
        {
            return Err(ResultMetadataError);
        }
    }
    Ok(())
}

fn valid_export_kind(kind: &str, media_type: &str) -> bool {
    match kind {
        "file" => media_type.chars().count() <= 128 && super::is_valid_media_type(media_type),
        "text" => media_type == "text/plain; charset=utf-8",
        "json" => media_type == "application/json",
        _ => false,
    }
}

fn parse_carrier_ordinal(path: &str) -> Option<usize> {
    let ordinal = path.strip_prefix("exports/")?;
    if ordinal.len() < 4 || !ordinal.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let value = ordinal.parse::<usize>().ok().filter(|value| *value != 0)?;
    (format!("{value:04}") == ordinal).then_some(value)
}

fn parse_timestamp(value: &str) -> Option<OffsetDateTime> {
    if !value.ends_with('Z') {
        return None;
    }
    let parsed = OffsetDateTime::parse(value, &Rfc3339).ok()?;
    (utc_timestamp(parsed).ok()?.as_str() == value).then_some(parsed)
}

struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueValueVisitor)
    }
}

struct UniqueValueVisitor;

impl<'de> Visitor<'de> for UniqueValueVisitor {
    type Value = UniqueValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON value without duplicate object members")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(UniqueValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<UniqueValue>()? {
            values.push(value.0);
        }
        Ok(UniqueValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = serde_json::Map::new();
        while let Some((name, value)) = map.next_entry::<String, UniqueValue>()? {
            if values.insert(name, value.0).is_some() {
                return Err(A::Error::custom("duplicate JSON object member"));
            }
        }
        Ok(UniqueValue(Value::Object(values)))
    }
}

#[cfg(test)]
mod tests;
