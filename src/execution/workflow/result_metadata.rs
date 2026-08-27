use std::collections::{BTreeMap, BTreeSet};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::de::{Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::public_id::valid_typed_id;

use super::MAXIMUM_PARALLEL_STEPS;
use super::document::FailurePolicy;
use super::publication::{
    CancellationReasonV1, DiagnosticStreamV1, ExportV1, FailureCodeV1, FailurePhaseV1, FailureV1,
    FinalizationTriggerV1, RecoveryHandlerFailureCodeV1, RecoveryHandlerOutcomeV1,
    RecoveryInvocationRoleV1, RecoveryInvocationStateV1, RecoveryTerminationV1, StepReasonV1,
    WorkflowNodeRoleV1, WorkflowOutcomeV1, WorkflowProvenanceV1, WorkflowResultV1,
    WorkflowStepStateV1, WorkflowStepV1,
};
use super::schema_common::{
    is_canonical_absolute_path, is_canonical_relative_path, is_identifier, is_lowercase_hex,
    parse_canonical_utc_timestamp,
};

const MAXIMUM_STEPS: usize = 256;
const MAXIMUM_EXPORTS: usize = 4_096;
const MAXIMUM_CARRIERS: usize = 4_096;
const MAXIMUM_RESULT_STRUCTURE_JSON_BYTES: u64 = 16 * 1024 * 1024;
const MAXIMUM_EXPORT_MEDIA_TYPE_JSON_BYTES: u64 = MAXIMUM_EXPORTS as u64 * 128 * 12;
pub(super) const MAXIMUM_RESULT_NON_STREAM_JSON_BYTES: u64 =
    MAXIMUM_RESULT_STRUCTURE_JSON_BYTES + MAXIMUM_EXPORT_MEDIA_TYPE_JSON_BYTES;
// Durable capture reserves the live run byte budget independently for stdout and
// stderr. Base64 expands their aggregate and may add one padded quartet per stream.
pub(super) const MAXIMUM_ENCODED_RETAINED_STREAM_BYTES: u64 = 2
    * (super::MAXIMUM_RETAINED_STREAM_BYTES_PER_RUN.div_ceil(3) * 4 + 2 * MAXIMUM_STEPS as u64 * 4);
pub(crate) const MAXIMUM_RESULT_JSON_BYTES: u64 =
    MAXIMUM_ENCODED_RETAINED_STREAM_BYTES + MAXIMUM_RESULT_NON_STREAM_JSON_BYTES;
const SHA256_ALGORITHM: &str = "sha256";
const BASE64_ENCODING: &str = "base64";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResultMetadataError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResultDocumentError {
    Encoding,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoverySummaryVersionError {
    Unsupported,
}

pub(crate) fn decode_document(bytes: &[u8]) -> Result<Value, ResultDocumentError> {
    if bytes.starts_with(&[0xef, 0xbb, 0xbf])
        || !bytes.ends_with(b"\n")
        || std::str::from_utf8(bytes).is_err()
    {
        return Err(ResultDocumentError::Encoding);
    }
    serde_json::from_slice::<UniqueValue>(bytes)
        .map(|unique| unique.0)
        .map_err(|_| ResultDocumentError::Json)
}

pub(crate) fn dispatch_recovery_summary_versions(
    document: &Value,
) -> Result<(), RecoverySummaryVersionError> {
    let recoveries = document
        .get("steps")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .chain(
            document
                .get("finalization")
                .and_then(|finalization| finalization.get("finalizers"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        )
        .filter_map(|step| step.get("recovery"));
    for recovery in recoveries {
        if recovery
            .get("schemaVersion")
            .is_some_and(|version| version.as_u64() != Some(1))
        {
            return Err(RecoverySummaryVersionError::Unsupported);
        }
    }
    Ok(())
}

pub(crate) fn validate_document_envelope(document: &mut Value) -> Result<(), ResultMetadataError> {
    let retained_exports = document
        .as_object_mut()
        .and_then(|object| object.get_mut("exports"))
        .filter(|exports| exports.is_object())
        .map(|exports| std::mem::replace(exports, Value::Object(serde_json::Map::new())));
    let validation = serde_json::from_value::<WorkflowResultV1>(document.clone())
        .map_err(|_| ResultMetadataError)
        .and_then(|result| validate(&result));
    if let Some(retained_exports) = retained_exports
        && let Some(exports) = document
            .as_object_mut()
            .and_then(|object| object.get_mut("exports"))
    {
        *exports = retained_exports;
    }
    validation
}

pub(crate) fn decode(bytes: &[u8]) -> Result<WorkflowResultV1, ResultMetadataError> {
    let document = decode_document(bytes).map_err(|_| ResultMetadataError)?;
    dispatch_recovery_summary_versions(&document).map_err(|_| ResultMetadataError)?;
    let result =
        serde_json::from_value::<WorkflowResultV1>(document).map_err(|_| ResultMetadataError)?;
    validate(&result)?;
    Ok(result)
}

pub(crate) fn validate(result: &WorkflowResultV1) -> Result<(), ResultMetadataError> {
    let origin_profile_valid =
        match &result.workflow.provenance {
            WorkflowProvenanceV1::Local { source_root } => {
                is_canonical_absolute_path(source_root)
                    && result
                        .execution
                        .execution_root
                        .as_deref()
                        .is_some_and(is_canonical_absolute_path)
                    && result.execution.capacity.is_none()
            }
            WorkflowProvenanceV1::Cloud {
                project_id,
                repository_connection_id,
                object_format,
                commit_oid,
            } => {
                valid_typed_id(project_id, "prj_")
                    && valid_typed_id(repository_connection_id, "rpc_")
                    && object_format == "sha1"
                    && is_lowercase_hex(commit_oid, 40)
                    && result.execution.execution_root.is_none()
                    && result.execution.capacity.as_ref().is_some_and(|capacity| {
                        valid_cloud_capacity(capacity, &result.workflow.digest)
                    })
            }
        };
    if result.schema_version != 1
        || result.attempt_number == 0
        || !origin_profile_valid
        || !is_canonical_relative_path(&result.workflow.path)
        || result.workflow.digest.algorithm != SHA256_ALGORITHM
        || !is_lowercase_hex(&result.workflow.digest.value, 64)
        || !(1..=MAXIMUM_PARALLEL_STEPS).contains(&result.execution.maximum_parallel_steps)
        || parse_canonical_utc_timestamp(&result.execution.started_at).is_none()
        || parse_canonical_utc_timestamp(&result.execution.finished_at).is_none()
        || result.command_output_policy.encoding != BASE64_ENCODING
        || result
            .command_output_policy
            .maximum_retained_bytes_per_stream
            != super::MAXIMUM_RETAINED_BYTES_PER_STREAM
        || result.steps.is_empty()
        || result
            .finalization
            .as_ref()
            .is_some_and(|finalization| finalization.finalizers.is_empty())
        || result.steps.len()
            + result
                .finalization
                .as_ref()
                .map_or(0, |finalization| finalization.finalizers.len())
            > MAXIMUM_STEPS
        || result.exports.len() > MAXIMUM_EXPORTS
    {
        return Err(ResultMetadataError);
    }

    let total_nodes = result.steps.len()
        + result
            .finalization
            .as_ref()
            .map_or(0, |finalization| finalization.finalizers.len());
    let mut ids = BTreeSet::new();
    validate_steps(
        &result.steps,
        WorkflowNodeRoleV1::Step,
        total_nodes,
        &mut ids,
    )?;
    if let Some(finalization) = &result.finalization {
        validate_steps(
            &finalization.finalizers,
            WorkflowNodeRoleV1::Finalizer,
            total_nodes,
            &mut ids,
        )?;
        validate_finalization(finalization)?;
    }
    validate_outcome(result)?;
    validate_exports(&result.exports)
}

fn validate_outcome(result: &WorkflowResultV1) -> Result<(), ResultMetadataError> {
    if let Some(cancellation) = &result.cancellation
        && (cancellation.reason == CancellationReasonV1::FinalizationForceAbort
            || parse_canonical_utc_timestamp(&cancellation.force_stop_deadline).is_none())
    {
        return Err(ResultMetadataError);
    }

    let ordinary_trigger = if let Some(finalization) = &result.finalization {
        finalization.trigger
    } else if result
        .primary_failure
        .as_ref()
        .is_some_and(|primary| primary.node.role == WorkflowNodeRoleV1::Step)
    {
        FinalizationTriggerV1::Failed
    } else if result.cancellation.is_some() {
        FinalizationTriggerV1::Cancelled
    } else {
        FinalizationTriggerV1::Succeeded
    };
    let ordinary_valid = match ordinary_trigger {
        FinalizationTriggerV1::Succeeded => {
            result.cancellation.is_none() && result.steps.iter().all(step_succeeds_workflow)
        }
        FinalizationTriggerV1::Failed => result
            .primary_failure
            .as_ref()
            .is_some_and(|primary| primary.node.role == WorkflowNodeRoleV1::Step),
        FinalizationTriggerV1::Cancelled => {
            result.cancellation.is_some()
                && result.steps.iter().all(|step| {
                    step_succeeds_workflow(step) || step.state == WorkflowStepStateV1::Cancelled
                })
                && result
                    .steps
                    .iter()
                    .any(|step| step.state == WorkflowStepStateV1::Cancelled)
        }
    };
    if !ordinary_valid {
        return Err(ResultMetadataError);
    }

    let required_finalization_issue = result.finalization.as_ref().is_some_and(|finalization| {
        finalization
            .issues
            .iter()
            .any(|issue| issue.impact == FailurePolicy::Required)
    });
    let finalization_cancelled = result
        .finalization
        .as_ref()
        .and_then(|finalization| finalization.cancellation.as_ref());
    let outcome_valid = match (ordinary_trigger, result.outcome) {
        (FinalizationTriggerV1::Succeeded, WorkflowOutcomeV1::Succeeded) => {
            result.primary_failure.is_none()
                && !required_finalization_issue
                && finalization_cancelled.is_none()
        }
        (FinalizationTriggerV1::Succeeded, WorkflowOutcomeV1::Failed) => {
            required_finalization_issue
                && result
                    .primary_failure
                    .as_ref()
                    .is_some_and(|primary| primary.node.role == WorkflowNodeRoleV1::Finalizer)
        }
        (FinalizationTriggerV1::Succeeded, WorkflowOutcomeV1::Cancelled) => {
            result.primary_failure.is_none()
                && !required_finalization_issue
                && finalization_cancelled.is_some()
        }
        (FinalizationTriggerV1::Failed, WorkflowOutcomeV1::Failed) => true,
        (FinalizationTriggerV1::Cancelled, WorkflowOutcomeV1::Cancelled) => {
            result.primary_failure.is_none()
        }
        _ => false,
    };
    if !outcome_valid {
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
    let candidates = match primary.node.role {
        WorkflowNodeRoleV1::Step => &result.steps,
        WorkflowNodeRoleV1::Finalizer => result
            .finalization
            .as_ref()
            .map(|finalization| &finalization.finalizers)
            .ok_or(ResultMetadataError)?,
    };
    candidates
        .iter()
        .any(|step| {
            step.id == primary.node.id
                && step.role == primary.node.role
                && step.failure_policy == FailurePolicy::Required
                && step.state == WorkflowStepStateV1::Failed
                && step.failure.as_ref() == Some(&failure)
        })
        .then_some(())
        .ok_or(ResultMetadataError)
}

fn step_succeeds_workflow(step: &WorkflowStepV1) -> bool {
    step.state == WorkflowStepStateV1::Succeeded
        || (step.failure_policy == FailurePolicy::Advisory
            && matches!(
                step.state,
                WorkflowStepStateV1::Failed | WorkflowStepStateV1::Blocked
            ))
}

fn valid_cloud_capacity(
    capacity: &super::publication::CloudExecutionCapacityV1,
    workflow_digest: &super::publication::DigestV1,
) -> bool {
    capacity.execution_contract == "workflow_v1_inputless_cloud_artifacts@1"
        && capacity.source_closure_digest == *workflow_digest
        && capacity.general_maximum_transitions >= 1
        && capacity.general_maximum_transitions <= 1_286
        && capacity.selected_maximum_transitions >= 1
        && capacity.selected_maximum_transitions <= 1_030
        && capacity.maximum_invocations >= 1
        && capacity.maximum_invocations <= 488
        && capacity.maximum_retained_bytes_per_invocation >= 1
        && capacity.maximum_retained_bytes_per_invocation <= 4_194_304
        && capacity.diagnostic_retention_bytes >= capacity.maximum_retained_bytes_per_invocation
        && capacity.diagnostic_retention_bytes <= 134_217_728
        && capacity.native_session_retention_bytes >= capacity.maximum_retained_bytes_per_invocation
        && capacity.native_session_retention_bytes <= 67_108_864
        && capacity
            .diagnostic_retention_bytes
            .checked_add(capacity.native_session_retention_bytes)
            == Some(capacity.aggregate_retention_bytes)
        && capacity.aggregate_retention_bytes <= 201_326_592
        && capacity
            .selected_maximum_transitions
            .checked_add(64)
            .and_then(|entries| entries.checked_mul(65_536))
            .and_then(|ordinary| ordinary.checked_add(33_554_432 - 65_536))
            == Some(capacity.encoded_outbox_bytes)
        && capacity.encoded_outbox_bytes <= 105_185_280
}

fn validate_finalization(
    finalization: &super::publication::FinalizationV1,
) -> Result<(), ResultMetadataError> {
    let expected_issues = finalization
        .finalizers
        .iter()
        .filter(|finalizer| {
            finalizer.state == WorkflowStepStateV1::Failed
                || (finalizer.state == WorkflowStepStateV1::Blocked
                    && finalizer.reason == Some(StepReasonV1::InputUnavailable))
        })
        .map(|finalizer| (&finalizer.id, finalizer.failure_policy))
        .collect::<Vec<_>>();
    if finalization.issues.len() != expected_issues.len()
        || finalization
            .issues
            .iter()
            .zip(expected_issues)
            .any(|(issue, (id, impact))| {
                issue.node.id != *id
                    || issue.node.role != WorkflowNodeRoleV1::Finalizer
                    || issue.impact != impact
            })
    {
        return Err(ResultMetadataError);
    }

    match (&finalization.cancellation, finalization.force_abort) {
        (None, false) => {}
        (Some(cancellation), false)
            if cancellation.reason != CancellationReasonV1::FinalizationForceAbort
                && cancellation
                    .force_stop_deadline
                    .as_deref()
                    .and_then(parse_canonical_utc_timestamp)
                    .is_some() => {}
        (Some(cancellation), true)
            if (cancellation.reason == CancellationReasonV1::FinalizationForceAbort
                && cancellation.force_stop_deadline.is_none())
                || (cancellation.reason != CancellationReasonV1::FinalizationForceAbort
                    && cancellation
                        .force_stop_deadline
                        .as_deref()
                        .and_then(parse_canonical_utc_timestamp)
                        .is_some()) => {}
        (None, true) | (Some(_), false | true) => return Err(ResultMetadataError),
    }

    let cancelled_finalizers = finalization
        .finalizers
        .iter()
        .filter(|finalizer| finalizer.state == WorkflowStepStateV1::Cancelled)
        .collect::<Vec<_>>();
    let cancellation_dispositions_valid = match &finalization.cancellation {
        None => cancelled_finalizers.is_empty(),
        Some(cancellation) => {
            let expected_reason = cancellation_step_reason(cancellation.reason);
            !cancelled_finalizers.is_empty()
                && cancelled_finalizers
                    .iter()
                    .all(|finalizer| finalizer.reason == Some(expected_reason))
        }
    };
    cancellation_dispositions_valid
        .then_some(())
        .ok_or(ResultMetadataError)
}

fn cancellation_step_reason(reason: CancellationReasonV1) -> StepReasonV1 {
    match reason {
        CancellationReasonV1::UserRequest => StepReasonV1::UserRequest,
        CancellationReasonV1::TerminationRequest => StepReasonV1::TerminationRequest,
        CancellationReasonV1::CallerOutputFailure => StepReasonV1::CallerOutputFailure,
        CancellationReasonV1::RunnerShutdown => StepReasonV1::RunnerShutdown,
        CancellationReasonV1::ExecutionLeaseExpired => StepReasonV1::ExecutionLeaseExpired,
        CancellationReasonV1::FinalizationForceAbort => StepReasonV1::FinalizationForceAbort,
    }
}

fn validate_steps(
    steps: &[WorkflowStepV1],
    expected_role: WorkflowNodeRoleV1,
    total_nodes: usize,
    ids: &mut BTreeSet<String>,
) -> Result<(), ResultMetadataError> {
    let maximum_stream_bytes = super::maximum_retained_bytes_per_stream(total_nodes);
    for step in steps {
        if !is_identifier(&step.id)
            || !ids.insert(step.id.clone())
            || step.role != expected_role
            || !matches!(step.kind.as_str(), "cmd" | "agent")
        {
            return Err(ResultMetadataError);
        }
        if let Some(failure) = &step.failure {
            validate_failure(failure)?;
        }
        match (&step.started_at, step.duration_milliseconds) {
            (Some(started_at), Some(_)) if parse_canonical_utc_timestamp(started_at).is_some() => {}
            (None, None) => {}
            _ => return Err(ResultMetadataError),
        }
        let exact_fields = match (expected_role, step.state) {
            (_, WorkflowStepStateV1::Succeeded) => {
                step.failure.is_none()
                    && step.dependency.is_none()
                    && step.reason.is_none()
                    && step.unavailable_references.is_none()
            }
            (_, WorkflowStepStateV1::Failed) => {
                step.failure.is_some()
                    && step.dependency.is_none()
                    && step.reason.is_none()
                    && step.unavailable_references.is_none()
            }
            (WorkflowNodeRoleV1::Step, WorkflowStepStateV1::Blocked) => {
                step.failure.is_none()
                    && step.dependency.as_deref().is_some_and(is_identifier)
                    && step.reason.is_none()
                    && step.unavailable_references.is_none()
            }
            (WorkflowNodeRoleV1::Finalizer, WorkflowStepStateV1::Blocked) => {
                step.failure.is_none()
                    && step.dependency.is_none()
                    && step.reason == Some(StepReasonV1::InputUnavailable)
                    && step
                        .unavailable_references
                        .as_ref()
                        .is_some_and(|references| {
                            !references.is_empty()
                                && references.windows(2).all(|pair| pair[0] < pair[1])
                                && references.iter().all(|reference| {
                                    reference.starts_with("outputs.")
                                        && reference.split('.').count() == 3
                                })
                        })
            }
            (WorkflowNodeRoleV1::Step, WorkflowStepStateV1::NotRun) => {
                step.failure.is_none()
                    && step.dependency.is_none()
                    && step.reason == Some(StepReasonV1::FailureStop)
                    && step.unavailable_references.is_none()
            }
            (WorkflowNodeRoleV1::Finalizer, WorkflowStepStateV1::NotRun) => {
                step.failure.is_none()
                    && step.dependency.is_none()
                    && step.reason == Some(StepReasonV1::FinalizerTriggerNotSelected)
                    && step.unavailable_references.is_none()
            }
            (role, WorkflowStepStateV1::Cancelled) => {
                step.failure.is_none()
                    && step.dependency.is_none()
                    && step.unavailable_references.is_none()
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
                    || (role == WorkflowNodeRoleV1::Finalizer
                        && step.reason == Some(StepReasonV1::FinalizationForceAbort)
                        && step.failure.is_none()
                        && step.dependency.is_none()
                        && step.unavailable_references.is_none())
            }
        };
        let timing_present = step.started_at.is_some();
        let output_present = step.command_output.is_some();
        let timing_valid = match step.state {
            WorkflowStepStateV1::Succeeded | WorkflowStepStateV1::Failed => timing_present,
            WorkflowStepStateV1::Blocked | WorkflowStepStateV1::NotRun => !timing_present,
            WorkflowStepStateV1::Cancelled => !output_present || timing_present,
        };
        let output_valid = match (step.kind.as_str(), step.state) {
            ("agent", _) => !output_present,
            ("cmd", WorkflowStepStateV1::Succeeded) => output_present,
            ("cmd", WorkflowStepStateV1::Failed) => {
                output_present
                    == step
                        .failure
                        .as_ref()
                        .is_some_and(|failure| failure.phase != FailurePhaseV1::Start)
            }
            ("cmd", WorkflowStepStateV1::Blocked | WorkflowStepStateV1::NotRun) => !output_present,
            ("cmd", WorkflowStepStateV1::Cancelled) => true,
            _ => false,
        };
        if !exact_fields
            || !timing_valid
            || !output_valid
            || step.command_output.as_ref().is_some_and(|output| {
                !valid_stream(&output.stdout, maximum_stream_bytes)
                    || !valid_stream(&output.stderr, maximum_stream_bytes)
            })
        {
            return Err(ResultMetadataError);
        }
        validate_step_recovery(step)?;
    }
    Ok(())
}

fn validate_step_recovery(step: &WorkflowStepV1) -> Result<(), ResultMetadataError> {
    let Some(recovery) = &step.recovery else {
        return step
            .invocations
            .is_empty()
            .then_some(())
            .ok_or(ResultMetadataError);
    };
    if step.role != WorkflowNodeRoleV1::Step
        || recovery.schema_version != 1
        || !(1..=10).contains(&recovery.configured_retries)
        || recovery.rounds.is_empty()
        || recovery.rounds.len() > usize::from(recovery.configured_retries)
        || step.invocations.is_empty()
    {
        return Err(ResultMetadataError);
    }

    let mut invocation_ids = BTreeSet::new();
    let mut target_executions = BTreeSet::new();
    let mut handler_rounds = BTreeSet::new();
    let mut previous_invocation = 0_u64;
    let mut retained_diagnostic_bytes = 0_u64;
    for invocation in &step.invocations {
        if invocation.invocation_id == 0
            || invocation.invocation_id <= previous_invocation
            || !invocation_ids.insert(invocation.invocation_id)
            || invocation.target_execution.is_some() == invocation.recovery_round.is_some()
            || (invocation.role == RecoveryInvocationRoleV1::Target)
                != invocation.target_execution.is_some()
            || parse_canonical_utc_timestamp(&invocation.started_at).is_none()
            || parse_canonical_utc_timestamp(&invocation.finished_at).is_none()
            || !valid_invocation_duration(invocation)
            || invocation
                .diagnostic_reference
                .as_deref()
                .is_some_and(|reference| !is_canonical_relative_path(reference))
        {
            return Err(ResultMetadataError);
        }
        match invocation.role {
            RecoveryInvocationRoleV1::Target => {
                if !target_executions
                    .insert(invocation.target_execution.ok_or(ResultMetadataError)?)
                {
                    return Err(ResultMetadataError);
                }
            }
            RecoveryInvocationRoleV1::RecoveryHandler => {
                if !handler_rounds.insert(invocation.recovery_round.ok_or(ResultMetadataError)?) {
                    return Err(ResultMetadataError);
                }
            }
        }
        for diagnostic in &invocation.diagnostics {
            if !is_canonical_relative_path(&diagnostic.reference)
                || !valid_stream(&diagnostic.stream, super::MAXIMUM_RETAINED_BYTES_PER_STREAM)
            {
                return Err(ResultMetadataError);
            }
            retained_diagnostic_bytes = retained_diagnostic_bytes
                .checked_add(diagnostic.stream.retained_bytes)
                .ok_or(ResultMetadataError)?;
        }
        previous_invocation = invocation.invocation_id;
    }
    if retained_diagnostic_bytes > super::MAXIMUM_RETAINED_STREAM_BYTES_PER_RUN {
        return Err(ResultMetadataError);
    }

    for (index, round) in recovery.rounds.iter().enumerate() {
        let expected_round = u8::try_from(index + 1).map_err(|_| ResultMetadataError)?;
        if round.number != expected_round
            || round.failed_execution.execution_number != expected_round
            || !validate_failure(&round.failed_execution.failure).is_ok()
            || !invocation_ids.contains(&round.failed_execution.invocation_id)
            || !step.invocations.iter().any(|invocation| {
                invocation.invocation_id == round.failed_execution.invocation_id
                    && invocation.role == RecoveryInvocationRoleV1::Target
                    && invocation.target_execution == Some(expected_round)
                    && invocation.state == RecoveryInvocationStateV1::Settled
            })
        {
            return Err(ResultMetadataError);
        }
        let terminal_handler_outcome = match &recovery.termination {
            RecoveryTerminationV1::GaveUp { round } if *round == expected_round => {
                Some(RecoveryHandlerOutcomeV1::GaveUp)
            }
            RecoveryTerminationV1::HandlerFailed { round, .. } if *round == expected_round => {
                Some(RecoveryHandlerOutcomeV1::Failed)
            }
            RecoveryTerminationV1::Cancelled {
                round,
                active_role: RecoveryInvocationRoleV1::RecoveryHandler,
                ..
            } if *round == expected_round => Some(RecoveryHandlerOutcomeV1::Cancelled),
            _ => None,
        };
        let handler_invocation_id = match (&recovery.handler_kind, &round.handler) {
            (None, None) => None,
            (Some(kind), Some(handler))
                if *kind == handler.kind
                    && handler.invocation_id > round.failed_execution.invocation_id
                    && handler.outcome
                        == terminal_handler_outcome
                            .unwrap_or(RecoveryHandlerOutcomeV1::Recheck) =>
            {
                validate_handler_summary(handler)?;
                let invocation = step
                    .invocations
                    .iter()
                    .find(|invocation| invocation.invocation_id == handler.invocation_id)
                    .ok_or(ResultMetadataError)?;
                let expected_state = if handler.outcome == RecoveryHandlerOutcomeV1::Cancelled {
                    RecoveryInvocationStateV1::Cancelled
                } else {
                    RecoveryInvocationStateV1::Settled
                };
                if invocation.role != RecoveryInvocationRoleV1::RecoveryHandler
                    || invocation.recovery_round != Some(expected_round)
                    || invocation.state != expected_state
                {
                    return Err(ResultMetadataError);
                }
                Some(handler.invocation_id)
            }
            _ => return Err(ResultMetadataError),
        };
        if terminal_handler_outcome.is_none() {
            let next_target = step
                .invocations
                .iter()
                .find(|invocation| {
                    invocation.role == RecoveryInvocationRoleV1::Target
                        && invocation.target_execution == expected_round.checked_add(1)
                })
                .ok_or(ResultMetadataError)?;
            if next_target.invocation_id
                <= handler_invocation_id.unwrap_or(round.failed_execution.invocation_id)
            {
                return Err(ResultMetadataError);
            }
        }
    }

    let last_round = u8::try_from(recovery.rounds.len()).map_err(|_| ResultMetadataError)?;
    let maximum_target = *target_executions
        .iter()
        .next_back()
        .ok_or(ResultMetadataError)?;
    let handler_rounds_valid = match recovery.handler_kind {
        None => handler_rounds.is_empty(),
        Some(_) => handler_rounds.iter().copied().eq(1..=last_round),
    };
    if target_executions.iter().copied().ne(1..=maximum_target) || !handler_rounds_valid {
        return Err(ResultMetadataError);
    }
    match &recovery.termination {
        RecoveryTerminationV1::Recovered { execution_number }
            if step.state == WorkflowStepStateV1::Succeeded
                && *execution_number == last_round.saturating_add(1)
                && maximum_target == *execution_number => {}
        RecoveryTerminationV1::Exhausted { execution_number }
            if step.state == WorkflowStepStateV1::Failed
                && recovery.rounds.len() == usize::from(recovery.configured_retries)
                && *execution_number == last_round.saturating_add(1)
                && maximum_target == *execution_number => {}
        RecoveryTerminationV1::GaveUp { round }
            if step.state == WorkflowStepStateV1::Failed
                && *round == last_round
                && maximum_target == *round
                && recovery.rounds.last().is_some_and(|record| {
                    record
                        .handler
                        .as_ref()
                        .is_some_and(|handler| handler.outcome == RecoveryHandlerOutcomeV1::GaveUp)
                })
                && step.failure.as_ref()
                    == recovery
                        .rounds
                        .last()
                        .map(|round| &round.failed_execution.failure) => {}
        RecoveryTerminationV1::HandlerFailed {
            round,
            handler_failure,
        } if step.state == WorkflowStepStateV1::Failed
            && *round == last_round
            && maximum_target == *round
            && recovery.rounds.last().is_some_and(|record| {
                record.handler.as_ref().is_some_and(|handler| {
                    handler.outcome == RecoveryHandlerOutcomeV1::Failed
                        && handler.failure.as_ref() == Some(handler_failure)
                })
            })
            && step.failure.as_ref()
                == recovery
                    .rounds
                    .last()
                    .map(|round| &round.failed_execution.failure) => {}
        RecoveryTerminationV1::Cancelled {
            round,
            active_role,
            execution_number,
        } if step.state == WorkflowStepStateV1::Cancelled
            && *round == last_round
            && ((*active_role == RecoveryInvocationRoleV1::Target
                && execution_number.is_some_and(|execution| execution == maximum_target))
                || (*active_role == RecoveryInvocationRoleV1::RecoveryHandler
                    && execution_number.is_none()))
            && step.invocations.iter().any(|invocation| {
                invocation.role == *active_role
                    && invocation.state == RecoveryInvocationStateV1::Cancelled
                    && (execution_number.is_none()
                        || invocation.target_execution == *execution_number)
            }) => {}
        _ => return Err(ResultMetadataError),
    }
    Ok(())
}

fn valid_invocation_duration(invocation: &super::publication::RecoveryInvocationV1) -> bool {
    let Some(started) = parse_canonical_utc_timestamp(&invocation.started_at) else {
        return false;
    };
    let Some(finished) = parse_canonical_utc_timestamp(&invocation.finished_at) else {
        return false;
    };
    u64::try_from((finished - started).whole_milliseconds()) == Ok(invocation.duration_milliseconds)
}

fn validate_handler_summary(
    handler: &super::publication::RecoveryHandlerSummaryV1,
) -> Result<(), ResultMetadataError> {
    match handler.outcome {
        RecoveryHandlerOutcomeV1::Recheck | RecoveryHandlerOutcomeV1::GaveUp => {
            if handler.summary.as_deref().is_none_or(|value| {
                value.is_empty()
                    || value.len() > super::recovery::MAXIMUM_RECOVERY_DECISION_TEXT_BYTES
            }) || handler.reason.as_deref().is_none_or(|value| {
                value.is_empty()
                    || value.len() > super::recovery::MAXIMUM_RECOVERY_DECISION_TEXT_BYTES
            }) || handler.failure.is_some()
            {
                return Err(ResultMetadataError);
            }
        }
        RecoveryHandlerOutcomeV1::Failed => {
            let failure = handler.failure.as_ref().ok_or(ResultMetadataError)?;
            if handler.summary.is_some()
                || handler.reason.is_some()
                || !valid_handler_failure(failure)
            {
                return Err(ResultMetadataError);
            }
        }
        RecoveryHandlerOutcomeV1::Cancelled => {
            if handler.summary.is_some() || handler.reason.is_some() || handler.failure.is_some() {
                return Err(ResultMetadataError);
            }
        }
    }
    Ok(())
}

fn valid_handler_failure(failure: &super::publication::RecoveryHandlerFailureV1) -> bool {
    let decision = failure.cause.decision_rejection.is_some();
    let exit = failure.cause.exit_code.is_some();
    match failure.cause.code {
        RecoveryHandlerFailureCodeV1::CommandExitFailed => !decision,
        RecoveryHandlerFailureCodeV1::DecisionInvalid
        | RecoveryHandlerFailureCodeV1::AgentResultInvalid => decision && !exit,
        _ => !decision && !exit,
    }
}

pub(super) fn validate_failure(failure: &FailureV1) -> Result<(), ResultMetadataError> {
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
    let protocol_rejection_valid = cause.protocol_rejection.is_none()
        || matches!(
            cause.code,
            FailureCodeV1::HarnessStartFailed | FailureCodeV1::HarnessProtocolFailed
        ) && matches!(
            failure.phase,
            FailurePhaseV1::Start | FailurePhaseV1::Execution
        );
    (valid && protocol_rejection_valid)
        .then_some(())
        .ok_or(ResultMetadataError)
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
            | FailureCodeV1::OutputInvalidUtf8
            | FailureCodeV1::OutputInvalidJson
            | FailureCodeV1::OutputDuplicateJsonMember
            | FailureCodeV1::OutputJsonSchemaMismatch
            | FailureCodeV1::CapturedFileCountLimit
            | FailureCodeV1::CapturedFileSizeLimit
            | FailureCodeV1::CapturedTotalSizeLimit
            | FailureCodeV1::CapturedGitCarrierCountLimit
            | FailureCodeV1::CapturedGitCarrierSizeLimit
            | FailureCodeV1::CapturedTotalGitCarrierSizeLimit
            | FailureCodeV1::GitExecutionRootRebound
            | FailureCodeV1::GitHeadUnavailable
            | FailureCodeV1::GitBaselineNotAncestor
            | FailureCodeV1::GitCleanlinessUnavailable
            | FailureCodeV1::GitWorkspaceDirty
            | FailureCodeV1::GitTreeUnavailable
            | FailureCodeV1::GitRequiredObjectsUnavailable
            | FailureCodeV1::GitSourceAuthorityChanged
            | FailureCodeV1::GitStructureLimitExceeded
            | FailureCodeV1::GitCommandTimedOut
            | FailureCodeV1::GitBundleGenerationFailed
            | FailureCodeV1::GitBundleProfileInvalid
            | FailureCodeV1::GitBundleVerificationFailed
            | FailureCodeV1::GitWorkspaceChanged
            | FailureCodeV1::GitTemporaryStorageUnavailable
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
        | FailureCodeV1::OutputInvalidUtf8
        | FailureCodeV1::OutputInvalidJson
        | FailureCodeV1::OutputDuplicateJsonMember
        | FailureCodeV1::OutputJsonSchemaMismatch
        | FailureCodeV1::CapturedFileCountLimit
        | FailureCodeV1::CapturedFileSizeLimit
        | FailureCodeV1::CapturedTotalSizeLimit
        | FailureCodeV1::CapturedGitCarrierCountLimit
        | FailureCodeV1::CapturedGitCarrierSizeLimit
        | FailureCodeV1::CapturedTotalGitCarrierSizeLimit
        | FailureCodeV1::GitExecutionRootRebound
        | FailureCodeV1::GitHeadUnavailable
        | FailureCodeV1::GitBaselineNotAncestor
        | FailureCodeV1::GitCleanlinessUnavailable
        | FailureCodeV1::GitWorkspaceDirty
        | FailureCodeV1::GitTreeUnavailable
        | FailureCodeV1::GitRequiredObjectsUnavailable
        | FailureCodeV1::GitSourceAuthorityChanged
        | FailureCodeV1::GitStructureLimitExceeded
        | FailureCodeV1::GitCommandTimedOut
        | FailureCodeV1::GitBundleGenerationFailed
        | FailureCodeV1::GitBundleProfileInvalid
        | FailureCodeV1::GitBundleVerificationFailed
        | FailureCodeV1::GitWorkspaceChanged
        | FailureCodeV1::GitTemporaryStorageUnavailable
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

fn valid_stream(stream: &DiagnosticStreamV1, maximum_stream_bytes: u64) -> bool {
    if stream.encoding != BASE64_ENCODING
        || stream.retained_bytes > maximum_stream_bytes
        || stream.truncated != (stream.discarded_bytes != 0)
        || (stream.discarded_bytes != 0 && stream.retained_bytes != maximum_stream_bytes)
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
        let carrier_path = match export {
            ExportV1::Available {
                kind,
                media_type,
                path,
                size_bytes: _,
                digest,
            } => {
                if !valid_export_kind(kind, media_type)
                    || !valid_digest(digest)
                    || parse_carrier_ordinal(path).is_none()
                {
                    return Err(ResultMetadataError);
                }
                Some(path.as_str())
            }
            ExportV1::GitBranch {
                artifact_version,
                object_format,
                base_oid,
                head_oid,
                tree_oid,
                carrier,
            } => {
                if *artifact_version != 1
                    || object_format != "sha1"
                    || !is_lowercase_hex(base_oid, 40)
                    || !is_lowercase_hex(head_oid, 40)
                    || !is_lowercase_hex(tree_oid, 40)
                    || (base_oid != head_oid) != carrier.is_some()
                {
                    return Err(ResultMetadataError);
                }
                match carrier {
                    Some(carrier)
                        if carrier.media_type == "application/vnd.git.bundle"
                            && valid_digest(&carrier.digest)
                            && parse_carrier_ordinal(&carrier.path).is_some() =>
                    {
                        Some(carrier.path.as_str())
                    }
                    Some(_) => return Err(ResultMetadataError),
                    None => None,
                }
            }
            ExportV1::Unavailable { .. } => None,
        };
        if let Some(path) = carrier_path {
            groups.entry(path).or_default().push((index + 1, export));
        }
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

fn valid_digest(digest: &super::publication::DigestV1) -> bool {
    digest.algorithm == SHA256_ALGORITHM && is_lowercase_hex(&digest.value, 64)
}

pub(super) fn valid_export_kind(kind: &str, media_type: &str) -> bool {
    match kind {
        "file" => media_type.chars().count() <= 128 && super::is_valid_media_type(media_type),
        "text" => media_type == "text/plain; charset=utf-8",
        "json" => media_type == "application/json",
        _ => false,
    }
}

pub(super) fn parse_carrier_ordinal(path: &str) -> Option<usize> {
    let ordinal = path.strip_prefix("exports/")?;
    if ordinal.len() < 4 || !ordinal.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let value = ordinal.parse::<usize>().ok().filter(|value| *value != 0)?;
    (format!("{value:04}") == ordinal).then_some(value)
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
