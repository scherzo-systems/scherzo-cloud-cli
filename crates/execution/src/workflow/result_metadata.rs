use std::collections::{BTreeMap, BTreeSet};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::de::{Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use scherzo_cloud_support::valid_typed_id;

use super::MAXIMUM_PARALLEL_STEPS;
use super::document::FailurePolicy;
use super::evidence::{
    CancellationDetail, FailureDetail, NodeDetail, NonExecutionCode, PrimaryIssueDetail,
    PrimaryIssueState,
};
use super::force_abort_evidence::{
    finalization_cancellation_matches_force_phase, finalization_node_cancellation_matches,
    ordinary_node_cancellation_matches,
};
use super::publication::{
    CancellationReasonV1, ContinuationDefinitionSourceV1, ContinuationRecordV1,
    ContinuationRequestedDefinitionV1, DiagnosticStreamV1, ExportProvenanceV1, ExportSourceV1,
    ExportUnavailableReasonV1, ExportV1, FailureCodeV1, FailurePhaseV1, FailureV1,
    FinalizationTriggerV1, ForceAbortPhaseV1, RecoveryHandlerFailureCodeV1,
    RecoveryHandlerOutcomeV1, RecoveryInvocationRoleV1, RecoveryInvocationStateV1,
    RecoveryTerminationV1, RunResultInvariant, WorkflowNodeRoleV1, WorkflowOutcomeV1,
    WorkflowProvenanceV1, WorkflowResultV1, WorkflowStepStateV1, WorkflowStepV1,
};
use super::schema_common::{
    is_canonical_absolute_path, is_canonical_relative_path, is_identifier, is_lowercase_hex,
    parse_canonical_utc_timestamp,
};

const MAXIMUM_STEPS: usize = 256;
const MAXIMUM_EXPORTS: usize = 4_096;
const MAXIMUM_CARRIERS: usize = 4_096;
pub(super) const MAXIMUM_EXPORT_MEDIA_TYPE_JSON_BYTES: u64 = MAXIMUM_EXPORTS as u64 * 128 * 12;
pub(super) const MAXIMUM_RESULT_NON_STREAM_JSON_BYTES: u64 = 64 * 1024 * 1024;
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
    let retained_export_sources = document
        .as_object_mut()
        .and_then(|object| object.get_mut("exportSources"))
        .filter(|sources| sources.is_object())
        .map(|sources| std::mem::replace(sources, Value::Object(serde_json::Map::new())));
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
    if let Some(retained_export_sources) = retained_export_sources
        && let Some(export_sources) = document
            .as_object_mut()
            .and_then(|object| object.get_mut("exportSources"))
    {
        *export_sources = retained_export_sources;
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
    validate_with_invariant(result).map_err(|_| ResultMetadataError)
}

pub(crate) fn validate_with_invariant(result: &WorkflowResultV1) -> Result<(), RunResultInvariant> {
    if result.schema_version != 1 {
        return Err(RunResultInvariant::ResultStructure);
    }
    if result.attempt_number == 0 {
        return Err(RunResultInvariant::AttemptMetadata);
    }
    let (provenance_valid, execution_origin_valid) = match &result.workflow.provenance {
        WorkflowProvenanceV1::Local { source_root } => (
            is_canonical_absolute_path(source_root),
            result
                .execution
                .execution_root
                .as_deref()
                .is_some_and(is_canonical_absolute_path)
                && result.execution.capacity.is_none(),
        ),
        WorkflowProvenanceV1::Cloud {
            project_id,
            repository_connection_id,
            object_format,
            commit_oid,
            source_display_snapshot,
        } => (
            valid_typed_id(project_id, "prj_")
                && valid_typed_id(repository_connection_id, "rpc_")
                && object_format == "sha1"
                && is_lowercase_hex(commit_oid, 40)
                && source_display_snapshot.as_ref().is_none_or(|snapshot| {
                    valid_organization_display_name(&snapshot.organization_display_name)
                        && valid_project_name(&snapshot.project_name)
                        && snapshot.repository.provider_kind == "github"
                        && valid_github_repository_name(&snapshot.repository.full_name)
                }),
            result.execution.execution_root.is_none()
                && result.execution.capacity.as_ref().is_some_and(|capacity| {
                    valid_cloud_capacity(capacity, &result.workflow.digest)
                }),
        ),
    };
    if !provenance_valid
        || !is_canonical_relative_path(&result.workflow.path)
        || result.workflow.digest.algorithm != SHA256_ALGORITHM
        || !is_lowercase_hex(&result.workflow.digest.value, 64)
    {
        return Err(RunResultInvariant::WorkflowMetadata);
    }
    if !execution_origin_valid
        || !(1..=MAXIMUM_PARALLEL_STEPS).contains(&result.execution.maximum_parallel_steps)
        || parse_canonical_utc_timestamp(&result.execution.started_at).is_none()
        || parse_canonical_utc_timestamp(&result.execution.finished_at).is_none()
    {
        return Err(RunResultInvariant::ExecutionMetadata);
    }
    let maximum_stream_bytes = result
        .command_output_policy
        .maximum_retained_bytes_per_stream;
    if result.command_output_policy.encoding != BASE64_ENCODING
        || maximum_stream_bytes == 0
        || maximum_stream_bytes > super::MAXIMUM_RETAINED_BYTES_PER_STREAM
        || result.execution.capacity.as_ref().is_some_and(|capacity| {
            maximum_stream_bytes > capacity.maximum_retained_bytes_per_invocation
        })
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
        return Err(RunResultInvariant::ResultStructure);
    }

    let mut ids = BTreeSet::new();
    validate_steps(
        &result.steps,
        WorkflowNodeRoleV1::Step,
        maximum_stream_bytes,
        &mut ids,
    )
    .map_err(|_| RunResultInvariant::StepMetadata)?;
    if let Some(finalization) = &result.finalization {
        validate_steps(
            &finalization.finalizers,
            WorkflowNodeRoleV1::Finalizer,
            maximum_stream_bytes,
            &mut ids,
        )
        .map_err(|_| RunResultInvariant::FinalizationMetadata)?;
        validate_finalization(finalization, result.force_abort)
            .map_err(|_| RunResultInvariant::FinalizationMetadata)?;
    }
    validate_continuation(result).map_err(|_| RunResultInvariant::Continuation)?;
    validate_force_abort(result).map_err(|_| RunResultInvariant::OutcomeMetadata)?;
    validate_outcome(result).map_err(|_| RunResultInvariant::OutcomeMetadata)?;
    validate_exports(result).map_err(|_| RunResultInvariant::ExportMetadata)
}

fn validate_continuation(result: &WorkflowResultV1) -> Result<(), ResultMetadataError> {
    let inherited_steps = result
        .steps
        .iter()
        .filter(|step| step.state == WorkflowStepStateV1::Inherited)
        .collect::<Vec<_>>();
    let Some(continuation) = &result.continuation else {
        return (inherited_steps.is_empty()
            && result.output_producers.is_empty()
            && result.export_sources.is_empty())
        .then_some(())
        .ok_or(ResultMetadataError);
    };
    if result.attempt_number <= 1 || !validate_continuation_record(continuation) {
        return Err(ResultMetadataError);
    }
    let local_request = match &continuation.request.definition {
        ContinuationRequestedDefinitionV1::Inherited(_) => None,
        ContinuationRequestedDefinitionV1::Replaced { replaced } => Some(matches!(
            replaced,
            super::publication::ContinuationReplacementDefinitionSourceV1::Local(_)
        )),
    };
    let request_valid = match &result.workflow.provenance {
        WorkflowProvenanceV1::Local { .. } => {
            local_request != Some(false) && continuation.request.expected_run_version.is_none()
        }
        WorkflowProvenanceV1::Cloud { .. } => {
            local_request != Some(true) && continuation.request.execution_root.is_none()
        }
    };
    let reexecuted = result
        .steps
        .iter()
        .filter(|step| step.state != WorkflowStepStateV1::Inherited)
        .map(|step| step.id.as_str())
        .collect::<Vec<_>>();
    if !request_valid
        || !result.export_sources.keys().eq(result.exports.keys())
        || reexecuted
            != continuation
                .reexecuted_steps
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        || inherited_steps.len() != continuation.inherited_steps.len()
        || inherited_steps
            .iter()
            .zip(&continuation.inherited_steps)
            .any(|(step, inherited)| {
                let Some(NodeDetail::Inherited(detail)) = &step.detail else {
                    return true;
                };
                step.id != inherited.id
                    || detail.prior_attempt_number.checked_add(1) != Some(result.attempt_number)
                    || detail.prior_state != inherited.prior_state
                    || detail.definition_changed != inherited.definition_changed
            })
    {
        return Err(ResultMetadataError);
    }

    for (node, outputs) in &result.output_producers {
        let step = inherited_steps
            .iter()
            .find(|step| step.id == *node)
            .ok_or(ResultMetadataError)?;
        let NodeDetail::Inherited(detail) = step.detail.as_ref().ok_or(ResultMetadataError)? else {
            return Err(ResultMetadataError);
        };
        if detail.prior_state == super::evidence::InheritedPriorState::Skipped || outputs.is_empty()
        {
            return Err(ResultMetadataError);
        }
        for (output, producer) in outputs {
            if !is_identifier(output)
                || producer.node != *node
                || producer.output != *output
                || !output_producer_matches_inherited_detail(producer, detail)
            {
                return Err(ResultMetadataError);
            }
        }
    }
    Ok(())
}

fn output_producer_matches_inherited_detail(
    producer: &super::runtime::OutputProducer,
    detail: &super::evidence::InheritedDetail,
) -> bool {
    if !super::local_run::is_canonical_uuid(&producer.attempt_id) {
        return false;
    }
    match detail.prior_state {
        super::evidence::InheritedPriorState::Succeeded => {
            producer.attempt_id == detail.prior_attempt_id
                && producer.attempt_number == detail.prior_attempt_number
        }
        super::evidence::InheritedPriorState::Inherited => {
            producer.attempt_number > 0 && producer.attempt_number < detail.prior_attempt_number
        }
        super::evidence::InheritedPriorState::Skipped => false,
    }
}

pub(super) fn validate_continuation_record(record: &ContinuationRecordV1) -> bool {
    let mut requested = BTreeSet::new();
    let mut partition = BTreeSet::new();
    if record.request.from_steps.is_empty()
        || record.request.from_steps != record.from_steps
        || record
            .from_steps
            .iter()
            .any(|id| !is_identifier(id) || !requested.insert(id.as_str()))
        || record
            .reexecuted_steps
            .iter()
            .any(|id| !is_identifier(id) || !partition.insert(id.as_str()))
        || record
            .inherited_steps
            .iter()
            .any(|step| !is_identifier(&step.id) || !partition.insert(step.id.as_str()))
        || !requested.iter().all(|requested| {
            record
                .reexecuted_steps
                .iter()
                .any(|step| step == *requested)
        })
    {
        return false;
    }
    let definition_valid = match (&record.request.definition, &record.definition_source) {
        (
            ContinuationRequestedDefinitionV1::Inherited(_),
            ContinuationDefinitionSourceV1::Inherited {
                manifest_digest,
                prior_manifest_digest,
            },
        ) => valid_digest(manifest_digest) && manifest_digest == prior_manifest_digest,
        (
            ContinuationRequestedDefinitionV1::Replaced { replaced },
            ContinuationDefinitionSourceV1::Replaced {
                manifest_digest,
                prior_manifest_digest,
            },
        ) => {
            let source_valid = match replaced {
                super::publication::ContinuationReplacementDefinitionSourceV1::Local(source) => {
                    is_canonical_absolute_path(&source.path)
                }
                super::publication::ContinuationReplacementDefinitionSourceV1::Cloud(source) => {
                    is_lowercase_hex(&source.commit_oid, 40)
                        && is_canonical_relative_path(&source.workflow_path)
                }
            };
            source_valid && valid_digest(manifest_digest) && valid_digest(prior_manifest_digest)
        }
        _ => false,
    };
    let workspace = &record.workspace;
    let quiescence_valid = workspace
        .quiescence
        .groups_terminated
        .checked_add(workspace.quiescence.groups_absent)
        == Some(workspace.quiescence.groups_recorded)
        && parse_canonical_utc_timestamp(&workspace.quiescence.proven_at).is_some();
    definition_valid
        && record
            .request
            .execution_root
            .as_deref()
            .is_none_or(is_canonical_absolute_path)
        && record
            .request
            .expected_run_version
            .is_none_or(|version| version > 0)
        && is_canonical_absolute_path(&workspace.execution_root)
        && is_canonical_absolute_path(&workspace.prior_execution_root)
        && workspace.start_snapshot.validate(false)
        && workspace
            .prior_settlement_snapshot
            .as_ref()
            .is_none_or(|snapshot| snapshot.validate(true))
        && workspace_modified_valid(workspace)
        && quiescence_valid
}

fn workspace_modified_valid(workspace: &super::publication::ContinuationWorkspaceV1) -> bool {
    let comparable = workspace.execution_root == workspace.prior_execution_root
        && workspace.start_snapshot.unavailable.is_none()
        && workspace.start_snapshot.value.is_some()
        && workspace
            .prior_settlement_snapshot
            .as_ref()
            .is_some_and(|prior| {
                prior.unavailable.is_none()
                    && prior.value.is_some()
                    && prior.algorithm == workspace.start_snapshot.algorithm
                    && prior.settled_by
                        == Some(super::workspace_snapshot::WorkspaceSnapshotSettlementV1::Engine)
            });
    match &workspace.modified {
        super::publication::WorkspaceModifiedV1::Known(modified) if comparable => workspace
            .prior_settlement_snapshot
            .as_ref()
            .is_some_and(|prior| *modified == (prior.value != workspace.start_snapshot.value)),
        super::publication::WorkspaceModifiedV1::Unknown(
            super::publication::WorkspaceModifiedUnknownV1::Unknown,
        ) => !comparable,
        super::publication::WorkspaceModifiedV1::Known(_) => false,
    }
}

fn validate_outcome(result: &WorkflowResultV1) -> Result<(), ResultMetadataError> {
    if let Some(cancellation) = &result.cancellation
        && (cancellation.reason == CancellationReasonV1::ForceAbort
            || parse_canonical_utc_timestamp(&cancellation.force_stop_deadline).is_none())
    {
        return Err(ResultMetadataError);
    }

    let ordinary_trigger = if let Some(finalization) = &result.finalization {
        finalization.trigger
    } else if result
        .primary_issue
        .as_ref()
        .is_some_and(|primary| primary_role(primary) == WorkflowNodeRoleV1::Step)
    {
        FinalizationTriggerV1::Failed
    } else if result.cancellation.is_some()
        || result
            .force_abort
            .is_some_and(|force_abort| force_abort.phase == ForceAbortPhaseV1::Ordinary)
    {
        FinalizationTriggerV1::Cancelled
    } else {
        FinalizationTriggerV1::Succeeded
    };
    let ordinary_valid = match ordinary_trigger {
        FinalizationTriggerV1::Succeeded => {
            result.cancellation.is_none() && result.steps.iter().all(step_succeeds_workflow)
        }
        FinalizationTriggerV1::Failed => result
            .primary_issue
            .as_ref()
            .is_some_and(|primary| primary_role(primary) == WorkflowNodeRoleV1::Step),
        FinalizationTriggerV1::Cancelled => {
            (result.cancellation.is_some()
                || result
                    .force_abort
                    .is_some_and(|force_abort| force_abort.phase == ForceAbortPhaseV1::Ordinary))
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
            result.primary_issue.is_none()
                && !required_finalization_issue
                && finalization_cancelled.is_none()
        }
        (FinalizationTriggerV1::Succeeded, WorkflowOutcomeV1::Failed) => {
            required_finalization_issue
                && result
                    .primary_issue
                    .as_ref()
                    .is_some_and(|primary| primary_role(primary) == WorkflowNodeRoleV1::Finalizer)
        }
        (FinalizationTriggerV1::Succeeded, WorkflowOutcomeV1::Cancelled) => {
            result.primary_issue.is_none()
                && !required_finalization_issue
                && finalization_cancelled.is_some()
        }
        (FinalizationTriggerV1::Failed, WorkflowOutcomeV1::Failed) => true,
        (FinalizationTriggerV1::Cancelled, WorkflowOutcomeV1::Cancelled) => {
            result.primary_issue.is_none()
        }
        _ => false,
    };
    if !outcome_valid {
        return Err(ResultMetadataError);
    }

    let Some(primary) = &result.primary_issue else {
        return Ok(());
    };
    let role = primary_role(primary);
    let detail = primary_node_detail(primary);
    let candidates = match role {
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
                && step.role == role
                && step.failure_policy == FailurePolicy::Required
                && step.state
                    == match primary.state {
                        PrimaryIssueState::Failed => WorkflowStepStateV1::Failed,
                        PrimaryIssueState::Blocked => WorkflowStepStateV1::Blocked,
                    }
                && step.detail.as_ref() == Some(&detail)
        })
        .then_some(())
        .ok_or(ResultMetadataError)
}

fn validate_force_abort(result: &WorkflowResultV1) -> Result<(), ResultMetadataError> {
    let force_abort = result.force_abort;
    if force_abort.is_some_and(|force_abort| {
        force_abort.reason != CancellationReasonV1::ForceAbort
            || (force_abort.phase == ForceAbortPhaseV1::Finalization
                && result.finalization.is_none())
    }) || result
        .finalization
        .as_ref()
        .is_some_and(|finalization| finalization.force_abort != force_abort.is_some())
    {
        return Err(ResultMetadataError);
    }

    let ordinary_cancellation = result
        .cancellation
        .as_ref()
        .map(|cancellation| cancellation_detail(cancellation.reason).code);
    let first_force_abort_phase = force_abort.map(|force_abort| force_abort.phase.into());
    if result
        .steps
        .iter()
        .filter(|step| step.state == WorkflowStepStateV1::Cancelled)
        .any(|step| {
            let Some(NodeDetail::Cancellation(detail)) = step.detail.as_ref() else {
                return true;
            };
            !ordinary_node_cancellation_matches(
                detail.code,
                ordinary_cancellation,
                super::admission::CancellationReason::ForceAbort,
                first_force_abort_phase,
            )
        })
    {
        return Err(ResultMetadataError);
    }
    Ok(())
}

fn valid_organization_display_name(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.chars().count() <= 200
        && !value.chars().any(char::is_control)
}

fn valid_project_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    let is_lowercase_or_digit = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes.first().is_some_and(is_lowercase_or_digit)
        && bytes.last().is_some_and(is_lowercase_or_digit)
        && bytes
            .iter()
            .all(|byte| is_lowercase_or_digit(byte) || *byte == b'-')
}

fn valid_github_repository_name(value: &str) -> bool {
    let valid_part = |part: &str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    };
    value.len() <= 255
        && value.split_once('/').is_some_and(|(owner, repository)| {
            valid_part(owner) && valid_part(repository) && !repository.contains('/')
        })
}

fn primary_role(primary: &super::evidence::PrimaryIssue) -> WorkflowNodeRoleV1 {
    match primary.node.role {
        super::validated::WorkflowNodeRole::Step => WorkflowNodeRoleV1::Step,
        super::validated::WorkflowNodeRole::Finalizer => WorkflowNodeRoleV1::Finalizer,
    }
}

fn primary_node_detail(primary: &super::evidence::PrimaryIssue) -> NodeDetail {
    match &primary.detail {
        PrimaryIssueDetail::Failed(detail) => NodeDetail::Failed(detail.clone()),
        PrimaryIssueDetail::Blocked(detail) => NodeDetail::Blocked(detail.clone()),
    }
}

fn step_succeeds_workflow(step: &WorkflowStepV1) -> bool {
    matches!(
        step.state,
        WorkflowStepStateV1::Succeeded | WorkflowStepStateV1::Skipped
    ) || matches!(
        &step.detail,
        Some(NodeDetail::Inherited(detail))
            if matches!(
                detail.prior_state,
                super::evidence::InheritedPriorState::Succeeded
                    | super::evidence::InheritedPriorState::Skipped
                    | super::evidence::InheritedPriorState::Inherited
            )
    ) || (step.failure_policy == FailurePolicy::Advisory
        && matches!(
            step.state,
            WorkflowStepStateV1::Failed | WorkflowStepStateV1::Blocked
        ))
}

fn valid_cloud_capacity(
    capacity: &super::publication::CloudExecutionCapacityV1,
    workflow_digest: &super::publication::DigestV1,
) -> bool {
    capacity.execution_contract == "workflow_v1_cloud_inputs_artifacts@1"
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
        && valid_condition_capacity(capacity)
}

// jscpd:ignore-start -- Portable-result replay and Runner admission independently validate this trust boundary.
fn valid_condition_capacity(capacity: &super::publication::CloudExecutionCapacityV1) -> bool {
    let Some(entries) = capacity.selected_maximum_transitions.checked_add(64) else {
        return false;
    };
    if capacity.condition_transition_count == 0 {
        return capacity.aggregate_condition_transition_bytes == 0
            && capacity.terminal_result_structure_bytes == 67_108_864
            && capacity.portable_result_bytes == 202_027_692
            && entries
                .checked_mul(262_144)
                .and_then(|ordinary| ordinary.checked_add(67_108_864 - 262_144))
                == Some(capacity.encoded_outbox_bytes);
    }
    let Some(large_entries) = capacity.condition_transition_count.checked_add(1) else {
        return false;
    };
    capacity.condition_transition_count <= 256
        && entries >= large_entries
        && (1..=268_435_456).contains(&capacity.aggregate_condition_transition_bytes)
        && capacity.aggregate_condition_transition_bytes.checked_mul(2)
            == Some(capacity.terminal_result_structure_bytes)
        && capacity.terminal_result_structure_bytes <= 536_870_912
        && capacity
            .terminal_result_structure_bytes
            .checked_add(364_209_496)
            == Some(capacity.portable_result_bytes)
        && capacity.portable_result_bytes <= 901_080_408
        && (entries - large_entries)
            .checked_mul(262_144)
            .and_then(|ordinary| {
                ordinary.checked_add(capacity.aggregate_condition_transition_bytes)
            })
            .and_then(|bytes| bytes.checked_add(capacity.terminal_result_structure_bytes))
            == Some(capacity.encoded_outbox_bytes)
        && capacity.encoded_outbox_bytes <= 1_024_720_896
}
// jscpd:ignore-end

fn validate_finalization(
    finalization: &super::publication::FinalizationV1,
    execution_force_abort: Option<super::publication::ForceAbortV1>,
) -> Result<(), ResultMetadataError> {
    let expected_issues = finalization
        .finalizers
        .iter()
        .filter(|finalizer| {
            matches!(
                finalizer.state,
                WorkflowStepStateV1::Failed | WorkflowStepStateV1::Blocked
            )
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

    let first_force_abort_phase = execution_force_abort.map(|force_abort| force_abort.phase.into());
    if !finalization_cancellation_matches_force_phase(
        finalization
            .cancellation
            .as_ref()
            .map(|cancellation| cancellation.reason),
        CancellationReasonV1::ForceAbort,
        first_force_abort_phase,
    ) {
        return Err(ResultMetadataError);
    }

    match (&finalization.cancellation, finalization.force_abort) {
        (None, false) => {}
        (Some(cancellation), false)
            if cancellation.reason != CancellationReasonV1::ForceAbort
                && cancellation
                    .force_stop_deadline
                    .as_deref()
                    .and_then(parse_canonical_utc_timestamp)
                    .is_some() => {}
        (Some(cancellation), true)
            if (cancellation.reason == CancellationReasonV1::ForceAbort
                && cancellation.force_stop_deadline.is_none())
                || (cancellation.reason != CancellationReasonV1::ForceAbort
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
            let only_trigger_ineligible = cancelled_finalizers.is_empty()
                && cancellation.reason == CancellationReasonV1::ForceAbort
                && execution_force_abort
                    == Some(super::publication::ForceAbortV1 {
                        reason: CancellationReasonV1::ForceAbort,
                        phase: ForceAbortPhaseV1::Ordinary,
                    })
                && finalization.finalizers.iter().all(|finalizer| {
                    matches!(
                        finalizer.detail.as_ref(),
                        Some(NodeDetail::NotRun(detail))
                            if detail.code == NonExecutionCode::FinalizerTriggerNotSelected
                    )
                });
            only_trigger_ineligible
                || !cancelled_finalizers.is_empty()
                    && cancelled_finalizers.iter().all(|finalizer| {
                        let Some(NodeDetail::Cancellation(detail)) = finalizer.detail.as_ref()
                        else {
                            return false;
                        };
                        finalization_node_cancellation_matches(
                            detail.code,
                            Some(cancellation_detail(cancellation.reason).code),
                            super::admission::CancellationReason::ForceAbort,
                            finalization.force_abort,
                        )
                    })
        }
    };
    cancellation_dispositions_valid
        .then_some(())
        .ok_or(ResultMetadataError)
}

fn cancellation_detail(reason: CancellationReasonV1) -> CancellationDetail {
    use super::admission::CancellationReason as Canonical;
    CancellationDetail::new(match reason {
        CancellationReasonV1::UserRequest => Canonical::UserRequest,
        CancellationReasonV1::TerminationRequest => Canonical::TerminationRequest,
        CancellationReasonV1::CallerOutputFailure => Canonical::CallerOutputFailure,
        CancellationReasonV1::RunnerShutdown => Canonical::RunnerShutdown,
        CancellationReasonV1::ExecutionLeaseExpired => Canonical::ExecutionLeaseExpired,
        CancellationReasonV1::ForceAbort => Canonical::ForceAbort,
    })
}

fn validate_steps(
    steps: &[WorkflowStepV1],
    expected_role: WorkflowNodeRoleV1,
    maximum_stream_bytes: u64,
    ids: &mut BTreeSet<String>,
) -> Result<(), ResultMetadataError> {
    for step in steps {
        if !is_identifier(&step.id)
            || !ids.insert(step.id.clone())
            || step.role != expected_role
            || !matches!(step.kind.as_str(), "cmd" | "agent")
        {
            return Err(ResultMetadataError);
        }
        match (&step.started_at, step.duration_milliseconds) {
            (Some(started_at), Some(_)) if parse_canonical_utc_timestamp(started_at).is_some() => {}
            (None, None) => {}
            _ => return Err(ResultMetadataError),
        }
        let exact_fields = match (expected_role, step.state, step.detail.as_ref()) {
            (_, WorkflowStepStateV1::Succeeded, None) => true,
            (
                WorkflowNodeRoleV1::Step,
                WorkflowStepStateV1::Inherited,
                Some(NodeDetail::Inherited(detail)),
            ) => {
                detail.prior_attempt_number > 0
                    && super::local_run::is_canonical_uuid(&detail.prior_attempt_id)
            }
            (_, WorkflowStepStateV1::Failed, Some(NodeDetail::Failed(_))) => true,
            (_, WorkflowStepStateV1::Blocked, Some(NodeDetail::Blocked(_))) => true,
            (_, WorkflowStepStateV1::Skipped, Some(NodeDetail::Skipped(_))) => true,
            (
                WorkflowNodeRoleV1::Step,
                WorkflowStepStateV1::NotRun,
                Some(NodeDetail::NotRun(detail)),
            ) => detail.code == NonExecutionCode::FailureStop,
            (
                WorkflowNodeRoleV1::Finalizer,
                WorkflowStepStateV1::NotRun,
                Some(NodeDetail::NotRun(detail)),
            ) => detail.code == NonExecutionCode::FinalizerTriggerNotSelected,
            (
                WorkflowNodeRoleV1::Step | WorkflowNodeRoleV1::Finalizer,
                WorkflowStepStateV1::Cancelled,
                Some(NodeDetail::Cancellation(_)),
            ) => true,
            _ => false,
        };
        let timing_present = step.started_at.is_some();
        let output_present = step.command_output.is_some();
        let timing_valid = match step.state {
            WorkflowStepStateV1::Succeeded => timing_present,
            WorkflowStepStateV1::Failed => {
                timing_present
                    == !matches!(
                        step.detail,
                        Some(NodeDetail::Failed(ref detail))
                            if detail.phase == super::evidence::FailurePhase::Condition
                    )
            }
            WorkflowStepStateV1::Inherited
            | WorkflowStepStateV1::Blocked
            | WorkflowStepStateV1::Skipped
            | WorkflowStepStateV1::NotRun => !timing_present,
            WorkflowStepStateV1::Cancelled => !output_present || timing_present,
        };
        let failure_phase = match step.detail.as_ref() {
            Some(NodeDetail::Failed(detail)) => Some(detail.phase),
            _ => None,
        };
        let output_valid = match (step.kind.as_str(), step.state) {
            ("agent", _) => !output_present,
            ("cmd", WorkflowStepStateV1::Succeeded) => output_present,
            ("cmd", WorkflowStepStateV1::Failed) => {
                output_present
                    == failure_phase.is_some_and(|phase| {
                        !matches!(
                            phase,
                            super::evidence::FailurePhase::Start
                                | super::evidence::FailurePhase::Condition
                        )
                    })
            }
            (
                "cmd",
                WorkflowStepStateV1::Inherited
                | WorkflowStepStateV1::Blocked
                | WorkflowStepStateV1::Skipped
                | WorkflowStepStateV1::NotRun,
            ) => !output_present,
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
        validate_step_recovery(step, maximum_stream_bytes)?;
    }
    Ok(())
}

fn validate_step_recovery(
    step: &WorkflowStepV1,
    maximum_stream_bytes: u64,
) -> Result<(), ResultMetadataError> {
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
                || !valid_stream(&diagnostic.stream, maximum_stream_bytes)
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
                && terminal_failure_detail(step)
                    == recovery.rounds.last().and_then(|round| {
                        failure_detail_from_recovery(&round.failed_execution.failure)
                    }) => {}
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
            && terminal_failure_detail(step)
                == recovery.rounds.last().and_then(|round| {
                    failure_detail_from_recovery(&round.failed_execution.failure)
                }) => {}
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

fn terminal_failure_detail(step: &WorkflowStepV1) -> Option<FailureDetail> {
    match step.detail.as_ref() {
        Some(NodeDetail::Failed(detail)) => Some(detail.clone()),
        _ => None,
    }
}

fn failure_detail_from_recovery(failure: &FailureV1) -> Option<FailureDetail> {
    let mut cause = serde_json::to_value(&failure.cause)
        .ok()?
        .as_object()?
        .clone();
    cause.insert(
        "phase".to_owned(),
        serde_json::to_value(failure.phase).ok()?,
    );
    serde_json::from_value(Value::Object(cause)).ok()
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
                value.is_empty() || super::recovery::recovery_decision_text_is_too_long(value)
            }) || handler.reason.as_deref().is_none_or(|value| {
                value.is_empty() || super::recovery::recovery_decision_text_is_too_long(value)
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
        FailureCodeV1::CommandWaitFailed | FailureCodeV1::ExecutionTaskUnavailable => {
            phase == FailurePhaseV1::Execution
        }
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

fn validate_exports(result: &WorkflowResultV1) -> Result<(), ResultMetadataError> {
    let mut groups = BTreeMap::<&str, Vec<(usize, &ExportV1)>>::new();
    for (index, (name, export)) in result.exports.iter().enumerate() {
        if !is_identifier(name) {
            return Err(ResultMetadataError);
        }
        let source = result.export_sources.get(name);
        let carrier_path = match export {
            ExportV1::Available {
                kind,
                media_type,
                path,
                size_bytes: _,
                digest,
                provenance,
                producer,
            } => {
                if !valid_export_origin(result, source, provenance.as_ref(), producer.as_ref())
                    || !valid_export_kind(kind, media_type)
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
                provenance,
                producer,
            } => {
                if !valid_export_origin(result, source, provenance.as_ref(), producer.as_ref())
                    || *artifact_version != 1
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
            ExportV1::Unavailable { reason } => {
                if !valid_unavailable_export_source(result, source, *reason) {
                    return Err(ResultMetadataError);
                }
                None
            }
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

fn result_export_source_step<'a>(
    result: &'a WorkflowResultV1,
    source: &ExportSourceV1,
) -> Option<&'a WorkflowStepV1> {
    let steps = match source.node.role {
        WorkflowNodeRoleV1::Step => Some(result.steps.as_slice()),
        WorkflowNodeRoleV1::Finalizer => result
            .finalization
            .as_ref()
            .map(|finalization| finalization.finalizers.as_slice()),
    }?;
    steps.iter().find(|step| step.id == source.node.id)
}

fn continuation_export_source_step<'a>(
    result: &'a WorkflowResultV1,
    source: Option<&'a ExportSourceV1>,
) -> Option<(&'a ExportSourceV1, &'a WorkflowStepV1)> {
    let source = source?;
    Some((source, result_export_source_step(result, source)?))
}

fn valid_export_origin(
    result: &WorkflowResultV1,
    source: Option<&ExportSourceV1>,
    provenance: Option<&ExportProvenanceV1>,
    producer: Option<&super::runtime::OutputProducer>,
) -> bool {
    if result.continuation.is_none() {
        return source.is_none() && provenance.is_none() && producer.is_none();
    }
    let Some((source, step)) = continuation_export_source_step(result, source) else {
        return false;
    };
    export_origin_matches(
        &result.output_producers,
        source,
        step.state,
        provenance,
        producer,
    )
}

fn valid_unavailable_export_source(
    result: &WorkflowResultV1,
    source: Option<&ExportSourceV1>,
    reason: ExportUnavailableReasonV1,
) -> bool {
    if result.continuation.is_none() {
        return source.is_none();
    }
    let Some((source, step)) = continuation_export_source_step(result, source) else {
        return false;
    };
    unavailable_export_source_matches(
        &result.output_producers,
        source,
        step.state,
        inherited_prior_state(step),
        reason,
    )
}

fn inherited_prior_state(step: &WorkflowStepV1) -> Option<super::evidence::InheritedPriorState> {
    match step.detail.as_ref() {
        Some(NodeDetail::Inherited(detail)) => Some(detail.prior_state),
        _ => None,
    }
}

pub(super) fn unavailable_export_source_matches(
    output_producers: &BTreeMap<String, BTreeMap<String, super::runtime::OutputProducer>>,
    source: &ExportSourceV1,
    source_state: WorkflowStepStateV1,
    inherited_prior_state: Option<super::evidence::InheritedPriorState>,
    reason: ExportUnavailableReasonV1,
) -> bool {
    if !is_identifier(&source.node.id)
        || !is_identifier(&source.output)
        || output_producers
            .get(&source.node.id)
            .is_some_and(|outputs| outputs.contains_key(&source.output))
    {
        return false;
    }
    let inherited_source_skipped = source_state == WorkflowStepStateV1::Inherited
        && match inherited_prior_state {
            Some(super::evidence::InheritedPriorState::Skipped) => true,
            Some(super::evidence::InheritedPriorState::Inherited) => {
                !output_producers.contains_key(&source.node.id)
            }
            Some(super::evidence::InheritedPriorState::Succeeded) | None => false,
        };
    inherited_source_skipped && reason == ExportUnavailableReasonV1::Skipped
        || matches!(
            (source_state, source.node.role, reason),
            (
                WorkflowStepStateV1::Skipped,
                _,
                ExportUnavailableReasonV1::Skipped
            ) | (
                WorkflowStepStateV1::Failed,
                _,
                ExportUnavailableReasonV1::Failed
            ) | (
                WorkflowStepStateV1::Blocked,
                WorkflowNodeRoleV1::Step,
                ExportUnavailableReasonV1::Blocked
            ) | (
                WorkflowStepStateV1::Blocked,
                WorkflowNodeRoleV1::Finalizer,
                ExportUnavailableReasonV1::InputUnavailable
            ) | (
                WorkflowStepStateV1::NotRun,
                WorkflowNodeRoleV1::Step,
                ExportUnavailableReasonV1::NotRun
            ) | (
                WorkflowStepStateV1::NotRun,
                WorkflowNodeRoleV1::Finalizer,
                ExportUnavailableReasonV1::TriggerNotSelected
            ) | (
                WorkflowStepStateV1::Cancelled,
                _,
                ExportUnavailableReasonV1::Cancelled
            )
        )
}

pub(super) fn export_origin_matches(
    output_producers: &BTreeMap<String, BTreeMap<String, super::runtime::OutputProducer>>,
    source: &ExportSourceV1,
    source_state: WorkflowStepStateV1,
    provenance: Option<&ExportProvenanceV1>,
    producer: Option<&super::runtime::OutputProducer>,
) -> bool {
    if !is_identifier(&source.node.id) || !is_identifier(&source.output) {
        return false;
    }
    let expected_producer = output_producers
        .get(&source.node.id)
        .and_then(|outputs| outputs.get(&source.output));
    match source_state {
        WorkflowStepStateV1::Inherited => {
            provenance == Some(&ExportProvenanceV1::Inherited)
                && producer.is_some()
                && producer == expected_producer
                && producer.is_some_and(valid_output_producer)
        }
        WorkflowStepStateV1::Succeeded => {
            provenance.is_none() && producer.is_none() && expected_producer.is_none()
        }
        WorkflowStepStateV1::Failed
        | WorkflowStepStateV1::Blocked
        | WorkflowStepStateV1::Skipped
        | WorkflowStepStateV1::NotRun
        | WorkflowStepStateV1::Cancelled => false,
    }
}

fn valid_output_producer(producer: &super::runtime::OutputProducer) -> bool {
    producer.attempt_number > 0
        && super::local_run::is_canonical_uuid(&producer.attempt_id)
        && is_identifier(&producer.node)
        && is_identifier(&producer.output)
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
