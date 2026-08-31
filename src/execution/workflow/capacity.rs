use super::resolution::WorkflowContentDigest;
use super::validated::ValidatedWorkflow;
use super::{MAXIMUM_RETAINED_BYTES_PER_STREAM, RUN_LOG_BYTE_BUDGET};

pub(crate) const GENERAL_MAXIMUM_TRANSITIONS_WITHOUT_FINALIZERS: u64 = 1_283;
pub(crate) const GENERAL_MAXIMUM_TRANSITIONS_WITH_FINALIZERS: u64 = 1_286;
pub(crate) const CLOUD_MAXIMUM_TRANSITIONS_WITHOUT_FINALIZERS: u64 = 1_027;
pub(crate) const CLOUD_MAXIMUM_TRANSITIONS_WITH_FINALIZERS: u64 = 1_030;
pub(crate) const RUNNER_OBSERVATION_RESERVE: u64 = 64;
pub(crate) const RUNNER_ORDINARY_FRAME_BYTES: u64 = 262_144;
pub(crate) const RUNNER_TERMINAL_FRAME_BYTES: u64 = 67_108_864;
pub(crate) const MAXIMUM_CONDITION_TRANSITION_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const MAXIMUM_TERMINAL_RESULT_STRUCTURE_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const MAXIMUM_PORTABLE_RESULT_BYTES: u64 = 901_080_408;
const JSON_ESCAPED_POINTER_SOURCE_MULTIPLIER: u64 = 3;
const MAXIMUM_CONDITION_PREDICATE_NODES: u64 = 256;
const MAXIMUM_CONDITION_PREDICATE_DEPTH: u64 = 16;
const MAXIMUM_PREDICATE_PATH_SEGMENT: &[u8] = b"/all/63";
const CONDITION_FALSE_TRANSITION_PREFIX: &[u8] =
    b"{\"state\":\"skipped\",\"detail\":{\"code\":\"condition_false\",\"evaluatedPredicates\":[";
const CONDITION_FALSE_ENTRY_PREFIX: &[u8] = b"{\"path\":\"";
const CONDITION_FALSE_ENTRY_SUFFIX: &[u8] = b"\",\"result\":false}";
const CONDITION_FALSE_TRANSITION_SUFFIX: &[u8] = b"]}}";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CapacityCalculationFailure {
    ArithmeticOverflow,
    GeneralTransitionCapacityExceeded,
    CloudTransitionCapacityExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConditionCapacityFailure {
    ArithmeticOverflow,
    SourceClosureCapacityExceeded,
    ConditionTransitionCapacityExceeded,
    TerminalResultStructureCapacityExceeded,
    PortableResultCapacityExceeded,
    OutboxEntryCapacityExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CapacityCounts {
    pub(crate) steps: u64,
    pub(crate) finalizers: u64,
    pub(crate) recovery_rounds: u64,
    pub(crate) handler_rounds: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ComputedWorkflowCapacity {
    pub(crate) general_maximum_transitions: u64,
    pub(crate) cloud_maximum_transitions: u64,
    pub(crate) maximum_invocations: u64,
    pub(crate) maximum_retained_bytes_per_invocation: u64,
    pub(crate) diagnostic_retention_bytes: u64,
    pub(crate) native_session_retention_bytes: u64,
    pub(crate) aggregate_retention_bytes: u64,
    pub(crate) encoded_outbox_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowCapacity {
    pub(crate) source_closure_digest: WorkflowContentDigest,
    pub(crate) requirements: ComputedWorkflowCapacity,
}

impl WorkflowCapacity {
    pub(crate) fn is_bound_to(&self, digest: &WorkflowContentDigest) -> bool {
        &self.source_closure_digest == digest
    }
}

pub(crate) fn resolve_workflow_capacity(
    workflow: &ValidatedWorkflow,
    source_closure_digest: WorkflowContentDigest,
) -> Result<WorkflowCapacity, CapacityCalculationFailure> {
    let steps = u64::try_from(workflow.steps.len())
        .map_err(|_| CapacityCalculationFailure::ArithmeticOverflow)?;
    let finalizers = u64::try_from(workflow.finalizers.len())
        .map_err(|_| CapacityCalculationFailure::ArithmeticOverflow)?;
    let (recovery_rounds, handler_rounds) = workflow.recoveries.values().try_fold(
        (0_u64, 0_u64),
        |(recovery_rounds, handler_rounds), recovery| {
            let Some(recovery) = recovery else {
                return Ok((recovery_rounds, handler_rounds));
            };
            let retries = u64::from(recovery.retries);
            let recovery_rounds = recovery_rounds
                .checked_add(retries)
                .ok_or(CapacityCalculationFailure::ArithmeticOverflow)?;
            let handler_rounds = if recovery.handler.is_some() {
                handler_rounds
                    .checked_add(retries)
                    .ok_or(CapacityCalculationFailure::ArithmeticOverflow)?
            } else {
                handler_rounds
            };
            Ok((recovery_rounds, handler_rounds))
        },
    )?;
    let requirements = calculate_capacity(CapacityCounts {
        steps,
        finalizers,
        recovery_rounds,
        handler_rounds,
    })?;
    Ok(WorkflowCapacity {
        source_closure_digest,
        requirements,
    })
}

pub(crate) fn calculate_capacity(
    counts: CapacityCounts,
) -> Result<ComputedWorkflowCapacity, CapacityCalculationFailure> {
    let (weighted_nodes, general_maximum_transitions, cloud_maximum_transitions) =
        transition_bounds(counts)?;
    validate_general_transition_bound(general_maximum_transitions, counts.finalizers)?;
    validate_cloud_transition_bound(cloud_maximum_transitions, counts.finalizers)?;

    let maximum_invocations = weighted_nodes
        .checked_add(counts.handler_rounds)
        .ok_or(CapacityCalculationFailure::ArithmeticOverflow)?;
    if maximum_invocations == 0 {
        return Err(CapacityCalculationFailure::ArithmeticOverflow);
    }
    let retained_bytes_per_invocation =
        (RUN_LOG_BYTE_BUDGET / maximum_invocations).min(MAXIMUM_RETAINED_BYTES_PER_STREAM);
    let native_session_retention_bytes =
        checked_product(maximum_invocations, retained_bytes_per_invocation)?;
    let diagnostic_retention_bytes = checked_product(native_session_retention_bytes, 2)?;
    let aggregate_retention_bytes = diagnostic_retention_bytes
        .checked_add(native_session_retention_bytes)
        .ok_or(CapacityCalculationFailure::ArithmeticOverflow)?;
    let encoded_outbox_entries = cloud_maximum_transitions
        .checked_add(RUNNER_OBSERVATION_RESERVE)
        .ok_or(CapacityCalculationFailure::ArithmeticOverflow)?;
    let encoded_outbox_bytes =
        checked_product(encoded_outbox_entries, RUNNER_ORDINARY_FRAME_BYTES)?
            .checked_add(RUNNER_TERMINAL_FRAME_BYTES - RUNNER_ORDINARY_FRAME_BYTES)
            .ok_or(CapacityCalculationFailure::ArithmeticOverflow)?;

    Ok(ComputedWorkflowCapacity {
        general_maximum_transitions,
        cloud_maximum_transitions,
        maximum_invocations,
        maximum_retained_bytes_per_invocation: retained_bytes_per_invocation,
        diagnostic_retention_bytes,
        native_session_retention_bytes,
        aggregate_retention_bytes,
        encoded_outbox_bytes,
    })
}

pub(crate) fn validate_general_transition_bound(
    maximum: u64,
    finalizers: u64,
) -> Result<(), CapacityCalculationFailure> {
    let cap = if finalizers == 0 {
        GENERAL_MAXIMUM_TRANSITIONS_WITHOUT_FINALIZERS
    } else {
        GENERAL_MAXIMUM_TRANSITIONS_WITH_FINALIZERS
    };
    if maximum > cap {
        Err(CapacityCalculationFailure::GeneralTransitionCapacityExceeded)
    } else {
        Ok(())
    }
}

pub(crate) fn validate_cloud_transition_bound(
    maximum: u64,
    finalizers: u64,
) -> Result<(), CapacityCalculationFailure> {
    let cap = if finalizers == 0 {
        CLOUD_MAXIMUM_TRANSITIONS_WITHOUT_FINALIZERS
    } else {
        CLOUD_MAXIMUM_TRANSITIONS_WITH_FINALIZERS
    };
    if maximum > cap {
        Err(CapacityCalculationFailure::CloudTransitionCapacityExceeded)
    } else {
        Ok(())
    }
}

pub(crate) fn transition_bounds(
    counts: CapacityCounts,
) -> Result<(u64, u64, u64), CapacityCalculationFailure> {
    let weighted_nodes = counts
        .steps
        .checked_add(counts.finalizers)
        .and_then(|value| value.checked_add(counts.recovery_rounds))
        .ok_or(CapacityCalculationFailure::ArithmeticOverflow)?;
    let general = weighted_bound(weighted_nodes, counts.finalizers, 5)?;
    let cloud = weighted_bound(weighted_nodes, counts.finalizers, 4)?
        .checked_add(counts.handler_rounds)
        .ok_or(CapacityCalculationFailure::ArithmeticOverflow)?;
    Ok((weighted_nodes, general, cloud))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ConditionEvidenceCapacity {
    pub(crate) maximum_json_escaped_pointer_bytes: u64,
    pub(crate) condition_transition_bytes: u64,
    pub(crate) terminal_result_structure_bytes: u64,
    pub(crate) portable_result_bytes: u64,
}

pub(crate) fn calculate_condition_evidence_capacity(
    source_closure_bytes: u64,
) -> Result<ConditionEvidenceCapacity, ConditionCapacityFailure> {
    let maximum_json_escaped_pointer_bytes =
        condition_checked_product(source_closure_bytes, JSON_ESCAPED_POINTER_SOURCE_MULTIPLIER)?;
    let pointer_failure_transition_bytes = maximum_json_escaped_pointer_bytes
        .checked_add(source_closure_bytes)
        .ok_or(ConditionCapacityFailure::ArithmeticOverflow)?;
    let condition_transition_bytes =
        pointer_failure_transition_bytes.max(condition_false_transition_bound()?);
    let terminal_result_structure_bytes = condition_checked_product(condition_transition_bytes, 2)?;
    let portable_result_bytes = portable_result_bound(terminal_result_structure_bytes)?;

    if source_closure_bytes > super::resolution::MAX_SOURCE_CLOSURE_BYTES {
        return Err(ConditionCapacityFailure::SourceClosureCapacityExceeded);
    }
    if condition_transition_bytes > MAXIMUM_CONDITION_TRANSITION_BYTES {
        return Err(ConditionCapacityFailure::ConditionTransitionCapacityExceeded);
    }
    if terminal_result_structure_bytes > MAXIMUM_TERMINAL_RESULT_STRUCTURE_BYTES {
        return Err(ConditionCapacityFailure::TerminalResultStructureCapacityExceeded);
    }
    if portable_result_bytes > MAXIMUM_PORTABLE_RESULT_BYTES {
        return Err(ConditionCapacityFailure::PortableResultCapacityExceeded);
    }

    Ok(ConditionEvidenceCapacity {
        maximum_json_escaped_pointer_bytes,
        condition_transition_bytes,
        terminal_result_structure_bytes,
        portable_result_bytes,
    })
}

pub(crate) fn calculate_condition_outbox_reservation(
    cloud_maximum_transitions: u64,
    condition_transition_count: u64,
    aggregate_condition_transition_bytes: u64,
    terminal_result_structure_bytes: u64,
) -> Result<u64, ConditionCapacityFailure> {
    let entries = cloud_maximum_transitions
        .checked_add(RUNNER_OBSERVATION_RESERVE)
        .ok_or(ConditionCapacityFailure::ArithmeticOverflow)?;
    let classified_large_entries = condition_transition_count
        .checked_add(1)
        .ok_or(ConditionCapacityFailure::ArithmeticOverflow)?;
    let ordinary_entries = entries
        .checked_sub(classified_large_entries)
        .ok_or(ConditionCapacityFailure::OutboxEntryCapacityExceeded)?;
    condition_checked_product(ordinary_entries, RUNNER_ORDINARY_FRAME_BYTES)?
        .checked_add(aggregate_condition_transition_bytes)
        .and_then(|bytes| bytes.checked_add(terminal_result_structure_bytes))
        .ok_or(ConditionCapacityFailure::ArithmeticOverflow)
}

fn portable_result_bound(
    terminal_result_structure_bytes: u64,
) -> Result<u64, ConditionCapacityFailure> {
    terminal_result_structure_bytes
        .checked_add(super::result_metadata::MAXIMUM_ENCODED_RETAINED_STREAM_BYTES)
        .and_then(|bytes| {
            bytes.checked_add(super::result_metadata::MAXIMUM_EXPORT_MEDIA_TYPE_JSON_BYTES)
        })
        .ok_or(ConditionCapacityFailure::ArithmeticOverflow)
}

fn condition_false_transition_bound() -> Result<u64, ConditionCapacityFailure> {
    let maximum_path_bytes = condition_checked_product(
        MAXIMUM_CONDITION_PREDICATE_DEPTH
            .checked_sub(1)
            .ok_or(ConditionCapacityFailure::ArithmeticOverflow)?,
        condition_encoded_bytes(MAXIMUM_PREDICATE_PATH_SEGMENT)?,
    )?;
    let entry_prefix_bytes = condition_encoded_bytes(CONDITION_FALSE_ENTRY_PREFIX)?;
    let entry_suffix_bytes = condition_encoded_bytes(CONDITION_FALSE_ENTRY_SUFFIX)?;
    let maximum_entry_bytes = entry_prefix_bytes
        .checked_add(maximum_path_bytes)
        .and_then(|bytes| bytes.checked_add(entry_suffix_bytes))
        .ok_or(ConditionCapacityFailure::ArithmeticOverflow)?;
    let entries_bytes =
        condition_checked_product(MAXIMUM_CONDITION_PREDICATE_NODES, maximum_entry_bytes)?
            .checked_add(
                MAXIMUM_CONDITION_PREDICATE_NODES
                    .checked_sub(1)
                    .ok_or(ConditionCapacityFailure::ArithmeticOverflow)?,
            )
            .ok_or(ConditionCapacityFailure::ArithmeticOverflow)?;
    let transition_prefix_bytes = condition_encoded_bytes(CONDITION_FALSE_TRANSITION_PREFIX)?;
    let transition_suffix_bytes = condition_encoded_bytes(CONDITION_FALSE_TRANSITION_SUFFIX)?;
    transition_prefix_bytes
        .checked_add(entries_bytes)
        .and_then(|bytes| bytes.checked_add(transition_suffix_bytes))
        .ok_or(ConditionCapacityFailure::ArithmeticOverflow)
}

fn condition_encoded_bytes(value: &[u8]) -> Result<u64, ConditionCapacityFailure> {
    u64::try_from(value.len()).map_err(|_| ConditionCapacityFailure::ArithmeticOverflow)
}

fn condition_checked_product(left: u64, right: u64) -> Result<u64, ConditionCapacityFailure> {
    left.checked_mul(right)
        .ok_or(ConditionCapacityFailure::ArithmeticOverflow)
}

#[cfg(test)]
mod tests;

pub(crate) fn checked_product(left: u64, right: u64) -> Result<u64, CapacityCalculationFailure> {
    left.checked_mul(right)
        .ok_or(CapacityCalculationFailure::ArithmeticOverflow)
}

fn weighted_bound(
    weighted_nodes: u64,
    finalizers: u64,
    weight: u64,
) -> Result<u64, CapacityCalculationFailure> {
    checked_product(weighted_nodes, weight)?
        .checked_add(if finalizers == 0 { 3 } else { 6 })
        .ok_or(CapacityCalculationFailure::ArithmeticOverflow)
}
