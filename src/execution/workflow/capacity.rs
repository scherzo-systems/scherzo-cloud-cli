use super::resolution::WorkflowContentDigest;
use super::validated::ValidatedWorkflow;
use super::{MAXIMUM_RETAINED_BYTES_PER_STREAM, RUN_LOG_BYTE_BUDGET};

pub(crate) const GENERAL_MAXIMUM_TRANSITIONS_WITHOUT_FINALIZERS: u64 = 1_283;
pub(crate) const GENERAL_MAXIMUM_TRANSITIONS_WITH_FINALIZERS: u64 = 1_286;
pub(crate) const CLOUD_MAXIMUM_TRANSITIONS_WITHOUT_FINALIZERS: u64 = 1_027;
pub(crate) const CLOUD_MAXIMUM_TRANSITIONS_WITH_FINALIZERS: u64 = 1_030;
pub(crate) const RUNNER_OBSERVATION_RESERVE: u64 = 64;
pub(crate) const RUNNER_ORDINARY_FRAME_BYTES: u64 = 65_536;
pub(crate) const RUNNER_TERMINAL_FRAME_BYTES: u64 = 33_554_432;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CapacityCalculationFailure {
    ArithmeticOverflow,
    GeneralTransitionCapacityExceeded,
    CloudTransitionCapacityExceeded,
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
