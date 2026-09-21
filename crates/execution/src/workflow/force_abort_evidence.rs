use super::publication::ForceAbortPhaseV1;
use super::runtime::RunCancellationPhase;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FirstForceAbortPhase {
    Ordinary,
    Finalization,
}

impl From<ForceAbortPhaseV1> for FirstForceAbortPhase {
    fn from(phase: ForceAbortPhaseV1) -> Self {
        match phase {
            ForceAbortPhaseV1::Ordinary => Self::Ordinary,
            ForceAbortPhaseV1::Finalization => Self::Finalization,
        }
    }
}

impl From<RunCancellationPhase> for FirstForceAbortPhase {
    fn from(phase: RunCancellationPhase) -> Self {
        match phase {
            RunCancellationPhase::Ordinary => Self::Ordinary,
            RunCancellationPhase::Finalization => Self::Finalization,
        }
    }
}

pub(crate) fn ordinary_node_cancellation_matches<Reason: Copy + Eq>(
    actual: Reason,
    ordinary_cancellation: Option<Reason>,
    force_abort_reason: Reason,
    first_force_abort_phase: Option<FirstForceAbortPhase>,
) -> bool {
    ordinary_cancellation == Some(actual)
        || (actual == force_abort_reason
            && first_force_abort_phase == Some(FirstForceAbortPhase::Ordinary))
}

pub(crate) fn finalization_node_cancellation_matches<Reason: Copy + Eq>(
    actual: Reason,
    finalization_cancellation: Option<Reason>,
    force_abort_reason: Reason,
    force_abort: bool,
) -> bool {
    finalization_cancellation == Some(actual) || (force_abort && actual == force_abort_reason)
}

pub(crate) fn finalization_cancellation_matches_force_phase<Reason: Copy + Eq>(
    finalization_cancellation: Option<Reason>,
    force_abort_reason: Reason,
    first_force_abort_phase: Option<FirstForceAbortPhase>,
) -> bool {
    first_force_abort_phase != Some(FirstForceAbortPhase::Ordinary)
        || finalization_cancellation == Some(force_abort_reason)
}
