use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};
use tokio::sync::{Notify, mpsc};

use super::Sleeper;
use super::artifact_delivery::{
    ArtifactCloudResponse, ArtifactDeliveryBroker, ArtifactDeliveryProtocolFailure,
};
use super::config::{AssignmentConfig, Config};
use super::execution::{AssignmentProcessGuards, ExecutionJob};
use super::source::{HttpSourceCredentialBroker, MaterializationFailure, SourceCredentialBroker};
use crate::execution::workflow::MAXIMUM_PARALLEL_STEPS;
use crate::execution::workflow::admission::{
    AdmissionFailure, AdmissionFailureKind, AdmittedWorkflow, CancellationPolicy,
    CancellationSource, EnvironmentSnapshot, ExecutionContext, ExecutionRootLifecycle,
    ResolvedImports, admit_workflow, default_execution_policy_limits,
};
use crate::execution::workflow::artifact::CaptureCancellation;
use crate::execution::workflow::cancellation::{
    MAXIMUM_CANCELLATION_GRACE, MINIMUM_CANCELLATION_GRACE,
};
use crate::execution::workflow::command_contract::{
    ServeWorkflowContractFailure, require_serve_workflow,
};
#[cfg(test)]
use crate::execution::workflow::resolution;
use crate::runner::control_protocol::AssignmentCounts;
use crate::runner_protocol::{
    AssignmentDecline, ExecutionLeaseGrant, ExecutionLeasePolicy, ExecutionSpecInvalidReason,
    ExecutionSpecV1RunnerProjection, MAXIMUM_ENCODED_FRAME_BYTES, RunnerEnvelope, RunnerFrame,
    RunnerUnableReason, encode_runner_frame,
};

const MAXIMUM_RETAINED_DECISIONS: usize = 256;
pub(super) const MAXIMUM_SERVICE_OBSERVATIONS: usize = 1_344;
pub(super) const OBSERVATION_RESERVE_BASE: usize = 64;
pub(super) const MAXIMUM_TRANSITIONS_PER_NODE: usize = 4;
const FINAL_ACKNOWLEDGEMENT_GRACE: Duration = Duration::from_secs(10);

pub(super) trait WallClockHealth: Send + Sync {
    fn uncertainty(&self) -> Result<Duration, WallClockHealthFailure>;
    fn now_utc(&self) -> Result<OffsetDateTime, WallClockHealthFailure>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WallClockHealthFailure;

pub(super) struct SystemWallClockHealth;

impl WallClockHealth for SystemWallClockHealth {
    fn uncertainty(&self) -> Result<Duration, WallClockHealthFailure> {
        system_wall_clock_uncertainty()
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "SystemWallClockHealth is the runner lease wall-clock boundary"
    )]
    fn now_utc(&self) -> Result<OffsetDateTime, WallClockHealthFailure> {
        Ok(OffsetDateTime::now_utc())
    }
}

#[cfg(target_os = "linux")]
#[allow(
    unsafe_code,
    reason = "adjtimex is the Linux boundary for kernel-maintained clock synchronization health"
)]
fn system_wall_clock_uncertainty() -> Result<Duration, WallClockHealthFailure> {
    // SAFETY: `timex` is a plain C data structure, and `adjtimex` receives a valid,
    // uniquely borrowed pointer for the duration of the call.
    let mut status: libc::timex = unsafe { std::mem::zeroed() };
    // SAFETY: `status` is initialized and exclusively borrowed by the syscall.
    let state = unsafe { libc::adjtimex(&mut status) };
    if state < 0
        || state == libc::TIME_ERROR
        || status.status & libc::STA_UNSYNC != 0
        || status.maxerror < 0
    {
        return Err(WallClockHealthFailure);
    }
    let microseconds = u64::try_from(status.maxerror).map_err(|_| WallClockHealthFailure)?;
    Ok(Duration::from_micros(microseconds))
}

#[cfg(not(target_os = "linux"))]
fn system_wall_clock_uncertainty() -> Result<Duration, WallClockHealthFailure> {
    Err(WallClockHealthFailure)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AssignmentOffer {
    pub(super) effect_id: String,
    pub(super) assignment_id: String,
    pub(super) run_id: String,
    pub(super) project_id: String,
    pub(super) attempt_id: String,
    pub(super) execution_spec: ExecutionSpecV1RunnerProjection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AssignmentStart {
    pub(super) effect_id: String,
    pub(super) assignment_id: String,
    pub(super) run_id: String,
    pub(super) attempt_id: String,
    pub(super) execution_spec_id: String,
    pub(super) lease: ExecutionLeaseGrant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AssignmentRenewal {
    pub(super) effect_id: String,
    pub(super) assignment_id: String,
    pub(super) run_id: String,
    pub(super) attempt_id: String,
    pub(super) lease: ExecutionLeaseGrant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum AssignmentDecision {
    Accepted {
        effect_id: String,
        assignment_id: String,
        offered_execution_spec_id: String,
    },
    Rejected {
        effect_id: String,
        assignment_id: String,
        decline: AssignmentDecline,
    },
}

impl AssignmentDecision {
    pub(super) fn assignment_id(&self) -> &str {
        match self {
            Self::Accepted { assignment_id, .. } | Self::Rejected { assignment_id, .. } => {
                assignment_id
            }
        }
    }

    pub(super) fn runner_frame(&self, envelope: RunnerEnvelope) -> RunnerFrame {
        match self {
            Self::Accepted {
                effect_id,
                assignment_id,
                offered_execution_spec_id,
            } => RunnerFrame::AssignmentAccepted {
                envelope,
                effect_id: effect_id.clone(),
                assignment_id: assignment_id.clone(),
                offered_execution_spec_id: offered_execution_spec_id.clone(),
            },
            Self::Rejected {
                effect_id,
                assignment_id,
                decline,
            } => RunnerFrame::AssignmentRejected {
                envelope,
                effect_id: effect_id.clone(),
                assignment_id: assignment_id.clone(),
                decline: *decline,
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ExecutionReport {
    AssignmentInterrupted {
        reason: String,
    },
    Started,
    Transition {
        execution_event_sequence: u64,
        workflow_event: Value,
    },
    Finished {
        final_execution_event_sequence: u64,
        outcome: Value,
        artifact_delivery: Value,
    },
    Interrupted {
        final_execution_event_sequence: u64,
        reason: String,
        terminal_outcome: Value,
        artifact_delivery: Value,
    },
    Aborted {
        last_execution_event_sequence: u64,
        reason: String,
    },
}

impl ExecutionReport {
    pub(super) fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::AssignmentInterrupted { .. }
                | Self::Finished { .. }
                | Self::Interrupted { .. }
                | Self::Aborted { .. }
        )
    }

    fn runner_frame(
        &self,
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
    ) -> RunnerFrame {
        match self {
            Self::AssignmentInterrupted { reason } => RunnerFrame::AssignmentInterrupted {
                envelope,
                assignment_id,
                attempt_id,
                reason: reason.clone(),
            },
            Self::Started => RunnerFrame::ExecutionStarted {
                envelope,
                assignment_id,
                attempt_id,
            },
            Self::Transition {
                execution_event_sequence,
                workflow_event,
            } => RunnerFrame::ExecutionTransition {
                envelope,
                assignment_id,
                attempt_id,
                execution_event_sequence: *execution_event_sequence,
                workflow_event: workflow_event.clone(),
            },
            Self::Finished {
                final_execution_event_sequence,
                outcome,
                artifact_delivery,
            } => RunnerFrame::ExecutionFinished {
                envelope,
                assignment_id,
                attempt_id,
                final_execution_event_sequence: *final_execution_event_sequence,
                outcome: outcome.clone(),
                artifact_delivery: artifact_delivery.clone(),
            },
            Self::Interrupted {
                final_execution_event_sequence,
                reason,
                terminal_outcome,
                artifact_delivery,
            } => RunnerFrame::ExecutionInterrupted {
                envelope,
                assignment_id,
                attempt_id,
                final_execution_event_sequence: *final_execution_event_sequence,
                reason: reason.clone(),
                terminal_outcome: terminal_outcome.clone(),
                artifact_delivery: artifact_delivery.clone(),
            },
            Self::Aborted {
                last_execution_event_sequence,
                reason,
            } => RunnerFrame::ExecutionAborted {
                envelope,
                assignment_id,
                attempt_id,
                last_execution_event_sequence: *last_execution_event_sequence,
                reason: reason.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ArtifactRequest {
    RegisterCarrier {
        assignment_id: String,
        attempt_id: String,
        portable_owner_path: String,
        media_type: String,
        size_bytes: u64,
        sha256: String,
        idempotency_key: String,
    },
    ConfirmCarrier {
        assignment_id: String,
        attempt_id: String,
        artifact_set_id: String,
        carrier_id: String,
    },
    RegisterResult {
        assignment_id: String,
        attempt_id: String,
        size_bytes: u64,
        sha256: String,
    },
    ConfirmResult {
        assignment_id: String,
        attempt_id: String,
        artifact_set_id: String,
    },
}

impl ArtifactRequest {
    fn assignment_id(&self) -> &str {
        match self {
            Self::RegisterCarrier { assignment_id, .. }
            | Self::ConfirmCarrier { assignment_id, .. }
            | Self::RegisterResult { assignment_id, .. }
            | Self::ConfirmResult { assignment_id, .. } => assignment_id,
        }
    }

    fn runner_frame(&self, envelope: RunnerEnvelope) -> RunnerFrame {
        match self {
            Self::RegisterCarrier {
                assignment_id,
                attempt_id,
                portable_owner_path,
                media_type,
                size_bytes,
                sha256,
                idempotency_key,
            } => RunnerFrame::ArtifactCarrierRegister {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                portable_owner_path: portable_owner_path.clone(),
                media_type: media_type.clone(),
                size_bytes: *size_bytes,
                sha256: sha256.clone(),
                idempotency_key: idempotency_key.clone(),
            },
            Self::ConfirmCarrier {
                assignment_id,
                attempt_id,
                artifact_set_id,
                carrier_id,
            } => RunnerFrame::ArtifactCarrierConfirm {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                artifact_set_id: artifact_set_id.clone(),
                carrier_id: carrier_id.clone(),
            },
            Self::RegisterResult {
                assignment_id,
                attempt_id,
                size_bytes,
                sha256,
            } => RunnerFrame::ArtifactResultRegister {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                size_bytes: *size_bytes,
                sha256: sha256.clone(),
            },
            Self::ConfirmResult {
                assignment_id,
                attempt_id,
                artifact_set_id,
            } => RunnerFrame::ArtifactResultConfirm {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                artifact_set_id: artifact_set_id.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum AssignmentObservation {
    Decision(AssignmentDecision),
    LeaseRenewalRequested {
        assignment_id: String,
        attempt_id: String,
        current_lease_sequence: u64,
    },
    Execution {
        assignment_id: String,
        attempt_id: String,
        report: ExecutionReport,
    },
    Artifact {
        delivery_id: u64,
        request: ArtifactRequest,
    },
}

impl AssignmentObservation {
    pub(super) fn assignment_id(&self) -> &str {
        match self {
            Self::Decision(decision) => decision.assignment_id(),
            Self::LeaseRenewalRequested { assignment_id, .. }
            | Self::Execution { assignment_id, .. } => assignment_id,
            Self::Artifact { request, .. } => request.assignment_id(),
        }
    }

    pub(super) fn is_terminal(&self) -> bool {
        matches!(self, Self::Execution { report, .. } if report.is_terminal())
    }

    pub(super) fn runner_frame(&self, envelope: RunnerEnvelope) -> RunnerFrame {
        match self {
            Self::Decision(decision) => decision.runner_frame(envelope),
            Self::LeaseRenewalRequested {
                assignment_id,
                attempt_id,
                current_lease_sequence,
            } => RunnerFrame::ExecutionLeaseRenewalRequested {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                current_lease_sequence: *current_lease_sequence,
            },
            Self::Execution {
                assignment_id,
                attempt_id,
                report,
            } => report.runner_frame(envelope, assignment_id.clone(), attempt_id.clone()),
            Self::Artifact { request, .. } => request.runner_frame(envelope),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PendingAssignmentObservation {
    pub(super) id: u64,
    pub(super) observation: AssignmentObservation,
}

impl PendingAssignmentObservation {
    pub(super) fn artifact_delivery_id(&self) -> Option<u64> {
        match &self.observation {
            AssignmentObservation::Artifact { delivery_id, .. } => Some(*delivery_id),
            _ => None,
        }
    }
}

struct ObservationEntry {
    id: u64,
    observation: AssignmentObservation,
    replayable: bool,
    encoded: bool,
}

struct ObservationOutboxState {
    entries: VecDeque<ObservationEntry>,
    next_id: u64,
}

#[derive(Clone)]
pub(super) struct ObservationOutbox {
    state: Arc<Mutex<ObservationOutboxState>>,
    changed: Arc<Notify>,
}

impl ObservationOutbox {
    pub(super) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ObservationOutboxState {
                entries: VecDeque::new(),
                next_id: 1,
            })),
            changed: Arc::new(Notify::new()),
        }
    }

    fn reserve(
        &self,
        step_count: usize,
        finalizer_count: usize,
    ) -> Result<usize, AssignmentDecline> {
        let node_count = step_count
            .checked_add(finalizer_count)
            .ok_or_else(environment_unavailable)?;
        if node_count > MAXIMUM_PARALLEL_STEPS {
            return Err(environment_unavailable());
        }
        let transition_entries = MAXIMUM_TRANSITIONS_PER_NODE
            .checked_mul(node_count)
            .and_then(|node_transitions| {
                node_transitions.checked_add(if finalizer_count == 0 { 3 } else { 6 })
            })
            .ok_or_else(environment_unavailable)?;
        let reservation = transition_entries
            .checked_add(OBSERVATION_RESERVE_BASE)
            .ok_or_else(environment_unavailable)?;
        if reservation > MAXIMUM_SERVICE_OBSERVATIONS {
            return Err(environment_unavailable());
        }
        let mut state = self.lock();
        if state.entries.capacity() < reservation {
            let additional = reservation.saturating_sub(state.entries.len());
            state
                .entries
                .try_reserve_exact(additional)
                .map_err(|_| environment_unavailable())?;
        }
        Ok(transition_entries)
    }

    pub(super) fn enqueue(&self, observation: AssignmentObservation) -> Result<u64, OutboxFailure> {
        let sizing_envelope = RunnerEnvelope {
            message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
            sequence: 1,
            sent_at: "2026-07-23T00:00:00Z".to_owned(),
        };
        let encoded = encode_runner_frame(&observation.runner_frame(sizing_envelope))
            .map_err(|_| OutboxFailure::Encoding)?;
        // Leave room for the longest service-generated sequence and RFC 3339 timestamp.
        if encoded.len().saturating_add(64) > MAXIMUM_ENCODED_FRAME_BYTES {
            return Err(OutboxFailure::Encoding);
        }

        let mut state = self.lock();
        if state.entries.len() == MAXIMUM_SERVICE_OBSERVATIONS {
            return Err(OutboxFailure::Capacity);
        }
        let id = state.next_id;
        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or(OutboxFailure::Sequence)?;
        state.entries.push_back(ObservationEntry {
            id,
            observation,
            replayable: true,
            encoded: false,
        });
        drop(state);
        self.changed.notify_waiters();
        Ok(id)
    }

    pub(super) fn pending(
        &self,
        in_flight: &BTreeSet<u64>,
        limit: usize,
    ) -> Vec<PendingAssignmentObservation> {
        self.lock()
            .entries
            .iter()
            .filter(|entry| entry.replayable && !entry.encoded && !in_flight.contains(&entry.id))
            .take(limit)
            .map(|entry| PendingAssignmentObservation {
                id: entry.id,
                observation: entry.observation.clone(),
            })
            .collect()
    }

    fn acknowledge(&self, id: u64) -> Option<AssignmentObservation> {
        let mut state = self.lock();
        let index = state.entries.iter().position(|entry| entry.id == id)?;
        state.entries.remove(index).map(|entry| entry.observation)
    }

    fn mark_encoded(&self, id: u64) {
        if let Some(entry) = self.lock().entries.iter_mut().find(|entry| entry.id == id) {
            entry.encoded = true;
        }
    }

    fn fence_assignment(&self, assignment_id: &str) {
        let mut state = self.lock();
        for entry in &mut state.entries {
            if entry.observation.assignment_id() == assignment_id {
                entry.replayable = false;
            }
        }
        state
            .entries
            .retain(|entry| entry.replayable || entry.encoded);
    }

    fn finish_transport(&self) -> BTreeSet<u64> {
        let mut state = self.lock();
        let removed = state
            .entries
            .iter()
            .filter(|entry| !entry.replayable)
            .map(|entry| entry.id)
            .collect();
        state.entries.retain(|entry| entry.replayable);
        for entry in &mut state.entries {
            entry.encoded = false;
        }
        removed
    }

    fn contains(&self, id: u64) -> bool {
        self.lock().entries.iter().any(|entry| entry.id == id)
    }

    pub(super) fn notification(&self) -> Arc<Notify> {
        Arc::clone(&self.changed)
    }

    pub(super) fn wake(&self) {
        self.changed.notify_waiters();
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().entries.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ObservationOutboxState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OutboxFailure {
    Capacity,
    Encoding,
    Sequence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WelcomePolicyFailure {
    Invalid,
    Changed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AssignmentManagerFailure {
    ConflictingOffer,
    DecisionCapacity,
}

struct RetainedDecision {
    offer: AssignmentOffer,
    response: AssignmentDecision,
    response_observation_id: Option<u64>,
    start: Option<AssignmentStart>,
    renewals: BTreeMap<String, AssignmentRenewal>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AssignmentIdentity {
    assignment_id: String,
    run_id: String,
    project_id: String,
    attempt_id: String,
    workflow_id: String,
    execution_spec_id: String,
}

pub(super) struct AssignmentRoot {
    pub(super) _temporary: TempDir,
    pub(super) execution: PathBuf,
    pub(super) private: PathBuf,
    pub(super) source: PathBuf,
}

pub(super) struct AcceptedAssignment {
    identity: AssignmentIdentity,
    pub(super) root: AssignmentRoot,
    pub(super) admitted: AdmittedWorkflow,
    pub(super) transition_budget: usize,
    pub(super) process_guards: AssignmentProcessGuards,
    pub(super) guard_processes: bool,
}

impl AcceptedAssignment {
    pub(super) fn assignment_id(&self) -> &str {
        &self.identity.assignment_id
    }

    pub(super) fn attempt_id(&self) -> &str {
        &self.identity.attempt_id
    }

    pub(super) fn run_id(&self) -> &str {
        &self.identity.run_id
    }

    pub(super) fn project_id(&self) -> &str {
        &self.identity.project_id
    }

    pub(super) fn workflow_id(&self) -> &str {
        &self.identity.workflow_id
    }
}

#[derive(Clone, Debug)]
pub(super) struct LeaseAuthority {
    pub(super) sequence: u64,
    pub(super) renewal_deadline: Instant,
    pub(super) cancellation_deadline: Instant,
    pub(super) stop_deadline: Instant,
    pub(super) force_stop_deadline: Instant,
    pub(super) expires_deadline: Instant,
    pub(super) terminal_report_delivery_budget: Duration,
    pub(super) revoked: bool,
}

struct RunningAssignment {
    identity: AssignmentIdentity,
    cancellation: CancellationSource,
    cancellation_grace: Duration,
    current_grant: ExecutionLeaseGrant,
    authority_updates: tokio::sync::watch::Sender<LeaseAuthority>,
}

struct PreparingAssignment {
    offer: AssignmentOffer,
    cancellation: CaptureCancellation,
}

struct FinishingAssignment {
    identity: AssignmentIdentity,
    final_observation_id: u64,
    _retained_root: Option<AssignmentRoot>,
}

enum LocalSlot {
    Preparing(PreparingAssignment),
    Accepted(Box<AcceptedAssignment>),
    Running(Box<RunningAssignment>),
    Finishing(FinishingAssignment),
}

pub(super) enum ManagerEvent {
    Prepared {
        offer: Box<AssignmentOffer>,
        admission: Box<Result<AcceptedAssignment, AssignmentDecline>>,
    },
    Finished {
        assignment_id: String,
        final_observation_id: Option<u64>,
        final_delivery_deadline: Option<Instant>,
        retained_root: Option<AssignmentRoot>,
    },
    FinalGraceElapsed {
        assignment_id: String,
        final_observation_id: u64,
        continue_reporting: bool,
    },
}

#[derive(Clone)]
struct AdmissionRuntime {
    pi_installation: Option<crate::execution::pi::ValidatedPiInstallation>,
    claude_code_installation:
        Option<crate::execution::claude_code::ValidatedClaudeCodeInstallation>,
    environment: EnvironmentSnapshot,
    outbox: ObservationOutbox,
    guard_processes: bool,
}

impl AdmissionRuntime {
    fn finish(
        &self,
        offer: &AssignmentOffer,
        root: AssignmentRoot,
        workflow: crate::execution::workflow::resolution::ResolvedWorkflow,
        git_capture: Option<crate::execution::workflow::git_capture::CloudGitCaptureProjection>,
    ) -> Result<AcceptedAssignment, AssignmentDecline> {
        let workflow = require_serve_workflow(workflow).map_err(serve_contract_decline)?;
        let transition_budget = self.outbox.reserve(
            workflow.definition.steps.len(),
            workflow.definition.finalizers.len(),
        )?;
        let cloud_git_capture = git_capture.is_some();
        let context = build_execution_context(
            &offer.execution_spec,
            &root.execution,
            git_capture,
            &self.environment,
            self.pi_installation.as_ref(),
            self.claude_code_installation.as_ref(),
        )?;
        let admitted = admit_workflow(workflow, ResolvedImports::default(), context)
            .map_err(|failure| admission_decline(failure, cloud_git_capture))?;
        Ok(AcceptedAssignment {
            identity: AssignmentIdentity {
                assignment_id: offer.assignment_id.clone(),
                run_id: offer.run_id.clone(),
                project_id: offer.project_id.clone(),
                attempt_id: offer.attempt_id.clone(),
                workflow_id: offer.execution_spec.registered_workflow_id.clone(),
                execution_spec_id: offer.execution_spec.execution_spec_id.clone(),
            },
            root,
            admitted,
            transition_budget,
            process_guards: AssignmentProcessGuards::new(),
            guard_processes: self.guard_processes,
        })
    }
}

pub(super) struct AssignmentManager {
    config: AssignmentConfig,
    pi_installation: Option<crate::execution::pi::ValidatedPiInstallation>,
    claude_code_installation:
        Option<crate::execution::claude_code::ValidatedClaudeCodeInstallation>,
    boot_id: String,
    environment: EnvironmentSnapshot,
    wall_clock: Arc<dyn WallClockHealth>,
    sleeper: Arc<dyn Sleeper>,
    source_broker: Option<Arc<dyn SourceCredentialBroker>>,
    #[cfg(test)]
    fixture_materialized_source: Option<(PathBuf, PathBuf)>,
    lease_policy: Option<ExecutionLeasePolicy>,
    slot: Option<LocalSlot>,
    reporting: Option<AssignmentIdentity>,
    decisions: VecDeque<RetainedDecision>,
    outbox: ObservationOutbox,
    artifact_delivery: ArtifactDeliveryBroker,
    events: mpsc::UnboundedReceiver<ManagerEvent>,
    event_sender: mpsc::UnboundedSender<ManagerEvent>,
    shutting_down: bool,
    guard_processes: bool,
}

impl AssignmentManager {
    #[cfg(test)]
    pub(super) fn new(
        config: &Config,
        boot_id: String,
        wall_clock: Arc<dyn WallClockHealth>,
    ) -> Self {
        Self::new_inner(
            config,
            boot_id,
            wall_clock,
            Arc::new(super::TokioSleeper),
            false,
        )
    }

    pub(super) fn new_with_sleeper(
        config: &Config,
        boot_id: String,
        wall_clock: Arc<dyn WallClockHealth>,
        sleeper: Arc<dyn Sleeper>,
    ) -> Self {
        Self::new_inner(config, boot_id, wall_clock, sleeper, true)
    }

    #[cfg(test)]
    fn new_for_test_with_sleeper(
        config: &Config,
        boot_id: String,
        wall_clock: Arc<dyn WallClockHealth>,
        sleeper: Arc<dyn Sleeper>,
    ) -> Self {
        Self::new_inner(config, boot_id, wall_clock, sleeper, false)
    }

    fn new_inner(
        config: &Config,
        boot_id: String,
        wall_clock: Arc<dyn WallClockHealth>,
        sleeper: Arc<dyn Sleeper>,
        guard_processes: bool,
    ) -> Self {
        let (event_sender, events) = mpsc::unbounded_channel();
        let outbox = ObservationOutbox::new();
        let artifact_delivery = ArtifactDeliveryBroker::new(outbox.clone(), Arc::clone(&sleeper));
        Self {
            config: config.assignment().clone(),
            pi_installation: config.pi_installation().cloned(),
            claude_code_installation: config.claude_code_installation().cloned(),
            boot_id: boot_id.clone(),
            environment: EnvironmentSnapshot::new(std::env::vars_os()),
            wall_clock,
            sleeper,
            source_broker: HttpSourceCredentialBroker::new(
                config.endpoint(),
                config.credential(),
                &boot_id,
            )
            .ok()
            .map(|broker| Arc::new(broker) as Arc<dyn SourceCredentialBroker>),
            #[cfg(test)]
            fixture_materialized_source: config.fixture_materialized_source().cloned(),
            lease_policy: None,
            slot: None,
            reporting: None,
            decisions: VecDeque::new(),
            outbox,
            artifact_delivery,
            events,
            event_sender,
            shutting_down: false,
            guard_processes,
        }
    }

    #[cfg(test)]
    pub(super) fn use_source_broker_fixture(&mut self, broker: Arc<dyn SourceCredentialBroker>) {
        self.fixture_materialized_source = None;
        self.source_broker = Some(broker);
    }

    pub(super) fn retain_lease_policy(
        &mut self,
        policy: &ExecutionLeasePolicy,
    ) -> Result<(), WelcomePolicyFailure> {
        validate_lease_policy(policy)?;
        match &self.lease_policy {
            Some(retained) if retained != policy => Err(WelcomePolicyFailure::Changed),
            Some(_) => Ok(()),
            None => {
                self.lease_policy = Some(policy.clone());
                Ok(())
            }
        }
    }

    pub(super) fn handle_offer(
        &mut self,
        offer: AssignmentOffer,
    ) -> Result<(), AssignmentManagerFailure> {
        self.drain_events();
        if let Some(LocalSlot::Preparing(preparing)) = &self.slot {
            if preparing.offer.assignment_id == offer.assignment_id {
                return if same_assignment(&preparing.offer, &offer) {
                    Ok(())
                } else {
                    Err(AssignmentManagerFailure::ConflictingOffer)
                };
            }
            if preparing.offer.effect_id == offer.effect_id {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            }
        }
        if let Some(index) = self
            .decisions
            .iter()
            .position(|decision| decision.offer.assignment_id == offer.assignment_id)
        {
            if !same_assignment(&self.decisions[index].offer, &offer) {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            }
            return Ok(());
        }
        if self.decisions.iter().any(|decision| {
            decision.offer.effect_id == offer.effect_id && !same_assignment(&decision.offer, &offer)
        }) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }

        if self.shutting_down {
            self.make_decision_room()?;
            let response = rejected(&offer, AssignmentDecline::CapacityUnavailable);
            return self.retain_decision(offer, response);
        }

        self.apply_successor_fences(&offer.assignment_id);
        self.make_decision_room()?;
        if self.slot.is_some() {
            let response = rejected(&offer, AssignmentDecline::CapacityUnavailable);
            return self.retain_decision(offer, response);
        }

        self.begin_source_admission(offer)
    }

    fn begin_source_admission(
        &mut self,
        offer: AssignmentOffer,
    ) -> Result<(), AssignmentManagerFailure> {
        #[cfg(test)]
        if self.fixture_materialized_source.is_some() {
            return self.admit_materialized_source_fixture(offer);
        }
        if let Err(decline) = self.validate_admission_prerequisites(&offer) {
            return self.retain_decision(offer.clone(), rejected(&offer, decline));
        }
        let root = match self.prepare_execution_root(&offer.assignment_id) {
            Ok(root) => root,
            Err(decline) => {
                return self.retain_decision(offer.clone(), rejected(&offer, decline));
            }
        };
        let source = offer.execution_spec.source.clone();
        let Some(broker) = self.source_broker.clone() else {
            return self.retain_decision(
                offer.clone(),
                rejected(
                    &offer,
                    AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable),
                ),
            );
        };

        let cancellation = CaptureCancellation::default();
        let worker_cancellation = cancellation.clone();
        let worker_offer = offer.clone();
        let environment = self.environment.clone();
        let runtime = self.admission_runtime();
        let sender = self.event_sender.clone();
        let wake = self.outbox.clone();
        let worker = std::thread::Builder::new()
            .name("runner-source-preparation".to_owned())
            .spawn(move || {
                let admission = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let materialized = super::source::materialize(
                        broker,
                        &environment,
                        &worker_offer.assignment_id,
                        &source,
                        &worker_cancellation,
                        &root.source,
                        &root.execution,
                        &root.private,
                    )
                    .map_err(materialization_decline)?;
                    let mut root = root;
                    root.execution = materialized.execution_root;
                    runtime.finish(
                        &worker_offer,
                        root,
                        materialized.workflow,
                        materialized.git_capture,
                    )
                }))
                .unwrap_or_else(|_| Err(environment_unavailable()));
                let _ = sender.send(ManagerEvent::Prepared {
                    offer: Box::new(worker_offer),
                    admission: Box::new(admission),
                });
                wake.wake();
            });
        if worker.is_err() {
            return self
                .retain_decision(offer.clone(), rejected(&offer, environment_unavailable()));
        }
        self.slot = Some(LocalSlot::Preparing(PreparingAssignment {
            offer,
            cancellation,
        }));
        Ok(())
    }

    #[cfg(test)]
    fn admit_materialized_source_fixture(
        &mut self,
        offer: AssignmentOffer,
    ) -> Result<(), AssignmentManagerFailure> {
        let admission = self
            .validate_admission_prerequisites(&offer)
            .and_then(|()| {
                let root = self.prepare_execution_root(&offer.assignment_id)?;
                let (source_root, workflow_path) = self
                    .fixture_materialized_source
                    .as_ref()
                    .expect("fixture source is present");
                let workflow = resolution::resolve(source_root, workflow_path).map_err(|_| {
                    AssignmentDecline::ExecutionSpecInvalid(
                        ExecutionSpecInvalidReason::WorkflowSourceInvalid,
                    )
                })?;
                self.admission_runtime()
                    .finish(&offer, root, workflow, None)
            });
        let response = match admission {
            Ok(accepted) => {
                self.slot = Some(LocalSlot::Accepted(Box::new(accepted)));
                AssignmentDecision::Accepted {
                    effect_id: offer.effect_id.clone(),
                    assignment_id: offer.assignment_id.clone(),
                    offered_execution_spec_id: offer.execution_spec.execution_spec_id.clone(),
                }
            }
            Err(decline) => rejected(&offer, decline),
        };
        let accepted = matches!(response, AssignmentDecision::Accepted { .. });
        if let Err(failure) = self.retain_decision(offer, response) {
            self.slot = None;
            return Err(failure);
        }
        if !accepted {
            self.slot = None;
        }
        Ok(())
    }

    pub(super) fn handle_start(
        &mut self,
        start: AssignmentStart,
    ) -> Result<Option<ExecutionJob>, AssignmentManagerFailure> {
        self.drain_events();
        if self.decisions.iter().any(|decision| {
            decision
                .start
                .as_ref()
                .is_some_and(|known| known.effect_id == start.effect_id && known != &start)
        }) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        let Some(index) = self
            .decisions
            .iter()
            .position(|decision| decision.offer.assignment_id == start.assignment_id)
        else {
            return Ok(None);
        };
        if !start_matches_offer(&start, &self.decisions[index].offer) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if let Some(known) = &self.decisions[index].start {
            return if known == &start {
                Ok(None)
            } else {
                Err(AssignmentManagerFailure::ConflictingOffer)
            };
        }
        self.decisions[index].start = Some(start.clone());

        let Some(slot) = self.slot.take() else {
            return Ok(None);
        };
        let LocalSlot::Accepted(accepted) = slot else {
            self.slot = Some(slot);
            return Ok(None);
        };
        if accepted.identity.assignment_id != start.assignment_id {
            self.slot = Some(LocalSlot::Accepted(accepted));
            return Ok(None);
        }
        let cancellation_grace = accepted.admitted.execution().cancellation().grace();
        let authority = match self.validate_grant(&start.lease, 1, cancellation_grace) {
            Ok(authority) => authority,
            Err(()) => {
                let identity = accepted.identity.clone();
                drop(accepted);
                self.finish_before_execution(identity, "execution_lease_expired")?;
                return Ok(None);
            }
        };
        let cancellation = accepted
            .admitted
            .execution()
            .cancellation()
            .source()
            .clone();
        let (authority_updates, authority_receiver) = tokio::sync::watch::channel(authority);
        self.slot = Some(LocalSlot::Running(Box::new(RunningAssignment {
            identity: accepted.identity.clone(),
            cancellation,
            cancellation_grace,
            current_grant: start.lease,
            authority_updates,
        })));
        Ok(Some(ExecutionJob::new(
            *accepted,
            self.outbox.clone(),
            self.artifact_delivery.clone(),
            self.event_sender.clone(),
            Arc::clone(&self.sleeper),
            authority_receiver,
        )))
    }

    pub(super) fn handle_renewal(
        &mut self,
        renewal: AssignmentRenewal,
    ) -> Result<(), AssignmentManagerFailure> {
        self.drain_events();
        if self.decisions.iter().any(|decision| {
            decision.offer.effect_id == renewal.effect_id
                || decision
                    .start
                    .as_ref()
                    .is_some_and(|start| start.effect_id == renewal.effect_id)
        }) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if let Some(known) = self
            .decisions
            .iter()
            .find_map(|decision| decision.renewals.get(&renewal.effect_id))
        {
            return if known == &renewal {
                Ok(())
            } else {
                Err(AssignmentManagerFailure::ConflictingOffer)
            };
        }
        let Some(index) = self
            .decisions
            .iter()
            .position(|decision| decision.offer.assignment_id == renewal.assignment_id)
        else {
            return Ok(());
        };
        let decision = &self.decisions[index];
        if decision.offer.run_id != renewal.run_id
            || decision.offer.attempt_id != renewal.attempt_id
        {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        let Some(LocalSlot::Running(running)) = &self.slot else {
            return Ok(());
        };
        if running.identity.assignment_id != renewal.assignment_id {
            return Ok(());
        }
        let now = self.sleeper.now();
        let monotonic_stopped = {
            let authority = running.authority_updates.borrow();
            authority.revoked || now >= authority.stop_deadline
        };
        let current_stopped = monotonic_stopped
            || parse_utc(&running.current_grant.runner_stop_before)
                .ok()
                .zip(self.wall_clock.now_utc().ok())
                .is_none_or(|(stop_before, now)| now >= stop_before);
        if current_stopped {
            let Some(LocalSlot::Running(running)) = &mut self.slot else {
                return Ok(());
            };
            revoke_authority(running, now);
        }

        let Some(LocalSlot::Running(running)) = &self.slot else {
            return Ok(());
        };
        if renewal.lease.sequence <= running.current_grant.sequence {
            return if renewal.lease.sequence == running.current_grant.sequence
                && decision
                    .renewals
                    .values()
                    .any(|known| known.lease.sequence == running.current_grant.sequence)
            {
                Err(AssignmentManagerFailure::ConflictingOffer)
            } else {
                Ok(())
            };
        }
        let expected_sequence = running
            .current_grant
            .sequence
            .checked_add(1)
            .ok_or(AssignmentManagerFailure::ConflictingOffer)?;
        if renewal.lease.sequence != expected_sequence
            || (running.cancellation.is_cancelled() && !current_stopped)
        {
            return if running.cancellation.is_cancelled() {
                Ok(())
            } else {
                Err(AssignmentManagerFailure::ConflictingOffer)
            };
        }
        let cancellation_grace = running.cancellation_grace;
        let current_grant = running.current_grant.clone();
        let authority = self
            .validate_grant(&renewal.lease, expected_sequence, cancellation_grace)
            .and_then(|authority| {
                grant_extends(&current_grant, &renewal.lease)
                    .then_some(authority)
                    .ok_or(())
            })
            .map_err(|()| AssignmentManagerFailure::ConflictingOffer)?;

        let Some(LocalSlot::Running(running)) = &mut self.slot else {
            return Ok(());
        };
        if !current_stopped {
            running.current_grant = renewal.lease.clone();
            running.authority_updates.send_replace(authority);
        }
        self.decisions[index]
            .renewals
            .insert(renewal.effect_id.clone(), renewal);
        Ok(())
    }

    pub(super) fn handle_release(
        &mut self,
        assignment_id: &str,
        run_id: &str,
        attempt_id: &str,
        reason: &str,
    ) -> Result<(), AssignmentManagerFailure> {
        self.drain_events();
        let retained_conflict = self.decisions.iter().any(|decision| {
            decision.offer.assignment_id == assignment_id
                && (decision.offer.run_id != run_id || decision.offer.attempt_id != attempt_id)
        });
        if retained_conflict {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if let Some(LocalSlot::Preparing(preparing)) = &self.slot
            && preparing.offer.assignment_id == assignment_id
        {
            if preparing.offer.run_id != run_id || preparing.offer.attempt_id != attempt_id {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            }
            let Some(LocalSlot::Preparing(preparing)) = self.slot.take() else {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            };
            preparing.cancellation.cancel();
            let offer = preparing.offer;
            let response = rejected(
                &offer,
                AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable),
            );
            self.retain_decision(offer, response)?;
            self.retire_assignment_observations(assignment_id);
            return Ok(());
        }
        let now = self.sleeper.now();
        match &mut self.slot {
            Some(LocalSlot::Accepted(accepted))
                if accepted.identity.assignment_id == assignment_id =>
            {
                self.slot = None;
                self.retire_assignment_observations(assignment_id);
            }
            Some(LocalSlot::Running(running))
                if running.identity.assignment_id == assignment_id
                    && reason == "execution_lease_expired" =>
            {
                revoke_authority(running, now);
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn pending_observations(
        &mut self,
        in_flight: &BTreeSet<u64>,
        limit: usize,
    ) -> Vec<PendingAssignmentObservation> {
        self.drain_events();
        self.outbox.pending(in_flight, limit)
    }

    pub(super) fn acknowledge_observation(&mut self, id: u64) {
        self.drain_events();
        let Some(observation) = self.outbox.acknowledge(id) else {
            return;
        };
        if let AssignmentObservation::Decision(decision) = &observation
            && let Some(retained) = self
                .decisions
                .iter_mut()
                .find(|retained| retained.offer.assignment_id == decision.assignment_id())
            && retained.response_observation_id == Some(id)
        {
            retained.response_observation_id = None;
        }
        if observation.is_terminal() {
            if matches!(
                &self.slot,
                Some(LocalSlot::Finishing(finishing)) if finishing.final_observation_id == id
            ) {
                self.slot = None;
                self.reporting = None;
            } else if self
                .reporting
                .as_ref()
                .is_some_and(|reporting| reporting.assignment_id == observation.assignment_id())
            {
                self.reporting = None;
            }
        }
        self.outbox.wake();
    }

    pub(super) fn mark_observation_encoded(&self, id: u64) {
        self.outbox.mark_encoded(id);
    }

    pub(super) fn handle_artifact_response(
        &mut self,
        observation_id: u64,
        delivery_id: u64,
        response: ArtifactCloudResponse,
    ) -> Result<(), ArtifactDeliveryProtocolFailure> {
        self.outbox.acknowledge(observation_id);
        self.artifact_delivery
            .handle_response(delivery_id, response)
    }

    #[cfg(test)]
    pub(super) fn artifact_delivery(&self) -> ArtifactDeliveryBroker {
        self.artifact_delivery.clone()
    }

    pub(super) fn finish_transport(&mut self) {
        let removed = self.outbox.finish_transport();
        for decision in &mut self.decisions {
            if decision
                .response_observation_id
                .is_some_and(|id| removed.contains(&id))
            {
                decision.response_observation_id = None;
            }
        }
    }

    pub(super) fn begin_shutdown(&mut self) -> Result<(), AssignmentManagerFailure> {
        self.drain_events();
        self.shutting_down = true;
        let Some(slot) = self.slot.take() else {
            self.outbox.wake();
            return Ok(());
        };
        match slot {
            LocalSlot::Preparing(preparing) => {
                preparing.cancellation.cancel();
                self.slot = Some(LocalSlot::Preparing(preparing));
            }
            LocalSlot::Accepted(accepted) => {
                let identity = accepted.identity.clone();
                drop(accepted);
                self.finish_before_execution(identity, "graceful_shutdown")?;
            }
            LocalSlot::Running(running) => {
                running.cancellation.request_cancellation(
                    crate::execution::workflow::admission::CancellationReason::RunnerShutdown,
                );
                self.slot = Some(LocalSlot::Running(running));
            }
            LocalSlot::Finishing(finishing) => {
                self.slot = Some(LocalSlot::Finishing(finishing));
            }
        }
        self.outbox.wake();
        Ok(())
    }

    fn finish_before_execution(
        &mut self,
        identity: AssignmentIdentity,
        reason: &str,
    ) -> Result<(), AssignmentManagerFailure> {
        let final_observation_id = self
            .outbox
            .enqueue(AssignmentObservation::Execution {
                assignment_id: identity.assignment_id.clone(),
                attempt_id: identity.attempt_id.clone(),
                report: ExecutionReport::AssignmentInterrupted {
                    reason: reason.to_owned(),
                },
            })
            .map_err(|_| AssignmentManagerFailure::DecisionCapacity)?;
        self.slot = Some(LocalSlot::Finishing(FinishingAssignment {
            identity: identity.clone(),
            final_observation_id,
            _retained_root: None,
        }));
        self.start_final_grace(
            identity.assignment_id,
            final_observation_id,
            FINAL_ACKNOWLEDGEMENT_GRACE,
            true,
        );
        Ok(())
    }

    pub(super) fn shutdown_complete(&mut self) -> bool {
        self.drain_events();
        self.slot.is_none() && self.reporting.is_none()
    }

    pub(super) fn status_counts(&mut self) -> AssignmentCounts {
        self.drain_events();
        let mut counts = AssignmentCounts::default();
        match &self.slot {
            Some(LocalSlot::Preparing(_)) => counts.preparing = 1,
            Some(LocalSlot::Accepted(_)) => counts.accepted = 1,
            Some(LocalSlot::Running(_)) => counts.running = 1,
            Some(LocalSlot::Finishing(_)) => counts.finishing = 1,
            None if self.reporting.is_some() => counts.reporting = 1,
            None => {}
        }
        counts
    }

    pub(super) fn notification(&self) -> Arc<Notify> {
        self.outbox.notification()
    }

    fn drain_events(&mut self) {
        self.artifact_delivery.drain_uploads();
        while let Ok(event) = self.events.try_recv() {
            match event {
                ManagerEvent::Prepared { offer, admission } => {
                    let Some(LocalSlot::Preparing(preparing)) = self.slot.take() else {
                        continue;
                    };
                    if !same_assignment(&preparing.offer, &offer) {
                        self.slot = Some(LocalSlot::Preparing(preparing));
                        continue;
                    }
                    if preparing.cancellation.is_cancelled() {
                        continue;
                    }
                    let response = match *admission {
                        Ok(accepted) => {
                            self.slot = Some(LocalSlot::Accepted(Box::new(accepted)));
                            AssignmentDecision::Accepted {
                                effect_id: offer.effect_id.clone(),
                                assignment_id: offer.assignment_id.clone(),
                                offered_execution_spec_id: offer
                                    .execution_spec
                                    .execution_spec_id
                                    .clone(),
                            }
                        }
                        Err(decline) => rejected(&offer, decline),
                    };
                    let accepted = matches!(response, AssignmentDecision::Accepted { .. });
                    if self.retain_decision(*offer, response).is_err() || !accepted {
                        self.slot = None;
                    }
                }
                ManagerEvent::Finished {
                    assignment_id,
                    final_observation_id,
                    final_delivery_deadline,
                    retained_root,
                } => {
                    let Some(LocalSlot::Running(running)) = self.slot.take() else {
                        continue;
                    };
                    if running.identity.assignment_id != assignment_id {
                        self.slot = Some(LocalSlot::Running(running));
                        continue;
                    }
                    let identity = running.identity;
                    let Some(final_observation_id) = final_observation_id else {
                        self.retire_assignment_observations(&assignment_id);
                        self.outbox.wake();
                        continue;
                    };
                    let Some(final_delivery_deadline) = final_delivery_deadline else {
                        self.retire_assignment_observations(&assignment_id);
                        self.outbox.wake();
                        continue;
                    };
                    let remaining =
                        final_delivery_deadline.saturating_duration_since(self.sleeper.now());
                    if remaining.is_zero() {
                        self.retire_assignment_observations(&assignment_id);
                        self.outbox.wake();
                        continue;
                    }
                    self.slot = Some(LocalSlot::Finishing(FinishingAssignment {
                        identity: identity.clone(),
                        final_observation_id,
                        _retained_root: retained_root,
                    }));
                    self.start_final_grace(
                        identity.assignment_id,
                        final_observation_id,
                        remaining,
                        false,
                    );
                }
                ManagerEvent::FinalGraceElapsed {
                    assignment_id,
                    final_observation_id,
                    continue_reporting,
                } => {
                    let Some(LocalSlot::Finishing(finishing)) = self.slot.take() else {
                        continue;
                    };
                    if finishing.identity.assignment_id == assignment_id
                        && finishing.final_observation_id == final_observation_id
                    {
                        if continue_reporting {
                            self.reporting = Some(finishing.identity);
                        } else {
                            self.retire_assignment_observations(&assignment_id);
                        }
                    } else {
                        self.slot = Some(LocalSlot::Finishing(finishing));
                    }
                }
            }
        }
    }

    fn start_final_grace(
        &self,
        assignment_id: String,
        final_observation_id: u64,
        duration: Duration,
        continue_reporting: bool,
    ) {
        let sleeper = Arc::clone(&self.sleeper);
        let sender = self.event_sender.clone();
        let outbox = self.outbox.clone();
        tokio::spawn(async move {
            sleeper.sleep(duration).await;
            let _ = sender.send(ManagerEvent::FinalGraceElapsed {
                assignment_id,
                final_observation_id,
                continue_reporting,
            });
            outbox.wake();
        });
    }

    fn apply_successor_fences(&mut self, successor_assignment_id: &str) {
        let predecessor = match &self.slot {
            Some(LocalSlot::Finishing(finishing))
                if finishing.identity.assignment_id != successor_assignment_id =>
            {
                Some(finishing.identity.clone())
            }
            _ => self
                .reporting
                .as_ref()
                .filter(|identity| identity.assignment_id != successor_assignment_id)
                .cloned(),
        };
        if let Some(predecessor) = predecessor {
            self.retire_assignment_observations(&predecessor.assignment_id);
            self.slot = None;
            self.reporting = None;
        }
        let rejected_assignments: Vec<_> = self
            .decisions
            .iter()
            .filter(|decision| {
                decision.offer.assignment_id != successor_assignment_id
                    && matches!(decision.response, AssignmentDecision::Rejected { .. })
            })
            .map(|decision| decision.offer.assignment_id.clone())
            .collect();
        for assignment_id in rejected_assignments {
            self.retire_assignment_observations(&assignment_id);
        }
    }

    fn retire_assignment_observations(&mut self, assignment_id: &str) {
        self.outbox.fence_assignment(assignment_id);
        self.artifact_delivery.cancel_assignment(assignment_id);
        for decision in &mut self.decisions {
            if decision.offer.assignment_id == assignment_id
                && decision
                    .response_observation_id
                    .is_some_and(|id| !self.outbox.contains(id))
            {
                decision.response_observation_id = None;
            }
        }
    }

    fn make_decision_room(&mut self) -> Result<(), AssignmentManagerFailure> {
        if self.decisions.len() < MAXIMUM_RETAINED_DECISIONS {
            return Ok(());
        }
        let active_assignment = match &self.slot {
            Some(LocalSlot::Accepted(accepted)) => Some(accepted.identity.assignment_id.as_str()),
            Some(LocalSlot::Running(running)) => Some(running.identity.assignment_id.as_str()),
            Some(LocalSlot::Finishing(finishing)) => {
                Some(finishing.identity.assignment_id.as_str())
            }
            Some(LocalSlot::Preparing(_)) | None => None,
        };
        let Some(index) = self.decisions.iter().position(|decision| {
            decision.response_observation_id.is_none()
                && active_assignment != Some(decision.offer.assignment_id.as_str())
        }) else {
            return Err(AssignmentManagerFailure::DecisionCapacity);
        };
        self.decisions.remove(index);
        Ok(())
    }

    fn retain_decision(
        &mut self,
        offer: AssignmentOffer,
        response: AssignmentDecision,
    ) -> Result<(), AssignmentManagerFailure> {
        let response_observation_id = self
            .outbox
            .enqueue(AssignmentObservation::Decision(response.clone()))
            .map_err(|_| AssignmentManagerFailure::DecisionCapacity)?;
        self.decisions.push_back(RetainedDecision {
            offer,
            response,
            response_observation_id: Some(response_observation_id),
            start: None,
            renewals: BTreeMap::new(),
        });
        Ok(())
    }

    fn validate_admission_prerequisites(
        &self,
        offer: &AssignmentOffer,
    ) -> Result<(), AssignmentDecline> {
        validate_execution_spec(&offer.execution_spec)?;
        self.validate_wall_clock()
    }

    fn admission_runtime(&self) -> AdmissionRuntime {
        AdmissionRuntime {
            pi_installation: self.pi_installation.clone(),
            claude_code_installation: self.claude_code_installation.clone(),
            environment: self.environment.clone(),
            outbox: self.outbox.clone(),
            guard_processes: self.guard_processes,
        }
    }

    fn validate_wall_clock(&self) -> Result<(), AssignmentDecline> {
        let policy = self
            .lease_policy
            .as_ref()
            .ok_or_else(environment_unavailable)?;
        let uncertainty = self.wall_clock.uncertainty().map_err(|_| {
            AssignmentDecline::RunnerUnable(RunnerUnableReason::ExecutionEnvironmentUnavailable)
        })?;
        let ceiling = u64::try_from(policy.max_clock_uncertainty_milliseconds)
            .map_err(|_| environment_unavailable())?;
        if uncertainty > Duration::from_millis(ceiling) {
            return Err(environment_unavailable());
        }
        Ok(())
    }

    fn validate_grant(
        &self,
        grant: &ExecutionLeaseGrant,
        expected_sequence: u64,
        cancellation_grace: Duration,
    ) -> Result<LeaseAuthority, ()> {
        if grant.sequence != expected_sequence {
            return Err(());
        }
        let expires_at = parse_utc(&grant.expires_at)?;
        let stop_before = parse_utc(&grant.runner_stop_before)?;
        let policy = self.lease_policy.as_ref().ok_or(())?;
        let fencing_margin = Duration::from_millis(policy.fencing_margin_milliseconds);
        let grant_margin: Duration = (expires_at - stop_before).try_into().map_err(|_| ())?;
        if grant_margin != fencing_margin {
            return Err(());
        }
        let now_utc = self.wall_clock.now_utc().map_err(|_| ())?;
        if now_utc + cancellation_grace > stop_before {
            return Err(());
        }
        let received_at = self.sleeper.now();
        let stop_after: Duration = (stop_before - now_utc).try_into().map_err(|_| ())?;
        let expires_after: Duration = (expires_at - now_utc).try_into().map_err(|_| ())?;
        let cancellation_after = stop_after.checked_sub(cancellation_grace).ok_or(())?;
        let renewal_delivery = policy_duration(policy.renewal_delivery_budget_milliseconds)?;
        let force_stop_and_reap_budget =
            policy_duration(policy.force_stop_and_reap_budget_milliseconds)?;
        let terminal_report_delivery_budget =
            policy_duration(policy.terminal_report_delivery_budget_milliseconds)?;
        let renewal_after = cancellation_after.saturating_sub(renewal_delivery);
        let renewal_deadline = received_at.checked_add(renewal_after).ok_or(())?;
        let cancellation_deadline = received_at.checked_add(cancellation_after).ok_or(())?;
        let stop_deadline = received_at.checked_add(stop_after).ok_or(())?;
        let force_stop_deadline = stop_deadline
            .checked_add(force_stop_and_reap_budget)
            .ok_or(())?;
        let expires_deadline = received_at.checked_add(expires_after).ok_or(())?;
        Ok(LeaseAuthority {
            sequence: grant.sequence,
            renewal_deadline,
            cancellation_deadline,
            stop_deadline,
            force_stop_deadline,
            expires_deadline,
            terminal_report_delivery_budget,
            revoked: false,
        })
    }

    fn prepare_execution_root(
        &self,
        assignment_id: &str,
    ) -> Result<AssignmentRoot, AssignmentDecline> {
        let boot_root = self.config.work_root().join(&self.boot_id);
        fs::create_dir_all(&boot_root).map_err(|_| environment_unavailable())?;
        let canonical_boot_root =
            fs::canonicalize(&boot_root).map_err(|_| environment_unavailable())?;
        if canonical_boot_root.parent() != Some(self.config.work_root()) {
            return Err(environment_unavailable());
        }
        let temporary = tempfile::Builder::new()
            .prefix(&format!("{assignment_id}-"))
            .tempdir_in(&canonical_boot_root)
            .map_err(|_| environment_unavailable())?;
        let execution = temporary.path().join("execution");
        let private = temporary.path().join("private");
        let source = temporary.path().join("source");
        fs::create_dir(&execution).map_err(|_| environment_unavailable())?;
        fs::create_dir(&private).map_err(|_| environment_unavailable())?;
        Ok(AssignmentRoot {
            _temporary: temporary,
            execution,
            private,
            source,
        })
    }

    #[cfg(test)]
    pub(super) fn enqueue_fixture_transitions(&self, count: u64) {
        for sequence in 1..=count {
            self.outbox
                .enqueue(AssignmentObservation::Execution {
                    assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    report: ExecutionReport::Transition {
                        execution_event_sequence: sequence,
                        workflow_event: serde_json::json!({
                            "eventVersion": 1,
                            "eventType": "step_state_changed",
                            "transitionSequence": sequence,
                            "stepId": "fixture",
                            "role": "step",
                            "failurePolicy": "required",
                            "from": "pending",
                            "to": "starting",
                        }),
                    },
                })
                .unwrap();
        }
    }

    #[cfg(test)]
    pub(super) fn enqueue_fixture_finalization_terminal(&self) {
        self.outbox
            .enqueue(AssignmentObservation::Execution {
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                report: ExecutionReport::Finished {
                    final_execution_event_sequence: 1,
                    outcome: serde_json::json!({
                        "outcome": "succeeded",
                        "finalization": {
                            "trigger": "succeeded",
                            "finalizers": [{
                                "id": "cleanup",
                                "role": "finalizer",
                                "failurePolicy": "required",
                                "state": "succeeded",
                            }],
                            "issues": [],
                            "forceAbort": false,
                        },
                    }),
                    artifact_delivery: serde_json::json!({
                        "outcome": "prepared",
                        "artifactSetId": "ats_01k0z6r1w8f4jy2m7q9v3x5abc",
                    }),
                },
            })
            .unwrap();
    }

    #[cfg(test)]
    fn active_step_count(&self) -> Option<usize> {
        match &self.slot {
            Some(LocalSlot::Accepted(accepted)) => {
                Some(accepted.admitted.workflow().definition.steps.len())
            }
            _ => None,
        }
    }
}

fn build_execution_context(
    execution_spec: &ExecutionSpecV1RunnerProjection,
    root: &Path,
    git_capture: Option<crate::execution::workflow::git_capture::CloudGitCaptureProjection>,
    environment: &EnvironmentSnapshot,
    pi_installation: Option<&crate::execution::pi::ValidatedPiInstallation>,
    claude_code_installation: Option<
        &crate::execution::claude_code::ValidatedClaudeCodeInstallation,
    >,
) -> Result<ExecutionContext, AssignmentDecline> {
    let maximum_parallel_steps =
        usize::try_from(execution_spec.execution_limits.maximum_parallel_steps)
            .map_err(|_| invalid_execution_limits())?;
    let cancellation_grace =
        Duration::from_secs(execution_spec.execution_limits.cancellation_grace_seconds);
    let lifecycle = if git_capture.is_some() {
        ExecutionRootLifecycle::CallerOwnedRetained
    } else {
        ExecutionRootLifecycle::EngineOwnedEphemeral
    };
    let context = ExecutionContext::new(
        root.to_owned(),
        lifecycle,
        default_execution_policy_limits(maximum_parallel_steps),
        environment.without_managed_runner_credentials_and_helpers(),
        CancellationPolicy::new(CancellationSource::new(), cancellation_grace),
    );
    let context = match git_capture {
        Some(projection) => context.with_cloud_git_capture(projection),
        None => context,
    };
    let context = match pi_installation {
        Some(installation) => context.with_pi_installation(installation.clone()),
        None => context,
    };
    Ok(match claude_code_installation {
        Some(installation) => context.with_claude_code_installation(installation.clone()),
        None => context,
    })
}

fn revoke_authority(running: &mut RunningAssignment, now: Instant) {
    running.authority_updates.send_modify(|authority| {
        authority.revoked = true;
        authority.renewal_deadline = now;
        authority.cancellation_deadline = now;
        authority.stop_deadline = now;
        authority.force_stop_deadline = now;
        authority.expires_deadline = now;
    });
    running.cancellation.request_cancellation(
        crate::execution::workflow::admission::CancellationReason::ExecutionLeaseExpired,
    );
}

fn grant_extends(current: &ExecutionLeaseGrant, next: &ExecutionLeaseGrant) -> bool {
    let (Ok(current_expires), Ok(next_expires)) =
        (parse_utc(&current.expires_at), parse_utc(&next.expires_at))
    else {
        return false;
    };
    let (Ok(current_stop), Ok(next_stop)) = (
        parse_utc(&current.runner_stop_before),
        parse_utc(&next.runner_stop_before),
    ) else {
        return false;
    };
    next_expires > current_expires && next_stop > current_stop
}

fn parse_utc(value: &str) -> Result<OffsetDateTime, ()> {
    let parsed = OffsetDateTime::parse(value, &Rfc3339).map_err(|_| ())?;
    if parsed.offset() != UtcOffset::UTC || !value.ends_with('Z') {
        return Err(());
    }
    Ok(parsed)
}

fn validate_lease_policy(policy: &ExecutionLeasePolicy) -> Result<(), WelcomePolicyFailure> {
    if policy.schema_version != 1 {
        return Err(WelcomePolicyFailure::Invalid);
    }
    let max_uncertainty = nonnegative(policy.max_clock_uncertainty_milliseconds)?;
    let force_stop = nonnegative(policy.force_stop_and_reap_budget_milliseconds)?;
    let terminal_report = nonnegative(policy.terminal_report_delivery_budget_milliseconds)?;
    let start_delivery = nonnegative(policy.start_delivery_budget_milliseconds)?;
    let renewal_delivery = nonnegative(policy.renewal_delivery_budget_milliseconds)?;
    if policy.lease_duration_milliseconds == 0 || policy.fencing_margin_milliseconds == 0 {
        return Err(WelcomePolicyFailure::Invalid);
    }
    let fencing_required = max_uncertainty
        .checked_add(force_stop)
        .and_then(|value| value.checked_add(terminal_report))
        .ok_or(WelcomePolicyFailure::Invalid)?;
    if policy.fencing_margin_milliseconds < fencing_required {
        return Err(WelcomePolicyFailure::Invalid);
    }
    let maximum_cancellation_grace_milliseconds =
        u64::try_from(MAXIMUM_CANCELLATION_GRACE.as_millis())
            .map_err(|_| WelcomePolicyFailure::Invalid)?;
    for delivery_budget in [start_delivery, renewal_delivery] {
        let lease_required = policy
            .fencing_margin_milliseconds
            .checked_add(maximum_cancellation_grace_milliseconds)
            .and_then(|value| value.checked_add(delivery_budget))
            .ok_or(WelcomePolicyFailure::Invalid)?;
        if policy.lease_duration_milliseconds < lease_required {
            return Err(WelcomePolicyFailure::Invalid);
        }
    }
    Ok(())
}

fn nonnegative(value: i64) -> Result<u64, WelcomePolicyFailure> {
    u64::try_from(value).map_err(|_| WelcomePolicyFailure::Invalid)
}

fn policy_duration(value: i64) -> Result<Duration, ()> {
    u64::try_from(value)
        .map(Duration::from_millis)
        .map_err(|_| ())
}

fn serve_contract_decline(_failure: ServeWorkflowContractFailure) -> AssignmentDecline {
    AssignmentDecline::ExecutionSpecInvalid(ExecutionSpecInvalidReason::WorkflowContractInvalid)
}

fn validate_execution_spec(
    execution_spec: &ExecutionSpecV1RunnerProjection,
) -> Result<(), AssignmentDecline> {
    if execution_spec.schema_version != 1 {
        return Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::UnsupportedSchemaVersion,
        ));
    }
    let maximum_parallel_steps =
        usize::try_from(execution_spec.execution_limits.maximum_parallel_steps)
            .map_err(|_| invalid_execution_limits())?;
    let cancellation_grace =
        Duration::from_secs(execution_spec.execution_limits.cancellation_grace_seconds);
    if !(1..=MAXIMUM_PARALLEL_STEPS).contains(&maximum_parallel_steps)
        || !(MINIMUM_CANCELLATION_GRACE..=MAXIMUM_CANCELLATION_GRACE).contains(&cancellation_grace)
    {
        return Err(invalid_execution_limits());
    }
    let source = &execution_spec.source;
    if source.object_format != "sha1" {
        return Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::UnsupportedSourceObjectFormat,
        ));
    }
    let valid_connection = source
        .repository_connection_id
        .parse::<crate::runner_protocol::generated::RepositoryConnectionId>()
        .is_ok();
    let valid_path = !source.workflow_path.is_empty()
        && source.workflow_path.chars().count() <= 4096
        && !source.workflow_path.starts_with('/')
        && !source.workflow_path.contains('\0')
        && source
            .workflow_path
            .split('/')
            .all(|component| !matches!(component, "" | "." | ".."));
    if !valid_connection
        || source.checkout_credential_reference != source.repository_connection_id
        || !lowercase_hex(&source.commit_oid, 40)
        || !valid_path
        || source.workflow_source_closure_digest.algorithm != "sha256"
        || !lowercase_hex(&source.workflow_source_closure_digest.value, 64)
    {
        return Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::InvalidSourceProjection,
        ));
    }
    Ok(())
}

fn lowercase_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

fn materialization_decline(failure: MaterializationFailure) -> AssignmentDecline {
    let reason = match failure {
        MaterializationFailure::UnsupportedObjectFormat => {
            return AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::UnsupportedSourceObjectFormat,
            );
        }
        MaterializationFailure::CommitUnavailable | MaterializationFailure::CommitMismatch => {
            ExecutionSpecInvalidReason::SourceCommitMismatch
        }
        MaterializationFailure::DirtyCheckout => ExecutionSpecInvalidReason::SourceCheckoutDirty,
        MaterializationFailure::WorkflowUnavailable => {
            ExecutionSpecInvalidReason::WorkflowSourceInvalid
        }
        MaterializationFailure::WorkflowDigestMismatch => {
            ExecutionSpecInvalidReason::WorkflowSourceDigestMismatch
        }
        MaterializationFailure::ProviderUnavailable | MaterializationFailure::AssignmentFenced => {
            return AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable);
        }
        MaterializationFailure::EnvironmentUnavailable => return environment_unavailable(),
    };
    AssignmentDecline::ExecutionSpecInvalid(reason)
}

fn invalid_execution_limits() -> AssignmentDecline {
    AssignmentDecline::ExecutionSpecInvalid(ExecutionSpecInvalidReason::InvalidExecutionLimits)
}

fn admission_decline(failure: AdmissionFailure, cloud_git_capture: bool) -> AssignmentDecline {
    let kind = failure.kind();
    if cloud_git_capture {
        match kind {
            AdmissionFailureKind::GitObjectFormatUnsupported => {
                return AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::UnsupportedSourceObjectFormat,
                );
            }
            AdmissionFailureKind::GitBaselineUnavailable => {
                return AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::SourceCommitMismatch,
                );
            }
            AdmissionFailureKind::GitInitialWorkspaceDirty => {
                return AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::SourceCheckoutDirty,
                );
            }
            AdmissionFailureKind::GitWorkflowDigestMismatch => {
                return AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::WorkflowSourceDigestMismatch,
                );
            }
            AdmissionFailureKind::GitContextUnavailable
            | AdmissionFailureKind::GitContextNotRepository
            | AdmissionFailureKind::GitContextExecutionRootMismatch => {
                return environment_unavailable();
            }
            _ => {}
        }
    }
    if kind.is_execution_root_failure() {
        environment_unavailable()
    } else if kind.is_projected_execution_limit_failure() {
        invalid_execution_limits()
    } else if kind == AdmissionFailureKind::AgentStepRuntimeUnsupported {
        AssignmentDecline::RunnerUnable(RunnerUnableReason::WorkflowEnvironmentUnsupported)
    } else {
        AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::WorkflowAdmissionInvalid,
        )
    }
}

fn environment_unavailable() -> AssignmentDecline {
    AssignmentDecline::RunnerUnable(RunnerUnableReason::ExecutionEnvironmentUnavailable)
}

fn rejected(offer: &AssignmentOffer, decline: AssignmentDecline) -> AssignmentDecision {
    AssignmentDecision::Rejected {
        effect_id: offer.effect_id.clone(),
        assignment_id: offer.assignment_id.clone(),
        decline,
    }
}

fn same_assignment(left: &AssignmentOffer, right: &AssignmentOffer) -> bool {
    left.assignment_id == right.assignment_id
        && left.run_id == right.run_id
        && left.attempt_id == right.attempt_id
        && left.execution_spec == right.execution_spec
}

fn start_matches_offer(start: &AssignmentStart, offer: &AssignmentOffer) -> bool {
    start.assignment_id == offer.assignment_id
        && start.run_id == offer.run_id
        && start.attempt_id == offer.attempt_id
        && start.execution_spec_id == offer.execution_spec.execution_spec_id
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};
    use std::io::{Read as _, Write as _};
    use std::net::TcpStream as StandardTcpStream;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::json;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::*;
    use crate::execution::claude_code::ValidatedClaudeCodeInstallation;
    use crate::execution::pi::ValidatedPiInstallation;
    use crate::runner::credential::test_credential;
    use crate::runner::service::config::Config;
    use crate::runner::service::source::{
        CredentialBrokerFailure, CredentialOperation, ProviderCredential,
    };
    use crate::runner::service::test_support::{
        SleepRelease, controlled_sleeper, sleep_request, with_watchdog,
    };
    use crate::runner_protocol::{
        ArtifactRegistrationOutcome, ArtifactRegistrationResponse,
        ArtifactResultRegistrationOutcome, ArtifactResultRegistrationResponse,
        ExecutionLimitsV1RunnerProjection, ExecutionSourceV1RunnerProjection,
        WorkflowSourceClosureDigestV1RunnerProjection,
    };

    const WORKFLOW_ID: &str = "wfl_01k0z6r1w8f4jy2m7q9v3x5abr";
    const NOW: &str = "2026-07-23T00:00:00Z";
    const COMMAND_FIXTURE_TEST_NAME: &str =
        "runner::service::assignment::tests::command_fixture_process";
    const FAILING_COMMAND_FIXTURE_TEST_NAME: &str =
        "runner::service::assignment::tests::failing_command_fixture_process";
    // SCHERZO_* variables are intentionally removed from admitted command environments.
    const COMMAND_FIXTURE_SOCKET: &str = "WORKFLOW_ASSIGNMENT_COMMAND_FIXTURE_SOCKET";
    const SUCCESSFUL_PI: &str = r#"#!/bin/sh
set -eu
printf '%s\0' "$*" >> "${0%/*}/pi.calls"
assistant='{"role":"assistant","content":[{"type":"text","text":"value"}],"api":"test-api","provider":"test-provider","model":"test-model","usage":{"input":1,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":2,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":2}'
printf '{"type":"session","version":3,"id":"00000000-0000-4000-8000-000000000099","timestamp":"2026-08-04T00:00:00Z","cwd":"%s"}\n' "$PWD"
printf '%s\n' '{"type":"agent_start"}' '{"type":"turn_start"}'
printf '{"type":"message_start","message":%s}\n' "$assistant"
printf '{"type":"message_end","message":%s}\n' "$assistant"
printf '{"type":"turn_end","message":%s,"toolResults":[]}\n' "$assistant"
printf '{"type":"agent_end","messages":[%s],"willRetry":false}\n' "$assistant"
printf '%s\n' '{"type":"agent_settled"}'
"#;
    const PI_ONLY_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: fixture/pi
        thinking: high
steps:
  pi:
    kind: agent
    agent:
      profile: coding
      systemPrompt: system.md
      message:
        text: [{ file: system.md }]
"#;
    const CLAUDE_CODE_ONLY_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: claude_code
      config:
        model: fixture/claude
        effort: xhigh
steps:
  claude:
    kind: agent
    agent:
      profile: coding
      systemPrompt: system.md
      message:
        text: [{ file: system.md }]
"#;
    const MIXED_HARNESS_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  piCoding:
    harness:
      kind: pi
      config:
        model: fixture/pi
        thinking: high
  claudeCoding:
    harness:
      kind: claude_code
      config:
        model: fixture/claude
        effort: xhigh
steps:
  pi:
    kind: agent
    agent:
      profile: piCoding
      systemPrompt: system.md
      message:
        text: [{ file: system.md }]
  claude:
    kind: agent
    dependsOn: [pi]
    agent:
      profile: claudeCoding
      systemPrompt: system.md
      message:
        text: [{ file: system.md }]
"#;
    const SUCCESSFUL_CLAUDE_CODE: &str = r#"#!/bin/sh
set -eu
printf '%s\0' "$*" >> "${0%/*}/claude.calls"
model=
session=
previous=
for argument in "$@"; do
  if [ "$previous" = --model ]; then model=$argument; fi
  if [ "$previous" = --session-id ]; then session=$argument; fi
  previous=$argument
done
while IFS= read -r _; do :; done
printf '{"type":"system","subtype":"init","cwd":"%s","session_id":"%s","model":"%s","permissionMode":"bypassPermissions","claude_code_version":"2.1.234"}\n' "$PWD" "$session" "$model"
if [ "${CLAUDE_FIXTURE_FAIL-}" = 1 ]; then exit 23; fi
printf '{"type":"stream_event","event":{"type":"message_start","message":{"id":"msg-runner","type":"message","role":"assistant","content":[],"model":"%s","usage":{"input_tokens":1,"output_tokens":0}}},"session_id":"%s","parent_tool_use_id":null}\n' "$model" "$session"
printf '{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"value"}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"assistant","message":{"id":"msg-runner","type":"message","role":"assistant","content":[{"type":"text","text":"value"}],"model":"%s"},"parent_tool_use_id":null,"session_id":"%s"}\n' "$model" "$session"
printf '{"type":"stream_event","event":{"type":"content_block_stop","index":0},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_stop"},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"result","subtype":"success","is_error":false,"terminal_reason":"completed","result":"value","session_id":"%s"}\n' "$session"
"#;

    struct BlockingSourceBroker {
        started: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
        stopped: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
        calls: AtomicUsize,
    }

    impl SourceCredentialBroker for BlockingSourceBroker {
        fn issue(
            &self,
            _assignment_id: &str,
            _repository_connection_id: &str,
            _operation: CredentialOperation,
            cancellation: &CaptureCancellation,
        ) -> Result<ProviderCredential, CredentialBrokerFailure> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if let Some(started) = self.started.lock().unwrap().take() {
                let _ = started.send(());
            }
            while !cancellation.is_cancelled() {
                crate::timing::sleep(Duration::from_millis(5));
            }
            if let Some(stopped) = self.stopped.lock().unwrap().take() {
                let _ = stopped.send(());
            }
            Err(CredentialBrokerFailure::Fenced)
        }
    }

    struct UnavailableSourceBroker;

    impl SourceCredentialBroker for UnavailableSourceBroker {
        fn issue(
            &self,
            _assignment_id: &str,
            _repository_connection_id: &str,
            _operation: CredentialOperation,
            _cancellation: &CaptureCancellation,
        ) -> Result<ProviderCredential, CredentialBrokerFailure> {
            Err(CredentialBrokerFailure::Unavailable)
        }
    }

    fn run_command_fixture() {
        let socket = std::env::var(COMMAND_FIXTURE_SOCKET).unwrap();
        let mut control = StandardTcpStream::connect(socket).unwrap();
        control.write_all(&[1]).unwrap();
        control.flush().unwrap();
        let mut release = [0_u8; 1];
        control.read_exact(&mut release).unwrap();
        assert_eq!(release, [1]);
    }

    // Keep commands alive until the test observes them. Bare `true` and `false`
    // commands can exit before the direct-child test path captures their process identity.
    #[test]
    #[ignore = "launched only as the assignment workflow command fixture"]
    fn command_fixture_process() {
        run_command_fixture();
    }

    #[test]
    #[ignore = "launched only as the failing assignment workflow command fixture"]
    fn failing_command_fixture_process() {
        run_command_fixture();
        panic!("intentional assignment command fixture failure");
    }

    struct FixedWallClockHealth {
        uncertainty: Mutex<Result<Duration, WallClockHealthFailure>>,
        now: OffsetDateTime,
    }

    impl WallClockHealth for FixedWallClockHealth {
        fn uncertainty(&self) -> Result<Duration, WallClockHealthFailure> {
            *self.uncertainty.lock().unwrap()
        }

        fn now_utc(&self) -> Result<OffsetDateTime, WallClockHealthFailure> {
            Ok(self.now)
        }
    }

    fn wall_clock(uncertainty: Duration) -> Arc<dyn WallClockHealth> {
        wall_clock_at(uncertainty, NOW)
    }

    fn wall_clock_at(uncertainty: Duration, now: &str) -> Arc<dyn WallClockHealth> {
        Arc::new(FixedWallClockHealth {
            uncertainty: Mutex::new(Ok(uncertainty)),
            now: OffsetDateTime::parse(now, &Rfc3339).unwrap(),
        })
    }

    fn policy() -> ExecutionLeasePolicy {
        ExecutionLeasePolicy {
            schema_version: 1,
            max_clock_uncertainty_milliseconds: 1000,
            force_stop_and_reap_budget_milliseconds: 5000,
            terminal_report_delivery_budget_milliseconds: 5000,
            start_delivery_budget_milliseconds: 5000,
            renewal_delivery_budget_milliseconds: 5000,
            lease_duration_milliseconds: 320_000,
            fencing_margin_milliseconds: 11_000,
        }
    }

    fn offer(suffix: &str) -> AssignmentOffer {
        AssignmentOffer {
            effect_id: format!("eff_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
            assignment_id: format!("asn_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
            run_id: format!("run_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
            project_id: "prj_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            attempt_id: format!("atm_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
            execution_spec: ExecutionSpecV1RunnerProjection {
                execution_spec_id: format!("xsp_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
                schema_version: 1,
                registered_workflow_id: WORKFLOW_ID.to_owned(),
                execution_limits: ExecutionLimitsV1RunnerProjection {
                    maximum_parallel_steps: 1,
                    cancellation_grace_seconds: 1,
                },
                source: production_source(),
            },
        }
    }

    fn production_source() -> ExecutionSourceV1RunnerProjection {
        let connection = "rpc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned();
        ExecutionSourceV1RunnerProjection {
            repository_connection_id: connection.clone(),
            object_format: "sha1".to_owned(),
            commit_oid: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            workflow_path: "workflows/build.yaml".to_owned(),
            workflow_source_closure_digest: WorkflowSourceClosureDigestV1RunnerProjection {
                algorithm: "sha256".to_owned(),
                value: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_owned(),
            },
            checkout_credential_reference: connection,
        }
    }

    fn assert_offer_declined(
        manager: &mut AssignmentManager,
        offered: AssignmentOffer,
        expected: AssignmentDecline,
    ) {
        manager.handle_offer(offered).unwrap();
        let mut pending = Vec::new();
        for _ in 0..100 {
            pending = manager.pending_observations(&BTreeSet::new(), 1);
            if !pending.is_empty() {
                break;
            }
            crate::timing::sleep(Duration::from_millis(10));
        }
        assert!(
            !pending.is_empty(),
            "assignment decline should become observable"
        );
        assert!(matches!(
            &pending[0].observation,
            AssignmentObservation::Decision(AssignmentDecision::Rejected { decline, .. })
                if *decline == expected
        ));
        assert!(manager.slot.is_none());
    }

    fn manager_fixture(workflow: &str) -> (tempfile::TempDir, AssignmentManager) {
        manager_fixture_with_harnesses(workflow, None, None)
    }

    fn manager_fixture_with_pi(
        workflow: &str,
        pi_source: Option<&str>,
    ) -> (tempfile::TempDir, AssignmentManager) {
        manager_fixture_with_harnesses(workflow, pi_source, None)
    }

    fn manager_fixture_with_harnesses(
        workflow: &str,
        pi_source: Option<&str>,
        claude_code_source: Option<&str>,
    ) -> (tempfile::TempDir, AssignmentManager) {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        let work = temporary.path().join("work");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&work).unwrap();
        fs::write(source.join("workflow.yaml"), workflow).unwrap();
        fs::write(source.join("system.md"), "System.\n").unwrap();
        let assignment = AssignmentConfig::new(&work).unwrap();
        let mut config = Config::new(
            "wss://gateway.example.test/v1/runner/connect",
            test_credential(),
            false,
            assignment,
        )
        .unwrap();
        if let Some(pi_source) = pi_source {
            let executable = install_fixture_executable(&temporary, "pi-fixture", pi_source);
            config = config.with_pi_installation(ValidatedPiInstallation::fixture(executable));
        }
        if let Some(claude_code_source) = claude_code_source {
            let executable =
                install_fixture_executable(&temporary, "claude-fixture", claude_code_source);
            config = config.with_claude_code_installation(
                ValidatedClaudeCodeInstallation::fixture(executable),
            );
        }
        let sleeper: Arc<dyn Sleeper> = Arc::new(crate::runner::service::TokioSleeper);
        let mut manager = AssignmentManager::new_for_test_with_sleeper(
            &config,
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
            wall_clock(Duration::ZERO),
            sleeper,
        );
        manager.fixture_materialized_source = Some((source, PathBuf::from("workflow.yaml")));
        manager.retain_lease_policy(&policy()).unwrap();
        let mut environment = manager.environment.variables().clone();
        environment.insert(
            OsString::from("CLAUDE_CONFIG_DIR"),
            temporary.path().join("claude-config").into_os_string(),
        );
        manager.environment = EnvironmentSnapshot::new(environment);
        (temporary, manager)
    }

    fn install_fixture_executable(
        temporary: &tempfile::TempDir,
        name: &str,
        source: &str,
    ) -> PathBuf {
        let executable = temporary.path().join(name);
        fs::write(&executable, source).unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        executable
    }

    fn only_harness_call(path: &Path) -> String {
        let calls = fs::read(path).unwrap();
        assert_eq!(calls.iter().filter(|byte| **byte == 0).count(), 1);
        assert_eq!(calls.last(), Some(&0));
        String::from_utf8(calls[..calls.len() - 1].to_vec()).unwrap()
    }

    fn start_for(offered: &AssignmentOffer) -> AssignmentStart {
        AssignmentStart {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abh".to_owned(),
            assignment_id: offered.assignment_id.clone(),
            run_id: offered.run_id.clone(),
            attempt_id: offered.attempt_id.clone(),
            execution_spec_id: offered.execution_spec.execution_spec_id.clone(),
            lease: ExecutionLeaseGrant {
                sequence: 1,
                expires_at: "2026-07-23T00:00:30Z".to_owned(),
                runner_stop_before: "2026-07-23T00:00:19Z".to_owned(),
            },
        }
    }

    fn renewal_for(offered: &AssignmentOffer) -> AssignmentRenewal {
        AssignmentRenewal {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abj".to_owned(),
            assignment_id: offered.assignment_id.clone(),
            run_id: offered.run_id.clone(),
            attempt_id: offered.attempt_id.clone(),
            lease: ExecutionLeaseGrant {
                sequence: 2,
                expires_at: "2026-07-23T00:01:00Z".to_owned(),
                runner_stop_before: "2026-07-23T00:00:49Z".to_owned(),
            },
        }
    }

    fn enqueue_finished(manager: &AssignmentManager, identity: &AssignmentIdentity) -> u64 {
        manager
            .outbox
            .enqueue(AssignmentObservation::Execution {
                assignment_id: identity.assignment_id.clone(),
                attempt_id: identity.attempt_id.clone(),
                report: ExecutionReport::Finished {
                    final_execution_event_sequence: 1,
                    outcome: json!({ "outcome": "succeeded" }),
                    artifact_delivery: json!({
                        "outcome": "prepared",
                        "artifactSetId": "ats_01k0z6r1w8f4jy2m7q9v3x5abc",
                    }),
                },
            })
            .unwrap()
    }

    fn enqueue_completion(manager: &AssignmentManager, final_delivery_deadline: Instant) -> u64 {
        let identity = match &manager.slot {
            Some(LocalSlot::Running(running)) => running.identity.clone(),
            _ => panic!("fixture assignment must be running"),
        };
        let final_observation_id = enqueue_finished(manager, &identity);
        manager
            .event_sender
            .send(ManagerEvent::Finished {
                assignment_id: identity.assignment_id,
                final_observation_id: Some(final_observation_id),
                final_delivery_deadline: Some(final_delivery_deadline),
                retained_root: None,
            })
            .unwrap();
        final_observation_id
    }

    fn execution_job(manager: &mut AssignmentManager, offered: &AssignmentOffer) -> ExecutionJob {
        manager
            .handle_start(start_for(offered))
            .unwrap()
            .expect("valid start dispatches execution")
    }

    fn spawn_execution(manager: &mut AssignmentManager, offered: &AssignmentOffer) {
        execution_job(manager, offered).spawn();
    }

    fn controlled_running_fixture() -> (
        tempfile::TempDir,
        AssignmentManager,
        tokio::sync::mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
        AssignmentOffer,
    ) {
        let workflow = "schemaVersion: 1\nsteps:\n  wait:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"sleep 60\"]\n";
        let (temporary, mut manager) = manager_fixture(workflow);
        let (sleeper, sleep_requests) = controlled_sleeper();
        manager.sleeper = sleeper;
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        spawn_execution(&mut manager, &offered);
        (temporary, manager, sleep_requests, offered)
    }

    fn assert_workflow_environment_unsupported(manager: &mut AssignmentManager) {
        let pending = manager.pending_observations(&BTreeSet::new(), 1);
        assert!(matches!(
            &pending[0].observation,
            AssignmentObservation::Decision(AssignmentDecision::Rejected {
                decline: AssignmentDecline::RunnerUnable(
                    RunnerUnableReason::WorkflowEnvironmentUnsupported
                ),
                ..
            })
        ));
        assert!(manager.slot.is_none());
    }

    async fn execute_to_terminal(
        manager: &mut AssignmentManager,
        offered: &AssignmentOffer,
    ) -> Vec<ExecutionReport> {
        spawn_execution(manager, offered);
        with_watchdog(wait_for_terminal(manager))
            .await
            .expect("workflow did not finish")
    }

    fn fail_pending_artifact_registrations(
        manager: &mut AssignmentManager,
        pending: &[PendingAssignmentObservation],
    ) -> bool {
        let registrations = pending
            .iter()
            .filter_map(|entry| match &entry.observation {
                AssignmentObservation::Artifact {
                    delivery_id,
                    request: ArtifactRequest::RegisterCarrier { .. },
                } => Some((entry.id, *delivery_id, false)),
                AssignmentObservation::Artifact {
                    delivery_id,
                    request: ArtifactRequest::RegisterResult { .. },
                } => Some((entry.id, *delivery_id, true)),
                _ => None,
            })
            .collect::<Vec<_>>();
        for (observation_id, delivery_id, is_result) in &registrations {
            let response = if *is_result {
                ArtifactCloudResponse::ResultRegistration(ArtifactResultRegistrationResponse {
                    request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    outcome: ArtifactResultRegistrationOutcome::Failed {
                        code: "storage_quota_exceeded".to_owned(),
                    },
                })
            } else {
                ArtifactCloudResponse::CarrierRegistration(ArtifactRegistrationResponse {
                    request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    outcome: ArtifactRegistrationOutcome::Failed {
                        code: "storage_quota_exceeded".to_owned(),
                    },
                })
            };
            manager
                .handle_artifact_response(*observation_id, *delivery_id, response)
                .expect("test Cloud must close artifact registration");
        }
        !registrations.is_empty()
    }

    async fn wait_for_terminal(manager: &mut AssignmentManager) -> Vec<ExecutionReport> {
        let notification = manager.notification();
        loop {
            let notified = notification.notified();
            tokio::pin!(notified);
            let pending = manager.pending_observations(&BTreeSet::new(), 100);
            if fail_pending_artifact_registrations(manager, &pending) {
                continue;
            }
            if pending
                .iter()
                .any(|pending| pending.observation.is_terminal())
            {
                return pending
                    .into_iter()
                    .filter_map(|pending| match pending.observation {
                        AssignmentObservation::Execution { report, .. } => Some(report),
                        AssignmentObservation::Decision(_)
                        | AssignmentObservation::LeaseRenewalRequested { .. }
                        | AssignmentObservation::Artifact { .. } => None,
                    })
                    .collect();
            }
            notified.await;
        }
    }

    fn command_fixture_arguments() -> Vec<String> {
        command_fixture_arguments_for(COMMAND_FIXTURE_TEST_NAME)
    }

    fn failing_command_fixture_arguments() -> Vec<String> {
        command_fixture_arguments_for(FAILING_COMMAND_FIXTURE_TEST_NAME)
    }

    fn command_fixture_arguments_for(test_name: &str) -> Vec<String> {
        std::iter::once(
            std::env::current_exe()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned(),
        )
        .chain(
            ["--ignored", "--exact", test_name, "--nocapture"]
                .into_iter()
                .map(str::to_owned),
        )
        .collect()
    }

    fn install_command_fixture_environment(manager: &mut AssignmentManager, address: String) {
        manager.environment = EnvironmentSnapshot::new([(
            OsString::from(COMMAND_FIXTURE_SOCKET),
            OsString::from(address),
        )]);
    }

    async fn release_command_fixtures(listener: &TcpListener, count: usize) {
        for _ in 0..count {
            let (mut control, _) = listener.accept().await.unwrap();
            let mut ready = [0_u8; 1];
            control.read_exact(&mut ready).await.unwrap();
            assert_eq!(ready, [1]);
            control.write_all(&[1]).await.unwrap();
        }
    }

    async fn execute_fixture_to_terminal(
        manager: &mut AssignmentManager,
        offered: &AssignmentOffer,
        listener: &TcpListener,
        command_count: usize,
    ) -> Vec<ExecutionReport> {
        spawn_execution(manager, offered);
        match with_watchdog(async {
            tokio::join!(
                wait_for_terminal(manager),
                release_command_fixtures(listener, command_count),
            )
        })
        .await
        {
            Ok((reports, ())) => reports,
            Err(_) => panic!(
                "assignment command fixture did not finish; pending: {:#?}",
                manager.pending_observations(&BTreeSet::new(), 100)
            ),
        }
    }

    async fn execute_fixture_workflow(
        workflow: &str,
        pi_source: Option<&str>,
        command_count: usize,
    ) -> Vec<ExecutionReport> {
        let (_temporary, mut manager) = manager_fixture_with_pi(workflow, pi_source);
        offer_and_execute_with_command_fixtures(&mut manager, command_count).await
    }

    fn assert_succeeded(reports: &[ExecutionReport]) {
        assert!(
            matches!(
                reports.last(),
                Some(ExecutionReport::Finished { outcome, .. })
                    if outcome == &json!({ "outcome": "succeeded" })
            ),
            "unexpected reports: {reports:#?}"
        );
    }

    async fn offer_and_execute(manager: &mut AssignmentManager) -> Vec<ExecutionReport> {
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        execute_to_terminal(manager, &offered).await
    }

    async fn offer_and_execute_with_command_fixtures(
        manager: &mut AssignmentManager,
        command_count: usize,
    ) -> Vec<ExecutionReport> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        install_command_fixture_environment(manager, listener.local_addr().unwrap().to_string());
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        execute_fixture_to_terminal(manager, &offered, &listener, command_count).await
    }

    fn assert_workflow_succeeded(reports: &[ExecutionReport]) {
        assert!(matches!(
            reports.last(),
            Some(ExecutionReport::Finished { outcome, .. })
                if outcome == &json!({ "outcome": "succeeded" })
        ));
    }

    #[test]
    fn execution_spec_accepts_maximum_cancellation_grace_and_rejects_above_it() {
        let mut execution_spec = offer("bg").execution_spec;
        execution_spec.execution_limits.cancellation_grace_seconds =
            MAXIMUM_CANCELLATION_GRACE.as_secs();
        assert_eq!(validate_execution_spec(&execution_spec), Ok(()));

        execution_spec.execution_limits.cancellation_grace_seconds += 1;
        assert_eq!(
            validate_execution_spec(&execution_spec),
            Err(AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::InvalidExecutionLimits
            ))
        );
    }

    #[test]
    fn admits_file_exports_for_cloud_delivery() {
        let workflow = "schemaVersion: 1\nsteps:\n  write:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      value:\n        kind: file\n        path: value.txt\n        mediaType: text/plain\nexports:\n  result:\n    ref: outputs.write.value\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        manager.handle_offer(offer("bg")).unwrap();
        let pending = manager.pending_observations(&BTreeSet::new(), 1);
        assert!(matches!(
            pending[0].observation,
            AssignmentObservation::Decision(AssignmentDecision::Accepted { .. })
        ));
    }

    #[test]
    fn malformed_and_unsupported_source_projections_have_closed_declines() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let mut unsupported = offer("bg");
        unsupported.execution_spec.source.object_format = "sha256".to_owned();
        assert_offer_declined(
            &mut manager,
            unsupported,
            AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::UnsupportedSourceObjectFormat,
            ),
        );

        let (_temporary, mut manager) = manager_fixture(workflow);
        let mut malformed = offer("bh");
        malformed.execution_spec.source.commit_oid = "not-an-oid".to_owned();
        assert_offer_declined(
            &mut manager,
            malformed,
            AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::InvalidSourceProjection,
            ),
        );
    }

    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "wall time only bounds the blocking source fixture's readiness messages"
    )]
    fn release_fences_source_preparation_and_exact_replay_cannot_reissue() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let offered = offer("bg");
        let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
        let (stopped_sender, stopped_receiver) = std::sync::mpsc::sync_channel(1);
        let broker = Arc::new(BlockingSourceBroker {
            started: Mutex::new(Some(started_sender)),
            stopped: Mutex::new(Some(stopped_sender)),
            calls: AtomicUsize::new(0),
        });
        manager.fixture_materialized_source = None;
        manager.source_broker = Some(broker.clone());

        manager.handle_offer(offered.clone()).unwrap();
        assert!(
            started_receiver
                .recv_timeout(Duration::from_secs(1))
                .is_ok()
        );
        assert!(matches!(manager.slot, Some(LocalSlot::Preparing(_))));
        manager
            .handle_release(
                &offered.assignment_id,
                &offered.run_id,
                &offered.attempt_id,
                "offer_expired",
            )
            .unwrap();
        assert!(manager.slot.is_none());
        assert!(
            stopped_receiver
                .recv_timeout(Duration::from_secs(1))
                .is_ok()
        );

        manager.handle_offer(offered).unwrap();
        assert_eq!(broker.calls.load(Ordering::Relaxed), 1);
        assert!(
            manager
                .pending_observations(&BTreeSet::new(), 10)
                .is_empty()
        );
    }

    #[test]
    fn source_materialization_declines_preserve_failure_provenance() {
        assert_eq!(
            materialization_decline(MaterializationFailure::CommitUnavailable),
            AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::SourceCommitMismatch,
            )
        );
        assert_eq!(
            materialization_decline(MaterializationFailure::EnvironmentUnavailable),
            AssignmentDecline::RunnerUnable(RunnerUnableReason::ExecutionEnvironmentUnavailable)
        );
    }

    #[test]
    fn source_credential_failure_declines_the_assignment() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let offered = offer("bg");
        manager.fixture_materialized_source = None;
        manager.source_broker = Some(Arc::new(UnavailableSourceBroker));

        assert_offer_declined(
            &mut manager,
            offered,
            AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable),
        );
    }

    #[test]
    fn execution_spec_accepts_the_shared_parallelism_limit_and_declines_the_next_value() {
        let mut execution_spec = offer("bg").execution_spec;
        execution_spec.execution_limits.maximum_parallel_steps =
            u64::try_from(MAXIMUM_PARALLEL_STEPS).unwrap();
        assert_eq!(validate_execution_spec(&execution_spec), Ok(()));

        execution_spec.execution_limits.maximum_parallel_steps += 1;
        assert_eq!(
            validate_execution_spec(&execution_spec),
            Err(invalid_execution_limits())
        );
    }

    #[test]
    fn command_workflows_do_not_require_an_agent_runtime() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        manager.handle_offer(offer("bg")).unwrap();
        assert_eq!(manager.active_step_count(), Some(1));
    }

    #[tokio::test]
    async fn managed_workflow_environment_excludes_runner_credentials_and_helpers() {
        let workflow = r#"schemaVersion: 1
steps:
  check:
    kind: cmd
    command:
      argv:
        - sh
        - -c
        - 'test "$RUNNER_VISIBLE" = retained && test -z "${GH_TOKEN+x}" && test -z "${GITHUB_TOKEN+x}" && test -z "${GIT_ASKPASS+x}" && test -z "${GIT_CONFIG_KEY_0+x}" && test -z "${GIT_CONFIG_VALUE_0+x}" && test -z "${GIT_SSH_COMMAND+x}" && test -z "${SSH_AUTH_SOCK+x}" && test -z "${SSH_AGENT_PID+x}" && test -z "${SCHERZO_SOURCE_TOKEN_FD+x}"'
"#;
        let (_temporary, mut manager) = manager_fixture(workflow);
        let mut variables = manager.environment.variables().clone();
        for (name, value) in [
            ("RUNNER_VISIBLE", "retained"),
            ("GIT_ASKPASS", "/runner/private/askpass"),
            ("GIT_ASKPASS_REQUIRE", "force"),
            ("GIT_TERMINAL_PROMPT", "0"),
            ("GIT_CONFIG", "runner-private"),
            ("GIT_CONFIG_COUNT", "1"),
            ("GIT_CONFIG_PARAMETERS", "runner-private"),
            ("GIT_CONFIG_GLOBAL", "/runner/private/gitconfig"),
            ("GIT_CONFIG_NOSYSTEM", "1"),
            ("GIT_CONFIG_SYSTEM", "/runner/private/system-gitconfig"),
            ("GIT_CONFIG_KEY_0", "http.extraHeader"),
            ("GIT_CONFIG_VALUE_0", "authorization: runner-private"),
            ("GIT_SSH", "/runner/private/ssh"),
            ("GIT_SSH_COMMAND", "/runner/private/ssh --private"),
            ("SSH_ASKPASS", "/runner/private/ssh-askpass"),
            ("SSH_ASKPASS_REQUIRE", "force"),
            ("SSH_AUTH_SOCK", "/runner/private/agent.sock"),
            ("SSH_AGENT_PID", "4242"),
            ("GH_TOKEN", "runner-private"),
            ("GITHUB_TOKEN", "runner-private"),
            ("SCHERZO_SOURCE_TOKEN_FD", "9"),
        ] {
            variables.insert(OsString::from(name), OsString::from(value));
        }
        manager.environment = EnvironmentSnapshot::new(variables);
        let offered = offer("bg");

        manager.handle_offer(offered.clone()).unwrap();

        let environment = match &manager.slot {
            Some(LocalSlot::Accepted(accepted)) => accepted.admitted.execution().environment(),
            _ => panic!("assignment should be accepted"),
        };
        assert_eq!(
            environment.variable(OsStr::new("RUNNER_VISIBLE")),
            Some(OsStr::new("retained"))
        );
        for name in [
            "GIT_ASKPASS",
            "GIT_ASKPASS_REQUIRE",
            "GIT_TERMINAL_PROMPT",
            "GIT_CONFIG",
            "GIT_CONFIG_COUNT",
            "GIT_CONFIG_PARAMETERS",
            "GIT_CONFIG_GLOBAL",
            "GIT_CONFIG_NOSYSTEM",
            "GIT_CONFIG_SYSTEM",
            "GIT_CONFIG_KEY_0",
            "GIT_CONFIG_VALUE_0",
            "GIT_SSH",
            "GIT_SSH_COMMAND",
            "SSH_ASKPASS",
            "SSH_ASKPASS_REQUIRE",
            "SSH_AUTH_SOCK",
            "SSH_AGENT_PID",
            "GH_TOKEN",
            "GITHUB_TOKEN",
            "SCHERZO_SOURCE_TOKEN_FD",
        ] {
            assert!(environment.variable(OsStr::new(name)).is_none(), "{name}");
        }

        assert_workflow_succeeded(&execute_to_terminal(&mut manager, &offered).await);
    }

    #[test]
    fn admission_requires_only_the_harness_selected_by_the_assignment() {
        let (pi_temporary, mut pi_manager) =
            manager_fixture_with_harnesses(PI_ONLY_WORKFLOW, None, Some(SUCCESSFUL_CLAUDE_CODE));
        pi_manager.handle_offer(offer("bg")).unwrap();
        assert_workflow_environment_unsupported(&mut pi_manager);
        assert!(!pi_temporary.path().join("claude.calls").exists());

        let (claude_temporary, mut claude_manager) =
            manager_fixture_with_harnesses(CLAUDE_CODE_ONLY_WORKFLOW, Some(SUCCESSFUL_PI), None);
        claude_manager.handle_offer(offer("bg")).unwrap();
        assert_workflow_environment_unsupported(&mut claude_manager);
        assert!(!claude_temporary.path().join("pi.calls").exists());
    }

    #[test]
    fn release_retires_an_unsent_acceptance_observation() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        assert_eq!(manager.pending_observations(&BTreeSet::new(), 10).len(), 1);

        manager
            .handle_release(
                &offered.assignment_id,
                &offered.run_id,
                &offered.attempt_id,
                "stale_or_invalid_acceptance",
            )
            .unwrap();

        assert!(manager.slot.is_none());
        assert_eq!(manager.pending_observations(&BTreeSet::new(), 10), vec![]);
    }

    #[tokio::test]
    async fn accepted_phase_shutdown_omits_non_authoritative_finalization() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\nfinalizers:\n  cleanup:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        manager.handle_offer(offer("bg")).unwrap();

        manager.begin_shutdown().unwrap();

        assert!(matches!(manager.slot, Some(LocalSlot::Finishing(_))));
        let pending = manager.pending_observations(&BTreeSet::new(), 10);
        assert_eq!(
            pending.len(),
            2,
            "semantic acceptance must remain ahead of accepted-phase interruption"
        );
        assert!(matches!(
            &pending[0].observation,
            AssignmentObservation::Decision(AssignmentDecision::Accepted { .. })
        ));
        assert!(matches!(
            &pending[1].observation,
            AssignmentObservation::Execution {
                report: ExecutionReport::AssignmentInterrupted { reason },
                ..
            } if reason == "graceful_shutdown"
        ));
    }

    #[test]
    fn shutdown_requests_runner_cancellation_for_running_work() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        let _job = execution_job(&mut manager, &offered);

        manager.begin_shutdown().unwrap();

        assert!(matches!(
            &manager.slot,
            Some(LocalSlot::Running(running))
                if running.cancellation.cancellation_reason()
                    == Some(crate::execution::workflow::admission::CancellationReason::RunnerShutdown)
        ));
    }

    #[tokio::test]
    async fn shutdown_rearms_after_the_finalization_boundary_and_reports_summary() {
        let workflow = "schemaVersion: 1\nsteps:\n  wait:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"sleep 60\"]\nfinalizers:\n  cleanup:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        spawn_execution(&mut manager, &offered);
        manager.begin_shutdown().unwrap();

        let reports = with_watchdog(wait_for_terminal(&mut manager))
            .await
            .expect("shutdown did not quiesce execution");

        assert!(matches!(
            reports.last(),
            Some(ExecutionReport::Interrupted {
                reason,
                terminal_outcome,
                ..
            }) if reason == "graceful_shutdown"
                && terminal_outcome["reason"] == "runner_shutdown"
                && terminal_outcome["finalization"]["trigger"] == "cancelled"
                && terminal_outcome["finalization"]["cancellation"]["reason"]
                    == "runner_shutdown"
                && terminal_outcome["finalization"]["finalizers"][0]["id"] == "cleanup"
                && terminal_outcome["finalization"]["finalizers"][0]["state"] == "cancelled"
        ));
    }

    #[test]
    fn exact_start_grant_dispatches_once() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        let start = start_for(&offered);
        assert!(manager.handle_start(start.clone()).unwrap().is_some());
        assert!(manager.handle_start(start).unwrap().is_none());
    }

    #[test]
    fn cancellation_grace_accepts_equality_and_rejects_one_millisecond_late() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, manager) = manager_fixture(workflow);
        let exact = ExecutionLeaseGrant {
            sequence: 1,
            expires_at: "2026-07-23T00:00:12Z".to_owned(),
            runner_stop_before: "2026-07-23T00:00:01Z".to_owned(),
        };
        assert!(
            manager
                .validate_grant(&exact, 1, Duration::from_secs(1))
                .is_ok()
        );

        let late = ExecutionLeaseGrant {
            runner_stop_before: "2026-07-23T00:00:00.999Z".to_owned(),
            ..exact
        };
        assert!(
            manager
                .validate_grant(&late, 1, Duration::from_secs(1))
                .is_err()
        );
    }

    #[test]
    fn grant_rejects_stop_window_shorter_than_welcomed_fencing_margin() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, manager) = manager_fixture(workflow);
        let grant = ExecutionLeaseGrant {
            sequence: 1,
            expires_at: "2026-07-23T00:00:02.001Z".to_owned(),
            runner_stop_before: "2026-07-23T00:00:02Z".to_owned(),
        };

        assert!(
            manager
                .validate_grant(&grant, 1, Duration::from_secs(1))
                .is_err(),
            "a grant must preserve the welcomed fencing margin between stop and expiry"
        );
    }

    #[test]
    fn wall_clock_uncertainty_accepts_equality_and_rejects_one_unit_late() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        manager.wall_clock = wall_clock(Duration::from_millis(1_000));
        assert_eq!(manager.validate_wall_clock(), Ok(()));
        manager.wall_clock = wall_clock(Duration::from_millis(1_001));
        assert!(matches!(
            manager.validate_wall_clock(),
            Err(AssignmentDecline::RunnerUnable(
                RunnerUnableReason::ExecutionEnvironmentUnavailable
            ))
        ));
    }

    #[test]
    fn accepted_runner_assignment_enables_stopped_spawn_registration() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        manager.guard_processes = true;
        manager.handle_offer(offer("bg")).unwrap();
        assert!(matches!(
            &manager.slot,
            Some(LocalSlot::Accepted(accepted))
                if accepted.process_guards.registry(accepted.guard_processes).is_durable()
        ));
    }

    #[tokio::test]
    async fn lease_terminal_delivery_uses_the_welcomed_budget_without_reporting() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        manager.sleeper = sleeper;
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        let job = execution_job(&mut manager, &offered);
        drop(job);
        let deadline = manager
            .sleeper
            .now()
            .checked_add(Duration::from_secs(5))
            .unwrap();
        enqueue_completion(&manager, deadline);
        manager.pending_observations(&BTreeSet::new(), 100);

        sleep_request(&mut sleep_requests, Duration::from_secs(5))
            .await
            .release();
        let notification = manager.notification();
        with_watchdog(async {
            loop {
                let notified = notification.notified();
                tokio::pin!(notified);
                manager.pending_observations(&BTreeSet::new(), 100);
                if manager.slot.is_none() {
                    break;
                }
                notified.await;
            }
        })
        .await
        .expect("terminal delivery budget did not fence replay");
        assert!(manager.reporting.is_none());
        assert_eq!(manager.pending_observations(&BTreeSet::new(), 100), vec![]);
    }

    #[tokio::test]
    async fn zero_terminal_delivery_budget_fences_the_report_at_selection() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let mut zero_delivery_policy = policy();
        zero_delivery_policy.terminal_report_delivery_budget_milliseconds = 0;
        manager.lease_policy = Some(zero_delivery_policy);
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        let job = execution_job(&mut manager, &offered);
        assert_eq!(
            job.authority_updates
                .borrow()
                .terminal_report_delivery_budget,
            Duration::ZERO
        );
        drop(job);
        let final_observation_id = enqueue_completion(&manager, manager.sleeper.now());

        let pending = manager.pending_observations(&BTreeSet::new(), 100);
        assert!(
            pending.iter().all(|entry| entry.id != final_observation_id),
            "an exclusive zero delivery budget must not leave the terminal report replayable"
        );
        assert!(manager.slot.is_none());
    }

    #[tokio::test]
    async fn ordinary_terminal_delivery_uses_the_welcomed_budget() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        manager.sleeper = sleeper;
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        let acceptance = manager.pending_observations(&BTreeSet::new(), 1)[0].id;
        manager.acknowledge_observation(acceptance);
        spawn_execution(&mut manager, &offered);

        let (renewal_duration, _renewal_release) =
            with_watchdog(sleep_requests.recv()).await.unwrap().unwrap();
        assert_eq!(renewal_duration, Duration::from_secs(13));
        let notification = manager.notification();
        with_watchdog(async {
            loop {
                let notified = notification.notified();
                tokio::pin!(notified);
                let pending = manager.pending_observations(&BTreeSet::new(), 100);
                if fail_pending_artifact_registrations(&mut manager, &pending) {
                    continue;
                }
                if pending.iter().any(|entry| entry.observation.is_terminal()) {
                    manager.pending_observations(&BTreeSet::new(), 100);
                    break;
                }
                notified.await;
            }
        })
        .await
        .expect("workflow did not select a terminal report");

        let (artifact_duration, _artifact_release) =
            with_watchdog(sleep_requests.recv()).await.unwrap().unwrap();
        assert_eq!(artifact_duration, Duration::from_secs(13));

        let (delivery_duration, _delivery_release) =
            with_watchdog(sleep_requests.recv()).await.unwrap().unwrap();
        assert_eq!(delivery_duration, Duration::from_secs(5));
    }

    #[test]
    fn completion_without_a_terminal_report_fences_assignment_writes() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        let job = execution_job(&mut manager, &offered);
        drop(job);
        manager
            .outbox
            .enqueue(AssignmentObservation::Execution {
                assignment_id: offered.assignment_id.clone(),
                attempt_id: offered.attempt_id.clone(),
                report: ExecutionReport::Started,
            })
            .unwrap();
        manager
            .event_sender
            .send(ManagerEvent::Finished {
                assignment_id: offered.assignment_id,
                final_observation_id: None,
                final_delivery_deadline: None,
                retained_root: None,
            })
            .unwrap();

        assert_eq!(manager.pending_observations(&BTreeSet::new(), 100), vec![]);
        assert!(manager.slot.is_none());
    }

    #[test]
    fn expiry_release_revokes_running_authority_immediately() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        let job = execution_job(&mut manager, &offered);

        manager
            .handle_release(
                &offered.assignment_id,
                &offered.run_id,
                &offered.attempt_id,
                "execution_lease_expired",
            )
            .unwrap();

        assert!(job.authority_updates.borrow().revoked);
        assert!(matches!(
            &manager.slot,
            Some(LocalSlot::Running(running))
                if running.cancellation.cancellation_reason()
                    == Some(crate::execution::workflow::admission::CancellationReason::ExecutionLeaseExpired)
        ));
    }

    #[tokio::test]
    async fn delayed_execution_job_does_not_start_at_cancellation_boundary() {
        let placeholder = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (temporary, mut manager) = manager_fixture(placeholder);
        let marker = temporary.path().join("launched");
        fs::write(
            temporary.path().join("source/workflow.yaml"),
            format!(
                "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"touch {}\"]\n",
                marker.display()
            ),
        )
        .unwrap();
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        manager.sleeper = Arc::clone(&sleeper);
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        let acceptance = manager.pending_observations(&BTreeSet::new(), 1)[0].id;
        manager.acknowledge_observation(acceptance);
        let job = execution_job(&mut manager, &offered);

        let advancing_sleeper = Arc::clone(&sleeper);
        let advance = tokio::spawn(async move {
            advancing_sleeper.sleep(Duration::from_secs(18)).await;
        });
        sleep_request(&mut sleep_requests, Duration::from_secs(18))
            .await
            .release();
        advance.await.unwrap();
        job.spawn();

        let notification = manager.notification();
        with_watchdog(async {
            loop {
                let notified = notification.notified();
                tokio::pin!(notified);
                let pending = manager.pending_observations(&BTreeSet::new(), 100);
                assert!(
                    pending.iter().all(|entry| !matches!(
                        entry.observation,
                        AssignmentObservation::Execution { .. }
                    )),
                    "a delayed execution job must not publish assignment observations"
                );
                if manager.slot.is_none() {
                    break;
                }
                notified.await;
            }
        })
        .await
        .expect("delayed execution job did not relinquish its assignment");
        assert!(
            !marker.exists(),
            "a delayed execution job launched its command"
        );
    }

    #[tokio::test]
    async fn schedules_renewal_before_initial_lease_loss_cancellation() {
        let (_temporary, _manager, mut sleep_requests, _offered) = controlled_running_fixture();

        let (duration, _release) = with_watchdog(sleep_requests.recv())
            .await
            .expect("runner did not schedule a lease timer")
            .expect("lease timer channel closed");

        // Cancellation starts after 18 seconds. The welcomed five-second renewal
        // delivery budget requires a renewal request no later than second 13.
        assert!(
            duration <= Duration::from_secs(13),
            "first lease timer was scheduled at {duration:?}"
        );
    }

    #[tokio::test]
    async fn lease_loss_quiescence_attempts_artifact_delivery_before_terminal_report() {
        let (_temporary, mut manager, mut sleep_requests, _offered) = controlled_running_fixture();

        sleep_request(&mut sleep_requests, Duration::from_secs(13))
            .await
            .release();
        sleep_request(&mut sleep_requests, Duration::from_secs(5))
            .await
            .release();

        let notification = manager.notification();
        let report = with_watchdog(async {
            loop {
                let notified = notification.notified();
                tokio::pin!(notified);
                let pending = manager.pending_observations(&BTreeSet::new(), 100);
                if let Some(artifact) = pending
                    .iter()
                    .find(|entry| entry.artifact_delivery_id().is_some())
                {
                    manager
                        .handle_artifact_response(
                            artifact.id,
                            artifact.artifact_delivery_id().unwrap(),
                            ArtifactCloudResponse::ResultRegistration(
                                ArtifactResultRegistrationResponse {
                                    request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc"
                                        .to_owned(),
                                    outcome: ArtifactResultRegistrationOutcome::Failed {
                                        code: "storage_quota_exceeded".to_owned(),
                                    },
                                },
                            ),
                        )
                        .expect("lease-loss delivery registration must remain authoritative");
                }
                if let Some(report) =
                    pending
                        .into_iter()
                        .find_map(|entry| match entry.observation {
                            AssignmentObservation::Execution { report, .. }
                                if report.is_terminal() =>
                            {
                                Some(report)
                            }
                            _ => None,
                        })
                {
                    break report;
                }
                assert!(
                    manager.slot.is_some(),
                    "orderly lease-loss quiescence dropped the terminal artifact delivery"
                );
                notified.await;
            }
        })
        .await
        .expect("lease-loss delivery did not close");

        assert!(
            matches!(
                &report,
                ExecutionReport::Interrupted {
                    reason,
                    artifact_delivery,
                    ..
                } if reason == "execution_lease_expired"
                    && artifact_delivery == &json!({
                        "outcome": "failed",
                        "phase": "registration",
                        "code": "storage_quota_exceeded",
                    })
            ),
            "unexpected lease-loss report: {report:?}"
        );
    }

    #[test]
    fn conflicting_replay_of_an_earlier_renewal_is_rejected() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        let _job = execution_job(&mut manager, &offered);

        let first = renewal_for(&offered);
        manager.handle_renewal(first.clone()).unwrap();
        let mut second = first.clone();
        second.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abk".to_owned();
        second.lease.sequence = 3;
        second.lease.expires_at = "2026-07-23T00:01:30Z".to_owned();
        second.lease.runner_stop_before = "2026-07-23T00:01:19Z".to_owned();
        manager.handle_renewal(second).unwrap();

        let mut conflicting_replay = first;
        conflicting_replay.lease.expires_at = "2026-07-23T00:01:01Z".to_owned();
        assert_eq!(
            manager.handle_renewal(conflicting_replay),
            Err(AssignmentManagerFailure::ConflictingOffer)
        );
    }

    #[tokio::test]
    async fn renewal_at_monotonic_stop_boundary_is_rejected_despite_wall_clock_lag() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        manager.sleeper = Arc::clone(&sleeper);
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        let _job = execution_job(&mut manager, &offered);

        let advancing_sleeper = Arc::clone(&sleeper);
        let advance = tokio::spawn(async move {
            advancing_sleeper.sleep(Duration::from_secs(19)).await;
        });
        sleep_request(&mut sleep_requests, Duration::from_secs(19))
            .await
            .release();
        advance.await.unwrap();
        manager.wall_clock = wall_clock_at(Duration::ZERO, "2026-07-23T00:00:18.999Z");

        manager.handle_renewal(renewal_for(&offered)).unwrap();

        let running = match &manager.slot {
            Some(LocalSlot::Running(running)) => running,
            _ => panic!("fixture assignment must remain represented while fencing"),
        };
        assert_eq!(
            running.current_grant.sequence, 1,
            "a renewal must not revive authority at the retained monotonic stop boundary"
        );
        assert_eq!(
            running.cancellation.cancellation_reason(),
            Some(crate::execution::workflow::admission::CancellationReason::ExecutionLeaseExpired)
        );
        assert!(running.authority_updates.borrow().revoked);
    }

    #[test]
    fn renewal_at_or_after_current_stop_boundary_is_rejected_and_fenced() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        for now in ["2026-07-23T00:00:19Z", "2026-07-23T00:00:19.001Z"] {
            let (_temporary, mut manager) = manager_fixture(workflow);
            let offered = offer("bg");
            manager.handle_offer(offered.clone()).unwrap();
            let _job = execution_job(&mut manager, &offered);
            manager.wall_clock = wall_clock_at(Duration::ZERO, now);

            manager.handle_renewal(renewal_for(&offered)).unwrap();

            assert!(matches!(
                &manager.slot,
                Some(LocalSlot::Running(running))
                    if running.current_grant.sequence == 1
                        && running.cancellation.cancellation_reason()
                            == Some(crate::execution::workflow::admission::CancellationReason::ExecutionLeaseExpired)
                        && running.authority_updates.borrow().revoked
            ));
        }
    }

    #[tokio::test]
    async fn exact_next_renewal_replaces_the_running_authority() {
        let (_temporary, mut manager, mut sleep_requests, offered) = controlled_running_fixture();

        let (duration, release) = with_watchdog(sleep_requests.recv()).await.unwrap().unwrap();
        assert_eq!(duration, Duration::from_secs(13));
        release.release();
        let notification = manager.notification();
        let requested = with_watchdog(async {
            loop {
                let notified = notification.notified();
                tokio::pin!(notified);
                if let Some(requested) = manager
                    .pending_observations(&BTreeSet::new(), 10)
                    .into_iter()
                    .find(|pending| {
                        matches!(
                            pending.observation,
                            AssignmentObservation::LeaseRenewalRequested { .. }
                        )
                    })
                {
                    break requested;
                }
                notified.await;
            }
        })
        .await
        .expect("runner did not request renewal");
        encode_runner_frame(&requested.observation.runner_frame(RunnerEnvelope {
            message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            sequence: 1,
            sent_at: NOW.to_owned(),
        }))
        .expect("renewal request satisfies the runner protocol");

        let renewal = renewal_for(&offered);
        manager.handle_renewal(renewal.clone()).unwrap();
        manager.handle_renewal(renewal).unwrap();
        let (duration, _release) = with_watchdog(async {
            loop {
                let request = sleep_requests.recv().await?;
                if request.0 != Duration::from_secs(5) {
                    break Some(request);
                }
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(duration, Duration::from_secs(43));

        let mut gap = renewal_for(&offered);
        gap.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abk".to_owned();
        gap.lease.sequence = 4;
        assert_eq!(
            manager.handle_renewal(gap),
            Err(AssignmentManagerFailure::ConflictingOffer)
        );
    }

    #[tokio::test]
    async fn command_pi_claude_and_mixed_assignment_matrix_invokes_only_declared_harnesses() {
        let fixture_argv = serde_json::to_string(&command_fixture_arguments()).unwrap();
        let command_only_workflow = format!(
            "schemaVersion: 1\nsteps:\n  command:\n    kind: cmd\n    command:\n      argv: {fixture_argv}\n"
        );
        for (
            workflow,
            pi_source,
            claude_code_source,
            command_count,
            expected_pi,
            expected_claude,
        ) in [
            (
                command_only_workflow.as_str(),
                Some(SUCCESSFUL_PI),
                Some(SUCCESSFUL_CLAUDE_CODE),
                1,
                false,
                false,
            ),
            (
                PI_ONLY_WORKFLOW,
                Some(SUCCESSFUL_PI),
                Some(SUCCESSFUL_CLAUDE_CODE),
                0,
                true,
                false,
            ),
            (
                CLAUDE_CODE_ONLY_WORKFLOW,
                Some(SUCCESSFUL_PI),
                Some(SUCCESSFUL_CLAUDE_CODE),
                0,
                false,
                true,
            ),
            (
                MIXED_HARNESS_WORKFLOW,
                Some(SUCCESSFUL_PI),
                Some(SUCCESSFUL_CLAUDE_CODE),
                0,
                true,
                true,
            ),
        ] {
            let (temporary, mut manager) =
                manager_fixture_with_harnesses(workflow, pi_source, claude_code_source);
            let reports = if command_count == 0 {
                offer_and_execute(&mut manager).await
            } else {
                offer_and_execute_with_command_fixtures(&mut manager, command_count).await
            };

            assert_workflow_succeeded(&reports);
            assert_eq!(
                reports.iter().filter(|report| report.is_terminal()).count(),
                1
            );
            let transcript = format!("{reports:?}");
            assert!(!transcript.contains("stream_event"));
            assert!(!transcript.contains("00000000-0000-4000-8000-00000000009"));
            assert_eq!(temporary.path().join("pi.calls").exists(), expected_pi);
            assert_eq!(
                temporary.path().join("claude.calls").exists(),
                expected_claude
            );
            if expected_pi {
                let _ = only_harness_call(&temporary.path().join("pi.calls"));
            }
            if expected_claude {
                let _ = only_harness_call(&temporary.path().join("claude.calls"));
            }
        }
    }

    #[tokio::test]
    async fn executes_explicit_command_dag_and_reports_dense_transitions() {
        let fixture_argv = serde_json::to_string(&command_fixture_arguments()).unwrap();
        let workflow = format!(
            "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: {fixture_argv}\n  consume:\n    kind: cmd\n    dependsOn: [produce]\n    command:\n      argv: {fixture_argv}\n"
        );
        let reports = execute_fixture_workflow(&workflow, None, 2).await;
        assert!(
            matches!(reports.first(), Some(ExecutionReport::Started)),
            "unexpected reports: {reports:#?}"
        );
        assert_succeeded(&reports);
        let sequences: Vec<_> = reports
            .iter()
            .filter_map(|report| match report {
                ExecutionReport::Transition {
                    execution_event_sequence,
                    ..
                } => Some(*execution_event_sequence),
                _ => None,
            })
            .collect();
        assert_eq!(sequences, (1..=sequences.len() as u64).collect::<Vec<_>>());
        assert_eq!(
            reports
                .iter()
                .filter(|report| matches!(
                    report,
                    ExecutionReport::Transition { workflow_event, .. }
                        if workflow_event["eventType"] == "workflow_state_changed"
                            && workflow_event["to"]["state"] == "succeeded"
                ))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn executes_outputless_agent_and_command_dag() {
        let fixture_argv = serde_json::to_string(&command_fixture_arguments()).unwrap();
        let workflow = format!(
            "schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: pi\n      config:\n        model: openai/gpt-5\n        thinking: high\nsteps:\n  agent:\n    kind: agent\n    agent:\n      profile: coding\n      systemPrompt: system.md\n      message:\n        text: [{{ file: system.md }}]\n  consume:\n    kind: cmd\n    dependsOn: [agent]\n    command:\n      argv: {fixture_argv}\n"
        );
        let reports = execute_fixture_workflow(&workflow, Some(SUCCESSFUL_PI), 1).await;
        assert_succeeded(&reports);
    }

    #[tokio::test]
    async fn claude_assignment_uses_its_startup_snapshot_after_path_changes() {
        let (temporary, mut manager) = manager_fixture_with_harnesses(
            CLAUDE_CODE_ONLY_WORKFLOW,
            None,
            Some(SUCCESSFUL_CLAUDE_CODE),
        );
        let changed_path = temporary.path().join("changed-path");
        fs::create_dir(&changed_path).unwrap();
        let decoy = changed_path.join("claude");
        fs::write(
            &decoy,
            "#!/bin/sh\nprintf decoy > \"${0%/*}/decoy.calls\"\nexit 99\n",
        )
        .unwrap();
        fs::set_permissions(&decoy, fs::Permissions::from_mode(0o700)).unwrap();
        let mut environment = manager.environment.variables().clone();
        environment.insert(OsString::from("PATH"), changed_path.into_os_string());
        manager.environment = EnvironmentSnapshot::new(environment);
        let reports = offer_and_execute(&mut manager).await;

        assert_workflow_succeeded(&reports);
        let call = only_harness_call(&temporary.path().join("claude.calls"));
        assert!(call.contains("--model fixture/claude"));
        assert!(!temporary.path().join("changed-path/decoy.calls").exists());
    }

    #[tokio::test]
    async fn mixed_assignment_invokes_each_snapshot_with_its_own_configuration() {
        let (temporary, mut manager) = manager_fixture_with_harnesses(
            MIXED_HARNESS_WORKFLOW,
            Some(SUCCESSFUL_PI),
            Some(SUCCESSFUL_CLAUDE_CODE),
        );
        let reports = offer_and_execute(&mut manager).await;

        assert_workflow_succeeded(&reports);
        let pi_call = only_harness_call(&temporary.path().join("pi.calls"));
        let claude_code_call = only_harness_call(&temporary.path().join("claude.calls"));
        assert!(pi_call.contains("--model fixture/pi"));
        assert!(!pi_call.contains("fixture/claude"));
        assert!(claude_code_call.contains("--model fixture/claude"));
        assert!(claude_code_call.contains("--effort xhigh"));
        assert!(!claude_code_call.contains("fixture/pi"));
    }

    #[tokio::test]
    async fn mixed_assignment_attributes_a_claude_failure_to_its_step() {
        let (temporary, mut manager) = manager_fixture_with_harnesses(
            MIXED_HARNESS_WORKFLOW,
            Some(SUCCESSFUL_PI),
            Some(SUCCESSFUL_CLAUDE_CODE),
        );
        let mut environment = manager.environment.variables().clone();
        environment.insert(OsString::from("CLAUDE_FIXTURE_FAIL"), OsString::from("1"));
        manager.environment = EnvironmentSnapshot::new(environment);
        let reports = offer_and_execute(&mut manager).await;

        let _ = only_harness_call(&temporary.path().join("pi.calls"));
        let _ = only_harness_call(&temporary.path().join("claude.calls"));
        assert!(reports.iter().any(|report| matches!(
            report,
            ExecutionReport::Transition { workflow_event, .. }
                if workflow_event["eventType"] == "step_state_changed"
                    && workflow_event["stepId"] == "claude"
                    && workflow_event["to"] == "failed"
                    && workflow_event["failure"]["cause"]
                        .as_str()
                        .is_some_and(|cause| cause.starts_with("agent_"))
        )));
        assert!(matches!(
            reports.last(),
            Some(ExecutionReport::Finished { outcome, .. })
                if outcome["outcome"] == "failed"
        ));
    }

    #[tokio::test]
    async fn outputless_finalizers_emit_roles_phase_and_authoritative_summary() {
        let successful_argv = serde_json::to_string(&command_fixture_arguments()).unwrap();
        let failing_argv = serde_json::to_string(&failing_command_fixture_arguments()).unwrap();
        let workflow = format!(
            "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: {successful_argv}\nfinalizers:\n  cleanup:\n    kind: cmd\n    inputs:\n      context:\n        ref: finalization.context\n    command:\n      argv: {successful_argv}\n  report:\n    kind: cmd\n    failurePolicy: advisory\n    command:\n      argv: {failing_argv}\n"
        );
        let reports = execute_fixture_workflow(&workflow, None, 3).await;

        assert!(reports.iter().any(|report| matches!(
            report,
            ExecutionReport::Transition { workflow_event, .. }
                if workflow_event["eventType"] == "workflow_state_changed"
                    && workflow_event["to"]["state"] == "finalizing"
                    && workflow_event["to"]["gate"] == "open"
        )));
        assert!(reports.iter().any(|report| matches!(
            report,
            ExecutionReport::Transition { workflow_event, .. }
                if workflow_event["eventType"] == "step_state_changed"
                    && workflow_event["stepId"] == "cleanup"
                    && workflow_event["role"] == "finalizer"
        )));
        assert!(matches!(
            reports.last(),
            Some(ExecutionReport::Finished { outcome, .. })
                if outcome["outcome"] == "succeeded"
                    && outcome["finalization"]["trigger"] == "succeeded"
                    && outcome["finalization"]["finalizers"].as_array().map(Vec::len) == Some(2)
                    && outcome["finalization"]["issues"][0]["node"]["id"] == "report"
                    && outcome["finalization"]["issues"][0]["impact"] == "advisory"
        ));
    }

    #[test]
    fn oversized_workflow_is_rejected_before_semantic_acceptance() {
        let mut workflow = String::from("schemaVersion: 1\nsteps:\n");
        for index in 0..257 {
            workflow.push_str(&format!(
                "  step{index}:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
            ));
        }
        let (_temporary, mut manager) = manager_fixture(&workflow);
        assert_offer_declined(
            &mut manager,
            offer("bg"),
            AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::WorkflowSourceInvalid,
            ),
        );
    }

    #[test]
    fn narrow_transition_reservation_uses_the_piecewise_node_bound() {
        let outbox = ObservationOutbox::new();
        for _ in 0..32 {
            outbox
                .enqueue(AssignmentObservation::Execution {
                    assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    report: ExecutionReport::Started,
                })
                .unwrap();
        }
        let ordinary_maximum = MAXIMUM_TRANSITIONS_PER_NODE * MAXIMUM_PARALLEL_STEPS + 3;
        let finalizer_maximum = MAXIMUM_TRANSITIONS_PER_NODE * MAXIMUM_PARALLEL_STEPS + 6;
        assert_eq!(ordinary_maximum, 1_027);
        assert_eq!(finalizer_maximum, 1_030);
        assert_eq!(
            outbox.reserve(MAXIMUM_PARALLEL_STEPS, 0),
            Ok(ordinary_maximum)
        );
        assert_eq!(
            outbox.reserve(MAXIMUM_PARALLEL_STEPS - 1, 1),
            Ok(finalizer_maximum)
        );
        assert!(outbox.lock().entries.capacity() >= finalizer_maximum + OBSERVATION_RESERVE_BASE);
        assert_eq!(
            outbox.reserve(MAXIMUM_PARALLEL_STEPS, 1),
            Err(environment_unavailable())
        );
        assert_eq!(outbox.len(), 32);
    }

    #[test]
    fn oversized_observation_is_rejected_before_queueing() {
        let outbox = ObservationOutbox::new();
        // This limit fixture deliberately keeps a complete schema-valid transition; sharing
        // the failure-mapper test's richer construction would obscure the size boundary.
        // jscpd:ignore-start
        assert_eq!(
            outbox.enqueue(AssignmentObservation::Execution {
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                report: ExecutionReport::Transition {
                    execution_event_sequence: 1,
                    workflow_event: json!({
                        "eventVersion": 1,
                        "eventType": "step_state_changed",
                        "transitionSequence": 1,
                        "stepId": "x".repeat(MAXIMUM_ENCODED_FRAME_BYTES),
                        "failurePolicy": "required",
                        "from": "pending",
                        "to": "starting",
                    }),
                },
            }),
            Err(OutboxFailure::Encoding)
        );
        // jscpd:ignore-end
        assert_eq!(outbox.len(), 0);
    }

    #[test]
    fn successor_fences_leave_room_for_more_than_256_completed_assignments() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let alphabet = b"0123456789abcdefghjkmnpqrstvwxyz";

        for index in 0..MAXIMUM_RETAINED_DECISIONS {
            let suffix = format!(
                "{}{}",
                char::from(alphabet[index / alphabet.len()]),
                char::from(alphabet[index % alphabet.len()])
            );
            manager.handle_offer(offer(&suffix)).unwrap();
            let acceptance_id = manager.pending_observations(&BTreeSet::new(), 10)[0].id;
            manager.mark_observation_encoded(acceptance_id);
            manager.finish_transport();
            let identity = match manager.slot.take().unwrap() {
                LocalSlot::Accepted(accepted) => accepted.identity.clone(),
                _ => panic!("offer must be accepted"),
            };
            let final_observation_id = enqueue_finished(&manager, &identity);
            manager.mark_observation_encoded(final_observation_id);
            manager.finish_transport();
            manager.slot = Some(LocalSlot::Finishing(FinishingAssignment {
                identity,
                final_observation_id,
                _retained_root: None,
            }));
        }

        assert_eq!(manager.handle_offer(offer("80")), Ok(()));
    }

    #[test]
    fn final_grace_acknowledgement_and_successor_fence_cleanup_state() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let predecessor = offer("bg");
        manager.handle_offer(predecessor.clone()).unwrap();
        let identity = match manager.slot.take().unwrap() {
            LocalSlot::Accepted(accepted) => accepted.identity.clone(),
            _ => panic!("offer must be accepted"),
        };
        let final_observation_id = enqueue_finished(&manager, &identity);
        manager.slot = Some(LocalSlot::Finishing(FinishingAssignment {
            identity: identity.clone(),
            final_observation_id,
            _retained_root: None,
        }));
        manager
            .event_sender
            .send(ManagerEvent::FinalGraceElapsed {
                assignment_id: identity.assignment_id.clone(),
                final_observation_id,
                continue_reporting: true,
            })
            .unwrap();
        manager.pending_observations(&BTreeSet::new(), 100);
        assert!(manager.slot.is_none());
        assert_eq!(manager.reporting, Some(identity.clone()));
        manager.acknowledge_observation(final_observation_id);
        assert!(manager.reporting.is_none());

        let final_observation_id = enqueue_finished(&manager, &identity);
        manager.mark_observation_encoded(final_observation_id);
        manager.slot = Some(LocalSlot::Finishing(FinishingAssignment {
            identity,
            final_observation_id,
            _retained_root: None,
        }));
        manager.handle_offer(offer("bh")).unwrap();
        assert_eq!(manager.outbox.len(), 2);
        assert_eq!(manager.pending_observations(&BTreeSet::new(), 100).len(), 1);
        manager.finish_transport();
        assert_eq!(manager.outbox.len(), 1);
    }
}
