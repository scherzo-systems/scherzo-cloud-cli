use std::collections::VecDeque;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use super::config::{AssignmentConfig, Config};
use crate::execution::workflow::admission::{
    AdmissionFailure, AdmittedCommandWorkflow, CancellationPolicy, CancellationSource,
    EnvironmentSnapshot, ExecutionContext, ExecutionRootLifecycle, ResolvedImports,
    admit_command_workflow, default_execution_policy_limits,
};
use crate::execution::workflow::command_contract::{
    CommandWorkflowContractFailure, CommandWorkflowContractFailureKind,
    require_command_workflow_no_outputs,
};
use crate::execution::workflow::resolution;
use crate::runner_protocol::{
    AssignmentDecline, ExecutionLeasePolicy, ExecutionSpecInvalidReason,
    ExecutionSpecV1RunnerProjection, RunnerEnvelope, RunnerFrame, RunnerUnableReason,
};

const MAXIMUM_RETAINED_DECISIONS: usize = 256;
const MINIMUM_PARALLEL_STEPS: u64 = 1;
const MAXIMUM_PARALLEL_STEPS: u64 = 64;
const MINIMUM_CANCELLATION_GRACE_SECONDS: u64 = 1;
const MAXIMUM_CANCELLATION_GRACE_SECONDS: u64 = 10;
const MAXIMUM_CANCELLATION_GRACE_MILLISECONDS: u64 = MAXIMUM_CANCELLATION_GRACE_SECONDS * 1000;

pub(super) trait WallClockHealth: Send + Sync {
    fn uncertainty(&self) -> Result<Duration, WallClockHealthFailure>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WallClockHealthFailure;

pub(super) struct SystemWallClockHealth;

impl WallClockHealth for SystemWallClockHealth {
    fn uncertainty(&self) -> Result<Duration, WallClockHealthFailure> {
        system_wall_clock_uncertainty()
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
    pub(super) execution_spec: ExecutionSpecV1RunnerProjection,
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
    pending: bool,
}

enum LocalSlot {
    Preparing,
    Accepted(Box<AcceptedAssignment>),
}

struct AcceptedAssignment {
    assignment_id: String,
    run_id: String,
    _execution_root: TempDir,
    _admitted: AdmittedCommandWorkflow,
}

pub(super) struct AssignmentManager {
    config: AssignmentConfig,
    pi_installation: Option<crate::execution::pi::ValidatedPiInstallation>,
    boot_id: String,
    environment: EnvironmentSnapshot,
    wall_clock: Arc<dyn WallClockHealth>,
    lease_policy: Option<ExecutionLeasePolicy>,
    slot: Option<LocalSlot>,
    decisions: VecDeque<RetainedDecision>,
    pending_decisions: VecDeque<String>,
}

impl AssignmentManager {
    pub(super) fn new(
        config: &Config,
        boot_id: String,
        wall_clock: Arc<dyn WallClockHealth>,
    ) -> Self {
        Self {
            config: config.assignment().clone(),
            pi_installation: config.pi_installation().cloned(),
            boot_id,
            environment: EnvironmentSnapshot::new(std::env::vars_os()),
            wall_clock,
            lease_policy: None,
            slot: None,
            decisions: VecDeque::new(),
            pending_decisions: VecDeque::new(),
        }
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
        if let Some(index) = self
            .decisions
            .iter()
            .position(|decision| decision.offer.assignment_id == offer.assignment_id)
        {
            if !same_assignment(&self.decisions[index].offer, &offer) {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            }
            self.queue_replay(index);
            return Ok(());
        }
        if self.decisions.iter().any(|decision| {
            decision.offer.effect_id == offer.effect_id && !same_assignment(&decision.offer, &offer)
        }) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }

        if self.slot.is_some() {
            let response = rejected(&offer, AssignmentDecline::CapacityUnavailable);
            return self.retain_decision(offer, response);
        }

        self.slot = Some(LocalSlot::Preparing);
        let admission = self.admit(&offer);
        let response = match admission {
            Ok((execution_root, admitted)) => {
                self.slot = Some(LocalSlot::Accepted(Box::new(AcceptedAssignment {
                    assignment_id: offer.assignment_id.clone(),
                    run_id: offer.run_id.clone(),
                    _execution_root: execution_root,
                    _admitted: admitted,
                })));
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

    pub(super) fn handle_release(
        &mut self,
        assignment_id: &str,
        run_id: &str,
    ) -> Result<(), AssignmentManagerFailure> {
        let retained_conflict = self.decisions.iter().any(|decision| {
            decision.offer.assignment_id == assignment_id && decision.offer.run_id != run_id
        });
        if retained_conflict {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        let release = matches!(
            &self.slot,
            Some(LocalSlot::Accepted(accepted))
                if accepted.assignment_id == assignment_id && accepted.run_id == run_id
        );
        if release {
            self.slot = None;
        }
        Ok(())
    }

    pub(super) fn pending_decision(&self) -> Option<AssignmentDecision> {
        let assignment_id = self.pending_decisions.front()?;
        self.decisions
            .iter()
            .find(|decision| decision.pending && decision.offer.assignment_id == *assignment_id)
            .map(|decision| decision.response.clone())
    }

    pub(super) fn acknowledge_decision(&mut self, assignment_id: &str) {
        if self
            .pending_decisions
            .front()
            .is_some_and(|pending| pending == assignment_id)
        {
            self.pending_decisions.pop_front();
            if let Some(decision) = self
                .decisions
                .iter_mut()
                .find(|decision| decision.offer.assignment_id == assignment_id)
            {
                decision.pending = false;
            }
        }
    }

    fn queue_replay(&mut self, index: usize) {
        if !self.decisions[index].pending {
            self.decisions[index].pending = true;
            self.pending_decisions
                .push_back(self.decisions[index].offer.assignment_id.clone());
        }
    }

    fn retain_decision(
        &mut self,
        offer: AssignmentOffer,
        response: AssignmentDecision,
    ) -> Result<(), AssignmentManagerFailure> {
        if self.decisions.len() == MAXIMUM_RETAINED_DECISIONS {
            let active_assignment = match &self.slot {
                Some(LocalSlot::Accepted(accepted)) => Some(accepted.assignment_id.as_str()),
                Some(LocalSlot::Preparing) | None => None,
            };
            let Some(index) = self.decisions.iter().position(|decision| {
                !decision.pending
                    && active_assignment != Some(decision.offer.assignment_id.as_str())
            }) else {
                return Err(AssignmentManagerFailure::DecisionCapacity);
            };
            self.decisions.remove(index);
        }
        self.pending_decisions
            .push_back(offer.assignment_id.clone());
        self.decisions.push_back(RetainedDecision {
            offer,
            response,
            pending: true,
        });
        Ok(())
    }

    fn admit(
        &self,
        offer: &AssignmentOffer,
    ) -> Result<(TempDir, AdmittedCommandWorkflow), AssignmentDecline> {
        validate_execution_spec(&offer.execution_spec)?;
        self.validate_wall_clock()?;
        if offer.execution_spec.registered_workflow_id != self.config.workflow_id() {
            return Err(AssignmentDecline::RunnerUnable(
                RunnerUnableReason::WorkflowMappingUnavailable,
            ));
        }
        let execution_root = self.prepare_execution_root(&offer.assignment_id)?;
        let workflow = resolution::resolve(
            self.config.workflow_source_root(),
            self.config.workflow_path(),
        )
        .map_err(|_| {
            AssignmentDecline::RunnerUnable(RunnerUnableReason::WorkflowSourceUnavailable)
        })?;
        let workflow =
            require_command_workflow_no_outputs(workflow).map_err(command_contract_decline)?;
        let context = self.execution_context(&offer.execution_spec, execution_root.path())?;
        let admitted = admit_command_workflow(workflow, ResolvedImports::default(), context)
            .map_err(admission_decline)?;
        Ok((execution_root, admitted))
    }

    fn validate_wall_clock(&self) -> Result<(), AssignmentDecline> {
        let policy = self
            .lease_policy
            .as_ref()
            .ok_or(environment_unavailable())?;
        let uncertainty = self.wall_clock.uncertainty().map_err(|_| {
            AssignmentDecline::RunnerUnable(RunnerUnableReason::ExecutionEnvironmentUnavailable)
        })?;
        let ceiling = u64::try_from(policy.max_clock_uncertainty_milliseconds).map_err(|_| {
            AssignmentDecline::RunnerUnable(RunnerUnableReason::ExecutionEnvironmentUnavailable)
        })?;
        if uncertainty > Duration::from_millis(ceiling) {
            return Err(AssignmentDecline::RunnerUnable(
                RunnerUnableReason::ExecutionEnvironmentUnavailable,
            ));
        }
        Ok(())
    }

    fn prepare_execution_root(&self, assignment_id: &str) -> Result<TempDir, AssignmentDecline> {
        let boot_root = self.config.work_root().join(&self.boot_id);
        fs::create_dir_all(&boot_root).map_err(|_| environment_unavailable())?;
        let canonical_boot_root =
            fs::canonicalize(&boot_root).map_err(|_| environment_unavailable())?;
        if canonical_boot_root.parent() != Some(self.config.work_root()) {
            return Err(environment_unavailable());
        }
        tempfile::Builder::new()
            .prefix(&format!("{assignment_id}-"))
            .tempdir_in(&canonical_boot_root)
            .map_err(|_| environment_unavailable())
    }

    fn execution_context(
        &self,
        execution_spec: &ExecutionSpecV1RunnerProjection,
        root: &Path,
    ) -> Result<ExecutionContext, AssignmentDecline> {
        let maximum_parallel_steps =
            usize::try_from(execution_spec.execution_limits.maximum_parallel_steps)
                .map_err(|_| invalid_execution_limits())?;
        let cancellation_grace =
            Duration::from_secs(execution_spec.execution_limits.cancellation_grace_seconds);
        let context = ExecutionContext::new(
            root.to_owned(),
            ExecutionRootLifecycle::EngineOwnedEphemeral,
            default_execution_policy_limits(maximum_parallel_steps),
            self.environment.clone(),
            CancellationPolicy::new(CancellationSource::new(), cancellation_grace),
        );
        Ok(match &self.pi_installation {
            Some(installation) => context.with_pi_installation(installation.clone()),
            None => context,
        })
    }

    #[cfg(test)]
    fn active_step_count(&self) -> Option<usize> {
        match &self.slot {
            Some(LocalSlot::Accepted(accepted)) => {
                Some(accepted._admitted.workflow().definition.steps.len())
            }
            Some(LocalSlot::Preparing) | None => None,
        }
    }
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
    for delivery_budget in [start_delivery, renewal_delivery] {
        let lease_required = policy
            .fencing_margin_milliseconds
            .checked_add(MAXIMUM_CANCELLATION_GRACE_MILLISECONDS)
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

fn command_contract_decline(failure: CommandWorkflowContractFailure) -> AssignmentDecline {
    let reason = match failure.kind() {
        CommandWorkflowContractFailureKind::InvalidStepCount => {
            RunnerUnableReason::WorkflowSourceUnavailable
        }
        CommandWorkflowContractFailureKind::AgentStep
        | CommandWorkflowContractFailureKind::DeclaredOutput
        | CommandWorkflowContractFailureKind::DeclaredExport => {
            RunnerUnableReason::WorkflowContractInvalid
        }
    };
    AssignmentDecline::RunnerUnable(reason)
}

fn validate_execution_spec(
    execution_spec: &ExecutionSpecV1RunnerProjection,
) -> Result<(), AssignmentDecline> {
    if execution_spec.schema_version != 1 {
        return Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::UnsupportedSchemaVersion,
        ));
    }
    if !(MINIMUM_PARALLEL_STEPS..=MAXIMUM_PARALLEL_STEPS)
        .contains(&execution_spec.execution_limits.maximum_parallel_steps)
        || !(MINIMUM_CANCELLATION_GRACE_SECONDS..=MAXIMUM_CANCELLATION_GRACE_SECONDS)
            .contains(&execution_spec.execution_limits.cancellation_grace_seconds)
    {
        return Err(invalid_execution_limits());
    }
    Ok(())
}

fn invalid_execution_limits() -> AssignmentDecline {
    AssignmentDecline::ExecutionSpecInvalid(ExecutionSpecInvalidReason::InvalidExecutionLimits)
}

fn admission_decline(failure: AdmissionFailure) -> AssignmentDecline {
    let kind = failure.kind();
    if kind.is_execution_root_failure() {
        environment_unavailable()
    } else if kind.is_projected_execution_limit_failure() {
        invalid_execution_limits()
    } else {
        AssignmentDecline::RunnerUnable(RunnerUnableReason::WorkflowAdmissionRejected)
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
        && left.execution_spec == right.execution_spec
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::sync::Mutex;

    use super::*;
    use crate::runner::credential::test_credential;
    use crate::runner::service::config::Config;
    use crate::runner_protocol::ExecutionLimitsV1RunnerProjection;

    const WORKFLOW_ID: &str = "wfl_01k0z6r1w8f4jy2m7q9v3x5abr";

    struct FixedWallClockHealth {
        result: Mutex<Result<Duration, WallClockHealthFailure>>,
    }

    impl WallClockHealth for FixedWallClockHealth {
        fn uncertainty(&self) -> Result<Duration, WallClockHealthFailure> {
            *self.result.lock().unwrap()
        }
    }

    fn wall_clock(uncertainty: Duration) -> Arc<dyn WallClockHealth> {
        Arc::new(FixedWallClockHealth {
            result: Mutex::new(Ok(uncertainty)),
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
            lease_duration_milliseconds: 30_000,
            fencing_margin_milliseconds: 11_000,
        }
    }

    fn offer(assignment_suffix: &str) -> AssignmentOffer {
        AssignmentOffer {
            effect_id: format!("eff_01k0z6r1w8f4jy2m7q9v3x5a{assignment_suffix}"),
            assignment_id: format!("asn_01k0z6r1w8f4jy2m7q9v3x5a{assignment_suffix}"),
            run_id: format!("run_01k0z6r1w8f4jy2m7q9v3x5a{assignment_suffix}"),
            execution_spec: ExecutionSpecV1RunnerProjection {
                execution_spec_id: format!("xsp_01k0z6r1w8f4jy2m7q9v3x5a{assignment_suffix}"),
                schema_version: 1,
                registered_workflow_id: WORKFLOW_ID.to_owned(),
                execution_limits: ExecutionLimitsV1RunnerProjection {
                    maximum_parallel_steps: 1,
                    cancellation_grace_seconds: 1,
                },
            },
        }
    }

    fn manager_fixture(
        workflow: &str,
        uncertainty: Duration,
    ) -> (tempfile::TempDir, AssignmentManager) {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        let work = temporary.path().join("work");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&work).unwrap();
        fs::write(source.join("workflow.yaml"), workflow).unwrap();
        let assignment = AssignmentConfig::new(
            WORKFLOW_ID.to_owned(),
            &source,
            Path::new("workflow.yaml"),
            &work,
        )
        .unwrap();
        let config = Config::new(
            "wss://gateway.example.test/v1/connect",
            test_credential(),
            false,
            assignment,
        )
        .unwrap();
        let mut manager = AssignmentManager::new(
            &config,
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
            wall_clock(uncertainty),
        );
        manager.retain_lease_policy(&policy()).unwrap();
        (temporary, manager)
    }

    fn command_workflow() -> &'static str {
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
    }

    #[test]
    fn accepts_once_and_replays_the_exact_decision_without_another_slot() {
        let (temporary, mut manager) = manager_fixture(command_workflow(), Duration::from_secs(1));
        let offered = offer("bg");
        manager.handle_offer(offered.clone()).unwrap();
        let first = manager.pending_decision().unwrap();
        assert!(matches!(first, AssignmentDecision::Accepted { .. }));
        assert_eq!(manager.active_step_count(), Some(1));
        let boot_root = temporary.path().join("work/rbt_01k0z6r1w8f4jy2m7q9v3x5abe");
        assert_eq!(fs::read_dir(&boot_root).unwrap().count(), 1);

        manager.acknowledge_decision(first.assignment_id());
        manager.handle_offer(offered).unwrap();
        assert_eq!(manager.pending_decision(), Some(first));
        assert_eq!(fs::read_dir(&boot_root).unwrap().count(), 1);

        manager.handle_offer(offer("bh")).unwrap();
        assert!(matches!(
            manager.decisions.back().map(|decision| &decision.response),
            Some(AssignmentDecision::Rejected {
                decline: AssignmentDecline::CapacityUnavailable,
                ..
            })
        ));
        assert_eq!(fs::read_dir(&boot_root).unwrap().count(), 1);
    }

    #[test]
    fn maps_spec_clock_mapping_source_contract_and_admission_failures() {
        let cases = [
            (
                command_workflow(),
                Duration::from_secs(1),
                Some(("schema", 2_u64)),
                AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::UnsupportedSchemaVersion,
                ),
            ),
            (
                command_workflow(),
                Duration::from_millis(1001),
                None,
                environment_unavailable(),
            ),
            (
                command_workflow(),
                Duration::from_secs(1),
                Some(("mapping", 0)),
                AssignmentDecline::RunnerUnable(RunnerUnableReason::WorkflowMappingUnavailable),
            ),
            (
                "not: [valid",
                Duration::from_secs(1),
                None,
                AssignmentDecline::RunnerUnable(RunnerUnableReason::WorkflowSourceUnavailable),
            ),
            (
                "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      result:\n        kind: file\n        path: result.txt\n        mediaType: text/plain\n",
                Duration::from_secs(1),
                None,
                AssignmentDecline::RunnerUnable(RunnerUnableReason::WorkflowContractInvalid),
            ),
            (
                "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    inputs:\n      prompt:\n        ref: imports.prompt\n    command:\n      argv: [\"true\"]\n",
                Duration::from_secs(1),
                None,
                AssignmentDecline::RunnerUnable(RunnerUnableReason::WorkflowAdmissionRejected),
            ),
        ];
        for (source, uncertainty, mutation, expected) in cases {
            let (_temporary, mut manager) = manager_fixture(source, uncertainty);
            let mut offered = offer("bg");
            match mutation {
                Some(("schema", value)) => offered.execution_spec.schema_version = value,
                Some(("mapping", _)) => {
                    offered.execution_spec.registered_workflow_id =
                        "wfl_01k0z6r1w8f4jy2m7q9v3x5abs".to_owned();
                }
                None => {}
                Some(_) => unreachable!(),
            }
            manager.handle_offer(offered).unwrap();
            assert!(matches!(
                manager.pending_decision(),
                Some(AssignmentDecision::Rejected { decline, .. }) if decline == expected
            ));
            assert!(manager.slot.is_none());
        }
    }

    #[test]
    fn classifies_oversized_workflow_as_source_validation_failure() {
        let mut source = String::from("schemaVersion: 1\nsteps:\n");
        for index in 0..=256 {
            source.push_str(&format!(
                "  step{index}:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
            ));
        }
        let (_temporary, mut manager) = manager_fixture(&source, Duration::ZERO);

        manager.handle_offer(offer("bg")).unwrap();

        let Some(AssignmentDecision::Rejected { decline, .. }) = manager.pending_decision() else {
            panic!("oversized workflow did not produce a rejection");
        };
        assert_eq!(
            decline,
            AssignmentDecline::RunnerUnable(RunnerUnableReason::WorkflowSourceUnavailable)
        );
    }

    #[test]
    fn maps_execution_root_preparation_failure_without_exposing_the_path() {
        let (temporary, mut manager) = manager_fixture(command_workflow(), Duration::ZERO);
        let work_root = temporary.path().join("work");
        fs::remove_dir(&work_root).unwrap();
        fs::write(&work_root, "not a directory").unwrap();

        manager.handle_offer(offer("bg")).unwrap();

        assert!(matches!(
            manager.pending_decision(),
            Some(AssignmentDecision::Rejected { decline, .. })
                if decline == environment_unavailable()
        ));
        assert!(manager.slot.is_none());
    }

    #[test]
    fn validates_cloud_execution_limits_before_context_construction() {
        let base = offer("bg").execution_spec;
        for (parallelism, grace) in [(1, 1), (64, 10)] {
            let mut spec = base.clone();
            spec.execution_limits.maximum_parallel_steps = parallelism;
            spec.execution_limits.cancellation_grace_seconds = grace;
            assert_eq!(validate_execution_spec(&spec), Ok(()));
        }
        for (parallelism, grace) in [(0, 1), (65, 1), (1, 0), (1, 11)] {
            let mut spec = base.clone();
            spec.execution_limits.maximum_parallel_steps = parallelism;
            spec.execution_limits.cancellation_grace_seconds = grace;
            assert_eq!(
                validate_execution_spec(&spec),
                Err(invalid_execution_limits())
            );
        }
    }

    #[test]
    fn validates_and_immutably_retains_the_welcomed_lease_policy() {
        let (_temporary, mut manager) = manager_fixture(command_workflow(), Duration::ZERO);
        assert_eq!(manager.retain_lease_policy(&policy()), Ok(()));

        let mut changed = policy();
        changed.lease_duration_milliseconds += 1;
        assert_eq!(
            manager.retain_lease_policy(&changed),
            Err(WelcomePolicyFailure::Changed)
        );

        let mut invalid = policy();
        invalid.fencing_margin_milliseconds = 10_999;
        let (_temporary, mut fresh) = manager_fixture(command_workflow(), Duration::ZERO);
        fresh.lease_policy = None;
        assert_eq!(
            fresh.retain_lease_policy(&invalid),
            Err(WelcomePolicyFailure::Invalid)
        );
    }
}
