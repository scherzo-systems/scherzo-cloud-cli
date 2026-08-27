use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
#[cfg(test)]
use std::future::{Future as _, poll_fn};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::watch;

use super::agent::{AgentCompatibilityProfile, AgentInvocationLimits, PositiveDuration};
use super::artifact::CaptureCancellation;
use super::cancellation::{MAXIMUM_CANCELLATION_GRACE, MINIMUM_CANCELLATION_GRACE};
use super::capacity::WorkflowCapacity;
use super::claude_code::ClaudeCodeConfig;
use super::claude_code_stream_json_v1::ClaudeCodeStreamJsonV1ProtocolLimits;
use super::codex::CodexConfig;
use super::codex_app_server_v1::CodexAppServerV1ProtocolLimits;
use super::execution_root::{AdmittedExecutionRoot, ExecutionRootAdmissionFailure};
use super::git_capture::{
    CloudGitCaptureProjection, GitCaptureContext, GitWorkspaceAdmissionFailure,
};
use super::pi::PiConfig;
use super::pi_json_v1::PiJsonV1ProtocolLimits;
use super::resolution::ResolvedWorkflow;
#[cfg(test)]
use super::test_support::SynchronousGate;
use super::validated::{ValidatedHarness, ValidatedRecoveryHandler, ValidatedStep};
use crate::execution::claude_code::{
    ClaudeCodeCompatibilityProfile, ValidatedClaudeCodeInstallation,
};
use crate::execution::codex::{CodexCompatibilityProfile, ValidatedCodexInstallation};
use crate::execution::pi::{PiCompatibilityProfile, ValidatedPiInstallation};

const MAXIMUM_CAPTURED_FILES: usize = 1024;
const MAXIMUM_CAPTURED_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TOTAL_CAPTURED_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_CAPTURED_GIT_CARRIERS: usize = 1024;
const MAXIMUM_CAPTURED_GIT_CARRIER_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TOTAL_CAPTURED_GIT_CARRIER_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_INPUT_VALUES: usize = 1024;
const MAXIMUM_INPUT_VALUE_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TOTAL_INPUT_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_LIVE_INPUT_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const MAXIMUM_AGENT_PROMPT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_AGENT_ATTACHMENTS: usize = 256;
const MAXIMUM_AGENT_ATTACHMENT_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const MAXIMUM_AGENT_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const MAXIMUM_AGENT_RESULT_BYTES: u64 = 8 * 1024 * 1024;
const MAXIMUM_AGENT_RESULT_REJECTION_FEEDBACK_BYTES: u64 = 8 * 1024;
const AGENT_RESULT_VALIDATION_DEADLINE: Duration = Duration::from_secs(60);
const AGENT_RESULT_SETTLEMENT_GRACE: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CancellationReason {
    UserRequest,
    TerminationRequest,
    CallerOutputFailure,
    RunnerShutdown,
    ExecutionLeaseExpired,
    FinalizationForceAbort,
}

impl CancellationReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::UserRequest => "user_request",
            Self::TerminationRequest => "termination_request",
            Self::CallerOutputFailure => "caller_output_failure",
            Self::RunnerShutdown => "runner_shutdown",
            Self::ExecutionLeaseExpired => "execution_lease_expired",
            Self::FinalizationForceAbort => "finalization_force_abort",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct CancellationOperationId(u64);

impl CancellationOperationId {
    pub(crate) const fn get(self) -> u64 {
        self.0
    }

    #[cfg(test)]
    pub(crate) const fn fixture(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CancellationOperation {
    Graceful {
        id: CancellationOperationId,
        reason: CancellationReason,
    },
    ForceAbort {
        id: CancellationOperationId,
    },
}

impl CancellationOperation {
    pub(crate) const fn id(self) -> CancellationOperationId {
        match self {
            Self::Graceful { id, .. } | Self::ForceAbort { id } => id,
        }
    }
}

#[derive(Debug)]
struct CancellationOperationState {
    operations: Vec<CancellationOperation>,
    next_id: u64,
    finalization_arming: bool,
    finalization_armed: bool,
    pending_finalization_reason: Option<CancellationReason>,
    pending_finalization_force_abort: bool,
    phase_reason: Option<CancellationReason>,
    force_abort_requested: bool,
}

impl Default for CancellationOperationState {
    fn default() -> Self {
        Self {
            operations: Vec::with_capacity(3),
            next_id: 1,
            finalization_arming: false,
            finalization_armed: false,
            pending_finalization_reason: None,
            pending_finalization_force_abort: false,
            phase_reason: None,
            force_abort_requested: false,
        }
    }
}

#[cfg(test)]
pub(super) type CancellationPendingPollBarrier = SynchronousGate;

#[derive(Clone, Debug)]
pub(crate) struct CancellationSource {
    reason: watch::Sender<Option<CancellationReason>>,
    operation_version: watch::Sender<u64>,
    operations: Arc<Mutex<CancellationOperationState>>,
    #[cfg(test)]
    pending_poll_barrier: Option<CancellationPendingPollBarrier>,
}

impl CancellationSource {
    pub(crate) fn new() -> Self {
        let (reason, _) = watch::channel(None);
        let (operation_version, _) = watch::channel(0);
        Self {
            reason,
            operation_version,
            operations: Arc::new(Mutex::new(CancellationOperationState::default())),
            #[cfg(test)]
            pending_poll_barrier: None,
        }
    }

    #[cfg(test)]
    pub(super) fn with_pending_poll_barrier(
        pending_poll_barrier: CancellationPendingPollBarrier,
    ) -> Self {
        let mut source = Self::new();
        source.pending_poll_barrier = Some(pending_poll_barrier);
        source
    }

    pub(crate) fn request_cancellation(&self, reason: CancellationReason) -> bool {
        if reason == CancellationReason::FinalizationForceAbort {
            return false;
        }
        let version = {
            let mut state = lock_cancellation_operations(&self.operations);
            if state.finalization_arming {
                if state.pending_finalization_reason.is_some()
                    || state.pending_finalization_force_abort
                {
                    return false;
                }
                state.pending_finalization_reason = Some(reason);
                None
            } else {
                if state.phase_reason.is_some() || state.force_abort_requested {
                    return false;
                }
                let id = CancellationOperationId(state.next_id);
                state.next_id = state.next_id.saturating_add(1);
                state.phase_reason = Some(reason);
                state
                    .operations
                    .push(CancellationOperation::Graceful { id, reason });
                Some(u64::try_from(state.operations.len()).unwrap_or(u64::MAX))
            }
        };
        if let Some(version) = version {
            self.reason.send_replace(Some(reason));
            self.operation_version.send_replace(version);
        }
        true
    }

    pub(crate) fn request_force_abort(&self) -> bool {
        let admission = {
            let mut state = lock_cancellation_operations(&self.operations);
            if state.finalization_arming {
                if state.pending_finalization_reason.is_none()
                    || state.pending_finalization_force_abort
                {
                    return false;
                }
                state.pending_finalization_force_abort = true;
                None
            } else {
                if !state.finalization_armed || state.force_abort_requested {
                    return false;
                }
                let id = CancellationOperationId(state.next_id);
                state.next_id = state.next_id.saturating_add(1);
                state.force_abort_requested = true;
                let closed_open_gate = state.phase_reason.is_none();
                if closed_open_gate {
                    state.phase_reason = Some(CancellationReason::FinalizationForceAbort);
                }
                state
                    .operations
                    .push(CancellationOperation::ForceAbort { id });
                Some((
                    u64::try_from(state.operations.len()).unwrap_or(u64::MAX),
                    closed_open_gate,
                ))
            }
        };
        if let Some((version, closed_open_gate)) = admission {
            if closed_open_gate {
                self.reason
                    .send_replace(Some(CancellationReason::FinalizationForceAbort));
            }
            self.operation_version.send_replace(version);
        }
        true
    }

    pub(crate) fn cancellation_reason(&self) -> Option<CancellationReason> {
        *self.reason.borrow()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancellation_reason().is_some()
    }

    pub(crate) async fn wait_for_cancellation(&self) -> CancellationReason {
        let mut subscription = self.subscribe();
        loop {
            if let Some(reason) = *subscription.borrow_and_update() {
                return reason;
            }
            let _ = subscription.changed().await;
        }
    }

    pub(super) fn subscribe(&self) -> CancellationSubscription {
        CancellationSubscription {
            receiver: self.reason.subscribe(),
            #[cfg(test)]
            pending_poll_barrier: self.pending_poll_barrier.clone(),
        }
    }

    pub(super) fn subscribe_operations(&self) -> CancellationOperationSubscription {
        CancellationOperationSubscription {
            source: self.clone(),
            receiver: self.operation_version.subscribe(),
            next_index: 0,
            #[cfg(test)]
            pending_poll_barrier: self.pending_poll_barrier.clone(),
        }
    }

    pub(super) fn begin_finalization_arm(&self) -> bool {
        let mut state = lock_cancellation_operations(&self.operations);
        if state.finalization_arming || state.finalization_armed {
            return false;
        }
        state.finalization_arming = true;
        true
    }

    #[cfg(test)]
    pub(crate) fn fixture_begin_finalization_arm(&self) -> bool {
        self.begin_finalization_arm()
    }

    pub(super) fn complete_finalization_arm(&self) -> bool {
        let (reason, version) = {
            let mut state = lock_cancellation_operations(&self.operations);
            if !state.finalization_arming || state.finalization_armed {
                return false;
            }
            state.finalization_arming = false;
            state.finalization_armed = true;
            state.phase_reason = None;
            state.force_abort_requested = false;
            let previous_operations = state.operations.len();
            let reason = state.pending_finalization_reason.take();
            if let Some(reason) = reason {
                let id = CancellationOperationId(state.next_id);
                state.next_id = state.next_id.saturating_add(1);
                state.phase_reason = Some(reason);
                state
                    .operations
                    .push(CancellationOperation::Graceful { id, reason });
            }
            if state.pending_finalization_force_abort {
                state.pending_finalization_force_abort = false;
                let id = CancellationOperationId(state.next_id);
                state.next_id = state.next_id.saturating_add(1);
                state.force_abort_requested = true;
                if state.phase_reason.is_none() {
                    state.phase_reason = Some(CancellationReason::FinalizationForceAbort);
                }
                state
                    .operations
                    .push(CancellationOperation::ForceAbort { id });
            }
            let version = (state.operations.len() > previous_operations)
                .then(|| u64::try_from(state.operations.len()).unwrap_or(u64::MAX));
            (state.phase_reason, version)
        };
        self.reason.send_replace(reason);
        if let Some(version) = version {
            self.operation_version.send_replace(version);
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn fixture_complete_finalization_arm(&self) -> bool {
        self.complete_finalization_arm()
    }

    pub(super) fn abort_finalization_arm(&self) -> bool {
        let mut state = lock_cancellation_operations(&self.operations);
        if !state.finalization_arming || state.finalization_armed {
            return false;
        }
        state.finalization_arming = false;
        state.pending_finalization_reason = None;
        state.pending_finalization_force_abort = false;
        true
    }
}

fn lock_cancellation_operations(
    operations: &Mutex<CancellationOperationState>,
) -> MutexGuard<'_, CancellationOperationState> {
    operations
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) struct CancellationSubscription {
    receiver: watch::Receiver<Option<CancellationReason>>,
    #[cfg(test)]
    pending_poll_barrier: Option<CancellationPendingPollBarrier>,
}

impl CancellationSubscription {
    pub(super) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        wait_for_watch_change(
            &mut self.receiver,
            #[cfg(test)]
            &mut self.pending_poll_barrier,
        )
        .await
    }

    pub(super) fn borrow_and_update(&mut self) -> watch::Ref<'_, Option<CancellationReason>> {
        self.receiver.borrow_and_update()
    }

    #[cfg(test)]
    pub(super) fn has_changed(&self) -> Result<bool, watch::error::RecvError> {
        self.receiver.has_changed()
    }
}

pub(super) struct CancellationOperationSubscription {
    source: CancellationSource,
    receiver: watch::Receiver<u64>,
    next_index: usize,
    #[cfg(test)]
    pending_poll_barrier: Option<CancellationPendingPollBarrier>,
}

impl CancellationOperationSubscription {
    pub(super) fn next_operation(&mut self) -> Option<CancellationOperation> {
        let operation = lock_cancellation_operations(&self.source.operations)
            .operations
            .get(self.next_index)
            .copied();
        if operation.is_some() {
            self.next_index = self.next_index.saturating_add(1);
            self.receiver.borrow_and_update();
        }
        operation
    }

    pub(super) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        if self.next_operation_available() {
            return Ok(());
        }
        wait_for_watch_change(
            &mut self.receiver,
            #[cfg(test)]
            &mut self.pending_poll_barrier,
        )
        .await
    }

    fn next_operation_available(&self) -> bool {
        lock_cancellation_operations(&self.source.operations)
            .operations
            .len()
            > self.next_index
    }
}

async fn wait_for_watch_change<T: Clone>(
    receiver: &mut watch::Receiver<T>,
    #[cfg(test)] pending_poll_barrier: &mut Option<CancellationPendingPollBarrier>,
) -> Result<(), watch::error::RecvError> {
    #[cfg(not(test))]
    {
        receiver.changed().await
    }
    #[cfg(test)]
    {
        poll_watch_change(receiver, pending_poll_barrier).await
    }
}

#[cfg(test)]
async fn poll_watch_change<T: Clone>(
    receiver: &mut watch::Receiver<T>,
    pending_poll_barrier: &mut Option<CancellationPendingPollBarrier>,
) -> Result<(), watch::error::RecvError> {
    let mut barrier = pending_poll_barrier.take();
    let changed = receiver.changed();
    tokio::pin!(changed);
    poll_fn(|context| {
        let result = changed.as_mut().poll(context);
        if result.is_pending()
            && let Some(barrier) = barrier.take()
        {
            barrier.block_until_resumed();
        }
        result
    })
    .await
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedAttachment {
    media_type: Arc<str>,
    bytes: Arc<[u8]>,
    diagnostic_source_name: Option<Arc<str>>,
}

impl ResolvedAttachment {
    pub(crate) fn new(media_type: Arc<str>, bytes: Arc<[u8]>) -> Self {
        Self {
            media_type,
            bytes,
            diagnostic_source_name: None,
        }
    }

    pub(crate) fn with_diagnostic_source_name(mut self, name: Arc<str>) -> Self {
        self.diagnostic_source_name = Some(name);
        self
    }

    pub(crate) fn media_type(&self) -> &str {
        &self.media_type
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn diagnostic_source_name(&self) -> Option<&str> {
        self.diagnostic_source_name.as_deref()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResolvedImports {
    prompt: Option<Arc<str>>,
    attachments: Arc<[ResolvedAttachment]>,
}

impl ResolvedImports {
    pub(crate) fn new(prompt: Option<Arc<str>>, attachments: Arc<[ResolvedAttachment]>) -> Self {
        Self {
            prompt,
            attachments,
        }
    }

    pub(crate) fn prompt(&self) -> Option<&str> {
        self.prompt.as_deref()
    }

    pub(crate) fn attachments(&self) -> &[ResolvedAttachment] {
        &self.attachments
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionRootLifecycle {
    CallerOwnedRetained,
    EngineOwnedRetained,
    EngineOwnedEphemeral,
}

#[derive(Clone, Debug)]
pub(crate) struct CancellationPolicy {
    source: CancellationSource,
    grace: Duration,
}

impl CancellationPolicy {
    pub(crate) fn new(source: CancellationSource, grace: Duration) -> Self {
        Self { source, grace }
    }

    pub(crate) fn source(&self) -> &CancellationSource {
        &self.source
    }

    pub(crate) fn grace(&self) -> Duration {
        self.grace
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct EnvironmentSnapshot {
    variables: Arc<BTreeMap<OsString, OsString>>,
}

impl EnvironmentSnapshot {
    pub(crate) fn new<I, Name, Value>(variables: I) -> Self
    where
        I: IntoIterator<Item = (Name, Value)>,
        Name: Into<OsString>,
        Value: Into<OsString>,
    {
        Self {
            variables: Arc::new(
                variables
                    .into_iter()
                    .map(|(name, value)| (name.into(), value.into()))
                    .collect(),
            ),
        }
    }

    pub(crate) fn variables(&self) -> &BTreeMap<OsString, OsString> {
        &self.variables
    }

    pub(crate) fn variable(&self, name: &OsStr) -> Option<&OsStr> {
        self.variables.get(name).map(OsString::as_os_str)
    }

    pub(super) fn with_variable(&self, name: OsString, value: OsString) -> Self {
        let mut variables = self.variables.as_ref().clone();
        variables.insert(name, value);
        Self {
            variables: Arc::new(variables),
        }
    }

    fn without_engine_reserved_variables(&self) -> Self {
        self.without_variables_matching(is_engine_reserved_environment_name)
    }

    pub(crate) fn without_managed_runner_credentials_and_helpers(&self) -> Self {
        self.without_variables_matching(is_managed_runner_private_environment_name)
    }

    fn without_variables_matching(&self, excluded: impl Fn(&OsStr) -> bool) -> Self {
        Self {
            variables: Arc::new(
                self.variables
                    .iter()
                    .filter(|(name, _)| !excluded(name))
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
            ),
        }
    }
}

fn is_engine_reserved_environment_name(name: &OsStr) -> bool {
    name.as_encoded_bytes().starts_with(b"SCHERZO_")
}

fn is_managed_runner_private_environment_name(name: &OsStr) -> bool {
    if is_engine_reserved_environment_name(name) {
        return true;
    }
    let name = name.as_encoded_bytes();
    matches!(
        name,
        b"GIT_ASKPASS"
            | b"GIT_ASKPASS_REQUIRE"
            | b"GIT_TERMINAL_PROMPT"
            | b"GIT_CONFIG"
            | b"GIT_CONFIG_COUNT"
            | b"GIT_CONFIG_PARAMETERS"
            | b"GIT_CONFIG_GLOBAL"
            | b"GIT_CONFIG_NOSYSTEM"
            | b"GIT_CONFIG_SYSTEM"
            | b"GIT_SSH"
            | b"GIT_SSH_COMMAND"
            | b"SSH_ASKPASS"
            | b"SSH_ASKPASS_REQUIRE"
            | b"SSH_AUTH_SOCK"
            | b"SSH_AGENT_PID"
            | b"GH_TOKEN"
            | b"GITHUB_TOKEN"
    ) || name.starts_with(b"GIT_CONFIG_KEY_")
        || name.starts_with(b"GIT_CONFIG_VALUE_")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CaptureLimits {
    maximum_files: usize,
    maximum_file_bytes: u64,
    maximum_total_bytes: u64,
    maximum_git_carriers: usize,
    maximum_git_carrier_bytes: u64,
    maximum_total_git_carrier_bytes: u64,
}

impl CaptureLimits {
    pub(crate) fn new(
        maximum_files: usize,
        maximum_file_bytes: u64,
        maximum_total_bytes: u64,
    ) -> Self {
        Self {
            maximum_files,
            maximum_file_bytes,
            maximum_total_bytes,
            maximum_git_carriers: MAXIMUM_CAPTURED_GIT_CARRIERS,
            maximum_git_carrier_bytes: MAXIMUM_CAPTURED_GIT_CARRIER_BYTES,
            maximum_total_git_carrier_bytes: MAXIMUM_TOTAL_CAPTURED_GIT_CARRIER_BYTES,
        }
    }

    pub(crate) fn with_git_carrier_limits(
        mut self,
        maximum_git_carriers: usize,
        maximum_git_carrier_bytes: u64,
        maximum_total_git_carrier_bytes: u64,
    ) -> Self {
        self.maximum_git_carriers = maximum_git_carriers;
        self.maximum_git_carrier_bytes = maximum_git_carrier_bytes;
        self.maximum_total_git_carrier_bytes = maximum_total_git_carrier_bytes;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InputLimits {
    maximum_values: usize,
    maximum_value_bytes: u64,
    maximum_total_bytes: u64,
    maximum_live_bytes: u64,
}

impl InputLimits {
    pub(crate) fn new(
        maximum_values: usize,
        maximum_value_bytes: u64,
        maximum_total_bytes: u64,
        maximum_live_bytes: u64,
    ) -> Self {
        Self {
            maximum_values,
            maximum_value_bytes,
            maximum_total_bytes,
            maximum_live_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionPolicyLimits {
    maximum_parallel_steps: usize,
    capture: CaptureLimits,
    input: InputLimits,
    maximum_step_log_bytes: u64,
}

impl ExecutionPolicyLimits {
    pub(crate) fn new(
        maximum_parallel_steps: usize,
        capture: CaptureLimits,
        input: InputLimits,
        maximum_step_log_bytes: u64,
    ) -> Self {
        Self {
            maximum_parallel_steps,
            capture,
            input,
            maximum_step_log_bytes,
        }
    }
}

pub(crate) fn default_execution_policy_limits(
    maximum_parallel_steps: usize,
) -> ExecutionPolicyLimits {
    ExecutionPolicyLimits::new(
        maximum_parallel_steps,
        CaptureLimits::new(
            MAXIMUM_CAPTURED_FILES,
            MAXIMUM_CAPTURED_FILE_BYTES,
            MAXIMUM_TOTAL_CAPTURED_BYTES,
        ),
        InputLimits::new(
            MAXIMUM_INPUT_VALUES,
            MAXIMUM_INPUT_VALUE_BYTES,
            MAXIMUM_TOTAL_INPUT_BYTES,
            MAXIMUM_LIVE_INPUT_BYTES,
        ),
        super::MAXIMUM_RETAINED_BYTES_PER_STREAM,
    )
}

#[derive(Clone, Debug)]
enum GitCaptureAdmission {
    None,
    Local,
    Cloud(CloudGitCaptureProjection),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowCapacityBudget {
    pub(crate) maximum_invocations: u64,
    pub(crate) diagnostic_retention_bytes: u64,
    pub(crate) native_session_retention_bytes: u64,
    pub(crate) aggregate_retention_bytes: u64,
    pub(crate) encoded_outbox_bytes: u64,
}

impl WorkflowCapacityBudget {
    pub(crate) const fn supported_maximum() -> Self {
        Self {
            maximum_invocations: 488,
            diagnostic_retention_bytes: 134_217_728,
            native_session_retention_bytes: 67_108_864,
            aggregate_retention_bytes: 201_326_592,
            encoded_outbox_bytes: 105_185_280,
        }
    }

    pub(crate) fn exact(capacity: &WorkflowCapacity) -> Self {
        let requirements = capacity.requirements;
        Self {
            maximum_invocations: requirements.maximum_invocations,
            diagnostic_retention_bytes: requirements.diagnostic_retention_bytes,
            native_session_retention_bytes: requirements.native_session_retention_bytes,
            aggregate_retention_bytes: requirements.aggregate_retention_bytes,
            encoded_outbox_bytes: requirements.encoded_outbox_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowExecutionContract {
    General,
    WorkflowV1InputlessCloudArtifactsV1,
}

impl WorkflowExecutionContract {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::General => "workflow_v1_general@1",
            Self::WorkflowV1InputlessCloudArtifactsV1 => "workflow_v1_inputless_cloud_artifacts@1",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AdmittedWorkflowCapacity {
    pub(crate) resolved: WorkflowCapacity,
    pub(crate) execution_contract: WorkflowExecutionContract,
    pub(crate) maximum_transitions: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ExecutionContext {
    root: PathBuf,
    root_lifecycle: ExecutionRootLifecycle,
    limits: ExecutionPolicyLimits,
    environment: EnvironmentSnapshot,
    cancellation: CancellationPolicy,
    pi_installation: Option<ValidatedPiInstallation>,
    claude_code_installation: Option<ValidatedClaudeCodeInstallation>,
    codex_installation: Option<ValidatedCodexInstallation>,
    git_capture: GitCaptureAdmission,
    capacity_budget: WorkflowCapacityBudget,
}

impl ExecutionContext {
    pub(crate) fn new(
        root: PathBuf,
        root_lifecycle: ExecutionRootLifecycle,
        limits: ExecutionPolicyLimits,
        environment: EnvironmentSnapshot,
        cancellation: CancellationPolicy,
    ) -> Self {
        Self {
            root,
            root_lifecycle,
            limits,
            environment,
            cancellation,
            pi_installation: None,
            claude_code_installation: None,
            codex_installation: None,
            git_capture: GitCaptureAdmission::None,
            capacity_budget: WorkflowCapacityBudget::supported_maximum(),
        }
    }

    pub(crate) fn with_local_git_capture(mut self) -> Self {
        self.git_capture = GitCaptureAdmission::Local;
        self
    }

    pub(crate) fn with_cloud_git_capture(mut self, projection: CloudGitCaptureProjection) -> Self {
        self.git_capture = GitCaptureAdmission::Cloud(projection);
        self
    }

    pub(crate) fn with_capacity_budget(mut self, budget: WorkflowCapacityBudget) -> Self {
        self.capacity_budget = budget;
        self
    }

    pub(crate) fn with_pi_installation(mut self, installation: ValidatedPiInstallation) -> Self {
        self.pi_installation = Some(installation);
        self
    }

    pub(crate) fn with_claude_code_installation(
        mut self,
        installation: ValidatedClaudeCodeInstallation,
    ) -> Self {
        self.claude_code_installation = Some(installation);
        self
    }

    pub(crate) fn with_codex_installation(
        mut self,
        installation: ValidatedCodexInstallation,
    ) -> Self {
        self.codex_installation = Some(installation);
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionLimits {
    maximum_parallel_steps: NonZeroUsize,
    maximum_captured_files: NonZeroUsize,
    maximum_captured_file_bytes: NonZeroU64,
    maximum_total_captured_bytes: NonZeroU64,
    maximum_captured_git_carriers: NonZeroUsize,
    maximum_captured_git_carrier_bytes: NonZeroU64,
    maximum_total_captured_git_carrier_bytes: NonZeroU64,
    maximum_input_values: NonZeroUsize,
    maximum_input_value_bytes: NonZeroU64,
    maximum_total_input_bytes: NonZeroU64,
    maximum_live_input_bytes: NonZeroU64,
    maximum_step_log_bytes: NonZeroU64,
}

impl ExecutionLimits {
    pub(crate) fn maximum_parallel_steps(self) -> NonZeroUsize {
        self.maximum_parallel_steps
    }

    pub(crate) fn maximum_captured_files(self) -> NonZeroUsize {
        self.maximum_captured_files
    }

    pub(crate) fn maximum_captured_file_bytes(self) -> NonZeroU64 {
        self.maximum_captured_file_bytes
    }

    pub(crate) fn maximum_total_captured_bytes(self) -> NonZeroU64 {
        self.maximum_total_captured_bytes
    }

    pub(crate) fn maximum_captured_git_carriers(self) -> NonZeroUsize {
        self.maximum_captured_git_carriers
    }

    pub(crate) fn maximum_captured_git_carrier_bytes(self) -> NonZeroU64 {
        self.maximum_captured_git_carrier_bytes
    }

    pub(crate) fn maximum_total_captured_git_carrier_bytes(self) -> NonZeroU64 {
        self.maximum_total_captured_git_carrier_bytes
    }

    pub(crate) fn maximum_input_values(self) -> NonZeroUsize {
        self.maximum_input_values
    }

    pub(crate) fn maximum_input_value_bytes(self) -> NonZeroU64 {
        self.maximum_input_value_bytes
    }

    pub(crate) fn maximum_total_input_bytes(self) -> NonZeroU64 {
        self.maximum_total_input_bytes
    }

    pub(crate) fn maximum_live_input_bytes(self) -> NonZeroU64 {
        self.maximum_live_input_bytes
    }

    pub(crate) fn maximum_step_log_bytes(self) -> NonZeroU64 {
        self.maximum_step_log_bytes
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AdmittedExecutionContext {
    root: AdmittedExecutionRoot,
    root_lifecycle: ExecutionRootLifecycle,
    limits: ExecutionLimits,
    environment: EnvironmentSnapshot,
    cancellation: CancellationPolicy,
}

impl AdmittedExecutionContext {
    pub(crate) fn root(&self) -> &Path {
        self.root.provenance_path()
    }

    pub(super) fn root_identity(&self) -> &AdmittedExecutionRoot {
        &self.root
    }

    pub(crate) fn root_lifecycle(&self) -> ExecutionRootLifecycle {
        self.root_lifecycle
    }

    pub(crate) fn limits(&self) -> ExecutionLimits {
        self.limits
    }

    pub(crate) fn environment(&self) -> &EnvironmentSnapshot {
        &self.environment
    }

    pub(crate) fn cancellation(&self) -> &CancellationPolicy {
        &self.cancellation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProjectTrustPolicy {
    InvocationScopedEnabled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PiJsonV1Admission {
    installation: Arc<ValidatedPiInstallation>,
    configuration: PiConfig,
    project_trust: ProjectTrustPolicy,
    limits: AgentInvocationLimits<PiJsonV1ProtocolLimits>,
}

impl PiJsonV1Admission {
    pub(crate) fn installation(&self) -> &ValidatedPiInstallation {
        &self.installation
    }

    pub(crate) fn configuration(&self) -> &PiConfig {
        &self.configuration
    }

    pub(crate) fn project_trust(&self) -> ProjectTrustPolicy {
        self.project_trust
    }

    pub(crate) fn limits(&self) -> &AgentInvocationLimits<PiJsonV1ProtocolLimits> {
        &self.limits
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClaudeCodeStreamJsonV1Admission {
    installation: Arc<ValidatedClaudeCodeInstallation>,
    configuration: ClaudeCodeConfig,
    limits: AgentInvocationLimits<ClaudeCodeStreamJsonV1ProtocolLimits>,
}

impl ClaudeCodeStreamJsonV1Admission {
    pub(crate) fn installation(&self) -> &ValidatedClaudeCodeInstallation {
        &self.installation
    }

    pub(crate) fn configuration(&self) -> &ClaudeCodeConfig {
        &self.configuration
    }

    pub(crate) fn limits(&self) -> &AgentInvocationLimits<ClaudeCodeStreamJsonV1ProtocolLimits> {
        &self.limits
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CodexAppServerV1Admission {
    installation: Arc<ValidatedCodexInstallation>,
    configuration: CodexConfig,
    limits: AgentInvocationLimits<CodexAppServerV1ProtocolLimits>,
}

impl CodexAppServerV1Admission {
    pub(crate) fn installation(&self) -> &ValidatedCodexInstallation {
        &self.installation
    }

    pub(crate) fn configuration(&self) -> &CodexConfig {
        &self.configuration
    }

    pub(crate) fn limits(&self) -> &AgentInvocationLimits<CodexAppServerV1ProtocolLimits> {
        &self.limits
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AdmittedHarness {
    Pi(PiJsonV1Admission),
    ClaudeCode(ClaudeCodeStreamJsonV1Admission),
    Codex(CodexAppServerV1Admission),
}

impl AdmittedHarness {
    pub(crate) const fn profile(&self) -> AgentCompatibilityProfile {
        match self {
            Self::Pi(_) => AgentCompatibilityProfile::PiJsonV1,
            Self::ClaudeCode(_) => AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
            Self::Codex(_) => AgentCompatibilityProfile::CodexAppServerV1,
        }
    }

    pub(crate) fn maximum_attachments(&self) -> NonZeroUsize {
        match self {
            Self::Pi(admission) => admission.limits().maximum_attachments(),
            Self::ClaudeCode(admission) => admission.limits().maximum_attachments(),
            Self::Codex(admission) => admission.limits().maximum_attachments(),
        }
    }

    pub(crate) fn maximum_attachment_bytes(&self) -> NonZeroU64 {
        match self {
            Self::Pi(admission) => admission.limits().maximum_attachment_bytes(),
            Self::ClaudeCode(admission) => admission.limits().maximum_attachment_bytes(),
            Self::Codex(admission) => admission.limits().maximum_attachment_bytes(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AdmittedWorkflow {
    workflow: Arc<ResolvedWorkflow>,
    imports: ResolvedImports,
    execution: AdmittedExecutionContext,
    agent_steps: Arc<BTreeMap<String, AdmittedHarness>>,
    recovery_handlers: Arc<BTreeMap<String, AdmittedHarness>>,
    capacity: AdmittedWorkflowCapacity,
    git_capture: Option<Arc<GitCaptureContext>>,
}

impl AdmittedWorkflow {
    pub(crate) fn workflow(&self) -> &ResolvedWorkflow {
        &self.workflow
    }

    pub(crate) fn imports(&self) -> &ResolvedImports {
        &self.imports
    }

    pub(crate) fn execution(&self) -> &AdmittedExecutionContext {
        &self.execution
    }

    pub(crate) fn agent_step(&self, step: &str) -> Option<&AdmittedHarness> {
        self.agent_steps.get(step)
    }

    pub(crate) fn agent_steps(&self) -> &BTreeMap<String, AdmittedHarness> {
        &self.agent_steps
    }

    pub(crate) fn recovery_handler(&self, step: &str) -> Option<&AdmittedHarness> {
        self.recovery_handlers.get(step)
    }

    pub(crate) fn recovery_handlers(&self) -> &BTreeMap<String, AdmittedHarness> {
        &self.recovery_handlers
    }

    pub(crate) fn capacity(&self) -> &AdmittedWorkflowCapacity {
        &self.capacity
    }

    pub(crate) fn has_recovery(&self) -> bool {
        self.workflow
            .definition
            .recoveries
            .values()
            .any(Option::is_some)
    }

    pub(crate) fn git_capture(&self) -> Option<&GitCaptureContext> {
        self.git_capture.as_deref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionFailureKind {
    MissingRequiredPrompt,
    InvalidAttachmentMediaType,
    AgentStepRuntimeUnsupported,
    ExecutionRootUnavailable,
    ExecutionRootNotDirectory,
    GitContextRequired,
    GitContextUnavailable,
    GitContextNotRepository,
    GitContextExecutionRootMismatch,
    GitObjectFormatUnsupported,
    GitBaselineUnavailable,
    GitInitialWorkspaceDirty,
    GitWorkflowDigestMismatch,
    NonPositiveParallelism,
    NonPositiveCapturedFiles,
    NonPositiveCapturedFileBytes,
    NonPositiveTotalCapturedBytes,
    NonPositiveCapturedGitCarriers,
    NonPositiveCapturedGitCarrierBytes,
    NonPositiveTotalCapturedGitCarrierBytes,
    NonPositiveInputValues,
    NonPositiveInputValueBytes,
    NonPositiveTotalInputBytes,
    NonPositiveLiveInputBytes,
    NonPositiveStepLogBytes,
    CancellationGraceTooShort,
    CancellationGraceTooLong,
    CapacitySourceBindingMismatch,
    InvocationCapacityUnavailable,
    DiagnosticRetentionCapacityUnavailable,
    NativeSessionRetentionCapacityUnavailable,
    AggregateRetentionCapacityUnavailable,
    EncodedOutboxCapacityUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionLocation {
    Workflow,
    PromptImport,
    AttachmentImport { index: usize },
    Step { step: String },
    RecoveryHandler { step: String },
    ExecutionRoot,
    GitContext,
    MaximumParallelSteps,
    MaximumCapturedFiles,
    MaximumCapturedFileBytes,
    MaximumTotalCapturedBytes,
    MaximumCapturedGitCarriers,
    MaximumCapturedGitCarrierBytes,
    MaximumTotalCapturedGitCarrierBytes,
    MaximumInputValues,
    MaximumInputValueBytes,
    MaximumTotalInputBytes,
    MaximumLiveInputBytes,
    MaximumStepLogBytes,
    CancellationPolicy,
    CapacitySourceBinding,
    MaximumInvocations,
    DiagnosticRetention,
    NativeSessionRetention,
    AggregateRetention,
    EncodedOutbox,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionFailure {
    kind: AdmissionFailureKind,
    location: AdmissionLocation,
}

impl AdmissionFailureKind {
    pub(crate) const fn is_execution_root_failure(self) -> bool {
        matches!(
            self,
            Self::ExecutionRootUnavailable | Self::ExecutionRootNotDirectory
        )
    }

    pub(crate) const fn is_projected_execution_limit_failure(self) -> bool {
        matches!(
            self,
            Self::NonPositiveParallelism
                | Self::CancellationGraceTooShort
                | Self::CancellationGraceTooLong
        )
    }
}

impl AdmissionFailure {
    pub(crate) fn kind(&self) -> AdmissionFailureKind {
        self.kind
    }

    pub(crate) fn location(&self) -> &AdmissionLocation {
        &self.location
    }

    fn new(kind: AdmissionFailureKind, location: AdmissionLocation) -> Self {
        Self { kind, location }
    }
}

impl fmt::Display for AdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "workflow admission failure at {:?}: {:?}",
            self.location, self.kind
        )
    }
}

impl std::error::Error for AdmissionFailure {}

pub(crate) fn admit_local_workflow(
    workflow: ResolvedWorkflow,
    imports: ResolvedImports,
    context: ExecutionContext,
) -> Result<AdmittedWorkflow, AdmissionFailure> {
    admit_workflow(workflow, imports, context)
}

pub(crate) fn admit_runner_workflow(
    workflow: ResolvedWorkflow,
    imports: ResolvedImports,
    context: ExecutionContext,
) -> Result<AdmittedWorkflow, AdmissionFailure> {
    admit_workflow_for(
        workflow,
        imports,
        context,
        WorkflowExecutionContract::WorkflowV1InputlessCloudArtifactsV1,
    )
}

pub(crate) fn admit_workflow(
    workflow: ResolvedWorkflow,
    imports: ResolvedImports,
    context: ExecutionContext,
) -> Result<AdmittedWorkflow, AdmissionFailure> {
    admit_workflow_for(
        workflow,
        imports,
        context,
        WorkflowExecutionContract::General,
    )
}

fn admit_workflow_for(
    workflow: ResolvedWorkflow,
    imports: ResolvedImports,
    context: ExecutionContext,
    execution_contract: WorkflowExecutionContract,
) -> Result<AdmittedWorkflow, AdmissionFailure> {
    let capacity = admit_capacity(&workflow, context.capacity_budget, execution_contract)?;
    if workflow.required_imports().prompt && imports.prompt().is_none() {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::MissingRequiredPrompt,
            AdmissionLocation::PromptImport,
        ));
    }

    if let Some((index, _)) = imports
        .attachments()
        .iter()
        .enumerate()
        .find(|(_, attachment)| !super::is_valid_media_type(attachment.media_type()))
    {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::InvalidAttachmentMediaType,
            AdmissionLocation::AttachmentImport { index },
        ));
    }

    let available_harnesses = AvailableHarnesses::new(
        context.pi_installation.as_ref(),
        context.claude_code_installation.as_ref(),
        context.codex_installation.as_ref(),
    );
    let agent_steps = admit_agent_steps(&workflow, &available_harnesses)?;
    let recovery_handlers = admit_recovery_handlers(&workflow, &available_harnesses)?;

    let maximum_parallel_steps = NonZeroUsize::new(context.limits.maximum_parallel_steps)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveParallelism,
                AdmissionLocation::MaximumParallelSteps,
            )
        })?;
    let maximum_captured_files = NonZeroUsize::new(context.limits.capture.maximum_files)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveCapturedFiles,
                AdmissionLocation::MaximumCapturedFiles,
            )
        })?;
    let maximum_captured_file_bytes = NonZeroU64::new(context.limits.capture.maximum_file_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveCapturedFileBytes,
                AdmissionLocation::MaximumCapturedFileBytes,
            )
        })?;
    let maximum_total_captured_bytes = NonZeroU64::new(context.limits.capture.maximum_total_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveTotalCapturedBytes,
                AdmissionLocation::MaximumTotalCapturedBytes,
            )
        })?;
    let maximum_captured_git_carriers =
        NonZeroUsize::new(context.limits.capture.maximum_git_carriers).ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveCapturedGitCarriers,
                AdmissionLocation::MaximumCapturedGitCarriers,
            )
        })?;
    let maximum_captured_git_carrier_bytes =
        NonZeroU64::new(context.limits.capture.maximum_git_carrier_bytes).ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveCapturedGitCarrierBytes,
                AdmissionLocation::MaximumCapturedGitCarrierBytes,
            )
        })?;
    let maximum_total_captured_git_carrier_bytes = NonZeroU64::new(
        context.limits.capture.maximum_total_git_carrier_bytes,
    )
    .ok_or_else(|| {
        AdmissionFailure::new(
            AdmissionFailureKind::NonPositiveTotalCapturedGitCarrierBytes,
            AdmissionLocation::MaximumTotalCapturedGitCarrierBytes,
        )
    })?;
    let maximum_input_values =
        NonZeroUsize::new(context.limits.input.maximum_values).ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveInputValues,
                AdmissionLocation::MaximumInputValues,
            )
        })?;
    let maximum_input_value_bytes = NonZeroU64::new(context.limits.input.maximum_value_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveInputValueBytes,
                AdmissionLocation::MaximumInputValueBytes,
            )
        })?;
    let maximum_total_input_bytes = NonZeroU64::new(context.limits.input.maximum_total_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveTotalInputBytes,
                AdmissionLocation::MaximumTotalInputBytes,
            )
        })?;
    let maximum_live_input_bytes = NonZeroU64::new(context.limits.input.maximum_live_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveLiveInputBytes,
                AdmissionLocation::MaximumLiveInputBytes,
            )
        })?;
    let configured_maximum_step_log_bytes = NonZeroU64::new(context.limits.maximum_step_log_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveStepLogBytes,
                AdmissionLocation::MaximumStepLogBytes,
            )
        })?;
    let maximum_step_log_bytes = NonZeroU64::new(
        configured_maximum_step_log_bytes.get().min(
            capacity
                .resolved
                .requirements
                .maximum_retained_bytes_per_invocation,
        ),
    )
    .ok_or_else(|| {
        AdmissionFailure::new(
            AdmissionFailureKind::NonPositiveStepLogBytes,
            AdmissionLocation::MaximumStepLogBytes,
        )
    })?;
    if context.cancellation.grace() < MINIMUM_CANCELLATION_GRACE {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::CancellationGraceTooShort,
            AdmissionLocation::CancellationPolicy,
        ));
    }
    if context.cancellation.grace() > MAXIMUM_CANCELLATION_GRACE {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::CancellationGraceTooLong,
            AdmissionLocation::CancellationPolicy,
        ));
    }

    let root = canonical_execution_root(&context.root)?;
    let execution = AdmittedExecutionContext {
        root,
        root_lifecycle: context.root_lifecycle,
        limits: ExecutionLimits {
            maximum_parallel_steps,
            maximum_captured_files,
            maximum_captured_file_bytes,
            maximum_total_captured_bytes,
            maximum_captured_git_carriers,
            maximum_captured_git_carrier_bytes,
            maximum_total_captured_git_carrier_bytes,
            maximum_input_values,
            maximum_input_value_bytes,
            maximum_total_input_bytes,
            maximum_live_input_bytes,
            maximum_step_log_bytes,
        },
        environment: context.environment.without_engine_reserved_variables(),
        cancellation: context.cancellation,
    };
    let git_capture = if workflow.requires_git_capture() {
        if let GitCaptureAdmission::Cloud(projection) = &context.git_capture
            && projection.workflow_digest() != workflow.content_digest.value
        {
            return Err(AdmissionFailure::new(
                AdmissionFailureKind::GitWorkflowDigestMismatch,
                AdmissionLocation::GitContext,
            ));
        }
        let capture = match &context.git_capture {
            GitCaptureAdmission::None => {
                return Err(AdmissionFailure::new(
                    AdmissionFailureKind::GitContextRequired,
                    AdmissionLocation::GitContext,
                ));
            }
            GitCaptureAdmission::Local => {
                GitCaptureContext::admit_local(&execution, &CaptureCancellation::default())
            }
            GitCaptureAdmission::Cloud(projection) => {
                GitCaptureContext::admit_cloud(&execution, projection)
            }
        }
        .map_err(git_admission_failure)?;
        Some(Arc::new(capture))
    } else {
        None
    };
    Ok(AdmittedWorkflow {
        workflow: Arc::new(workflow),
        imports,
        execution,
        agent_steps: Arc::new(agent_steps),
        recovery_handlers: Arc::new(recovery_handlers),
        capacity,
        git_capture,
    })
}

fn admit_capacity(
    workflow: &ResolvedWorkflow,
    budget: WorkflowCapacityBudget,
    execution_contract: WorkflowExecutionContract,
) -> Result<AdmittedWorkflowCapacity, AdmissionFailure> {
    if !workflow.capacity_is_bound_to_source_closure() {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::CapacitySourceBindingMismatch,
            AdmissionLocation::CapacitySourceBinding,
        ));
    }
    let requirements = workflow.capacity.requirements;
    for (available, required, kind, location) in [
        (
            budget.maximum_invocations,
            requirements.maximum_invocations,
            AdmissionFailureKind::InvocationCapacityUnavailable,
            AdmissionLocation::MaximumInvocations,
        ),
        (
            budget.diagnostic_retention_bytes,
            requirements.diagnostic_retention_bytes,
            AdmissionFailureKind::DiagnosticRetentionCapacityUnavailable,
            AdmissionLocation::DiagnosticRetention,
        ),
        (
            budget.native_session_retention_bytes,
            requirements.native_session_retention_bytes,
            AdmissionFailureKind::NativeSessionRetentionCapacityUnavailable,
            AdmissionLocation::NativeSessionRetention,
        ),
        (
            budget.aggregate_retention_bytes,
            requirements.aggregate_retention_bytes,
            AdmissionFailureKind::AggregateRetentionCapacityUnavailable,
            AdmissionLocation::AggregateRetention,
        ),
    ] {
        if available < required {
            return Err(AdmissionFailure::new(kind, location));
        }
    }
    if execution_contract == WorkflowExecutionContract::WorkflowV1InputlessCloudArtifactsV1
        && budget.encoded_outbox_bytes < requirements.encoded_outbox_bytes
    {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::EncodedOutboxCapacityUnavailable,
            AdmissionLocation::EncodedOutbox,
        ));
    }
    let maximum_transitions = match execution_contract {
        WorkflowExecutionContract::General => requirements.general_maximum_transitions,
        WorkflowExecutionContract::WorkflowV1InputlessCloudArtifactsV1 => {
            requirements.cloud_maximum_transitions
        }
    };
    Ok(AdmittedWorkflowCapacity {
        resolved: workflow.capacity.clone(),
        execution_contract,
        maximum_transitions,
    })
}

fn git_admission_failure(failure: GitWorkspaceAdmissionFailure) -> AdmissionFailure {
    let kind = match failure {
        GitWorkspaceAdmissionFailure::Cancelled
        | GitWorkspaceAdmissionFailure::GitUnavailable
        | GitWorkspaceAdmissionFailure::GitTimedOut
        | GitWorkspaceAdmissionFailure::GitOutputLimitExceeded => {
            AdmissionFailureKind::GitContextUnavailable
        }
        GitWorkspaceAdmissionFailure::NotWorkTree => AdmissionFailureKind::GitContextNotRepository,
        GitWorkspaceAdmissionFailure::ExecutionRootRebound
        | GitWorkspaceAdmissionFailure::ExecutionRootNotWorkTreeRoot => {
            AdmissionFailureKind::GitContextExecutionRootMismatch
        }
        GitWorkspaceAdmissionFailure::UnsupportedObjectFormat => {
            AdmissionFailureKind::GitObjectFormatUnsupported
        }
        GitWorkspaceAdmissionFailure::BaselineUnavailable => {
            AdmissionFailureKind::GitBaselineUnavailable
        }
        GitWorkspaceAdmissionFailure::InitialWorkspaceDirty => {
            AdmissionFailureKind::GitInitialWorkspaceDirty
        }
    };
    AdmissionFailure::new(kind, AdmissionLocation::GitContext)
}

struct AvailableHarnesses {
    pi: Option<Arc<ValidatedPiInstallation>>,
    claude_code: Option<Arc<ValidatedClaudeCodeInstallation>>,
    codex: Option<Arc<ValidatedCodexInstallation>>,
}

impl AvailableHarnesses {
    fn new(
        pi: Option<&ValidatedPiInstallation>,
        claude_code: Option<&ValidatedClaudeCodeInstallation>,
        codex: Option<&ValidatedCodexInstallation>,
    ) -> Self {
        Self {
            pi: pi.cloned().map(Arc::new),
            claude_code: claude_code.cloned().map(Arc::new),
            codex: codex.cloned().map(Arc::new),
        }
    }
}

fn admit_agent_steps(
    workflow: &ResolvedWorkflow,
    available: &AvailableHarnesses,
) -> Result<BTreeMap<String, AdmittedHarness>, AdmissionFailure> {
    let requests = workflow
        .definition
        .steps
        .iter()
        .chain(
            workflow
                .definition
                .finalizers
                .iter()
                .map(|(name, finalizer)| (name, &finalizer.body)),
        )
        .filter_map(|(step_name, step)| {
            let ValidatedStep::Agent(step) = step else {
                return None;
            };
            Some((
                step_name.clone(),
                &step.agent.harness,
                AdmissionLocation::Step {
                    step: step_name.clone(),
                },
            ))
        });
    admit_harness_requests(requests, available)
}

fn admit_recovery_handlers(
    workflow: &ResolvedWorkflow,
    available: &AvailableHarnesses,
) -> Result<BTreeMap<String, AdmittedHarness>, AdmissionFailure> {
    let requests = workflow
        .definition
        .recoveries
        .iter()
        .filter_map(|(step_name, recovery)| {
            let Some(super::validated::ValidatedStepRecovery {
                handler: Some(ValidatedRecoveryHandler::Agent { harness, .. }),
                ..
            }) = recovery
            else {
                return None;
            };
            Some((
                step_name.clone(),
                harness,
                AdmissionLocation::RecoveryHandler {
                    step: step_name.clone(),
                },
            ))
        });
    admit_harness_requests(requests, available)
}

fn admit_harness_requests<'a>(
    requests: impl IntoIterator<Item = (String, &'a ValidatedHarness, AdmissionLocation)>,
    available: &AvailableHarnesses,
) -> Result<BTreeMap<String, AdmittedHarness>, AdmissionFailure> {
    requests
        .into_iter()
        .map(|(name, harness, location)| {
            admit_harness(harness, available, location).map(|admitted| (name, admitted))
        })
        .collect()
}

fn admit_harness(
    harness: &ValidatedHarness,
    available: &AvailableHarnesses,
    location: AdmissionLocation,
) -> Result<AdmittedHarness, AdmissionFailure> {
    let missing_installation = || {
        AdmissionFailure::new(
            AdmissionFailureKind::AgentStepRuntimeUnsupported,
            location.clone(),
        )
    };
    match harness {
        ValidatedHarness::Pi(configuration) => {
            let installation = available.pi.as_ref().ok_or_else(missing_installation)?;
            let PiCompatibilityProfile::PiJsonV1 = installation.profile();
            Ok(AdmittedHarness::Pi(PiJsonV1Admission {
                installation: Arc::clone(installation),
                configuration: configuration.clone(),
                project_trust: ProjectTrustPolicy::InvocationScopedEnabled,
                limits: pi_json_v1_limits(),
            }))
        }
        ValidatedHarness::ClaudeCode(configuration) => {
            let installation = available
                .claude_code
                .as_ref()
                .ok_or_else(missing_installation)?;
            let ClaudeCodeCompatibilityProfile::ClaudeCodeStreamJsonV1 = installation.profile();
            Ok(AdmittedHarness::ClaudeCode(
                ClaudeCodeStreamJsonV1Admission {
                    installation: Arc::clone(installation),
                    configuration: configuration.clone(),
                    limits: claude_code_stream_json_v1_limits(),
                },
            ))
        }
        ValidatedHarness::Codex(configuration) => {
            let installation = available.codex.as_ref().ok_or_else(missing_installation)?;
            let CodexCompatibilityProfile::CodexAppServerV1 = installation.profile();
            Ok(AdmittedHarness::Codex(CodexAppServerV1Admission {
                installation: Arc::clone(installation),
                configuration: configuration.clone(),
                limits: codex_app_server_v1_limits(),
            }))
        }
    }
}

fn pi_json_v1_limits() -> AgentInvocationLimits<PiJsonV1ProtocolLimits> {
    agent_invocation_limits(PiJsonV1ProtocolLimits::profile())
}

fn claude_code_stream_json_v1_limits() -> AgentInvocationLimits<ClaudeCodeStreamJsonV1ProtocolLimits>
{
    agent_invocation_limits(ClaudeCodeStreamJsonV1ProtocolLimits::profile())
}

fn codex_app_server_v1_limits() -> AgentInvocationLimits<CodexAppServerV1ProtocolLimits> {
    agent_invocation_limits(CodexAppServerV1ProtocolLimits::profile())
}

fn agent_invocation_limits<ProtocolLimits>(
    protocol: ProtocolLimits,
) -> AgentInvocationLimits<ProtocolLimits> {
    AgentInvocationLimits::new(
        positive_u64(MAXIMUM_AGENT_PROMPT_BYTES),
        positive_u64(MAXIMUM_AGENT_PROMPT_BYTES),
        positive_usize(MAXIMUM_AGENT_ATTACHMENTS),
        positive_u64(MAXIMUM_AGENT_ATTACHMENT_BYTES),
        positive_u64(MAXIMUM_AGENT_RESPONSE_BYTES),
        positive_u64(MAXIMUM_AGENT_RESULT_BYTES),
        positive_u64(MAXIMUM_AGENT_RESULT_REJECTION_FEEDBACK_BYTES),
        positive_duration(AGENT_RESULT_VALIDATION_DEADLINE),
        positive_duration(AGENT_RESULT_SETTLEMENT_GRACE),
        protocol,
    )
}

fn positive_u64(value: u64) -> NonZeroU64 {
    let Some(value) = NonZeroU64::new(value) else {
        unreachable!("the fixed agent byte bounds are positive");
    };
    value
}

fn positive_usize(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        unreachable!("the fixed agent count bounds are positive");
    };
    value
}

fn positive_duration(value: Duration) -> PositiveDuration {
    let Some(value) = PositiveDuration::new(value) else {
        unreachable!("the fixed agent duration bounds are positive");
    };
    value
}

fn canonical_execution_root(root: &Path) -> Result<AdmittedExecutionRoot, AdmissionFailure> {
    AdmittedExecutionRoot::admit(root).map_err(|failure| {
        let kind = match failure {
            ExecutionRootAdmissionFailure::Unavailable => {
                AdmissionFailureKind::ExecutionRootUnavailable
            }
            ExecutionRootAdmissionFailure::NotDirectory => {
                AdmissionFailureKind::ExecutionRootNotDirectory
            }
        };
        AdmissionFailure::new(kind, AdmissionLocation::ExecutionRoot)
    })
}

#[cfg(test)]
mod tests;
