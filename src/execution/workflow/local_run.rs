use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsString;
use std::fmt;
use std::fs::File;
use std::future::{Future, ready};
use std::io::{self, Read, Write};
use std::os::fd::{AsFd as _, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use ring::digest::{SHA256, digest};
use rustix::fs::{
    AtFlags, FileType, FlockOperation, Mode, OFlags, RenameFlags, fchmod, fcntl_lock, fstat,
    mkdirat, openat, renameat_with, statat, unlinkat,
};
use rustix::io::{Errno, dup};
use rustix::process::{Flock, FlockOffsetType, FlockType, fcntl_getlk};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::admission::{AdmittedWorkflow, ResolvedAttachment, ResolvedImports};
use super::agent_diagnostics::AgentDiagnosticSessionStore;
use super::cancellation::MAXIMUM_CANCELLATION_GRACE;
use super::coordinator::{CommitPort, CommittedActionKind, CommittedReduction};
use super::document::FailurePolicy;
use super::execution_root::AdmittedExecutionRoot;
use super::private_staging::{
    create_staging_root, directory_entry_names, open_directory_path, remove_staging_root, same_file,
};
use super::process_group::{
    AuthenticatedProcessGroup, AuthenticatedSignalResult, DurableProcessGuardStore,
    ProcessGuardRegistry, ProcessIdentityObservation, system_process_identity_observation,
    terminate_authenticated_process_group,
};
use super::publication::{
    CancellationReasonV1, FailureV1, FinalizationTriggerV1, StepReasonV1, cancellation_reason,
    cancellation_step_reason, failure_v1, finalization_trigger,
};
use super::resolution::{ResolvedWorkflow, resolve_retained};
use super::runtime::{FinalizationSummary, StepState, WorkflowState};
use super::schema_common::{
    is_canonical_absolute_path, is_canonical_relative_path, is_lowercase_hex, lowercase_hex,
    utc_timestamp,
};
use super::step_runtime::StepFailureCause;

const RUN_FILE: &str = "run.json";
const STATE_FILE: &str = "state.json";
const LOCK_FILE: &str = "run.lock";
const WORKFLOW_DIRECTORY: &str = "workflow";
const WORKFLOW_FILES_DIRECTORY: &str = "files";
const WORKFLOW_MANIFEST_FILE: &str = "manifest.json";
const ATTEMPTS_DIRECTORY: &str = "attempts";
const PRIVATE_DIRECTORY: &str = ".private";
const INITIAL_ATTEMPT_NUMBER: u64 = 1;
const INITIAL_ATTEMPT_DIRECTORY: &str = "000001";
const STAGING_ATTEMPTS: usize = 16;
const PRIVATE_STAGING_ATTEMPTS: usize = 16;
const STATUS_SNAPSHOT_ATTEMPTS: usize = 8;
pub(super) const MAXIMUM_DURABLE_JSON_BYTES: u64 = 4 * 1024 * 1024;
const MAXIMUM_RETAINED_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_RETAINED_SOURCE_CLOSURE_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_RETAINED_PROMPT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_RETAINED_ATTACHMENT_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_RETAINED_CAPTURED_FILE_BYTES: u64 = MAXIMUM_RETAINED_SOURCE_CLOSURE_BYTES
    + MAXIMUM_RETAINED_PROMPT_BYTES
    + MAXIMUM_RETAINED_ATTACHMENT_BYTES;
const MAXIMUM_RETAINED_RUN_JSON_BYTES: u64 =
    3 * MAXIMUM_DURABLE_JSON_BYTES + super::result_metadata::MAXIMUM_RESULT_NON_STREAM_JSON_BYTES;
// An archived-attempt read covers the immutable workflow/import captures, the base64
// representation of both bounded diagnostic streams, and the run, state, manifest,
// and result JSON envelopes. Each term is independently enforced while it is read.
const MAXIMUM_RETAINED_TOTAL_BYTES: u64 = MAXIMUM_RETAINED_CAPTURED_FILE_BYTES
    + super::result_metadata::MAXIMUM_ENCODED_RETAINED_STREAM_BYTES
    + MAXIMUM_RETAINED_RUN_JSON_BYTES;
const MAXIMUM_DIAGNOSTICS: usize = 256;
const QUIESCENCE_POLL_INTERVAL: Duration = Duration::from_millis(5);
const QUIESCENCE_POLL_ATTEMPTS: usize =
    (MAXIMUM_CANCELLATION_GRACE.as_millis() / QUIESCENCE_POLL_INTERVAL.as_millis()) as usize;
const SHA256_ALGORITHM: &str = "sha256";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalRunDirectoryError {
    InvalidPath,
    ParentUnavailable,
    DestinationExists,
    ExecutionRootOverlap,
    StagingUnavailable,
    LockUnavailable,
    IdentityUnavailable,
    HostIdentityUnavailable,
    SerializationUnavailable,
    StateInvalid,
    StateConflict,
    StateWriteUnavailable,
    AtomicCommitUnavailable,
    PublicationUnavailable,
}

impl fmt::Display for LocalRunDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "local run directory failure: {self:?}")
    }
}

impl std::error::Error for LocalRunDirectoryError {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DigestV1 {
    pub(super) algorithm: String,
    pub(super) value: String,
}

impl DigestV1 {
    fn sha256(bytes: &[u8]) -> Self {
        Self {
            algorithm: SHA256_ALGORITHM.to_owned(),
            value: lowercase_hex(digest(&SHA256, bytes).as_ref()),
        }
    }

    fn validate(&self) -> bool {
        self.algorithm == SHA256_ALGORITHM && is_lowercase_hex(&self.value, 64)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct LocalRunV1 {
    pub(super) schema_version: u8,
    pub(super) local_run_id: String,
    pub(super) created_at: String,
    pub(super) workflow_digest: DigestV1,
    pub(super) workflow_manifest_digest: DigestV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkflowManifestV1 {
    schema_version: u8,
    workflow_path: String,
    source_root: String,
    maximum_parallel_steps: usize,
    source_files: Vec<ManifestSourceFileV1>,
    imports: ManifestImportsV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestSourceFileV1 {
    path: String,
    #[serde(flatten)]
    file: ManifestFileV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestFileV1 {
    ordinal: u64,
    relative_file: String,
    size_bytes: u64,
    digest: DigestV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestImportsV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt: Option<ManifestFileV1>,
    attachments: Vec<ManifestAttachmentV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestAttachmentV1 {
    media_type: String,
    #[serde(flatten)]
    file: ManifestFileV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct LocalRunStateV1 {
    pub(super) schema_version: u8,
    pub(super) local_run_id: String,
    pub(super) revision: u64,
    pub(super) current_attempt_number: u64,
    pub(super) attempts: Vec<LocalAttemptV1>,
    diagnostics: Vec<LocalDiagnosticV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct LocalAttemptV1 {
    attempt_id: String,
    pub(super) attempt_number: u64,
    pub(super) trigger: AttemptTriggerV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) prior_attempt_number: Option<u64>,
    pub(super) state: AttemptStateV1,
    pub(super) execution_root: String,
    pub(super) created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) settled_at: Option<String>,
    owner: AttemptOwnerV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cancellation: Option<AttemptCancellationV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    interruption: Option<AttemptInterruptionV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rejection: Option<AttemptRejectionV1>,
    pub(super) progress: AttemptProgressV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) finalization: Option<AttemptFinalizationV1>,
    process_guards: Vec<ProcessGuardV1>,
    pub(super) result: AttemptResultV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum AttemptTriggerV1 {
    Initial,
    ExplicitRetry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum AttemptStateV1 {
    Created,
    Running,
    Cancelling,
    Succeeded,
    WorkflowFailed,
    Cancelled,
    Interrupted,
    Rejected,
}

impl AttemptStateV1 {
    fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::WorkflowFailed
                | Self::Cancelled
                | Self::Interrupted
                | Self::Rejected
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AttemptOwnerV1 {
    owner_nonce: String,
    execution_host: ExecutionHostV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExecutionHostV1 {
    kind: ExecutionHostKindV1,
    value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionHostKindV1 {
    HostBoot,
    IsolationInstance,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AttemptCancellationV1 {
    pub(super) reason: CancellationReasonV1,
    pub(super) requested_at: String,
    pub(super) force_stop_deadline: String,
    workflow_confirmed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AttemptInterruptionV1 {
    cause: InterruptionCauseV1,
    execution_may_have_started: bool,
    cancellation_requested: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum InterruptionCauseV1 {
    ExecutorShutdown,
    ExecutionLeaseExpired,
    ExecutionOwnerLost,
    ExecutorFault,
    StatePersistenceFailure,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AttemptRejectionV1 {
    code: RejectionCodeV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RejectionCodeV1 {
    ImmutableSpecificationUnusable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AttemptProgressV1 {
    accepted_occurrence_ordinal: u64,
    last_transition_sequence: u64,
    pub(super) steps: Vec<AttemptStepV1>,
    outstanding_actions: Vec<OutstandingActionV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AttemptStepV1 {
    pub(super) id: String,
    pub(super) role: AttemptNodeRoleV1,
    pub(super) failure_policy: FailurePolicy,
    pub(super) state: AttemptStepStateV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum AttemptNodeRoleV1 {
    Step,
    Finalizer,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(super) enum AttemptFinalizationV1 {
    Progress(AttemptFinalizationProgressV1),
    Complete(AttemptFinalizationCompleteV1),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AttemptFinalizationProgressV1 {
    complete: bool,
    trigger: FinalizationTriggerV1,
    finalizers: Vec<AttemptStepV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cancellation: Option<DurableFinalizationCancellationV1>,
    force_abort: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AttemptFinalizationCompleteV1 {
    pub(super) complete: bool,
    pub(super) trigger: FinalizationTriggerV1,
    pub(super) finalizers: Vec<DurableFinalizerV1>,
    pub(super) issues: Vec<DurableFinalizationIssueV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cancellation: Option<DurableFinalizationCancellationV1>,
    pub(super) force_abort: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DurableFinalizerV1 {
    pub(super) id: String,
    pub(super) role: AttemptNodeRoleV1,
    pub(super) failure_policy: FailurePolicy,
    pub(super) state: AttemptStepStateV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) failure: Option<FailureV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reason: Option<StepReasonV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) unavailable_references: Option<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DurableFinalizationIssueV1 {
    pub(super) finalizer_id: String,
    pub(super) impact: FailurePolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DurableFinalizationCancellationV1 {
    pub(super) reason: CancellationReasonV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) force_stop_deadline: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum AttemptStepStateV1 {
    Pending,
    Starting,
    Running,
    CapturingOutputs,
    Cancelling,
    Succeeded,
    Failed,
    Blocked,
    NotRun,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OutstandingActionV1 {
    action_id: u64,
    kind: OutstandingActionKindV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    step_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_role: Option<AttemptNodeRoleV1>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OutstandingActionKindV1 {
    StartStep,
    CaptureOutputs,
    CancelStep,
    FinishRun,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProcessGuardV1 {
    guard_id: String,
    action_id: u64,
    step_id: String,
    node_role: AttemptNodeRoleV1,
    state: ProcessGuardStateV1,
    execution_host: ExecutionHostV1,
    process_group_id: i64,
    liveness: ProcessLivenessV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProcessGuardStateV1 {
    Prepared,
    Released,
    Quiesced,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProcessLivenessV1 {
    kind: ProcessLivenessKindV1,
    value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProcessLivenessKindV1 {
    LeaderStartIdentity,
    GuardHandleIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum AttemptResultV1 {
    NotPublished {
        reason: ResultAbsentReasonV1,
    },
    Published {
        #[serde(rename = "relativeDirectory")]
        relative_directory: String,
    },
    PublicationFailed {
        phase: PublicationFailurePhaseV1,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ResultAbsentReasonV1 {
    AttemptNonterminal,
    PublicationPending,
    Interrupted,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PublicationFailurePhaseV1 {
    ExportCopy,
    Serialization,
    Close,
    Verification,
    Rename,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LocalDiagnosticV1 {
    sequence: u64,
    attempt_number: u64,
    code: DiagnosticCodeV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    step_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    action_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    guard_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DiagnosticCodeV1 {
    StaleOccurrence,
    StatePersistenceFailure,
    ResultPublicationFailure,
    PrivateCleanupFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalStatusErrorCode {
    RunDirectoryUnavailable,
    RunDirectoryInvalid,
    LockQueryFailed,
    StatusSnapshotUnstable,
}

impl LocalStatusErrorCode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RunDirectoryUnavailable => "run_directory_unavailable",
            Self::RunDirectoryInvalid => "run_directory_invalid",
            Self::LockQueryFailed => "lock_query_failed",
            Self::StatusSnapshotUnstable => "status_snapshot_unstable",
        }
    }

    pub(crate) const fn message(self) -> &'static str {
        match self {
            Self::RunDirectoryUnavailable => "The run directory is unavailable.",
            Self::RunDirectoryInvalid => "The run directory does not contain valid V1 state.",
            Self::LockQueryFailed => "The run lock could not be inspected.",
            Self::StatusSnapshotUnstable => {
                "The run state changed too quickly to obtain a stable snapshot."
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalStatusError {
    pub(crate) code: LocalStatusErrorCode,
    pub(crate) run_directory: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnershipUnprovenReason {
    ExecutionHostIdentityUnavailable,
    ProcessIdentityInspectionUnavailable,
}

impl OwnershipUnprovenReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionHostIdentityUnavailable => "execution_host_identity_unavailable",
            Self::ProcessIdentityInspectionUnavailable => "process_identity_inspection_unavailable",
        }
    }

    pub(crate) const fn remedy(self) -> &'static str {
        match self {
            Self::ExecutionHostIdentityUnavailable => {
                "restore execution-host identity inspection or restart the host boundary"
            }
            Self::ProcessIdentityInspectionUnavailable => {
                "restore process identity inspection or restart the host boundary"
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LocalRecoveryStatus {
    Active,
    Settled,
    Abandoned,
    OwnershipUnproven {
        guard_ids: Vec<String>,
        reason: OwnershipUnprovenReason,
    },
}

impl LocalRecoveryStatus {
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Settled => "settled",
            Self::Abandoned => "abandoned",
            Self::OwnershipUnproven { .. } => "ownership_unproven",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryIneligibilityReason {
    RunLocked,
    LatestAttemptSucceeded,
    LatestAttemptRejected,
    OwnershipUnproven,
}

impl RetryIneligibilityReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RunLocked => "run_locked",
            Self::LatestAttemptSucceeded => "latest_attempt_succeeded",
            Self::LatestAttemptRejected => "latest_attempt_rejected",
            Self::OwnershipUnproven => "ownership_unproven",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalRetryEligibility {
    Eligible,
    Ineligible(RetryIneligibilityReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalRetryRejection {
    run_directory: PathBuf,
    attempt_number: u64,
    reason: RetryIneligibilityReason,
    guard_ids: Vec<String>,
    ownership_reason: Option<OwnershipUnprovenReason>,
}

impl LocalRetryRejection {
    pub(crate) fn run_directory(&self) -> &Path {
        &self.run_directory
    }

    pub(crate) const fn attempt_number(&self) -> u64 {
        self.attempt_number
    }

    pub(crate) const fn reason(&self) -> RetryIneligibilityReason {
        self.reason
    }

    pub(crate) fn guard_ids(&self) -> &[String] {
        &self.guard_ids
    }

    pub(crate) const fn ownership_reason(&self) -> Option<OwnershipUnprovenReason> {
        self.ownership_reason
    }
}

pub(crate) enum LocalRetryOpen {
    Acquired(Box<PendingLocalRetry>),
    Rejected(LocalRetryRejection),
}

pub(crate) struct PendingLocalRetry {
    normalized: PathBuf,
    root: Arc<OwnedFd>,
    lock: File,
    state: Arc<StateStore>,
    workflow: ResolvedWorkflow,
    imports: ResolvedImports,
    maximum_parallel_steps: usize,
}

impl PendingLocalRetry {
    pub(crate) fn run_directory(&self) -> &Path {
        &self.normalized
    }

    pub(crate) fn execution_specification(&self) -> (&ResolvedWorkflow, &ResolvedImports, usize) {
        (&self.workflow, &self.imports, self.maximum_parallel_steps)
    }

    pub(crate) fn reused_execution_root_attempts(
        &self,
        admitted: &AdmittedWorkflow,
    ) -> Result<Vec<u64>, LocalRunDirectoryError> {
        let execution_root = admitted
            .execution()
            .root()
            .to_str()
            .ok_or(LocalRunDirectoryError::InvalidPath)?;
        let state = lock_state(&self.state.current)?;
        Ok(state
            .attempts
            .iter()
            .filter(|attempt| attempt.execution_root == execution_root)
            .map(|attempt| attempt.attempt_number)
            .collect())
    }

    pub(crate) fn begin(
        self,
        admitted: &AdmittedWorkflow,
    ) -> Result<LocalAttemptOwner, LocalRetryBeginError> {
        begin_local_retry(self, admitted, &SystemLocalRecoveryAuthority)
    }
}

pub(crate) enum LocalRetryBeginError {
    Rejected(LocalRetryRejection),
    Operational(LocalRunDirectoryError),
}

impl From<LocalRunDirectoryError> for LocalRetryBeginError {
    fn from(error: LocalRunDirectoryError) -> Self {
        Self::Operational(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalStatusAttempt {
    pub(crate) attempt_number: u64,
    pub(crate) trigger: &'static str,
    pub(crate) state: &'static str,
    pub(crate) result: LocalStatusResult,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LocalStatusResult {
    NotPublished { reason: &'static str },
    Published { relative_directory: String },
    PublicationFailed { phase: &'static str },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LocalRunStatusSnapshot {
    pub(crate) run_directory: PathBuf,
    pub(crate) run: Value,
    pub(crate) state: Value,
    pub(crate) current_attempt_number: u64,
    pub(crate) current_attempt_state: &'static str,
    pub(crate) current_result: LocalStatusResult,
    pub(crate) attempts: Vec<LocalStatusAttempt>,
    pub(crate) recovery: LocalRecoveryStatus,
    pub(crate) retry: LocalRetryEligibility,
}

pub(crate) trait DurableDeadline {
    fn deadline_utc(&self) -> OffsetDateTime;
}

pub(crate) struct LocalAttemptOwner {
    normalized: PathBuf,
    root: Arc<OwnedFd>,
    lock: Option<File>,
    private_directory: PathBuf,
    attempt_directory: OwnedFd,
    result_directory: PathBuf,
    attempt_number: u64,
    finalizers: Arc<[AttemptStepV1]>,
    state: Arc<StateStore>,
}

pub(crate) type InitialLocalRun = LocalAttemptOwner;

pub(crate) struct LocalAttemptOwnershipReleased {
    _private: (),
}

impl LocalAttemptOwner {
    pub(crate) fn create(
        requested: &Path,
        admitted: &AdmittedWorkflow,
    ) -> Result<Self, LocalRunDirectoryError> {
        create_with_observer(requested, admitted, &mut NoopInitialPublicationObserver)
    }

    pub(crate) fn run_directory(&self) -> &Path {
        &self.normalized
    }

    pub(crate) fn private_directory(&self) -> &Path {
        &self.private_directory
    }

    pub(crate) fn result_directory(&self) -> &Path {
        &self.result_directory
    }

    pub(crate) fn attempt_directory_handle(&self) -> &OwnedFd {
        &self.attempt_directory
    }

    pub(crate) fn private_directory_handle(&self) -> &OwnedFd {
        &self.state.private
    }

    pub(crate) fn create_agent_diagnostic_sessions(
        &self,
    ) -> Result<AgentDiagnosticSessionStore, LocalRunDirectoryError> {
        let state = lock_state(&self.state.current)?;
        if state.current_attempt_number != self.attempt_number {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        let local_run_id = Arc::<str>::from(state.local_run_id.as_str());
        drop(state);
        let attempt_name = attempt_directory_name(self.attempt_number)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        let attempt_path = self.normalized.join(ATTEMPTS_DIRECTORY).join(attempt_name);
        AgentDiagnosticSessionStore::create(
            &self.attempt_directory,
            &attempt_path,
            local_run_id,
            self.attempt_number,
        )
        .map_err(|_| LocalRunDirectoryError::StagingUnavailable)
    }

    pub(crate) fn create_private_staging(
        &self,
    ) -> Result<AttemptPrivateStaging, LocalRunDirectoryError> {
        let (identity, root) =
            create_staging_root(&self.state.private, "workflow", PRIVATE_STAGING_ATTEMPTS)
                .map_err(|()| LocalRunDirectoryError::StagingUnavailable)?;
        Ok(AttemptPrivateStaging {
            parent: Arc::clone(&self.state.private),
            path: self.private_directory.join(identity.as_ref()),
            identity,
            root,
            released: false,
        })
    }

    pub(crate) const fn attempt_number(&self) -> u64 {
        self.attempt_number
    }

    pub(crate) fn release(mut self) -> LocalAttemptOwnershipReleased {
        self.release_lock();
        LocalAttemptOwnershipReleased { _private: () }
    }

    pub(crate) fn commit_port(&self) -> LocalRunCommitPort {
        LocalRunCommitPort {
            state: Arc::clone(&self.state),
            finalizers: Arc::clone(&self.finalizers),
        }
    }

    pub(crate) fn process_guard_registry(&self) -> ProcessGuardRegistry {
        let state: Arc<dyn DurableProcessGuardStore> = self.state.clone();
        ProcessGuardRegistry::durable(state)
    }

    pub(crate) fn record_result_published(&self) -> Result<(), LocalRunDirectoryError> {
        self.state.update(|state| {
            let attempt = current_attempt_mut(state)?;
            if !attempt.state.is_terminal() {
                return Err(LocalRunDirectoryError::StateConflict);
            }
            attempt.result = AttemptResultV1::Published {
                relative_directory: attempt_result_relative_path(attempt.attempt_number),
            };
            Ok(())
        })
    }

    pub(crate) fn record_result_publication_failed(
        &self,
        phase: PublicationFailurePhaseV1,
    ) -> Result<(), LocalRunDirectoryError> {
        self.state.update(|state| {
            let attempt_number = state.current_attempt_number;
            let attempt = current_attempt_mut(state)?;
            if !attempt.state.is_terminal() {
                return Err(LocalRunDirectoryError::StateConflict);
            }
            attempt.result = AttemptResultV1::PublicationFailed { phase };
            append_diagnostic(
                state,
                attempt_number,
                DiagnosticCodeV1::ResultPublicationFailure,
            )
        })
    }

    pub(crate) fn record_executor_fault_before_execution(
        &self,
    ) -> Result<(), LocalRunDirectoryError> {
        self.state.update(|state| {
            let attempt = current_attempt_mut(state)?;
            if attempt.state != AttemptStateV1::Created {
                return Err(LocalRunDirectoryError::StateConflict);
            }
            settle_interrupted_attempt(attempt, InterruptionCauseV1::ExecutorFault, false)
        })
    }

    pub(crate) fn record_state_persistence_failure(&self) -> Result<(), LocalRunDirectoryError> {
        self.state.update(|state| {
            let attempt_number = state.current_attempt_number;
            {
                let attempt = current_attempt_mut(state)?;
                if attempt.state.is_terminal() {
                    return Ok(());
                }
                let execution_may_have_started = attempt.started_at.is_some();
                settle_interrupted_attempt(
                    attempt,
                    InterruptionCauseV1::StatePersistenceFailure,
                    execution_may_have_started,
                )?;
            }
            append_diagnostic(
                state,
                attempt_number,
                DiagnosticCodeV1::StatePersistenceFailure,
            )
        })
    }

    pub(crate) fn record_private_cleanup_failure(&self) -> Result<(), LocalRunDirectoryError> {
        self.state.update(|state| {
            append_diagnostic(
                state,
                state.current_attempt_number,
                DiagnosticCodeV1::PrivateCleanupFailure,
            )
        })
    }

    pub(crate) fn root_handle(&self) -> &OwnedFd {
        &self.root
    }

    fn release_lock(&mut self) {
        if let Some(lock) = self.lock.take() {
            // Closing normally releases the process-associated lock. Unlock explicitly so
            // an orderly owner release is immediately visible to status and retry queries.
            let _ = fcntl_lock(&lock, FlockOperation::Unlock);
        }
    }
}

pub(crate) struct AttemptPrivateStaging {
    parent: Arc<OwnedFd>,
    path: PathBuf,
    identity: Arc<str>,
    root: OwnedFd,
    released: bool,
}

impl AttemptPrivateStaging {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn root_handle(&self) -> &OwnedFd {
        &self.root
    }

    pub(crate) fn release(mut self) -> Result<(), LocalRunDirectoryError> {
        remove_staging_root(&self.parent, &self.identity, &self.root)
            .map_err(|_| LocalRunDirectoryError::StagingUnavailable)?;
        self.released = true;
        Ok(())
    }
}

impl Drop for AttemptPrivateStaging {
    fn drop(&mut self) {
        if !self.released {
            let _ = remove_staging_root(&self.parent, &self.identity, &self.root);
        }
    }
}

impl Drop for LocalAttemptOwner {
    fn drop(&mut self) {
        self.release_lock();
    }
}

pub(crate) struct LocalRunCommitPort {
    state: Arc<StateStore>,
    finalizers: Arc<[AttemptStepV1]>,
}

impl<Deadline>
    CommitPort<CommittedReduction<StepFailureCause, super::value::CapturedValue, Deadline>>
    for LocalRunCommitPort
where
    Deadline: DurableDeadline,
{
    type Error = LocalRunDirectoryError;

    fn commit(
        &mut self,
        commit: CommittedReduction<StepFailureCause, super::value::CapturedValue, Deadline>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.state.commit_runtime(&commit, &self.finalizers))
    }
}

struct StateStore {
    root: Arc<OwnedFd>,
    private: Arc<OwnedFd>,
    current: Mutex<LocalRunStateV1>,
}

impl StateStore {
    fn commit_runtime<Deadline>(
        &self,
        commit: &CommittedReduction<StepFailureCause, super::value::CapturedValue, Deadline>,
        finalizers: &[AttemptStepV1],
    ) -> Result<(), LocalRunDirectoryError>
    where
        Deadline: DurableDeadline,
    {
        self.update(|state| {
            let now = timestamp(crate::timing::utc_now())?;
            let attempt = current_attempt_mut(state)?;
            if attempt.state.is_terminal() {
                return Err(LocalRunDirectoryError::StateConflict);
            }
            attempt.started_at.get_or_insert_with(|| now.clone());
            if commit.occurrence_accepted {
                attempt.progress.accepted_occurrence_ordinal = commit.occurrence_ordinal.get();
            }
            attempt.progress.last_transition_sequence = commit.state.last_transition_sequence.get();
            update_step_progress(attempt, &commit.state, finalizers)?;
            attempt.progress.outstanding_actions =
                outstanding_actions(attempt, &commit.state.steps)?;
            for requested in &commit.actions {
                if !matches!(requested.kind, CommittedActionKind::FinishRun)
                    && !attempt.progress.outstanding_actions.iter().any(|action| {
                        action.action_id == requested.id.transition_sequence.get()
                            && action.step_id == requested.step
                            && action.node_role
                                == requested
                                    .step
                                    .as_deref()
                                    .and_then(|id| attempt_node_role(attempt, id))
                    })
                {
                    return Err(LocalRunDirectoryError::StateConflict);
                }
            }
            for event in &commit.events {
                if let super::runtime::TransitionEvent::CancellationAccepted {
                    reason,
                    deadline,
                    ..
                } = event
                {
                    attempt.cancellation = Some(AttemptCancellationV1 {
                        reason: cancellation_reason(*reason),
                        requested_at: now.clone(),
                        force_stop_deadline: timestamp(deadline.deadline_utc())?,
                        workflow_confirmed: false,
                    });
                }
            }
            attempt.state = match &commit.state.workflow {
                WorkflowState::Executing {
                    gate: super::runtime::SchedulingGate::Cancelling { .. },
                }
                | WorkflowState::Finalizing {
                    gate: super::runtime::FinalizationGate::Cancelling { .. },
                    ..
                } => AttemptStateV1::Cancelling,
                WorkflowState::Executing { .. }
                | WorkflowState::Finalizing {
                    gate: super::runtime::FinalizationGate::Open,
                    ..
                } => AttemptStateV1::Running,
                WorkflowState::Succeeded => AttemptStateV1::Succeeded,
                WorkflowState::Failed { .. } => AttemptStateV1::WorkflowFailed,
                WorkflowState::Cancelled { .. } => AttemptStateV1::Cancelled,
            };
            if attempt.state.is_terminal() {
                attempt.settled_at = Some(now);
                attempt.progress.outstanding_actions.clear();
                attempt.result = AttemptResultV1::NotPublished {
                    reason: ResultAbsentReasonV1::PublicationPending,
                };
                if let Some(cancellation) = &mut attempt.cancellation
                    && matches!(attempt.state, AttemptStateV1::Cancelled)
                {
                    cancellation.workflow_confirmed = true;
                }
            }
            if !commit.occurrence_accepted {
                let attempt_number = attempt.attempt_number;
                append_diagnostic(state, attempt_number, DiagnosticCodeV1::StaleOccurrence)?;
            }
            Ok(())
        })
    }

    fn update(
        &self,
        mutate: impl FnOnce(&mut LocalRunStateV1) -> Result<(), LocalRunDirectoryError>,
    ) -> Result<(), LocalRunDirectoryError> {
        self.update_with_observer(mutate, &mut NoopStateCommitObserver)
    }

    fn update_with_observer(
        &self,
        mutate: impl FnOnce(&mut LocalRunStateV1) -> Result<(), LocalRunDirectoryError>,
        observer: &mut impl StateCommitObserver,
    ) -> Result<(), LocalRunDirectoryError> {
        let mut current = lock_state(&self.current)?;
        let authoritative_bytes = read_regular_file(&self.root, STATE_FILE)?;
        let authoritative = decode_state(&authoritative_bytes)?;
        if authoritative != *current {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        let mut next = current.clone();
        mutate(&mut next)?;
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        validate_state(&next)?;
        replace_state(
            &self.root,
            &self.private,
            &authoritative_bytes,
            &next,
            observer,
        )?;
        *current = next;
        Ok(())
    }
}

impl DurableProcessGuardStore for StateStore {
    fn register(
        &self,
        step: &str,
        action_id: u64,
        identity: &AuthenticatedProcessGroup,
    ) -> Result<String, ()> {
        let guard_id = generate_uuid().map_err(|_| ())?;
        self.update(|state| {
            let attempt = current_attempt_mut(state)?;
            let registered_action = attempt
                .progress
                .outstanding_actions
                .iter()
                .find(|action| {
                    action.action_id == action_id
                        && matches!(action.kind, OutstandingActionKindV1::StartStep)
                        && action.step_id.as_deref() == Some(step)
                })
                .and_then(|action| action.node_role)
                .ok_or(LocalRunDirectoryError::StateConflict)?;
            if attempt
                .process_guards
                .iter()
                .any(|guard| guard.action_id == action_id || guard.guard_id == guard_id)
            {
                return Err(LocalRunDirectoryError::StateConflict);
            }
            attempt.process_guards.push(ProcessGuardV1 {
                guard_id: guard_id.clone(),
                action_id,
                step_id: step.to_owned(),
                node_role: registered_action,
                state: ProcessGuardStateV1::Prepared,
                execution_host: attempt.owner.execution_host.clone(),
                process_group_id: i64::from(identity.process_group().as_raw_pid()),
                liveness: ProcessLivenessV1 {
                    kind: ProcessLivenessKindV1::LeaderStartIdentity,
                    value: identity.leader_start_identity().to_owned(),
                },
            });
            Ok(())
        })
        .map_err(|_| ())?;
        Ok(guard_id)
    }

    fn mark_released(&self, guard_id: &str) -> Result<(), ()> {
        update_process_guard_state(self, guard_id, ProcessGuardStateV1::Released)
    }

    fn mark_quiesced(&self, guard_id: &str) -> Result<(), ()> {
        update_process_guard_state(self, guard_id, ProcessGuardStateV1::Quiesced)
    }
}

fn update_process_guard_state(
    store: &StateStore,
    guard_id: &str,
    next: ProcessGuardStateV1,
) -> Result<(), ()> {
    store
        .update(|state| {
            let guard = current_attempt_mut(state)?
                .process_guards
                .iter_mut()
                .find(|guard| guard.guard_id == guard_id)
                .ok_or(LocalRunDirectoryError::StateConflict)?;
            match (guard.state, next) {
                (ProcessGuardStateV1::Prepared, ProcessGuardStateV1::Released)
                | (_, ProcessGuardStateV1::Quiesced) => guard.state = next,
                (ProcessGuardStateV1::Released, ProcessGuardStateV1::Released) => {}
                _ => return Err(LocalRunDirectoryError::StateConflict),
            }
            Ok(())
        })
        .map_err(|_| ())
}

fn lock_state(
    state: &Mutex<LocalRunStateV1>,
) -> Result<MutexGuard<'_, LocalRunStateV1>, LocalRunDirectoryError> {
    state
        .lock()
        .map_err(|_| LocalRunDirectoryError::StateConflict)
}

trait InitialPublicationObserver {
    fn published(&mut self, _path: &Path, _lock: &File) -> Result<(), LocalRunDirectoryError> {
        Ok(())
    }
}

struct NoopInitialPublicationObserver;

impl InitialPublicationObserver for NoopInitialPublicationObserver {}

fn create_with_observer(
    requested: &Path,
    admitted: &AdmittedWorkflow,
    observer: &mut impl InitialPublicationObserver,
) -> Result<InitialLocalRun, LocalRunDirectoryError> {
    let target = RunDirectoryTarget::validate(requested, admitted)?;
    let (staging_name, staging_root) =
        create_staging_root(&target.parent, ".run", STAGING_ATTEMPTS)
            .map_err(|()| LocalRunDirectoryError::StagingUnavailable)?;
    let mut staging = InitialStaging {
        parent: &target.parent,
        name: staging_name,
        root: staging_root,
        committed: false,
    };
    let run_staging = create_run_staging(&staging.root, &target.suffix)?;

    let lock = create_file(&run_staging, LOCK_FILE, Mode::RUSR | Mode::WUSR)?;
    fcntl_lock(&lock, FlockOperation::NonBlockingLockExclusive)
        .map_err(|_| LocalRunDirectoryError::LockUnavailable)?;

    mkdir(&run_staging, WORKFLOW_DIRECTORY)?;
    let workflow_directory = open_directory_at(&run_staging, WORKFLOW_DIRECTORY)?;
    mkdir(&workflow_directory, WORKFLOW_FILES_DIRECTORY)?;
    let workflow_files = open_directory_at(&workflow_directory, WORKFLOW_FILES_DIRECTORY)?;
    mkdir(&run_staging, ATTEMPTS_DIRECTORY)?;
    let attempts = open_directory_at(&run_staging, ATTEMPTS_DIRECTORY)?;
    mkdir(&attempts, INITIAL_ATTEMPT_DIRECTORY)?;
    let attempt_directory = open_directory_at(&attempts, INITIAL_ATTEMPT_DIRECTORY)?;
    mkdir(&run_staging, PRIVATE_DIRECTORY)?;
    let private = open_directory_at(&run_staging, PRIVATE_DIRECTORY)?;

    let manifest = retain_execution_specification(&workflow_files, admitted)?;
    let manifest_bytes = encode_json(&manifest)?;
    write_new_immutable_file(&workflow_directory, WORKFLOW_MANIFEST_FILE, &manifest_bytes)?;

    let created_at = timestamp(crate::timing::utc_now())?;
    let local_run_id = generate_uuid()?;
    let run = LocalRunV1 {
        schema_version: 1,
        local_run_id: local_run_id.clone(),
        created_at: created_at.clone(),
        workflow_digest: DigestV1 {
            algorithm: admitted
                .workflow()
                .content_digest
                .algorithm
                .as_str()
                .to_owned(),
            value: admitted.workflow().content_digest.value.clone(),
        },
        workflow_manifest_digest: DigestV1::sha256(&manifest_bytes),
    };
    validate_run(&run)?;
    let run_bytes = encode_json(&run)?;
    write_new_immutable_file(&run_staging, RUN_FILE, &run_bytes)?;

    let initial_state = initial_state(admitted, local_run_id, created_at)?;
    validate_state(&initial_state)?;
    let state_bytes = encode_json(&initial_state)?;
    decode_state(&state_bytes)?;
    write_new_state_file(&run_staging, &state_bytes)?;
    sync_directory(&workflow_files)?;
    sync_directory(&workflow_directory)?;
    sync_directory(&attempts)?;
    sync_directory(&private)?;
    sync_directory(&run_staging)?;
    sync_directory(&staging.root)?;
    verify_initial_staging(&run_staging, &run, &initial_state)?;
    let retained_root =
        dup(&run_staging).map_err(|_| LocalRunDirectoryError::PublicationUnavailable)?;

    target.verify_parent_and_absence()?;
    renameat_with(
        &target.parent,
        staging.name.as_ref(),
        &target.parent,
        &target.name,
        RenameFlags::NOREPLACE,
    )
    .map_err(|failure| match failure {
        Errno::EXIST | Errno::NOTEMPTY => LocalRunDirectoryError::DestinationExists,
        _ => LocalRunDirectoryError::PublicationUnavailable,
    })?;
    staging.committed = true;
    // Process-termination recovery relies on atomic visibility, not host-power-loss
    // durability; a post-rename directory sync cannot revoke the published commit.
    let _ = sync_directory(&target.parent);
    observer.published(&target.normalized, &lock)?;

    let root = Arc::new(retained_root);
    let private = Arc::new(private);
    let state = Arc::new(StateStore {
        root: Arc::clone(&root),
        private,
        current: Mutex::new(initial_state),
    });
    let private_directory = target.normalized.join(PRIVATE_DIRECTORY);
    let result_directory = target
        .normalized
        .join(ATTEMPTS_DIRECTORY)
        .join(INITIAL_ATTEMPT_DIRECTORY)
        .join("result");
    Ok(LocalAttemptOwner {
        normalized: target.normalized,
        root,
        lock: Some(lock),
        private_directory,
        attempt_directory,
        result_directory,
        attempt_number: INITIAL_ATTEMPT_NUMBER,
        finalizers: Arc::from(fresh_finalizer_progress(admitted)?),
        state,
    })
}

struct RunDirectoryTarget {
    supplied_parent: PathBuf,
    parent: OwnedFd,
    name: OsString,
    suffix: Vec<OsString>,
    normalized: PathBuf,
    execution_root: AdmittedExecutionRoot,
}

impl RunDirectoryTarget {
    fn validate(
        requested: &Path,
        admitted: &AdmittedWorkflow,
    ) -> Result<Self, LocalRunDirectoryError> {
        let requested = if requested.is_absolute() {
            requested.to_owned()
        } else {
            std::env::current_dir()
                .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?
                .join(requested)
        };
        let (supplied_parent, suffix) = nearest_existing_parent(&requested)?;
        let canonical_parent = std::fs::canonicalize(&supplied_parent)
            .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?;
        let parent = open_directory_path(&canonical_parent)
            .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?;
        let name = suffix
            .first()
            .ok_or(LocalRunDirectoryError::DestinationExists)?
            .clone();
        ensure_absent(&parent, &name)?;
        let normalized = suffix
            .iter()
            .fold(canonical_parent, |path, component| path.join(component));
        if normalized.to_str().is_none() {
            return Err(LocalRunDirectoryError::InvalidPath);
        }
        let execution_root = admitted.execution().root_identity().clone();
        if run_directory_overlaps_execution_root(
            &normalized,
            admitted.execution().root(),
            &execution_root,
            &parent,
        )? {
            return Err(LocalRunDirectoryError::ExecutionRootOverlap);
        }
        Ok(Self {
            supplied_parent,
            parent,
            name,
            suffix,
            normalized,
            execution_root,
        })
    }

    fn verify_parent_and_absence(&self) -> Result<(), LocalRunDirectoryError> {
        let rebound = std::fs::canonicalize(&self.supplied_parent)
            .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?;
        let rebound =
            open_directory_path(&rebound).map_err(|_| LocalRunDirectoryError::ParentUnavailable)?;
        if !same_file(&self.parent, &rebound)
            .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?
        {
            return Err(LocalRunDirectoryError::ParentUnavailable);
        }
        if run_directory_overlaps_execution_root(
            &self.normalized,
            self.execution_root.provenance_path(),
            &self.execution_root,
            &self.parent,
        )? {
            return Err(LocalRunDirectoryError::ExecutionRootOverlap);
        }
        ensure_absent(&self.parent, &self.name)
    }
}

fn nearest_existing_parent(
    requested: &Path,
) -> Result<(PathBuf, Vec<OsString>), LocalRunDirectoryError> {
    let mut candidate = requested.to_owned();
    let mut suffix = VecDeque::new();
    loop {
        match std::fs::symlink_metadata(&candidate) {
            Ok(_) if suffix.is_empty() => {
                return Err(LocalRunDirectoryError::DestinationExists);
            }
            Ok(_) => break,
            Err(failure) if failure.kind() == io::ErrorKind::NotFound => {
                let component = candidate
                    .file_name()
                    .filter(|component| *component != "." && *component != "..")
                    .ok_or(LocalRunDirectoryError::InvalidPath)?;
                if component.to_str().is_none() {
                    return Err(LocalRunDirectoryError::InvalidPath);
                }
                suffix.push_front(component.to_owned());
                if !candidate.pop() {
                    return Err(LocalRunDirectoryError::ParentUnavailable);
                }
            }
            Err(_) => return Err(LocalRunDirectoryError::ParentUnavailable),
        }
    }
    if suffix.is_empty() {
        return Err(LocalRunDirectoryError::DestinationExists);
    }
    Ok((candidate, suffix.into_iter().collect()))
}

fn create_run_staging(
    staging_root: &OwnedFd,
    suffix: &[OsString],
) -> Result<OwnedFd, LocalRunDirectoryError> {
    let mut current = dup(staging_root).map_err(|_| LocalRunDirectoryError::StagingUnavailable)?;
    for component in suffix.iter().skip(1) {
        mkdir(&current, component)?;
        current = open_directory_at(&current, component)?;
    }
    Ok(current)
}

struct InitialStaging<'a> {
    parent: &'a OwnedFd,
    name: Arc<str>,
    root: OwnedFd,
    committed: bool,
}

impl Drop for InitialStaging<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = remove_staging_root(self.parent, &self.name, &self.root);
        }
    }
}

fn retain_execution_specification(
    files: &OwnedFd,
    admitted: &AdmittedWorkflow,
) -> Result<WorkflowManifestV1, LocalRunDirectoryError> {
    let source_root = admitted
        .workflow()
        .source
        .source_root
        .to_str()
        .ok_or(LocalRunDirectoryError::InvalidPath)?
        .to_owned();
    let mut ordinal = 0_u64;
    let mut source_files = Vec::with_capacity(admitted.workflow().source_closure.len());
    for (path, bytes) in &admitted.workflow().source_closure {
        ordinal = ordinal
            .checked_add(1)
            .ok_or(LocalRunDirectoryError::SerializationUnavailable)?;
        let file = retain_file(files, ordinal, bytes)?;
        source_files.push(ManifestSourceFileV1 {
            path: path.clone(),
            file,
        });
    }
    let prompt = admitted
        .imports()
        .prompt()
        .map(|prompt| {
            ordinal = ordinal
                .checked_add(1)
                .ok_or(LocalRunDirectoryError::SerializationUnavailable)?;
            retain_file(files, ordinal, prompt.as_bytes())
        })
        .transpose()?;
    let mut attachments = Vec::with_capacity(admitted.imports().attachments().len());
    for attachment in admitted.imports().attachments() {
        ordinal = ordinal
            .checked_add(1)
            .ok_or(LocalRunDirectoryError::SerializationUnavailable)?;
        attachments.push(ManifestAttachmentV1 {
            media_type: attachment.media_type().to_owned(),
            file: retain_file(files, ordinal, attachment.bytes())?,
        });
    }
    let manifest = WorkflowManifestV1 {
        schema_version: 1,
        workflow_path: admitted.workflow().source.workflow_path.clone(),
        source_root,
        maximum_parallel_steps: admitted.execution().limits().maximum_parallel_steps().get(),
        source_files,
        imports: ManifestImportsV1 {
            prompt,
            attachments,
        },
    };
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn retain_file(
    directory: &OwnedFd,
    ordinal: u64,
    bytes: &[u8],
) -> Result<ManifestFileV1, LocalRunDirectoryError> {
    let name = retained_file_name(ordinal)?;
    write_new_immutable_file(directory, &name, bytes)?;
    Ok(ManifestFileV1 {
        ordinal,
        relative_file: format!("{WORKFLOW_FILES_DIRECTORY}/{name}"),
        size_bytes: u64::try_from(bytes.len())
            .map_err(|_| LocalRunDirectoryError::SerializationUnavailable)?,
        digest: DigestV1::sha256(bytes),
    })
}

fn initial_state(
    admitted: &AdmittedWorkflow,
    local_run_id: String,
    created_at: String,
) -> Result<LocalRunStateV1, LocalRunDirectoryError> {
    let attempt = fresh_attempt(
        admitted,
        INITIAL_ATTEMPT_NUMBER,
        AttemptTriggerV1::Initial,
        None,
        created_at,
    )?;
    Ok(LocalRunStateV1 {
        schema_version: 1,
        local_run_id,
        revision: 1,
        current_attempt_number: INITIAL_ATTEMPT_NUMBER,
        attempts: vec![attempt],
        diagnostics: Vec::new(),
    })
}

fn update_step_progress<Deadline>(
    attempt: &mut LocalAttemptV1,
    runtime: &super::runtime::RuntimeState<StepFailureCause, super::value::CapturedValue, Deadline>,
    finalizers: &[AttemptStepV1],
) -> Result<(), LocalRunDirectoryError>
where
    Deadline: DurableDeadline,
{
    if attempt.progress.steps.len() + finalizers.len() != runtime.steps.len() {
        return Err(LocalRunDirectoryError::StateConflict);
    }
    update_progress_nodes(&mut attempt.progress.steps, &runtime.steps)?;

    if let Some(summary) = &runtime.finalization_summary {
        if finalizers.is_empty() {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        let mut complete = durable_finalization_summary(summary)?;
        let finalizer_count = complete.finalizers.len();
        let mut retained = complete
            .finalizers
            .into_iter()
            .map(|finalizer| (finalizer.id.clone(), finalizer))
            .collect::<BTreeMap<_, _>>();
        if retained.len() != finalizer_count || retained.len() != finalizers.len() {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        complete.finalizers = finalizers
            .iter()
            .map(|expected| {
                let finalizer = retained
                    .remove(&expected.id)
                    .ok_or(LocalRunDirectoryError::StateConflict)?;
                if finalizer.role != expected.role
                    || finalizer.failure_policy != expected.failure_policy
                {
                    return Err(LocalRunDirectoryError::StateConflict);
                }
                Ok(finalizer)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if !retained.is_empty() {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        complete.issues = durable_finalization_issues(&complete.finalizers);
        attempt.finalization = Some(AttemptFinalizationV1::Complete(complete));
        return Ok(());
    }

    let WorkflowState::Finalizing { trigger, gate, .. } = &runtime.workflow else {
        if attempt.finalization.is_some()
            || (!finalizers.is_empty()
                && matches!(
                    runtime.workflow,
                    WorkflowState::Succeeded
                        | WorkflowState::Failed { .. }
                        | WorkflowState::Cancelled { .. }
                ))
        {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        return Ok(());
    };

    if finalizers.is_empty() {
        return Err(LocalRunDirectoryError::StateConflict);
    }
    if attempt.finalization.is_none() {
        attempt.finalization = Some(AttemptFinalizationV1::Progress(
            AttemptFinalizationProgressV1 {
                complete: false,
                trigger: finalization_trigger(*trigger),
                finalizers: finalizers.to_vec(),
                cancellation: None,
                force_abort: false,
            },
        ));
    }
    let Some(AttemptFinalizationV1::Progress(progress)) = &mut attempt.finalization else {
        return Err(LocalRunDirectoryError::StateConflict);
    };
    progress.trigger = finalization_trigger(*trigger);
    match gate {
        super::runtime::FinalizationGate::Open => {
            progress.cancellation = None;
            progress.force_abort = false;
        }
        super::runtime::FinalizationGate::Cancelling {
            reason,
            deadline,
            force_abort,
        } => {
            progress.cancellation = Some(DurableFinalizationCancellationV1 {
                reason: cancellation_reason(*reason),
                force_stop_deadline: deadline
                    .as_ref()
                    .map(|deadline| timestamp(deadline.deadline_utc()))
                    .transpose()?,
            });
            progress.force_abort = *force_abort;
        }
    }
    update_progress_nodes(&mut progress.finalizers, &runtime.steps)
}

fn update_progress_nodes<Cause, Output>(
    nodes: &mut [AttemptStepV1],
    runtime_steps: &BTreeMap<String, super::runtime::StepRuntimeState<Cause, Output>>,
) -> Result<(), LocalRunDirectoryError> {
    for node in nodes {
        let runtime = runtime_steps
            .get(&node.id)
            .ok_or(LocalRunDirectoryError::StateConflict)?;
        node.state = attempt_step_state(&runtime.state);
    }
    Ok(())
}

fn attempt_step_state<Cause, Output>(state: &StepState<Cause, Output>) -> AttemptStepStateV1 {
    match state {
        StepState::Pending => AttemptStepStateV1::Pending,
        StepState::Starting => AttemptStepStateV1::Starting,
        StepState::Running => AttemptStepStateV1::Running,
        StepState::CapturingOutputs => AttemptStepStateV1::CapturingOutputs,
        StepState::Cancelling { .. } => AttemptStepStateV1::Cancelling,
        StepState::Succeeded { .. } => AttemptStepStateV1::Succeeded,
        StepState::Failed { .. } => AttemptStepStateV1::Failed,
        StepState::Blocked { .. } | StepState::InputUnavailable { .. } => {
            AttemptStepStateV1::Blocked
        }
        StepState::NotRun { .. } => AttemptStepStateV1::NotRun,
        StepState::Cancelled { .. } => AttemptStepStateV1::Cancelled,
    }
}

fn durable_finalization_summary<Deadline>(
    summary: &FinalizationSummary<StepFailureCause, Deadline>,
) -> Result<AttemptFinalizationCompleteV1, LocalRunDirectoryError>
where
    Deadline: DurableDeadline,
{
    let finalizers = summary
        .finalizers
        .iter()
        .map(|finalizer| {
            let (state, failure, reason, unavailable_references) = match &finalizer.disposition {
                StepState::Succeeded { .. } => (AttemptStepStateV1::Succeeded, None, None, None),
                StepState::Failed { phase, cause } => (
                    AttemptStepStateV1::Failed,
                    Some(
                        failure_v1(*phase, cause)
                            .map_err(|_| LocalRunDirectoryError::SerializationUnavailable)?,
                    ),
                    None,
                    None,
                ),
                StepState::InputUnavailable { references } => (
                    AttemptStepStateV1::Blocked,
                    None,
                    Some(StepReasonV1::InputUnavailable),
                    Some(references.clone()),
                ),
                StepState::NotRun {
                    reason: super::runtime::NotRunReason::FinalizerTriggerNotSelected,
                } => (
                    AttemptStepStateV1::NotRun,
                    None,
                    Some(StepReasonV1::FinalizerTriggerNotSelected),
                    None,
                ),
                StepState::Cancelled { reason } => (
                    AttemptStepStateV1::Cancelled,
                    None,
                    Some(cancellation_step_reason(*reason)),
                    None,
                ),
                StepState::Pending
                | StepState::Starting
                | StepState::Running
                | StepState::CapturingOutputs
                | StepState::Cancelling { .. }
                | StepState::Blocked { .. }
                | StepState::NotRun {
                    reason: super::runtime::NotRunReason::FailureStop,
                } => return Err(LocalRunDirectoryError::StateConflict),
            };
            Ok(DurableFinalizerV1 {
                id: finalizer.finalizer.clone(),
                role: AttemptNodeRoleV1::Finalizer,
                failure_policy: finalizer.failure_policy,
                state,
                failure,
                reason,
                unavailable_references,
            })
        })
        .collect::<Result<Vec<_>, LocalRunDirectoryError>>()?;
    let issues = durable_finalization_issues(&finalizers);
    let cancellation = summary
        .cancellation
        .as_ref()
        .map(|cancellation| {
            Ok(DurableFinalizationCancellationV1 {
                reason: cancellation_reason(cancellation.reason),
                force_stop_deadline: cancellation
                    .deadline
                    .as_ref()
                    .map(|deadline| timestamp(deadline.deadline_utc()))
                    .transpose()?,
            })
        })
        .transpose()?;
    Ok(AttemptFinalizationCompleteV1 {
        complete: true,
        trigger: finalization_trigger(summary.trigger),
        finalizers,
        issues,
        cancellation,
        force_abort: summary.force_abort,
    })
}

fn durable_finalization_issues(
    finalizers: &[DurableFinalizerV1],
) -> Vec<DurableFinalizationIssueV1> {
    finalizers
        .iter()
        .filter(|finalizer| {
            finalizer.state == AttemptStepStateV1::Failed
                || (finalizer.state == AttemptStepStateV1::Blocked
                    && finalizer.reason == Some(StepReasonV1::InputUnavailable))
        })
        .map(|finalizer| DurableFinalizationIssueV1 {
            finalizer_id: finalizer.id.clone(),
            impact: finalizer.failure_policy,
        })
        .collect()
}

fn outstanding_actions<Cause, Output>(
    attempt: &LocalAttemptV1,
    runtime_steps: &BTreeMap<String, super::runtime::StepRuntimeState<Cause, Output>>,
) -> Result<Vec<OutstandingActionV1>, LocalRunDirectoryError> {
    let ordered_nodes = attempt.progress.steps.iter().chain(
        attempt
            .finalization
            .iter()
            .filter_map(|finalization| match finalization {
                AttemptFinalizationV1::Progress(progress) => Some(progress.finalizers.as_slice()),
                AttemptFinalizationV1::Complete(_) => None,
            })
            .flatten(),
    );
    let mut actions = ordered_nodes
        .filter_map(|node| {
            let runtime = match runtime_steps.get(&node.id) {
                Some(runtime) => runtime,
                None => return Some(Err(LocalRunDirectoryError::StateConflict)),
            };
            let action = runtime.current_action?;
            let kind = match runtime.state {
                StepState::Starting | StepState::Running => OutstandingActionKindV1::StartStep,
                StepState::CapturingOutputs => OutstandingActionKindV1::CaptureOutputs,
                StepState::Cancelling { .. } => OutstandingActionKindV1::CancelStep,
                StepState::Pending
                | StepState::Succeeded { .. }
                | StepState::Failed { .. }
                | StepState::Blocked { .. }
                | StepState::InputUnavailable { .. }
                | StepState::NotRun { .. }
                | StepState::Cancelled { .. } => {
                    return Some(Err(LocalRunDirectoryError::StateConflict));
                }
            };
            Some(Ok(OutstandingActionV1 {
                action_id: action.transition_sequence.get(),
                kind,
                step_id: Some(node.id.clone()),
                node_role: Some(node.role),
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    actions.sort_unstable_by_key(|action| action.action_id);
    Ok(actions)
}

fn attempt_node_role(attempt: &LocalAttemptV1, id: &str) -> Option<AttemptNodeRoleV1> {
    if attempt.progress.steps.iter().any(|node| node.id == id) {
        return Some(AttemptNodeRoleV1::Step);
    }
    attempt
        .finalization
        .as_ref()
        .and_then(|finalization| match finalization {
            AttemptFinalizationV1::Progress(progress) => progress
                .finalizers
                .iter()
                .find(|node| node.id == id)
                .map(|node| node.role),
            AttemptFinalizationV1::Complete(complete) => complete
                .finalizers
                .iter()
                .find(|node| node.id == id)
                .map(|node| node.role),
        })
}

fn settle_interrupted_attempt(
    attempt: &mut LocalAttemptV1,
    cause: InterruptionCauseV1,
    execution_may_have_started: bool,
) -> Result<(), LocalRunDirectoryError> {
    let cancellation_requested = attempt.cancellation.is_some()
        || attempt
            .finalization
            .as_ref()
            .is_some_and(|finalization| match finalization {
                AttemptFinalizationV1::Progress(progress) => progress.cancellation.is_some(),
                AttemptFinalizationV1::Complete(complete) => complete.cancellation.is_some(),
            });
    attempt.state = AttemptStateV1::Interrupted;
    attempt.settled_at = Some(timestamp(crate::timing::utc_now())?);
    attempt.interruption = Some(AttemptInterruptionV1 {
        cause,
        execution_may_have_started,
        cancellation_requested,
    });
    attempt.result = AttemptResultV1::NotPublished {
        reason: ResultAbsentReasonV1::Interrupted,
    };
    attempt.progress.outstanding_actions.clear();
    Ok(())
}

fn append_diagnostic(
    state: &mut LocalRunStateV1,
    attempt_number: u64,
    code: DiagnosticCodeV1,
) -> Result<(), LocalRunDirectoryError> {
    let sequence = state
        .diagnostics
        .last()
        .map_or(Some(1), |diagnostic| diagnostic.sequence.checked_add(1))
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    state.diagnostics.push(LocalDiagnosticV1 {
        sequence,
        attempt_number,
        code,
        step_id: None,
        action_id: None,
        guard_id: None,
    });
    if state.diagnostics.len() > MAXIMUM_DIAGNOSTICS {
        state.diagnostics.remove(0);
    }
    Ok(())
}

fn current_attempt_mut(
    state: &mut LocalRunStateV1,
) -> Result<&mut LocalAttemptV1, LocalRunDirectoryError> {
    let attempt = state
        .attempts
        .last_mut()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    if attempt.attempt_number != state.current_attempt_number {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok(attempt)
}

fn attempt_result_relative_path(attempt_number: u64) -> String {
    format!(
        "{ATTEMPTS_DIRECTORY}/{}/result",
        attempt_directory_name(attempt_number).unwrap_or_else(|| attempt_number.to_string())
    )
}

pub(crate) fn attempt_directory_name(attempt_number: u64) -> Option<String> {
    if attempt_number == 0 {
        None
    } else if attempt_number < 1_000_000 {
        Some(format!("{attempt_number:06}"))
    } else {
        Some(attempt_number.to_string())
    }
}

fn retained_file_name(ordinal: u64) -> Result<String, LocalRunDirectoryError> {
    if ordinal == 0 {
        Err(LocalRunDirectoryError::SerializationUnavailable)
    } else if ordinal < 10_000 {
        Ok(format!("{ordinal:04}"))
    } else {
        Ok(ordinal.to_string())
    }
}

fn replace_state(
    root: &OwnedFd,
    private: &OwnedFd,
    expected_authoritative: &[u8],
    state: &LocalRunStateV1,
    observer: &mut impl StateCommitObserver,
) -> Result<(), LocalRunDirectoryError> {
    let bytes = encode_json(state)?;
    decode_state(&bytes)?;
    let (temporary_name, mut temporary) = create_state_temporary(private)?;
    let mut cleanup = StateTemporary {
        parent: private,
        name: temporary_name.clone(),
        committed: false,
    };
    observer
        .write_temporary(&mut temporary, &bytes)
        .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?;
    temporary
        .flush()
        .and_then(|()| temporary.sync_all())
        .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?;
    drop(temporary);
    observer.temporary_complete()?;
    renameat_with(
        private,
        &temporary_name,
        root,
        STATE_FILE,
        RenameFlags::EXCHANGE,
    )
    .map_err(|_| LocalRunDirectoryError::AtomicCommitUnavailable)?;
    let replaced = read_regular_file(private, &temporary_name)?;
    if replaced != expected_authoritative {
        if renameat_with(
            private,
            &temporary_name,
            root,
            STATE_FILE,
            RenameFlags::EXCHANGE,
        )
        .is_err()
        {
            cleanup.committed = true;
            return Err(LocalRunDirectoryError::AtomicCommitUnavailable);
        }
        return Err(LocalRunDirectoryError::StateConflict);
    }
    sync_directory(root)?;
    observer.replaced()?;
    Ok(())
}

trait StateCommitObserver {
    fn write_temporary(&mut self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(bytes)
    }

    fn temporary_complete(&mut self) -> Result<(), LocalRunDirectoryError> {
        Ok(())
    }

    fn replaced(&mut self) -> Result<(), LocalRunDirectoryError> {
        Ok(())
    }
}

struct NoopStateCommitObserver;

impl StateCommitObserver for NoopStateCommitObserver {}

struct StateTemporary<'a> {
    parent: &'a OwnedFd,
    name: String,
    committed: bool,
}

impl Drop for StateTemporary<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = unlinkat(self.parent, &self.name, AtFlags::empty());
        }
    }
}

fn create_state_temporary(parent: &OwnedFd) -> Result<(String, File), LocalRunDirectoryError> {
    for _ in 0..STAGING_ATTEMPTS {
        let name = format!(".state-{}", generate_uuid()?);
        match openat(
            parent,
            &name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        ) {
            Ok(file) => return Ok((name, File::from(file))),
            Err(Errno::EXIST) => {}
            Err(_) => return Err(LocalRunDirectoryError::StateWriteUnavailable),
        }
    }
    Err(LocalRunDirectoryError::StateWriteUnavailable)
}

fn verify_initial_staging(
    root: &OwnedFd,
    expected_run: &LocalRunV1,
    expected_state: &LocalRunStateV1,
) -> Result<(), LocalRunDirectoryError> {
    let entries = directory_entries(root)?;
    if entries != run_root_entries()
        || read_run(root)? != *expected_run
        || read_state(root)? != *expected_state
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok(())
}

fn read_run(root: &OwnedFd) -> Result<LocalRunV1, LocalRunDirectoryError> {
    read_run_with_size(root).map(|(run, _)| run)
}

fn read_run_with_size(root: &OwnedFd) -> Result<(LocalRunV1, u64), LocalRunDirectoryError> {
    let bytes = read_regular_file(root, RUN_FILE)?;
    let size = u64::try_from(bytes.len()).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    Ok((decode_run(&bytes)?, size))
}

fn read_state(root: &OwnedFd) -> Result<LocalRunStateV1, LocalRunDirectoryError> {
    read_state_with_size(root).map(|(state, _)| state)
}

fn read_state_with_size(root: &OwnedFd) -> Result<(LocalRunStateV1, u64), LocalRunDirectoryError> {
    let bytes = read_regular_file(root, STATE_FILE)?;
    let size = u64::try_from(bytes.len()).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    Ok((decode_state(&bytes)?, size))
}

pub(crate) fn acquire_local_retry(
    requested: &Path,
) -> Result<LocalRetryOpen, LocalRunDirectoryError> {
    for _ in 0..STATUS_SNAPSHOT_ATTEMPTS {
        let normalized = std::fs::canonicalize(requested)
            .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?;
        if normalized.to_str().is_none() {
            return Err(LocalRunDirectoryError::InvalidPath);
        }
        let root = open_directory_path(&normalized)
            .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?;
        let lock = open_retry_lock(&root)?;
        match fcntl_lock(&lock, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => {
                verify_retry_lock_identity(&root, &lock)?;
                return open_locked_retry(normalized, root, lock);
            }
            Err(Errno::AGAIN | Errno::ACCESS) => {
                let snapshot = read_local_run_status(requested)
                    .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
                if snapshot.retry
                    == LocalRetryEligibility::Ineligible(RetryIneligibilityReason::RunLocked)
                {
                    return Ok(LocalRetryOpen::Rejected(retry_rejection_from_snapshot(
                        &snapshot,
                        RetryIneligibilityReason::RunLocked,
                    )));
                }
            }
            Err(_) => return Err(LocalRunDirectoryError::LockUnavailable),
        }
    }
    Err(LocalRunDirectoryError::StateConflict)
}

fn open_retry_lock(root: &OwnedFd) -> Result<File, LocalRunDirectoryError> {
    let lock = openat(
        root,
        LOCK_FILE,
        OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| LocalRunDirectoryError::LockUnavailable)?;
    verify_retry_lock_identity(root, &lock)?;
    Ok(lock)
}

fn verify_retry_lock_identity(root: &OwnedFd, lock: &File) -> Result<(), LocalRunDirectoryError> {
    let opened = fstat(lock).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    let current = statat(root, LOCK_FILE, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    if FileType::from_raw_mode(opened.st_mode) != FileType::RegularFile
        || FileType::from_raw_mode(current.st_mode) != FileType::RegularFile
        || opened.st_dev != current.st_dev
        || opened.st_ino != current.st_ino
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok(())
}

fn open_locked_retry(
    normalized: PathBuf,
    root: OwnedFd,
    lock: File,
) -> Result<LocalRetryOpen, LocalRunDirectoryError> {
    verify_existing_run_layout(&root)?;
    let run = read_run(&root)?;
    let state = read_state(&root)?;
    if state.local_run_id != run.local_run_id {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let current = state
        .attempts
        .last()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let recovery = recovery_status(current, false);
    if let LocalRetryEligibility::Ineligible(reason) =
        retry_eligibility(current.state, &recovery, false)
    {
        return Ok(LocalRetryOpen::Rejected(retry_rejection(
            normalized,
            current.attempt_number,
            reason,
            &recovery,
        )));
    }

    let (workflow, imports, maximum_parallel_steps) = load_retained_execution(&root, &run)?;
    let private = Arc::new(open_directory_at(&root, PRIVATE_DIRECTORY)?);
    let root = Arc::new(root);
    let state = Arc::new(StateStore {
        root: Arc::clone(&root),
        private,
        current: Mutex::new(state),
    });
    Ok(LocalRetryOpen::Acquired(Box::new(PendingLocalRetry {
        normalized,
        root,
        lock,
        state,
        workflow,
        imports,
        maximum_parallel_steps,
    })))
}

fn verify_existing_run_layout(root: &OwnedFd) -> Result<(), LocalRunDirectoryError> {
    if directory_entries(root)? != run_root_entries() {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    open_directory_at(root, ATTEMPTS_DIRECTORY)?;
    Ok(())
}

fn run_root_entries() -> BTreeSet<Vec<u8>> {
    BTreeSet::from([
        RUN_FILE.as_bytes().to_vec(),
        STATE_FILE.as_bytes().to_vec(),
        LOCK_FILE.as_bytes().to_vec(),
        WORKFLOW_DIRECTORY.as_bytes().to_vec(),
        ATTEMPTS_DIRECTORY.as_bytes().to_vec(),
        PRIVATE_DIRECTORY.as_bytes().to_vec(),
    ])
}

pub(super) fn load_retained_execution(
    root: &OwnedFd,
    run: &LocalRunV1,
) -> Result<(ResolvedWorkflow, ResolvedImports, usize), LocalRunDirectoryError> {
    load_retained_execution_with_budget(root, run, &mut RetainedReadBudget::default())
}

pub(super) fn load_retained_execution_with_budget(
    root: &OwnedFd,
    run: &LocalRunV1,
    budget: &mut RetainedReadBudget,
) -> Result<(ResolvedWorkflow, ResolvedImports, usize), LocalRunDirectoryError> {
    let workflow_directory = open_directory_at(root, WORKFLOW_DIRECTORY)?;
    let expected_workflow_entries = BTreeSet::from([
        WORKFLOW_MANIFEST_FILE.as_bytes().to_vec(),
        WORKFLOW_FILES_DIRECTORY.as_bytes().to_vec(),
    ]);
    if directory_entries(&workflow_directory)? != expected_workflow_entries {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let manifest_bytes = read_regular_file(&workflow_directory, WORKFLOW_MANIFEST_FILE)?;
    budget.account(&manifest_bytes)?;
    if DigestV1::sha256(&manifest_bytes) != run.workflow_manifest_digest {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let manifest: WorkflowManifestV1 = decode_schema_one(&manifest_bytes)?;
    validate_manifest(&manifest).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    let files = open_directory_at(&workflow_directory, WORKFLOW_FILES_DIRECTORY)?;
    let expected_entries = (1..retained_manifest_file_count(&manifest)?)
        .map(retained_file_name)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(String::into_bytes)
        .collect::<BTreeSet<_>>();
    if directory_entries(&files)? != expected_entries {
        return Err(LocalRunDirectoryError::StateInvalid);
    }

    let mut captured_file_bytes = 0;
    let mut read_file = |file: &ManifestFileV1| {
        let name = retained_file_name(file.ordinal)?;
        let bytes = read_retained_file(&files, &name)?;
        let size = u64::try_from(bytes.len()).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
        account_retained_bytes(
            &mut captured_file_bytes,
            size,
            MAXIMUM_RETAINED_CAPTURED_FILE_BYTES,
        )?;
        budget.account(&bytes)?;
        if size != file.size_bytes || DigestV1::sha256(&bytes) != file.digest {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        Ok(bytes)
    };

    let mut source_closure = BTreeMap::new();
    for source in &manifest.source_files {
        if source_closure
            .insert(
                source.path.clone(),
                Arc::<[u8]>::from(read_file(&source.file)?),
            )
            .is_some()
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
    }
    let prompt = manifest
        .imports
        .prompt
        .as_ref()
        .map(|file| {
            String::from_utf8(read_file(file)?)
                .map(Arc::<str>::from)
                .map_err(|_| LocalRunDirectoryError::StateInvalid)
        })
        .transpose()?;
    let attachments = manifest
        .imports
        .attachments
        .iter()
        .map(|attachment| {
            Ok(ResolvedAttachment::new(
                Arc::<str>::from(attachment.media_type.as_str()),
                Arc::<[u8]>::from(read_file(&attachment.file)?),
            ))
        })
        .collect::<Result<Vec<_>, LocalRunDirectoryError>>()?;
    let workflow = resolve_retained(
        PathBuf::from(&manifest.source_root),
        &manifest.workflow_path,
        source_closure,
    )
    .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    if workflow.content_digest.algorithm.as_str() != run.workflow_digest.algorithm
        || workflow.content_digest.value != run.workflow_digest.value
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok((
        workflow,
        ResolvedImports::new(prompt, Arc::from(attachments)),
        manifest.maximum_parallel_steps,
    ))
}

fn retained_manifest_file_count(
    manifest: &WorkflowManifestV1,
) -> Result<u64, LocalRunDirectoryError> {
    let count = manifest
        .source_files
        .len()
        .checked_add(usize::from(manifest.imports.prompt.is_some()))
        .and_then(|count| count.checked_add(manifest.imports.attachments.len()))
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    u64::try_from(count)
        .ok()
        .and_then(|count| count.checked_add(1))
        .ok_or(LocalRunDirectoryError::StateInvalid)
}

fn read_retained_file(parent: &OwnedFd, name: &str) -> Result<Vec<u8>, LocalRunDirectoryError> {
    read_regular_file_bounded(parent, name, MAXIMUM_RETAINED_FILE_BYTES)
}

fn retry_rejection_from_snapshot(
    snapshot: &LocalRunStatusSnapshot,
    reason: RetryIneligibilityReason,
) -> LocalRetryRejection {
    retry_rejection(
        snapshot.run_directory.clone(),
        snapshot.current_attempt_number,
        reason,
        &snapshot.recovery,
    )
}

fn retry_rejection(
    run_directory: PathBuf,
    attempt_number: u64,
    reason: RetryIneligibilityReason,
    recovery: &LocalRecoveryStatus,
) -> LocalRetryRejection {
    let (guard_ids, ownership_reason) = match recovery {
        LocalRecoveryStatus::OwnershipUnproven { guard_ids, reason } => {
            (guard_ids.clone(), Some(*reason))
        }
        LocalRecoveryStatus::Active
        | LocalRecoveryStatus::Settled
        | LocalRecoveryStatus::Abandoned => (Vec::new(), None),
    };
    LocalRetryRejection {
        run_directory,
        attempt_number,
        reason,
        guard_ids,
        ownership_reason,
    }
}

#[derive(Default)]
pub(super) struct RetainedReadBudget {
    total_bytes: u64,
}

impl RetainedReadBudget {
    pub(super) fn with_bytes(total_bytes: u64) -> Result<Self, LocalRunDirectoryError> {
        (total_bytes <= MAXIMUM_RETAINED_TOTAL_BYTES)
            .then_some(Self { total_bytes })
            .ok_or(LocalRunDirectoryError::StateInvalid)
    }

    pub(super) fn account(&mut self, bytes: &[u8]) -> Result<(), LocalRunDirectoryError> {
        let size = u64::try_from(bytes.len()).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
        account_retained_bytes(&mut self.total_bytes, size, MAXIMUM_RETAINED_TOTAL_BYTES)
    }
}

fn account_retained_bytes(
    total_bytes: &mut u64,
    size: u64,
    maximum_bytes: u64,
) -> Result<(), LocalRunDirectoryError> {
    *total_bytes = total_bytes
        .checked_add(size)
        .filter(|total| *total <= maximum_bytes)
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    Ok(())
}

pub(super) struct StableLocalRunSnapshot {
    pub(super) run_directory: PathBuf,
    pub(super) root: OwnedFd,
    pub(super) run: LocalRunV1,
    pub(super) state: LocalRunStateV1,
    pub(super) retained_json_bytes: u64,
    pub(super) lock_held: bool,
}

pub(super) fn read_stable_local_run_snapshot(
    requested: &Path,
) -> Result<StableLocalRunSnapshot, LocalStatusError> {
    let normalized = std::fs::canonicalize(requested).map_err(|_| LocalStatusError {
        code: LocalStatusErrorCode::RunDirectoryUnavailable,
        run_directory: None,
    })?;
    let reported_directory = normalized.to_str().map(|_| normalized.clone());
    if reported_directory.is_none() {
        return Err(LocalStatusError {
            code: LocalStatusErrorCode::RunDirectoryUnavailable,
            run_directory: None,
        });
    }
    let root = open_directory_path(&normalized).map_err(|_| LocalStatusError {
        code: LocalStatusErrorCode::RunDirectoryUnavailable,
        run_directory: reported_directory.clone(),
    })?;
    let (run, run_bytes) =
        read_run_with_size(&root).map_err(|_| invalid_status_error(&reported_directory))?;
    let lock = open_status_lock(&root, &reported_directory)?;

    for _ in 0..STATUS_SNAPSHOT_ATTEMPTS {
        let (before, _) =
            read_state_with_size(&root).map_err(|_| invalid_status_error(&reported_directory))?;
        if before.local_run_id != run.local_run_id {
            return Err(invalid_status_error(&reported_directory));
        }
        let lock_held = query_status_lock(&lock).map_err(|()| LocalStatusError {
            code: LocalStatusErrorCode::LockQueryFailed,
            run_directory: reported_directory.clone(),
        })?;
        let (after, state_bytes) =
            read_state_with_size(&root).map_err(|_| invalid_status_error(&reported_directory))?;
        if after.local_run_id != run.local_run_id {
            return Err(invalid_status_error(&reported_directory));
        }
        verify_status_lock_identity(&root, &lock, &reported_directory)?;
        if before.revision != after.revision {
            continue;
        }
        let retained_json_bytes = run_bytes
            .checked_add(state_bytes)
            .ok_or_else(|| invalid_status_error(&reported_directory))?;
        RetainedReadBudget::with_bytes(retained_json_bytes)
            .map_err(|_| invalid_status_error(&reported_directory))?;
        return Ok(StableLocalRunSnapshot {
            run_directory: normalized,
            root,
            run,
            state: after,
            retained_json_bytes,
            lock_held,
        });
    }

    Err(LocalStatusError {
        code: LocalStatusErrorCode::StatusSnapshotUnstable,
        run_directory: reported_directory,
    })
}

pub(crate) fn read_local_run_status(
    requested: &Path,
) -> Result<LocalRunStatusSnapshot, LocalStatusError> {
    let snapshot = read_stable_local_run_snapshot(requested)?;
    let reported_directory = Some(snapshot.run_directory.clone());
    let run_value = serde_json::to_value(&snapshot.run)
        .map_err(|_| invalid_status_error(&reported_directory))?;
    status_snapshot(
        snapshot.run_directory,
        run_value,
        snapshot.state,
        snapshot.lock_held,
    )
    .map_err(|_| invalid_status_error(&reported_directory))
}

fn invalid_status_error(run_directory: &Option<PathBuf>) -> LocalStatusError {
    LocalStatusError {
        code: LocalStatusErrorCode::RunDirectoryInvalid,
        run_directory: run_directory.clone(),
    }
}

fn open_status_lock(
    root: &OwnedFd,
    run_directory: &Option<PathBuf>,
) -> Result<File, LocalStatusError> {
    let metadata = statat(root, LOCK_FILE, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| invalid_status_error(run_directory))?;
    if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile {
        return Err(invalid_status_error(run_directory));
    }
    let lock = openat(
        root,
        LOCK_FILE,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| LocalStatusError {
        code: LocalStatusErrorCode::LockQueryFailed,
        run_directory: run_directory.clone(),
    })?;
    verify_status_lock_identity(root, &lock, run_directory)?;
    Ok(lock)
}

fn verify_status_lock_identity(
    root: &OwnedFd,
    lock: &File,
    run_directory: &Option<PathBuf>,
) -> Result<(), LocalStatusError> {
    let opened = fstat(lock).map_err(|_| invalid_status_error(run_directory))?;
    let current = statat(root, LOCK_FILE, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| invalid_status_error(run_directory))?;
    if FileType::from_raw_mode(opened.st_mode) != FileType::RegularFile
        || FileType::from_raw_mode(current.st_mode) != FileType::RegularFile
        || opened.st_dev != current.st_dev
        || opened.st_ino != current.st_ino
    {
        return Err(invalid_status_error(run_directory));
    }
    Ok(())
}

fn query_status_lock(lock: &File) -> Result<bool, ()> {
    let requested = Flock {
        start: 0,
        length: 0,
        pid: None,
        typ: FlockType::WriteLock,
        offset_type: FlockOffsetType::Set,
    };
    fcntl_getlk(lock, &requested)
        .map(|blocking| blocking.is_some())
        .map_err(|_| ())
}

fn status_snapshot(
    run_directory: PathBuf,
    run: Value,
    state: LocalRunStateV1,
    lock_held: bool,
) -> Result<LocalRunStatusSnapshot, ()> {
    let current = state.attempts.last().ok_or(())?;
    let recovery = recovery_status(current, lock_held);
    let retry = retry_eligibility(current.state, &recovery, lock_held);
    let current_result = status_result(&current.result);
    let attempts = state
        .attempts
        .iter()
        .map(|attempt| LocalStatusAttempt {
            attempt_number: attempt.attempt_number,
            trigger: attempt_trigger_name(attempt.trigger),
            state: attempt_state_name(attempt.state),
            result: status_result(&attempt.result),
        })
        .collect();
    let state_value = serde_json::to_value(&state).map_err(|_| ())?;
    Ok(LocalRunStatusSnapshot {
        run_directory,
        run,
        state: state_value,
        current_attempt_number: current.attempt_number,
        current_attempt_state: attempt_state_name(current.state),
        current_result,
        attempts,
        recovery,
        retry,
    })
}

trait LocalRecoveryAuthority {
    fn execution_host(&self) -> Result<ExecutionHostV1, ()>;

    fn observe_process(&self, guard: &ProcessGuardV1) -> ProcessIdentityObservation;
}

struct SystemLocalRecoveryAuthority;

impl LocalRecoveryAuthority for SystemLocalRecoveryAuthority {
    fn execution_host(&self) -> Result<ExecutionHostV1, ()> {
        execution_host().map_err(|_| ())
    }

    fn observe_process(&self, guard: &ProcessGuardV1) -> ProcessIdentityObservation {
        process_identity_observation(guard)
    }
}

trait LocalQuiescenceAuthority: LocalRecoveryAuthority {
    fn terminate_process(&self, guard: &ProcessGuardV1) -> AuthenticatedSignalResult;

    fn wait_for_process_change(&self);
}

impl LocalQuiescenceAuthority for SystemLocalRecoveryAuthority {
    fn terminate_process(&self, guard: &ProcessGuardV1) -> AuthenticatedSignalResult {
        authenticated_process_group(guard)
            .map_or(AuthenticatedSignalResult::Unavailable, |identity| {
                terminate_authenticated_process_group(&identity)
            })
    }

    fn wait_for_process_change(&self) {
        crate::timing::sleep(QUIESCENCE_POLL_INTERVAL);
    }
}

fn begin_local_retry(
    pending: PendingLocalRetry,
    admitted: &AdmittedWorkflow,
    authority: &impl LocalQuiescenceAuthority,
) -> Result<LocalAttemptOwner, LocalRetryBeginError> {
    verify_retry_lock_identity(&pending.root, &pending.lock)?;
    if admitted.workflow().content_digest.algorithm.as_str()
        != pending.workflow.content_digest.algorithm.as_str()
        || admitted.workflow().content_digest.value != pending.workflow.content_digest.value
    {
        return Err(LocalRunDirectoryError::StateConflict.into());
    }
    if existing_run_overlaps_execution_root(&pending, admitted)? {
        return Err(LocalRunDirectoryError::ExecutionRootOverlap.into());
    }

    let current = lock_state(&pending.state.current)?.clone();
    let prior = current
        .attempts
        .last()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let recovery = recovery_status_with(prior, false, authority);
    if let LocalRetryEligibility::Ineligible(reason) =
        retry_eligibility(prior.state, &recovery, false)
    {
        return Err(LocalRetryBeginError::Rejected(retry_rejection(
            pending.normalized.clone(),
            prior.attempt_number,
            reason,
            &recovery,
        )));
    }

    let next_attempt_number = prior
        .attempt_number
        .checked_add(1)
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let next_attempt = retry_attempt(admitted, next_attempt_number, prior.attempt_number)?;
    if let Err((guard_ids, ownership_reason)) = quiesce_attempt(prior, authority) {
        return Err(LocalRetryBeginError::Rejected(LocalRetryRejection {
            run_directory: pending.normalized.clone(),
            attempt_number: prior.attempt_number,
            reason: RetryIneligibilityReason::OwnershipUnproven,
            guard_ids,
            ownership_reason: Some(ownership_reason),
        }));
    }

    let attempt_directory = create_or_verify_attempt_directory(&pending.root, next_attempt_number)?;
    pending.state.update(|state| {
        if state.current_attempt_number != prior.attempt_number {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        let current_attempt = current_attempt_mut(state)?;
        if !current_attempt.state.is_terminal() {
            for guard in &mut current_attempt.process_guards {
                guard.state = ProcessGuardStateV1::Quiesced;
            }
            let execution_may_have_started = current_attempt.started_at.is_some();
            settle_interrupted_attempt(
                current_attempt,
                InterruptionCauseV1::ExecutionOwnerLost,
                execution_may_have_started,
            )?;
        }
        state.current_attempt_number = next_attempt_number;
        state.attempts.push(next_attempt.clone());
        Ok(())
    })?;

    let attempt_directory_name =
        attempt_directory_name(next_attempt_number).ok_or(LocalRunDirectoryError::StateInvalid)?;
    let private_directory = pending.normalized.join(PRIVATE_DIRECTORY);
    let result_directory = pending
        .normalized
        .join(ATTEMPTS_DIRECTORY)
        .join(attempt_directory_name)
        .join("result");
    Ok(LocalAttemptOwner {
        normalized: pending.normalized,
        root: pending.root,
        lock: Some(pending.lock),
        private_directory,
        attempt_directory,
        result_directory,
        attempt_number: next_attempt_number,
        finalizers: Arc::from(fresh_finalizer_progress(admitted)?),
        state: pending.state,
    })
}

fn existing_run_overlaps_execution_root(
    pending: &PendingLocalRetry,
    admitted: &AdmittedWorkflow,
) -> Result<bool, LocalRunDirectoryError> {
    Ok(
        paths_overlap(&pending.normalized, admitted.execution().root())
            || admitted
                .execution()
                .root_identity()
                .contains_directory(&pending.root)
                .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?,
    )
}

fn retry_attempt(
    admitted: &AdmittedWorkflow,
    attempt_number: u64,
    prior_attempt_number: u64,
) -> Result<LocalAttemptV1, LocalRunDirectoryError> {
    fresh_attempt(
        admitted,
        attempt_number,
        AttemptTriggerV1::ExplicitRetry,
        Some(prior_attempt_number),
        timestamp(crate::timing::utc_now())?,
    )
}

fn fresh_attempt(
    admitted: &AdmittedWorkflow,
    attempt_number: u64,
    trigger: AttemptTriggerV1,
    prior_attempt_number: Option<u64>,
    created_at: String,
) -> Result<LocalAttemptV1, LocalRunDirectoryError> {
    let execution_root = admitted
        .execution()
        .root()
        .to_str()
        .ok_or(LocalRunDirectoryError::InvalidPath)?
        .to_owned();
    let steps = admitted
        .workflow()
        .definition
        .presentation_order
        .iter()
        .map(|id| {
            let node = admitted
                .workflow()
                .definition
                .steps
                .get(id)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            fresh_progress_node(id, AttemptNodeRoleV1::Step, node)
        })
        .collect::<Result<Vec<_>, LocalRunDirectoryError>>()?;
    Ok(LocalAttemptV1 {
        attempt_id: generate_uuid()?,
        attempt_number,
        trigger,
        prior_attempt_number,
        state: AttemptStateV1::Created,
        execution_root,
        created_at,
        started_at: None,
        settled_at: None,
        owner: AttemptOwnerV1 {
            owner_nonce: generate_uuid()?,
            execution_host: execution_host()?,
        },
        cancellation: None,
        interruption: None,
        rejection: None,
        progress: AttemptProgressV1 {
            accepted_occurrence_ordinal: 0,
            last_transition_sequence: 0,
            steps,
            outstanding_actions: Vec::new(),
        },
        finalization: None,
        process_guards: Vec::new(),
        result: AttemptResultV1::NotPublished {
            reason: ResultAbsentReasonV1::AttemptNonterminal,
        },
    })
}

fn fresh_finalizer_progress(
    admitted: &AdmittedWorkflow,
) -> Result<Vec<AttemptStepV1>, LocalRunDirectoryError> {
    admitted
        .workflow()
        .definition
        .finalizer_presentation_order
        .iter()
        .map(|id| {
            let node = &admitted
                .workflow()
                .definition
                .finalizers
                .get(id)
                .ok_or(LocalRunDirectoryError::StateInvalid)?
                .body;
            fresh_progress_node(id, AttemptNodeRoleV1::Finalizer, node)
        })
        .collect()
}

fn fresh_progress_node(
    id: &str,
    role: AttemptNodeRoleV1,
    node: &super::validated::ValidatedStep,
) -> Result<AttemptStepV1, LocalRunDirectoryError> {
    let failure_policy = match node {
        super::validated::ValidatedStep::Command(command) => command.common.failure_policy,
        super::validated::ValidatedStep::Agent(agent) => agent.common.failure_policy,
    };
    Ok(AttemptStepV1 {
        id: id.to_owned(),
        role,
        failure_policy,
        state: AttemptStepStateV1::Pending,
    })
}

fn quiesce_attempt(
    attempt: &LocalAttemptV1,
    authority: &impl LocalQuiescenceAuthority,
) -> Result<(), (Vec<String>, OwnershipUnprovenReason)> {
    let guards = attempt
        .process_guards
        .iter()
        .filter(|guard| !matches!(guard.state, ProcessGuardStateV1::Quiesced))
        .collect::<Vec<_>>();
    if guards.is_empty() {
        return Ok(());
    }
    let current_host = authority.execution_host().map_err(|()| {
        (
            guards.iter().map(|guard| guard.guard_id.clone()).collect(),
            OwnershipUnprovenReason::ExecutionHostIdentityUnavailable,
        )
    })?;
    let matching = guards
        .iter()
        .copied()
        .filter(|guard| guard.execution_host == current_host)
        .collect::<Vec<_>>();
    let mut exact = Vec::new();
    let mut unproven = Vec::new();
    for guard in matching {
        match authority.observe_process(guard) {
            ProcessIdentityObservation::Exact { .. } => exact.push(guard),
            ProcessIdentityObservation::Absent => {}
            ProcessIdentityObservation::Unavailable => unproven.push(guard.guard_id.clone()),
        }
    }
    if !unproven.is_empty() {
        return Err(process_inspection_unproven(unproven));
    }
    for guard in &exact {
        match authority.terminate_process(guard) {
            AuthenticatedSignalResult::Signalled | AuthenticatedSignalResult::Absent => {}
            AuthenticatedSignalResult::Unavailable => unproven.push(guard.guard_id.clone()),
        }
    }
    if !unproven.is_empty() {
        return Err(process_inspection_unproven(unproven));
    }
    for _ in 0..QUIESCENCE_POLL_ATTEMPTS {
        let mut surviving = Vec::new();
        let mut unavailable = Vec::new();
        for guard in &exact {
            match authority.observe_process(guard) {
                ProcessIdentityObservation::Absent => {}
                ProcessIdentityObservation::Exact { .. } => surviving.push(*guard),
                ProcessIdentityObservation::Unavailable => {
                    unavailable.push(guard.guard_id.clone());
                }
            }
        }
        if !unavailable.is_empty() {
            return Err(process_inspection_unproven(unavailable));
        }
        if surviving.is_empty() {
            return Ok(());
        }
        authority.wait_for_process_change();
    }
    Err(process_inspection_unproven(
        exact.iter().map(|guard| guard.guard_id.clone()).collect(),
    ))
}

fn process_inspection_unproven(guard_ids: Vec<String>) -> (Vec<String>, OwnershipUnprovenReason) {
    (
        guard_ids,
        OwnershipUnprovenReason::ProcessIdentityInspectionUnavailable,
    )
}

fn create_or_verify_attempt_directory(
    root: &OwnedFd,
    attempt_number: u64,
) -> Result<OwnedFd, LocalRunDirectoryError> {
    let attempts = open_directory_at(root, ATTEMPTS_DIRECTORY)?;
    let name =
        attempt_directory_name(attempt_number).ok_or(LocalRunDirectoryError::StateInvalid)?;
    match mkdirat(&attempts, &name, Mode::RWXU) {
        Ok(()) => {
            sync_directory(&attempts)?;
            open_directory_at(&attempts, &name)
        }
        Err(Errno::EXIST) => {
            let attempt = open_directory_at(&attempts, &name)?;
            if directory_entries(&attempt)?.is_empty() {
                Ok(attempt)
            } else {
                Err(LocalRunDirectoryError::StateConflict)
            }
        }
        Err(_) => Err(LocalRunDirectoryError::StagingUnavailable),
    }
}

fn recovery_status(attempt: &LocalAttemptV1, lock_held: bool) -> LocalRecoveryStatus {
    recovery_status_with(attempt, lock_held, &SystemLocalRecoveryAuthority)
}

fn recovery_status_with(
    attempt: &LocalAttemptV1,
    lock_held: bool,
    authority: &impl LocalRecoveryAuthority,
) -> LocalRecoveryStatus {
    if lock_held {
        return LocalRecoveryStatus::Active;
    }
    if attempt.state.is_terminal() {
        return LocalRecoveryStatus::Settled;
    }
    let guards = attempt
        .process_guards
        .iter()
        .filter(|guard| !matches!(guard.state, ProcessGuardStateV1::Quiesced))
        .collect::<Vec<_>>();
    if guards.is_empty() {
        return LocalRecoveryStatus::Abandoned;
    }
    let current_host = match authority.execution_host() {
        Ok(host) => host,
        Err(()) => {
            return LocalRecoveryStatus::OwnershipUnproven {
                guard_ids: guards.iter().map(|guard| guard.guard_id.clone()).collect(),
                reason: OwnershipUnprovenReason::ExecutionHostIdentityUnavailable,
            };
        }
    };
    let guard_ids = guards
        .iter()
        .filter(|guard| guard.execution_host == current_host)
        .filter(|guard| {
            matches!(
                authority.observe_process(guard),
                ProcessIdentityObservation::Unavailable
            )
        })
        .map(|guard| guard.guard_id.clone())
        .collect::<Vec<_>>();
    if guard_ids.is_empty() {
        LocalRecoveryStatus::Abandoned
    } else {
        LocalRecoveryStatus::OwnershipUnproven {
            guard_ids,
            reason: OwnershipUnprovenReason::ProcessIdentityInspectionUnavailable,
        }
    }
}

fn retry_eligibility(
    state: AttemptStateV1,
    recovery: &LocalRecoveryStatus,
    lock_held: bool,
) -> LocalRetryEligibility {
    let reason = if lock_held {
        Some(RetryIneligibilityReason::RunLocked)
    } else if matches!(recovery, LocalRecoveryStatus::OwnershipUnproven { .. }) {
        Some(RetryIneligibilityReason::OwnershipUnproven)
    } else if matches!(state, AttemptStateV1::Succeeded) {
        Some(RetryIneligibilityReason::LatestAttemptSucceeded)
    } else if matches!(state, AttemptStateV1::Rejected) {
        Some(RetryIneligibilityReason::LatestAttemptRejected)
    } else {
        None
    };
    reason.map_or(
        LocalRetryEligibility::Eligible,
        LocalRetryEligibility::Ineligible,
    )
}

fn status_result(result: &AttemptResultV1) -> LocalStatusResult {
    match result {
        AttemptResultV1::NotPublished { reason } => LocalStatusResult::NotPublished {
            reason: result_absent_reason_name(*reason),
        },
        AttemptResultV1::Published { relative_directory } => LocalStatusResult::Published {
            relative_directory: relative_directory.clone(),
        },
        AttemptResultV1::PublicationFailed { phase } => LocalStatusResult::PublicationFailed {
            phase: publication_failure_phase_name(*phase),
        },
    }
}

const fn attempt_trigger_name(trigger: AttemptTriggerV1) -> &'static str {
    match trigger {
        AttemptTriggerV1::Initial => "initial",
        AttemptTriggerV1::ExplicitRetry => "explicit_retry",
    }
}

const fn attempt_state_name(state: AttemptStateV1) -> &'static str {
    match state {
        AttemptStateV1::Created => "created",
        AttemptStateV1::Running => "running",
        AttemptStateV1::Cancelling => "cancelling",
        AttemptStateV1::Succeeded => "succeeded",
        AttemptStateV1::WorkflowFailed => "workflow_failed",
        AttemptStateV1::Cancelled => "cancelled",
        AttemptStateV1::Interrupted => "interrupted",
        AttemptStateV1::Rejected => "rejected",
    }
}

const fn result_absent_reason_name(reason: ResultAbsentReasonV1) -> &'static str {
    match reason {
        ResultAbsentReasonV1::AttemptNonterminal => "attempt_nonterminal",
        ResultAbsentReasonV1::PublicationPending => "publication_pending",
        ResultAbsentReasonV1::Interrupted => "interrupted",
        ResultAbsentReasonV1::Rejected => "rejected",
    }
}

const fn publication_failure_phase_name(phase: PublicationFailurePhaseV1) -> &'static str {
    match phase {
        PublicationFailurePhaseV1::ExportCopy => "export_copy",
        PublicationFailurePhaseV1::Serialization => "serialization",
        PublicationFailurePhaseV1::Close => "close",
        PublicationFailurePhaseV1::Verification => "verification",
        PublicationFailurePhaseV1::Rename => "rename",
    }
}

fn process_identity_observation(guard: &ProcessGuardV1) -> ProcessIdentityObservation {
    authenticated_process_group(guard).map_or(ProcessIdentityObservation::Unavailable, |identity| {
        system_process_identity_observation(&identity)
    })
}

fn authenticated_process_group(guard: &ProcessGuardV1) -> Option<AuthenticatedProcessGroup> {
    if !matches!(
        guard.liveness.kind,
        ProcessLivenessKindV1::LeaderStartIdentity
    ) {
        return None;
    }
    let process_group = i32::try_from(guard.process_group_id)
        .ok()
        .and_then(rustix::process::Pid::from_raw)?;
    AuthenticatedProcessGroup::new(process_group, guard.liveness.value.clone())
}

fn decode_run(bytes: &[u8]) -> Result<LocalRunV1, LocalRunDirectoryError> {
    let run: LocalRunV1 = decode_schema_one(bytes)?;
    validate_run(&run)?;
    Ok(run)
}

fn decode_state(bytes: &[u8]) -> Result<LocalRunStateV1, LocalRunDirectoryError> {
    let state: LocalRunStateV1 = decode_schema_one(bytes)?;
    validate_state(&state)?;
    Ok(state)
}

fn decode_schema_one<Document>(bytes: &[u8]) -> Result<Document, LocalRunDirectoryError>
where
    Document: for<'de> Deserialize<'de>,
{
    if bytes.starts_with(&[0xef, 0xbb, 0xbf]) || !bytes.ends_with(b"\n") {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    if contains_null(&value)
        || value
            .get("schemaVersion")
            .and_then(Value::as_u64)
            .is_none_or(|version| version != 1)
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    serde_json::from_value(value).map_err(|_| LocalRunDirectoryError::StateInvalid)
}

fn contains_null(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Array(values) => values.iter().any(contains_null),
        Value::Object(properties) => properties.values().any(contains_null),
        Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

fn validate_run(run: &LocalRunV1) -> Result<(), LocalRunDirectoryError> {
    if run.schema_version != 1
        || !is_canonical_uuid(&run.local_run_id)
        || !valid_timestamp(&run.created_at)
        || !run.workflow_digest.validate()
        || !run.workflow_manifest_digest.validate()
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok(())
}

fn validate_manifest(manifest: &WorkflowManifestV1) -> Result<(), LocalRunDirectoryError> {
    if manifest.schema_version != 1
        || !is_canonical_relative_path(&manifest.workflow_path)
        || !is_canonical_absolute_path(&manifest.source_root)
        || !(1..=256).contains(&manifest.maximum_parallel_steps)
        || manifest.source_files.is_empty()
    {
        return Err(LocalRunDirectoryError::SerializationUnavailable);
    }
    let mut source_paths = BTreeSet::new();
    let mut previous_source_path: Option<&[u8]> = None;
    for source in &manifest.source_files {
        if !is_canonical_relative_path(&source.path) {
            return Err(LocalRunDirectoryError::SerializationUnavailable);
        }
        if !source_paths.insert(source.path.as_str()) {
            return Err(LocalRunDirectoryError::SerializationUnavailable);
        }
        if previous_source_path.is_some_and(|path| path >= source.path.as_bytes()) {
            return Err(LocalRunDirectoryError::SerializationUnavailable);
        }
        previous_source_path = Some(source.path.as_bytes());
    }
    if !source_paths.contains(manifest.workflow_path.as_str()) {
        return Err(LocalRunDirectoryError::SerializationUnavailable);
    }
    let mut expected_ordinal = 1_u64;
    for file in manifest
        .source_files
        .iter()
        .map(|source| &source.file)
        .chain(manifest.imports.prompt.iter())
        .chain(
            manifest
                .imports
                .attachments
                .iter()
                .map(|attachment| &attachment.file),
        )
    {
        if file.ordinal != expected_ordinal
            || file.relative_file
                != format!(
                    "{WORKFLOW_FILES_DIRECTORY}/{}",
                    retained_file_name(expected_ordinal)?
                )
            || !file.digest.validate()
        {
            return Err(LocalRunDirectoryError::SerializationUnavailable);
        }
        expected_ordinal = expected_ordinal
            .checked_add(1)
            .ok_or(LocalRunDirectoryError::SerializationUnavailable)?;
    }
    Ok(())
}

fn validate_state(state: &LocalRunStateV1) -> Result<(), LocalRunDirectoryError> {
    if state.schema_version != 1
        || !is_canonical_uuid(&state.local_run_id)
        || state.revision == 0
        || state.current_attempt_number == 0
        || state.attempts.is_empty()
        || state.attempts.last().map(|attempt| attempt.attempt_number)
            != Some(state.current_attempt_number)
        || state.diagnostics.len() > MAXIMUM_DIAGNOSTICS
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    for (index, attempt) in state.attempts.iter().enumerate() {
        let number = u64::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        validate_attempt(attempt, number)?;
    }
    let mut prior_sequence = 0;
    for diagnostic in &state.diagnostics {
        if diagnostic.sequence == 0
            || diagnostic.sequence <= prior_sequence
            || diagnostic.attempt_number == 0
            || diagnostic.attempt_number > state.current_attempt_number
            || diagnostic.action_id == Some(0)
            || diagnostic
                .guard_id
                .as_deref()
                .is_some_and(|guard| !is_canonical_uuid(guard))
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        prior_sequence = diagnostic.sequence;
    }
    Ok(())
}

fn validate_attempt(
    attempt: &LocalAttemptV1,
    expected_number: u64,
) -> Result<(), LocalRunDirectoryError> {
    let terminal = attempt.state.is_terminal();
    let valid_trigger = match attempt.trigger {
        AttemptTriggerV1::Initial => {
            attempt.attempt_number == 1 && attempt.prior_attempt_number.is_none()
        }
        AttemptTriggerV1::ExplicitRetry => {
            attempt.prior_attempt_number == Some(expected_number - 1)
        }
    };
    if attempt.attempt_number != expected_number
        || !valid_trigger
        || !is_canonical_uuid(&attempt.attempt_id)
        || !is_canonical_absolute_path(&attempt.execution_root)
        || !valid_timestamp(&attempt.created_at)
        || attempt
            .started_at
            .as_deref()
            .is_some_and(|value| !valid_timestamp(value))
        || attempt
            .settled_at
            .as_deref()
            .is_some_and(|value| !valid_timestamp(value))
        || terminal != attempt.settled_at.is_some()
        || !validate_owner(&attempt.owner)
        || attempt.progress.steps.is_empty()
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    if matches!(attempt.state, AttemptStateV1::Created) && attempt.started_at.is_some() {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    if !matches!(
        attempt.state,
        AttemptStateV1::Created | AttemptStateV1::Rejected
    ) && attempt.started_at.is_none()
        && !matches!(
            attempt.interruption,
            Some(AttemptInterruptionV1 {
                execution_may_have_started: false,
                ..
            })
        )
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let finalization_cancelled =
        attempt
            .finalization
            .as_ref()
            .is_some_and(|finalization| match finalization {
                AttemptFinalizationV1::Progress(progress) => progress.cancellation.is_some(),
                AttemptFinalizationV1::Complete(complete) => complete.cancellation.is_some(),
            });
    if attempt.cancellation.as_ref().is_some_and(|cancellation| {
        cancellation.reason == CancellationReasonV1::FinalizationForceAbort
            || !valid_timestamp(&cancellation.requested_at)
            || !valid_timestamp(&cancellation.force_stop_deadline)
    }) || matches!(
        attempt.state,
        AttemptStateV1::Cancelling | AttemptStateV1::Cancelled
    ) && attempt.cancellation.is_none()
        && !finalization_cancelled
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    if attempt.cancellation.as_ref().is_some_and(|cancellation| {
        cancellation.workflow_confirmed != matches!(attempt.state, AttemptStateV1::Cancelled)
    }) || attempt.interruption.as_ref().is_some_and(|interruption| {
        interruption.cancellation_requested
            != (attempt.cancellation.is_some() || finalization_cancelled)
    }) {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    if matches!(attempt.state, AttemptStateV1::Interrupted) != attempt.interruption.is_some()
        || matches!(attempt.state, AttemptStateV1::Rejected) != attempt.rejection.is_some()
        || (!matches!(attempt.state, AttemptStateV1::Interrupted) && attempt.interruption.is_some())
        || (!matches!(attempt.state, AttemptStateV1::Rejected) && attempt.rejection.is_some())
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let mut step_ids = BTreeSet::new();
    for step in &attempt.progress.steps {
        if step.id.is_empty()
            || step.role != AttemptNodeRoleV1::Step
            || !step_ids.insert(step.id.as_str())
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
    }
    validate_attempt_finalization(attempt, &mut step_ids)?;
    let mut action_ids = BTreeSet::new();
    let mut prior_action_id = 0;
    for action in &attempt.progress.outstanding_actions {
        let requires_step = !matches!(action.kind, OutstandingActionKindV1::FinishRun);
        if action.action_id == 0
            || action.action_id <= prior_action_id
            || !action_ids.insert(action.action_id)
            || requires_step != action.step_id.is_some()
            || requires_step != action.node_role.is_some()
            || action
                .step_id
                .as_deref()
                .is_some_and(|id| attempt_node_role(attempt, id) != action.node_role)
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        prior_action_id = action.action_id;
    }
    if terminal && !attempt.progress.outstanding_actions.is_empty() {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let mut guard_ids = BTreeSet::new();
    for guard in &attempt.process_guards {
        if !is_canonical_uuid(&guard.guard_id)
            || !guard_ids.insert(guard.guard_id.as_str())
            || guard.action_id == 0
            || !step_ids.contains(guard.step_id.as_str())
            || attempt_node_role(attempt, &guard.step_id) != Some(guard.node_role)
            || !validate_execution_host(&guard.execution_host)
            || guard.process_group_id <= 0
            || guard.liveness.value.is_empty()
            || guard.liveness.value.len() > 256
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
    }
    validate_attempt_result(attempt)
}

fn validate_attempt_finalization<'a>(
    attempt: &'a LocalAttemptV1,
    node_ids: &mut BTreeSet<&'a str>,
) -> Result<(), LocalRunDirectoryError> {
    let Some(finalization) = &attempt.finalization else {
        return Ok(());
    };
    match finalization {
        AttemptFinalizationV1::Progress(progress) => {
            if progress.complete
                || progress.finalizers.is_empty()
                || progress.finalizers.iter().any(|finalizer| {
                    finalizer.role != AttemptNodeRoleV1::Finalizer
                        || finalizer.id.is_empty()
                        || !node_ids.insert(finalizer.id.as_str())
                })
            {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            if matches!(
                attempt.state,
                AttemptStateV1::Created | AttemptStateV1::Rejected
            ) {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            if !valid_finalization_interruption(
                progress.cancellation.as_ref(),
                progress.force_abort,
            ) {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            if matches!(
                attempt.state,
                AttemptStateV1::Succeeded
                    | AttemptStateV1::WorkflowFailed
                    | AttemptStateV1::Cancelled
            ) {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
        }
        AttemptFinalizationV1::Complete(complete) => {
            if !complete.complete
                || complete.finalizers.is_empty()
                || !matches!(
                    attempt.state,
                    AttemptStateV1::Succeeded
                        | AttemptStateV1::WorkflowFailed
                        | AttemptStateV1::Cancelled
                )
                || complete.finalizers.iter().any(|finalizer| {
                    finalizer.role != AttemptNodeRoleV1::Finalizer
                        || finalizer.id.is_empty()
                        || !node_ids.insert(finalizer.id.as_str())
                        || !durable_finalizer_valid(finalizer)
                })
            {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            let expected_issues = complete
                .finalizers
                .iter()
                .filter(|finalizer| {
                    finalizer.state == AttemptStepStateV1::Failed
                        || (finalizer.state == AttemptStepStateV1::Blocked
                            && finalizer.reason == Some(StepReasonV1::InputUnavailable))
                })
                .map(|finalizer| (finalizer.id.as_str(), finalizer.failure_policy))
                .collect::<Vec<_>>();
            if complete.issues.len() != expected_issues.len()
                || complete
                    .issues
                    .iter()
                    .zip(expected_issues)
                    .any(|(issue, (id, impact))| issue.finalizer_id != id || issue.impact != impact)
            {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            if !valid_finalization_interruption(
                complete.cancellation.as_ref(),
                complete.force_abort,
            ) {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
        }
    }
    Ok(())
}

fn valid_finalization_interruption(
    cancellation: Option<&DurableFinalizationCancellationV1>,
    force_abort: bool,
) -> bool {
    match (cancellation, force_abort) {
        (None, false) => true,
        (Some(cancellation), false) => {
            cancellation.reason != CancellationReasonV1::FinalizationForceAbort
                && cancellation
                    .force_stop_deadline
                    .as_deref()
                    .is_some_and(valid_timestamp)
        }
        (Some(cancellation), true) => {
            (cancellation.reason == CancellationReasonV1::FinalizationForceAbort
                && cancellation.force_stop_deadline.is_none())
                || (cancellation.reason != CancellationReasonV1::FinalizationForceAbort
                    && cancellation
                        .force_stop_deadline
                        .as_deref()
                        .is_some_and(valid_timestamp))
        }
        (None, true) => false,
    }
}

fn durable_finalizer_valid(finalizer: &DurableFinalizerV1) -> bool {
    match finalizer.state {
        AttemptStepStateV1::Succeeded => {
            finalizer.failure.is_none()
                && finalizer.reason.is_none()
                && finalizer.unavailable_references.is_none()
        }
        AttemptStepStateV1::Failed => {
            finalizer
                .failure
                .as_ref()
                .is_some_and(|failure| super::result_metadata::validate_failure(failure).is_ok())
                && finalizer.reason.is_none()
                && finalizer.unavailable_references.is_none()
        }
        AttemptStepStateV1::Blocked => {
            finalizer.failure.is_none()
                && finalizer.reason == Some(StepReasonV1::InputUnavailable)
                && finalizer
                    .unavailable_references
                    .as_ref()
                    .is_some_and(|references| {
                        !references.is_empty()
                            && references.windows(2).all(|pair| pair[0] < pair[1])
                    })
        }
        AttemptStepStateV1::NotRun => {
            finalizer.failure.is_none()
                && finalizer.reason == Some(StepReasonV1::FinalizerTriggerNotSelected)
                && finalizer.unavailable_references.is_none()
        }
        AttemptStepStateV1::Cancelled => {
            finalizer.failure.is_none()
                && matches!(
                    finalizer.reason,
                    Some(
                        StepReasonV1::UserRequest
                            | StepReasonV1::TerminationRequest
                            | StepReasonV1::CallerOutputFailure
                            | StepReasonV1::RunnerShutdown
                            | StepReasonV1::ExecutionLeaseExpired
                            | StepReasonV1::FinalizationForceAbort
                    )
                )
                && finalizer.unavailable_references.is_none()
        }
        AttemptStepStateV1::Pending
        | AttemptStepStateV1::Starting
        | AttemptStepStateV1::Running
        | AttemptStepStateV1::CapturingOutputs
        | AttemptStepStateV1::Cancelling => false,
    }
}

fn validate_attempt_result(attempt: &LocalAttemptV1) -> Result<(), LocalRunDirectoryError> {
    let valid = match (&attempt.state, &attempt.result) {
        (
            AttemptStateV1::Created | AttemptStateV1::Running | AttemptStateV1::Cancelling,
            AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::AttemptNonterminal,
            },
        ) => true,
        (
            AttemptStateV1::Succeeded | AttemptStateV1::WorkflowFailed | AttemptStateV1::Cancelled,
            AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            }
            | AttemptResultV1::PublicationFailed { .. },
        ) => true,
        (
            AttemptStateV1::Interrupted,
            AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::Interrupted,
            },
        ) => true,
        (
            AttemptStateV1::Rejected,
            AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::Rejected,
            },
        ) => true,
        (_, AttemptResultV1::Published { relative_directory }) => {
            *relative_directory == attempt_result_relative_path(attempt.attempt_number)
                && matches!(
                    attempt.state,
                    AttemptStateV1::Succeeded
                        | AttemptStateV1::WorkflowFailed
                        | AttemptStateV1::Cancelled
                )
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(LocalRunDirectoryError::StateInvalid)
    }
}

fn validate_owner(owner: &AttemptOwnerV1) -> bool {
    is_canonical_uuid(&owner.owner_nonce) && validate_execution_host(&owner.execution_host)
}

fn validate_execution_host(host: &ExecutionHostV1) -> bool {
    !host.value.is_empty() && host.value.len() <= 256
}

fn read_regular_file(parent: &OwnedFd, name: &str) -> Result<Vec<u8>, LocalRunDirectoryError> {
    read_regular_file_bounded(parent, name, MAXIMUM_DURABLE_JSON_BYTES)
}

fn read_regular_file_bounded(
    parent: &OwnedFd,
    name: &str,
    maximum_bytes: u64,
) -> Result<Vec<u8>, LocalRunDirectoryError> {
    let file = openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    let metadata = fstat(&file).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile
        || metadata.st_size < 0
        || u64::try_from(metadata.st_size)
            .ok()
            .is_none_or(|size| size > maximum_bytes)
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let mut file = File::from(file);
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    if u64::try_from(bytes.len())
        .ok()
        .is_none_or(|size| size > maximum_bytes)
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok(bytes)
}

fn encode_json(document: &impl Serialize) -> Result<Vec<u8>, LocalRunDirectoryError> {
    let mut bytes = serde_json::to_vec_pretty(document)
        .map_err(|_| LocalRunDirectoryError::SerializationUnavailable)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn write_new_immutable_file(
    parent: &OwnedFd,
    name: &str,
    bytes: &[u8],
) -> Result<(), LocalRunDirectoryError> {
    let mut file = create_file(parent, name, Mode::RUSR | Mode::WUSR)?;
    file.write_all(bytes)
        .and_then(|()| file.flush())
        .and_then(|()| file.sync_all())
        .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?;
    fchmod(file.as_fd(), Mode::RUSR).map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)
}

fn write_new_state_file(parent: &OwnedFd, bytes: &[u8]) -> Result<(), LocalRunDirectoryError> {
    let mut file = create_file(parent, STATE_FILE, Mode::RUSR | Mode::WUSR)?;
    file.write_all(bytes)
        .and_then(|()| file.flush())
        .and_then(|()| file.sync_all())
        .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)
}

fn create_file(parent: &OwnedFd, name: &str, mode: Mode) -> Result<File, LocalRunDirectoryError> {
    openat(
        parent,
        name,
        OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        mode,
    )
    .map(File::from)
    .map_err(|_| LocalRunDirectoryError::StagingUnavailable)
}

fn mkdir(parent: &OwnedFd, name: impl rustix::path::Arg) -> Result<(), LocalRunDirectoryError> {
    mkdirat(parent, name, Mode::RWXU).map_err(|_| LocalRunDirectoryError::StagingUnavailable)
}

pub(super) fn open_directory_at(
    parent: &OwnedFd,
    name: impl rustix::path::Arg,
) -> Result<OwnedFd, LocalRunDirectoryError> {
    openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| LocalRunDirectoryError::StagingUnavailable)
}

fn sync_directory(directory: &OwnedFd) -> Result<(), LocalRunDirectoryError> {
    let duplicate = dup(directory).map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?;
    File::from(duplicate)
        .sync_all()
        .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)
}

fn ensure_absent(parent: &OwnedFd, name: &std::ffi::OsStr) -> Result<(), LocalRunDirectoryError> {
    match statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(_) => Err(LocalRunDirectoryError::DestinationExists),
        Err(Errno::NOENT) => Ok(()),
        Err(_) => Err(LocalRunDirectoryError::ParentUnavailable),
    }
}

fn run_directory_overlaps_execution_root(
    run_directory: &Path,
    execution_path: &Path,
    execution_root: &AdmittedExecutionRoot,
    run_parent: &OwnedFd,
) -> Result<bool, LocalRunDirectoryError> {
    Ok(paths_overlap(run_directory, execution_path)
        || execution_root
            .contains_directory(run_parent)
            .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?)
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn directory_entries(directory: &OwnedFd) -> Result<BTreeSet<Vec<u8>>, LocalRunDirectoryError> {
    directory_entry_names(directory).map_err(|_| LocalRunDirectoryError::StateInvalid)
}

fn timestamp(value: OffsetDateTime) -> Result<String, LocalRunDirectoryError> {
    utc_timestamp(value).map_err(|_| LocalRunDirectoryError::SerializationUnavailable)
}

fn valid_timestamp(value: &str) -> bool {
    value.ends_with('Z') && OffsetDateTime::parse(value, &Rfc3339).is_ok()
}

fn generate_uuid() -> Result<String, LocalRunDirectoryError> {
    super::identity::random_uuid_v4().map_err(|()| LocalRunDirectoryError::IdentityUnavailable)
}

fn is_canonical_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
        })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn execution_host() -> Result<ExecutionHostV1, LocalRunDirectoryError> {
    let value = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map_err(|_| LocalRunDirectoryError::HostIdentityUnavailable)?;
    let value = value.trim_end_matches(['\r', '\n']).to_ascii_lowercase();
    validated_execution_host(value)
}

#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "the host-boot identity boundary reads the fixed kern.bootsessionuuid sysctl"
)]
fn execution_host() -> Result<ExecutionHostV1, LocalRunDirectoryError> {
    let mut length = 0_usize;
    // SAFETY: the fixed C string is valid and the null output pointer requests only the size.
    let result = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            std::ptr::null_mut(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 || length == 0 || length > 257 {
        return Err(LocalRunDirectoryError::HostIdentityUnavailable);
    }
    let mut bytes = vec![0_u8; length];
    // SAFETY: sysctl writes at most the reported length into the allocated byte buffer.
    let result = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            bytes.as_mut_ptr().cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 {
        return Err(LocalRunDirectoryError::HostIdentityUnavailable);
    }
    bytes.truncate(length);
    if bytes.last() == Some(&0) {
        bytes.pop();
    }
    let value = String::from_utf8(bytes)
        .map_err(|_| LocalRunDirectoryError::HostIdentityUnavailable)?
        .to_ascii_lowercase();
    validated_execution_host(value)
}

fn validated_execution_host(value: String) -> Result<ExecutionHostV1, LocalRunDirectoryError> {
    if !is_canonical_uuid(&value) {
        return Err(LocalRunDirectoryError::HostIdentityUnavailable);
    }
    Ok(ExecutionHostV1 {
        kind: ExecutionHostKindV1::HostBoot,
        value,
    })
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn execution_host() -> Result<ExecutionHostV1, LocalRunDirectoryError> {
    Err(LocalRunDirectoryError::HostIdentityUnavailable)
}

#[cfg(test)]
mod tests;
