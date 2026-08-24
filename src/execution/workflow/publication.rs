use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use ring::digest::{Context as DigestContext, SHA256};
use rustix::fd::OwnedFd;
use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, RenameFlags, fstat, mkdirat, openat, renameat_with, statat,
    unlinkat,
};
use rustix::io::Errno;
use serde::de::Error as _;
use serde::ser::SerializeStruct as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use time::OffsetDateTime;

use super::admission::CancellationReason;
use super::agent::{AgentFailure, AgentFailureCause, AgentProtocolRejectionDiagnostic};
use super::agent_input::AgentInputStartFailure;
use super::artifact::{ArtifactExposeFailure, ArtifactStaging, CaptureFailureKind, StagedCarrier};
use super::artifact_set;
use super::canonical_json;
use super::diagnostic::{CapturedDiagnosticStream, StepDiagnostic};
use super::document::{FailurePolicy, FinalizationTrigger};
use super::execution_root::open_directory;
use super::git_capture::GitCaptureFailure;
use super::input::InputPreparationFailureKind;
use super::private_staging::{directory_entry_names, same_file};
use super::resolution::WorkflowContentDigest;
use super::result_metadata;
use super::runtime::{
    ActiveStepInvocation, ExportSet, ExportUnavailableReason, ExportValue, FailurePhase,
    NotRunReason, RecoveryHandlerFailurePhase, RecoveryHandlerKind, RecoveryHandlerOutcome,
    RecoveryTerminalDisposition, RunOutcome, StepFailure, StepRecoveryState, StepState,
};
use super::schema_common::{lowercase_hex, utc_timestamp};
use super::step_runtime::{
    CommandExecutionFailure, CommandLaunchFailure, CommandPreparationFailure, OutputCaptureFailure,
    StepExecutionFailure, StepFailureCause, StepStartFailure, WorkingDirectoryFailure,
};
use super::validated::WorkflowNodeRole;
use super::validated::{ResolvedOutputSource, WorkflowValueType};
use super::value::CapturedValue;

const COMMAND: &str = "scherzo-cloud workflow run";
const RETRY_COMMAND: &str = "scherzo-cloud workflow retry";
const RESULT_FILE: &str = "result.json";
const EXPORT_DIRECTORY: &str = "exports";
const STAGING_ATTEMPTS: usize = 16;

#[derive(Clone, Debug)]
pub(crate) struct WorkflowRunTiming {
    pub(crate) started_at: OffsetDateTime,
    pub(crate) finished_at: OffsetDateTime,
    pub(crate) duration: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowStepTiming {
    pub(crate) started_at: OffsetDateTime,
    pub(crate) duration: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct WorkflowRunCancellation {
    pub(crate) reason: CancellationReason,
    pub(crate) force_stop_deadline: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunStepKind {
    Command,
    Agent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunStep {
    pub(crate) id: String,
    pub(crate) role: WorkflowNodeRole,
    pub(crate) kind: WorkflowRunStepKind,
    pub(crate) failure_policy: FailurePolicy,
    pub(crate) state: StepState<StepFailureCause, CapturedValue>,
    pub(crate) timing: Option<WorkflowStepTiming>,
    pub(crate) command_output: Option<StepDiagnostic>,
    pub(crate) recovery: Option<StepRecoverySummaryV1>,
    pub(crate) invocations: Vec<RecoveryInvocationV1>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunFinalization {
    pub(crate) trigger: FinalizationTrigger,
    pub(crate) finalizers: Vec<WorkflowRunStep>,
    pub(crate) cancellation: Option<WorkflowRunFinalizationCancellation>,
    pub(crate) force_abort: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunFinalizationCancellation {
    pub(crate) reason: CancellationReason,
    pub(crate) force_stop_deadline: Option<OffsetDateTime>,
}

#[derive(Clone, Debug)]
pub(crate) struct WorkflowRunResult {
    pub(crate) run_directory: PathBuf,
    pub(crate) attempt_number: u64,
    pub(crate) workflow_path: String,
    pub(crate) source_root: PathBuf,
    pub(crate) content_digest: WorkflowContentDigest,
    pub(crate) execution_root: PathBuf,
    pub(crate) maximum_parallel_steps: NonZeroUsize,
    pub(crate) timing: WorkflowRunTiming,
    pub(crate) outcome: RunOutcome<StepFailureCause>,
    pub(crate) cancellation: Option<WorkflowRunCancellation>,
    pub(crate) steps: Vec<WorkflowRunStep>,
    pub(crate) finalization: Option<WorkflowRunFinalization>,
    pub(crate) exports: ExportSet<CapturedValue>,
    pub(crate) export_sources: BTreeMap<String, ResolvedOutputSource>,
}

#[derive(Clone)]
pub(crate) enum CloudCarrierBody {
    Staged(StagedCarrier),
    Bytes(Arc<[u8]>),
}

#[derive(Clone)]
pub(crate) struct CloudResultCarrier {
    pub(crate) portable_owner_path: String,
    pub(crate) idempotency_key: String,
    pub(crate) media_type: String,
    pub(crate) size_bytes: u64,
    pub(crate) sha256: String,
    pub(crate) body: CloudCarrierBody,
}

pub(crate) struct PreparedCloudWorkflowResult {
    pub(crate) result_json: Arc<[u8]>,
    pub(crate) carriers: Vec<CloudResultCarrier>,
}

pub(crate) fn summary_disposition_matches(
    summarized: &StepState<StepFailureCause, ()>,
    state: &StepState<StepFailureCause, CapturedValue>,
) -> bool {
    match (summarized, state) {
        (StepState::Succeeded { .. }, StepState::Succeeded { .. }) => true,
        (
            StepState::Failed {
                phase: left_phase,
                cause: left_cause,
            },
            StepState::Failed {
                phase: right_phase,
                cause: right_cause,
            },
        ) => left_phase == right_phase && left_cause == right_cause,
        (
            StepState::Blocked {
                dependency: left_dependency,
            },
            StepState::Blocked {
                dependency: right_dependency,
            },
        ) => left_dependency == right_dependency,
        (
            StepState::InputUnavailable {
                references: left_references,
            },
            StepState::InputUnavailable {
                references: right_references,
            },
        ) => left_references == right_references,
        (
            StepState::NotRun {
                reason: left_reason,
            },
            StepState::NotRun {
                reason: right_reason,
            },
        ) => left_reason == right_reason,
        (
            StepState::Cancelled {
                reason: left_reason,
            },
            StepState::Cancelled {
                reason: right_reason,
            },
        ) => left_reason == right_reason,
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalPublicationPhase {
    TargetValidation,
    Staging,
    ExportCopy,
    Serialization,
    Close,
    Verification,
    Commit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalPublicationFailureKind {
    InvalidResultPath,
    ParentUnavailable,
    DestinationExists,
    StagingUnavailable,
    CarrierHandoffUnavailable,
    ExportWriteUnavailable,
    UnsupportedExport,
    InvalidRunResult,
    ResultConflict,
    SerializationUnavailable,
    VerificationUnavailable,
    AtomicPublicationUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalPublicationError {
    phase: LocalPublicationPhase,
    kind: LocalPublicationFailureKind,
    export: Option<String>,
}

impl LocalPublicationError {
    pub(crate) fn phase(&self) -> LocalPublicationPhase {
        self.phase
    }

    pub(crate) fn kind(&self) -> LocalPublicationFailureKind {
        self.kind
    }

    pub(crate) fn export(&self) -> Option<&str> {
        self.export.as_deref()
    }

    fn new(phase: LocalPublicationPhase, kind: LocalPublicationFailureKind) -> Self {
        Self {
            phase,
            kind,
            export: None,
        }
    }

    fn for_export(
        phase: LocalPublicationPhase,
        kind: LocalPublicationFailureKind,
        export: &str,
    ) -> Self {
        Self {
            phase,
            kind,
            export: Some(export.to_owned()),
        }
    }
}

impl fmt::Display for LocalPublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "local result publication failure during {:?}: {:?}",
            self.phase, self.kind
        )?;
        if let Some(export) = &self.export {
            write!(formatter, " for export {export:?}")?;
        }
        Ok(())
    }
}

impl std::error::Error for LocalPublicationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkflowOutcomeV1 {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowRunTerminalResultV1 {
    schema_version: u8,
    command: &'static str,
    outcome: WorkflowOutcomeV1,
    exit_status: u16,
    run_directory: String,
    attempt_number: u64,
    result_directory: String,
    result: WorkflowResultV1,
}

impl WorkflowRunTerminalResultV1 {
    pub(crate) fn outcome(&self) -> WorkflowOutcomeV1 {
        self.outcome
    }

    pub(crate) fn exit_status(&self) -> u16 {
        self.exit_status
    }

    pub(crate) fn run_directory(&self) -> &str {
        &self.run_directory
    }

    pub(crate) fn attempt_number(&self) -> u64 {
        self.attempt_number
    }

    pub(crate) fn result_directory(&self) -> &str {
        &self.result_directory
    }

    pub(crate) fn result(&self) -> &WorkflowResultV1 {
        &self.result
    }

    pub(crate) fn mark_retry(&mut self) {
        self.command = RETRY_COMMAND;
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WorkflowResultV1 {
    pub(crate) schema_version: u8,
    pub(crate) attempt_number: u64,
    pub(crate) workflow: WorkflowIdentityV1,
    pub(crate) execution: WorkflowExecutionV1,
    pub(crate) command_output_policy: CommandOutputPolicyV1,
    pub(crate) outcome: WorkflowOutcomeV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) primary_failure: Option<PrimaryFailureV1>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) cancellation: Option<CancellationV1>,
    pub(crate) steps: Vec<WorkflowStepV1>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) finalization: Option<FinalizationV1>,
    pub(crate) exports: BTreeMap<String, ExportV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowIdentityV1 {
    pub(crate) path: String,
    pub(crate) provenance: WorkflowProvenanceV1,
    pub(crate) digest: DigestV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum WorkflowProvenanceV1 {
    Local {
        #[serde(rename = "sourceRoot")]
        source_root: String,
    },
    Cloud {
        #[serde(rename = "projectId")]
        project_id: String,
        #[serde(rename = "repositoryConnectionId")]
        repository_connection_id: String,
        #[serde(rename = "objectFormat")]
        object_format: String,
        #[serde(rename = "commitOid")]
        commit_oid: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WorkflowExecutionV1 {
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) execution_root: Option<String>,
    pub(crate) maximum_parallel_steps: usize,
    pub(crate) started_at: String,
    pub(crate) finished_at: String,
    pub(crate) duration_milliseconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CommandOutputPolicyV1 {
    pub(crate) encoding: String,
    pub(crate) maximum_retained_bytes_per_stream: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DigestV1 {
    pub(crate) algorithm: String,
    pub(crate) value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CancellationV1 {
    pub(crate) reason: CancellationReasonV1,
    pub(crate) force_stop_deadline: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CancellationReasonV1 {
    UserRequest,
    TerminationRequest,
    CallerOutputFailure,
    RunnerShutdown,
    ExecutionLeaseExpired,
    FinalizationForceAbort,
}

// The published result's terminal-only enum must remain closed independently of the
// durable attempt projection, which also admits live states.
// jscpd:ignore-start
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkflowStepStateV1 {
    Succeeded,
    Failed,
    Blocked,
    NotRun,
    Cancelled,
}
// jscpd:ignore-end

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkflowNodeRoleV1 {
    Step,
    Finalizer,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowNodeV1 {
    pub(crate) id: String,
    pub(crate) role: WorkflowNodeRoleV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WorkflowStepV1 {
    pub(crate) id: String,
    pub(crate) role: WorkflowNodeRoleV1,
    pub(crate) kind: String,
    pub(crate) failure_policy: FailurePolicy,
    pub(crate) state: WorkflowStepStateV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) recovery: Option<StepRecoverySummaryV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) invocations: Vec<RecoveryInvocationV1>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) started_at: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) duration_milliseconds: Option<u64>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) failure: Option<FailureV1>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) dependency: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) reason: Option<StepReasonV1>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) unavailable_references: Option<Vec<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) command_output: Option<CommandOutputV1>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryHandlerKindV1 {
    Cmd,
    Agent,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct StepRecoverySummaryV1 {
    pub(crate) schema_version: u8,
    pub(crate) configured_retries: u8,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) handler_kind: Option<RecoveryHandlerKindV1>,
    pub(crate) rounds: Vec<RecoveryRoundSummaryV1>,
    pub(crate) termination: RecoveryTerminationV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryRoundSummaryV1 {
    pub(crate) number: u8,
    pub(crate) failed_execution: RecoveryFailedExecutionV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) handler: Option<RecoveryHandlerSummaryV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryFailedExecutionV1 {
    pub(crate) execution_number: u8,
    pub(crate) invocation_id: u64,
    pub(crate) failure: FailureV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryHandlerOutcomeV1 {
    Recheck,
    GaveUp,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryHandlerSummaryV1 {
    pub(crate) kind: RecoveryHandlerKindV1,
    pub(crate) invocation_id: u64,
    pub(crate) outcome: RecoveryHandlerOutcomeV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) summary: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) reason: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) failure: Option<RecoveryHandlerFailureV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RecoveryTerminationV1 {
    Recovered {
        #[serde(rename = "executionNumber")]
        execution_number: u8,
    },
    Exhausted {
        #[serde(rename = "executionNumber")]
        execution_number: u8,
    },
    GaveUp {
        round: u8,
    },
    HandlerFailed {
        round: u8,
        #[serde(rename = "handlerFailure")]
        handler_failure: RecoveryHandlerFailureV1,
    },
    Cancelled {
        round: u8,
        #[serde(rename = "activeRole")]
        active_role: RecoveryInvocationRoleV1,
        #[serde(
            rename = "executionNumber",
            default,
            deserialize_with = "deserialize_non_null_option",
            skip_serializing_if = "Option::is_none"
        )]
        execution_number: Option<u8>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryHandlerFailurePhaseV1 {
    Start,
    Execution,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryHandlerFailureV1 {
    pub(crate) phase: RecoveryHandlerFailurePhaseV1,
    pub(crate) cause: RecoveryHandlerFailureCauseV1,
}

// Recovery handler and target failures are separate closed schemas; keeping each field
// explicit is clearer than a generic cause that could admit fields at the wrong boundary.
// jscpd:ignore-start
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryHandlerFailureCauseV1 {
    pub(crate) code: RecoveryHandlerFailureCodeV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) exit_code: Option<i32>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) decision_rejection: Option<RecoveryDecisionRejectionV1>,
}
// jscpd:ignore-end

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryHandlerFailureCodeV1 {
    ContextUnavailable,
    HandlerUnavailable,
    WorkingDirectoryUnavailable,
    CommandPreparationFailed,
    CommandLaunchFailed,
    CommandWaitFailed,
    CommandExitFailed,
    ProcessQuiescenceFailed,
    ResultMissing,
    ResultSymbolicLink,
    ResultNotRegular,
    ResultUnavailable,
    ResultTooLarge,
    DecisionInvalid,
    AgentInputFailed,
    AgentFailed,
    AgentResultMissing,
    AgentResultInvalid,
    SettlementFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryDecisionRejectionV1 {
    InputTooLarge,
    InvalidUtf8,
    InvalidJson,
    DuplicateKey,
    UnknownField,
    UnsupportedSchemaVersion,
    UnknownDecision,
    EmptySummary,
    SummaryTooLong,
    EmptyReason,
    ReasonTooLong,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryInvocationRoleV1 {
    Target,
    RecoveryHandler,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryInvocationStateV1 {
    Settled,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryInvocationV1 {
    pub(crate) invocation_id: u64,
    pub(crate) role: RecoveryInvocationRoleV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) target_execution: Option<u8>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) recovery_round: Option<u8>,
    pub(crate) state: RecoveryInvocationStateV1,
    pub(crate) started_at: String,
    pub(crate) finished_at: String,
    pub(crate) duration_milliseconds: u64,
    pub(crate) usage: RecoveryInvocationUsageV1,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) diagnostics: Vec<RecoveryInvocationDiagnosticV1>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) diagnostic_reference: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryInvocationUsageV1 {
    pub(crate) input_tokens: u64,
    pub(crate) output_tokens: u64,
}

pub(crate) fn total_recovery_usage(
    invocations: &[RecoveryInvocationV1],
) -> RecoveryInvocationUsageV1 {
    invocations
        .iter()
        .fold(RecoveryInvocationUsageV1::default(), |total, invocation| {
            RecoveryInvocationUsageV1 {
                input_tokens: total
                    .input_tokens
                    .saturating_add(invocation.usage.input_tokens),
                output_tokens: total
                    .output_tokens
                    .saturating_add(invocation.usage.output_tokens),
            }
        })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryDiagnosticKindV1 {
    CommandStdout,
    CommandStderr,
    AgentHarnessStdout,
    AgentHarnessStderr,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryInvocationDiagnosticV1 {
    pub(crate) kind: RecoveryDiagnosticKindV1,
    pub(crate) reference: String,
    pub(crate) stream: DiagnosticStreamV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FinalizationV1 {
    pub(crate) trigger: FinalizationTriggerV1,
    pub(crate) finalizers: Vec<WorkflowStepV1>,
    pub(crate) issues: Vec<FinalizationIssueV1>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) cancellation: Option<FinalizationCancellationV1>,
    pub(crate) force_abort: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FinalizationTriggerV1 {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FinalizationIssueV1 {
    pub(crate) node: WorkflowNodeV1,
    pub(crate) impact: FailurePolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FinalizationCancellationV1 {
    pub(crate) reason: CancellationReasonV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) force_stop_deadline: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StepReasonV1 {
    FailureStop,
    UserRequest,
    TerminationRequest,
    CallerOutputFailure,
    RunnerShutdown,
    ExecutionLeaseExpired,
    FinalizationForceAbort,
    InputUnavailable,
    FinalizerTriggerNotSelected,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FailureV1 {
    pub(crate) phase: FailurePhaseV1,
    pub(crate) cause: FailureCauseV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FailurePhaseV1 {
    Start,
    Execution,
    OutputCapture,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FailureCodeV1 {
    StepUnavailable,
    PreparationTaskUnavailable,
    InputsUnavailable,
    OutputsUnsupported,
    AgentRuntimeUnavailable,
    AgentStepUnavailable,
    AgentAdmissionUnavailable,
    AgentInputsUnavailable,
    AgentInputMissingUpstream,
    AgentInputTypeMismatch,
    AgentSourceUnavailable,
    AgentSourceTextInvalid,
    AgentResultSchemaUnavailable,
    AgentValueModeInvalid,
    AgentAttachmentCountLimit,
    AgentAttachmentBytesLimit,
    ArtifactStagingMismatch,
    AgentStagingMismatch,
    AgentInputStagingUnavailable,
    HarnessStartFailed,
    HarnessInputTooLarge,
    HarnessFailed,
    HarnessProtocolFailed,
    MissingResponse,
    MissingResult,
    ResultValidationLimitExceeded,
    CapturedValueTooLarge,
    ResultSettlementFailed,
    InputInvalidName,
    InputValueCountLimit,
    InputValueSizeLimit,
    InputTotalSizeLimit,
    InputCollectionOrdinalLimit,
    InputTypeMismatch,
    InputSourceUnavailable,
    InputStagingUnavailable,
    InputLiveLimit,
    ExecutionRootRebound,
    WorkingDirectoryUnavailable,
    WorkingDirectoryEscape,
    WorkingDirectoryNotDirectory,
    CommandArgvInvalid,
    CommandPathUnconfigured,
    ExecutableNotFound,
    ExecutableUnavailable,
    CommandLaunchNotFound,
    CommandLaunchPermissionDenied,
    CommandLaunchInvalidInput,
    CommandLaunchFailed,
    CommandExit,
    CommandWaitFailed,
    OutputUnsupported,
    CaptureTaskUnavailable,
    OutputPathAbsolute,
    OutputPathEscape,
    OutputPathEmpty,
    OutputMissing,
    OutputSymbolicLink,
    OutputParentNotDirectory,
    OutputNotRegularFile,
    OutputSourceUnavailable,
    CapturedFileCountLimit,
    CapturedFileSizeLimit,
    CapturedTotalSizeLimit,
    CapturedGitCarrierCountLimit,
    CapturedGitCarrierSizeLimit,
    CapturedTotalGitCarrierSizeLimit,
    GitExecutionRootRebound,
    GitHeadUnavailable,
    GitBaselineNotAncestor,
    GitCleanlinessUnavailable,
    GitWorkspaceDirty,
    GitTreeUnavailable,
    GitRequiredObjectsUnavailable,
    GitSourceAuthorityChanged,
    GitStructureLimitExceeded,
    GitCommandTimedOut,
    GitBundleGenerationFailed,
    GitBundleProfileInvalid,
    GitBundleVerificationFailed,
    GitWorkspaceChanged,
    GitTemporaryStorageUnavailable,
    OutputStagingUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FailureCauseV1 {
    pub(crate) code: FailureCodeV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) input: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) collection_index: Option<usize>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) output: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) exit_code: Option<i32>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) protocol_rejection: Option<AgentProtocolRejectionDiagnostic>,
}

fn deserialize_non_null_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

impl FailureCauseV1 {
    fn code(code: FailureCodeV1) -> Self {
        Self {
            code,
            input: None,
            collection_index: None,
            output: None,
            exit_code: None,
            protocol_rejection: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrimaryFailureV1 {
    pub(crate) node: WorkflowNodeV1,
    pub(crate) phase: FailurePhaseV1,
    pub(crate) cause: FailureCauseV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommandOutputV1 {
    pub(crate) stdout: DiagnosticStreamV1,
    pub(crate) stderr: DiagnosticStreamV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DiagnosticStreamV1 {
    pub(crate) encoding: String,
    pub(crate) data: String,
    pub(crate) retained_bytes: u64,
    pub(crate) discarded_bytes: u64,
    pub(crate) truncated: bool,
    pub(crate) fully_drained: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExportV1 {
    Available {
        kind: String,
        media_type: String,
        path: String,
        size_bytes: u64,
        digest: DigestV1,
    },
    GitBranch {
        artifact_version: u8,
        object_format: String,
        base_oid: String,
        head_oid: String,
        tree_oid: String,
        carrier: Option<GitBranchCarrierV1>,
    },
    Unavailable {
        reason: ExportUnavailableReasonV1,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GitBranchCarrierV1 {
    pub(crate) path: String,
    pub(crate) media_type: String,
    pub(crate) size_bytes: u64,
    pub(crate) digest: DigestV1,
}

impl Serialize for ExportV1 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Available {
                kind,
                media_type,
                path,
                size_bytes,
                digest,
            } => {
                let mut state = serializer.serialize_struct("AvailableExportV1", 6)?;
                state.serialize_field("state", "available")?;
                state.serialize_field("kind", kind)?;
                state.serialize_field("mediaType", media_type)?;
                state.serialize_field("path", path)?;
                state.serialize_field("sizeBytes", size_bytes)?;
                state.serialize_field("digest", digest)?;
                state.end()
            }
            Self::GitBranch {
                artifact_version,
                object_format,
                base_oid,
                head_oid,
                tree_oid,
                carrier,
            } => {
                let mut state = serializer
                    .serialize_struct("GitBranchExportV1", if carrier.is_some() { 8 } else { 7 })?;
                state.serialize_field("state", "available")?;
                state.serialize_field("kind", "git_branch")?;
                state.serialize_field("artifactVersion", artifact_version)?;
                state.serialize_field("objectFormat", object_format)?;
                state.serialize_field("baseOid", base_oid)?;
                state.serialize_field("headOid", head_oid)?;
                state.serialize_field("treeOid", tree_oid)?;
                if let Some(carrier) = carrier {
                    state.serialize_field("carrier", carrier)?;
                }
                state.end()
            }
            Self::Unavailable { reason } => {
                let mut state = serializer.serialize_struct("UnavailableExportV1", 2)?;
                state.serialize_field("state", "unavailable")?;
                state.serialize_field("reason", reason)?;
                state.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ExportV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let state = value.get("state").and_then(Value::as_str);
        let kind = value.get("kind").and_then(Value::as_str);
        match (state, kind) {
            (Some("available"), Some("git_branch")) => {
                let wire = serde_json::from_value::<GitBranchExportWire>(value)
                    .map_err(D::Error::custom)?;
                if wire.state != "available" || wire.kind != "git_branch" {
                    return Err(D::Error::custom("invalid Git branch export"));
                }
                Ok(Self::GitBranch {
                    artifact_version: wire.artifact_version,
                    object_format: wire.object_format,
                    base_oid: wire.base_oid,
                    head_oid: wire.head_oid,
                    tree_oid: wire.tree_oid,
                    carrier: wire.carrier,
                })
            }
            (Some("available"), Some(_)) => {
                let wire = serde_json::from_value::<AvailableExportWire>(value)
                    .map_err(D::Error::custom)?;
                if wire.state != "available" {
                    return Err(D::Error::custom("invalid available export"));
                }
                Ok(Self::Available {
                    kind: wire.kind,
                    media_type: wire.media_type,
                    path: wire.path,
                    size_bytes: wire.size_bytes,
                    digest: wire.digest,
                })
            }
            (Some("unavailable"), None) => {
                let wire = serde_json::from_value::<UnavailableExportWire>(value)
                    .map_err(D::Error::custom)?;
                if wire.state != "unavailable" {
                    return Err(D::Error::custom("invalid unavailable export"));
                }
                Ok(Self::Unavailable {
                    reason: wire.reason,
                })
            }
            _ => Err(D::Error::custom("invalid export state or kind")),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AvailableExportWire {
    state: String,
    kind: String,
    media_type: String,
    path: String,
    size_bytes: u64,
    digest: DigestV1,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GitBranchExportWire {
    state: String,
    kind: String,
    artifact_version: u8,
    object_format: String,
    base_oid: String,
    head_oid: String,
    tree_oid: String,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    carrier: Option<GitBranchCarrierV1>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnavailableExportWire {
    state: String,
    reason: ExportUnavailableReasonV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum ExportUnavailableReasonV1 {
    #[serde(rename = "source_failed")]
    Failed,
    #[serde(rename = "source_blocked")]
    Blocked,
    #[serde(rename = "source_input_unavailable")]
    InputUnavailable,
    #[serde(rename = "source_not_run")]
    NotRun,
    #[serde(rename = "source_trigger_not_selected")]
    TriggerNotSelected,
    #[serde(rename = "source_cancelled")]
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PublicationBoundary {
    StagingCreated,
    BeforeExportMaterialization { export: String },
    AfterExportMaterialization { export: String },
    BeforeSerialization,
    StagingComplete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StagedFile {
    Export { export: String },
    Result,
}

trait PublicationObserver {
    fn observe(&mut self, _boundary: &PublicationBoundary) -> Result<(), ()> {
        Ok(())
    }

    fn close_staged_file(&mut self, file: File, _staged_file: &StagedFile) -> io::Result<()> {
        close_file(file)
    }
}

struct NoopPublicationObserver;

impl PublicationObserver for NoopPublicationObserver {}

pub(crate) struct PreparedResultDestination {
    target: PublicationTarget,
}

impl PreparedResultDestination {
    pub(crate) fn result_directory(&self) -> &str {
        &self.target.normalized
    }
}

pub(crate) fn prepare_result_destination(
    destination: &Path,
) -> Result<PreparedResultDestination, LocalPublicationError> {
    PublicationTarget::validate(destination, None, None)
        .map(|target| PreparedResultDestination { target })
}

pub(crate) fn prepare_attempt_result_destination(
    destination: &Path,
    private_staging: &Path,
    expected_result_parent: &OwnedFd,
    expected_staging_parent: &OwnedFd,
) -> Result<PreparedResultDestination, LocalPublicationError> {
    PublicationTarget::validate(
        destination,
        Some(private_staging),
        Some((expected_result_parent, expected_staging_parent)),
    )
    .map(|target| PreparedResultDestination { target })
}

pub(crate) fn publish_prepared_workflow_result(
    destination: &PreparedResultDestination,
    artifacts: &ArtifactStaging,
    run: &WorkflowRunResult,
) -> Result<WorkflowRunTerminalResultV1, LocalPublicationError> {
    publish_prepared_with_observer(destination, artifacts, run, &mut NoopPublicationObserver)
}

pub(crate) fn publish_workflow_result(
    destination: &Path,
    artifacts: &ArtifactStaging,
    run: &WorkflowRunResult,
) -> Result<WorkflowRunTerminalResultV1, LocalPublicationError> {
    let destination = prepare_result_destination(destination)?;
    publish_prepared_workflow_result(&destination, artifacts, run)
}

pub(crate) fn prepare_cloud_workflow_result(
    run: &WorkflowRunResult,
    project_id: String,
    repository_connection_id: String,
    object_format: String,
    commit_oid: String,
) -> Result<PreparedCloudWorkflowResult, LocalPublicationError> {
    if !run.exports.keys().eq(run.export_sources.keys()) {
        return Err(invalid_run_result());
    }
    let mut exports = BTreeMap::new();
    let mut carriers = Vec::new();
    let mut sources = BTreeMap::<(String, String), SourcePublication>::new();
    for (index, (name, export)) in run.exports.iter().enumerate() {
        let ordinal = index.checked_add(1).ok_or_else(invalid_run_result)?;
        let source = run
            .export_sources
            .get(name)
            .ok_or_else(invalid_run_result)?;
        let identity = (source.node.id.clone(), source.output.clone());
        let metadata = match export {
            ExportValue::Unavailable { reason } => {
                unavailable_export(&mut sources, identity, source, *reason)?
            }
            ExportValue::Available { output } => {
                if !captured_type_matches(source.value_type, output) {
                    return Err(invalid_run_result());
                }
                match existing_available_export(&sources, &identity, source, output)? {
                    Some(metadata) => metadata,
                    None => {
                        let (metadata, carrier) =
                            cloud_available_export(name, ordinal, source, output)?;
                        if let Some(carrier) = carrier {
                            carriers.push(carrier);
                        }
                        insert_available_source(&mut sources, identity, source, output, &metadata);
                        metadata
                    }
                }
            }
        };
        exports.insert(name.clone(), metadata);
    }
    let result = build_result_with_provenance(
        run,
        WorkflowProvenanceV1::Cloud {
            project_id,
            repository_connection_id,
            object_format,
            commit_oid,
        },
        exports,
    )?;
    let result_json = encode_result_json(&result)?;
    Ok(PreparedCloudWorkflowResult {
        result_json: Arc::from(result_json),
        carriers,
    })
}

fn encode_result_json(result: &WorkflowResultV1) -> Result<Vec<u8>, LocalPublicationError> {
    result_metadata::validate(result).map_err(|_| invalid_run_result())?;
    let mut bytes = serde_json::to_vec_pretty(result).map_err(|_| {
        LocalPublicationError::new(
            LocalPublicationPhase::Serialization,
            LocalPublicationFailureKind::SerializationUnavailable,
        )
    })?;
    bytes.push(b'\n');
    if u64::try_from(bytes.len())
        .ok()
        .is_none_or(|size| size > result_metadata::MAXIMUM_RESULT_JSON_BYTES)
    {
        return Err(LocalPublicationError::new(
            LocalPublicationPhase::Serialization,
            LocalPublicationFailureKind::SerializationUnavailable,
        ));
    }
    Ok(bytes)
}

fn cloud_available_export(
    name: &str,
    ordinal: usize,
    source: &ResolvedOutputSource,
    output: &CapturedValue,
) -> Result<(ExportV1, Option<CloudResultCarrier>), LocalPublicationError> {
    let path = format!("{EXPORT_DIRECTORY}/{ordinal:04}");
    let idempotency_key = format!("capture:{}:{}", source.node.id, source.output);
    let (metadata, body) = match output {
        CapturedValue::File(file) => (
            ExportV1::Available {
                kind: "file".to_owned(),
                media_type: file.media_type().to_owned(),
                path: path.clone(),
                size_bytes: file.size(),
                digest: DigestV1 {
                    algorithm: "sha256".to_owned(),
                    value: file.sha256().to_owned(),
                },
            },
            Some(CloudCarrierBody::Staged(file.carrier().clone())),
        ),
        CapturedValue::Text(text) => byte_backed_cloud_export(
            name,
            "text",
            "text/plain; charset=utf-8",
            &path,
            Arc::from(text.as_bytes()),
        )?,
        CapturedValue::Json(value) => {
            let mut encoded = Vec::new();
            canonical_json::to_writer(&mut encoded, value).map_err(|_| export_write_error(name))?;
            byte_backed_cloud_export(name, "json", "application/json", &path, Arc::from(encoded))?
        }
        CapturedValue::GitBranch(branch) => {
            let branch_metadata = branch.metadata();
            let carrier = branch.carrier().map(|carrier| GitBranchCarrierV1 {
                path: path.clone(),
                media_type: carrier.media_type().to_owned(),
                size_bytes: carrier.size(),
                digest: DigestV1 {
                    algorithm: "sha256".to_owned(),
                    value: carrier.sha256().to_owned(),
                },
            });
            if (branch_metadata.base_oid() != branch_metadata.head_oid()) != carrier.is_some() {
                return Err(invalid_run_result());
            }
            let body = branch
                .carrier()
                .map(|carrier| CloudCarrierBody::Staged(carrier.staged().clone()));
            (
                ExportV1::GitBranch {
                    artifact_version: branch_metadata.artifact_version(),
                    object_format: branch_metadata.object_format().as_str().to_owned(),
                    base_oid: branch_metadata.base_oid().to_owned(),
                    head_oid: branch_metadata.head_oid().to_owned(),
                    tree_oid: branch_metadata.tree_oid().to_owned(),
                    carrier,
                },
                body,
            )
        }
    };
    let carrier = body.map(|body| {
        let (media_type, size_bytes, sha256) = match &metadata {
            ExportV1::Available {
                media_type,
                size_bytes,
                digest,
                ..
            } => (media_type.clone(), *size_bytes, digest.value.clone()),
            ExportV1::GitBranch {
                carrier: Some(carrier),
                ..
            } => (
                carrier.media_type.clone(),
                carrier.size_bytes,
                carrier.digest.value.clone(),
            ),
            ExportV1::GitBranch { carrier: None, .. } | ExportV1::Unavailable { .. } => {
                unreachable!("a Cloud carrier body always has carrier metadata")
            }
        };
        CloudResultCarrier {
            portable_owner_path: path,
            idempotency_key,
            media_type,
            size_bytes,
            sha256,
            body,
        }
    });
    Ok((metadata, carrier))
}

fn byte_backed_cloud_export(
    name: &str,
    kind: &str,
    media_type: &str,
    path: &str,
    bytes: Arc<[u8]>,
) -> Result<(ExportV1, Option<CloudCarrierBody>), LocalPublicationError> {
    let size_bytes = u64::try_from(bytes.len()).map_err(|_| export_write_error(name))?;
    let sha256 = lowercase_hex(ring::digest::digest(&SHA256, &bytes).as_ref());
    Ok((
        ExportV1::Available {
            kind: kind.to_owned(),
            media_type: media_type.to_owned(),
            path: path.to_owned(),
            size_bytes,
            digest: DigestV1 {
                algorithm: "sha256".to_owned(),
                value: sha256,
            },
        },
        Some(CloudCarrierBody::Bytes(bytes)),
    ))
}

#[cfg(test)]
fn publish_with_observer(
    destination: &Path,
    artifacts: &ArtifactStaging,
    run: &WorkflowRunResult,
    observer: &mut impl PublicationObserver,
) -> Result<WorkflowRunTerminalResultV1, LocalPublicationError> {
    let destination = prepare_result_destination(destination)?;
    publish_prepared_with_observer(&destination, artifacts, run, observer)
}

fn publish_prepared_with_observer(
    destination: &PreparedResultDestination,
    artifacts: &ArtifactStaging,
    run: &WorkflowRunResult,
    observer: &mut impl PublicationObserver,
) -> Result<WorkflowRunTerminalResultV1, LocalPublicationError> {
    let target = &destination.target;
    let mut staging = StagingDirectory::create(target)?;
    observe(
        observer,
        &PublicationBoundary::StagingCreated,
        LocalPublicationPhase::Staging,
        LocalPublicationFailureKind::StagingUnavailable,
        None,
    )?;

    if !run.exports.keys().eq(run.export_sources.keys()) {
        return Err(invalid_run_result());
    }
    let mut exports = BTreeMap::new();
    let mut sources = BTreeMap::<(String, String), SourcePublication>::new();
    for (index, (name, export)) in run.exports.iter().enumerate() {
        let ordinal = index.checked_add(1).ok_or_else(|| {
            LocalPublicationError::for_export(
                LocalPublicationPhase::Serialization,
                LocalPublicationFailureKind::InvalidRunResult,
                name,
            )
        })?;
        let source = run
            .export_sources
            .get(name)
            .ok_or_else(invalid_run_result)?;
        let identity = (source.node.id.clone(), source.output.clone());
        let metadata = match export {
            ExportValue::Available { output } => {
                if !captured_type_matches(source.value_type, output) {
                    return Err(invalid_run_result());
                }
                match existing_available_export(&sources, &identity, source, output)? {
                    Some(metadata) => metadata,
                    None => {
                        let metadata = write_available_export(
                            &mut staging,
                            observer,
                            artifacts,
                            name,
                            ordinal,
                            output,
                        )?;
                        insert_available_source(&mut sources, identity, source, output, &metadata);
                        metadata
                    }
                }
            }
            ExportValue::Unavailable { reason } => {
                unavailable_export(&mut sources, identity, source, *reason)?
            }
        };
        exports.insert(name.clone(), metadata);
    }

    observe(
        observer,
        &PublicationBoundary::BeforeSerialization,
        LocalPublicationPhase::Serialization,
        LocalPublicationFailureKind::SerializationUnavailable,
        None,
    )?;
    let result = build_result(run, exports)?;
    let result_bytes = encode_result_json(&result)?;
    let result_file = staging.write_result(&result_bytes)?;
    observer
        .close_staged_file(result_file, &StagedFile::Result)
        .map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Close,
                LocalPublicationFailureKind::SerializationUnavailable,
            )
        })?;
    staging.verify(&result).map_err(|_| {
        LocalPublicationError::new(
            LocalPublicationPhase::Verification,
            LocalPublicationFailureKind::VerificationUnavailable,
        )
    })?;

    observe(
        observer,
        &PublicationBoundary::StagingComplete,
        LocalPublicationPhase::Commit,
        LocalPublicationFailureKind::AtomicPublicationUnavailable,
        None,
    )?;
    target.verify_parent()?;
    staging.verify(&result).map_err(|_| {
        LocalPublicationError::new(
            LocalPublicationPhase::Verification,
            LocalPublicationFailureKind::VerificationUnavailable,
        )
    })?;
    match target.existing_publication(&result, &result_bytes)? {
        ExistingPublication::Absent => {
            if let Err(error) = staging.commit(target)
                && (error.kind() != LocalPublicationFailureKind::DestinationExists
                    || target.existing_publication(&result, &result_bytes)?
                        != ExistingPublication::Identical)
            {
                return Err(
                    if error.kind() == LocalPublicationFailureKind::DestinationExists {
                        result_conflict()
                    } else {
                        error
                    },
                );
            }
        }
        ExistingPublication::Identical => {}
        ExistingPublication::Conflict => return Err(result_conflict()),
    }
    drop(staging);

    let outcome = result.outcome;
    let terminal = WorkflowRunTerminalResultV1 {
        schema_version: 1,
        command: COMMAND,
        outcome,
        exit_status: exit_status(run, outcome),
        run_directory: retained_path(&run.run_directory)?,
        attempt_number: run.attempt_number,
        result_directory: target.normalized.clone(),
        result,
    };
    Ok(terminal)
}

enum SourcePublication {
    Available {
        source: ResolvedOutputSource,
        output: CapturedValue,
        metadata: Box<ExportV1>,
    },
    Unavailable {
        source: ResolvedOutputSource,
        reason: ExportUnavailableReason,
    },
}

fn insert_available_source(
    sources: &mut BTreeMap<(String, String), SourcePublication>,
    identity: (String, String),
    source: &ResolvedOutputSource,
    output: &CapturedValue,
    metadata: &ExportV1,
) {
    sources.insert(
        identity,
        SourcePublication::Available {
            source: source.clone(),
            output: output.clone(),
            metadata: Box::new(metadata.clone()),
        },
    );
}

fn existing_available_export(
    sources: &BTreeMap<(String, String), SourcePublication>,
    identity: &(String, String),
    source: &ResolvedOutputSource,
    output: &CapturedValue,
) -> Result<Option<ExportV1>, LocalPublicationError> {
    let Some(publication) = sources.get(identity) else {
        return Ok(None);
    };
    let SourcePublication::Available {
        source: owner_source,
        output: owner_output,
        metadata,
    } = publication
    else {
        return Err(invalid_run_result());
    };
    if owner_source != source || owner_output != output {
        return Err(invalid_run_result());
    }
    Ok(Some(metadata.as_ref().clone()))
}

fn unavailable_export(
    sources: &mut BTreeMap<(String, String), SourcePublication>,
    identity: (String, String),
    source: &ResolvedOutputSource,
    reason: ExportUnavailableReason,
) -> Result<ExportV1, LocalPublicationError> {
    match sources.get(&identity) {
        None => {
            sources.insert(
                identity,
                SourcePublication::Unavailable {
                    source: source.clone(),
                    reason,
                },
            );
        }
        Some(SourcePublication::Unavailable {
            source: owner_source,
            reason: owner_reason,
        }) if owner_source == source && *owner_reason == reason => {}
        Some(_) => return Err(invalid_run_result()),
    }
    Ok(ExportV1::Unavailable {
        reason: export_unavailable_reason(reason),
    })
}

fn captured_type_matches(value_type: WorkflowValueType, output: &CapturedValue) -> bool {
    matches!(
        (value_type, output),
        (WorkflowValueType::File, CapturedValue::File(_))
            | (WorkflowValueType::Text, CapturedValue::Text(_))
            | (WorkflowValueType::Json, CapturedValue::Json(_))
            | (WorkflowValueType::GitBranch, CapturedValue::GitBranch(_))
    )
}

fn write_available_export(
    staging: &mut StagingDirectory<'_>,
    observer: &mut impl PublicationObserver,
    artifacts: &ArtifactStaging,
    name: &str,
    ordinal: usize,
    output: &CapturedValue,
) -> Result<ExportV1, LocalPublicationError> {
    if let CapturedValue::GitBranch(branch) = output {
        return write_git_branch_export(staging, observer, artifacts, name, ordinal, branch);
    }
    let file_name = format!("{ordinal:04}");
    if let CapturedValue::File(file) = output {
        expose_staged_carrier(
            staging,
            observer,
            artifacts,
            name,
            &file_name,
            file.carrier(),
            file.output_identity(),
        )?;
        return Ok(ExportV1::Available {
            kind: "file".to_owned(),
            media_type: file.media_type().to_owned(),
            path: format!("{EXPORT_DIRECTORY}/{file_name}"),
            size_bytes: file.size(),
            digest: DigestV1 {
                algorithm: "sha256".to_owned(),
                value: file.sha256().to_owned(),
            },
        });
    }

    observe(
        observer,
        &PublicationBoundary::BeforeExportMaterialization {
            export: name.to_owned(),
        },
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::ExportWriteUnavailable,
        Some(name),
    )?;
    let (kind, media_type, expected_size) = match output {
        CapturedValue::Text(text) => (
            "text",
            "text/plain; charset=utf-8".to_owned(),
            u64::try_from(text.len()).map_err(|_| {
                LocalPublicationError::for_export(
                    LocalPublicationPhase::ExportCopy,
                    LocalPublicationFailureKind::UnsupportedExport,
                    name,
                )
            })?,
        ),
        CapturedValue::Json(value) => (
            "json",
            "application/json".to_owned(),
            canonical_json::encoded_size(value, u64::MAX).map_err(|_| {
                LocalPublicationError::for_export(
                    LocalPublicationPhase::ExportCopy,
                    LocalPublicationFailureKind::UnsupportedExport,
                    name,
                )
            })?,
        ),
        CapturedValue::File(_) | CapturedValue::GitBranch(_) => {
            return Err(LocalPublicationError::for_export(
                LocalPublicationPhase::ExportCopy,
                LocalPublicationFailureKind::UnsupportedExport,
                name,
            ));
        }
    };
    let mut destination = staging.create_export(&file_name).map_err(|kind| {
        LocalPublicationError::for_export(LocalPublicationPhase::ExportCopy, kind, name)
    })?;
    let mut digest = DigestContext::new(&SHA256);
    let written = {
        let mut hashing = HashingWriter {
            destination: &mut destination,
            digest: &mut digest,
            bytes: 0,
        };
        match output {
            CapturedValue::Text(text) => hashing
                .write_all(text.as_bytes())
                .map_err(|_| export_write_error(name))?,
            CapturedValue::Json(value) => canonical_json::to_writer(&mut hashing, value)
                .map_err(|_| export_write_error(name))?,
            CapturedValue::File(_) | CapturedValue::GitBranch(_) => {
                return Err(LocalPublicationError::for_export(
                    LocalPublicationPhase::ExportCopy,
                    LocalPublicationFailureKind::UnsupportedExport,
                    name,
                ));
            }
        }
        hashing.flush().map_err(|_| export_write_error(name))?;
        hashing.bytes
    };
    destination.flush().map_err(|_| export_write_error(name))?;
    if written != expected_size {
        return Err(export_write_error(name));
    }
    observer
        .close_staged_file(
            destination,
            &StagedFile::Export {
                export: name.to_owned(),
            },
        )
        .map_err(|_| {
            LocalPublicationError::for_export(
                LocalPublicationPhase::Close,
                LocalPublicationFailureKind::ExportWriteUnavailable,
                name,
            )
        })?;
    let metadata = ExportV1::Available {
        kind: kind.to_owned(),
        media_type,
        path: format!("{EXPORT_DIRECTORY}/{file_name}"),
        size_bytes: written,
        digest: DigestV1 {
            algorithm: "sha256".to_owned(),
            value: lowercase_hex(digest.finish().as_ref()),
        },
    };
    observe(
        observer,
        &PublicationBoundary::AfterExportMaterialization {
            export: name.to_owned(),
        },
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::ExportWriteUnavailable,
        Some(name),
    )?;
    Ok(metadata)
}

fn write_git_branch_export(
    staging: &mut StagingDirectory<'_>,
    observer: &mut impl PublicationObserver,
    artifacts: &ArtifactStaging,
    name: &str,
    ordinal: usize,
    branch: &super::artifact::CapturedGitBranch,
) -> Result<ExportV1, LocalPublicationError> {
    let metadata = branch.metadata();
    let has_delta = metadata.base_oid() != metadata.head_oid();
    if has_delta != branch.carrier().is_some() {
        return Err(invalid_run_result());
    }
    let carrier = match branch.carrier() {
        None => None,
        Some(carrier) => {
            let file_name = format!("{ordinal:04}");
            expose_staged_carrier(
                staging,
                observer,
                artifacts,
                name,
                &file_name,
                carrier.staged(),
                branch.output_identity(),
            )?;
            Some(GitBranchCarrierV1 {
                path: format!("{EXPORT_DIRECTORY}/{file_name}"),
                media_type: carrier.media_type().to_owned(),
                size_bytes: carrier.size(),
                digest: DigestV1 {
                    algorithm: "sha256".to_owned(),
                    value: carrier.sha256().to_owned(),
                },
            })
        }
    };
    Ok(ExportV1::GitBranch {
        artifact_version: metadata.artifact_version(),
        object_format: metadata.object_format().as_str().to_owned(),
        base_oid: metadata.base_oid().to_owned(),
        head_oid: metadata.head_oid().to_owned(),
        tree_oid: metadata.tree_oid().to_owned(),
        carrier,
    })
}

fn observe(
    observer: &mut impl PublicationObserver,
    boundary: &PublicationBoundary,
    phase: LocalPublicationPhase,
    kind: LocalPublicationFailureKind,
    export: Option<&str>,
) -> Result<(), LocalPublicationError> {
    observer.observe(boundary).map_err(|()| {
        export.map_or_else(
            || LocalPublicationError::new(phase, kind),
            |export| LocalPublicationError::for_export(phase, kind, export),
        )
    })
}

fn expose_staged_carrier(
    staging: &mut StagingDirectory<'_>,
    observer: &mut impl PublicationObserver,
    artifacts: &ArtifactStaging,
    export: &str,
    file_name: &str,
    carrier: &StagedCarrier,
    expected_output_identity: &str,
) -> Result<(), LocalPublicationError> {
    observe(
        observer,
        &PublicationBoundary::BeforeExportMaterialization {
            export: export.to_owned(),
        },
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::CarrierHandoffUnavailable,
        Some(export),
    )?;
    staging
        .expose_export(artifacts, carrier, expected_output_identity, file_name)
        .map_err(|failure| carrier_handoff_error(export, failure))?;
    observe(
        observer,
        &PublicationBoundary::AfterExportMaterialization {
            export: export.to_owned(),
        },
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::CarrierHandoffUnavailable,
        Some(export),
    )
}

fn export_write_error(export: &str) -> LocalPublicationError {
    LocalPublicationError::for_export(
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::ExportWriteUnavailable,
        export,
    )
}

fn carrier_handoff_error(export: &str, _failure: ArtifactExposeFailure) -> LocalPublicationError {
    LocalPublicationError::for_export(
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::CarrierHandoffUnavailable,
        export,
    )
}

fn build_result(
    run: &WorkflowRunResult,
    exports: BTreeMap<String, ExportV1>,
) -> Result<WorkflowResultV1, LocalPublicationError> {
    let source_root = retained_path(&run.source_root)?;
    let execution_root = retained_path(&run.execution_root)?;
    build_result_with_provenance(run, WorkflowProvenanceV1::Local { source_root }, exports).map(
        |mut result| {
            result.execution.execution_root = Some(execution_root);
            result
        },
    )
}

fn build_result_with_provenance(
    run: &WorkflowRunResult,
    provenance: WorkflowProvenanceV1,
    exports: BTreeMap<String, ExportV1>,
) -> Result<WorkflowResultV1, LocalPublicationError> {
    let (outcome, primary_failure) = match &run.outcome {
        RunOutcome::Succeeded => (WorkflowOutcomeV1::Succeeded, None),
        RunOutcome::Failed {
            primary_failure, ..
        } => (
            WorkflowOutcomeV1::Failed,
            Some(primary_failure_v1(primary_failure)?),
        ),
        RunOutcome::Cancelled { .. } => (WorkflowOutcomeV1::Cancelled, None),
    };
    let cancellation = run
        .cancellation
        .as_ref()
        .map(|cancellation| {
            Ok(CancellationV1 {
                reason: cancellation_reason(cancellation.reason),
                force_stop_deadline: timestamp(cancellation.force_stop_deadline)?,
            })
        })
        .transpose()?;
    let steps = run
        .steps
        .iter()
        .map(step_v1)
        .collect::<Result<Vec<_>, _>>()?;
    let finalization = run.finalization.as_ref().map(finalization_v1).transpose()?;

    if run.attempt_number == 0 {
        return Err(invalid_run_result());
    }
    Ok(WorkflowResultV1 {
        schema_version: 1,
        attempt_number: run.attempt_number,
        workflow: WorkflowIdentityV1 {
            path: run.workflow_path.clone(),
            provenance,
            digest: DigestV1 {
                algorithm: run.content_digest.algorithm.as_str().to_owned(),
                value: run.content_digest.value.clone(),
            },
        },
        execution: WorkflowExecutionV1 {
            execution_root: None,
            maximum_parallel_steps: run.maximum_parallel_steps.get(),
            started_at: timestamp(run.timing.started_at)?,
            finished_at: timestamp(run.timing.finished_at)?,
            duration_milliseconds: duration_milliseconds(run.timing.duration)?,
        },
        command_output_policy: CommandOutputPolicyV1 {
            encoding: "base64".to_owned(),
            maximum_retained_bytes_per_stream: super::MAXIMUM_RETAINED_BYTES_PER_STREAM,
        },
        outcome,
        primary_failure,
        cancellation,
        steps,
        finalization,
        exports,
    })
}

fn finalization_v1(
    finalization: &WorkflowRunFinalization,
) -> Result<FinalizationV1, LocalPublicationError> {
    let finalizers = finalization
        .finalizers
        .iter()
        .map(step_v1)
        .collect::<Result<Vec<_>, _>>()?;
    if finalizers
        .iter()
        .any(|finalizer| finalizer.role != WorkflowNodeRoleV1::Finalizer)
    {
        return Err(invalid_run_result());
    }
    let issues = finalizers
        .iter()
        .filter(|finalizer| {
            finalizer.state == WorkflowStepStateV1::Failed
                || (finalizer.state == WorkflowStepStateV1::Blocked
                    && finalizer.reason == Some(StepReasonV1::InputUnavailable))
        })
        .map(|finalizer| FinalizationIssueV1 {
            node: WorkflowNodeV1 {
                id: finalizer.id.clone(),
                role: WorkflowNodeRoleV1::Finalizer,
            },
            impact: finalizer.failure_policy,
        })
        .collect();
    let cancellation = finalization
        .cancellation
        .as_ref()
        .map(|cancellation| {
            Ok(FinalizationCancellationV1 {
                reason: cancellation_reason(cancellation.reason),
                force_stop_deadline: cancellation
                    .force_stop_deadline
                    .map(timestamp)
                    .transpose()?,
            })
        })
        .transpose()?;
    Ok(FinalizationV1 {
        trigger: finalization_trigger(finalization.trigger),
        finalizers,
        issues,
        cancellation,
        force_abort: finalization.force_abort,
    })
}

fn step_v1(step: &WorkflowRunStep) -> Result<WorkflowStepV1, LocalPublicationError> {
    let (started_at, duration_milliseconds) = match &step.timing {
        Some(timing) => (
            Some(timestamp(timing.started_at)?),
            Some(duration_milliseconds(timing.duration)?),
        ),
        None => (None, None),
    };
    let (state, failure, dependency, reason, unavailable_references) = match &step.state {
        StepState::Succeeded { .. } => (WorkflowStepStateV1::Succeeded, None, None, None, None),
        StepState::Failed { phase, cause } => (
            WorkflowStepStateV1::Failed,
            Some(failure_v1(*phase, cause)?),
            None,
            None,
            None,
        ),
        StepState::Blocked { dependency } => (
            WorkflowStepStateV1::Blocked,
            None,
            Some(dependency.clone()),
            None,
            None,
        ),
        StepState::InputUnavailable { references } => (
            WorkflowStepStateV1::Blocked,
            None,
            None,
            Some(StepReasonV1::InputUnavailable),
            Some(references.clone()),
        ),
        StepState::NotRun {
            reason: NotRunReason::FailureStop,
        } => (
            WorkflowStepStateV1::NotRun,
            None,
            None,
            Some(StepReasonV1::FailureStop),
            None,
        ),
        StepState::NotRun {
            reason: NotRunReason::FinalizerTriggerNotSelected,
        } => (
            WorkflowStepStateV1::NotRun,
            None,
            None,
            Some(StepReasonV1::FinalizerTriggerNotSelected),
            None,
        ),
        StepState::Cancelled { reason } => (
            WorkflowStepStateV1::Cancelled,
            None,
            None,
            Some(cancellation_step_reason(*reason)),
            None,
        ),
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Cancelling { .. } => return Err(invalid_run_result()),
    };
    let command_output = step
        .command_output
        .as_ref()
        .map(command_output_v1)
        .transpose()?;

    if step.kind == WorkflowRunStepKind::Agent && command_output.is_some() {
        return Err(invalid_run_result());
    }

    Ok(WorkflowStepV1 {
        id: step.id.clone(),
        role: workflow_node_role(step.role),
        kind: match step.kind {
            WorkflowRunStepKind::Command => "cmd",
            WorkflowRunStepKind::Agent => "agent",
        }
        .to_owned(),
        failure_policy: step.failure_policy,
        state,
        recovery: step.recovery.clone(),
        invocations: step.invocations.clone(),
        started_at,
        duration_milliseconds,
        failure,
        dependency,
        reason,
        unavailable_references,
        command_output,
    })
}

pub(crate) fn command_output_v1(
    diagnostic: &StepDiagnostic,
) -> Result<CommandOutputV1, LocalPublicationError> {
    Ok(CommandOutputV1 {
        stdout: diagnostic_stream_v1(diagnostic.standard_output())?,
        stderr: diagnostic_stream_v1(diagnostic.standard_error())?,
    })
}

fn diagnostic_stream_v1(
    stream: &CapturedDiagnosticStream,
) -> Result<DiagnosticStreamV1, LocalPublicationError> {
    let retained_bytes = u64::try_from(stream.bytes().len()).map_err(|_| invalid_run_result())?;
    if retained_bytes > super::MAXIMUM_RETAINED_BYTES_PER_STREAM {
        return Err(invalid_run_result());
    }
    let discarded_bytes = stream
        .truncation()
        .map_or(0, |truncation| truncation.discarded_bytes());
    Ok(DiagnosticStreamV1 {
        encoding: "base64".to_owned(),
        data: BASE64_STANDARD.encode(stream.bytes()),
        retained_bytes,
        discarded_bytes,
        truncated: discarded_bytes != 0,
        fully_drained: stream.fully_drained(),
    })
}

pub(crate) fn step_recovery_summary_v1(
    recovery: Option<&StepRecoveryState<StepFailureCause>>,
) -> Result<Option<StepRecoverySummaryV1>, LocalPublicationError> {
    let Some(recovery) = recovery else {
        return Ok(None);
    };
    if recovery.rounds.is_empty() {
        return if recovery.terminal_disposition.is_none() {
            Ok(None)
        } else {
            Err(invalid_run_result())
        };
    }
    let rounds = recovery_round_summaries_v1(recovery, true)?;
    let termination = match recovery
        .terminal_disposition
        .ok_or_else(invalid_run_result)?
    {
        RecoveryTerminalDisposition::Recovered { execution_number } => {
            RecoveryTerminationV1::Recovered {
                execution_number: execution_number.get(),
            }
        }
        RecoveryTerminalDisposition::Exhausted { execution_number } => {
            RecoveryTerminationV1::Exhausted {
                execution_number: execution_number.get(),
            }
        }
        RecoveryTerminalDisposition::GaveUp { round } => {
            RecoveryTerminationV1::GaveUp { round: round.get() }
        }
        RecoveryTerminalDisposition::HandlerFailed { round, phase } => {
            let failure = recovery
                .rounds
                .iter()
                .find(|candidate| candidate.number == round)
                .and_then(|round| round.handler.as_ref())
                .and_then(|handler| match &handler.outcome {
                    RecoveryHandlerOutcome::Failed {
                        phase: retained_phase,
                        cause,
                    } if *retained_phase == phase => Some(cause),
                    _ => None,
                })
                .ok_or_else(invalid_run_result)?;
            RecoveryTerminationV1::HandlerFailed {
                round: round.get(),
                handler_failure: recovery_handler_failure_v1(phase, failure)?,
            }
        }
        RecoveryTerminalDisposition::Cancelled { round, active } => {
            let (active_role, execution_number) = match active {
                ActiveStepInvocation::Target { execution_number } => (
                    RecoveryInvocationRoleV1::Target,
                    Some(execution_number.get()),
                ),
                ActiveStepInvocation::RecoveryHandler { .. } => {
                    (RecoveryInvocationRoleV1::RecoveryHandler, None)
                }
            };
            RecoveryTerminationV1::Cancelled {
                round: round.get(),
                active_role,
                execution_number,
            }
        }
    };
    Ok(Some(StepRecoverySummaryV1 {
        schema_version: 1,
        configured_retries: recovery.configured_rounds,
        handler_kind: recovery.handler_kind.map(recovery_handler_kind_v1),
        rounds,
        termination,
    }))
}

pub(crate) fn recovery_round_summaries_v1(
    recovery: &StepRecoveryState<StepFailureCause>,
    require_settled_handlers: bool,
) -> Result<Vec<RecoveryRoundSummaryV1>, LocalPublicationError> {
    recovery
        .rounds
        .iter()
        .map(|round| {
            let handler = round
                .handler
                .as_ref()
                .map(|handler| {
                    let (outcome, summary, reason, failure) = match &handler.outcome {
                        RecoveryHandlerOutcome::Recheck { summary, reason } => (
                            RecoveryHandlerOutcomeV1::Recheck,
                            Some(summary.clone()),
                            Some(reason.clone()),
                            None,
                        ),
                        RecoveryHandlerOutcome::GaveUp { summary, reason } => (
                            RecoveryHandlerOutcomeV1::GaveUp,
                            Some(summary.clone()),
                            Some(reason.clone()),
                            None,
                        ),
                        RecoveryHandlerOutcome::Failed { phase, cause } => (
                            RecoveryHandlerOutcomeV1::Failed,
                            None,
                            None,
                            Some(recovery_handler_failure_v1(*phase, cause)?),
                        ),
                        RecoveryHandlerOutcome::Cancelled => {
                            (RecoveryHandlerOutcomeV1::Cancelled, None, None, None)
                        }
                        RecoveryHandlerOutcome::Starting | RecoveryHandlerOutcome::Running
                            if !require_settled_handlers =>
                        {
                            return Ok(None);
                        }
                        RecoveryHandlerOutcome::Starting | RecoveryHandlerOutcome::Running => {
                            return Err(invalid_run_result());
                        }
                    };
                    Ok(Some(RecoveryHandlerSummaryV1 {
                        kind: recovery_handler_kind_v1(handler.kind),
                        invocation_id: handler.invocation.transition_sequence.get(),
                        outcome,
                        summary,
                        reason,
                        failure,
                    }))
                })
                .transpose()?
                .flatten();
            Ok(RecoveryRoundSummaryV1 {
                number: round.number.get(),
                failed_execution: RecoveryFailedExecutionV1 {
                    execution_number: round.failed_execution.execution_number.get(),
                    invocation_id: round.failed_execution.invocation.transition_sequence.get(),
                    failure: failure_v1(
                        round.failed_execution.phase,
                        &round.failed_execution.cause,
                    )?,
                },
                handler,
            })
        })
        .collect()
}

pub(crate) fn recovery_handler_kind_v1(kind: RecoveryHandlerKind) -> RecoveryHandlerKindV1 {
    match kind {
        RecoveryHandlerKind::Command => RecoveryHandlerKindV1::Cmd,
        RecoveryHandlerKind::Agent => RecoveryHandlerKindV1::Agent,
    }
}

fn recovery_handler_failure_v1(
    phase: RecoveryHandlerFailurePhase,
    cause: &StepFailureCause,
) -> Result<RecoveryHandlerFailureV1, LocalPublicationError> {
    let StepFailureCause::RecoveryHandler(cause) = cause else {
        return Err(invalid_run_result());
    };
    let (code, exit_code, decision_rejection) = match cause {
        super::recovery::RecoveryHandlerFailure::ContextUnavailable => {
            (RecoveryHandlerFailureCodeV1::ContextUnavailable, None, None)
        }
        super::recovery::RecoveryHandlerFailure::HandlerUnavailable => {
            (RecoveryHandlerFailureCodeV1::HandlerUnavailable, None, None)
        }
        super::recovery::RecoveryHandlerFailure::WorkingDirectoryUnavailable => (
            RecoveryHandlerFailureCodeV1::WorkingDirectoryUnavailable,
            None,
            None,
        ),
        super::recovery::RecoveryHandlerFailure::CommandPreparationFailed => (
            RecoveryHandlerFailureCodeV1::CommandPreparationFailed,
            None,
            None,
        ),
        super::recovery::RecoveryHandlerFailure::CommandLaunchFailed => (
            RecoveryHandlerFailureCodeV1::CommandLaunchFailed,
            None,
            None,
        ),
        super::recovery::RecoveryHandlerFailure::CommandWaitFailed => {
            (RecoveryHandlerFailureCodeV1::CommandWaitFailed, None, None)
        }
        super::recovery::RecoveryHandlerFailure::CommandExitFailed { code } => {
            (RecoveryHandlerFailureCodeV1::CommandExitFailed, *code, None)
        }
        super::recovery::RecoveryHandlerFailure::ProcessQuiescenceFailed => (
            RecoveryHandlerFailureCodeV1::ProcessQuiescenceFailed,
            None,
            None,
        ),
        super::recovery::RecoveryHandlerFailure::ResultMissing => {
            (RecoveryHandlerFailureCodeV1::ResultMissing, None, None)
        }
        super::recovery::RecoveryHandlerFailure::ResultSymbolicLink => {
            (RecoveryHandlerFailureCodeV1::ResultSymbolicLink, None, None)
        }
        super::recovery::RecoveryHandlerFailure::ResultNotRegular => {
            (RecoveryHandlerFailureCodeV1::ResultNotRegular, None, None)
        }
        super::recovery::RecoveryHandlerFailure::ResultUnavailable => {
            (RecoveryHandlerFailureCodeV1::ResultUnavailable, None, None)
        }
        super::recovery::RecoveryHandlerFailure::ResultTooLarge => {
            (RecoveryHandlerFailureCodeV1::ResultTooLarge, None, None)
        }
        super::recovery::RecoveryHandlerFailure::DecisionInvalid(rejection) => (
            RecoveryHandlerFailureCodeV1::DecisionInvalid,
            None,
            Some(recovery_decision_rejection_v1(*rejection)),
        ),
        super::recovery::RecoveryHandlerFailure::AgentInputFailed => {
            (RecoveryHandlerFailureCodeV1::AgentInputFailed, None, None)
        }
        super::recovery::RecoveryHandlerFailure::AgentFailed => {
            (RecoveryHandlerFailureCodeV1::AgentFailed, None, None)
        }
        super::recovery::RecoveryHandlerFailure::AgentResultMissing => {
            (RecoveryHandlerFailureCodeV1::AgentResultMissing, None, None)
        }
        super::recovery::RecoveryHandlerFailure::AgentResultInvalid(rejection) => (
            RecoveryHandlerFailureCodeV1::AgentResultInvalid,
            None,
            Some(recovery_decision_rejection_v1(*rejection)),
        ),
        super::recovery::RecoveryHandlerFailure::SettlementFailed => {
            (RecoveryHandlerFailureCodeV1::SettlementFailed, None, None)
        }
    };
    Ok(RecoveryHandlerFailureV1 {
        phase: match phase {
            RecoveryHandlerFailurePhase::Start => RecoveryHandlerFailurePhaseV1::Start,
            RecoveryHandlerFailurePhase::Execution => RecoveryHandlerFailurePhaseV1::Execution,
        },
        cause: RecoveryHandlerFailureCauseV1 {
            code,
            exit_code,
            decision_rejection,
        },
    })
}

fn recovery_decision_rejection_v1(
    rejection: super::recovery::RecoveryDecisionFailureKind,
) -> RecoveryDecisionRejectionV1 {
    use super::recovery::RecoveryDecisionFailureKind as Source;
    match rejection {
        Source::InputTooLarge => RecoveryDecisionRejectionV1::InputTooLarge,
        Source::InvalidUtf8 => RecoveryDecisionRejectionV1::InvalidUtf8,
        Source::InvalidJson => RecoveryDecisionRejectionV1::InvalidJson,
        Source::DuplicateKey => RecoveryDecisionRejectionV1::DuplicateKey,
        Source::UnknownField => RecoveryDecisionRejectionV1::UnknownField,
        Source::UnsupportedSchemaVersion => RecoveryDecisionRejectionV1::UnsupportedSchemaVersion,
        Source::UnknownDecision => RecoveryDecisionRejectionV1::UnknownDecision,
        Source::EmptySummary => RecoveryDecisionRejectionV1::EmptySummary,
        Source::SummaryTooLong => RecoveryDecisionRejectionV1::SummaryTooLong,
        Source::EmptyReason => RecoveryDecisionRejectionV1::EmptyReason,
        Source::ReasonTooLong => RecoveryDecisionRejectionV1::ReasonTooLong,
    }
}

fn primary_failure_v1(
    failure: &StepFailure<StepFailureCause>,
) -> Result<PrimaryFailureV1, LocalPublicationError> {
    let failure_v1 = failure_v1(failure.phase, &failure.cause)?;
    Ok(PrimaryFailureV1 {
        node: WorkflowNodeV1 {
            id: failure.step.clone(),
            role: workflow_node_role(failure.role),
        },
        phase: failure_v1.phase,
        cause: failure_v1.cause,
    })
}

pub(super) fn failure_v1(
    phase: FailurePhase,
    cause: &StepFailureCause,
) -> Result<FailureV1, LocalPublicationError> {
    let (phase, cause) = match (phase, cause) {
        (FailurePhase::Start, StepFailureCause::Start(cause)) => {
            (FailurePhaseV1::Start, start_failure_cause(cause)?)
        }
        (FailurePhase::Execution, StepFailureCause::Execution(cause)) => {
            (FailurePhaseV1::Execution, execution_failure_cause(cause))
        }
        (FailurePhase::OutputCapture, StepFailureCause::OutputCapture(cause)) => (
            FailurePhaseV1::OutputCapture,
            output_capture_failure_cause(cause),
        ),
        _ => return Err(invalid_run_result()),
    };
    Ok(FailureV1 { phase, cause })
}

fn start_failure_cause(
    failure: &StepStartFailure,
) -> Result<FailureCauseV1, LocalPublicationError> {
    let cause = match failure {
        StepStartFailure::StepUnavailable => FailureCauseV1::code(FailureCodeV1::StepUnavailable),
        StepStartFailure::PreparationTaskUnavailable => {
            FailureCauseV1::code(FailureCodeV1::PreparationTaskUnavailable)
        }
        StepStartFailure::InputsUnavailable => {
            FailureCauseV1::code(FailureCodeV1::InputsUnavailable)
        }
        StepStartFailure::InputPreparation(failure) => {
            let code = match failure.kind() {
                InputPreparationFailureKind::InvalidInputName => FailureCodeV1::InputInvalidName,
                InputPreparationFailureKind::ValueCountLimitExceeded => {
                    FailureCodeV1::InputValueCountLimit
                }
                InputPreparationFailureKind::ValueSizeLimitExceeded => {
                    FailureCodeV1::InputValueSizeLimit
                }
                InputPreparationFailureKind::TotalSizeLimitExceeded => {
                    FailureCodeV1::InputTotalSizeLimit
                }
                InputPreparationFailureKind::CollectionOrdinalLimitExceeded => {
                    FailureCodeV1::InputCollectionOrdinalLimit
                }
                InputPreparationFailureKind::ValueTypeMismatch => FailureCodeV1::InputTypeMismatch,
                InputPreparationFailureKind::SourceUnavailable => {
                    FailureCodeV1::InputSourceUnavailable
                }
                InputPreparationFailureKind::StagingUnavailable => {
                    FailureCodeV1::InputStagingUnavailable
                }
                InputPreparationFailureKind::LiveLimitExceeded => FailureCodeV1::InputLiveLimit,
            };
            let mut cause = FailureCauseV1::code(code);
            cause.input = failure.input_identity().map(str::to_owned);
            cause.collection_index = failure.collection_index();
            cause
        }
        StepStartFailure::AgentInput(failure) => agent_input_failure_cause(failure),
        StepStartFailure::AgentRuntimeUnavailable => {
            FailureCauseV1::code(FailureCodeV1::AgentRuntimeUnavailable)
        }
        StepStartFailure::Agent(failure) => agent_failure_cause(failure),
        StepStartFailure::OutputsUnsupported => {
            FailureCauseV1::code(FailureCodeV1::OutputsUnsupported)
        }
        StepStartFailure::WorkingDirectory(failure) => FailureCauseV1::code(match failure {
            WorkingDirectoryFailure::ExecutionRootRebound => FailureCodeV1::ExecutionRootRebound,
            WorkingDirectoryFailure::Unavailable => FailureCodeV1::WorkingDirectoryUnavailable,
            WorkingDirectoryFailure::EscapesExecutionRoot => FailureCodeV1::WorkingDirectoryEscape,
            WorkingDirectoryFailure::NotDirectory => FailureCodeV1::WorkingDirectoryNotDirectory,
        }),
        StepStartFailure::CommandPreparation(failure) => FailureCauseV1::code(match failure {
            CommandPreparationFailure::InvalidArgv => FailureCodeV1::CommandArgvInvalid,
            CommandPreparationFailure::PathNotConfigured => FailureCodeV1::CommandPathUnconfigured,
            CommandPreparationFailure::ExecutableNotFound => FailureCodeV1::ExecutableNotFound,
            CommandPreparationFailure::ExecutableUnavailable => {
                FailureCodeV1::ExecutableUnavailable
            }
        }),
        StepStartFailure::CommandLaunch(failure) => FailureCauseV1::code(match failure {
            CommandLaunchFailure::NotFound => FailureCodeV1::CommandLaunchNotFound,
            CommandLaunchFailure::PermissionDenied => FailureCodeV1::CommandLaunchPermissionDenied,
            CommandLaunchFailure::InvalidInput => FailureCodeV1::CommandLaunchInvalidInput,
            CommandLaunchFailure::Other => FailureCodeV1::CommandLaunchFailed,
        }),
    };
    Ok(cause)
}

fn agent_input_failure_cause(failure: &AgentInputStartFailure) -> FailureCauseV1 {
    FailureCauseV1::code(match failure {
        AgentInputStartFailure::StepUnavailable => FailureCodeV1::AgentStepUnavailable,
        AgentInputStartFailure::AgentAdmissionUnavailable => {
            FailureCodeV1::AgentAdmissionUnavailable
        }
        AgentInputStartFailure::InputsUnavailable => FailureCodeV1::AgentInputsUnavailable,
        AgentInputStartFailure::MissingUpstreamValue { .. } => {
            FailureCodeV1::AgentInputMissingUpstream
        }
        AgentInputStartFailure::ValueTypeMismatch { .. } => FailureCodeV1::AgentInputTypeMismatch,
        AgentInputStartFailure::RetainedSourceUnavailable { .. } => {
            FailureCodeV1::AgentSourceUnavailable
        }
        AgentInputStartFailure::InvalidRetainedText { .. } => FailureCodeV1::AgentSourceTextInvalid,
        AgentInputStartFailure::ResultSchemaUnavailable { .. } => {
            FailureCodeV1::AgentResultSchemaUnavailable
        }
        AgentInputStartFailure::InvalidValueMode => FailureCodeV1::AgentValueModeInvalid,
        AgentInputStartFailure::AttachmentCountLimitExceeded { .. } => {
            FailureCodeV1::AgentAttachmentCountLimit
        }
        AgentInputStartFailure::AttachmentBytesLimitExceeded { .. } => {
            FailureCodeV1::AgentAttachmentBytesLimit
        }
        AgentInputStartFailure::WorkingDirectory(failure) => match failure {
            WorkingDirectoryFailure::ExecutionRootRebound => FailureCodeV1::ExecutionRootRebound,
            WorkingDirectoryFailure::Unavailable => FailureCodeV1::WorkingDirectoryUnavailable,
            WorkingDirectoryFailure::EscapesExecutionRoot => FailureCodeV1::WorkingDirectoryEscape,
            WorkingDirectoryFailure::NotDirectory => FailureCodeV1::WorkingDirectoryNotDirectory,
        },
        AgentInputStartFailure::ArtifactStagingMismatch => FailureCodeV1::ArtifactStagingMismatch,
        AgentInputStartFailure::AgentStagingMismatch => FailureCodeV1::AgentStagingMismatch,
        AgentInputStartFailure::StagingUnavailable => FailureCodeV1::AgentInputStagingUnavailable,
    })
}

fn agent_failure_cause(failure: &AgentFailure) -> FailureCauseV1 {
    let mut cause = FailureCauseV1::code(agent_failure_code(failure.cause()));
    cause.protocol_rejection = failure.protocol_rejection().cloned();
    cause
}

fn agent_failure_code(failure: &AgentFailureCause) -> FailureCodeV1 {
    match failure {
        AgentFailureCause::HarnessStartFailed | AgentFailureCause::HarnessSetupFailed { .. } => {
            FailureCodeV1::HarnessStartFailed
        }
        AgentFailureCause::HarnessInputTooLarge { .. } => FailureCodeV1::HarnessInputTooLarge,
        AgentFailureCause::HarnessFailed { .. } => FailureCodeV1::HarnessFailed,
        AgentFailureCause::HarnessProtocolFailed => FailureCodeV1::HarnessProtocolFailed,
        AgentFailureCause::MissingResponse => FailureCodeV1::MissingResponse,
        AgentFailureCause::MissingResult => FailureCodeV1::MissingResult,
        AgentFailureCause::ResultValidationLimitExceeded { .. } => {
            FailureCodeV1::ResultValidationLimitExceeded
        }
        AgentFailureCause::CapturedValueTooLarge => FailureCodeV1::CapturedValueTooLarge,
        AgentFailureCause::ResultSettlementFailed => FailureCodeV1::ResultSettlementFailed,
    }
}

fn execution_failure_cause(failure: &StepExecutionFailure) -> FailureCauseV1 {
    match failure {
        StepExecutionFailure::Command(CommandExecutionFailure::UnsuccessfulExit { code }) => {
            let mut cause = FailureCauseV1::code(FailureCodeV1::CommandExit);
            cause.exit_code = *code;
            cause
        }
        StepExecutionFailure::Command(CommandExecutionFailure::Wait) => {
            FailureCauseV1::code(FailureCodeV1::CommandWaitFailed)
        }
        StepExecutionFailure::Agent(failure) => agent_failure_cause(failure),
    }
}

fn output_capture_failure_cause(failure: &OutputCaptureFailure) -> FailureCauseV1 {
    match failure {
        OutputCaptureFailure::StepUnavailable => {
            FailureCauseV1::code(FailureCodeV1::StepUnavailable)
        }
        OutputCaptureFailure::UnsupportedOutput => {
            FailureCauseV1::code(FailureCodeV1::OutputUnsupported)
        }
        OutputCaptureFailure::TaskUnavailable => {
            FailureCauseV1::code(FailureCodeV1::CaptureTaskUnavailable)
        }
        OutputCaptureFailure::Capture(failure) => {
            let code = match failure.kind() {
                CaptureFailureKind::AbsolutePath => FailureCodeV1::OutputPathAbsolute,
                CaptureFailureKind::LexicalEscape => FailureCodeV1::OutputPathEscape,
                CaptureFailureKind::EmptyPath => FailureCodeV1::OutputPathEmpty,
                CaptureFailureKind::Missing => FailureCodeV1::OutputMissing,
                CaptureFailureKind::SymbolicLink => FailureCodeV1::OutputSymbolicLink,
                CaptureFailureKind::NotDirectory => FailureCodeV1::OutputParentNotDirectory,
                CaptureFailureKind::NotRegularFile => FailureCodeV1::OutputNotRegularFile,
                CaptureFailureKind::SourceUnavailable => FailureCodeV1::OutputSourceUnavailable,
                CaptureFailureKind::FileCountLimitExceeded => FailureCodeV1::CapturedFileCountLimit,
                CaptureFailureKind::FileSizeLimitExceeded => FailureCodeV1::CapturedFileSizeLimit,
                CaptureFailureKind::TotalSizeLimitExceeded => FailureCodeV1::CapturedTotalSizeLimit,
                CaptureFailureKind::GitCarrierCountLimitExceeded => {
                    FailureCodeV1::CapturedGitCarrierCountLimit
                }
                CaptureFailureKind::GitCarrierSizeLimitExceeded => {
                    FailureCodeV1::CapturedGitCarrierSizeLimit
                }
                CaptureFailureKind::TotalGitCarrierSizeLimitExceeded => {
                    FailureCodeV1::CapturedTotalGitCarrierSizeLimit
                }
                CaptureFailureKind::CarrierProducerUnavailable => {
                    FailureCodeV1::GitBundleGenerationFailed
                }
                CaptureFailureKind::StagingUnavailable => FailureCodeV1::OutputStagingUnavailable,
            };
            let mut cause = FailureCauseV1::code(code);
            cause.output = Some(failure.output_identity().to_owned());
            cause
        }
        OutputCaptureFailure::Git { output, failure } => {
            let code = match failure {
                GitCaptureFailure::Cancelled | GitCaptureFailure::Artifact(_) => {
                    FailureCodeV1::OutputStagingUnavailable
                }
                GitCaptureFailure::ExecutionRootRebound => FailureCodeV1::GitExecutionRootRebound,
                GitCaptureFailure::StagingMismatch => FailureCodeV1::OutputStagingUnavailable,
                GitCaptureFailure::HeadUnavailable => FailureCodeV1::GitHeadUnavailable,
                GitCaptureFailure::BaselineNotAncestor => FailureCodeV1::GitBaselineNotAncestor,
                GitCaptureFailure::CleanlinessUnavailable => {
                    FailureCodeV1::GitCleanlinessUnavailable
                }
                GitCaptureFailure::WorkspaceDirty => FailureCodeV1::GitWorkspaceDirty,
                GitCaptureFailure::TreeUnavailable => FailureCodeV1::GitTreeUnavailable,
                GitCaptureFailure::RequiredObjectsUnavailable => {
                    FailureCodeV1::GitRequiredObjectsUnavailable
                }
                GitCaptureFailure::SourceAuthorityChanged => {
                    FailureCodeV1::GitSourceAuthorityChanged
                }
                GitCaptureFailure::GitStructureLimitExceeded => {
                    FailureCodeV1::GitStructureLimitExceeded
                }
                GitCaptureFailure::CommandTimedOut(_) => FailureCodeV1::GitCommandTimedOut,
                GitCaptureFailure::BundleGenerationFailed => {
                    FailureCodeV1::GitBundleGenerationFailed
                }
                GitCaptureFailure::BundleProfileInvalid => FailureCodeV1::GitBundleProfileInvalid,
                GitCaptureFailure::BundleVerificationFailed => {
                    FailureCodeV1::GitBundleVerificationFailed
                }
                GitCaptureFailure::WorkspaceChanged => FailureCodeV1::GitWorkspaceChanged,
                GitCaptureFailure::TemporaryStorageUnavailable => {
                    FailureCodeV1::GitTemporaryStorageUnavailable
                }
            };
            let mut cause = FailureCauseV1::code(code);
            cause.output = Some(output.clone());
            cause
        }
    }
}

fn retained_path(path: &Path) -> Result<String, LocalPublicationError> {
    if !path.is_absolute() {
        return Err(invalid_run_result());
    }
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(invalid_run_result)
}

fn timestamp(value: OffsetDateTime) -> Result<String, LocalPublicationError> {
    utc_timestamp(value).map_err(|_| invalid_run_result())
}

fn duration_milliseconds(duration: Duration) -> Result<u64, LocalPublicationError> {
    u64::try_from(duration.as_millis()).map_err(|_| invalid_run_result())
}

fn invalid_run_result() -> LocalPublicationError {
    LocalPublicationError::new(
        LocalPublicationPhase::Serialization,
        LocalPublicationFailureKind::InvalidRunResult,
    )
}

fn workflow_node_role(role: WorkflowNodeRole) -> WorkflowNodeRoleV1 {
    match role {
        WorkflowNodeRole::Step => WorkflowNodeRoleV1::Step,
        WorkflowNodeRole::Finalizer => WorkflowNodeRoleV1::Finalizer,
    }
}

pub(super) fn finalization_trigger(trigger: FinalizationTrigger) -> FinalizationTriggerV1 {
    match trigger {
        FinalizationTrigger::Succeeded => FinalizationTriggerV1::Succeeded,
        FinalizationTrigger::Failed => FinalizationTriggerV1::Failed,
        FinalizationTrigger::Cancelled => FinalizationTriggerV1::Cancelled,
    }
}

pub(super) fn cancellation_reason(reason: CancellationReason) -> CancellationReasonV1 {
    match reason {
        CancellationReason::UserRequest => CancellationReasonV1::UserRequest,
        CancellationReason::TerminationRequest => CancellationReasonV1::TerminationRequest,
        CancellationReason::CallerOutputFailure => CancellationReasonV1::CallerOutputFailure,
        CancellationReason::RunnerShutdown => CancellationReasonV1::RunnerShutdown,
        CancellationReason::ExecutionLeaseExpired => CancellationReasonV1::ExecutionLeaseExpired,
        CancellationReason::FinalizationForceAbort => CancellationReasonV1::FinalizationForceAbort,
    }
}

pub(super) fn cancellation_step_reason(reason: CancellationReason) -> StepReasonV1 {
    match reason {
        CancellationReason::UserRequest => StepReasonV1::UserRequest,
        CancellationReason::TerminationRequest => StepReasonV1::TerminationRequest,
        CancellationReason::CallerOutputFailure => StepReasonV1::CallerOutputFailure,
        CancellationReason::RunnerShutdown => StepReasonV1::RunnerShutdown,
        CancellationReason::ExecutionLeaseExpired => StepReasonV1::ExecutionLeaseExpired,
        CancellationReason::FinalizationForceAbort => StepReasonV1::FinalizationForceAbort,
    }
}

fn export_unavailable_reason(reason: ExportUnavailableReason) -> ExportUnavailableReasonV1 {
    match reason {
        ExportUnavailableReason::Failed => ExportUnavailableReasonV1::Failed,
        ExportUnavailableReason::Blocked => ExportUnavailableReasonV1::Blocked,
        ExportUnavailableReason::InputUnavailable => ExportUnavailableReasonV1::InputUnavailable,
        ExportUnavailableReason::NotRun => ExportUnavailableReasonV1::NotRun,
        ExportUnavailableReason::TriggerNotSelected => {
            ExportUnavailableReasonV1::TriggerNotSelected
        }
        ExportUnavailableReason::Cancelled => ExportUnavailableReasonV1::Cancelled,
    }
}

fn exit_status(run: &WorkflowRunResult, outcome: WorkflowOutcomeV1) -> u16 {
    use crate::exit_code::ExitCode;

    if run
        .steps
        .iter()
        .chain(
            run.finalization
                .iter()
                .flat_map(|finalization| &finalization.finalizers),
        )
        .any(|step| {
            step.command_output.as_ref().is_some_and(|output| {
                !output.standard_output().fully_drained()
                    || !output.standard_error().fully_drained()
            }) || step.invocations.iter().any(|invocation| {
                invocation
                    .diagnostics
                    .iter()
                    .any(|diagnostic| !diagnostic.stream.fully_drained)
            })
        })
        || run.finalization.as_ref().is_some_and(|finalization| {
            finalization.force_abort
                || finalization
                    .cancellation
                    .as_ref()
                    .is_some_and(|cancellation| {
                        matches!(
                            cancellation.reason,
                            CancellationReason::CallerOutputFailure
                                | CancellationReason::RunnerShutdown
                                | CancellationReason::ExecutionLeaseExpired
                                | CancellationReason::FinalizationForceAbort
                        )
                    })
        })
    {
        return ExitCode::GeneralFailure.as_u16();
    }
    match outcome {
        WorkflowOutcomeV1::Succeeded => ExitCode::Success.as_u16(),
        WorkflowOutcomeV1::Failed => ExitCode::GeneralFailure.as_u16(),
        WorkflowOutcomeV1::Cancelled => match &run.outcome {
            RunOutcome::Cancelled {
                reason: CancellationReason::UserRequest,
            } => ExitCode::Interrupted.as_u16(),
            RunOutcome::Cancelled {
                reason: CancellationReason::TerminationRequest,
            } => ExitCode::Terminated.as_u16(),
            RunOutcome::Cancelled {
                reason:
                    CancellationReason::CallerOutputFailure
                    | CancellationReason::RunnerShutdown
                    | CancellationReason::ExecutionLeaseExpired
                    | CancellationReason::FinalizationForceAbort,
            }
            | RunOutcome::Succeeded
            | RunOutcome::Failed { .. } => ExitCode::GeneralFailure.as_u16(),
        },
    }
}

struct HashingWriter<'a> {
    destination: &'a mut File,
    digest: &'a mut DigestContext,
    bytes: u64,
}

impl Write for HashingWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.destination.write(bytes)?;
        self.digest.update(&bytes[..written]);
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(written).map_err(|_| io::Error::other("export size"))?)
            .ok_or_else(|| io::Error::other("export size"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.destination.flush()
    }
}

struct PublicationTarget {
    supplied_parent: PathBuf,
    parent: OwnedFd,
    staging_parent: OwnedFd,
    name: OsString,
    normalized: String,
}

impl PublicationTarget {
    fn validate(
        destination: &Path,
        private_staging: Option<&Path>,
        expected_parents: Option<(&OwnedFd, &OwnedFd)>,
    ) -> Result<Self, LocalPublicationError> {
        let name = destination.file_name().ok_or_else(|| {
            LocalPublicationError::new(
                LocalPublicationPhase::TargetValidation,
                LocalPublicationFailureKind::InvalidResultPath,
            )
        })?;
        if name == OsStr::new(".") || name == OsStr::new("..") || name.to_str().is_none() {
            return Err(LocalPublicationError::new(
                LocalPublicationPhase::TargetValidation,
                LocalPublicationFailureKind::InvalidResultPath,
            ));
        }
        let supplied_parent = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let supplied_parent = if supplied_parent.is_absolute() {
            supplied_parent.to_owned()
        } else {
            std::env::current_dir()
                .map_err(|_| {
                    LocalPublicationError::new(
                        LocalPublicationPhase::TargetValidation,
                        LocalPublicationFailureKind::ParentUnavailable,
                    )
                })?
                .join(supplied_parent)
        };
        let canonical_parent = std::fs::canonicalize(&supplied_parent).map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::TargetValidation,
                LocalPublicationFailureKind::ParentUnavailable,
            )
        })?;
        let parent = open_directory(&canonical_parent).map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::TargetValidation,
                LocalPublicationFailureKind::ParentUnavailable,
            )
        })?;
        let normalized = canonical_parent
            .join(name)
            .to_str()
            .map(str::to_owned)
            .ok_or_else(|| {
                LocalPublicationError::new(
                    LocalPublicationPhase::TargetValidation,
                    LocalPublicationFailureKind::InvalidResultPath,
                )
            })?;
        let staging_parent = match private_staging {
            Some(path) => {
                let canonical = std::fs::canonicalize(path).map_err(|_| {
                    LocalPublicationError::new(
                        LocalPublicationPhase::TargetValidation,
                        LocalPublicationFailureKind::ParentUnavailable,
                    )
                })?;
                open_directory(&canonical).map_err(|_| {
                    LocalPublicationError::new(
                        LocalPublicationPhase::TargetValidation,
                        LocalPublicationFailureKind::ParentUnavailable,
                    )
                })?
            }
            None => rustix::io::dup(&parent).map_err(|_| {
                LocalPublicationError::new(
                    LocalPublicationPhase::TargetValidation,
                    LocalPublicationFailureKind::ParentUnavailable,
                )
            })?,
        };
        if let Some((expected_parent, expected_staging_parent)) = expected_parents
            && (!same_file(expected_parent, &parent).map_err(|_| invalid_publication_parent())?
                || !same_file(expected_staging_parent, &staging_parent)
                    .map_err(|_| invalid_publication_parent())?)
        {
            return Err(invalid_publication_parent());
        }
        verify_publication_capability(&staging_parent, &parent)?;
        Ok(Self {
            supplied_parent,
            parent,
            staging_parent,
            name: name.to_owned(),
            normalized,
        })
    }

    fn verify_parent(&self) -> Result<(), LocalPublicationError> {
        let rebound = std::fs::canonicalize(&self.supplied_parent).map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Commit,
                LocalPublicationFailureKind::ParentUnavailable,
            )
        })?;
        let rebound = open_directory(&rebound).map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Commit,
                LocalPublicationFailureKind::ParentUnavailable,
            )
        })?;
        if !same_file(&self.parent, &rebound).map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Commit,
                LocalPublicationFailureKind::ParentUnavailable,
            )
        })? {
            return Err(LocalPublicationError::new(
                LocalPublicationPhase::Commit,
                LocalPublicationFailureKind::ParentUnavailable,
            ));
        }
        Ok(())
    }

    fn existing_publication(
        &self,
        expected: &WorkflowResultV1,
        expected_bytes: &[u8],
    ) -> Result<ExistingPublication, LocalPublicationError> {
        let named = match statat(&self.parent, &self.name, AtFlags::SYMLINK_NOFOLLOW) {
            Err(Errno::NOENT) => return Ok(ExistingPublication::Absent),
            Ok(named) => named,
            Err(_) => return Ok(ExistingPublication::Conflict),
        };
        if FileType::from_raw_mode(named.st_mode) != FileType::Directory {
            return Ok(ExistingPublication::Conflict);
        }
        let root = match openat(
            &self.parent,
            &self.name,
            directory_open_flags(),
            Mode::empty(),
        ) {
            Ok(root) => root,
            Err(_) => return Ok(ExistingPublication::Conflict),
        };
        let opened = fstat(&root).map_err(|_| result_conflict())?;
        if opened.st_dev != named.st_dev || opened.st_ino != named.st_ino {
            return Ok(ExistingPublication::Conflict);
        }
        let retained = match artifact_set::read_and_validate(
            &root,
            result_metadata::MAXIMUM_RESULT_JSON_BYTES,
        ) {
            Ok(retained) => retained,
            Err(_) => return Ok(ExistingPublication::Conflict),
        };
        if retained != *expected {
            return Ok(ExistingPublication::Conflict);
        }
        let descriptor = match openat(
            &root,
            RESULT_FILE,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(descriptor) => descriptor,
            Err(_) => return Ok(ExistingPublication::Conflict),
        };
        let mut file = File::from(descriptor);
        let mut bytes = Vec::new();
        if Read::by_ref(&mut file)
            .take(result_metadata::MAXIMUM_RESULT_JSON_BYTES.saturating_add(1))
            .read_to_end(&mut bytes)
            .is_err()
        {
            return Ok(ExistingPublication::Conflict);
        }
        Ok(if bytes == expected_bytes {
            ExistingPublication::Identical
        } else {
            ExistingPublication::Conflict
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExistingPublication {
    Absent,
    Identical,
    Conflict,
}

fn result_conflict() -> LocalPublicationError {
    LocalPublicationError::new(
        LocalPublicationPhase::Commit,
        LocalPublicationFailureKind::ResultConflict,
    )
}

fn invalid_publication_parent() -> LocalPublicationError {
    LocalPublicationError::new(
        LocalPublicationPhase::TargetValidation,
        LocalPublicationFailureKind::ParentUnavailable,
    )
}

struct StagingDirectory<'a> {
    parent: &'a OwnedFd,
    identity: String,
    root: OwnedFd,
    exports: Option<OwnedFd>,
    export_files: Vec<String>,
    result_created: bool,
    committed: bool,
}

impl<'a> StagingDirectory<'a> {
    fn create(target: &'a PublicationTarget) -> Result<Self, LocalPublicationError> {
        let (identity, root) = create_staging_root(&target.staging_parent)?;
        let mut staging = Self {
            parent: &target.staging_parent,
            identity,
            root,
            exports: None,
            export_files: Vec::new(),
            result_created: false,
            committed: false,
        };
        mkdirat(&staging.root, EXPORT_DIRECTORY, Mode::RWXU).map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Staging,
                LocalPublicationFailureKind::StagingUnavailable,
            )
        })?;
        let exports = openat(
            &staging.root,
            EXPORT_DIRECTORY,
            directory_open_flags(),
            Mode::empty(),
        )
        .map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Staging,
                LocalPublicationFailureKind::StagingUnavailable,
            )
        })?;
        staging.exports = Some(exports);
        Ok(staging)
    }

    fn create_export(&mut self, name: &str) -> Result<File, LocalPublicationFailureKind> {
        let exports = self
            .exports
            .as_ref()
            .ok_or(LocalPublicationFailureKind::StagingUnavailable)?;
        let file = openat(
            exports,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|_| LocalPublicationFailureKind::ExportWriteUnavailable)?;
        self.export_files.push(name.to_owned());
        Ok(File::from(file))
    }

    fn expose_export(
        &mut self,
        artifacts: &ArtifactStaging,
        carrier: &StagedCarrier,
        expected_output_identity: &str,
        name: &str,
    ) -> Result<(), ArtifactExposeFailure> {
        let exports = self
            .exports
            .as_ref()
            .ok_or(ArtifactExposeFailure::Unavailable)?;
        self.export_files.push(name.to_owned());
        artifacts.expose_carrier(carrier, expected_output_identity, exports, OsStr::new(name))
    }

    fn write_result(&mut self, bytes: &[u8]) -> Result<File, LocalPublicationError> {
        let mut result = openat(
            &self.root,
            RESULT_FILE,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map(File::from)
        .map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Serialization,
                LocalPublicationFailureKind::SerializationUnavailable,
            )
        })?;
        self.result_created = true;
        result
            .write_all(bytes)
            .and_then(|()| result.flush())
            .map_err(|_| {
                LocalPublicationError::new(
                    LocalPublicationPhase::Serialization,
                    LocalPublicationFailureKind::SerializationUnavailable,
                )
            })?;
        Ok(result)
    }

    fn verify(&self, result: &WorkflowResultV1) -> Result<(), Errno> {
        let named = statat(self.parent, &self.identity, AtFlags::SYMLINK_NOFOLLOW)?;
        let opened = fstat(&self.root)?;
        if named.st_dev != opened.st_dev
            || named.st_ino != opened.st_ino
            || FileType::from_raw_mode(named.st_mode) != FileType::Directory
        {
            return Err(Errno::IO);
        }
        let root_entries = directory_entries(&self.root)?;
        if root_entries
            != BTreeSet::from([
                RESULT_FILE.as_bytes().to_vec(),
                EXPORT_DIRECTORY.as_bytes().to_vec(),
            ])
        {
            return Err(Errno::IO);
        }
        let named_result = statat(&self.root, RESULT_FILE, AtFlags::SYMLINK_NOFOLLOW)?;
        let named_exports = statat(&self.root, EXPORT_DIRECTORY, AtFlags::SYMLINK_NOFOLLOW)?;
        let exports = self.exports.as_ref().ok_or(Errno::IO)?;
        let opened_exports = fstat(exports)?;
        if FileType::from_raw_mode(named_result.st_mode) != FileType::RegularFile
            || FileType::from_raw_mode(named_exports.st_mode) != FileType::Directory
            || FileType::from_raw_mode(opened_exports.st_mode) != FileType::Directory
            || named_exports.st_dev != opened_exports.st_dev
            || named_exports.st_ino != opened_exports.st_ino
        {
            return Err(Errno::IO);
        }
        let expected_exports = self
            .export_files
            .iter()
            .map(|name| name.as_bytes().to_vec())
            .collect::<BTreeSet<_>>();
        if directory_entries(exports)? != expected_exports {
            return Err(Errno::IO);
        }
        for name in &self.export_files {
            if FileType::from_raw_mode(statat(exports, name, AtFlags::SYMLINK_NOFOLLOW)?.st_mode)
                != FileType::RegularFile
            {
                return Err(Errno::IO);
            }
        }
        let staged =
            artifact_set::read_and_validate(&self.root, result_metadata::MAXIMUM_RESULT_JSON_BYTES)
                .map_err(|_| Errno::IO)?;
        (staged == *result).then_some(()).ok_or(Errno::IO)
    }

    fn commit(&mut self, target: &PublicationTarget) -> Result<(), LocalPublicationError> {
        renameat_with(
            self.parent,
            &self.identity,
            &target.parent,
            &target.name,
            RenameFlags::NOREPLACE,
        )
        .map_err(|failure| {
            let kind = match failure {
                Errno::EXIST | Errno::NOTEMPTY => LocalPublicationFailureKind::DestinationExists,
                _ => LocalPublicationFailureKind::AtomicPublicationUnavailable,
            };
            LocalPublicationError::new(LocalPublicationPhase::Commit, kind)
        })?;
        self.committed = true;
        Ok(())
    }

    fn cleanup(&mut self) {
        if self.committed {
            return;
        }
        let same_root = statat(self.parent, &self.identity, AtFlags::SYMLINK_NOFOLLOW)
            .and_then(|named| {
                let opened = fstat(&self.root)?;
                Ok(named.st_dev == opened.st_dev && named.st_ino == opened.st_ino)
            })
            .unwrap_or(false);
        if !same_root {
            return;
        }
        if let Some(exports) = self.exports.take() {
            for name in &self.export_files {
                let _ = unlinkat(&exports, name, AtFlags::empty());
            }
            drop(exports);
        }
        if self.result_created {
            let _ = unlinkat(&self.root, RESULT_FILE, AtFlags::empty());
        }
        let _ = unlinkat(&self.root, EXPORT_DIRECTORY, AtFlags::REMOVEDIR);
        let _ = unlinkat(self.parent, &self.identity, AtFlags::REMOVEDIR);
    }
}

impl Drop for StagingDirectory<'_> {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn verify_publication_capability(
    staging_parent: &OwnedFd,
    target_parent: &OwnedFd,
) -> Result<(), LocalPublicationError> {
    let source = create_validation_directory(staging_parent)?;
    for _ in 0..STAGING_ATTEMPTS {
        let destination = format!(
            ".result-preflight-{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        );
        match renameat_with(
            staging_parent,
            &source,
            target_parent,
            &destination,
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) => {
                return unlinkat(target_parent, destination, AtFlags::REMOVEDIR).map_err(|_| {
                    LocalPublicationError::new(
                        LocalPublicationPhase::TargetValidation,
                        LocalPublicationFailureKind::ParentUnavailable,
                    )
                });
            }
            Err(Errno::EXIST | Errno::NOTEMPTY) => {}
            Err(_) => {
                let _ = unlinkat(staging_parent, &source, AtFlags::REMOVEDIR);
                return Err(LocalPublicationError::new(
                    LocalPublicationPhase::TargetValidation,
                    LocalPublicationFailureKind::AtomicPublicationUnavailable,
                ));
            }
        }
    }
    let _ = unlinkat(staging_parent, source, AtFlags::REMOVEDIR);
    Err(LocalPublicationError::new(
        LocalPublicationPhase::TargetValidation,
        LocalPublicationFailureKind::AtomicPublicationUnavailable,
    ))
}

fn create_validation_directory(parent: &OwnedFd) -> Result<String, LocalPublicationError> {
    for _ in 0..STAGING_ATTEMPTS {
        let identity = format!(
            ".result-preflight-{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        );
        match mkdirat(parent, &identity, Mode::RWXU) {
            Ok(()) => return Ok(identity),
            Err(Errno::EXIST) => {}
            Err(_) => {
                return Err(LocalPublicationError::new(
                    LocalPublicationPhase::TargetValidation,
                    LocalPublicationFailureKind::ParentUnavailable,
                ));
            }
        }
    }
    Err(LocalPublicationError::new(
        LocalPublicationPhase::TargetValidation,
        LocalPublicationFailureKind::ParentUnavailable,
    ))
}

fn create_staging_root(parent: &OwnedFd) -> Result<(String, OwnedFd), LocalPublicationError> {
    for _ in 0..STAGING_ATTEMPTS {
        let identity = format!(
            ".result-{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        );
        match mkdirat(parent, &identity, Mode::RWXU) {
            Ok(()) => {
                let root = openat(parent, &identity, directory_open_flags(), Mode::empty());
                return match root {
                    Ok(root) => Ok((identity, root)),
                    Err(_) => {
                        let _ = unlinkat(parent, &identity, AtFlags::REMOVEDIR);
                        Err(LocalPublicationError::new(
                            LocalPublicationPhase::Staging,
                            LocalPublicationFailureKind::StagingUnavailable,
                        ))
                    }
                };
            }
            Err(Errno::EXIST) => {}
            Err(_) => {
                return Err(LocalPublicationError::new(
                    LocalPublicationPhase::Staging,
                    LocalPublicationFailureKind::StagingUnavailable,
                ));
            }
        }
    }
    Err(LocalPublicationError::new(
        LocalPublicationPhase::Staging,
        LocalPublicationFailureKind::StagingUnavailable,
    ))
}

fn directory_open_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

fn close_file(file: File) -> io::Result<()> {
    nix::unistd::close(file).map_err(io::Error::from)
}

fn directory_entries(directory: &OwnedFd) -> Result<BTreeSet<Vec<u8>>, Errno> {
    directory_entry_names(directory)
}

#[cfg(test)]
mod tests;
