use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
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
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::admission::CancellationReason;
use super::agent_input::AgentInputStartFailure;
use super::artifact::{ArtifactReadFailure, ArtifactStaging, CaptureFailureKind};
use super::canonical_json;
use super::diagnostic::{CapturedDiagnosticStream, StepDiagnostic};
use super::execution_root::open_directory;
use super::input::InputPreparationFailureKind;
use super::private_staging::{directory_entry_names, same_file};
use super::resolution::WorkflowContentDigest;
use super::runtime::{
    ExportSet, ExportUnavailableReason, ExportValue, FailurePhase, NotRunReason, RunOutcome,
    StepFailure, StepState,
};
use super::schema_common::{lowercase_hex, utc_timestamp};
use super::step_runtime::{
    CommandExecutionFailure, CommandLaunchFailure, CommandPreparationFailure, OutputCaptureFailure,
    StepExecutionFailure, StepFailureCause, StepStartFailure, WorkingDirectoryFailure,
};
use super::value::CapturedValue;

const COMMAND: &str = "scherzo-cloud workflow run";
const RETRY_COMMAND: &str = "scherzo-cloud workflow retry";
const RESULT_FILE: &str = "result.json";
const EXPORT_DIRECTORY: &str = "exports";
const STAGING_ATTEMPTS: usize = 16;
const MAXIMUM_RETAINED_BYTES_PER_STREAM: u64 = 65_536;

#[derive(Clone, Debug)]
pub(crate) struct WorkflowRunTiming {
    pub(crate) started_at: OffsetDateTime,
    pub(crate) finished_at: OffsetDateTime,
    pub(crate) duration: Duration,
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub(crate) struct WorkflowRunStep {
    pub(crate) id: String,
    pub(crate) kind: WorkflowRunStepKind,
    pub(crate) state: StepState<StepFailureCause, CapturedValue>,
    pub(crate) timing: Option<WorkflowStepTiming>,
    pub(crate) command_output: Option<StepDiagnostic>,
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
    pub(crate) exports: ExportSet<CapturedValue>,
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
    ArtifactUnavailable,
    ExportWriteUnavailable,
    UnsupportedExport,
    InvalidRunResult,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowResultV1 {
    schema_version: u8,
    attempt_number: u64,
    workflow: WorkflowIdentityV1,
    execution: WorkflowExecutionV1,
    command_output_policy: CommandOutputPolicyV1,
    outcome: WorkflowOutcomeV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_failure: Option<PrimaryFailureV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cancellation: Option<CancellationV1>,
    steps: Vec<WorkflowStepV1>,
    exports: BTreeMap<String, ExportV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct WorkflowIdentityV1 {
    path: String,
    provenance: WorkflowProvenanceV1,
    digest: DigestV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowProvenanceV1 {
    kind: &'static str,
    source_root: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowExecutionV1 {
    execution_root: String,
    maximum_parallel_steps: usize,
    started_at: String,
    finished_at: String,
    duration_milliseconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CommandOutputPolicyV1 {
    encoding: &'static str,
    maximum_retained_bytes_per_stream: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct DigestV1 {
    algorithm: &'static str,
    value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CancellationV1 {
    reason: CancellationReasonV1,
    force_stop_deadline: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CancellationReasonV1 {
    UserRequest,
    TerminationRequest,
    CallerOutputFailure,
    RunnerShutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum WorkflowStepStateV1 {
    Succeeded,
    Failed,
    Blocked,
    NotRun,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowStepV1 {
    id: String,
    kind: &'static str,
    state: WorkflowStepStateV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_milliseconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    committed_output_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<FailureV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dependency: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<StepReasonV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_output: Option<CommandOutputV1>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum StepReasonV1 {
    FailureStop,
    UserRequest,
    TerminationRequest,
    CallerOutputFailure,
    RunnerShutdown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct FailureV1 {
    phase: FailurePhaseV1,
    cause: FailureCauseV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum FailurePhaseV1 {
    Start,
    Execution,
    OutputCapture,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct FailureCauseV1 {
    code: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collection_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
}

impl FailureCauseV1 {
    fn code(code: &'static str) -> Self {
        Self {
            code,
            input: None,
            collection_index: None,
            output: None,
            exit_code: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct PrimaryFailureV1 {
    step: String,
    phase: FailurePhaseV1,
    cause: FailureCauseV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct CommandOutputV1 {
    stdout: DiagnosticStreamV1,
    stderr: DiagnosticStreamV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticStreamV1 {
    encoding: &'static str,
    data: String,
    retained_bytes: u64,
    discarded_bytes: u64,
    truncated: bool,
    fully_drained: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum ExportV1 {
    Available {
        kind: &'static str,
        #[serde(rename = "mediaType")]
        media_type: String,
        path: String,
        #[serde(rename = "sizeBytes")]
        size_bytes: u64,
        digest: DigestV1,
    },
    Unavailable {
        reason: ExportUnavailableReasonV1,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
enum ExportUnavailableReasonV1 {
    #[serde(rename = "source_failed")]
    Failed,
    #[serde(rename = "source_blocked")]
    Blocked,
    #[serde(rename = "source_not_run")]
    NotRun,
    #[serde(rename = "source_cancelled")]
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PublicationBoundary {
    StagingCreated,
    BeforeExportCopy { export: String },
    AfterExportCopy { export: String },
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

    let mut exports = BTreeMap::new();
    for (index, (name, export)) in run.exports.iter().enumerate() {
        let ordinal = index.checked_add(1).ok_or_else(|| {
            LocalPublicationError::for_export(
                LocalPublicationPhase::Serialization,
                LocalPublicationFailureKind::InvalidRunResult,
                name,
            )
        })?;
        match export {
            ExportValue::Available { output } => {
                observe(
                    observer,
                    &PublicationBoundary::BeforeExportCopy {
                        export: name.clone(),
                    },
                    LocalPublicationPhase::ExportCopy,
                    LocalPublicationFailureKind::ArtifactUnavailable,
                    Some(name),
                )?;
                let file_name = format!("{ordinal:04}");
                let (kind, media_type, expected_size) = match output {
                    CapturedValue::File(file) => {
                        ("file", file.media_type().to_owned(), file.size())
                    }
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
                };
                let mut destination = staging.create_export(&file_name).map_err(|kind| {
                    LocalPublicationError::for_export(LocalPublicationPhase::ExportCopy, kind, name)
                })?;
                let mut digest = DigestContext::new(&SHA256);
                let copied = {
                    let mut hashing = HashingWriter {
                        destination: &mut destination,
                        digest: &mut digest,
                        bytes: 0,
                    };
                    match output {
                        CapturedValue::File(file) => {
                            artifacts
                                .copy_to(file.handle(), &mut hashing)
                                .map_err(|failure| copy_error(name, failure))?;
                        }
                        CapturedValue::Text(text) => hashing
                            .write_all(text.as_bytes())
                            .map_err(|_| export_write_error(name))?,
                        CapturedValue::Json(value) => {
                            canonical_json::to_writer(&mut hashing, value)
                                .map_err(|_| export_write_error(name))?
                        }
                    }
                    hashing.flush().map_err(|_| export_write_error(name))?;
                    hashing.bytes
                };
                destination.flush().map_err(|_| export_write_error(name))?;
                if copied != expected_size {
                    return Err(LocalPublicationError::for_export(
                        LocalPublicationPhase::ExportCopy,
                        LocalPublicationFailureKind::ArtifactUnavailable,
                        name,
                    ));
                }
                observer
                    .close_staged_file(
                        destination,
                        &StagedFile::Export {
                            export: name.clone(),
                        },
                    )
                    .map_err(|_| {
                        LocalPublicationError::for_export(
                            LocalPublicationPhase::Close,
                            LocalPublicationFailureKind::ExportWriteUnavailable,
                            name,
                        )
                    })?;

                exports.insert(
                    name.clone(),
                    ExportV1::Available {
                        kind,
                        media_type,
                        path: format!("{EXPORT_DIRECTORY}/{file_name}"),
                        size_bytes: copied,
                        digest: DigestV1 {
                            algorithm: "sha256",
                            value: lowercase_hex(digest.finish().as_ref()),
                        },
                    },
                );
                observe(
                    observer,
                    &PublicationBoundary::AfterExportCopy {
                        export: name.clone(),
                    },
                    LocalPublicationPhase::ExportCopy,
                    LocalPublicationFailureKind::ArtifactUnavailable,
                    Some(name),
                )?;
            }
            ExportValue::Unavailable { reason } => {
                exports.insert(
                    name.clone(),
                    ExportV1::Unavailable {
                        reason: export_unavailable_reason(*reason),
                    },
                );
            }
        }
    }

    observe(
        observer,
        &PublicationBoundary::BeforeSerialization,
        LocalPublicationPhase::Serialization,
        LocalPublicationFailureKind::SerializationUnavailable,
        None,
    )?;
    let result = build_result(run, exports)?;
    let mut result_bytes = serde_json::to_vec_pretty(&result).map_err(|_| {
        LocalPublicationError::new(
            LocalPublicationPhase::Serialization,
            LocalPublicationFailureKind::SerializationUnavailable,
        )
    })?;
    result_bytes.push(b'\n');
    let result_file = staging.write_result(&result_bytes)?;
    observer
        .close_staged_file(result_file, &StagedFile::Result)
        .map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Close,
                LocalPublicationFailureKind::SerializationUnavailable,
            )
        })?;
    staging.verify().map_err(|_| {
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
    target.verify_parent_and_absence()?;
    staging.verify().map_err(|_| {
        LocalPublicationError::new(
            LocalPublicationPhase::Verification,
            LocalPublicationFailureKind::VerificationUnavailable,
        )
    })?;
    staging.commit(target)?;
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

fn export_write_error(export: &str) -> LocalPublicationError {
    LocalPublicationError::for_export(
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::ExportWriteUnavailable,
        export,
    )
}

fn copy_error(export: &str, failure: ArtifactReadFailure) -> LocalPublicationError {
    let kind = match failure {
        ArtifactReadFailure::UnknownHandle | ArtifactReadFailure::Unavailable => {
            LocalPublicationFailureKind::ArtifactUnavailable
        }
        ArtifactReadFailure::DestinationWrite => {
            LocalPublicationFailureKind::ExportWriteUnavailable
        }
    };
    LocalPublicationError::for_export(LocalPublicationPhase::ExportCopy, kind, export)
}

fn build_result(
    run: &WorkflowRunResult,
    exports: BTreeMap<String, ExportV1>,
) -> Result<WorkflowResultV1, LocalPublicationError> {
    let source_root = retained_path(&run.source_root)?;
    let execution_root = retained_path(&run.execution_root)?;
    let (outcome, primary_failure, expected_cancellation) = match &run.outcome {
        RunOutcome::Succeeded => (WorkflowOutcomeV1::Succeeded, None, None),
        RunOutcome::Failed {
            primary_failure,
            later_cancellation,
        } => (
            WorkflowOutcomeV1::Failed,
            Some(primary_failure_v1(primary_failure)?),
            *later_cancellation,
        ),
        RunOutcome::Cancelled { reason } => (WorkflowOutcomeV1::Cancelled, None, Some(*reason)),
    };
    let cancellation = match (&run.cancellation, expected_cancellation) {
        (None, None) => None,
        (Some(cancellation), Some(reason)) if cancellation.reason == reason => {
            Some(CancellationV1 {
                reason: cancellation_reason(reason),
                force_stop_deadline: timestamp(cancellation.force_stop_deadline)?,
            })
        }
        (None, Some(_)) | (Some(_), None) | (Some(_), Some(_)) => {
            return Err(invalid_run_result());
        }
    };
    let steps = run
        .steps
        .iter()
        .map(step_v1)
        .collect::<Result<Vec<_>, _>>()?;

    if run.attempt_number == 0 {
        return Err(invalid_run_result());
    }
    Ok(WorkflowResultV1 {
        schema_version: 1,
        attempt_number: run.attempt_number,
        workflow: WorkflowIdentityV1 {
            path: run.workflow_path.clone(),
            provenance: WorkflowProvenanceV1 {
                kind: "local",
                source_root,
            },
            digest: DigestV1 {
                algorithm: run.content_digest.algorithm.as_str(),
                value: run.content_digest.value.clone(),
            },
        },
        execution: WorkflowExecutionV1 {
            execution_root,
            maximum_parallel_steps: run.maximum_parallel_steps.get(),
            started_at: timestamp(run.timing.started_at)?,
            finished_at: timestamp(run.timing.finished_at)?,
            duration_milliseconds: duration_milliseconds(run.timing.duration)?,
        },
        command_output_policy: CommandOutputPolicyV1 {
            encoding: "base64",
            maximum_retained_bytes_per_stream: MAXIMUM_RETAINED_BYTES_PER_STREAM,
        },
        outcome,
        primary_failure,
        cancellation,
        steps,
        exports,
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
    let (state, committed_output_count, failure, dependency, reason) = match &step.state {
        StepState::Succeeded { outputs } => (
            WorkflowStepStateV1::Succeeded,
            Some(outputs.len()),
            None,
            None,
            None,
        ),
        StepState::Failed { phase, cause } => (
            WorkflowStepStateV1::Failed,
            None,
            Some(failure_v1(*phase, cause)?),
            None,
            None,
        ),
        StepState::Blocked { dependency } => (
            WorkflowStepStateV1::Blocked,
            None,
            None,
            Some(dependency.clone()),
            None,
        ),
        StepState::NotRun {
            reason: NotRunReason::FailureStop,
        } => (
            WorkflowStepStateV1::NotRun,
            None,
            None,
            None,
            Some(StepReasonV1::FailureStop),
        ),
        StepState::Cancelled { reason } => (
            WorkflowStepStateV1::Cancelled,
            None,
            None,
            None,
            Some(cancellation_step_reason(*reason)),
        ),
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
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
        kind: match step.kind {
            WorkflowRunStepKind::Command => "cmd",
            WorkflowRunStepKind::Agent => "agent",
        },
        state,
        started_at,
        duration_milliseconds,
        committed_output_count,
        failure,
        dependency,
        reason,
        command_output,
    })
}

fn command_output_v1(
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
    if retained_bytes > MAXIMUM_RETAINED_BYTES_PER_STREAM {
        return Err(invalid_run_result());
    }
    let discarded_bytes = stream
        .truncation()
        .map_or(0, |truncation| truncation.discarded_bytes());
    Ok(DiagnosticStreamV1 {
        encoding: "base64",
        data: BASE64_STANDARD.encode(stream.bytes()),
        retained_bytes,
        discarded_bytes,
        truncated: discarded_bytes != 0,
        fully_drained: stream.fully_drained(),
    })
}

fn primary_failure_v1(
    failure: &StepFailure<StepFailureCause>,
) -> Result<PrimaryFailureV1, LocalPublicationError> {
    let failure_v1 = failure_v1(failure.phase, &failure.cause)?;
    Ok(PrimaryFailureV1 {
        step: failure.step.clone(),
        phase: failure_v1.phase,
        cause: failure_v1.cause,
    })
}

fn failure_v1(
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
        StepStartFailure::StepUnavailable => FailureCauseV1::code("step_unavailable"),
        StepStartFailure::PreparationTaskUnavailable => {
            FailureCauseV1::code("preparation_task_unavailable")
        }
        StepStartFailure::InputsUnavailable => FailureCauseV1::code("inputs_unavailable"),
        StepStartFailure::InputPreparation(failure) => {
            let code = match failure.kind() {
                InputPreparationFailureKind::InvalidInputName => "input_invalid_name",
                InputPreparationFailureKind::ValueCountLimitExceeded => "input_value_count_limit",
                InputPreparationFailureKind::ValueSizeLimitExceeded => "input_value_size_limit",
                InputPreparationFailureKind::TotalSizeLimitExceeded => "input_total_size_limit",
                InputPreparationFailureKind::CollectionOrdinalLimitExceeded => {
                    "input_collection_ordinal_limit"
                }
                InputPreparationFailureKind::ValueTypeMismatch => "input_type_mismatch",
                InputPreparationFailureKind::SourceUnavailable => "input_source_unavailable",
                InputPreparationFailureKind::StagingUnavailable => "input_staging_unavailable",
                InputPreparationFailureKind::LiveLimitExceeded => "input_live_limit",
            };
            let mut cause = FailureCauseV1::code(code);
            cause.input = failure.input_identity().map(str::to_owned);
            cause.collection_index = failure.collection_index();
            cause
        }
        StepStartFailure::AgentInput(failure) => agent_input_failure_cause(failure),
        StepStartFailure::AgentRuntimeUnavailable => {
            FailureCauseV1::code("agent_runtime_unavailable")
        }
        StepStartFailure::Agent(failure) => FailureCauseV1::code(failure.code()),
        StepStartFailure::OutputsUnsupported => FailureCauseV1::code("outputs_unsupported"),
        StepStartFailure::WorkingDirectory(failure) => FailureCauseV1::code(match failure {
            WorkingDirectoryFailure::ExecutionRootRebound => "execution_root_rebound",
            WorkingDirectoryFailure::Unavailable => "working_directory_unavailable",
            WorkingDirectoryFailure::EscapesExecutionRoot => "working_directory_escape",
            WorkingDirectoryFailure::NotDirectory => "working_directory_not_directory",
        }),
        StepStartFailure::CommandPreparation(failure) => FailureCauseV1::code(match failure {
            CommandPreparationFailure::InvalidArgv => "command_argv_invalid",
            CommandPreparationFailure::PathNotConfigured => "command_path_unconfigured",
            CommandPreparationFailure::ExecutableNotFound => "executable_not_found",
            CommandPreparationFailure::ExecutableUnavailable => "executable_unavailable",
        }),
        StepStartFailure::CommandLaunch(failure) => FailureCauseV1::code(match failure {
            CommandLaunchFailure::NotFound => "command_launch_not_found",
            CommandLaunchFailure::PermissionDenied => "command_launch_permission_denied",
            CommandLaunchFailure::InvalidInput => "command_launch_invalid_input",
            CommandLaunchFailure::Other => "command_launch_failed",
        }),
    };
    Ok(cause)
}

fn agent_input_failure_cause(failure: &AgentInputStartFailure) -> FailureCauseV1 {
    FailureCauseV1::code(match failure {
        AgentInputStartFailure::StepUnavailable => "agent_step_unavailable",
        AgentInputStartFailure::AgentAdmissionUnavailable => "agent_admission_unavailable",
        AgentInputStartFailure::InputsUnavailable => "agent_inputs_unavailable",
        AgentInputStartFailure::MissingUpstreamValue { .. } => "agent_input_missing_upstream",
        AgentInputStartFailure::ValueTypeMismatch { .. } => "agent_input_type_mismatch",
        AgentInputStartFailure::RetainedSourceUnavailable { .. } => "agent_source_unavailable",
        AgentInputStartFailure::InvalidRetainedText { .. } => "agent_source_text_invalid",
        AgentInputStartFailure::ResultSchemaUnavailable { .. } => "agent_result_schema_unavailable",
        AgentInputStartFailure::InvalidValueMode => "agent_value_mode_invalid",
        AgentInputStartFailure::AttachmentCountLimitExceeded { .. } => {
            "agent_attachment_count_limit"
        }
        AgentInputStartFailure::AttachmentBytesLimitExceeded { .. } => {
            "agent_attachment_bytes_limit"
        }
        AgentInputStartFailure::WorkingDirectory(failure) => match failure {
            WorkingDirectoryFailure::ExecutionRootRebound => "execution_root_rebound",
            WorkingDirectoryFailure::Unavailable => "working_directory_unavailable",
            WorkingDirectoryFailure::EscapesExecutionRoot => "working_directory_escape",
            WorkingDirectoryFailure::NotDirectory => "working_directory_not_directory",
        },
        AgentInputStartFailure::ArtifactStagingMismatch => "artifact_staging_mismatch",
        AgentInputStartFailure::AgentStagingMismatch => "agent_staging_mismatch",
        AgentInputStartFailure::StagingUnavailable => "agent_input_staging_unavailable",
    })
}

fn execution_failure_cause(failure: &StepExecutionFailure) -> FailureCauseV1 {
    match failure {
        StepExecutionFailure::Command(CommandExecutionFailure::UnsuccessfulExit { code }) => {
            let mut cause = FailureCauseV1::code("command_exit");
            cause.exit_code = *code;
            cause
        }
        StepExecutionFailure::Command(CommandExecutionFailure::Wait) => {
            FailureCauseV1::code("command_wait_failed")
        }
        StepExecutionFailure::Agent(failure) => FailureCauseV1::code(failure.code()),
    }
}

fn output_capture_failure_cause(failure: &OutputCaptureFailure) -> FailureCauseV1 {
    match failure {
        OutputCaptureFailure::StepUnavailable => FailureCauseV1::code("step_unavailable"),
        OutputCaptureFailure::UnsupportedOutput => FailureCauseV1::code("output_unsupported"),
        OutputCaptureFailure::TaskUnavailable => FailureCauseV1::code("capture_task_unavailable"),
        OutputCaptureFailure::Capture(failure) => {
            let code = match failure.kind() {
                CaptureFailureKind::AbsolutePath => "output_path_absolute",
                CaptureFailureKind::LexicalEscape => "output_path_escape",
                CaptureFailureKind::EmptyPath => "output_path_empty",
                CaptureFailureKind::Missing => "output_missing",
                CaptureFailureKind::SymbolicLink => "output_symbolic_link",
                CaptureFailureKind::NotDirectory => "output_parent_not_directory",
                CaptureFailureKind::NotRegularFile => "output_not_regular_file",
                CaptureFailureKind::SourceUnavailable => "output_source_unavailable",
                CaptureFailureKind::FileCountLimitExceeded => "captured_file_count_limit",
                CaptureFailureKind::FileSizeLimitExceeded => "captured_file_size_limit",
                CaptureFailureKind::TotalSizeLimitExceeded => "captured_total_size_limit",
                CaptureFailureKind::StagingUnavailable => "output_staging_unavailable",
            };
            let mut cause = FailureCauseV1::code(code);
            cause.output = Some(failure.output_identity().to_owned());
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

pub(super) fn cancellation_reason(reason: CancellationReason) -> CancellationReasonV1 {
    match reason {
        CancellationReason::UserRequest => CancellationReasonV1::UserRequest,
        CancellationReason::TerminationRequest => CancellationReasonV1::TerminationRequest,
        CancellationReason::CallerOutputFailure => CancellationReasonV1::CallerOutputFailure,
        CancellationReason::RunnerShutdown => CancellationReasonV1::RunnerShutdown,
    }
}

fn cancellation_step_reason(reason: CancellationReason) -> StepReasonV1 {
    match reason {
        CancellationReason::UserRequest => StepReasonV1::UserRequest,
        CancellationReason::TerminationRequest => StepReasonV1::TerminationRequest,
        CancellationReason::CallerOutputFailure => StepReasonV1::CallerOutputFailure,
        CancellationReason::RunnerShutdown => StepReasonV1::RunnerShutdown,
    }
}

fn export_unavailable_reason(reason: ExportUnavailableReason) -> ExportUnavailableReasonV1 {
    match reason {
        ExportUnavailableReason::Failed => ExportUnavailableReasonV1::Failed,
        ExportUnavailableReason::Blocked => ExportUnavailableReasonV1::Blocked,
        ExportUnavailableReason::NotRun => ExportUnavailableReasonV1::NotRun,
        ExportUnavailableReason::Cancelled => ExportUnavailableReasonV1::Cancelled,
    }
}

fn exit_status(run: &WorkflowRunResult, outcome: WorkflowOutcomeV1) -> u16 {
    if run.steps.iter().any(|step| {
        step.command_output.as_ref().is_some_and(|output| {
            !output.standard_output().fully_drained() || !output.standard_error().fully_drained()
        })
    }) {
        return 1;
    }
    match outcome {
        WorkflowOutcomeV1::Succeeded => 0,
        WorkflowOutcomeV1::Failed => 1,
        WorkflowOutcomeV1::Cancelled => match &run.outcome {
            RunOutcome::Cancelled {
                reason: CancellationReason::UserRequest,
            } => 130,
            RunOutcome::Cancelled {
                reason: CancellationReason::TerminationRequest,
            } => 143,
            RunOutcome::Cancelled {
                reason: CancellationReason::CallerOutputFailure | CancellationReason::RunnerShutdown,
            }
            | RunOutcome::Succeeded
            | RunOutcome::Failed { .. } => 1,
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
        ensure_absent(&parent, name).map_err(|kind| {
            LocalPublicationError::new(LocalPublicationPhase::TargetValidation, kind)
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

    fn verify_parent_and_absence(&self) -> Result<(), LocalPublicationError> {
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
        ensure_absent(&self.parent, &self.name)
            .map_err(|kind| LocalPublicationError::new(LocalPublicationPhase::Commit, kind))
    }
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

    fn verify(&self) -> Result<(), Errno> {
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
        Ok(())
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

fn ensure_absent(parent: &OwnedFd, name: &OsStr) -> Result<(), LocalPublicationFailureKind> {
    match statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(_) => Err(LocalPublicationFailureKind::DestinationExists),
        Err(Errno::NOENT) => Ok(()),
        Err(_) => Err(LocalPublicationFailureKind::ParentUnavailable),
    }
}

#[cfg(test)]
mod tests;
