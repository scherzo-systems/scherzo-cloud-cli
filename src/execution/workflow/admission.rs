use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use super::resolution::ResolvedWorkflow;
use super::validated::ValidatedStep;

pub(crate) const MAX_CANCELLATION_GRACE: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CancellationReason {
    UserRequest,
    RunnerShutdown,
}

#[derive(Clone, Debug)]
pub(crate) struct CancellationSource {
    reason: watch::Sender<Option<CancellationReason>>,
}

impl CancellationSource {
    pub(crate) fn new() -> Self {
        let (reason, _) = watch::channel(None);
        Self { reason }
    }

    pub(crate) fn request_cancellation(&self, reason: CancellationReason) -> bool {
        self.reason.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(reason);
            true
        })
    }

    pub(crate) fn cancellation_reason(&self) -> Option<CancellationReason> {
        *self.reason.borrow()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancellation_reason().is_some()
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<Option<CancellationReason>> {
        self.reason.subscribe()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedAttachment {
    media_type: Arc<str>,
    bytes: Arc<[u8]>,
}

impl ResolvedAttachment {
    pub(crate) fn new(media_type: Arc<str>, bytes: Arc<[u8]>) -> Self {
        Self { media_type, bytes }
    }

    pub(crate) fn media_type(&self) -> &str {
        &self.media_type
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
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

    fn without_reserved_variables(self) -> Self {
        Self {
            variables: Arc::new(
                self.variables
                    .iter()
                    .filter(|(name, _)| !is_reserved_environment_name(name))
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
            ),
        }
    }
}

fn is_reserved_environment_name(name: &OsStr) -> bool {
    name.as_encoded_bytes().starts_with(b"SCHERZO_")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CaptureLimits {
    maximum_files: usize,
    maximum_file_bytes: u64,
    maximum_total_bytes: u64,
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
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ExecutionContext {
    root: PathBuf,
    root_lifecycle: ExecutionRootLifecycle,
    maximum_parallel_steps: usize,
    capture_limits: CaptureLimits,
    maximum_step_log_bytes: u64,
    environment: EnvironmentSnapshot,
    cancellation: CancellationPolicy,
}

impl ExecutionContext {
    pub(crate) fn new(
        root: PathBuf,
        root_lifecycle: ExecutionRootLifecycle,
        maximum_parallel_steps: usize,
        capture_limits: CaptureLimits,
        maximum_step_log_bytes: u64,
        environment: EnvironmentSnapshot,
        cancellation: CancellationPolicy,
    ) -> Self {
        Self {
            root,
            root_lifecycle,
            maximum_parallel_steps,
            capture_limits,
            maximum_step_log_bytes,
            environment,
            cancellation,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionLimits {
    maximum_parallel_steps: NonZeroUsize,
    maximum_captured_files: NonZeroUsize,
    maximum_captured_file_bytes: NonZeroU64,
    maximum_total_captured_bytes: NonZeroU64,
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

    pub(crate) fn maximum_step_log_bytes(self) -> NonZeroU64 {
        self.maximum_step_log_bytes
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AdmittedExecutionContext {
    root: PathBuf,
    root_lifecycle: ExecutionRootLifecycle,
    limits: ExecutionLimits,
    environment: EnvironmentSnapshot,
    cancellation: CancellationPolicy,
}

impl AdmittedExecutionContext {
    pub(crate) fn root(&self) -> &Path {
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

#[derive(Clone, Debug)]
pub(crate) struct AdmittedWorkflow {
    workflow: Arc<ResolvedWorkflow>,
    imports: ResolvedImports,
    execution: AdmittedExecutionContext,
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionFailureKind {
    MissingRequiredPrompt,
    InvalidAttachmentMediaType,
    AgentStepRuntimeUnsupported,
    ExecutionRootUnavailable,
    ExecutionRootNotDirectory,
    NonPositiveParallelism,
    NonPositiveCapturedFiles,
    NonPositiveCapturedFileBytes,
    NonPositiveTotalCapturedBytes,
    NonPositiveStepLogBytes,
    NonPositiveCancellationGrace,
    CancellationGraceTooLong,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionLocation {
    PromptImport,
    AttachmentImport { index: usize },
    Step { step: String },
    ExecutionRoot,
    MaximumParallelSteps,
    MaximumCapturedFiles,
    MaximumCapturedFileBytes,
    MaximumTotalCapturedBytes,
    MaximumStepLogBytes,
    CancellationPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionFailure {
    kind: AdmissionFailureKind,
    location: AdmissionLocation,
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

pub(crate) fn admit_workflow(
    workflow: ResolvedWorkflow,
    imports: ResolvedImports,
    context: ExecutionContext,
) -> Result<AdmittedWorkflow, AdmissionFailure> {
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

    if let Some((step, _)) = workflow
        .definition
        .steps
        .iter()
        .find(|(_, step)| matches!(step, ValidatedStep::Agent(_)))
    {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::AgentStepRuntimeUnsupported,
            AdmissionLocation::Step { step: step.clone() },
        ));
    }

    let maximum_parallel_steps =
        NonZeroUsize::new(context.maximum_parallel_steps).ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveParallelism,
                AdmissionLocation::MaximumParallelSteps,
            )
        })?;
    let maximum_captured_files = NonZeroUsize::new(context.capture_limits.maximum_files)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveCapturedFiles,
                AdmissionLocation::MaximumCapturedFiles,
            )
        })?;
    let maximum_captured_file_bytes = NonZeroU64::new(context.capture_limits.maximum_file_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveCapturedFileBytes,
                AdmissionLocation::MaximumCapturedFileBytes,
            )
        })?;
    let maximum_total_captured_bytes = NonZeroU64::new(context.capture_limits.maximum_total_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveTotalCapturedBytes,
                AdmissionLocation::MaximumTotalCapturedBytes,
            )
        })?;
    let maximum_step_log_bytes =
        NonZeroU64::new(context.maximum_step_log_bytes).ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveStepLogBytes,
                AdmissionLocation::MaximumStepLogBytes,
            )
        })?;
    if context.cancellation.grace().is_zero() {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::NonPositiveCancellationGrace,
            AdmissionLocation::CancellationPolicy,
        ));
    }
    if context.cancellation.grace() > MAX_CANCELLATION_GRACE {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::CancellationGraceTooLong,
            AdmissionLocation::CancellationPolicy,
        ));
    }

    let root = canonical_execution_root(&context.root)?;
    Ok(AdmittedWorkflow {
        workflow: Arc::new(workflow),
        imports,
        execution: AdmittedExecutionContext {
            root,
            root_lifecycle: context.root_lifecycle,
            limits: ExecutionLimits {
                maximum_parallel_steps,
                maximum_captured_files,
                maximum_captured_file_bytes,
                maximum_total_captured_bytes,
                maximum_step_log_bytes,
            },
            environment: context.environment.without_reserved_variables(),
            cancellation: context.cancellation,
        },
    })
}

fn canonical_execution_root(root: &Path) -> Result<PathBuf, AdmissionFailure> {
    let canonical = fs::canonicalize(root).map_err(|_| {
        AdmissionFailure::new(
            AdmissionFailureKind::ExecutionRootUnavailable,
            AdmissionLocation::ExecutionRoot,
        )
    })?;
    let metadata = fs::metadata(&canonical).map_err(|_| {
        AdmissionFailure::new(
            AdmissionFailureKind::ExecutionRootUnavailable,
            AdmissionLocation::ExecutionRoot,
        )
    })?;
    if !metadata.is_dir() {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::ExecutionRootNotDirectory,
            AdmissionLocation::ExecutionRoot,
        ));
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests;
