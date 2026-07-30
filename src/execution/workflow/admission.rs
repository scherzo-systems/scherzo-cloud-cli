use std::fmt;
use std::fs;
use std::num::NonZeroUsize;
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

#[derive(Clone, Debug)]
pub(crate) struct ExecutionContext {
    root: PathBuf,
    root_lifecycle: ExecutionRootLifecycle,
    maximum_parallel_steps: usize,
    cancellation: CancellationPolicy,
}

impl ExecutionContext {
    pub(crate) fn new(
        root: PathBuf,
        root_lifecycle: ExecutionRootLifecycle,
        maximum_parallel_steps: usize,
        cancellation: CancellationPolicy,
    ) -> Self {
        Self {
            root,
            root_lifecycle,
            maximum_parallel_steps,
            cancellation,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionLimits {
    maximum_parallel_steps: NonZeroUsize,
}

impl ExecutionLimits {
    pub(crate) fn maximum_parallel_steps(self) -> NonZeroUsize {
        self.maximum_parallel_steps
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AdmittedExecutionContext {
    root: PathBuf,
    root_lifecycle: ExecutionRootLifecycle,
    limits: ExecutionLimits,
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

    pub(crate) fn cancellation(&self) -> &CancellationPolicy {
        &self.cancellation
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AdmittedCommandWorkflow {
    workflow: Arc<ResolvedWorkflow>,
    imports: ResolvedImports,
    execution: AdmittedExecutionContext,
}

impl AdmittedCommandWorkflow {
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

pub(crate) fn admit_command_workflow(
    workflow: ResolvedWorkflow,
    imports: ResolvedImports,
    context: ExecutionContext,
) -> Result<AdmittedCommandWorkflow, AdmissionFailure> {
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
    Ok(AdmittedCommandWorkflow {
        workflow: Arc::new(workflow),
        imports,
        execution: AdmittedExecutionContext {
            root,
            root_lifecycle: context.root_lifecycle,
            limits: ExecutionLimits {
                maximum_parallel_steps,
            },
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
