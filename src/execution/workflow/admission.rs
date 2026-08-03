use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
#[cfg(test)]
use std::future::{Future as _, poll_fn};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Barrier;
use std::time::Duration;

use tokio::sync::watch;

use super::resolution::ResolvedWorkflow;
use super::validated::ValidatedStep;

pub(crate) const MAX_CANCELLATION_GRACE: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CancellationReason {
    UserRequest,
    TerminationRequest,
    CallerOutputFailure,
    RunnerShutdown,
}

#[cfg(test)]
#[derive(Clone)]
pub(super) struct CancellationPendingPollBarrier {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

#[cfg(test)]
impl CancellationPendingPollBarrier {
    pub(super) fn new() -> Self {
        Self {
            reached: Arc::new(Barrier::new(2)),
            resume: Arc::new(Barrier::new(2)),
        }
    }

    pub(super) fn wait_until_pending(&self) {
        self.reached.wait();
    }

    pub(super) fn resume(&self) {
        self.resume.wait();
    }

    fn block_until_resumed(&self) {
        self.reached.wait();
        self.resume.wait();
    }
}

#[cfg(test)]
impl fmt::Debug for CancellationPendingPollBarrier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationPendingPollBarrier")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CancellationSource {
    reason: watch::Sender<Option<CancellationReason>>,
    #[cfg(test)]
    pending_poll_barrier: Option<CancellationPendingPollBarrier>,
}

impl CancellationSource {
    pub(crate) fn new() -> Self {
        let (reason, _) = watch::channel(None);
        Self {
            reason,
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

    pub(super) fn subscribe(&self) -> CancellationSubscription {
        CancellationSubscription {
            receiver: self.reason.subscribe(),
            #[cfg(test)]
            pending_poll_barrier: self.pending_poll_barrier.clone(),
        }
    }
}

pub(super) struct CancellationSubscription {
    receiver: watch::Receiver<Option<CancellationReason>>,
    #[cfg(test)]
    pending_poll_barrier: Option<CancellationPendingPollBarrier>,
}

impl CancellationSubscription {
    pub(super) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        #[cfg(not(test))]
        {
            self.receiver.changed().await
        }
        #[cfg(test)]
        {
            let mut pending_poll_barrier = self.pending_poll_barrier.take();
            let changed = self.receiver.changed();
            tokio::pin!(changed);
            poll_fn(|context| {
                let result = changed.as_mut().poll(context);
                if result.is_pending()
                    && let Some(barrier) = pending_poll_barrier.take()
                {
                    barrier.block_until_resumed();
                }
                result
            })
            .await
        }
    }

    pub(super) fn borrow_and_update(&mut self) -> watch::Ref<'_, Option<CancellationReason>> {
        self.receiver.borrow_and_update()
    }

    #[cfg(test)]
    pub(super) fn has_changed(&self) -> Result<bool, watch::error::RecvError> {
        self.receiver.has_changed()
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

    pub(super) fn with_variable(&self, name: OsString, value: OsString) -> Self {
        let mut variables = self.variables.as_ref().clone();
        variables.insert(name, value);
        Self {
            variables: Arc::new(variables),
        }
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

#[derive(Clone, Debug)]
pub(crate) struct ExecutionContext {
    root: PathBuf,
    root_lifecycle: ExecutionRootLifecycle,
    limits: ExecutionPolicyLimits,
    environment: EnvironmentSnapshot,
    cancellation: CancellationPolicy,
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
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionLimits {
    maximum_parallel_steps: NonZeroUsize,
    maximum_captured_files: NonZeroUsize,
    maximum_captured_file_bytes: NonZeroU64,
    maximum_total_captured_bytes: NonZeroU64,
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
    NonPositiveInputValues,
    NonPositiveInputValueBytes,
    NonPositiveTotalInputBytes,
    NonPositiveLiveInputBytes,
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
    MaximumInputValues,
    MaximumInputValueBytes,
    MaximumTotalInputBytes,
    MaximumLiveInputBytes,
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
    let maximum_step_log_bytes = NonZeroU64::new(context.limits.maximum_step_log_bytes)
        .ok_or_else(|| {
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
                maximum_input_values,
                maximum_input_value_bytes,
                maximum_total_input_bytes,
                maximum_live_input_bytes,
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
