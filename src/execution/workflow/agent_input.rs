use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use serde_json::Value;

use rustix::fs::{AtFlags, Mode, OFlags, fchmod, mkdirat, openat, unlinkat};
use rustix::io::Errno;

use super::admission::{
    AdmittedExecutionContext, AdmittedHarness, AdmittedWorkflow, CancellationReason,
    CancellationSource,
};
use super::agent::{
    AdmittedAgentAdapter, AgentCompatibilityProfile, AgentInvocation, AgentInvocationIdentity,
    AgentInvocationStaging, AgentObservationSink, AgentProcessContext, AgentPrompt, AgentValueMode,
    MAXIMUM_INLINE_AGENT_INPUT_BYTES, StagedAgentAttachment,
};
use super::agent_diagnostics::AgentDiagnosticSessionStore;
use super::artifact::{ArtifactReadFailure, ArtifactStaging, CapturedArtifact};
use super::canonical_json;
use super::claude_code::ClaudeCodeConfig;
use super::claude_code_stream_json_v1::ClaudeCodeStreamJsonV1ProtocolLimits;
use super::document::Output;
use super::execution_root::{AdmittedExecutionRoot, open_directory};
use super::pi::PiConfig;
use super::pi_json_v1::PiJsonV1ProtocolLimits;
#[cfg(test)]
use super::private_staging::CleanupBlocker;
use super::private_staging::{
    StagingLifecycle, cleanup_staging, create_staging_root, finish_payload_file,
    mark_cleanup_failed, remove_open_tree_at, remove_staging_root,
};
use super::process_group::ProcessGuardRegistry;
use super::step_runtime::{WorkingDirectoryFailure, resolve_working_directory};
use super::validated::{
    ResolvedOutputSource, ResolvedValueSource, ValidatedAgentStep, ValidatedMessageSource,
    ValidatedStep, WorkflowImport, WorkflowValueType,
};
use super::value::CapturedValue;

const IDENTITY_ATTEMPTS: usize = 16;
const COPY_BUFFER_BYTES: usize = 64 * 1024;
const ATTACHMENT_DIRECTORY: &str = "attachments";
const MESSAGE_FILE: &str = "message.md";
const RESULT_ENDPOINT_DIRECTORY: &str = "result-endpoint";
const STATIC_ATTACHMENT_MEDIA_TYPE: &str = "application/octet-stream";

type PiJsonV1Invocation<Sink> = AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>;
type ClaudeCodeStreamJsonV1Invocation<Sink> =
    AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>;

pub(crate) enum ClosedAgentInvocation<Sink>
where
    Sink: AgentObservationSink,
{
    Pi(PiJsonV1Invocation<Sink>),
    ClaudeCode(ClaudeCodeStreamJsonV1Invocation<Sink>),
}

impl<Sink> ClosedAgentInvocation<Sink>
where
    Sink: AgentObservationSink,
{
    pub(crate) fn profile(&self) -> AgentCompatibilityProfile {
        match self {
            Self::Pi(invocation) => invocation.adapter().profile(),
            Self::ClaudeCode(invocation) => invocation.adapter().profile(),
        }
    }

    pub(crate) fn identity(&self) -> &AgentInvocationIdentity {
        match self {
            Self::Pi(invocation) => invocation.identity(),
            Self::ClaudeCode(invocation) => invocation.identity(),
        }
    }

    pub(crate) fn process(&self) -> &AgentProcessContext {
        match self {
            Self::Pi(invocation) => invocation.process(),
            Self::ClaudeCode(invocation) => invocation.process(),
        }
    }

    pub(crate) fn staging(&self) -> &AgentInvocationStaging {
        match self {
            Self::Pi(invocation) => invocation.staging(),
            Self::ClaudeCode(invocation) => invocation.staging(),
        }
    }

    pub(crate) fn diagnostic_session(&self) -> &super::agent_diagnostics::AgentDiagnosticSession {
        match self {
            Self::Pi(invocation) => invocation.diagnostic_session(),
            Self::ClaudeCode(invocation) => invocation.diagnostic_session(),
        }
    }

    pub(crate) fn prompt(&self) -> &AgentPrompt {
        match self {
            Self::Pi(invocation) => invocation.prompt(),
            Self::ClaudeCode(invocation) => invocation.prompt(),
        }
    }

    pub(crate) fn attachments(&self) -> &[StagedAgentAttachment] {
        match self {
            Self::Pi(invocation) => invocation.attachments(),
            Self::ClaudeCode(invocation) => invocation.attachments(),
        }
    }

    pub(crate) fn value_mode(&self) -> &AgentValueMode {
        match self {
            Self::Pi(invocation) => invocation.value_mode(),
            Self::ClaudeCode(invocation) => invocation.value_mode(),
        }
    }

    pub(crate) fn cancellation(&self) -> &CancellationSource {
        match self {
            Self::Pi(invocation) => invocation.cancellation(),
            Self::ClaudeCode(invocation) => invocation.cancellation(),
        }
    }

    pub(crate) fn observations(&self) -> &super::agent::OrderedAgentObservationSink<Sink> {
        match self {
            Self::Pi(invocation) => invocation.observations(),
            Self::ClaudeCode(invocation) => invocation.observations(),
        }
    }

    pub(crate) fn process_control(&self) -> &super::agent::AgentProcessControl {
        match self {
            Self::Pi(invocation) => invocation.process_control(),
            Self::ClaudeCode(invocation) => invocation.process_control(),
        }
    }

    pub(crate) fn maximum_response_bytes(&self) -> std::num::NonZeroU64 {
        match self {
            Self::Pi(invocation) => invocation.limits().maximum_response_bytes(),
            Self::ClaudeCode(invocation) => invocation.limits().maximum_response_bytes(),
        }
    }

    pub(crate) fn maximum_result_bytes(&self) -> std::num::NonZeroU64 {
        match self {
            Self::Pi(invocation) => invocation.limits().maximum_result_bytes(),
            Self::ClaudeCode(invocation) => invocation.limits().maximum_result_bytes(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentInputStagingFailure {
    ExecutionRootUnavailable,
    StagingParentUnavailable,
    StagingParentExposed,
    IdentityUnavailable,
}

impl fmt::Display for AgentInputStagingFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "agent input staging failure: {self:?}")
    }
}

impl std::error::Error for AgentInputStagingFailure {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentInputStagingReleaseFailure {
    CleanupUnavailable,
}

impl fmt::Display for AgentInputStagingReleaseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "agent input staging release failure: {self:?}")
    }
}

impl std::error::Error for AgentInputStagingReleaseFailure {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AgentInputStartFailure {
    StepUnavailable,
    AgentAdmissionUnavailable,
    InputsUnavailable,
    MissingUpstreamValue { source: ResolvedOutputSource },
    ValueTypeMismatch { source: ResolvedOutputSource },
    RetainedSourceUnavailable { path: String },
    InvalidRetainedText { path: String },
    ResultSchemaUnavailable { output: String },
    InvalidValueMode,
    AttachmentCountLimitExceeded { maximum: usize },
    AttachmentBytesLimitExceeded { maximum: u64 },
    WorkingDirectory(WorkingDirectoryFailure),
    ArtifactStagingMismatch,
    AgentStagingMismatch,
    StagingUnavailable,
}

impl fmt::Display for AgentInputStartFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "agent input start failure: {self:?}")
    }
}

impl std::error::Error for AgentInputStartFailure {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AgentInputMaterializationError {
    Start(AgentInputStartFailure),
    Cancelled { reason: CancellationReason },
}

impl fmt::Display for AgentInputMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start(failure) => failure.fmt(formatter),
            Self::Cancelled { reason } => {
                write!(
                    formatter,
                    "agent input materialization cancelled: {reason:?}"
                )
            }
        }
    }
}

impl std::error::Error for AgentInputMaterializationError {}

#[derive(Clone)]
pub(crate) struct AgentInputStaging {
    inner: Arc<AgentInputStagingInner>,
}

struct AgentInputStagingInner {
    execution_root: AdmittedExecutionRoot,
    staging_parent: OwnedFd,
    staging_root: OwnedFd,
    staging_path: PathBuf,
    staging_identity: Arc<str>,
    lifecycle: RwLock<StagingLifecycle>,
    active_views: Mutex<BTreeSet<Arc<str>>>,
    #[cfg(test)]
    observer: Option<Arc<dyn AgentMaterializationBoundaryObserver>>,
    #[cfg(test)]
    cleanup_blocker: CleanupBlocker,
}

pub(crate) struct AgentInputStagingLease {
    inner: Arc<AgentInputStagingInner>,
    identity: Arc<str>,
    directory: OwnedFd,
    attachment_directory: OwnedFd,
    _result_endpoint_directory: OwnedFd,
    path: PathBuf,
    attachment_path: PathBuf,
    result_endpoint_path: PathBuf,
    released: bool,
}

impl AgentInputStagingLease {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    fn attachment_path(&self) -> &Path {
        &self.attachment_path
    }

    fn result_endpoint_path(&self) -> &Path {
        &self.result_endpoint_path
    }

    pub(crate) fn release(mut self) -> Result<(), AgentInputStagingReleaseFailure> {
        self.release_inner()
    }

    fn release_inner(&mut self) -> Result<(), AgentInputStagingReleaseFailure> {
        if self.released {
            return Ok(());
        }
        if !self.inner.remove_view(&self.identity, &self.directory) {
            return Err(AgentInputStagingReleaseFailure::CleanupUnavailable);
        }
        self.released = true;
        Ok(())
    }
}

impl Drop for AgentInputStagingLease {
    fn drop(&mut self) {
        let _ = self.release_inner();
    }
}

pub(crate) struct MaterializedAgentInvocation<Sink>
where
    Sink: AgentObservationSink,
{
    invocation: ClosedAgentInvocation<Sink>,
    staging: AgentInputStagingLease,
}

impl<Sink> MaterializedAgentInvocation<Sink>
where
    Sink: AgentObservationSink,
{
    pub(crate) fn invocation(&self) -> &ClosedAgentInvocation<Sink> {
        &self.invocation
    }

    pub(crate) fn staging_path(&self) -> &Path {
        self.staging.path()
    }

    pub(crate) fn into_parts(self) -> (ClosedAgentInvocation<Sink>, AgentInputStagingLease) {
        (self.invocation, self.staging)
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentMaterializationBoundary {
    BeforeAttachment { index: usize },
    Ready,
}

#[cfg(test)]
pub(crate) trait AgentMaterializationBoundaryObserver: Send + Sync {
    fn reached(&self, boundary: AgentMaterializationBoundary);
}

impl AgentInputStaging {
    pub(crate) fn create(
        execution: &AdmittedExecutionContext,
        staging_parent: &Path,
    ) -> Result<Self, AgentInputStagingFailure> {
        Self::create_with_observer(execution, staging_parent, None)
    }

    fn create_with_observer(
        execution: &AdmittedExecutionContext,
        staging_parent: &Path,
        #[cfg(test)] observer: Option<Arc<dyn AgentMaterializationBoundaryObserver>>,
        #[cfg(not(test))] _observer: Option<()>,
    ) -> Result<Self, AgentInputStagingFailure> {
        if !execution.root_identity().pathname_is_bound() {
            return Err(AgentInputStagingFailure::ExecutionRootUnavailable);
        }
        let canonical_staging_parent = std::fs::canonicalize(staging_parent)
            .map_err(|_| AgentInputStagingFailure::StagingParentUnavailable)?;
        if canonical_staging_parent.starts_with(execution.root()) {
            return Err(AgentInputStagingFailure::StagingParentExposed);
        }
        let staging_parent = open_directory(&canonical_staging_parent)
            .map_err(|_| AgentInputStagingFailure::StagingParentUnavailable)?;
        let (staging_identity, staging_root) =
            create_staging_root(&staging_parent, ".agent-inputs", IDENTITY_ATTEMPTS)
                .map_err(|()| AgentInputStagingFailure::IdentityUnavailable)?;
        let staging_path = canonical_staging_parent.join(staging_identity.as_ref());
        Ok(Self {
            inner: Arc::new(AgentInputStagingInner {
                execution_root: execution.root_identity().clone(),
                staging_parent,
                staging_root,
                staging_path,
                staging_identity,
                lifecycle: RwLock::new(StagingLifecycle::Active),
                active_views: Mutex::new(BTreeSet::new()),
                #[cfg(test)]
                observer,
                #[cfg(test)]
                cleanup_blocker: CleanupBlocker::default(),
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_observer(
        execution: &AdmittedExecutionContext,
        staging_parent: &Path,
        observer: Arc<dyn AgentMaterializationBoundaryObserver>,
    ) -> Result<Self, AgentInputStagingFailure> {
        Self::create_with_observer(execution, staging_parent, Some(observer))
    }

    pub(crate) fn is_bound_to(&self, execution: &AdmittedExecutionContext) -> bool {
        execution.root_identity().pathname_is_bound()
            && self
                .inner
                .execution_root
                .is_same_directory(execution.root_identity())
    }

    pub(crate) fn release(&self) -> Result<(), AgentInputStagingReleaseFailure> {
        self.inner.cleanup()
    }

    fn reserve_view(&self) -> Result<AgentInputStagingLease, AgentInputMaterializationError> {
        for _ in 0..IDENTITY_ATTEMPTS {
            let identity = Arc::<str>::from(format!(
                "invocation-{}",
                ulid::Ulid::generate().to_string().to_ascii_lowercase()
            ));
            match mkdirat(&self.inner.staging_root, identity.as_ref(), Mode::RWXU) {
                Ok(()) => {
                    let directory = match openat(
                        &self.inner.staging_root,
                        identity.as_ref(),
                        directory_open_flags(),
                        Mode::empty(),
                    ) {
                        Ok(directory) => directory,
                        Err(_) => {
                            let _ = unlinkat(
                                &self.inner.staging_root,
                                identity.as_ref(),
                                AtFlags::REMOVEDIR,
                            );
                            return Err(staging_error());
                        }
                    };
                    let (attachment_directory, result_endpoint_directory) =
                        match create_view_directories(&directory) {
                            Ok(directories) => directories,
                            Err(error) => {
                                let _ = remove_open_tree_at(
                                    &self.inner.staging_root,
                                    identity.as_ref(),
                                    &directory,
                                );
                                return Err(error);
                            }
                        };
                    lock_views(&self.inner.active_views).insert(Arc::clone(&identity));
                    let path = self.inner.staging_path.join(identity.as_ref());
                    let attachment_path = path.join(ATTACHMENT_DIRECTORY);
                    let result_endpoint_path = path.join(RESULT_ENDPOINT_DIRECTORY);
                    return Ok(AgentInputStagingLease {
                        inner: Arc::clone(&self.inner),
                        identity,
                        directory,
                        attachment_directory,
                        _result_endpoint_directory: result_endpoint_directory,
                        path,
                        attachment_path,
                        result_endpoint_path,
                        released: false,
                    });
                }
                Err(Errno::EXIST) => {}
                Err(_) => return Err(staging_error()),
            }
        }
        Err(staging_error())
    }

    fn boundary(
        &self,
        boundary: AgentMaterializationBoundaryInternal,
        cancellation: &CancellationSource,
    ) -> Result<(), AgentInputMaterializationError> {
        #[cfg(not(test))]
        let _ = boundary;
        #[cfg(test)]
        if let Some(observer) = &self.inner.observer {
            observer.reached(match boundary {
                AgentMaterializationBoundaryInternal::BeforeAttachment { index } => {
                    AgentMaterializationBoundary::BeforeAttachment { index }
                }
                AgentMaterializationBoundaryInternal::Ready => AgentMaterializationBoundary::Ready,
            });
        }
        check_cancellation(cancellation)
    }

    #[cfg(test)]
    pub(crate) fn active_view_count(&self) -> usize {
        lock_views(&self.inner.active_views).len()
    }

    #[cfg(test)]
    pub(super) fn cleanup_blocker(&self) -> &CleanupBlocker {
        &self.inner.cleanup_blocker
    }
}

impl AgentInputStagingInner {
    fn remove_view(&self, identity: &str, directory: &OwnedFd) -> bool {
        let Ok(lifecycle) = self.lifecycle.read() else {
            return false;
        };
        if *lifecycle == StagingLifecycle::Released {
            return true;
        }
        #[cfg(test)]
        if self.cleanup_blocker.is_blocked() {
            drop(lifecycle);
            mark_cleanup_failed(&self.lifecycle);
            return false;
        }
        let removed = remove_open_tree_at(&self.staging_root, identity, directory).is_ok();
        drop(lifecycle);
        if removed {
            lock_views(&self.active_views).remove(identity);
        } else {
            mark_cleanup_failed(&self.lifecycle);
        }
        removed
    }

    fn cleanup(&self) -> Result<(), AgentInputStagingReleaseFailure> {
        cleanup_staging(
            &self.lifecycle,
            AgentInputStagingReleaseFailure::CleanupUnavailable,
            || self.cleanup_active(),
        )
    }

    fn cleanup_active(&self) -> Result<(), AgentInputStagingReleaseFailure> {
        #[cfg(test)]
        if self.cleanup_blocker.is_blocked() {
            return Err(AgentInputStagingReleaseFailure::CleanupUnavailable);
        }
        remove_staging_root(
            &self.staging_parent,
            self.staging_identity.as_ref(),
            &self.staging_root,
        )
        .map_err(|_| AgentInputStagingReleaseFailure::CleanupUnavailable)?;
        lock_views(&self.active_views).clear();
        Ok(())
    }
}

impl Drop for AgentInputStagingInner {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

#[derive(Clone, Copy)]
enum AgentMaterializationBoundaryInternal {
    BeforeAttachment { index: usize },
    Ready,
}

enum PlannedAttachment<'a> {
    Bytes(&'a [u8]),
    Json(&'a Value),
    Artifact(&'a CapturedArtifact),
}

struct PlannedAgentAttachment<'a> {
    payload: PlannedAttachment<'a>,
    media_type: Arc<str>,
    diagnostic_source_name: Option<Arc<str>>,
}

struct AgentMaterializationPlan<'a> {
    prompt: AgentPrompt,
    attachments: Vec<PlannedAgentAttachment<'a>>,
    value_mode: AgentValueMode,
}

struct AttachmentBudget {
    count: usize,
    bytes: u64,
    maximum_count: usize,
    maximum_bytes: u64,
}

impl AttachmentBudget {
    fn new(harness: &AdmittedHarness) -> Self {
        Self {
            count: 0,
            bytes: 0,
            maximum_count: harness.maximum_attachments().get(),
            maximum_bytes: harness.maximum_attachment_bytes().get(),
        }
    }

    fn push<'a>(
        &mut self,
        attachment: PlannedAgentAttachment<'a>,
        attachments: &mut Vec<PlannedAgentAttachment<'a>>,
    ) -> Result<(), AgentInputMaterializationError> {
        let count = self
            .count
            .checked_add(1)
            .filter(|count| *count <= self.maximum_count)
            .ok_or_else(|| {
                start_error(AgentInputStartFailure::AttachmentCountLimitExceeded {
                    maximum: self.maximum_count,
                })
            })?;
        let remaining = self.maximum_bytes.saturating_sub(self.bytes);
        let attachment_bytes =
            planned_attachment_size(&attachment.payload, remaining, self.maximum_bytes)?;
        let bytes = self
            .bytes
            .checked_add(attachment_bytes)
            .filter(|bytes| *bytes <= self.maximum_bytes)
            .ok_or_else(|| {
                start_error(AgentInputStartFailure::AttachmentBytesLimitExceeded {
                    maximum: self.maximum_bytes,
                })
            })?;
        attachments
            .try_reserve(1)
            .map_err(|_| start_error(AgentInputStartFailure::InputsUnavailable))?;
        attachments.push(attachment);
        self.count = count;
        self.bytes = bytes;
        Ok(())
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "materialization makes every launch-owned dependency explicit"
)]
pub(crate) fn materialize_agent_invocation<Sink>(
    admitted: &AdmittedWorkflow,
    artifacts: &ArtifactStaging,
    staging: &AgentInputStaging,
    diagnostic_sessions: &AgentDiagnosticSessionStore,
    identity: AgentInvocationIdentity,
    upstream_outputs: &BTreeMap<ResolvedOutputSource, CapturedValue>,
    cancellation: CancellationSource,
    process_guards: ProcessGuardRegistry,
    observation_sink: Sink,
) -> Result<MaterializedAgentInvocation<Sink>, AgentInputMaterializationError>
where
    Sink: AgentObservationSink,
{
    check_cancellation(&cancellation)?;
    let step_name = identity.step();
    let definition = admitted
        .workflow()
        .definition
        .steps
        .get(step_name)
        .ok_or_else(|| start_error(AgentInputStartFailure::StepUnavailable))?;
    let ValidatedStep::Agent(step) = definition else {
        return Err(start_error(AgentInputStartFailure::StepUnavailable));
    };
    let admitted_step = admitted
        .agent_step(step_name)
        .ok_or_else(|| start_error(AgentInputStartFailure::AgentAdmissionUnavailable))?;
    if !artifacts.is_bound_to(admitted.execution()) {
        return Err(start_error(AgentInputStartFailure::ArtifactStagingMismatch));
    }
    if !staging.is_bound_to(admitted.execution()) {
        return Err(start_error(AgentInputStartFailure::AgentStagingMismatch));
    }

    let working_directory = resolve_working_directory(
        admitted.execution().root_identity(),
        step.common.cwd.as_deref(),
    )
    .map_err(|failure| start_error(AgentInputStartFailure::WorkingDirectory(failure)))?;
    let plan = build_plan(admitted, step_name, step, upstream_outputs, admitted_step)?;
    check_cancellation(&cancellation)?;
    let lifecycle = staging
        .inner
        .lifecycle
        .read()
        .map_err(|_| staging_error())?;
    if *lifecycle != StagingLifecycle::Active {
        return Err(staging_error());
    }
    let view = staging.reserve_view()?;
    let staged_attachments =
        match stage_attachments(staging, artifacts, &view, &plan.attachments, &cancellation) {
            Ok(attachments) => attachments,
            Err(error) => {
                drop(lifecycle);
                return Err(abort_view(view, error));
            }
        };
    let message_file = if plan.prompt.message().len() > MAXIMUM_INLINE_AGENT_INPUT_BYTES {
        match stage_message(&view, plan.prompt.message(), &cancellation) {
            Ok(path) => Some(path),
            Err(error) => {
                drop(lifecycle);
                return Err(abort_view(view, error));
            }
        }
    } else {
        None
    };
    if fchmod(&view.attachment_directory, Mode::RUSR | Mode::XUSR).is_err() {
        drop(lifecycle);
        return Err(abort_view(view, staging_error()));
    }
    if let Err(error) = staging.boundary(AgentMaterializationBoundaryInternal::Ready, &cancellation)
    {
        drop(lifecycle);
        return Err(abort_view(view, error));
    }
    if !working_directory.validate_execution_root() {
        drop(lifecycle);
        return Err(abort_view(
            view,
            start_error(AgentInputStartFailure::WorkingDirectory(
                WorkingDirectoryFailure::ExecutionRootRebound,
            )),
        ));
    }

    let mut invocation_staging =
        AgentInvocationStaging::new(view.result_endpoint_path().to_owned());
    if let Some(message_file) = message_file {
        invocation_staging = invocation_staging.with_message_file(message_file);
    }
    let harness_version = match admitted_step {
        AdmittedHarness::Pi(admission) => admission.installation().version().as_str(),
        AdmittedHarness::ClaudeCode(admission) => admission.installation().version().as_str(),
    };
    let diagnostic_session =
        match diagnostic_sessions.allocate(&identity, admitted_step.profile(), harness_version) {
            Ok(session) => session,
            Err(_) => {
                drop(lifecycle);
                return Err(abort_view(
                    view,
                    start_error(AgentInputStartFailure::StagingUnavailable),
                ));
            }
        };
    let invocation = match admitted_step {
        AdmittedHarness::Pi(admission) => ClosedAgentInvocation::Pi(AgentInvocation::new(
            identity,
            AdmittedAgentAdapter::new(
                AgentCompatibilityProfile::PiJsonV1,
                admission.installation().executable().to_owned(),
                Arc::from(admission.installation().version().as_str()),
                admission.configuration().clone(),
            ),
            AgentProcessContext::new(
                working_directory,
                admitted.execution().environment().clone(),
            ),
            invocation_staging,
            diagnostic_session,
            plan.prompt,
            Arc::from(staged_attachments),
            plan.value_mode,
            admission.limits().clone(),
            cancellation,
            process_guards,
            observation_sink,
        )),
        AdmittedHarness::ClaudeCode(admission) => {
            ClosedAgentInvocation::ClaudeCode(AgentInvocation::new(
                identity,
                AdmittedAgentAdapter::new(
                    AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
                    admission.installation().executable().to_owned(),
                    Arc::from(admission.installation().version().as_str()),
                    admission.configuration().clone(),
                ),
                AgentProcessContext::new(
                    working_directory,
                    admitted.execution().environment().clone(),
                ),
                invocation_staging,
                diagnostic_session,
                plan.prompt,
                Arc::from(staged_attachments),
                plan.value_mode,
                admission.limits().clone(),
                cancellation,
                process_guards,
                observation_sink,
            ))
        }
    };
    drop(lifecycle);
    Ok(MaterializedAgentInvocation {
        invocation,
        staging: view,
    })
}

fn build_plan<'a>(
    admitted: &'a AdmittedWorkflow,
    step_name: &str,
    step: &'a ValidatedAgentStep,
    upstream_outputs: &'a BTreeMap<ResolvedOutputSource, CapturedValue>,
    harness: &AdmittedHarness,
) -> Result<AgentMaterializationPlan<'a>, AgentInputMaterializationError> {
    let system_prompt = retained_text(admitted, &step.agent.system_prompt)?;
    let mut message = String::new();
    for (index, source) in step.agent.message.text.iter().enumerate() {
        let part = resolve_text(admitted, source, upstream_outputs)?;
        let separator_bytes = usize::from(index > 0) * 2;
        message
            .try_reserve(separator_bytes.saturating_add(part.len()))
            .map_err(|_| start_error(AgentInputStartFailure::InputsUnavailable))?;
        if index > 0 {
            message.push_str("\n\n");
        }
        message.push_str(part);
    }

    let mut attachments = Vec::new();
    let mut attachment_budget = AttachmentBudget::new(harness);
    for source in &step.agent.message.attachments {
        resolve_attachments(
            admitted,
            source,
            upstream_outputs,
            &mut attachment_budget,
            &mut attachments,
        )?;
    }

    Ok(AgentMaterializationPlan {
        prompt: AgentPrompt::new(Arc::from(system_prompt), Arc::from(message)),
        attachments,
        value_mode: resolve_value_mode(admitted, step_name, step)?,
    })
}

fn resolve_text<'a>(
    admitted: &'a AdmittedWorkflow,
    source: &'a ValidatedMessageSource,
    upstream_outputs: &'a BTreeMap<ResolvedOutputSource, CapturedValue>,
) -> Result<&'a str, AgentInputMaterializationError> {
    match source {
        ValidatedMessageSource::File { path } => retained_text(admitted, path),
        ValidatedMessageSource::Reference {
            source: ResolvedValueSource::Import(WorkflowImport::Prompt),
            value_type: WorkflowValueType::Text,
        } => admitted
            .imports()
            .prompt()
            .ok_or_else(|| start_error(AgentInputStartFailure::InputsUnavailable)),
        ValidatedMessageSource::Reference {
            source: ResolvedValueSource::Output(source),
            value_type: WorkflowValueType::Text,
        } => match upstream_value(upstream_outputs, source)? {
            CapturedValue::Text(value) => Ok(value),
            CapturedValue::Json(_) | CapturedValue::File(_) | CapturedValue::GitBranch(_) => {
                Err(start_error(AgentInputStartFailure::ValueTypeMismatch {
                    source: source.clone(),
                }))
            }
        },
        ValidatedMessageSource::Reference { .. } => {
            Err(start_error(AgentInputStartFailure::InputsUnavailable))
        }
    }
}

fn resolve_attachments<'a>(
    admitted: &'a AdmittedWorkflow,
    source: &'a ValidatedMessageSource,
    upstream_outputs: &'a BTreeMap<ResolvedOutputSource, CapturedValue>,
    budget: &mut AttachmentBudget,
    attachments: &mut Vec<PlannedAgentAttachment<'a>>,
) -> Result<(), AgentInputMaterializationError> {
    match source {
        ValidatedMessageSource::File { path } => {
            let bytes = retained_bytes(admitted, path)?;
            budget.push(
                PlannedAgentAttachment {
                    payload: PlannedAttachment::Bytes(bytes),
                    media_type: Arc::from(STATIC_ATTACHMENT_MEDIA_TYPE),
                    diagnostic_source_name: Some(Arc::from(path.as_str())),
                },
                attachments,
            )?;
        }
        ValidatedMessageSource::Reference {
            source: ResolvedValueSource::Import(WorkflowImport::Attachments),
            value_type: WorkflowValueType::AttachmentCollection,
        } => {
            for attachment in admitted.imports().attachments() {
                budget.push(
                    PlannedAgentAttachment {
                        payload: PlannedAttachment::Bytes(attachment.bytes()),
                        media_type: Arc::from(attachment.media_type()),
                        diagnostic_source_name: attachment.diagnostic_source_name().map(Arc::from),
                    },
                    attachments,
                )?;
            }
        }
        ValidatedMessageSource::Reference {
            source: ResolvedValueSource::Output(source),
            value_type,
        } => {
            let value = upstream_value(upstream_outputs, source)?;
            let diagnostic_source_name = Some(Arc::from(format!(
                "outputs.{}.{}",
                source.step, source.output
            )));
            let attachment = match (value_type, value) {
                (WorkflowValueType::Json, CapturedValue::Json(value)) => PlannedAgentAttachment {
                    payload: PlannedAttachment::Json(value),
                    media_type: Arc::from("application/json"),
                    diagnostic_source_name,
                },
                (WorkflowValueType::File, CapturedValue::File(file))
                    if file.output_identity() == source.output =>
                {
                    PlannedAgentAttachment {
                        payload: PlannedAttachment::Artifact(file),
                        media_type: Arc::from(file.media_type()),
                        diagnostic_source_name,
                    }
                }
                _ => {
                    return Err(start_error(AgentInputStartFailure::ValueTypeMismatch {
                        source: source.clone(),
                    }));
                }
            };
            budget.push(attachment, attachments)?;
        }
        ValidatedMessageSource::Reference { .. } => {
            return Err(start_error(AgentInputStartFailure::InputsUnavailable));
        }
    }
    Ok(())
}

fn planned_attachment_size(
    payload: &PlannedAttachment<'_>,
    remaining: u64,
    maximum: u64,
) -> Result<u64, AgentInputMaterializationError> {
    match payload {
        PlannedAttachment::Bytes(bytes) => u64::try_from(bytes.len())
            .ok()
            .filter(|bytes| *bytes <= remaining)
            .ok_or_else(|| {
                start_error(AgentInputStartFailure::AttachmentBytesLimitExceeded { maximum })
            }),
        PlannedAttachment::Json(value) => match canonical_json::encoded_size(value, remaining) {
            Ok(bytes) => Ok(bytes),
            Err(canonical_json::CanonicalJsonError::SizeLimitExceeded) => Err(start_error(
                AgentInputStartFailure::AttachmentBytesLimitExceeded { maximum },
            )),
            Err(canonical_json::CanonicalJsonError::SerializationFailed) => {
                Err(start_error(AgentInputStartFailure::InputsUnavailable))
            }
        },
        PlannedAttachment::Artifact(artifact) if artifact.size() <= remaining => {
            Ok(artifact.size())
        }
        PlannedAttachment::Artifact(_) => Err(start_error(
            AgentInputStartFailure::AttachmentBytesLimitExceeded { maximum },
        )),
    }
}

fn resolve_value_mode(
    admitted: &AdmittedWorkflow,
    step_name: &str,
    step: &ValidatedAgentStep,
) -> Result<AgentValueMode, AgentInputMaterializationError> {
    let mut mode = None;
    for (output, definition) in &step.common.outputs {
        let candidate = match &definition.definition {
            Output::AgentResponse => Some(AgentValueMode::Response {
                output: Arc::from(output.as_str()),
            }),
            Output::AgentResult { .. } => {
                let schema = admitted
                    .workflow()
                    .result_schema(step_name, output)
                    .cloned()
                    .ok_or_else(|| {
                        start_error(AgentInputStartFailure::ResultSchemaUnavailable {
                            output: output.clone(),
                        })
                    })?;
                Some(AgentValueMode::Result {
                    output: Arc::from(output.as_str()),
                    schema,
                })
            }
            Output::File { .. } | Output::GitBranch => None,
        };
        if let Some(candidate) = candidate {
            if mode.is_some() {
                return Err(start_error(AgentInputStartFailure::InvalidValueMode));
            }
            mode = Some(candidate);
        }
    }
    Ok(mode.unwrap_or(AgentValueMode::None))
}

fn retained_text<'a>(
    admitted: &'a AdmittedWorkflow,
    path: &str,
) -> Result<&'a str, AgentInputMaterializationError> {
    let bytes = retained_bytes(admitted, path)?;
    std::str::from_utf8(bytes).map_err(|_| {
        start_error(AgentInputStartFailure::InvalidRetainedText {
            path: path.to_owned(),
        })
    })
}

fn retained_bytes<'a>(
    admitted: &'a AdmittedWorkflow,
    path: &str,
) -> Result<&'a [u8], AgentInputMaterializationError> {
    admitted.workflow().source_bytes(path).ok_or_else(|| {
        start_error(AgentInputStartFailure::RetainedSourceUnavailable {
            path: path.to_owned(),
        })
    })
}

fn upstream_value<'a>(
    upstream_outputs: &'a BTreeMap<ResolvedOutputSource, CapturedValue>,
    source: &ResolvedOutputSource,
) -> Result<&'a CapturedValue, AgentInputMaterializationError> {
    upstream_outputs.get(source).ok_or_else(|| {
        start_error(AgentInputStartFailure::MissingUpstreamValue {
            source: source.clone(),
        })
    })
}

fn stage_message(
    view: &AgentInputStagingLease,
    message: &str,
    cancellation: &CancellationSource,
) -> Result<PathBuf, AgentInputMaterializationError> {
    let path = view.path().join(MESSAGE_FILE);
    let mut destination = create_payload_file(&view.directory, MESSAGE_FILE)?;
    write_bytes(&mut destination, message.as_bytes(), cancellation)?;
    check_cancellation(cancellation)?;
    finish_payload_file(destination).map_err(|_| staging_error())?;
    Ok(path)
}

fn stage_attachments(
    staging: &AgentInputStaging,
    artifacts: &ArtifactStaging,
    view: &AgentInputStagingLease,
    attachments: &[PlannedAgentAttachment<'_>],
    cancellation: &CancellationSource,
) -> Result<Vec<StagedAgentAttachment>, AgentInputMaterializationError> {
    let mut staged = Vec::new();
    staged
        .try_reserve(attachments.len())
        .map_err(|_| start_error(AgentInputStartFailure::InputsUnavailable))?;
    for (index, attachment) in attachments.iter().enumerate() {
        staging.boundary(
            AgentMaterializationBoundaryInternal::BeforeAttachment { index },
            cancellation,
        )?;
        let payload_name = format!("{index:06}");
        let path = view.attachment_path().join(&payload_name);
        let mut destination = create_payload_file(&view.attachment_directory, &payload_name)?;
        let write_result = match &attachment.payload {
            PlannedAttachment::Bytes(bytes) => write_bytes(&mut destination, bytes, cancellation),
            PlannedAttachment::Json(value) => write_json(&mut destination, value, cancellation),
            PlannedAttachment::Artifact(artifact) => {
                write_artifact(artifacts, &mut destination, artifact, cancellation)
            }
        };
        write_result?;
        check_cancellation(cancellation)?;
        finish_payload_file(destination).map_err(|_| staging_error())?;
        staged.push(StagedAgentAttachment::new(
            path,
            Arc::clone(&attachment.media_type),
            attachment.diagnostic_source_name.clone(),
        ));
    }
    Ok(staged)
}

fn create_view_directories(
    parent: &OwnedFd,
) -> Result<(OwnedFd, OwnedFd), AgentInputMaterializationError> {
    let attachments = create_private_directory(parent, ATTACHMENT_DIRECTORY)?;
    let result_endpoint = create_private_directory(parent, RESULT_ENDPOINT_DIRECTORY)?;
    Ok((attachments, result_endpoint))
}

fn create_private_directory(
    parent: &OwnedFd,
    name: &str,
) -> Result<OwnedFd, AgentInputMaterializationError> {
    mkdirat(parent, name, Mode::RWXU).map_err(|_| staging_error())?;
    match openat(parent, name, directory_open_flags(), Mode::empty()) {
        Ok(directory) if fchmod(&directory, Mode::RWXU).is_ok() => Ok(directory),
        Ok(_) | Err(_) => {
            let _ = unlinkat(parent, name, AtFlags::REMOVEDIR);
            Err(staging_error())
        }
    }
}

fn create_payload_file(
    directory: &OwnedFd,
    payload_name: &str,
) -> Result<File, AgentInputMaterializationError> {
    openat(
        directory,
        payload_name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map(File::from)
    .map_err(|_| staging_error())
}

fn write_bytes(
    destination: &mut File,
    bytes: &[u8],
    cancellation: &CancellationSource,
) -> Result<(), AgentInputMaterializationError> {
    for chunk in bytes.chunks(COPY_BUFFER_BYTES) {
        check_cancellation(cancellation)?;
        destination.write_all(chunk).map_err(|_| staging_error())?;
    }
    check_cancellation(cancellation)
}

fn write_json(
    destination: &mut File,
    value: &Value,
    cancellation: &CancellationSource,
) -> Result<(), AgentInputMaterializationError> {
    let mut writer = CancellationWriter::new(destination, cancellation);
    let result = canonical_json::to_writer(&mut writer, value);
    writer.finish(result.map_err(io::Error::other))
}

fn write_artifact(
    artifacts: &ArtifactStaging,
    destination: &mut File,
    artifact: &CapturedArtifact,
    cancellation: &CancellationSource,
) -> Result<(), AgentInputMaterializationError> {
    let mut writer = CancellationWriter::new(destination, cancellation);
    let copied = artifacts.copy_to(artifact.handle(), &mut writer);
    writer.finish(Ok(()))?;
    let copied = copied.map_err(|failure| match failure {
        ArtifactReadFailure::UnknownHandle | ArtifactReadFailure::Unavailable => {
            start_error(AgentInputStartFailure::InputsUnavailable)
        }
        ArtifactReadFailure::DestinationWrite => staging_error(),
    })?;
    if copied != artifact.size() {
        return Err(start_error(AgentInputStartFailure::InputsUnavailable));
    }
    Ok(())
}

struct CancellationWriter<'a> {
    destination: &'a mut File,
    cancellation: &'a CancellationSource,
    cancelled: Option<CancellationReason>,
}

impl<'a> CancellationWriter<'a> {
    fn new(destination: &'a mut File, cancellation: &'a CancellationSource) -> Self {
        Self {
            destination,
            cancellation,
            cancelled: None,
        }
    }

    fn finish(self, result: io::Result<()>) -> Result<(), AgentInputMaterializationError> {
        if let Some(reason) = self
            .cancelled
            .or_else(|| self.cancellation.cancellation_reason())
        {
            return Err(AgentInputMaterializationError::Cancelled { reason });
        }
        result.map_err(|_| staging_error())
    }
}

impl Write for CancellationWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Some(reason) = self.cancellation.cancellation_reason() {
            self.cancelled = Some(reason);
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        self.destination.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.destination.flush()
    }
}

fn check_cancellation(
    cancellation: &CancellationSource,
) -> Result<(), AgentInputMaterializationError> {
    match cancellation.cancellation_reason() {
        Some(reason) => Err(AgentInputMaterializationError::Cancelled { reason }),
        None => Ok(()),
    }
}

fn abort_view(
    view: AgentInputStagingLease,
    error: AgentInputMaterializationError,
) -> AgentInputMaterializationError {
    match view.release() {
        Ok(()) => error,
        Err(_) if matches!(error, AgentInputMaterializationError::Cancelled { .. }) => error,
        Err(_) => staging_error(),
    }
}

fn start_error(failure: AgentInputStartFailure) -> AgentInputMaterializationError {
    AgentInputMaterializationError::Start(failure)
}

fn staging_error() -> AgentInputMaterializationError {
    start_error(AgentInputStartFailure::StagingUnavailable)
}

fn directory_open_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

fn lock_views(views: &Mutex<BTreeSet<Arc<str>>>) -> MutexGuard<'_, BTreeSet<Arc<str>>> {
    views
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests;
