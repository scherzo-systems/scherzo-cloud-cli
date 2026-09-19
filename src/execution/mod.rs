mod claude_code;
mod codex;
mod harness_installation;
mod owned_tree;
mod pi;
mod workflow;

pub(crate) use claude_code::{
    CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION, CLAUDE_CODE_STREAM_JSON_V1_SUPPORTED_RANGE,
    ClaudeCodeCompatibilityProfile, ClaudeCodeIncompatibility, ClaudeCodeInstallationFailure,
    ClaudeCodeProbe, ValidatedClaudeCodeInstallation,
    discover_and_validate_claude_code_installation,
};
pub(crate) use codex::{
    CODEX_APP_SERVER_V1_QUALIFICATION_VERSION, CODEX_APP_SERVER_V1_SUPPORTED_RANGE,
    CodexCompatibilityProfile, CodexIncompatibility, CodexInstallationFailure, CodexProbe,
    ValidatedCodexInstallation, discover_and_validate_codex_installation,
};
pub(crate) use owned_tree::{
    RemovalError, open_directory_at, open_regular_file_at, remove_open_tree_at,
};
pub(crate) use pi::{
    PI_JSON_V1_QUALIFICATION_VERSION, PI_JSON_V1_SUPPORTED_RANGE, PiCompatibilityProfile,
    PiIncompatibility, PiInstallationFailure, PiProbe, ValidatedPiInstallation,
    discover_and_validate_pi_installation,
};
#[cfg(test)]
pub(crate) use workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM;
#[cfg(test)]
pub(crate) use workflow::admission::admit_workflow;
pub(crate) use workflow::admission::{
    AdmissionFailure, AdmissionFailureKind, AdmittedWorkflow, CancellationPolicy,
    CancellationReason, CancellationSource, EnvironmentSnapshot, ExecutionContext,
    OrdinaryCancellationRequestResult, ResolvedAttachment, ResolvedFile, ResolvedInput,
    ResolvedInputs, ResolvedJsonInput, SourceRevisionProvenance, WorkflowCapacityBudget,
    admit_local_workflow, admit_runner_workflow, default_execution_policy_limits,
};
pub(crate) use workflow::agent::WorkflowRunId;
pub(crate) use workflow::agent::dispatch::production_agent_dispatcher;
pub(crate) use workflow::agent_diagnostics::AgentDiagnosticSessionStore;
pub(crate) use workflow::agent_input::AgentInputStaging;
pub(crate) use workflow::archived_attempt::{
    ArchivedAttemptLoadError, load_local_archived_attempt, reconcile_current_result_publication,
};
pub(crate) use workflow::archived_presentation::{
    ArchivedViewOutput, ineligibility_code, operational_error_code,
};
pub(crate) use workflow::artifact::{ArtifactStaging, CaptureCancellation, StagedCarrier};
pub(crate) use workflow::cancellation::{MAXIMUM_CANCELLATION_GRACE, MINIMUM_CANCELLATION_GRACE};
pub(crate) use workflow::capacity::RUNNER_TERMINAL_FRAME_BYTES;
pub(crate) use workflow::child_guard::{
    internal_worker_requested as child_guard_worker_requested,
    run_internal_worker as run_child_guard_worker,
};
pub(crate) use workflow::coordinator::{CoordinationError, CoordinatorClock};
pub(crate) use workflow::diagnostic::StepDiagnosticLog;
#[cfg(test)]
pub(crate) use workflow::diagnostic::{CapturedDiagnosticStream, StepDiagnostic};
pub(crate) use workflow::document::FailurePolicy;
#[cfg(test)]
pub(crate) use workflow::document::FinalizationTrigger;
#[cfg(test)]
pub(crate) use workflow::evidence::{BlockedDetail, Prerequisite};
pub(crate) use workflow::evidence::{FailureDetail, NodeDetail, PrimaryIssue};
pub(crate) use workflow::execution::{NoopCommitPort, WorkflowExecutionResult, execute_workflow};
pub(crate) use workflow::git_capture::CloudGitCaptureProjection;
pub(crate) use workflow::input::InputStaging;
pub(crate) use workflow::invocation_accounting::InvocationAccountingLog;
pub(crate) use workflow::local_run::{
    DurableDeadline, DurableInvocationStateV1, DurableInvocationV1, InitialLocalRun,
    LocalAttemptOwner, LocalAttemptOwnershipReleased, LocalRecoveryStatus, LocalRetryBeginError,
    LocalRetryEligibility, LocalRetryOpen, LocalRetryRejection, LocalRunStatusSnapshot,
    LocalStatusError, LocalStatusResult, PublicationFailurePhaseV1, RetryIneligibilityReason,
    acquire_local_retry, read_local_run_status,
};
pub(crate) use workflow::observation::{
    ExecutionObservation, ExecutionObserver, ObservedStepTransition, TransitionObservation,
};
pub(crate) use workflow::portable_artifact::{
    ArtifactDiagnostic, ArtifactValidationSummary, PortableArtifactValidation,
    PortableArtifactValidationFailure, validate_portable_artifact_set,
};
pub(crate) use workflow::presentation::{
    ColorChoice, PresentationConfig, PresentationFailure, PresentationFailureOperation,
    PresentationMode, PublicationPresentation, RequestedPresentationMode, SystemObservationClock,
    TerminalCapabilities, WorkflowRunOutput, WorkflowRunPresentation,
    WorkflowRunPresentationResult, styled_terminal_text, visible_text,
};
pub(crate) use workflow::presentation_feed::{DisplayDeadline, normalize_terminal_scalar};
pub(crate) use workflow::process_group::{
    AuthenticatedProcessGroup, DurableProcessGuardStore, ProcessGuardRegistry,
    ProcessIdentityInspector, ProcessIdentityObservation, SystemProcessIdentityInspector,
    terminate_authenticated_process_group,
};
pub(crate) use workflow::publication::{
    CloudCarrierBody, CloudExecutionCapacityV1, CloudResultCarrier, DigestV1,
    LocalPublicationError, LocalPublicationPhase, PreparedCloudWorkflowResult,
    RecoveryDiagnosticKindV1, RecoveryInvocationDiagnosticV1, RecoveryInvocationRoleV1,
    RecoveryInvocationStateV1, RecoveryInvocationUsageV1, RecoveryInvocationV1, WorkflowResultV1,
    WorkflowRunCancellation, WorkflowRunFinalization, WorkflowRunFinalizationCancellation,
    WorkflowRunResult, WorkflowRunStep, WorkflowRunStepKind, WorkflowRunTerminalResultV1,
    WorkflowRunTiming, WorkflowStepTiming, command_output_v1, prepare_attempt_result_destination,
    prepare_cloud_workflow_result, publish_prepared_workflow_result, step_recovery_summary_v1,
    summary_disposition_matches,
};
pub(crate) use workflow::rejection::{RejectionDiagnostic, human_resolution_remedy};
pub(crate) use workflow::resolution::{
    ResolutionFailure, ResolvedWorkflow, resolve, resolve_workflow_file,
};
pub(crate) use workflow::result_validation::{
    internal_worker_requested as result_validation_worker_requested,
    run_internal_worker as run_result_validation_worker,
};
#[cfg(test)]
pub(crate) use workflow::run_timing::ObservationTime;
pub(crate) use workflow::run_timing::{ObservationClock, RunTimingObservation, RunTimingSnapshot};
pub(crate) use workflow::run_view_model::{
    WorkflowRunCleanupResult, WorkflowRunPublicationResult, WorkflowRunViewModel,
};
#[cfg(test)]
pub(crate) use workflow::runtime::ExportValue;
pub(crate) use workflow::runtime::{
    ActionId, ActiveStepInvocation, FinalizationGate, FinalizationSummary, FinalizerResult,
    ForceAbortEvidence, RecoveryDecisionKind, RecoveryHandlerActivity, RecoveryHandlerKind,
    RunOutcome, SchedulingGate, StepRecoveryState, StepState, StepStateKind, TransitionEvent,
    TransitionSequence, WorkflowState,
};
#[cfg(test)]
pub(crate) use workflow::runtime::{RecoveryRoundNumber, TargetExecutionNumber};
#[cfg(test)]
pub(crate) use workflow::step_runtime::spawn_isolated_command_launch;
pub(crate) use workflow::step_runtime::{AgentExecution, StepFailureCause};
pub(crate) use workflow::terminal_host::archived::{
    ArchivedTerminalHostExit, ArchivedWorkflowTerminalHost,
};
pub(crate) use workflow::terminal_host::{TerminalHostExit, WorkflowTerminalHost};
pub(crate) use workflow::validated::{
    ValidatedHarness, ValidatedRecoveryHandler, ValidatedStep, WorkflowNodeRole, WorkflowValueType,
};
#[cfg(test)]
pub(crate) use workflow::value::CapturedValue;
pub(crate) use workflow::{
    MAXIMUM_PARALLEL_STEPS, STRUCTURAL_SCHEMA, is_input_name, is_lowercase_hex,
    is_valid_input_display_name, is_valid_media_type, lowercase_hex,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionOutcome {
    Succeeded,
    Failed,
    Interrupted,
    Terminated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AgentHarnessInstallationFailure {
    Pi(PiInstallationFailure),
    ClaudeCode(ClaudeCodeInstallationFailure),
    Codex(CodexInstallationFailure),
}
