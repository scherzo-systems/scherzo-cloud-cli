#![cfg_attr(
    test,
    allow(
        clippy::disallowed_macros,
        clippy::expect_used,
        clippy::panic,
        clippy::unwrap_used,
        reason = "unit tests use Cargo-provided fixture paths and panic shortcuts"
    )
)]

mod claude_code;
mod codex;
mod harness_installation;
mod owned_tree;
mod pi;
mod process;
mod workflow;

pub use claude_code::{
    CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION, CLAUDE_CODE_STREAM_JSON_V1_SUPPORTED_RANGE,
    ClaudeCodeCompatibilityProfile, ClaudeCodeIncompatibility, ClaudeCodeInstallationFailure,
    ClaudeCodeProbe, ValidatedClaudeCodeInstallation,
    discover_and_validate_claude_code_installation,
};
pub use codex::{
    CODEX_APP_SERVER_V1_QUALIFICATION_VERSION, CODEX_APP_SERVER_V1_SUPPORTED_RANGE,
    CodexCompatibilityProfile, CodexIncompatibility, CodexInstallationFailure, CodexProbe,
    ValidatedCodexInstallation, discover_and_validate_codex_installation,
};
pub use owned_tree::{RemovalError, open_directory_at, open_regular_file_at, remove_open_tree_at};
pub use pi::{
    PI_JSON_V1_QUALIFICATION_VERSION, PI_JSON_V1_SUPPORTED_RANGE, PiCompatibilityProfile,
    PiIncompatibility, PiInstallationFailure, PiProbe, ValidatedPiInstallation,
    discover_and_validate_pi_installation,
};
pub use workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM;
pub use workflow::admission::admit_workflow;
pub use workflow::admission::{
    AdmissionFailure, AdmissionFailureKind, AdmittedWorkflow, CancellationPolicy,
    CancellationReason, CancellationSource, EnvironmentSnapshot, ExecutionContext,
    OrdinaryCancellationRequestResult, ResolvedAttachment, ResolvedFile, ResolvedInput,
    ResolvedInputs, ResolvedJsonInput, SourceRevisionProvenance, WorkflowCapacityBudget,
    admit_local_workflow, admit_runner_workflow, default_execution_policy_limits,
};
pub use workflow::agent::WorkflowRunId;
pub use workflow::agent::dispatch::{ProductionAgentDispatcher, production_agent_dispatcher};
pub use workflow::agent_diagnostics::AgentDiagnosticSessionStore;
pub use workflow::agent_input::AgentInputStaging;
pub use workflow::archived_attempt::{
    ArchivedAttemptLoadError, load_local_archived_attempt, reconcile_current_result_publication,
};
pub use workflow::archived_presentation::{
    ArchivedViewOutput, ineligibility_code, operational_error_code,
};
pub use workflow::artifact::{ArtifactStaging, CaptureCancellation, StagedCarrier};
pub use workflow::cancellation::{MAXIMUM_CANCELLATION_GRACE, MINIMUM_CANCELLATION_GRACE};
pub use workflow::capacity::RUNNER_TERMINAL_FRAME_BYTES;
pub use workflow::child_guard::{
    internal_worker_requested as child_guard_worker_requested,
    run_internal_worker as run_child_guard_worker,
};
pub use workflow::coordinator::{CoordinationError, CoordinatorClock};
pub use workflow::diagnostic::StepDiagnosticLog;
pub use workflow::diagnostic::{CapturedDiagnosticStream, StepDiagnostic};
pub use workflow::document::FailurePolicy;
pub use workflow::document::FinalizationTrigger;
pub use workflow::evidence::{BlockedDetail, Prerequisite};
pub use workflow::evidence::{
    FailureDetail, InheritedDetail, InheritedPriorState, NodeDetail, PrimaryIssue,
};
pub use workflow::execution::{NoopCommitPort, WorkflowExecutionResult, execute_workflow};
pub use workflow::git_capture::CloudGitCaptureProjection;
pub use workflow::input::InputStaging;
pub use workflow::invocation_accounting::InvocationAccountingLog;
pub use workflow::local_run::{
    DurableDeadline, DurableInvocationStateV1, DurableInvocationV1, InitialLocalRun,
    LocalAttemptOwner, LocalAttemptOwnershipReleased, LocalRecoveryStatus, LocalRetryBeginError,
    LocalRetryEligibility, LocalRetryOpen, LocalRetryRejection, LocalRunStatusSnapshot,
    LocalStatusError, LocalStatusResult, PublicationFailurePhaseV1, RetryIneligibilityReason,
    acquire_local_retry, read_local_run_status,
};
pub use workflow::observation::{
    ExecutionObservation, ExecutionObserver, ObservedStepTransition, TransitionObservation,
};
pub use workflow::portable_artifact::{
    ArtifactDiagnostic, ArtifactValidationSummary, PortableArtifactValidation,
    PortableArtifactValidationFailure, validate_portable_artifact_set,
};
pub use workflow::presentation::{
    ColorChoice, PresentationConfig, PresentationFailure, PresentationFailureOperation,
    PresentationMode, PublicationPresentation, RequestedPresentationMode, SystemObservationClock,
    TerminalCapabilities, WorkflowRunOutput, WorkflowRunPresentation,
    WorkflowRunPresentationResult, styled_terminal_text, visible_text,
};
pub use workflow::presentation_feed::{DisplayDeadline, normalize_terminal_scalar};
pub use workflow::process_group::{
    AuthenticatedProcessGroup, DurableProcessGuardStore, ProcessGuardRegistry,
    ProcessGuardStoreError, ProcessIdentityInspector, ProcessIdentityObservation,
    SystemProcessIdentityInspector, terminate_authenticated_process_group,
};
pub use workflow::publication::{
    CloudCarrierBody, CloudExecutionCapacityV1, CloudResultCarrier, CloudSourceDisplayRepositoryV1,
    CloudSourceDisplaySnapshotV1, ContinuationRecordV1, DigestV1, LocalPublicationError,
    LocalPublicationPhase, PreparedCloudWorkflowResult, RecoveryDiagnosticKindV1,
    RecoveryInvocationDiagnosticV1, RecoveryInvocationRoleV1, RecoveryInvocationStateV1,
    RecoveryInvocationUsageV1, RecoveryInvocationV1, WorkflowResultV1, WorkflowRunCancellation,
    WorkflowRunFinalization, WorkflowRunFinalizationCancellation, WorkflowRunResult,
    WorkflowRunStep, WorkflowRunStepKind, WorkflowRunTerminalResultV1, WorkflowRunTiming,
    WorkflowStepTiming, command_output_v1, prepare_attempt_result_destination,
    prepare_cloud_workflow_result, publish_prepared_workflow_result, step_recovery_summary_v1,
    summary_disposition_matches,
};
pub use workflow::rejection::{RejectionDiagnostic, human_resolution_remedy};
pub use workflow::resolution::{
    ResolutionFailure, ResolvedWorkflow, resolve, resolve_workflow_file,
};
pub use workflow::result_validation::{
    internal_worker_requested as result_validation_worker_requested,
    run_internal_worker as run_result_validation_worker,
};
pub use workflow::run_timing::ObservationTime;
pub use workflow::run_timing::{ObservationClock, RunTimingObservation, RunTimingSnapshot};
pub use workflow::run_view_model::{
    WorkflowRunCleanupResult, WorkflowRunPublicationResult, WorkflowRunViewModel,
};
pub use workflow::runtime::ExportValue;
pub use workflow::runtime::{
    ActionId, ActiveStepInvocation, ExecutionSeed, FinalizationGate, FinalizationSummary,
    FinalizerResult, ForceAbortEvidence, InheritedDisposition, OutputProducer,
    RecoveryDecisionKind, RecoveryHandlerActivity, RecoveryHandlerKind, RunOutcome, SchedulingGate,
    StepRecoveryState, StepState, StepStateKind, TransitionEvent, TransitionSequence,
    WorkflowState,
};
pub use workflow::runtime::{RecoveryRoundNumber, TargetExecutionNumber};
pub use workflow::step_runtime::spawn_isolated_command_launch;
pub use workflow::step_runtime::{AgentExecution, StepFailureCause, WorkflowExecutionStart};
pub use workflow::terminal_host::archived::{
    ArchivedTerminalHostExit, ArchivedWorkflowTerminalHost,
};
pub use workflow::terminal_host::{TerminalHostExit, WorkflowTerminalHost};
pub use workflow::validated::{
    ValidatedHarness, ValidatedRecoveryHandler, ValidatedStep, WorkflowNodeRole, WorkflowValueType,
};
pub use workflow::value::CapturedValue;
pub use workflow::{
    MAXIMUM_PARALLEL_STEPS, STRUCTURAL_SCHEMA, is_input_name, is_lowercase_hex,
    is_valid_input_display_name, is_valid_media_type, lowercase_hex,
};

pub use claude_code::ClaudeCodeCapability;
pub use codex::CodexCapability;
pub use codex::CodexInstallationIdentity;
pub use harness_installation::StableVersion;
pub use pi::PiCapability;
pub use workflow::ClaudeCodeConfig;
pub use workflow::PiConfig;
pub use workflow::admission::AdmittedExecutionContext;
pub use workflow::admission::AdmittedHarness;
pub use workflow::admission::AdmittedWorkflowCapacity;
pub use workflow::admission::ExecutionLimits;
pub use workflow::admission::ExecutionPolicyLimits;
pub use workflow::admission::ResolvedJsonInputError;
pub use workflow::admission::WorkflowExecutionContract;
pub use workflow::agent_diagnostics::AgentDiagnosticSessionError;
pub use workflow::agent_input::AgentInputStagingFailure;
pub use workflow::agent_input::AgentInputStagingReleaseFailure;
pub use workflow::archived_attempt::ArchivedAttemptIneligibilityReason;
pub use workflow::archived_attempt::ArchivedAttemptIneligible;
pub use workflow::archived_attempt::ArchivedAttemptOperationalError;
pub use workflow::archived_attempt::ArchivedAttemptOperationalErrorCode;
pub use workflow::archived_attempt::LoadedLocalArchivedAttempt;
pub use workflow::archived_attempt::LocalArchivedAttempt;
pub use workflow::archived_presentation::ArchivedViewOutputFailure;
pub use workflow::artifact::ArtifactReadFailure;
pub use workflow::artifact::ArtifactReleaseFailure;
pub use workflow::artifact::ArtifactStagingFailure;
pub use workflow::capacity::ComputedWorkflowCapacity;
pub use workflow::codex::CodexConfig;
pub use workflow::evidence::CancellationDetail;
pub use workflow::evidence::ConditionFalseDetail;
pub use workflow::evidence::EvidenceError;
pub use workflow::evidence::NonExecutionDetail;
pub use workflow::git_capture::GitCaptureContext;
pub use workflow::git_capture::LocalGitBaseline;
pub use workflow::input::InputStagingFailure;
pub use workflow::input::InputStagingReleaseFailure;
pub use workflow::invocation_accounting::InvocationUsage;
pub use workflow::invocation_accounting::NativeSessionFact;
pub use workflow::local_run::AttemptPrivateStaging;
pub use workflow::local_run::DurableInvocationDiagnosticV1;
pub use workflow::local_run::LocalRunCommitPort;
pub use workflow::local_run::LocalRunDirectoryError;
pub use workflow::local_run::LocalStatusAttempt;
pub use workflow::local_run::LocalStatusErrorCode;
pub use workflow::local_run::OwnershipUnprovenReason;
pub use workflow::local_run::PendingLocalRetry;
pub use workflow::process_group::AuthenticatedSignalResult;
pub use workflow::process_group::ProcessGuardRegistration;
pub use workflow::publication::CommandOutputV1;
pub use workflow::publication::PreparedResultDestination;
pub use workflow::publication::StepRecoverySummaryV1;
pub use workflow::publication::WorkflowStepV1;
pub use workflow::rejection::RejectionLocation;
pub use workflow::resolution::ContentDigestAlgorithm;
pub use workflow::resolution::WorkflowContentDigest;
pub use workflow::resolution::WorkflowSourceProvenance;
pub use workflow::run_timing::ObservedStepTiming;
pub use workflow::run_view_model::WorkflowRunPublicationFailure;
pub use workflow::run_view_model::WorkflowRunViewModelError;
pub use workflow::run_view_model::WorkflowRunViewSnapshot;
pub use workflow::runtime::FinalizationCancellation;
pub use workflow::runtime::RunCancellationPhase;
pub use workflow::step_runtime::NoAgentDispatcher;
pub use workflow::terminal_host::archived::ArchivedTerminalExitRequest;
pub use workflow::validated::ResolvedOutputSource;
pub use workflow::validated::ValidatedAgent;
pub use workflow::validated::ValidatedAgentStep;
pub use workflow::validated::ValidatedCommandStep;
pub use workflow::validated::ValidatedCommonStep;
pub use workflow::validated::ValidatedFinalizer;
pub use workflow::validated::ValidatedStepRecovery;
pub use workflow::validated::ValidatedWorkflow;

pub use claude_code::ClaudeCodeStreamJsonV1Capabilities;
pub use codex::CodexAppServerV1Capabilities;
pub use pi::PiJsonV1Capabilities;
pub use process::{
    CommandOutput, CommandProbeError, CommandRequest, CommandRunner, ManagedProcessGroup,
    SystemCommandRunner,
};
pub use workflow::CanonicalJsonError;
pub use workflow::admission::ClaudeCodeStreamJsonV1Admission;
pub use workflow::admission::CodexAppServerV1Admission;
pub use workflow::admission::PiJsonV1Admission;
pub use workflow::agent::AgentFailure;
pub use workflow::agent::AgentInvocation;
pub use workflow::agent::AgentObservationEnvelope;
pub use workflow::agent::AgentObservationSink;
pub use workflow::agent::AgentStartCallback;
pub use workflow::agent::AgentTerminalCallback;
pub use workflow::agent::dispatch::AgentInvocationDispatcher;
pub use workflow::agent_input::AgentInputStartFailure;
pub use workflow::agent_input::ClosedAgentInvocation;
pub use workflow::artifact::ArtifactHandle;
pub use workflow::artifact::CaptureCandidateSet;
pub use workflow::artifact::CaptureFailure;
pub use workflow::artifact::CapturedArtifact;
pub use workflow::artifact::CapturedGitBranch;
pub use workflow::capacity::WorkflowCapacity;
pub use workflow::claude_code_stream_json_v1::ClaudeCodeStreamJsonV1ProtocolLimits;
pub use workflow::codex_app_server_v1::CodexAppServerV1ProtocolLimits;
pub use workflow::coordinator::CommitPort;
pub use workflow::coordinator::CommittedReduction;
pub use workflow::git_capture::GitCaptureFailure;
pub use workflow::git_capture::GitCommandTimeout;
pub use workflow::input::InputPreparationFailure;
pub use workflow::observation::CommandOutputClosedObservation;
pub use workflow::observation::CommandOutputObservation;
pub use workflow::pi_json_v1::PiJsonV1ProtocolLimits;
pub use workflow::process_group::LeaderState;
pub use workflow::publication::DiagnosticStreamV1;
pub use workflow::publication::RunResultInvariant;
pub use workflow::recovery::RecoveryDecisionFailureKind;
pub use workflow::recovery::RecoveryHandlerFailure;
pub use workflow::runtime::ExportUnavailableReason;
pub use workflow::step_runtime::AgentExecutionObservationSink;
pub use workflow::step_runtime::CommandExecutionFailure;
pub use workflow::step_runtime::CommandLaunchFailure;
pub use workflow::step_runtime::CommandPreparationFailure;
pub use workflow::step_runtime::OutputCaptureFailure;
pub use workflow::step_runtime::StepExecutionFailure;
pub use workflow::step_runtime::StepStartFailure;
pub use workflow::step_runtime::WorkflowAgentDispatcher;
pub use workflow::step_runtime::WorkflowCommitPort;
pub use workflow::step_runtime::WorkingDirectoryFailure;
pub use workflow::value::CapturedJson;
pub use workflow::value::CapturedText;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionOutcome {
    Succeeded,
    Failed,
    Interrupted,
    Terminated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgentHarnessInstallationFailure {
    Pi(PiInstallationFailure),
    ClaudeCode(ClaudeCodeInstallationFailure),
    Codex(CodexInstallationFailure),
}
