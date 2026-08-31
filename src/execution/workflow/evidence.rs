use std::cmp::Ordering;
use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use super::admission::CancellationReason;
use super::agent::{AgentFailure, AgentFailureCause};
use super::agent_input::AgentInputStartFailure;
use super::artifact::CaptureFailureKind;
use super::git_capture::GitCaptureFailure;
use super::input::InputPreparationFailureKind;
use super::step_runtime::{
    CommandExecutionFailure, CommandLaunchFailure, CommandPreparationFailure, OutputCaptureFailure,
    StepExecutionFailure, StepFailureCause, StepStartFailure, WorkingDirectoryFailure,
};
use super::validated::{WorkflowNode, WorkflowNodeRole};

pub(crate) const MAXIMUM_PREREQUISITES: usize = 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FailurePhase {
    Start,
    Execution,
    OutputCapture,
}

impl FailurePhase {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Execution => "execution",
            Self::OutputCapture => "output_capture",
        }
    }
}

// Canonical committed codes deliberately mirror the separately versioned provisional
// recovery-summary wire enum; sharing them would make recovery history node evidence.
// jscpd:ignore-start
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FailureCode {
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
    ExecutionTaskUnavailable,
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
    OutputInvalidUtf8,
    OutputInvalidJson,
    OutputDuplicateJsonMember,
    OutputJsonSchemaMismatch,
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
// jscpd:ignore-end

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FailureDetail {
    pub(crate) phase: FailurePhase,
    pub(crate) code: FailureCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) input: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) collection_index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) exit_code: Option<i32>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FailureDetailWire {
    phase: FailurePhase,
    code: FailureCode,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    input: Option<String>,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    collection_index: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    output: Option<String>,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    exit_code: Option<i32>,
}

impl<'de> Deserialize<'de> for FailureDetail {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FailureDetailWire::deserialize(deserializer)?;
        Self::new(
            wire.phase,
            wire.code,
            wire.input,
            wire.collection_index,
            wire.output,
            wire.exit_code,
        )
        .map_err(D::Error::custom)
    }
}

impl FailureDetail {
    fn code(phase: FailurePhase, code: FailureCode) -> Self {
        Self {
            phase,
            code,
            input: None,
            collection_index: None,
            output: None,
            exit_code: None,
        }
    }

    pub(crate) fn new(
        phase: FailurePhase,
        code: FailureCode,
        input: Option<String>,
        collection_index: Option<u64>,
        output: Option<String>,
        exit_code: Option<i32>,
    ) -> Result<Self, EvidenceError> {
        let detail = Self {
            phase,
            code,
            input,
            collection_index,
            output,
            exit_code,
        };
        detail.validate()?;
        Ok(detail)
    }

    fn validate(&self) -> Result<(), EvidenceError> {
        use FailureCode as Code;
        use FailurePhase as Phase;

        let phase_valid = match self.code {
            Code::StepUnavailable => matches!(self.phase, Phase::Start | Phase::OutputCapture),
            Code::HarnessStartFailed
            | Code::HarnessInputTooLarge
            | Code::HarnessFailed
            | Code::HarnessProtocolFailed
            | Code::MissingResponse
            | Code::MissingResult
            | Code::ResultValidationLimitExceeded
            | Code::CapturedValueTooLarge
            | Code::ResultSettlementFailed => {
                matches!(self.phase, Phase::Start | Phase::Execution)
            }
            Code::CommandExit | Code::CommandWaitFailed | Code::ExecutionTaskUnavailable => {
                self.phase == Phase::Execution
            }
            Code::OutputUnsupported
            | Code::CaptureTaskUnavailable
            | Code::OutputPathAbsolute
            | Code::OutputPathEscape
            | Code::OutputPathEmpty
            | Code::OutputMissing
            | Code::OutputSymbolicLink
            | Code::OutputParentNotDirectory
            | Code::OutputNotRegularFile
            | Code::OutputSourceUnavailable
            | Code::OutputInvalidUtf8
            | Code::OutputInvalidJson
            | Code::OutputDuplicateJsonMember
            | Code::OutputJsonSchemaMismatch
            | Code::CapturedFileCountLimit
            | Code::CapturedFileSizeLimit
            | Code::CapturedTotalSizeLimit
            | Code::CapturedGitCarrierCountLimit
            | Code::CapturedGitCarrierSizeLimit
            | Code::CapturedTotalGitCarrierSizeLimit
            | Code::GitExecutionRootRebound
            | Code::GitHeadUnavailable
            | Code::GitBaselineNotAncestor
            | Code::GitCleanlinessUnavailable
            | Code::GitWorkspaceDirty
            | Code::GitTreeUnavailable
            | Code::GitRequiredObjectsUnavailable
            | Code::GitSourceAuthorityChanged
            | Code::GitStructureLimitExceeded
            | Code::GitCommandTimedOut
            | Code::GitBundleGenerationFailed
            | Code::GitBundleProfileInvalid
            | Code::GitBundleVerificationFailed
            | Code::GitWorkspaceChanged
            | Code::GitTemporaryStorageUnavailable
            | Code::OutputStagingUnavailable => self.phase == Phase::OutputCapture,
            _ => self.phase == Phase::Start,
        };
        if !phase_valid {
            return Err(EvidenceError::InvalidFailureDetail);
        }

        let input_invalid_name = self.code == Code::InputInvalidName;
        let input_preparation = matches!(
            self.code,
            Code::InputValueCountLimit
                | Code::InputValueSizeLimit
                | Code::InputTotalSizeLimit
                | Code::InputCollectionOrdinalLimit
                | Code::InputTypeMismatch
                | Code::InputSourceUnavailable
                | Code::InputStagingUnavailable
                | Code::InputLiveLimit
        );
        let output_failure = self.phase == Phase::OutputCapture
            && !matches!(
                self.code,
                Code::StepUnavailable | Code::OutputUnsupported | Code::CaptureTaskUnavailable
            );
        let input_valid = match self.input.as_deref() {
            None => !input_invalid_name,
            Some(input) if input_invalid_name => !input.is_empty() && input.len() <= 4_096,
            Some(input) => input_preparation && is_workflow_value_identifier(input),
        };
        if !input_valid
            || (self.collection_index.is_some() && !input_preparation)
            || (output_failure
                && self
                    .output
                    .as_deref()
                    .is_none_or(|output| !is_workflow_value_identifier(output)))
            || (!output_failure && self.output.is_some())
            || (self.exit_code.is_some()
                && (self.code != Code::CommandExit || self.exit_code == Some(0)))
        {
            return Err(EvidenceError::InvalidFailureDetail);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PrerequisiteKind {
    Control,
    Condition,
    Body,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Prerequisite {
    Control { node: String },
    Condition { r#ref: String },
    Body { r#ref: String },
}

impl Prerequisite {
    pub(crate) fn control(node: impl Into<String>) -> Result<Self, EvidenceError> {
        let node = node.into();
        (!node.is_empty())
            .then_some(Self::Control { node })
            .ok_or(EvidenceError::InvalidPrerequisite)
    }

    pub(crate) fn body(reference: impl Into<String>) -> Result<Self, EvidenceError> {
        let r#ref = reference.into();
        is_output_reference(&r#ref)
            .then_some(Self::Body { r#ref })
            .ok_or(EvidenceError::InvalidPrerequisite)
    }

    pub(crate) fn kind(&self) -> PrerequisiteKind {
        match self {
            Self::Control { .. } => PrerequisiteKind::Control,
            Self::Condition { .. } => PrerequisiteKind::Condition,
            Self::Body { .. } => PrerequisiteKind::Body,
        }
    }

    pub(crate) fn target(&self) -> &str {
        match self {
            Self::Control { node } => node,
            Self::Condition { r#ref } | Self::Body { r#ref } => r#ref,
        }
    }
}

impl Ord for Prerequisite {
    fn cmp(&self, other: &Self) -> Ordering {
        self.kind()
            .cmp(&other.kind())
            .then_with(|| self.target().as_bytes().cmp(other.target().as_bytes()))
    }
}

impl PartialOrd for Prerequisite {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BlockedCode {
    PrerequisitesUnsatisfied,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BlockedDetail {
    pub(crate) code: BlockedCode,
    pub(crate) prerequisites: Vec<Prerequisite>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BlockedDetailWire {
    code: BlockedCode,
    prerequisites: Vec<Prerequisite>,
}

impl<'de> Deserialize<'de> for BlockedDetail {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BlockedDetailWire::deserialize(deserializer)?;
        let original = wire.prerequisites;
        let detail = Self::new(original.clone()).map_err(D::Error::custom)?;
        if detail.prerequisites != original {
            return Err(D::Error::custom(EvidenceError::InvalidPrerequisite));
        }
        Ok(detail)
    }
}

impl BlockedDetail {
    pub(crate) fn new(
        prerequisites: impl IntoIterator<Item = Prerequisite>,
    ) -> Result<Self, EvidenceError> {
        let mut prerequisites = prerequisites.into_iter().collect::<Vec<_>>();
        prerequisites.sort();
        prerequisites.dedup();
        if prerequisites.is_empty() || prerequisites.len() > MAXIMUM_PREREQUISITES {
            return Err(EvidenceError::InvalidPrerequisiteCount);
        }
        if prerequisites.iter().any(|prerequisite| {
            prerequisite.target().is_empty()
                || (prerequisite.kind() != PrerequisiteKind::Control
                    && !is_output_reference(prerequisite.target()))
        }) {
            return Err(EvidenceError::InvalidPrerequisite);
        }
        Ok(Self {
            code: BlockedCode::PrerequisitesUnsatisfied,
            prerequisites,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NonExecutionCode {
    FailureStop,
    FinalizerTriggerNotSelected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NonExecutionDetail {
    pub(crate) code: NonExecutionCode,
}

impl NonExecutionDetail {
    pub(crate) const fn failure_stop() -> Self {
        Self {
            code: NonExecutionCode::FailureStop,
        }
    }

    pub(crate) const fn finalizer_trigger_not_selected() -> Self {
        Self {
            code: NonExecutionCode::FinalizerTriggerNotSelected,
        }
    }

    pub(crate) fn for_role(
        role: WorkflowNodeRole,
        code: NonExecutionCode,
    ) -> Result<Self, EvidenceError> {
        if matches!(
            (role, code),
            (WorkflowNodeRole::Step, NonExecutionCode::FailureStop)
                | (
                    WorkflowNodeRole::Finalizer,
                    NonExecutionCode::FinalizerTriggerNotSelected
                )
        ) {
            Ok(Self { code })
        } else {
            Err(EvidenceError::NodeRoleMismatch)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CancellationDetail {
    pub(crate) code: CancellationReason,
}

impl CancellationDetail {
    pub(crate) const fn new(code: CancellationReason) -> Self {
        Self { code }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum NodeDetail {
    Failed(FailureDetail),
    Blocked(BlockedDetail),
    NotRun(NonExecutionDetail),
    Cancellation(CancellationDetail),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum PrimaryIssueDetail {
    Failed(FailureDetail),
    Blocked(BlockedDetail),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PrimaryIssueState {
    Failed,
    Blocked,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PrimaryIssue {
    pub(crate) node: WorkflowNode,
    pub(crate) state: PrimaryIssueState,
    pub(crate) detail: PrimaryIssueDetail,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PrimaryIssueWire {
    node: WorkflowNode,
    state: PrimaryIssueState,
    detail: serde_json::Value,
}

impl<'de> Deserialize<'de> for PrimaryIssue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PrimaryIssueWire::deserialize(deserializer)?;
        let detail = match wire.state {
            PrimaryIssueState::Failed => serde_json::from_value(wire.detail)
                .map(PrimaryIssueDetail::Failed)
                .map_err(D::Error::custom)?,
            PrimaryIssueState::Blocked => serde_json::from_value(wire.detail)
                .map(PrimaryIssueDetail::Blocked)
                .map_err(D::Error::custom)?,
        };
        Ok(Self {
            node: wire.node,
            state: wire.state,
            detail,
        })
    }
}

impl PrimaryIssue {
    pub(crate) fn failed(node: WorkflowNode, detail: FailureDetail) -> Self {
        Self {
            node,
            state: PrimaryIssueState::Failed,
            detail: PrimaryIssueDetail::Failed(detail),
        }
    }

    pub(crate) fn blocked(node: WorkflowNode, detail: BlockedDetail) -> Self {
        Self {
            node,
            state: PrimaryIssueState::Blocked,
            detail: PrimaryIssueDetail::Blocked(detail),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EvidenceError {
    InvalidFailureCause,
    InvalidFailureDetail,
    InvalidPrerequisite,
    InvalidPrerequisiteCount,
    NodeRoleMismatch,
}

impl fmt::Display for EvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid workflow node evidence: {self:?}")
    }
}

impl std::error::Error for EvidenceError {}

pub(crate) trait NodeFailureSource {
    fn node_failure_detail(&self, phase: FailurePhase) -> Result<FailureDetail, EvidenceError>;
}

impl NodeFailureSource for StepFailureCause {
    fn node_failure_detail(&self, phase: FailurePhase) -> Result<FailureDetail, EvidenceError> {
        failure_detail(phase, self)
    }
}

#[cfg(test)]
impl NodeFailureSource for String {
    fn node_failure_detail(&self, phase: FailurePhase) -> Result<FailureDetail, EvidenceError> {
        let code = match phase {
            FailurePhase::Start | FailurePhase::OutputCapture => FailureCode::StepUnavailable,
            FailurePhase::Execution => FailureCode::CommandWaitFailed,
        };
        FailureDetail::new(phase, code, None, None, None, None)
    }
}

pub(crate) fn failure_detail(
    phase: FailurePhase,
    source: &StepFailureCause,
) -> Result<FailureDetail, EvidenceError> {
    let detail = match (phase, source) {
        (FailurePhase::Start, StepFailureCause::Start(source)) => start_failure_detail(source)?,
        (FailurePhase::Execution, StepFailureCause::Execution(source)) => {
            execution_failure_detail(source)
        }
        (FailurePhase::OutputCapture, StepFailureCause::OutputCapture(source)) => {
            output_capture_failure_detail(source)
        }
        _ => return Err(EvidenceError::InvalidFailureCause),
    };
    detail.validate()?;
    Ok(detail)
}

fn start_failure_detail(source: &StepStartFailure) -> Result<FailureDetail, EvidenceError> {
    let detail = match source {
        StepStartFailure::StepUnavailable => {
            code(FailurePhase::Start, FailureCode::StepUnavailable)
        }
        StepStartFailure::PreparationTaskUnavailable => {
            code(FailurePhase::Start, FailureCode::PreparationTaskUnavailable)
        }
        StepStartFailure::InputsUnavailable => {
            code(FailurePhase::Start, FailureCode::InputsUnavailable)
        }
        StepStartFailure::InputPreparation(source) => {
            let failure_code = match source.kind() {
                InputPreparationFailureKind::InvalidInputName => FailureCode::InputInvalidName,
                InputPreparationFailureKind::ValueCountLimitExceeded => {
                    FailureCode::InputValueCountLimit
                }
                InputPreparationFailureKind::ValueSizeLimitExceeded => {
                    FailureCode::InputValueSizeLimit
                }
                InputPreparationFailureKind::TotalSizeLimitExceeded => {
                    FailureCode::InputTotalSizeLimit
                }
                InputPreparationFailureKind::CollectionOrdinalLimitExceeded => {
                    FailureCode::InputCollectionOrdinalLimit
                }
                InputPreparationFailureKind::ValueTypeMismatch => FailureCode::InputTypeMismatch,
                InputPreparationFailureKind::SourceUnavailable => {
                    FailureCode::InputSourceUnavailable
                }
                InputPreparationFailureKind::StagingUnavailable => {
                    FailureCode::InputStagingUnavailable
                }
                InputPreparationFailureKind::LiveLimitExceeded => FailureCode::InputLiveLimit,
            };
            let mut detail = code(FailurePhase::Start, failure_code);
            detail.input = source.input_identity().map(str::to_owned);
            detail.collection_index = source.collection_index().map(|value| value as u64);
            detail
        }
        StepStartFailure::AgentInput(source) => agent_input_failure_detail(source),
        StepStartFailure::Agent(source) => agent_failure_detail(FailurePhase::Start, source),
        StepStartFailure::AgentRuntimeUnavailable => {
            code(FailurePhase::Start, FailureCode::AgentRuntimeUnavailable)
        }
        StepStartFailure::OutputsUnsupported => {
            code(FailurePhase::Start, FailureCode::OutputsUnsupported)
        }
        StepStartFailure::WorkingDirectory(source) => working_directory_detail(source),
        StepStartFailure::CommandPreparation(source) => code(
            FailurePhase::Start,
            match source {
                CommandPreparationFailure::InvalidArgv => FailureCode::CommandArgvInvalid,
                CommandPreparationFailure::PathNotConfigured => {
                    FailureCode::CommandPathUnconfigured
                }
                CommandPreparationFailure::ExecutableNotFound => FailureCode::ExecutableNotFound,
                CommandPreparationFailure::ExecutableUnavailable => {
                    FailureCode::ExecutableUnavailable
                }
            },
        ),
        StepStartFailure::CommandLaunch(source) => code(
            FailurePhase::Start,
            match source {
                CommandLaunchFailure::NotFound => FailureCode::CommandLaunchNotFound,
                CommandLaunchFailure::PermissionDenied => {
                    FailureCode::CommandLaunchPermissionDenied
                }
                CommandLaunchFailure::InvalidInput => FailureCode::CommandLaunchInvalidInput,
                CommandLaunchFailure::Other => FailureCode::CommandLaunchFailed,
            },
        ),
    };
    detail.validate()?;
    Ok(detail)
}

fn agent_input_failure_detail(source: &AgentInputStartFailure) -> FailureDetail {
    code(
        FailurePhase::Start,
        match source {
            AgentInputStartFailure::StepUnavailable => FailureCode::AgentStepUnavailable,
            AgentInputStartFailure::AgentAdmissionUnavailable => {
                FailureCode::AgentAdmissionUnavailable
            }
            AgentInputStartFailure::InputsUnavailable => FailureCode::AgentInputsUnavailable,
            AgentInputStartFailure::MissingUpstreamValue { .. } => {
                FailureCode::AgentInputMissingUpstream
            }
            AgentInputStartFailure::ValueTypeMismatch { .. } => FailureCode::AgentInputTypeMismatch,
            AgentInputStartFailure::RetainedSourceUnavailable { .. } => {
                FailureCode::AgentSourceUnavailable
            }
            AgentInputStartFailure::InvalidRetainedText { .. } => {
                FailureCode::AgentSourceTextInvalid
            }
            AgentInputStartFailure::ResultSchemaUnavailable { .. } => {
                FailureCode::AgentResultSchemaUnavailable
            }
            AgentInputStartFailure::InvalidValueMode => FailureCode::AgentValueModeInvalid,
            AgentInputStartFailure::AttachmentCountLimitExceeded { .. } => {
                FailureCode::AgentAttachmentCountLimit
            }
            AgentInputStartFailure::AttachmentBytesLimitExceeded { .. } => {
                FailureCode::AgentAttachmentBytesLimit
            }
            AgentInputStartFailure::WorkingDirectory(source) => {
                return working_directory_detail(source);
            }
            AgentInputStartFailure::ArtifactStagingMismatch => FailureCode::ArtifactStagingMismatch,
            AgentInputStartFailure::AgentStagingMismatch => FailureCode::AgentStagingMismatch,
            AgentInputStartFailure::StagingUnavailable => FailureCode::AgentInputStagingUnavailable,
        },
    )
}

fn working_directory_detail(source: &WorkingDirectoryFailure) -> FailureDetail {
    code(
        FailurePhase::Start,
        match source {
            WorkingDirectoryFailure::ExecutionRootRebound => FailureCode::ExecutionRootRebound,
            WorkingDirectoryFailure::Unavailable => FailureCode::WorkingDirectoryUnavailable,
            WorkingDirectoryFailure::EscapesExecutionRoot => FailureCode::WorkingDirectoryEscape,
            WorkingDirectoryFailure::NotDirectory => FailureCode::WorkingDirectoryNotDirectory,
        },
    )
}

fn agent_failure_detail(phase: FailurePhase, source: &AgentFailure) -> FailureDetail {
    code(
        phase,
        match source.cause() {
            AgentFailureCause::HarnessStartFailed
            | AgentFailureCause::HarnessSetupFailed { .. } => FailureCode::HarnessStartFailed,
            AgentFailureCause::HarnessInputTooLarge { .. } => FailureCode::HarnessInputTooLarge,
            AgentFailureCause::HarnessFailed { .. } => FailureCode::HarnessFailed,
            AgentFailureCause::HarnessProtocolFailed => FailureCode::HarnessProtocolFailed,
            AgentFailureCause::MissingResponse => FailureCode::MissingResponse,
            AgentFailureCause::MissingResult => FailureCode::MissingResult,
            AgentFailureCause::ResultValidationLimitExceeded { .. } => {
                FailureCode::ResultValidationLimitExceeded
            }
            AgentFailureCause::CapturedValueTooLarge => FailureCode::CapturedValueTooLarge,
            AgentFailureCause::ResultSettlementFailed => FailureCode::ResultSettlementFailed,
        },
    )
}

fn execution_failure_detail(source: &StepExecutionFailure) -> FailureDetail {
    match source {
        StepExecutionFailure::Command(CommandExecutionFailure::UnsuccessfulExit {
            code: status,
        }) => {
            let mut detail = code(FailurePhase::Execution, FailureCode::CommandExit);
            detail.exit_code = *status;
            detail
        }
        StepExecutionFailure::Command(CommandExecutionFailure::Wait) => {
            code(FailurePhase::Execution, FailureCode::CommandWaitFailed)
        }
        StepExecutionFailure::Agent(source) => {
            agent_failure_detail(FailurePhase::Execution, source)
        }
        StepExecutionFailure::TaskUnavailable => code(
            FailurePhase::Execution,
            FailureCode::ExecutionTaskUnavailable,
        ),
    }
}

fn output_capture_failure_detail(source: &OutputCaptureFailure) -> FailureDetail {
    let (failure_code, output) = match source {
        OutputCaptureFailure::StepUnavailable => (FailureCode::StepUnavailable, None),
        OutputCaptureFailure::UnsupportedOutput => (FailureCode::OutputUnsupported, None),
        OutputCaptureFailure::TaskUnavailable => (FailureCode::CaptureTaskUnavailable, None),
        OutputCaptureFailure::Capture(source) => (
            match source.kind() {
                CaptureFailureKind::AbsolutePath => FailureCode::OutputPathAbsolute,
                CaptureFailureKind::LexicalEscape => FailureCode::OutputPathEscape,
                CaptureFailureKind::EmptyPath => FailureCode::OutputPathEmpty,
                CaptureFailureKind::Missing => FailureCode::OutputMissing,
                CaptureFailureKind::SymbolicLink => FailureCode::OutputSymbolicLink,
                CaptureFailureKind::NotDirectory => FailureCode::OutputParentNotDirectory,
                CaptureFailureKind::NotRegularFile => FailureCode::OutputNotRegularFile,
                CaptureFailureKind::SourceUnavailable => FailureCode::OutputSourceUnavailable,
                CaptureFailureKind::InvalidTextEncoding => FailureCode::OutputInvalidUtf8,
                CaptureFailureKind::InvalidJson => FailureCode::OutputInvalidJson,
                CaptureFailureKind::DuplicateJsonMember => FailureCode::OutputDuplicateJsonMember,
                CaptureFailureKind::JsonSchemaMismatch => FailureCode::OutputJsonSchemaMismatch,
                CaptureFailureKind::FileCountLimitExceeded => FailureCode::CapturedFileCountLimit,
                CaptureFailureKind::FileSizeLimitExceeded => FailureCode::CapturedFileSizeLimit,
                CaptureFailureKind::TotalSizeLimitExceeded => FailureCode::CapturedTotalSizeLimit,
                CaptureFailureKind::GitCarrierCountLimitExceeded => {
                    FailureCode::CapturedGitCarrierCountLimit
                }
                CaptureFailureKind::GitCarrierSizeLimitExceeded => {
                    FailureCode::CapturedGitCarrierSizeLimit
                }
                CaptureFailureKind::TotalGitCarrierSizeLimitExceeded => {
                    FailureCode::CapturedTotalGitCarrierSizeLimit
                }
                CaptureFailureKind::CarrierProducerUnavailable => {
                    FailureCode::GitBundleGenerationFailed
                }
                CaptureFailureKind::StagingUnavailable => FailureCode::OutputStagingUnavailable,
            },
            Some(source.output_identity().to_owned()),
        ),
        OutputCaptureFailure::Git { output, failure } => (
            match failure {
                GitCaptureFailure::Cancelled
                | GitCaptureFailure::Artifact(_)
                | GitCaptureFailure::StagingMismatch => FailureCode::OutputStagingUnavailable,
                GitCaptureFailure::ExecutionRootRebound => FailureCode::GitExecutionRootRebound,
                GitCaptureFailure::HeadUnavailable => FailureCode::GitHeadUnavailable,
                GitCaptureFailure::BaselineNotAncestor => FailureCode::GitBaselineNotAncestor,
                GitCaptureFailure::CleanlinessUnavailable => FailureCode::GitCleanlinessUnavailable,
                GitCaptureFailure::WorkspaceDirty => FailureCode::GitWorkspaceDirty,
                GitCaptureFailure::TreeUnavailable => FailureCode::GitTreeUnavailable,
                GitCaptureFailure::RequiredObjectsUnavailable => {
                    FailureCode::GitRequiredObjectsUnavailable
                }
                GitCaptureFailure::SourceAuthorityChanged => FailureCode::GitSourceAuthorityChanged,
                GitCaptureFailure::GitStructureLimitExceeded => {
                    FailureCode::GitStructureLimitExceeded
                }
                GitCaptureFailure::CommandTimedOut(_) => FailureCode::GitCommandTimedOut,
                GitCaptureFailure::BundleGenerationFailed => FailureCode::GitBundleGenerationFailed,
                GitCaptureFailure::BundleProfileInvalid => FailureCode::GitBundleProfileInvalid,
                GitCaptureFailure::BundleVerificationFailed => {
                    FailureCode::GitBundleVerificationFailed
                }
                GitCaptureFailure::WorkspaceChanged => FailureCode::GitWorkspaceChanged,
                GitCaptureFailure::TemporaryStorageUnavailable => {
                    FailureCode::GitTemporaryStorageUnavailable
                }
            },
            Some(output.clone()),
        ),
    };
    let mut detail = code(FailurePhase::OutputCapture, failure_code);
    detail.output = output;
    detail
}

fn code(phase: FailurePhase, code: FailureCode) -> FailureDetail {
    FailureDetail::code(phase, code)
}

fn is_output_reference(value: &str) -> bool {
    let mut segments = value.split('.');
    matches!(
        (segments.next(), segments.next(), segments.next(), segments.next()),
        (Some("outputs"), Some(node), Some(output), None)
            if is_workflow_node_identifier(node) && is_workflow_value_identifier(output)
    )
}

fn is_workflow_node_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.first().is_some_and(u8::is_ascii_alphabetic)
        && bytes.len() <= 64
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn is_workflow_value_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.first().is_some_and(u8::is_ascii_lowercase)
        && bytes.len() <= 64
        && bytes[1..].iter().all(u8::is_ascii_alphanumeric)
}

pub(crate) fn deserialize_non_null_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn blocked_detail_deduplicates_and_sorts_exact_pairs() {
        let detail = BlockedDetail::new([
            Prerequisite::body("outputs.z.value").unwrap(),
            Prerequisite::control("z").unwrap(),
            Prerequisite::body("outputs.a.value").unwrap(),
            Prerequisite::control("z").unwrap(),
        ])
        .unwrap();
        assert_eq!(
            detail.prerequisites,
            [
                Prerequisite::Control {
                    node: "z".to_owned()
                },
                Prerequisite::Body {
                    r#ref: "outputs.a.value".to_owned()
                },
                Prerequisite::Body {
                    r#ref: "outputs.z.value".to_owned()
                },
            ]
        );
    }

    #[test]
    fn failure_detail_rejects_cross_phase_auxiliary_and_unsafe_names() {
        for value in [
            json!({"phase":"start","code":"command_exit","exitCode":1}),
            json!({"phase":"execution","code":"command_exit","output":"value"}),
            json!({"phase":"execution","code":"command_exit","exitCode":0}),
            json!({"phase":"start","code":"inputs_unavailable","input":null}),
            json!({"phase":"start","code":"inputs_unavailable","future":true}),
            json!({"phase":"start","code":"input_source_unavailable","input":"SECRET_PATH"}),
            json!({"phase":"output_capture","code":"output_missing","output":"../../secret"}),
        ] {
            assert!(serde_json::from_value::<FailureDetail>(value).is_err());
        }
        assert!(
            serde_json::from_value::<FailureDetail>(
                json!({"phase":"start","code":"input_invalid_name","input":"SECRET_PATH"})
            )
            .is_ok()
        );
    }

    #[test]
    fn prerequisite_references_require_safe_authored_identifiers() {
        for reference in [
            "outputs..value",
            "outputs.build.",
            "outputs.bad/path.value",
            "outputs.build.bad/path",
        ] {
            assert!(
                Prerequisite::body(reference).is_err(),
                "accepted {reference}"
            );
        }
        assert!(Prerequisite::body("outputs.Build_1.safeValue2").is_ok());
    }

    #[test]
    fn primary_issue_state_selects_the_closed_detail_member() {
        let failed = json!({
            "node":{"id":"build","role":"step"},
            "state":"failed",
            "detail":{"phase":"execution","code":"command_exit","exitCode":23}
        });
        assert!(serde_json::from_value::<PrimaryIssue>(failed).is_ok());
        let mismatched = json!({
            "node":{"id":"build","role":"step"},
            "state":"failed",
            "detail":{"code":"prerequisites_unsatisfied","prerequisites":[{"kind":"control","node":"lint"}]}
        });
        assert!(serde_json::from_value::<PrimaryIssue>(mismatched).is_err());
    }
}
