use std::fmt;
use std::os::unix::ffi::OsStrExt as _;

use serde::Serialize;

use super::DecodeFailureKind;
use super::admission::{AdmissionFailure, AdmissionFailureKind, AdmissionLocation};
use super::presentation_feed::normalize_terminal_scalar;
use super::resolution::{ResolutionFailure, ResolutionFailureKind, ResolutionLocation};
use super::validation::{ValidationFailureKind, ValidationLocation};
use crate::execution::AgentHarnessInstallationFailure;
use crate::execution::claude_code::{
    ClaudeCodeIncompatibility, ClaudeCodeInstallationFailure, ClaudeCodeProbe,
};
use crate::execution::codex::{CodexIncompatibility, CodexInstallationFailure, CodexProbe};
use crate::execution::pi::{PiIncompatibility, PiInstallationFailure, PiProbe};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct RejectionDiagnostic<'a> {
    pub(crate) code: &'static str,
    pub(crate) message: &'static str,
    pub(crate) location: RejectionLocation<'a>,
}

impl<'a> RejectionDiagnostic<'a> {
    pub(crate) fn from_resolution(failure: &'a ResolutionFailure) -> Self {
        let (code, message) = resolution_classification(failure.kind());
        Self {
            code,
            message,
            location: RejectionLocation::from_resolution(failure.location()),
        }
    }

    pub(crate) fn from_admission(failure: &'a AdmissionFailure) -> Option<Self> {
        let (code, message) = admission_classification(failure.kind())?;
        Some(Self {
            code,
            message,
            location: RejectionLocation::from_admission(failure.location())?,
        })
    }

    pub(crate) fn from_agent_harness_installation(
        failure: &'a AgentHarnessInstallationFailure,
    ) -> Self {
        let (code, message, profile, version, executable_path) = match failure {
            AgentHarnessInstallationFailure::Pi(failure) => {
                let (code, message) = pi_installation_classification(failure);
                (code, message, "PiJsonV1", None, None)
            }
            AgentHarnessInstallationFailure::ClaudeCode(failure) => {
                let (code, message) = claude_code_installation_classification(failure);
                (code, message, "ClaudeCodeStreamJsonV1", None, None)
            }
            AgentHarnessInstallationFailure::Codex(failure) => {
                let (code, message) = codex_installation_classification(failure);
                let identity = failure.identity();
                (
                    code,
                    message,
                    identity
                        .map(|identity| identity.profile().as_str())
                        .unwrap_or("CodexAppServerV1"),
                    identity.map(|identity| identity.version().as_str()),
                    identity.and_then(|identity| identity.executable().to_str()),
                )
            }
        };
        Self {
            code,
            message,
            location: RejectionLocation {
                profile: Some(profile),
                version,
                executable_path,
                ..RejectionLocation::simple("agent_harness")
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RejectionLocation<'a> {
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    step: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    finalizer: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    executable_path: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    export: Option<&'a str>,
}

impl<'a> RejectionLocation<'a> {
    fn simple(kind: &'static str) -> Self {
        Self {
            kind,
            step: None,
            finalizer: None,
            index: None,
            input: None,
            output: None,
            profile: None,
            version: None,
            executable_path: None,
            export: None,
        }
    }

    fn for_step(kind: &'static str, step: &'a str) -> Self {
        Self {
            step: Some(step),
            ..Self::simple(kind)
        }
    }

    fn for_finalizer(kind: &'static str, finalizer: &'a str) -> Self {
        Self {
            finalizer: Some(finalizer),
            ..Self::simple(kind)
        }
    }

    fn from_resolution(location: &'a ResolutionLocation) -> Self {
        match location {
            ResolutionLocation::SourceRoot => Self::simple("source_root"),
            ResolutionLocation::Workflow => Self::simple("workflow"),
            ResolutionLocation::Semantic(location) => Self::from_validation(location),
            ResolutionLocation::SystemPrompt { step } => Self::for_step("system_prompt", step),
            ResolutionLocation::MessageText { step, index } => Self {
                index: Some(*index),
                ..Self::for_step("message_text", step)
            },
            ResolutionLocation::MessageAttachment { step, index } => Self {
                index: Some(*index),
                ..Self::for_step("message_attachment", step)
            },
            ResolutionLocation::ResultSchema { step, output } => Self {
                output: Some(output),
                ..Self::for_step("result_schema", step)
            },
            ResolutionLocation::FinalizerSystemPrompt { finalizer } => {
                Self::for_finalizer("system_prompt", finalizer)
            }
            ResolutionLocation::FinalizerMessageText { finalizer, index } => Self {
                index: Some(*index),
                ..Self::for_finalizer("message_text", finalizer)
            },
            ResolutionLocation::FinalizerMessageAttachment { finalizer, index } => Self {
                index: Some(*index),
                ..Self::for_finalizer("message_attachment", finalizer)
            },
            ResolutionLocation::FinalizerResultSchema { finalizer, output } => Self {
                output: Some(output),
                ..Self::for_finalizer("result_schema", finalizer)
            },
            ResolutionLocation::RecoveryPrompt { step } => Self::for_step("recovery_prompt", step),
            ResolutionLocation::ContentDigest => Self::simple("content_digest"),
            ResolutionLocation::Capacity => Self::simple("capacity"),
        }
    }

    fn from_validation(location: &'a ValidationLocation) -> Self {
        match location {
            ValidationLocation::WorkflowGraph => Self::simple("workflow_graph"),
            ValidationLocation::WorkflowNamespace => Self::simple("workflow_namespace"),
            ValidationLocation::AgentProfile { profile } => Self {
                profile: Some(profile),
                ..Self::simple("agent_profile")
            },
            ValidationLocation::AgentProfileReference { step } => {
                Self::for_step("agent_profile_reference", step)
            }
            ValidationLocation::RecoveryAgentProfileReference { step } => {
                Self::for_step("recovery_agent_profile_reference", step)
            }
            ValidationLocation::FinalizerAgentProfileReference { finalizer } => {
                Self::for_finalizer("agent_profile_reference", finalizer)
            }
            ValidationLocation::StepDependency { step, index } => Self {
                index: Some(*index),
                ..Self::for_step("step_dependency", step)
            },
            ValidationLocation::StepInput { step, input } => Self {
                input: Some(input),
                ..Self::for_step("step_input", step)
            },
            ValidationLocation::MessageText { step, index } => Self {
                index: Some(*index),
                ..Self::for_step("message_text", step)
            },
            ValidationLocation::MessageAttachment { step, index } => Self {
                index: Some(*index),
                ..Self::for_step("message_attachment", step)
            },
            ValidationLocation::StepOutput { step, output } => Self {
                output: Some(output),
                ..Self::for_step("step_output", step)
            },
            ValidationLocation::FinalizerAfter { finalizer, index } => Self {
                index: Some(*index),
                ..Self::for_finalizer("finalizer_after", finalizer)
            },
            ValidationLocation::FinalizerInput { finalizer, input } => Self {
                input: Some(input),
                ..Self::for_finalizer("finalizer_input", finalizer)
            },
            ValidationLocation::FinalizerMessageText { finalizer, index } => Self {
                index: Some(*index),
                ..Self::for_finalizer("finalizer_message_text", finalizer)
            },
            ValidationLocation::FinalizerMessageAttachment { finalizer, index } => Self {
                index: Some(*index),
                ..Self::for_finalizer("finalizer_message_attachment", finalizer)
            },
            ValidationLocation::FinalizerOutput { finalizer, output } => Self {
                output: Some(output),
                ..Self::for_finalizer("finalizer_output", finalizer)
            },
            ValidationLocation::Export { name } => Self {
                export: Some(name),
                ..Self::simple("export")
            },
        }
    }

    fn from_admission(location: &'a AdmissionLocation) -> Option<Self> {
        match location {
            AdmissionLocation::Workflow => Some(Self::simple("workflow")),
            AdmissionLocation::PromptImport => Some(Self::simple("prompt_import")),
            AdmissionLocation::AttachmentImport { index } => Some(Self {
                index: Some(*index),
                ..Self::simple("attachment_import")
            }),
            AdmissionLocation::Step { step } => Some(Self::for_step("step", step)),
            AdmissionLocation::RecoveryHandler { step } => {
                Some(Self::for_step("recovery_handler", step))
            }
            AdmissionLocation::ExecutionRoot => Some(Self::simple("execution_root")),
            AdmissionLocation::GitContext => Some(Self::simple("git_context")),
            AdmissionLocation::MaximumParallelSteps
            | AdmissionLocation::MaximumCapturedFiles
            | AdmissionLocation::MaximumCapturedFileBytes
            | AdmissionLocation::MaximumTotalCapturedBytes
            | AdmissionLocation::MaximumCapturedGitCarriers
            | AdmissionLocation::MaximumCapturedGitCarrierBytes
            | AdmissionLocation::MaximumTotalCapturedGitCarrierBytes
            | AdmissionLocation::MaximumInputValues
            | AdmissionLocation::MaximumInputValueBytes
            | AdmissionLocation::MaximumTotalInputBytes
            | AdmissionLocation::MaximumLiveInputBytes
            | AdmissionLocation::MaximumStepLogBytes
            | AdmissionLocation::CancellationPolicy
            | AdmissionLocation::CapacitySourceBinding
            | AdmissionLocation::MaximumInvocations
            | AdmissionLocation::DiagnosticRetention
            | AdmissionLocation::NativeSessionRetention
            | AdmissionLocation::AggregateRetention
            | AdmissionLocation::EncodedOutbox => None,
        }
    }
}

impl RejectionLocation<'_> {
    fn write_node(&self, formatter: &mut fmt::Formatter<'_>, role: &str, id: &str) -> fmt::Result {
        write!(formatter, " ({role} {id}")?;
        if let Some(index) = self.index {
            write!(formatter, ", index {index}")?;
        }
        if let Some(input) = self.input {
            write!(formatter, ", input {input}")?;
        }
        if let Some(output) = self.output {
            write!(formatter, ", output {output}")?;
        }
        formatter.write_str(")")
    }
}

impl fmt::Display for RejectionLocation<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind)?;
        if let Some(step) = self.step {
            self.write_node(formatter, "step", step)?;
        } else if let Some(finalizer) = self.finalizer {
            self.write_node(formatter, "finalizer", finalizer)?;
        } else if let Some(index) = self.index {
            write!(formatter, " (index {index})")?;
        } else if let Some(profile) = self.profile {
            write!(formatter, " ({profile}")?;
            if let Some(version) = self.version {
                write!(formatter, ", version {version}")?;
            }
            if let Some(executable) = self.executable_path {
                write!(
                    formatter,
                    ", executable {}",
                    normalize_terminal_scalar(executable.as_bytes())
                )?;
            }
            formatter.write_str(")")?;
        } else if let Some(name) = self.export {
            write!(formatter, " ({name})")?;
        }
        Ok(())
    }
}

pub(crate) fn human_resolution_remedy(failure: &ResolutionFailure) -> String {
    let default = resolution_classification(failure.kind()).1;
    let Some(source_path) = failure.source_path() else {
        return default.to_owned();
    };
    let source_path = normalize_terminal_scalar(source_path.as_os_str().as_bytes());
    match failure.kind() {
        ResolutionFailureKind::SourceUnavailable => {
            format!("Add or make readable the required workflow source file:\n  {source_path}")
        }
        ResolutionFailureKind::SourceNotRegularFile => {
            format!("Replace the workflow source with a regular file:\n  {source_path}")
        }
        _ => default.to_owned(),
    }
}

fn resolution_classification(kind: ResolutionFailureKind) -> (&'static str, &'static str) {
    match kind {
        ResolutionFailureKind::SourceRootUnavailable => (
            "source_root_unavailable",
            "Choose an existing, readable directory for --source-root.",
        ),
        ResolutionFailureKind::SourceRootNotDirectory => (
            "source_root_not_directory",
            "The --source-root value must identify a directory.",
        ),
        ResolutionFailureKind::LexicalSourceEscape => (
            "source_path_escape",
            "Keep the selected workflow and every static source path within --source-root.",
        ),
        ResolutionFailureKind::SourceUnavailable => (
            "source_unavailable",
            "Add or make readable the required workflow source file at this location.",
        ),
        ResolutionFailureKind::SymbolicLinkEscape => (
            "symbolic_link_escape",
            "Remove the symbolic link that resolves outside --source-root.",
        ),
        ResolutionFailureKind::SourceNotRegularFile => (
            "source_not_regular_file",
            "Replace the source at this location with a regular file.",
        ),
        ResolutionFailureKind::InvalidCanonicalPath => (
            "invalid_canonical_path",
            "Use UTF-8 source path components that normalize within --source-root.",
        ),
        ResolutionFailureKind::SourceChangedDuringResolution => (
            "source_changed",
            "Retry validation after workflow source files stop changing.",
        ),
        ResolutionFailureKind::InvalidWorkflowDocument(kind) => decode_classification(kind),
        ResolutionFailureKind::InvalidWorkflowDefinition(kind) => validation_classification(kind),
        ResolutionFailureKind::InvalidTextEncoding => (
            "invalid_text_encoding",
            "Encode the system prompt or message text source as UTF-8.",
        ),
        ResolutionFailureKind::InvalidResultSchemaEncoding => (
            "invalid_result_schema_encoding",
            "Encode the agent result schema as UTF-8 JSON.",
        ),
        ResolutionFailureKind::InvalidResultSchemaJson => (
            "invalid_result_schema_json",
            "Provide a well-formed JSON document for the agent result schema.",
        ),
        ResolutionFailureKind::InvalidResultSchemaDialect => (
            "invalid_result_schema_dialect",
            "Use one Draft 2020-12 schema resource without authored vocabularies.",
        ),
        ResolutionFailureKind::InvalidResultSchemaReference => (
            "invalid_result_schema_reference",
            "Keep result-schema references within one self-contained schema resource.",
        ),
        ResolutionFailureKind::InvalidResultSchema => (
            "invalid_result_schema",
            "Correct the agent result schema so it is a valid Draft 2020-12 schema.",
        ),
        ResolutionFailureKind::DigestInputTooLarge => (
            "source_closure_too_large",
            "Reduce the total size of the workflow definition and its static source files.",
        ),
        ResolutionFailureKind::CapacityArithmeticOverflow => (
            "workflow_capacity_overflow",
            "Reduce the workflow size or configured recovery rounds.",
        ),
        ResolutionFailureKind::GeneralTransitionCapacityExceeded => (
            "workflow_transition_capacity_exceeded",
            "Reduce the workflow size or configured recovery rounds.",
        ),
        ResolutionFailureKind::CloudTransitionCapacityExceeded => (
            "cloud_transition_capacity_exceeded",
            "Reduce the workflow size or configured recovery rounds.",
        ),
    }
}

fn decode_classification(kind: DecodeFailureKind) -> (&'static str, &'static str) {
    match kind {
        DecodeFailureKind::MalformedYaml => (
            "malformed_yaml",
            "Correct the workflow so it is one well-formed YAML document.",
        ),
        DecodeFailureKind::ForbiddenYaml => (
            "forbidden_yaml",
            "Remove forbidden YAML features such as aliases, custom tags, or duplicate keys.",
        ),
        DecodeFailureKind::StructuralContract => (
            "invalid_workflow_structure",
            "Correct the workflow fields and values to match the workflow contract.",
        ),
    }
}

fn validation_classification(kind: ValidationFailureKind) -> (&'static str, &'static str) {
    match kind {
        ValidationFailureKind::MissingDependency => (
            "missing_dependency",
            "Declare the referenced dependency step or correct its name.",
        ),
        ValidationFailureKind::SelfDependency => (
            "self_dependency",
            "Remove the step's dependency on or output reference to itself.",
        ),
        ValidationFailureKind::DuplicateDependency => (
            "duplicate_dependency",
            "List each direct step dependency only once.",
        ),
        ValidationFailureKind::DependencyCycle => (
            "dependency_cycle",
            "Remove dependency edges until the workflow step graph is acyclic.",
        ),
        ValidationFailureKind::InvalidAgentProfileConfig => (
            "invalid_agent_profile_config",
            "Correct the agent profile's harness configuration.",
        ),
        ValidationFailureKind::UnknownAgentProfile => (
            "unknown_agent_profile",
            "Reference an agent profile declared by this workflow.",
        ),
        ValidationFailureKind::UnknownImport => (
            "unknown_import",
            "Use a workflow import name or correct the value reference.",
        ),
        ValidationFailureKind::UnknownOutputStep => (
            "unknown_output_step",
            "Reference an output from a declared workflow step.",
        ),
        ValidationFailureKind::UnknownOutput => (
            "unknown_output",
            "Reference an output declared by the producing step.",
        ),
        ValidationFailureKind::MessageTypeMismatch => (
            "message_type_mismatch",
            "Reference a value type accepted by this message destination.",
        ),
        ValidationFailureKind::TerminalOutputReference => (
            "terminal_output_reference",
            "Export git_branch directly; it cannot bind a downstream step value.",
        ),
        ValidationFailureKind::IllegalCommandOutput => (
            "illegal_command_output",
            "Declare only file or git_branch outputs on command steps.",
        ),
        ValidationFailureKind::ExcessAgentResponseOutput => (
            "excess_agent_response_output",
            "Declare at most one agent response output on a step.",
        ),
        ValidationFailureKind::ExcessAgentResultOutput => (
            "excess_agent_result_output",
            "Declare at most one agent result output on a step.",
        ),
        ValidationFailureKind::ConflictingAgentValueOutputs => (
            "conflicting_agent_value_outputs",
            "Declare either one agent response output or one agent result output, not both.",
        ),
        ValidationFailureKind::AdvisoryDataDependency => (
            "advisory_data_dependency",
            "Make the data consumer advisory or use a required output producer.",
        ),
        ValidationFailureKind::InvalidExportTarget => (
            "invalid_export_target",
            "Reference a declared step output from the workflow export.",
        ),
        ValidationFailureKind::AdvisoryExportTarget => (
            "advisory_export_target",
            "Export an output from a required workflow node.",
        ),
        ValidationFailureKind::TooManyNodes => (
            "too_many_workflow_nodes",
            "Reduce the combined number of steps and finalizers to 256.",
        ),
        ValidationFailureKind::DuplicateNodeId => (
            "duplicate_workflow_node_id",
            "Give every step and finalizer a globally unique ID.",
        ),
        ValidationFailureKind::InvalidSourceOrder => (
            "invalid_workflow_source_order",
            "Declare every workflow node exactly once.",
        ),
        ValidationFailureKind::CrossPhaseOutputReference => (
            "cross_phase_output_reference",
            "Ordinary steps cannot reference finalizer outputs.",
        ),
        ValidationFailureKind::InvalidFinalizerAfterTarget => (
            "invalid_finalizer_after_target",
            "Reference only declared finalizers from after.",
        ),
        ValidationFailureKind::InvalidFinalizerTrigger => (
            "invalid_finalizer_trigger",
            "Select at least one finalization trigger.",
        ),
        ValidationFailureKind::IncompatibleFinalizerTriggers => (
            "incompatible_finalizer_triggers",
            "Make the consumer trigger set a subset of its output producer's trigger set.",
        ),
        ValidationFailureKind::InvalidFinalizationContext => (
            "invalid_finalization_context",
            "Reference finalization.context only from a finalizer command input or agent attachment.",
        ),
        ValidationFailureKind::FinalizerExportTrigger => (
            "finalizer_export_trigger",
            "Export from a required finalizer that is eligible after ordinary success.",
        ),
    }
}

fn pi_installation_classification(failure: &PiInstallationFailure) -> (&'static str, &'static str) {
    match failure {
        PiInstallationFailure::Missing => (
            "missing_pi_installation",
            "Install a supported `pi` executable in the inherited PATH.",
        ),
        PiInstallationFailure::Unexecutable { .. } => (
            "unexecutable_pi_installation",
            "The selected `pi` executable could not complete its validation probes.",
        ),
        PiInstallationFailure::Malformed {
            probe: PiProbe::Version,
            ..
        } => (
            "malformed_pi_version",
            "The selected `pi` executable returned a malformed version.",
        ),
        PiInstallationFailure::Malformed {
            probe: PiProbe::Capabilities,
            ..
        } => (
            "malformed_pi_capabilities",
            "The selected `pi` executable returned malformed capability help.",
        ),
        PiInstallationFailure::Unsupported(PiIncompatibility::Version(_)) => (
            "unsupported_pi_version",
            "The selected `pi` version is outside the supported PiJsonV1 range.",
        ),
        PiInstallationFailure::Unsupported(PiIncompatibility::Capability { .. }) => (
            "unsupported_pi_capability",
            "The selected `pi` executable lacks a capability required by PiJsonV1.",
        ),
    }
}

fn claude_code_installation_classification(
    failure: &ClaudeCodeInstallationFailure,
) -> (&'static str, &'static str) {
    match failure {
        ClaudeCodeInstallationFailure::Missing => (
            "missing_claude_code_installation",
            "Install a supported stable `claude` executable in the inherited PATH.",
        ),
        ClaudeCodeInstallationFailure::Unexecutable { .. } => (
            "unexecutable_claude_code_installation",
            "The selected `claude` executable could not complete its validation probes.",
        ),
        ClaudeCodeInstallationFailure::Malformed {
            probe: ClaudeCodeProbe::Version,
            ..
        } => (
            "malformed_claude_code_version",
            "The selected `claude` executable returned a malformed version.",
        ),
        ClaudeCodeInstallationFailure::Malformed {
            probe: ClaudeCodeProbe::Capabilities,
            ..
        } => (
            "malformed_claude_code_capabilities",
            "The selected `claude` executable returned malformed capability help.",
        ),
        ClaudeCodeInstallationFailure::Unsupported(ClaudeCodeIncompatibility::Version(_)) => (
            "unsupported_claude_code_version",
            "The selected `claude` version is outside the supported ClaudeCodeStreamJsonV1 range.",
        ),
        ClaudeCodeInstallationFailure::Unsupported(ClaudeCodeIncompatibility::Capability {
            ..
        }) => (
            "unsupported_claude_code_capability",
            "The selected `claude` executable lacks a capability required by ClaudeCodeStreamJsonV1.",
        ),
    }
}

fn codex_installation_classification(
    failure: &CodexInstallationFailure,
) -> (&'static str, &'static str) {
    match failure {
        CodexInstallationFailure::Missing => (
            "missing_codex_installation",
            "Install a supported stable `codex` executable in the inherited PATH.",
        ),
        CodexInstallationFailure::Unexecutable { .. } => (
            "unexecutable_codex_installation",
            "The selected `codex` executable could not complete its validation probes.",
        ),
        CodexInstallationFailure::Malformed {
            probe: CodexProbe::Version,
            ..
        } => (
            "malformed_codex_version",
            "The selected `codex` executable returned a malformed stable version.",
        ),
        CodexInstallationFailure::Malformed {
            probe: CodexProbe::AppServerSchema,
            ..
        } => (
            "malformed_codex_app_server_schema",
            "The selected `codex` executable returned malformed App Server schemas.",
        ),
        CodexInstallationFailure::Unsupported {
            incompatibility: CodexIncompatibility::Version(_),
            ..
        } => (
            "unsupported_codex_version",
            "The selected `codex` version is outside the CodexAppServerV1 release line.",
        ),
        CodexInstallationFailure::Unsupported {
            incompatibility: CodexIncompatibility::Capability(_),
            ..
        } => (
            "unsupported_codex_capability",
            "The selected `codex` executable lacks the App Server schema required by CodexAppServerV1.",
        ),
    }
}

fn admission_classification(kind: AdmissionFailureKind) -> Option<(&'static str, &'static str)> {
    match kind {
        AdmissionFailureKind::MissingRequiredPrompt => Some((
            "missing_required_prompt",
            "Supply --prompt-file because this workflow requires imports.prompt.",
        )),
        AdmissionFailureKind::InvalidAttachmentMediaType => Some((
            "invalid_attachment_media_type",
            "Supply a syntactically valid media type for this attachment.",
        )),
        AdmissionFailureKind::AgentStepRuntimeUnsupported => Some((
            "agent_step_runtime_unsupported",
            "Supply the validated installation required by this agent step.",
        )),
        AdmissionFailureKind::ExecutionRootUnavailable => Some((
            "execution_root_unavailable",
            "Choose an existing, readable execution root.",
        )),
        AdmissionFailureKind::ExecutionRootNotDirectory => Some((
            "execution_root_not_directory",
            "The execution root must identify a directory.",
        )),
        AdmissionFailureKind::GitContextRequired => Some((
            "git_context_required",
            "Use an execution adapter that supplies Git branch capture context.",
        )),
        AdmissionFailureKind::GitContextUnavailable => Some((
            "git_context_unavailable",
            "Make Git available before running a workflow with git_branch output.",
        )),
        AdmissionFailureKind::GitContextNotRepository => Some((
            "git_context_not_repository",
            "Use a Git worktree root as the execution root.",
        )),
        AdmissionFailureKind::GitContextExecutionRootMismatch => Some((
            "git_context_execution_root_mismatch",
            "Bind the execution root to exactly one Git worktree root.",
        )),
        AdmissionFailureKind::GitObjectFormatUnsupported => Some((
            "git_object_format_unsupported",
            "Use a SHA-1 Git repository for git_branch output.",
        )),
        AdmissionFailureKind::GitBaselineUnavailable => Some((
            "git_baseline_unavailable",
            "Check out a readable baseline commit before running the workflow.",
        )),
        AdmissionFailureKind::GitInitialWorkspaceDirty
        | AdmissionFailureKind::GitWorkflowDigestMismatch
        | AdmissionFailureKind::NonPositiveParallelism
        | AdmissionFailureKind::NonPositiveCapturedFiles
        | AdmissionFailureKind::NonPositiveCapturedFileBytes
        | AdmissionFailureKind::NonPositiveTotalCapturedBytes
        | AdmissionFailureKind::NonPositiveCapturedGitCarriers
        | AdmissionFailureKind::NonPositiveCapturedGitCarrierBytes
        | AdmissionFailureKind::NonPositiveTotalCapturedGitCarrierBytes
        | AdmissionFailureKind::NonPositiveInputValues
        | AdmissionFailureKind::NonPositiveInputValueBytes
        | AdmissionFailureKind::NonPositiveTotalInputBytes
        | AdmissionFailureKind::NonPositiveLiveInputBytes
        | AdmissionFailureKind::NonPositiveStepLogBytes
        | AdmissionFailureKind::CancellationGraceTooShort
        | AdmissionFailureKind::CancellationGraceTooLong
        | AdmissionFailureKind::CapacitySourceBindingMismatch
        | AdmissionFailureKind::InvocationCapacityUnavailable
        | AdmissionFailureKind::DiagnosticRetentionCapacityUnavailable
        | AdmissionFailureKind::NativeSessionRetentionCapacityUnavailable
        | AdmissionFailureKind::AggregateRetentionCapacityUnavailable
        | AdmissionFailureKind::EncodedOutboxCapacityUnavailable => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_installation_rejection_uses_the_closed_profile_identity() {
        let failure =
            AgentHarnessInstallationFailure::Codex(CodexInstallationFailure::Unsupported {
                incompatibility: CodexIncompatibility::Version("0.150.0".to_owned()),
                identity: None,
            });

        let diagnostic = RejectionDiagnostic::from_agent_harness_installation(&failure);

        assert_eq!(diagnostic.code, "unsupported_codex_version");
        assert_eq!(diagnostic.location.kind, "agent_harness");
        assert_eq!(diagnostic.location.profile, Some("CodexAppServerV1"));
        assert_eq!(diagnostic.location.version, None);
        assert_eq!(diagnostic.location.executable_path, None);
    }

    #[test]
    fn codex_capability_rejection_retains_the_probed_identity() {
        use std::path::Path;

        use crate::execution::codex::{
            CodexCapability, CodexCompatibilityProfile, CodexInstallationIdentity, CodexVersion,
        };

        let identity = CodexInstallationIdentity::new(
            Path::new("/canonical/codex"),
            &CodexVersion::parse("0.147.23").unwrap(),
            CodexCompatibilityProfile::CodexAppServerV1,
        );
        let failure =
            AgentHarnessInstallationFailure::Codex(CodexInstallationFailure::Unsupported {
                incompatibility: CodexIncompatibility::Capability(
                    CodexCapability::AppServerSchemaV1,
                ),
                identity: Some(identity),
            });

        let diagnostic = RejectionDiagnostic::from_agent_harness_installation(&failure);

        assert_eq!(diagnostic.code, "unsupported_codex_capability");
        assert_eq!(
            serde_json::to_value(&diagnostic.location).unwrap(),
            serde_json::json!({
                "kind": "agent_harness",
                "profile": "CodexAppServerV1",
                "version": "0.147.23",
                "executablePath": "/canonical/codex",
            })
        );
    }
}
