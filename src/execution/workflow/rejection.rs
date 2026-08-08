use std::fmt;

use serde::Serialize;

use super::DecodeFailureKind;
use super::admission::{AdmissionFailure, AdmissionFailureKind, AdmissionLocation};
use super::resolution::{ResolutionFailure, ResolutionFailureKind, ResolutionLocation};
use super::validation::{ValidationFailureKind, ValidationLocation};
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

    pub(crate) fn from_pi_installation(failure: &PiInstallationFailure) -> Self {
        let (code, message) = pi_installation_classification(failure);
        Self {
            code,
            message,
            location: RejectionLocation {
                profile: Some("PiJsonV1"),
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
    index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    export: Option<&'a str>,
}

impl<'a> RejectionLocation<'a> {
    fn simple(kind: &'static str) -> Self {
        Self {
            kind,
            step: None,
            index: None,
            input: None,
            output: None,
            profile: None,
            export: None,
        }
    }

    fn for_step(kind: &'static str, step: &'a str) -> Self {
        Self {
            step: Some(step),
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
            ResolutionLocation::ContentDigest => Self::simple("content_digest"),
        }
    }

    fn from_validation(location: &'a ValidationLocation) -> Self {
        match location {
            ValidationLocation::WorkflowGraph => Self::simple("workflow_graph"),
            ValidationLocation::AgentProfile { profile } => Self {
                profile: Some(profile),
                ..Self::simple("agent_profile")
            },
            ValidationLocation::AgentProfileReference { step } => {
                Self::for_step("agent_profile_reference", step)
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
            ValidationLocation::Export { name } => Self {
                export: Some(name),
                ..Self::simple("export")
            },
        }
    }

    fn from_admission(location: &'a AdmissionLocation) -> Option<Self> {
        match location {
            AdmissionLocation::PromptImport => Some(Self::simple("prompt_import")),
            AdmissionLocation::AttachmentImport { index } => Some(Self {
                index: Some(*index),
                ..Self::simple("attachment_import")
            }),
            AdmissionLocation::Step { step } => Some(Self::for_step("step", step)),
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
            | AdmissionLocation::CancellationPolicy => None,
        }
    }
}

impl fmt::Display for RejectionLocation<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind)?;
        if let Some(step) = self.step {
            write!(formatter, " (step {step}")?;
            if let Some(index) = self.index {
                write!(formatter, ", index {index}")?;
            }
            if let Some(input) = self.input {
                write!(formatter, ", input {input}")?;
            }
            if let Some(output) = self.output {
                write!(formatter, ", output {output}")?;
            }
            formatter.write_str(")")?;
        } else if let Some(index) = self.index {
            write!(formatter, " (index {index})")?;
        } else if let Some(profile) = self.profile {
            write!(formatter, " ({profile})")?;
        } else if let Some(name) = self.export {
            write!(formatter, " ({name})")?;
        }
        Ok(())
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
            "Correct the workflow fields and values to match the closed Workflow V1 contract.",
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
            "Correct the agent profile's Pi configuration.",
        ),
        ValidationFailureKind::UnknownAgentProfile => (
            "unknown_agent_profile",
            "Reference an agent profile declared by this workflow.",
        ),
        ValidationFailureKind::UnknownImport => (
            "unknown_import",
            "Use a Workflow V1 import name or correct the value reference.",
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
        ValidationFailureKind::InvalidExportTarget => (
            "invalid_export_target",
            "Reference a declared step output from the workflow export.",
        ),
    }
}

fn pi_installation_classification(failure: &PiInstallationFailure) -> (&'static str, &'static str) {
    match failure {
        PiInstallationFailure::Missing => (
            "missing_pi_installation",
            "Install a supported `pi` executable in the inherited PATH.",
        ),
        PiInstallationFailure::Unexecutable => (
            "unexecutable_pi_installation",
            "The selected `pi` executable could not complete its validation probes.",
        ),
        PiInstallationFailure::Malformed(PiProbe::Version) => (
            "malformed_pi_version",
            "The selected `pi` executable returned a malformed version.",
        ),
        PiInstallationFailure::Malformed(PiProbe::Capabilities) => (
            "malformed_pi_capabilities",
            "The selected `pi` executable returned malformed capability help.",
        ),
        PiInstallationFailure::Unsupported(PiIncompatibility::Version(_)) => (
            "unsupported_pi_version",
            "The selected `pi` version is outside the supported PiJsonV1 range.",
        ),
        PiInstallationFailure::Unsupported(PiIncompatibility::Capability(_)) => (
            "unsupported_pi_capability",
            "The selected `pi` executable lacks a capability required by PiJsonV1.",
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
            "Use a command-only workflow with this runtime.",
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
        AdmissionFailureKind::NonPositiveParallelism
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
        | AdmissionFailureKind::NonPositiveCancellationGrace
        | AdmissionFailureKind::CancellationGraceTooLong => None,
    }
}
