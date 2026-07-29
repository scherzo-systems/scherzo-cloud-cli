use std::fmt;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Args;
use serde::Serialize;

use crate::execution::workflow::DecodeFailureKind;
use crate::execution::workflow::resolution::{
    ResolutionFailure, ResolutionFailureKind, ResolutionLocation, ResolvedWorkflow, resolve,
};
use crate::execution::workflow::validation::{ValidationFailureKind, ValidationLocation};

pub(super) const ABOUT: &str = "Validate a local Workflow V1 bundle without executing it";
const COMMAND_NAME: &str = "scherzo-cloud workflow validate";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(
        long,
        value_name = "ROOT",
        help = "Explicit directory boundary for workflow source files"
    )]
    source_root: PathBuf,

    #[arg(
        value_name = "WORKFLOW_PATH",
        help = "Workflow YAML path selected within the source root"
    )]
    workflow_path: PathBuf,

    #[arg(long, help = "Print the schema-version-1 validation result as JSON")]
    json: bool,
}

impl Command {
    pub(super) fn execute(self) -> ExitCode {
        match resolve(&self.source_root, &self.workflow_path) {
            Ok(workflow) => {
                let result = if self.json {
                    write_json_valid(&workflow)
                } else {
                    write_human_valid(&workflow)
                };
                finish_output(result, ExitCode::SUCCESS)
            }
            Err(failure) => {
                let result = if self.json {
                    write_json_invalid(&failure)
                } else {
                    write_human_invalid(&failure)
                };
                finish_output(result, ExitCode::FAILURE)
            }
        }
    }
}

fn finish_output(result: Result<(), OutputError>, exit_code: ExitCode) -> ExitCode {
    match result {
        Ok(()) => exit_code,
        Err(error) => {
            eprintln!("Error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn write_human_valid(workflow: &ResolvedWorkflow) -> Result<(), OutputError> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✓ Workflow V1 definition is valid.")?;
    writeln!(stdout, "Workflow: {}", workflow.source.workflow_path)?;
    writeln!(
        stdout,
        "Digest: {}:{}",
        workflow.content_digest.algorithm.as_str(),
        workflow.content_digest.value
    )?;
    writeln!(stdout, "Steps: {}", workflow.definition.steps.len())?;
    writeln!(
        stdout,
        "Required optional imports: {}",
        human_required_imports(workflow)
    )?;
    writeln!(stdout, "No workflow steps were executed.")?;
    Ok(())
}

fn write_human_invalid(failure: &ResolutionFailure) -> Result<(), OutputError> {
    let diagnostic = Diagnostic::from_failure(failure);
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✗ Workflow V1 definition is invalid.")?;
    if let Some(workflow_path) = failure.workflow_path() {
        writeln!(stdout, "Workflow: {workflow_path}")?;
    }
    writeln!(stdout, "Code: {}", diagnostic.code)?;
    writeln!(stdout, "Location: {}", diagnostic.location)?;
    writeln!(stdout, "{}", diagnostic.message)?;
    writeln!(stdout, "No workflow steps were executed.")?;
    Ok(())
}

fn human_required_imports(workflow: &ResolvedWorkflow) -> &'static str {
    if workflow.required_imports().prompt {
        "prompt"
    } else {
        "none"
    }
}

fn write_json_valid(workflow: &ResolvedWorkflow) -> Result<(), OutputError> {
    let required_imports = if workflow.required_imports().prompt {
        vec!["prompt"]
    } else {
        Vec::new()
    };
    let report = JsonReport {
        schema_version: 1,
        command: COMMAND_NAME,
        result: JsonResult::Valid {
            workflow: WorkflowIdentity {
                path: &workflow.source.workflow_path,
            },
            digest: JsonDigest {
                algorithm: workflow.content_digest.algorithm.as_str(),
                value: &workflow.content_digest.value,
            },
            step_count: workflow.definition.steps.len(),
            required_imports,
        },
    };
    write_json(&report)
}

fn write_json_invalid(failure: &ResolutionFailure) -> Result<(), OutputError> {
    let report = JsonReport {
        schema_version: 1,
        command: COMMAND_NAME,
        result: JsonResult::Invalid {
            workflow: failure
                .workflow_path()
                .map(|path| WorkflowIdentity { path }),
            diagnostics: [Diagnostic::from_failure(failure)],
        },
    };
    write_json(&report)
}

fn write_json(report: &JsonReport<'_>) -> Result<(), OutputError> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, report).map_err(OutputError::WriteJson)?;
    writeln!(stdout)?;
    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonReport<'a> {
    schema_version: u8,
    command: &'static str,
    #[serde(flatten)]
    result: JsonResult<'a>,
}

#[derive(Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum JsonResult<'a> {
    Valid {
        workflow: WorkflowIdentity<'a>,
        digest: JsonDigest<'a>,
        #[serde(rename = "stepCount")]
        step_count: usize,
        #[serde(rename = "requiredImports")]
        required_imports: Vec<&'static str>,
    },
    Invalid {
        workflow: Option<WorkflowIdentity<'a>>,
        diagnostics: [Diagnostic<'a>; 1],
    },
}

#[derive(Serialize)]
struct WorkflowIdentity<'a> {
    path: &'a str,
}

#[derive(Serialize)]
struct JsonDigest<'a> {
    algorithm: &'static str,
    value: &'a str,
}

#[derive(Serialize)]
struct Diagnostic<'a> {
    code: &'static str,
    message: &'static str,
    location: DiagnosticLocation<'a>,
}

impl<'a> Diagnostic<'a> {
    fn from_failure(failure: &'a ResolutionFailure) -> Self {
        let (code, message) = diagnostic_classification(failure.kind());
        Self {
            code,
            message,
            location: DiagnosticLocation::from_resolution(failure.location()),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticLocation<'a> {
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
    export: Option<&'a str>,
}

impl<'a> DiagnosticLocation<'a> {
    fn simple(kind: &'static str) -> Self {
        Self {
            kind,
            step: None,
            index: None,
            input: None,
            output: None,
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
}

impl fmt::Display for DiagnosticLocation<'_> {
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
        } else if let Some(name) = self.export {
            write!(formatter, " ({name})")?;
        }
        Ok(())
    }
}

fn diagnostic_classification(kind: ResolutionFailureKind) -> (&'static str, &'static str) {
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
        ResolutionFailureKind::InvalidWorkflowDocument(kind) => decode_diagnostic(kind),
        ResolutionFailureKind::InvalidWorkflowDefinition(kind) => validation_diagnostic(kind),
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
            "Declare JSON Schema Draft 2020-12 in the agent result schema.",
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

fn decode_diagnostic(kind: DecodeFailureKind) -> (&'static str, &'static str) {
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

fn validation_diagnostic(kind: ValidationFailureKind) -> (&'static str, &'static str) {
    match kind {
        ValidationFailureKind::MissingDependency => (
            "missing_dependency",
            "Declare the referenced dependency step or correct its name.",
        ),
        ValidationFailureKind::SelfDependency => {
            ("self_dependency", "Remove the step's dependency on itself.")
        }
        ValidationFailureKind::DuplicateDependency => (
            "duplicate_dependency",
            "List each direct step dependency only once.",
        ),
        ValidationFailureKind::DependencyCycle => (
            "dependency_cycle",
            "Remove dependency edges until the workflow step graph is acyclic.",
        ),
        ValidationFailureKind::UnknownImport => (
            "unknown_import",
            "Use a Workflow V1 import name or correct the input reference.",
        ),
        ValidationFailureKind::UnknownOutputStep => (
            "unknown_output_step",
            "Reference an output from a declared workflow step.",
        ),
        ValidationFailureKind::UnknownOutput => (
            "unknown_output",
            "Reference an output declared by the producing step.",
        ),
        ValidationFailureKind::OutputProducerNotDependency => (
            "output_producer_not_dependency",
            "Make the output producer a transitive dependency of the consuming step.",
        ),
        ValidationFailureKind::UnknownMessageInput => (
            "unknown_message_input",
            "Bind the referenced message input in the agent step's inputs.",
        ),
        ValidationFailureKind::MessageTypeMismatch => (
            "message_type_mismatch",
            "Use an input type accepted by this message destination.",
        ),
        ValidationFailureKind::UnusedAgentInput => (
            "unused_agent_input",
            "Reference every agent input from message text or attachments.",
        ),
        ValidationFailureKind::IllegalCommandOutput => (
            "illegal_command_output",
            "Declare only file outputs on command steps.",
        ),
        ValidationFailureKind::ExcessAgentResponseOutput => (
            "excess_agent_response_output",
            "Declare at most one agent response output on a step.",
        ),
        ValidationFailureKind::ExcessAgentResultOutput => (
            "excess_agent_result_output",
            "Declare at most one agent result output on a step.",
        ),
        ValidationFailureKind::InvalidExportTarget => (
            "invalid_export_target",
            "Reference a declared step output from the workflow export.",
        ),
    }
}

#[derive(Debug)]
enum OutputError {
    WriteJson(serde_json::Error),
    WriteOutput(io::Error),
}

impl From<io::Error> for OutputError {
    fn from(error: io::Error) -> Self {
        Self::WriteOutput(error)
    }
}

impl fmt::Display for OutputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriteJson(error) => {
                write!(formatter, "write JSON workflow validation result: {error}")
            }
            Self::WriteOutput(error) => {
                write!(formatter, "write workflow validation result: {error}")
            }
        }
    }
}
