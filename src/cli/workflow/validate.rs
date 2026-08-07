use std::fmt;
use std::io::{self, Write};
use std::process::ExitCode;

use clap::Args;
use serde::Serialize;

use crate::execution::workflow::rejection::RejectionDiagnostic as Diagnostic;
use crate::execution::workflow::resolution::{
    ResolutionFailure, ResolvedWorkflow, resolve_workflow_file,
};

pub(super) const ABOUT: &str = "Validate a local Workflow V1 bundle without executing it";
const COMMAND_NAME: &str = "scherzo-cloud workflow validate";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    source: super::LocalWorkflowSource,

    #[arg(long, help = "Print the schema-version-1 validation result as JSON")]
    json: bool,
}

impl Command {
    pub(super) fn execute(self) -> ExitCode {
        match resolve_workflow_file(&self.source.source_root, &self.source.workflow_file) {
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
    let diagnostic = Diagnostic::from_resolution(failure);
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
            diagnostics: [Diagnostic::from_resolution(failure)],
        },
    };
    write_json(&report)
}

fn write_json(report: &JsonReport<'_>) -> Result<(), OutputError> {
    super::super::write_pretty_json(report).map_err(OutputError::WriteOutput)
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

#[derive(Debug)]
enum OutputError {
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
            Self::WriteOutput(error) => {
                write!(formatter, "write workflow validation result: {error}")
            }
        }
    }
}
