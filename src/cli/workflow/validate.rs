use std::io::{self, Write};

use anyhow::Context;
use clap::Args;
use serde::Serialize;

use crate::execution::workflow::rejection::{
    RejectionDiagnostic as Diagnostic, human_resolution_remedy,
};
use crate::execution::workflow::resolution::{
    ResolutionFailure, ResolvedWorkflow, resolve_workflow_file,
};
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "Validate a local workflow definition";
const COMMAND_NAME: &str = "scherzo-cloud workflow validate";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    source: super::LocalWorkflowSource,

    #[arg(long, help = "Print the validation result as JSON")]
    json: bool,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        match resolve_workflow_file(&self.source.source_root, &self.source.workflow_file) {
            Ok(workflow) => {
                let result = if self.json {
                    write_json_valid(&workflow)
                } else {
                    write_human_valid(&workflow)
                };
                finish_output(result, ExitCode::Success)
            }
            Err(failure) => {
                let result = if self.json {
                    write_json_invalid(&failure)
                } else {
                    write_human_invalid(&failure)
                };
                finish_output(result, ExitCode::GeneralFailure)
            }
        }
    }
}

fn finish_output(result: anyhow::Result<()>, exit_code: ExitCode) -> super::super::CommandResult {
    result.context("write workflow validation result")?;
    Ok(exit_code)
}

fn write_human_valid(workflow: &ResolvedWorkflow) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✓ Workflow definition is valid.")?;
    writeln!(stdout, "workflow: {}", workflow.source.workflow_path)?;
    writeln!(
        stdout,
        "digest: {}:{}",
        workflow.content_digest.algorithm.as_str(),
        workflow.content_digest.value
    )?;
    writeln!(stdout, "steps: {}", workflow.definition.steps.len())?;
    writeln!(
        stdout,
        "optional imports: {}",
        human_required_imports(workflow)
    )?;
    Ok(())
}

fn write_human_invalid(failure: &ResolutionFailure) -> anyhow::Result<()> {
    let diagnostic = Diagnostic::from_resolution(failure);
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✗ Workflow definition is invalid.")?;
    if let Some(workflow_path) = failure.workflow_path() {
        writeln!(stdout, "workflow: {workflow_path}")?;
    }
    writeln!(stdout, "code: {}", diagnostic.code)?;
    writeln!(stdout, "location: {}", diagnostic.location)?;
    writeln!(stdout, "No workflow steps were executed.")?;
    writeln!(stdout)?;
    writeln!(stdout, "{}", human_resolution_remedy(failure))?;
    Ok(())
}

fn human_required_imports(workflow: &ResolvedWorkflow) -> &'static str {
    if workflow.required_imports().prompt {
        "prompt"
    } else {
        "none"
    }
}

fn write_json_valid(workflow: &ResolvedWorkflow) -> anyhow::Result<()> {
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

fn write_json_invalid(failure: &ResolutionFailure) -> anyhow::Result<()> {
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

fn write_json(report: &JsonReport<'_>) -> anyhow::Result<()> {
    super::super::write_pretty_json(report).context("write workflow validation result")
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
