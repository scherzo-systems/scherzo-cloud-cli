use std::collections::BTreeMap;
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
        "finalizers: {}",
        workflow.definition.finalizers.len()
    )?;
    writeln!(
        stdout,
        "required inputs: {}",
        human_required_inputs(workflow)
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

fn human_required_inputs(workflow: &ResolvedWorkflow) -> String {
    if workflow.required_inputs().is_empty() {
        return "none".to_owned();
    }
    workflow
        .required_inputs()
        .iter()
        .map(|(name, kind)| format!("{name}:{}", input_kind(*kind)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn input_kind(kind: crate::execution::workflow::validated::WorkflowValueType) -> &'static str {
    match kind {
        crate::execution::workflow::validated::WorkflowValueType::Text => "text",
        crate::execution::workflow::validated::WorkflowValueType::AttachmentCollection => {
            "attachments"
        }
        crate::execution::workflow::validated::WorkflowValueType::Json => "json",
        crate::execution::workflow::validated::WorkflowValueType::File => "file",
        crate::execution::workflow::validated::WorkflowValueType::GitBranch => "git_branch",
    }
}

fn write_json_valid(workflow: &ResolvedWorkflow) -> anyhow::Result<()> {
    let required_inputs = workflow
        .required_inputs()
        .iter()
        .map(|(name, kind)| (name.as_str(), input_kind(*kind)))
        .collect::<BTreeMap<_, _>>();
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
            finalizer_count: workflow.definition.finalizers.len(),
            required_inputs,
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
        #[serde(rename = "finalizerCount")]
        finalizer_count: usize,
        #[serde(rename = "requiredInputs")]
        required_inputs: BTreeMap<&'a str, &'static str>,
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
