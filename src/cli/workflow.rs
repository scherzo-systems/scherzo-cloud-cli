mod run;
mod validate;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Subcommand};

pub(super) const ABOUT: &str = "Validate and run local Workflow V1 definitions";
const NAME: &str = "workflow";

#[derive(Debug, Args)]
pub(super) struct LocalWorkflowSource {
    #[arg(
        long,
        value_name = "ROOT",
        help = "Explicit directory boundary for workflow source files"
    )]
    pub(super) source_root: PathBuf,

    #[arg(
        value_name = "WORKFLOW_PATH",
        help = "Workflow YAML path selected within the source root"
    )]
    pub(super) workflow_path: PathBuf,
}

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<WorkflowCommand>,
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    #[command(about = run::ABOUT)]
    Run(run::Command),
    #[command(about = validate::ABOUT)]
    Validate(validate::Command),
}

impl Command {
    pub(super) fn execute(self) -> ExitCode {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(WorkflowCommand::Run(command)) => command.execute(),
            Some(WorkflowCommand::Validate(command)) => command.execute(),
        }
    }
}
