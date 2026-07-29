mod validate;

use std::process::ExitCode;

use clap::{Args, Subcommand};

pub(super) const ABOUT: &str = "Inspect local Workflow V1 definitions";
const NAME: &str = "workflow";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<WorkflowCommand>,
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    #[command(about = validate::ABOUT)]
    Validate(validate::Command),
}

impl Command {
    pub(super) fn execute(self) -> ExitCode {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(WorkflowCommand::Validate(command)) => command.execute(),
        }
    }
}
