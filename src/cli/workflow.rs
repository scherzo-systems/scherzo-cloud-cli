mod retry;
mod run;
mod status;
mod validate;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Subcommand, ValueEnum};

pub(super) const ABOUT: &str = "Validate, run, retry, and inspect local Workflow V1 definitions";
const NAME: &str = "workflow";

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(super) enum ColorArgument {
    Auto,
    Always,
    Never,
}

#[derive(Debug, Args)]
pub(super) struct PresentationOptions {
    #[arg(long, conflicts_with = "json", help = "Force plain human presentation")]
    pub(super) plain: bool,

    #[arg(long, help = "Print one schema-version-1 JSON result")]
    pub(super) json: bool,

    #[arg(
        long,
        value_enum,
        value_name = "auto|always|never",
        default_value_t = ColorArgument::Auto,
        help = "Select renderer color behavior"
    )]
    pub(super) color: ColorArgument,
}

#[derive(Debug, Args)]
pub(super) struct ExistingLocalRun {
    #[arg(
        long,
        value_name = "PATH",
        help = "Existing durable directory for exactly one workflow run"
    )]
    pub(super) run_dir: PathBuf,
}

#[derive(Debug, Args)]
pub(super) struct LocalExecutionRoot {
    #[arg(
        long,
        value_name = "PATH",
        help = "Existing caller-owned workflow execution directory"
    )]
    pub(super) execution_root: PathBuf,
}

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
    #[command(about = retry::ABOUT)]
    Retry(retry::Command),
    #[command(about = run::ABOUT, after_help = run::AFTER_HELP)]
    Run(run::Command),
    #[command(about = status::ABOUT)]
    Status(status::Command),
    #[command(about = validate::ABOUT)]
    Validate(validate::Command),
}

impl Command {
    pub(super) fn execute(self) -> ExitCode {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(WorkflowCommand::Retry(command)) => command.execute(),
            Some(WorkflowCommand::Run(command)) => command.execute(),
            Some(WorkflowCommand::Status(command)) => command.execute(),
            Some(WorkflowCommand::Validate(command)) => command.execute(),
        }
    }
}
