mod reference;
mod retry;
mod run;
mod schema;
mod status;
mod validate;
mod view;

use std::future::Future;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Subcommand, ValueEnum};

use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "Work with local workflow definitions and runs";
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

    #[arg(long, help = "Print the result as JSON")]
    pub(super) json: bool,

    #[arg(
        long,
        value_enum,
        value_name = "WHEN",
        default_value_t = ColorArgument::Auto,
        help = "Select renderer color behavior"
    )]
    pub(super) color: ColorArgument,
}

#[derive(Debug, Args)]
pub(super) struct ExistingLocalRun {
    #[arg(value_name = "RUN_DIR", help = "Directory containing the workflow run")]
    pub(super) run_dir: PathBuf,
}

#[derive(Debug, Args)]
pub(super) struct LocalExecutionRoot {
    #[arg(
        long,
        value_name = "PATH",
        help = "Directory for workflow execution (must already exist)"
    )]
    pub(super) execution_root: PathBuf,
}

#[derive(Debug, Args)]
pub(super) struct LocalWorkflowSource {
    #[arg(
        long,
        value_name = "ROOT",
        help = "Directory boundary for workflow source files"
    )]
    pub(super) source_root: PathBuf,

    #[arg(
        value_name = "WORKFLOW_FILE",
        help = "Workflow definition path, resolved from the initial working directory"
    )]
    pub(super) workflow_file: PathBuf,
}

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<WorkflowCommand>,
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    #[command(about = reference::ABOUT)]
    Reference(reference::Command),
    #[command(about = retry::ABOUT, after_help = retry::AFTER_HELP)]
    Retry(retry::Command),
    #[command(about = run::ABOUT, after_help = run::AFTER_HELP)]
    Run(run::Command),
    #[command(about = schema::ABOUT, after_help = schema::AFTER_HELP)]
    Schema(schema::Command),
    #[command(about = status::ABOUT)]
    Status(status::Command),
    #[command(about = validate::ABOUT)]
    Validate(validate::Command),
    #[command(about = view::ABOUT, after_help = view::AFTER_HELP)]
    View(view::Command),
}

pub(super) fn write_embedded_asset(
    asset: &'static str,
    context: &'static str,
) -> super::CommandResult {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    stdout
        .write_all(asset.as_bytes())
        .and_then(|()| stdout.flush())
        .context(context)?;
    Ok(ExitCode::Success)
}

// Read-only commands use a current-thread runtime so signal exit can abandon a blocked
// filesystem worker without delaying process shutdown.
pub(super) fn execute_with_abandonable_runtime(
    leaf: &'static str,
    execution: impl Future<Output = super::CommandResult>,
) -> super::CommandResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .with_context(|| format!("start workflow {leaf} runtime"))?;
    let result = runtime.block_on(execution);
    runtime.shutdown_timeout(Duration::ZERO);
    result
}

pub(super) fn observe_workflow_signals(
    leaf: &'static str,
) -> anyhow::Result<(tokio::signal::unix::Signal, tokio::signal::unix::Signal)> {
    (|| -> io::Result<_> {
        let interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        Ok((interrupt, terminate))
    })()
    .with_context(|| format!("install workflow {leaf} signal observation"))
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(WorkflowCommand::Reference(command)) => command.execute(),
            Some(WorkflowCommand::Retry(command)) => command.execute(),
            Some(WorkflowCommand::Run(command)) => command.execute(),
            Some(WorkflowCommand::Schema(command)) => command.execute(),
            Some(WorkflowCommand::Status(command)) => command.execute(),
            Some(WorkflowCommand::Validate(command)) => command.execute(),
            Some(WorkflowCommand::View(command)) => command.execute(),
        }
    }
}
