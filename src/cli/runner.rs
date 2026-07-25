mod doctor;
mod serve;

use std::process::ExitCode;

use clap::{Args, Subcommand};

pub(super) const ABOUT: &str = "Run and manage the Scherzo Cloud runner";
const NAME: &str = "runner";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<RunnerCommand>,
}

#[derive(Debug, Subcommand)]
enum RunnerCommand {
    #[command(about = doctor::ABOUT)]
    Doctor(doctor::Command),
    #[command(about = serve::ABOUT)]
    Serve(serve::Command),
}

impl Command {
    pub(super) fn execute(self) -> ExitCode {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(RunnerCommand::Doctor(command)) => command.execute(),
            Some(RunnerCommand::Serve(command)) => command.execute(),
        }
    }
}
