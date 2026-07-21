mod serve;

use std::process::ExitCode;

use clap::{Args, Subcommand};

pub const ABOUT: &str = "Run and manage the Scherzo Cloud runner";
const NAME: &str = "runner";

#[derive(Debug, Args)]
pub struct Command {
    #[command(subcommand)]
    command: Option<RunnerCommand>,
}

#[derive(Debug, Subcommand)]
enum RunnerCommand {
    #[command(about = serve::ABOUT)]
    Serve(serve::Command),
}

impl Command {
    pub fn execute(self) -> ExitCode {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(RunnerCommand::Serve(command)) => command.execute(),
        }
    }
}
