mod validate;

use std::process::ExitCode;

use clap::{Args, Subcommand};

pub(super) const ABOUT: &str = "Inspect and validate portable workflow artifacts";
const NAME: &str = "artifact";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<ArtifactCommand>,
}

#[derive(Debug, Subcommand)]
enum ArtifactCommand {
    #[command(about = validate::ABOUT)]
    Validate(validate::Command),
}

impl Command {
    pub(super) fn execute(self) -> ExitCode {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(ArtifactCommand::Validate(command)) => command.execute(),
        }
    }
}
