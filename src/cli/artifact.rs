mod assembly;
mod download;
mod validate;

use clap::{Args, Subcommand};

pub(super) const ABOUT: &str = "Work with portable workflow artifacts";
const NAME: &str = "artifact";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<ArtifactCommand>,
}

#[derive(Debug, Subcommand)]
enum ArtifactCommand {
    #[command(about = download::ABOUT)]
    Download(download::Command),
    #[command(about = validate::ABOUT)]
    Validate(validate::Command),
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(ArtifactCommand::Download(command)) => super::execute_deployment_command(
                Some(command),
                &[NAME],
                "configure Scherzo Cloud Artifact Set access",
                download::Command::execute,
            ),
            Some(ArtifactCommand::Validate(command)) => command.execute(),
        }
    }
}
