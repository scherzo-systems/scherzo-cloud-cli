mod signup;

use std::process::ExitCode;

use clap::{Args, Subcommand};

pub const ABOUT: &str = "Manage your Scherzo Cloud account";
const NAME: &str = "account";

#[derive(Debug, Args)]
pub struct Command {
    #[command(subcommand)]
    command: Option<AccountCommand>,
}

#[derive(Debug, Subcommand)]
enum AccountCommand {
    #[command(about = signup::ABOUT)]
    Signup(signup::Command),
}

impl Command {
    pub fn execute(self) -> ExitCode {
        super::execute_deployment_command(
            self.command,
            &[NAME],
            "configure Scherzo Cloud account",
            |command, deployment| match command {
                AccountCommand::Signup(command) => command.execute(deployment),
            },
        )
    }
}
