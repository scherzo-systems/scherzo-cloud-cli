mod login;
mod logout;
mod status;

use std::process::ExitCode;

use clap::{Args, Subcommand};

pub const ABOUT: &str = "Manage your Scherzo Cloud sign-in";
const NAME: &str = "auth";

#[derive(Debug, Args)]
pub struct Command {
    #[command(subcommand)]
    command: Option<AuthCommand>,
}

#[derive(Debug, Subcommand)]
enum AuthCommand {
    #[command(about = login::ABOUT)]
    Login(login::Command),
    #[command(about = status::ABOUT)]
    Status(status::Command),
    #[command(about = logout::ABOUT)]
    Logout(logout::Command),
}

impl Command {
    pub fn execute(self) -> ExitCode {
        super::execute_deployment_command(
            self.command,
            &[NAME],
            "configure Scherzo Cloud sign-in",
            |command, deployment| match command {
                AuthCommand::Login(command) => command.execute(deployment),
                AuthCommand::Status(command) => command.execute(deployment),
                AuthCommand::Logout(command) => command.execute(deployment),
            },
        )
    }
}
