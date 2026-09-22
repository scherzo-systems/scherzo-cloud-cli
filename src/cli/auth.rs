mod identities;
mod login;
mod logout;
mod status;

use clap::{Args, Subcommand};

use crate::human_auth::deployment::Deployment;

pub(super) const ABOUT: &str = "Manage your Scherzo Cloud sign-in";
const NAME: &str = "auth";

#[derive(Debug, Args)]
pub(super) struct Command {
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
    #[command(about = identities::ABOUT)]
    Identities(identities::Command),
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(AuthCommand::Login(command)) => execute_leaf(command, login::Command::execute),
            Some(AuthCommand::Status(command)) => execute_leaf(command, status::Command::execute),
            Some(AuthCommand::Logout(command)) => execute_leaf(command, logout::Command::execute),
            Some(AuthCommand::Identities(command)) => command.execute(),
        }
    }
}

fn execute_leaf<T>(
    command: T,
    execute: impl FnOnce(T, &Deployment) -> super::CommandResult,
) -> super::CommandResult {
    super::execute_deployment_command(
        Some(command),
        &[NAME],
        "configure Scherzo Cloud sign-in",
        execute,
    )
}
