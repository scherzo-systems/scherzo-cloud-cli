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
    pub fn execute(self, allow_insecure_http: bool) -> ExitCode {
        let permit_http = allow_insecure_http
            || self
                .command
                .as_ref()
                .is_some_and(|command| !command.uses_network());
        let (command, deployment) = match super::prepare_network_command(
            self.command,
            &[NAME],
            permit_http,
            "configure Scherzo Cloud sign-in",
        ) {
            Ok(prepared) => prepared,
            Err(exit_code) => return exit_code,
        };

        match command {
            AuthCommand::Login(command) => command.execute(&deployment),
            AuthCommand::Status(command) => command.execute(&deployment),
            AuthCommand::Logout(command) => command.execute(&deployment),
        }
    }
}

impl AuthCommand {
    fn uses_network(&self) -> bool {
        matches!(self, Self::Login(_) | Self::Status(_))
    }
}
