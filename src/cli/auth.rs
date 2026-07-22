mod login;
mod logout;
mod status;

use std::process::ExitCode;

use clap::{Args, Subcommand};

use crate::human_auth::deployment::Deployment;

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
        let Some(command) = self.command else {
            return super::print_help(&[NAME]);
        };
        let permit_http = allow_insecure_http || !command.uses_network();
        let deployment = match Deployment::load(permit_http) {
            Ok(deployment) => deployment,
            Err(error) => {
                eprintln!("Error: configure Scherzo Cloud sign-in: {error}");
                return ExitCode::FAILURE;
            }
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
