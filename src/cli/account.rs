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
    pub fn execute(self, allow_insecure_http: bool) -> ExitCode {
        let (command, deployment) = match super::prepare_network_command(
            self.command,
            &[NAME],
            allow_insecure_http,
            "configure Scherzo Cloud account",
        ) {
            Ok(prepared) => prepared,
            Err(exit_code) => return exit_code,
        };

        match command {
            AccountCommand::Signup(command) => command.execute(&deployment),
        }
    }
}
