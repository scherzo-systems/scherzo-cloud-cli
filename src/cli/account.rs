mod signup;

use clap::{Args, Subcommand};

pub(super) const ABOUT: &str = "Manage your Scherzo Cloud account";
const NAME: &str = "account";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<AccountCommand>,
}

#[derive(Debug, Subcommand)]
enum AccountCommand {
    #[command(about = signup::ABOUT)]
    Signup(signup::Command),
}

// Account and auth retain separate command enums so each family owns its public dispatch surface.
// jscpd:ignore-start
impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
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
// jscpd:ignore-end
