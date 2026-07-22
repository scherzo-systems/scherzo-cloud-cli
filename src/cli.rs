mod account;
mod auth;
mod principal;
mod runner;
mod version;

use std::ffi::OsString;
use std::io;
use std::process::ExitCode;

use clap::{CommandFactory, Parser, Subcommand};

use crate::human_auth::deployment::Deployment;

#[derive(Debug, Parser)]
#[command(
    name = "scherzo-cloud",
    about = "Scherzo Cloud CLI",
    version = crate::build_info::VERSION
)]
pub struct Cli {
    #[arg(
        long,
        global = true,
        help = "Allow Scherzo Cloud requests over insecure HTTP connections"
    )]
    allow_insecure_http: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = account::ABOUT)]
    Account(account::Command),
    #[command(about = auth::ABOUT)]
    Auth(auth::Command),
    #[command(about = version::ABOUT)]
    Version(version::Command),
    #[command(about = runner::ABOUT)]
    Runner(runner::Command),
}

pub fn parse<I, S>(args: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString> + Clone,
{
    Cli::try_parse_from(args)
}

impl Cli {
    pub fn execute(self) -> ExitCode {
        match self.command {
            None => print_help(&[]),
            Some(Command::Account(command)) => command.execute(self.allow_insecure_http),
            Some(Command::Auth(command)) => command.execute(self.allow_insecure_http),
            Some(Command::Version(command)) => command.execute(),
            Some(Command::Runner(command)) => command.execute(),
        }
    }
}

fn prepare_network_command<T>(
    command: Option<T>,
    command_path: &[&str],
    permit_http: bool,
    error_context: &'static str,
) -> Result<(T, Deployment), ExitCode> {
    let Some(command) = command else {
        return Err(print_help(command_path));
    };
    let deployment = Deployment::load(permit_http).map_err(|error| {
        eprintln!("Error: {error_context}: {error}");
        ExitCode::FAILURE
    })?;
    Ok((command, deployment))
}

fn print_help(command_path: &[&str]) -> ExitCode {
    let mut root = Cli::command();
    root.build();
    let mut command = &mut root;

    for name in command_path {
        let Some(subcommand) = command.find_subcommand_mut(name) else {
            eprintln!("Error: command help metadata is unavailable for {name}");
            return ExitCode::FAILURE;
        };
        command = subcommand;
    }

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    match command.write_help(&mut stdout) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: failed to write command help: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::Cli;

    #[test]
    fn root_help_is_composed_from_command_metadata() {
        let help = Cli::command().render_help().to_string();

        assert!(help.contains("account  Manage your Scherzo Cloud account"));
        assert!(help.contains("auth     Manage your Scherzo Cloud sign-in"));
        assert!(help.contains("version  Print version information"));
        assert!(help.contains("runner   Run and manage the Scherzo Cloud runner"));
        assert!(help.contains("--allow-insecure-http"));
    }

    #[test]
    fn account_help_is_composed_from_command_metadata() {
        let mut root = Cli::command();
        let account = root
            .find_subcommand_mut("account")
            .expect("account command should exist");
        let help = account.render_help().to_string();

        assert!(help.contains("signup  Create your Scherzo Cloud account"));
    }

    #[test]
    fn auth_help_is_composed_from_command_metadata() {
        let mut root = Cli::command();
        let auth = root
            .find_subcommand_mut("auth")
            .expect("auth command should exist");
        let help = auth.render_help().to_string();

        assert!(help.contains("login   Sign in to Scherzo Cloud"));
        assert!(help.contains("status  Show your Scherzo Cloud sign-in status"));
        assert!(help.contains("logout  Sign out of Scherzo Cloud on this device"));
    }

    #[test]
    fn runner_help_is_composed_from_serve_metadata() {
        let mut root = Cli::command();
        let runner = root
            .find_subcommand_mut("runner")
            .expect("runner command should exist");
        let help = runner.render_help().to_string();

        assert!(help.contains("serve  Connect to Scherzo Cloud and serve run assignments"));
    }
}
