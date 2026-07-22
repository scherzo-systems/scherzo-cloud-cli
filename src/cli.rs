mod account;
mod auth;
mod principal;
mod runner;
mod version;

use std::ffi::OsString;
use std::io;
use std::process::ExitCode;

use clap::{Args, CommandFactory, Parser, Subcommand};

use crate::api::HttpTransportPolicy;
use crate::human_auth::deployment::Deployment;

#[derive(Debug, Parser)]
#[command(
    name = "scherzo-cloud",
    about = "Scherzo Cloud CLI",
    version = crate::build_info::VERSION
)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Args)]
struct HttpOptions {
    #[arg(
        long,
        help = "Allow this command's Scherzo Cloud requests over insecure HTTP connections"
    )]
    allow_insecure_http: bool,
}

impl HttpOptions {
    fn transport_policy(&self) -> HttpTransportPolicy {
        if self.allow_insecure_http {
            HttpTransportPolicy::AllowInsecureHttp
        } else {
            HttpTransportPolicy::HttpsOnly
        }
    }
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
            Some(Command::Account(command)) => command.execute(),
            Some(Command::Auth(command)) => command.execute(),
            Some(Command::Version(command)) => command.execute(),
            Some(Command::Runner(command)) => command.execute(),
        }
    }
}

fn execute_deployment_command<T>(
    command: Option<T>,
    command_path: &[&str],
    error_context: &'static str,
    execute: impl FnOnce(T, &Deployment) -> ExitCode,
) -> ExitCode {
    let Some(command) = command else {
        return print_help(command_path);
    };
    let deployment = match Deployment::load() {
        Ok(deployment) => deployment,
        Err(error) => {
            eprintln!("Error: {error_context}: {error}");
            return ExitCode::FAILURE;
        }
    };
    execute(command, &deployment)
}

fn finish_command<E: std::fmt::Display>(result: Result<ExitCode, E>) -> ExitCode {
    match result {
        Ok(exit_code) => exit_code,
        Err(error) => {
            eprintln!("Error: {error}");
            ExitCode::FAILURE
        }
    }
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

    fn command_help(path: &[&str]) -> String {
        let mut root = Cli::command();
        let mut command = &mut root;
        for name in path {
            command = command
                .find_subcommand_mut(name)
                .expect("command should exist");
        }
        command.render_help().to_string()
    }

    #[test]
    fn root_help_is_composed_from_command_metadata() {
        let help = command_help(&[]);

        assert!(help.contains("account  Manage your Scherzo Cloud account"));
        assert!(help.contains("auth     Manage your Scherzo Cloud sign-in"));
        assert!(help.contains("version  Print version information"));
        assert!(help.contains("runner   Run and manage the Scherzo Cloud runner"));
        assert!(!help.contains("--allow-insecure-http"));
    }

    #[test]
    fn account_help_is_composed_from_command_metadata() {
        let help = command_help(&["account"]);

        assert!(help.contains("signup  Create your Scherzo Cloud account"));
        assert!(command_help(&["account", "signup"]).contains("--allow-insecure-http"));
    }

    #[test]
    fn auth_help_is_composed_from_command_metadata() {
        let help = command_help(&["auth"]);

        assert!(help.contains("login   Sign in to Scherzo Cloud"));
        assert!(help.contains("status  Show your Scherzo Cloud sign-in status"));
        assert!(help.contains("logout  Sign out of Scherzo Cloud on this device"));
        assert!(command_help(&["auth", "login"]).contains("--allow-insecure-http"));
        assert!(command_help(&["auth", "status"]).contains("--allow-insecure-http"));
        assert!(!command_help(&["auth", "logout"]).contains("--allow-insecure-http"));
    }

    #[test]
    fn runner_help_is_composed_from_serve_metadata() {
        let help = command_help(&["runner"]);

        assert!(help.contains("serve  Connect to Scherzo Cloud and serve run assignments"));
    }
}
