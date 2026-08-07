mod account;
mod auth;
mod organization;
mod principal;
mod runner;
mod version;
mod workflow;

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
pub(crate) struct Cli {
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
    #[command(about = organization::ABOUT)]
    Organization(organization::Command),
    #[command(about = version::ABOUT)]
    Version(version::Command),
    #[command(about = runner::ABOUT)]
    Runner(runner::Command),
    #[command(about = workflow::ABOUT)]
    Workflow(workflow::Command),
}

pub(crate) fn parse<I, S>(args: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString> + Clone,
{
    Cli::try_parse_from(args)
}

impl Cli {
    pub(crate) fn execute(self) -> ExitCode {
        match self.command {
            None => print_help(&[]),
            Some(Command::Account(command)) => command.execute(),
            Some(Command::Auth(command)) => command.execute(),
            Some(Command::Organization(command)) => command.execute(),
            Some(Command::Version(command)) => command.execute(),
            Some(Command::Runner(command)) => command.execute(),
            Some(Command::Workflow(command)) => command.execute(),
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

    fn collect_command_paths(command: &clap::Command, prefix: &str, paths: &mut Vec<String>) {
        assert!(
            !command.is_allow_external_subcommands_set(),
            "customer command {prefix:?} accepts external subcommands"
        );
        for child in command
            .get_subcommands()
            .filter(|child| child.get_name() != "help")
        {
            for name in std::iter::once(child.get_name()).chain(child.get_all_aliases()) {
                let path = if prefix.is_empty() {
                    name.to_owned()
                } else {
                    format!("{prefix} {name}")
                };
                paths.push(path.clone());
                collect_command_paths(child, &path, paths);
            }
        }
    }

    #[test]
    fn customer_command_surface_is_exact_and_has_no_operator_entrypoint() {
        let mut actual = Vec::new();
        collect_command_paths(&Cli::command(), "", &mut actual);
        actual.sort();
        actual.dedup();
        let expected = [
            "account",
            "account signup",
            "auth",
            "auth login",
            "auth logout",
            "auth status",
            "organization",
            "organization create",
            "organization members",
            "organization members list",
            "organization show",
            "organization update",
            "runner",
            "runner doctor",
            "runner serve",
            "version",
            "workflow",
            "workflow retry",
            "workflow run",
            "workflow status",
            "workflow validate",
            "workflow view",
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn root_help_is_composed_from_command_metadata() {
        let help = command_help(&[]);

        assert!(help.contains("account       Manage your Scherzo Cloud account"));
        assert!(help.contains("auth          Manage your Scherzo Cloud sign-in"));
        assert!(help.contains("organization  Manage Scherzo Cloud organizations"));
        assert!(help.contains("version       Print version information"));
        assert!(help.contains("runner        Run and manage the Scherzo Cloud runner"));
        assert!(help.contains(
            "workflow      Validate, run, retry, and inspect local Workflow V1 definitions"
        ));
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
    fn organization_help_is_composed_from_command_metadata() {
        let help = command_help(&["organization"]);

        assert!(help.contains("create   Create a Scherzo Cloud organization"));
        assert!(help.contains("show     Show a Scherzo Cloud organization"));
        assert!(help.contains("update   Update a Scherzo Cloud organization"));
        assert!(help.contains("members  Manage Scherzo Cloud organization members"));
        assert!(command_help(&["organization", "create"]).contains("--display-name"));
        assert!(command_help(&["organization", "create"]).contains("--allow-insecure-http"));
        assert!(command_help(&["organization", "show"]).contains("<ORGANIZATION>"));

        let update = command_help(&["organization", "update"]);
        assert!(update.contains("--display-name"));
        assert!(update.contains("--slug"));
        assert!(update.contains("--allow-insecure-http"));

        let members = command_help(&["organization", "members"]);
        assert!(members.contains("list  List one page of organization members"));
        let list = command_help(&["organization", "members", "list"]);
        assert!(list.contains("--limit <LIMIT>"));
        assert!(list.contains("--cursor <CURSOR>"));
        assert!(list.contains("--allow-insecure-http"));
    }

    #[test]
    fn runner_help_is_composed_from_leaf_metadata() {
        let help = command_help(&["runner"]);

        assert!(help.contains("doctor  Inspect local runner prerequisites"));
        assert!(help.contains("serve   Connect to Scherzo Cloud and serve run assignments"));
    }

    #[test]
    fn workflow_help_is_composed_from_leaf_metadata() {
        let help = command_help(&["workflow"]);
        let validate = command_help(&["workflow", "validate"]);

        assert!(
            help.contains("retry     Retry every step of an eligible durable local workflow run")
        );
        assert!(help.contains("run       Execute a local Workflow V1 command and agent DAG"));
        assert!(
            help.contains("status    Inspect one durable local workflow run without changing it")
        );
        assert!(
            help.contains("validate  Validate a local Workflow V1 bundle without executing it")
        );
        assert!(
            help.contains("view      Inspect one published local workflow attempt interactively")
        );
        let run = command_help(&["workflow", "run"]);
        for option in [
            "--source-root <ROOT>",
            "--execution-root <PATH>",
            "--run-dir <PATH>",
            "--prompt-file <PATH|->",
            "--attachment <MEDIA_TYPE> <PATH>",
            "--max-parallel <COUNT>",
            "--plain",
            "--json",
            "--color <auto|always|never>",
            "<WORKFLOW_PATH>",
        ] {
            assert!(run.contains(option), "run help should contain {option}");
        }
        let retry = command_help(&["workflow", "retry"]);
        for option in [
            "--run-dir <PATH>",
            "--execution-root <PATH>",
            "--plain",
            "--json",
            "--color <auto|always|never>",
        ] {
            assert!(retry.contains(option), "retry help should contain {option}");
        }
        assert!(!retry.contains("--source-root"));
        assert!(!retry.contains("--prompt-file"));
        assert!(!retry.contains("--max-parallel"));
        let status = command_help(&["workflow", "status"]);
        for option in [
            "--run-dir <PATH>",
            "--plain",
            "--json",
            "--color <auto|always|never>",
        ] {
            assert!(
                status.contains(option),
                "status help should contain {option}"
            );
        }
        assert!(!status.contains("--source-root"));
        assert!(!status.contains("--execution-root"));
        let view = command_help(&["workflow", "view"]);
        for option in [
            "--run-dir <PATH>",
            "--attempt <NUMBER>",
            "--color <auto|always|never>",
        ] {
            assert!(view.contains(option), "view help should contain {option}");
        }
        assert!(!view.contains("--plain"));
        assert!(!view.contains("--json"));
        assert!(!view.contains("--source-root"));
        assert!(!view.contains("--execution-root"));
        assert!(validate.contains("--source-root <ROOT>"));
        assert!(validate.contains("<WORKFLOW_PATH>"));
        assert!(validate.contains("--json"));
    }
}
