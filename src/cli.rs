mod account;
mod artifact;
mod auth;
mod github;
mod organization;
mod principal;
mod project;
mod run;
mod runner;
mod version;
mod workflow;

use std::ffi::OsString;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::{Args, CommandFactory, Parser, Subcommand, builder::NonEmptyStringValueParser};
use serde::Serialize;

use crate::api::HttpTransportPolicy;
use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::deployment::Deployment;

pub(crate) type CommandResult = Result<ExitCode, CommandFailure>;

pub(crate) struct CommandFailure {
    error: anyhow::Error,
    exit_code: ExitCode,
}

impl CommandFailure {
    pub(crate) fn new(error: anyhow::Error) -> Self {
        Self {
            error,
            exit_code: ExitCode::GeneralFailure,
        }
    }

    pub(crate) fn with_exit_code(error: anyhow::Error, exit_code: ExitCode) -> Self {
        Self { error, exit_code }
    }

    pub(crate) fn for_outcome(error: anyhow::Error, outcome: OutcomeClass) -> Self {
        Self::with_exit_code(error, outcome.exit_code())
    }

    pub(crate) fn error(&self) -> &anyhow::Error {
        &self.error
    }

    pub(crate) fn exit_code(&self) -> ExitCode {
        self.exit_code
    }
}

impl From<anyhow::Error> for CommandFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::new(error)
    }
}

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

#[derive(Debug, Args)]
struct PaginationArgs {
    #[arg(
        long,
        value_parser = clap::value_parser!(u16).range(1..=200),
        help = "Maximum items to return (1-200)"
    )]
    limit: Option<u16>,

    #[arg(
        long,
        value_parser = NonEmptyStringValueParser::new(),
        help = "Opaque continuation cursor"
    )]
    cursor: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = account::ABOUT)]
    Account(account::Command),
    #[command(about = artifact::ABOUT)]
    Artifact(artifact::Command),
    #[command(about = auth::ABOUT)]
    Auth(auth::Command),
    #[command(about = github::ABOUT)]
    Github(github::Command),
    #[command(about = organization::ABOUT)]
    Organization(organization::Command),
    #[command(about = project::ABOUT)]
    Project(project::Command),
    #[command(about = run::ABOUT)]
    Run(run::Command),
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
    pub(crate) fn execute(self) -> CommandResult {
        match self.command {
            None => print_help(&[]),
            Some(Command::Account(command)) => command.execute(),
            Some(Command::Artifact(command)) => command.execute(),
            Some(Command::Auth(command)) => command.execute(),
            Some(Command::Github(command)) => command.execute(),
            Some(Command::Organization(command)) => command.execute(),
            Some(Command::Project(command)) => command.execute(),
            Some(Command::Run(command)) => command.execute(),
            Some(Command::Version(command)) => command.execute(),
            Some(Command::Runner(command)) => command.execute(),
            Some(Command::Workflow(command)) => command.execute(),
        }
    }
}

pub(crate) const fn unreachable_outcome_class(
    category: crate::api::UnreachableCategory,
) -> OutcomeClass {
    match category {
        crate::api::UnreachableCategory::RateLimited => OutcomeClass::RateLimited,
        crate::api::UnreachableCategory::Dns
        | crate::api::UnreachableCategory::Timeout
        | crate::api::UnreachableCategory::Connection
        | crate::api::UnreachableCategory::Tls
        | crate::api::UnreachableCategory::Server => OutcomeClass::Unreachable,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CloudFailureResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after: Option<u64>,
}

fn write_pretty_json(value: &impl Serialize) -> io::Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    bytes.push(b'\n');
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    stdout.write_all(&bytes)?;
    stdout.flush()
}

struct ProcessSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

impl ProcessSignals {
    fn install(context: &str) -> anyhow::Result<Self> {
        (|| -> io::Result<_> {
            Ok(Self {
                interrupt: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::interrupt(),
                )?,
                terminate: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate(),
                )?,
            })
        })()
        .with_context(|| format!("install {context} signal observation"))
    }

    async fn recv(&mut self) -> ExitCode {
        tokio::select! {
            biased;
            _ = self.interrupt.recv() => OutcomeClass::Interrupted.exit_code(),
            _ = self.terminate.recv() => OutcomeClass::Terminated.exit_code(),
        }
    }
}

fn execute_read_only_with_signals(
    context: &'static str,
    operation: impl FnOnce(&AtomicBool, &AtomicBool) -> CommandResult + Send + 'static,
) -> CommandResult {
    execute_blocking_with_signals(context, operation, Ok)
}

fn execute_mutation_with_signals(
    context: &'static str,
    operation: impl FnOnce(&AtomicBool, &AtomicBool) -> CommandResult + Send + 'static,
    incomplete_signal: impl FnOnce(ExitCode) -> CommandResult + 'static,
) -> CommandResult {
    execute_blocking_with_signals(context, operation, incomplete_signal)
}

// A terminal result and a local stop compete for one output claim so timeout/signal
// races cannot emit two machine documents or replace a terminal result after it wins.
const OBSERVATION_ACTIVE: u8 = 0;
const OBSERVATION_COMPLETING: u8 = 1;
const OBSERVATION_STOPPED: u8 = 2;

struct BlockingObservationControl {
    state: AtomicU8,
}

impl BlockingObservationControl {
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(OBSERVATION_ACTIVE),
        }
    }

    fn is_stopped(&self) -> bool {
        self.state.load(Ordering::Acquire) == OBSERVATION_STOPPED
    }

    fn begin_completion(&self) -> bool {
        self.state
            .compare_exchange(
                OBSERVATION_ACTIVE,
                OBSERVATION_COMPLETING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn stop(&self) -> bool {
        self.state
            .compare_exchange(
                OBSERVATION_ACTIVE,
                OBSERVATION_STOPPED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

struct ObservationTimeout {
    duration: Option<Duration>,
}

impl ObservationTimeout {
    async fn wait(self) {
        match self.duration {
            Some(duration) => crate::timing::async_sleep(duration).await,
            None => std::future::pending().await,
        }
    }
}

fn blocking_signal_runtime(context: &str) -> anyhow::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .with_context(|| format!("start {context} runtime"))
}

fn execute_observation_with_signals_and_timeout(
    context: &'static str,
    timeout: Option<Duration>,
    operation: impl FnOnce(&BlockingObservationControl) -> CommandResult + Send + 'static,
    timed_out: impl FnOnce() -> CommandResult + 'static,
) -> CommandResult {
    let runtime = blocking_signal_runtime(context)?;
    let result = runtime.block_on(async move {
        let mut signals = ProcessSignals::install(context)?;
        let control = Arc::new(BlockingObservationControl::new());
        let operation_control = Arc::clone(&control);
        let mut running = tokio::task::spawn_blocking(move || operation(&operation_control));
        tokio::select! {
            biased;
            signal = signals.recv() => {
                if control.stop() {
                    Ok(signal)
                } else {
                    finish_read_only_operation(context, running.await)
                }
            }
            () = (ObservationTimeout { duration: timeout }).wait() => {
                if control.stop() {
                    timed_out()
                } else {
                    finish_read_only_operation(context, running.await)
                }
            }
            result = &mut running => finish_read_only_operation(context, result),
        }
    });
    runtime.shutdown_timeout(Duration::ZERO);
    result
}

fn execute_blocking_with_signals(
    context: &'static str,
    operation: impl FnOnce(&AtomicBool, &AtomicBool) -> CommandResult + Send + 'static,
    incomplete_signal: impl FnOnce(ExitCode) -> CommandResult + 'static,
) -> CommandResult {
    let runtime = blocking_signal_runtime(context)?;
    let result = runtime.block_on(async move {
        let mut signals = ProcessSignals::install(context)?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicBool::new(false));
        let operation_cancelled = Arc::clone(&cancelled);
        let operation_completed = Arc::clone(&completed);
        let mut running = tokio::task::spawn_blocking(move || {
            operation(&operation_cancelled, &operation_completed)
        });
        tokio::select! {
            biased;
            signal = signals.recv() => {
                cancelled.store(true, Ordering::Release);
                if completed.load(Ordering::Acquire) {
                    finish_read_only_operation(context, running.await)
                } else {
                    incomplete_signal(signal)
                }
            }
            result = &mut running => finish_read_only_operation(context, result),
        }
    });
    runtime.shutdown_timeout(Duration::ZERO);
    result
}

fn finish_read_only_operation(
    context: &str,
    result: Result<CommandResult, tokio::task::JoinError>,
) -> CommandResult {
    result.with_context(|| format!("complete {context} operation"))?
}

fn execute_deployment_leaf<T>(
    command: T,
    command_path: &[&str],
    error_context: &'static str,
    execute: impl FnOnce(T, &Deployment) -> anyhow::Result<ExitCode>,
) -> CommandResult {
    execute_deployment_command(
        Some(command),
        command_path,
        error_context,
        |command, deployment| execute(command, deployment).map_err(Into::into),
    )
}

fn execute_deployment_command<T>(
    command: Option<T>,
    command_path: &[&str],
    error_context: &'static str,
    execute: impl FnOnce(T, &Deployment) -> CommandResult,
) -> CommandResult {
    let Some(command) = command else {
        return print_help(command_path);
    };
    let deployment = Deployment::load()
        .map_err(|error| anyhow!(error))
        .context(error_context)?;
    execute(command, &deployment)
}

fn print_help(command_path: &[&str]) -> CommandResult {
    let mut root = Cli::command();
    root.build();
    let mut command = &mut root;

    for name in command_path {
        let Some(subcommand) = command.find_subcommand_mut(name) else {
            return Err(anyhow!("command help metadata is unavailable for {name}").into());
        };
        command = subcommand;
    }

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    command
        .write_help(&mut stdout)
        .context("failed to write command help")?;
    Ok(ExitCode::Success)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use clap::CommandFactory;

    use super::{Cli, unreachable_outcome_class};
    use crate::api::UnreachableCategory;
    use crate::exit_code::OutcomeClass;

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

    fn customer_command_paths() -> Vec<String> {
        let mut paths = Vec::new();
        collect_command_paths(&Cli::command(), "", &mut paths);
        paths.sort();
        paths.dedup();
        paths
    }

    fn help_snapshot_paths() -> Vec<String> {
        let snapshot_directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/cmd/help");
        let mut paths = snapshot_directory
            .read_dir()
            .expect("help snapshot directory should exist")
            .filter_map(|entry| {
                let path = entry
                    .expect("help snapshot entry should be readable")
                    .path();
                path.extension()
                    .is_some_and(|extension| extension == "trycmd")
                    .then_some(path)
            })
            .map(|path| {
                let snapshot =
                    fs::read_to_string(path).expect("help snapshot should be readable as UTF-8");
                snapshot
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("$ scherzo-cloud")
                            .and_then(|invocation| invocation.strip_suffix(" --help"))
                    })
                    .expect("help snapshot should declare its command invocation")
                    .trim()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }

    #[test]
    fn every_unreachable_category_uses_the_shared_outcome_table() {
        for category in [
            UnreachableCategory::Dns,
            UnreachableCategory::Timeout,
            UnreachableCategory::Connection,
            UnreachableCategory::Tls,
            UnreachableCategory::Server,
        ] {
            assert_eq!(
                unreachable_outcome_class(category),
                OutcomeClass::Unreachable
            );
        }
        assert_eq!(
            unreachable_outcome_class(UnreachableCategory::RateLimited),
            OutcomeClass::RateLimited
        );
    }

    #[test]
    fn every_customer_command_has_one_help_snapshot() {
        let mut command_paths = vec![String::new()];
        command_paths.extend(customer_command_paths());

        assert_eq!(help_snapshot_paths(), command_paths);
    }

    #[test]
    fn customer_command_surface_is_exact_and_has_no_operator_entrypoint() {
        let actual = customer_command_paths();
        let expected = [
            "account",
            "account signup",
            "artifact",
            "artifact download",
            "artifact validate",
            "auth",
            "auth login",
            "auth logout",
            "auth status",
            "github",
            "github installation",
            "github installation disconnect",
            "github installation list",
            "github repository",
            "github repository list",
            "github setup",
            "github setup begin",
            "github setup complete",
            "organization",
            "organization create",
            "organization list",
            "organization members",
            "organization members list",
            "organization show",
            "organization update",
            "project",
            "project create",
            "project list",
            "project rename",
            "project repository",
            "project repository detach",
            "project repository installation",
            "project repository installation list",
            "project repository list",
            "project repository set",
            "project repository show",
            "project repository update",
            "project runner-pool",
            "project runner-pool remove",
            "project runner-pool set",
            "project show",
            "run",
            "run create",
            "run show",
            "run wait",
            "runner",
            "runner activation",
            "runner activation create",
            "runner activation list",
            "runner activation revoke",
            "runner create",
            "runner credential",
            "runner credential list",
            "runner credential retire",
            "runner credential revoke",
            "runner delete",
            "runner disable",
            "runner doctor",
            "runner drain",
            "runner enable",
            "runner enroll",
            "runner list",
            "runner move",
            "runner pool",
            "runner pool create",
            "runner pool delete",
            "runner pool list",
            "runner pool rename",
            "runner pool show",
            "runner rename",
            "runner serve",
            "runner show",
            "runner status",
            "version",
            "workflow",
            "workflow reference",
            "workflow retry",
            "workflow run",
            "workflow schema",
            "workflow status",
            "workflow validate",
            "workflow view",
        ];

        assert_eq!(actual, expected);
    }
}
