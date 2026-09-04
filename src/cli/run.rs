use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand, builder::NonEmptyStringValueParser};
use serde::Serialize;

use crate::api::{HttpClient, HttpTransportPolicy, Run, RunApi, RunFailure, RunState};
use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::deployment::Deployment;
use crate::human_auth::session::{self, RequiredOperation};

pub(super) const ABOUT: &str = "Work with Scherzo Cloud runs";
const NAME: &str = "run";
const WAIT_POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAXIMUM_CONSECUTIVE_OBSERVATION_FAILURES: usize = 2;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<RunCommand>,
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    #[command(about = "Create an inputless Scherzo Cloud run")]
    Create(CreateCommand),
    #[command(about = "Show a Scherzo Cloud run")]
    Show(ShowCommand),
    #[command(about = "Wait for a Scherzo Cloud run")]
    Wait(WaitCommand),
}

#[derive(Debug, Args)]
struct RunOptions {
    #[arg(long, help = "Print the run result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::HttpOptions,
}

// This leaf keeps its API identities explicit; sharing Clap fields with runner-pool
// creation would couple unrelated command contracts and their help text.
// jscpd:ignore-start
#[derive(Debug, Args)]
struct CreateCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(long, value_name = "PROJECT", help = "Exact Project ID")]
    project_id: String,
    // jscpd:ignore-end
    #[arg(
        long,
        value_name = "WORKFLOW",
        help = "Canonical repository-relative workflow path"
    )]
    workflow_path: String,

    #[arg(
        long,
        value_parser = NonEmptyStringValueParser::new(),
        help = "Exact source branch (the project default when omitted)"
    )]
    source_branch: Option<String>,

    #[arg(
        long,
        value_parser = NonEmptyStringValueParser::new(),
        help = "Set the run display name"
    )]
    display_name: Option<String>,

    #[command(flatten)]
    options: RunOptions,
}

#[derive(Debug, Args)]
struct RunReference {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(value_name = "RUN", help = "Exact Run ID")]
    run_id: String,
}

#[derive(Debug, Args)]
struct ShowCommand {
    #[command(flatten)]
    run: RunReference,

    #[command(flatten)]
    options: RunOptions,
}

#[derive(Debug, Args)]
struct WaitCommand {
    #[command(flatten)]
    run: RunReference,

    #[arg(
        long,
        value_name = "DURATION",
        value_parser = parse_wait_timeout,
        help = "Stop waiting after a positive duration (units: ms, s, m, or h)"
    )]
    timeout: Option<Duration>,

    #[command(flatten)]
    options: RunOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(RunCommand::Create(command)) => super::execute_deployment_command(
                Some(command),
                &[NAME],
                "configure Scherzo Cloud run creation",
                |command, deployment| command.execute(deployment.clone()),
            ),
            Some(RunCommand::Show(command)) => super::execute_deployment_command(
                Some(command),
                &[NAME],
                "configure Scherzo Cloud run access",
                |command, deployment| command.execute(deployment.clone()),
            ),
            Some(RunCommand::Wait(command)) => super::execute_deployment_command(
                Some(command),
                &[NAME],
                "configure Scherzo Cloud run observation",
                |command, deployment| command.execute(deployment.clone()),
            ),
        }
    }
}

struct CreateDispatchState {
    dispatched: AtomicBool,
}

impl CreateCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        let state = Arc::new(CreateDispatchState {
            dispatched: AtomicBool::new(false),
        });
        let operation_state = Arc::clone(&state);
        let signal_state = Arc::clone(&state);
        let signal_deployment = deployment.clone();
        let signal_organization = self.organization.clone();
        let signal_json = self.options.json;
        super::execute_mutation_with_signals(
            "Cloud run creation",
            move |cancelled, completed| {
                self.execute_blocking(&deployment, &operation_state, cancelled, completed)
            },
            move |signal| {
                if !signal_state.dispatched.load(Ordering::Acquire) {
                    return Ok(signal);
                }
                write_create_unknown(
                    signal_deployment.fingerprint().api_url(),
                    &signal_organization,
                    signal_json,
                    signal,
                )
                .map_err(Into::into)
            },
        )
    }

    fn execute_blocking(
        self,
        deployment: &Deployment,
        state: &CreateDispatchState,
        cancelled: &AtomicBool,
        completed: &AtomicBool,
    ) -> super::CommandResult {
        let idempotency_key = crate::idempotency::generate_idempotency_key()
            .context("generate Cloud run request identity")?;
        if cancelled.load(Ordering::Acquire) {
            return Ok(ExitCode::GeneralFailure);
        }
        let result = with_api(deployment, self.options.http.transport_policy(), |api| {
            state.dispatched.store(true, Ordering::Release);
            api.create(
                &self.organization,
                &idempotency_key,
                &self.project_id,
                &self.workflow_path,
                self.source_branch.as_deref(),
                self.display_name.as_deref(),
            )
        })?;
        if cancelled.load(Ordering::Acquire) {
            return Ok(ExitCode::GeneralFailure);
        }
        completed.store(true, Ordering::Release);
        write_create(
            deployment.fingerprint().api_url(),
            &self.organization,
            result,
            self.options.json,
        )
        .map_err(Into::into)
    }
}

impl ShowCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        super::execute_read_only_with_signals("Cloud run show", move |cancelled, completed| {
            self.execute_blocking(&deployment, cancelled, completed)
        })
    }

    fn execute_blocking(
        self,
        deployment: &Deployment,
        cancelled: &AtomicBool,
        completed: &AtomicBool,
    ) -> super::CommandResult {
        let result = with_api(deployment, self.options.http.transport_policy(), |api| {
            api.get(&self.run.organization, &self.run.run_id)
        })?;
        if cancelled.load(Ordering::Acquire) {
            return Ok(ExitCode::GeneralFailure);
        }
        completed.store(true, Ordering::Release);
        write_show(
            deployment.fingerprint().api_url(),
            &self.run.organization,
            &self.run.run_id,
            result,
            self.options.json,
        )
        .map_err(Into::into)
    }
}

impl WaitCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        let timeout = self.timeout;
        let timeout_deployment = deployment.fingerprint().api_url().to_owned();
        let timeout_organization = self.run.organization.clone();
        let timeout_run_id = self.run.run_id.clone();
        let timeout_json = self.options.json;
        let operation = move |control: &super::BlockingObservationControl| {
            self.execute_blocking(&deployment, control)
        };

        super::execute_observation_with_signals_and_timeout(
            "Cloud run wait",
            timeout,
            operation,
            move || {
                write_wait_timeout(
                    &timeout_deployment,
                    &timeout_organization,
                    &timeout_run_id,
                    timeout_json,
                )
                .map_err(Into::into)
            },
        )
    }

    fn execute_blocking(
        self,
        deployment: &Deployment,
        control: &super::BlockingObservationControl,
    ) -> super::CommandResult {
        let clock = SystemWaitClock;
        let started_at = clock.now();
        let result = with_api(deployment, self.options.http.transport_policy(), |api| {
            wait_for_terminal_run(
                api,
                &self.run.organization,
                &self.run.run_id,
                started_at,
                self.timeout,
                control,
                &clock,
            )
        })?;
        if !control.begin_completion() {
            return Ok(ExitCode::GeneralFailure);
        }
        match result {
            Ok(WaitObservation::Terminal { run, state }) => write_wait_terminal(
                deployment.fingerprint().api_url(),
                &run,
                state,
                self.options.json,
            ),
            Ok(WaitObservation::TimedOut) => write_wait_timeout(
                deployment.fingerprint().api_url(),
                &self.run.organization,
                &self.run.run_id,
                self.options.json,
            ),
            Ok(WaitObservation::Stopped) => Ok(ExitCode::GeneralFailure),
            Err(failure) => write_failure(
                deployment.fingerprint().api_url(),
                &self.run.organization,
                Some(&self.run.run_id),
                &failure,
                self.options.json,
            ),
        }
        .map_err(Into::into)
    }
}

trait RunObservationApi {
    fn get_run(&self, organization: &str, run_id: &str) -> Result<Run, RunFailure>;
}

impl RunObservationApi for RunApi {
    fn get_run(&self, organization: &str, run_id: &str) -> Result<Run, RunFailure> {
        self.get(organization, run_id)
    }
}

trait WaitClock {
    fn now(&self) -> Instant;
    fn sleep(&self, duration: Duration);
}

struct SystemWaitClock;

impl WaitClock for SystemWaitClock {
    fn now(&self) -> Instant {
        crate::timing::monotonic_now()
    }

    fn sleep(&self, duration: Duration) {
        crate::timing::sleep(duration);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalRunState {
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
    Rejected,
}

impl TerminalRunState {
    const fn outcome(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
            Self::Rejected => "rejected",
        }
    }

    const fn heading(self) -> &'static str {
        match self {
            Self::Succeeded => "✓ Run succeeded.",
            Self::Failed => "✗ Run failed.",
            Self::Cancelled => "✗ Run cancelled.",
            Self::Interrupted => "✗ Run interrupted.",
            Self::Rejected => "✗ Run rejected.",
        }
    }

    const fn exit_code(self) -> ExitCode {
        match self {
            Self::Succeeded => ExitCode::Success,
            Self::Failed | Self::Cancelled | Self::Interrupted | Self::Rejected => {
                ExitCode::GeneralFailure
            }
        }
    }
}

enum WaitObservation {
    Terminal {
        run: Box<Run>,
        state: TerminalRunState,
    },
    TimedOut,
    Stopped,
}

fn wait_for_terminal_run(
    api: &impl RunObservationApi,
    organization: &str,
    run_id: &str,
    started_at: Instant,
    timeout: Option<Duration>,
    control: &super::BlockingObservationControl,
    clock: &impl WaitClock,
) -> Result<WaitObservation, RunFailure> {
    let mut consecutive_failures = 0;
    loop {
        if control.is_stopped() {
            return Ok(WaitObservation::Stopped);
        }
        if remaining_wait(timeout, started_at, clock.now()).is_none() {
            return Ok(WaitObservation::TimedOut);
        }

        match api.get_run(organization, run_id) {
            Ok(run) => {
                consecutive_failures = 0;
                if let Some(state) = terminal_run_state(run.state) {
                    return Ok(WaitObservation::Terminal {
                        run: Box::new(run),
                        state,
                    });
                }
            }
            Err(failure)
                if failure.retryable_observation()
                    && consecutive_failures + 1 < MAXIMUM_CONSECUTIVE_OBSERVATION_FAILURES =>
            {
                consecutive_failures += 1;
            }
            Err(failure) => return Err(failure),
        }

        if control.is_stopped() {
            return Ok(WaitObservation::Stopped);
        }
        let Some(remaining) = remaining_wait(timeout, started_at, clock.now()) else {
            return Ok(WaitObservation::TimedOut);
        };
        clock.sleep(WAIT_POLL_INTERVAL.min(remaining));
    }
}

fn remaining_wait(
    timeout: Option<Duration>,
    started_at: Instant,
    now: Instant,
) -> Option<Duration> {
    match timeout {
        Some(timeout) => timeout
            .checked_sub(now.saturating_duration_since(started_at))
            .filter(|remaining| !remaining.is_zero()),
        None => Some(WAIT_POLL_INTERVAL),
    }
}

const fn terminal_run_state(state: RunState) -> Option<TerminalRunState> {
    match state {
        RunState::Queued
        | RunState::Assigning
        | RunState::Preparing
        | RunState::Assigned
        | RunState::Running => None,
        RunState::Succeeded => Some(TerminalRunState::Succeeded),
        RunState::Failed => Some(TerminalRunState::Failed),
        RunState::Cancelled => Some(TerminalRunState::Cancelled),
        RunState::Interrupted => Some(TerminalRunState::Interrupted),
        RunState::Rejected => Some(TerminalRunState::Rejected),
    }
}

fn parse_wait_timeout(value: &str) -> Result<Duration, String> {
    let (quantity, milliseconds) = if let Some(quantity) = value.strip_suffix("ms") {
        (quantity, 1)
    } else if let Some(quantity) = value.strip_suffix('s') {
        (quantity, 1_000)
    } else if let Some(quantity) = value.strip_suffix('m') {
        (quantity, 60_000)
    } else if let Some(quantity) = value.strip_suffix('h') {
        (quantity, 3_600_000)
    } else {
        (value, 1_000)
    };
    let quantity = quantity
        .parse::<u64>()
        .map_err(|_| "duration must be a positive integer followed by ms, s, m, or h".to_owned())?;
    let total_milliseconds = quantity
        .checked_mul(milliseconds)
        .filter(|duration| *duration > 0)
        .ok_or_else(|| {
            "duration must be a positive integer followed by ms, s, m, or h".to_owned()
        })?;
    Ok(Duration::from_millis(total_milliseconds))
}

fn with_api<T>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    mut operation: impl FnMut(&RunApi) -> Result<T, RunFailure>,
) -> anyhow::Result<Result<T, RunFailure>> {
    let client = HttpClient::new(transport_policy)
        .map_err(|error| anyhow!(error))
        .context("prepare human session networking")?;
    match session::execute_required(
        &client,
        deployment,
        |access_token| {
            let api = RunApi::new(
                deployment.fingerprint().api_url(),
                access_token.expose(),
                transport_policy,
            )
            .map_err(|error| anyhow!(error))
            .context("prepare Cloud run networking")?;
            Ok(operation(&api))
        },
        |result| {
            result.as_ref().is_ok_and(|operation| {
                operation
                    .as_ref()
                    .is_err_and(RunFailure::credential_rejected)
            })
        },
    ) {
        Ok(RequiredOperation::Unauthenticated) => Ok(Err(RunFailure::Unauthenticated)),
        Ok(RequiredOperation::Completed(result)) => result,
        Err(error) => match error.unreachable_category() {
            Some(category) => Ok(Err(RunFailure::Unreachable(category))),
            None => Err(anyhow!(error).context("acquire human session for Cloud run operation")),
        },
    }
}

fn write_create(
    deployment: &str,
    organization: &str,
    result: Result<crate::api::RunCreationAcceptance, RunFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(acceptance) => {
            if json {
                write_json(&CreateResult {
                    schema_version: 1,
                    deployment,
                    outcome: "accepted",
                    organization_ref: organization,
                    run_id: &acceptance.run_id,
                    replayed: acceptance.replayed,
                })?;
            } else {
                let stdout = io::stdout();
                let mut stdout = stdout.lock();
                writeln!(stdout, "✓ Run accepted.\n")?;
                writeln!(stdout, "run: {}", acceptance.run_id)?;
                writeln!(
                    stdout,
                    "replayed: {}",
                    if acceptance.replayed { "yes" } else { "no" }
                )?;
                writeln!(stdout, "organization: {organization}")?;
                writeln!(stdout, "deployment: {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(deployment, organization, None, &failure, json),
    }
}

fn write_show(
    deployment: &str,
    organization: &str,
    requested_run_id: &str,
    result: Result<Run, RunFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(run) => {
            if json {
                write_json(&ShowResult {
                    schema_version: 1,
                    deployment,
                    outcome: "found",
                    run: &run,
                })?;
            } else {
                write_run_human(deployment, "✓ Run found.", &run)?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(
            deployment,
            organization,
            Some(requested_run_id),
            &failure,
            json,
        ),
    }
}

fn write_wait_terminal(
    deployment: &str,
    run: &Run,
    state: TerminalRunState,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&WaitResult {
            schema_version: 1,
            deployment,
            outcome: state.outcome(),
            run,
        })?;
    } else {
        write_run_human(deployment, state.heading(), run)?;
    }
    Ok(state.exit_code())
}

fn write_wait_timeout(
    deployment: &str,
    organization: &str,
    run_id: &str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&WaitTimeoutResult {
            schema_version: 1,
            deployment,
            outcome: "timed_out",
            organization_ref: organization,
            run_id,
        })?;
    } else {
        writeln!(
            io::stderr().lock(),
            "error: Cloud run wait reached its timeout\n\nrun: {run_id}\norganization: {organization}\n\nRun the command again with a longer --timeout, or omit --timeout."
        )?;
    }
    Ok(ExitCode::GeneralFailure)
}

fn write_run_human(deployment: &str, heading: &str, run: &Run) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "{heading}\n")?;
    writeln!(stdout, "run: {}", run.id)?;
    writeln!(
        stdout,
        "display name: {}",
        run.display_name.as_deref().unwrap_or("none")
    )?;
    writeln!(stdout, "organization: {}", run.organization_id)?;
    writeln!(stdout, "project: {}", run.project_id)?;
    writeln!(stdout, "execution spec: {}", run.execution_spec_id)?;
    writeln!(stdout, "state: {}", enum_text(&run.state)?)?;
    writeln!(stdout, "version: {}", run.version)?;
    writeln!(
        stdout,
        "attempt: {} (number {})",
        run.current_attempt_id, run.current_attempt_number
    )?;
    writeln!(stdout, "source branch: {}", run.source_branch)?;
    writeln!(stdout, "\nworkflow source:")?;
    writeln!(
        stdout,
        "  repository connection: {}",
        run.workflow_definition_source.repository_connection_id
    )?;
    writeln!(
        stdout,
        "  object format: {}",
        enum_text(&run.workflow_definition_source.object_format)?
    )?;
    writeln!(
        stdout,
        "  commit: {}",
        run.workflow_definition_source.commit_oid
    )?;
    writeln!(
        stdout,
        "  workflow: {}",
        run.workflow_definition_source.workflow_path
    )?;
    writeln!(
        stdout,
        "  source closure: {}:{}",
        enum_text(
            &run.workflow_definition_source
                .workflow_source_closure_digest
                .algorithm
        )?,
        run.workflow_definition_source
            .workflow_source_closure_digest
            .value
    )?;
    writeln!(stdout, "\nprimary workspace source:")?;
    writeln!(
        stdout,
        "  kind: {}",
        enum_text(&run.primary_workspace_source.kind)?
    )?;
    writeln!(
        stdout,
        "  provider: {}",
        enum_text(&run.primary_workspace_source.provider_kind)?
    )?;
    writeln!(
        stdout,
        "  repository connection: {}",
        run.primary_workspace_source.repository_connection_id
    )?;
    writeln!(
        stdout,
        "  object format: {}",
        enum_text(&run.primary_workspace_source.object_format)?
    )?;
    writeln!(
        stdout,
        "  commit: {}",
        run.primary_workspace_source.commit_oid
    )?;
    writeln!(
        stdout,
        "  materialization: {}",
        enum_text(&run.primary_workspace_source.materialization_contract)?
    )?;
    writeln!(stdout, "\ninputs:")?;
    writeln!(
        stdout,
        "  input set: {}",
        run.inputs.input_set_id.as_deref().unwrap_or("none")
    )?;
    writeln!(
        stdout,
        "  prompt: {}",
        if run.inputs.prompt_present {
            "yes"
        } else {
            "no"
        }
    )?;
    writeln!(stdout, "  attachments: {}", run.inputs.attachment_count)?;
    writeln!(stdout, "  bytes: {}", run.inputs.aggregate_bytes)?;
    writeln!(
        stdout,
        "  availability: {}",
        enum_text(&run.inputs.availability)?
    )?;
    writeln!(stdout, "\ncreated: {}", run.created_at)?;
    writeln!(stdout, "updated: {}", run.updated_at)?;
    writeln!(stdout, "deployment: {deployment}")?;
    Ok(())
}

fn enum_text(value: &impl Serialize) -> anyhow::Result<String> {
    match serde_json::to_value(value).context("serialize Cloud run field")? {
        serde_json::Value::String(value) => Ok(value),
        _ => Err(anyhow!("Cloud run field is not a contracted string")),
    }
}

fn write_failure(
    deployment: &str,
    organization: &str,
    run_id: Option<&str>,
    failure: &RunFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (outcome, category, human, class) = match failure {
        RunFailure::Unauthenticated => (
            "unauthenticated",
            None,
            "error: Cloud run access requires sign-in\n\nSign in first:\n  scherzo-cloud auth login".to_owned(),
            OutcomeClass::Unauthenticated,
        ),
        RunFailure::Forbidden => (
            "forbidden",
            None,
            "error: Cloud run operation is not permitted for this account\n\nAsk an organization owner to perform this operation.".to_owned(),
            OutcomeClass::Forbidden,
        ),
        RunFailure::InvalidInput => (
            "invalid_input",
            None,
            format!("error: Cloud run input rejected by {deployment}\n\nCheck the organization, project, workflow path, and optional values, then try again."),
            OutcomeClass::GeneralFailure,
        ),
        RunFailure::NotFound => (
            "not_found",
            None,
            "error: Cloud run resource not found or unavailable\n\nCheck the organization and resource identifier, then try again.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunFailure::Conflict => (
            "conflict",
            None,
            "error: Cloud run request conflicts with current state\n\nCheck project readiness and source availability, then try again.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunFailure::Unreachable(category) => (
            "unreachable",
            Some(category.as_str()),
            format!("error: contact Cloud run API at {deployment}: {}\n\nCheck network access to the deployment and try again.", category.as_str()),
            super::unreachable_outcome_class(*category),
        ),
        RunFailure::Protocol { .. } => (
            "invalid_response",
            None,
            "error: Cloud run API response does not match the public contract\n\nTry again later.".to_owned(),
            OutcomeClass::Protocol,
        ),
    };
    if json {
        write_json(&FailureResult {
            schema_version: 1,
            deployment,
            outcome,
            organization_ref: organization,
            run_id,
            category,
        })?;
    } else {
        writeln!(io::stderr().lock(), "{human}")?;
    }
    Ok(class.exit_code())
}

fn write_create_unknown(
    deployment: &str,
    organization: &str,
    json: bool,
    exit_code: ExitCode,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&UnknownCreateResult {
            schema_version: 1,
            deployment,
            outcome: "unknown",
            organization_ref: organization,
            commitment: "unknown",
        })?;
    } else {
        writeln!(
            io::stderr().lock(),
            "error: run acceptance is unknown after interruption\n\norganization: {organization}\ncommitment: unknown\n\nThe CLI cannot safely determine whether the run was accepted. Inspect the deployment before creating another run."
        )?;
    }
    Ok(exit_code)
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, value).context("serialize JSON Cloud run result")?;
    writeln!(stdout).context("write Cloud run result")
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    run_id: &'a str,
    replayed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShowResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    run: &'a Run,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WaitResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    run: &'a Run,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WaitTimeoutResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    run_id: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FailureResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UnknownCreateResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    commitment: &'static str,
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;

    use super::*;
    use crate::api::UnreachableCategory;

    struct ScriptedObservationApi {
        responses: RefCell<VecDeque<Result<Run, RunFailure>>>,
    }

    impl ScriptedObservationApi {
        fn new(responses: impl IntoIterator<Item = Result<Run, RunFailure>>) -> Self {
            Self {
                responses: RefCell::new(responses.into_iter().collect()),
            }
        }
    }

    impl RunObservationApi for ScriptedObservationApi {
        fn get_run(&self, _organization: &str, _run_id: &str) -> Result<Run, RunFailure> {
            self.responses
                .borrow_mut()
                .pop_front()
                .expect("the polling scenario should provide another response")
        }
    }

    struct ControlledWaitClock {
        now: Cell<Instant>,
        sleeps: RefCell<Vec<Duration>>,
    }

    impl ControlledWaitClock {
        fn new(now: Instant) -> Self {
            Self {
                now: Cell::new(now),
                sleeps: RefCell::new(Vec::new()),
            }
        }
    }

    impl WaitClock for ControlledWaitClock {
        fn now(&self) -> Instant {
            self.now.get()
        }

        fn sleep(&self, duration: Duration) {
            self.sleeps.borrow_mut().push(duration);
            self.now.set(self.now.get() + duration);
        }
    }

    fn observe(
        api: &ScriptedObservationApi,
        timeout: Option<Duration>,
        clock: &ControlledWaitClock,
    ) -> Result<WaitObservation, RunFailure> {
        wait_for_terminal_run(
            api,
            "acme-research",
            "run_01k0z6r1w8f4jy2m7q9v3x5abc",
            clock.now(),
            timeout,
            &super::super::BlockingObservationControl::new(),
            clock,
        )
    }

    fn run(state: RunState) -> Run {
        let state = serde_json::to_value(state).expect("run state should serialize");
        serde_json::from_value(serde_json::json!({
            "id": "run_01k0z6r1w8f4jy2m7q9v3x5abc",
            "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
            "projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
            "displayName": null,
            "executionSpecId": "xsp_01k0z6r1w8f4jy2m7q9v3x5abc",
            "state": state,
            "version": 1,
            "currentAttemptId": "atm_01k0z6r1w8f4jy2m7q9v3x5abc",
            "currentAttemptNumber": 1,
            "sourceBranch": "main",
            "workflowDefinitionSource": {
                "repositoryConnectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
                "objectFormat": "sha1",
                "commitOid": "0123456789abcdef0123456789abcdef01234567",
                "workflowPath": "workflow.yaml",
                "workflowSourceClosureDigest": {
                    "algorithm": "sha256",
                    "value": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                }
            },
            "primaryWorkspaceSource": {
                "kind": "connected_repository",
                "providerKind": "github",
                "repositoryConnectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
                "objectFormat": "sha1",
                "commitOid": "0123456789abcdef0123456789abcdef01234567",
                "materializationContract": "git_full_clone_v1"
            },
            "inputs": {
                "inputSetId": null,
                "promptPresent": false,
                "attachmentCount": 0,
                "aggregateBytes": 0,
                "availability": "available"
            },
            "createdAt": "2026-08-10T12:00:00Z",
            "updatedAt": "2026-08-10T12:00:00Z"
        }))
        .expect("run fixture should match the generated model")
    }

    #[test]
    fn wait_polls_every_nonterminal_state_until_each_terminal_state() {
        let nonterminal = [
            RunState::Queued,
            RunState::Assigning,
            RunState::Preparing,
            RunState::Assigned,
            RunState::Running,
        ];
        let terminal = [
            (RunState::Succeeded, TerminalRunState::Succeeded),
            (RunState::Failed, TerminalRunState::Failed),
            (RunState::Cancelled, TerminalRunState::Cancelled),
            (RunState::Interrupted, TerminalRunState::Interrupted),
            (RunState::Rejected, TerminalRunState::Rejected),
        ];

        for (terminal_state, expected) in terminal {
            let responses = nonterminal
                .into_iter()
                .chain([terminal_state])
                .map(|state| Ok(run(state)));
            let api = ScriptedObservationApi::new(responses);
            let started_at = crate::timing::monotonic_now();
            let clock = ControlledWaitClock::new(started_at);

            let result = observe(&api, None, &clock).expect("the polling scenario should complete");

            match result {
                WaitObservation::Terminal { run, state } => {
                    assert_eq!(run.state, terminal_state);
                    assert_eq!(state, expected);
                }
                WaitObservation::TimedOut | WaitObservation::Stopped => {
                    panic!("the polling scenario did not observe its terminal run")
                }
            }
            assert_eq!(
                clock.sleeps.into_inner(),
                vec![WAIT_POLL_INTERVAL; nonterminal.len()]
            );
        }
    }

    #[test]
    fn wait_recovers_from_one_retryable_observation_failure() {
        let api = ScriptedObservationApi::new([
            Err(RunFailure::Unreachable(UnreachableCategory::Server)),
            Ok(run(RunState::Succeeded)),
        ]);
        let started_at = crate::timing::monotonic_now();
        let clock = ControlledWaitClock::new(started_at);

        let result = observe(&api, None, &clock)
            .expect("one recoverable failure should not end observation");

        assert!(matches!(
            result,
            WaitObservation::Terminal {
                state: TerminalRunState::Succeeded,
                ..
            }
        ));
        assert_eq!(clock.sleeps.into_inner(), vec![WAIT_POLL_INTERVAL]);
    }

    #[test]
    fn wait_timeout_uses_the_remaining_duration_without_an_extra_request() {
        let api = ScriptedObservationApi::new([
            Ok(run(RunState::Queued)),
            Ok(run(RunState::Assigned)),
            Ok(run(RunState::Running)),
        ]);
        let started_at = crate::timing::monotonic_now();
        let clock = ControlledWaitClock::new(started_at);

        let result = observe(&api, Some(Duration::from_secs(5)), &clock)
            .expect("timeout is a local wait outcome");

        assert!(matches!(result, WaitObservation::TimedOut));
        assert_eq!(
            clock.sleeps.into_inner(),
            vec![
                Duration::from_secs(2),
                Duration::from_secs(2),
                Duration::from_secs(1)
            ]
        );
        assert!(api.responses.into_inner().is_empty());
    }

    #[test]
    fn wait_bounds_retries_and_preserves_the_transport_failure() {
        let failure = RunFailure::Unreachable(UnreachableCategory::Connection);
        let api = ScriptedObservationApi::new([Err(failure), Err(failure)]);
        let started_at = crate::timing::monotonic_now();
        let clock = ControlledWaitClock::new(started_at);

        let result = observe(&api, None, &clock);

        assert_eq!(result.err(), Some(failure));
        assert_eq!(clock.sleeps.into_inner(), vec![WAIT_POLL_INTERVAL]);
    }

    #[test]
    fn wait_timeout_parser_accepts_documented_units_and_rejects_zero() {
        assert_eq!(parse_wait_timeout("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_wait_timeout("30"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_wait_timeout("10m"), Ok(Duration::from_secs(600)));
        assert_eq!(parse_wait_timeout("2h"), Ok(Duration::from_secs(7_200)));
        assert!(parse_wait_timeout("0s").is_err());
        assert!(parse_wait_timeout("1.5s").is_err());
    }
}
