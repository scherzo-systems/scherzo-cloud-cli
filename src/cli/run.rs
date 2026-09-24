use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand, builder::NonEmptyStringValueParser};
use serde::Serialize;

use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::deployment::Deployment;
#[cfg(test)]
use scherzo_cloud_api::HttpClient;
use scherzo_cloud_api::{
    CreateRunInput, HttpTransportPolicy, Run, RunApi, RunArtifactDelivery, RunFailure, RunRead,
    RunState,
};
use scherzo_cloud_execution::visible_text;

use super::{OrganizationArg, ProjectArg};

mod acquisition;
mod input_set;
mod inputs;

pub(super) const ABOUT: &str = "Work with Scherzo Cloud runs";
const NAME: &str = "run";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<RunCommand>,
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    #[command(about = "Create a Scherzo Cloud run")]
    Create(CreateCommand),
    #[command(about = inputs::ABOUT)]
    Input(inputs::Command),
    #[command(about = input_set::ABOUT)]
    InputSet(input_set::Command),
    #[command(about = "Show a Scherzo Cloud run")]
    Show(ShowCommand),
    #[command(about = "Wait for a Scherzo Cloud run")]
    Wait(WaitCommand),
}

type RunOptions = super::CommonArgs<super::RunJson, super::PrincipalAuthenticationArgs>;
type CloudInputOptions = super::CommonArgs<super::RunJson, super::PrincipalAuthenticationArgs>;

// This leaf keeps its API identities explicit; sharing Clap fields with runner-pool
// creation would couple unrelated command contracts and their help text.
// jscpd:ignore-start
#[derive(Debug, Args)]
struct CreateCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(long, value_name = ProjectArg::VALUE_NAME, help = ProjectArg::HELP)]
    project_id: ProjectArg,
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

    #[arg(
        long,
        value_name = "INPUT_SET",
        value_parser = parse_input_set_id,
        conflicts_with_all = [
            "input_text",
            "input_text_file",
            "input_json",
            "input_json_file",
            "input_file",
            "input_attachment",
            "input_attachments_empty"
        ],
        help = "Consume an existing sealed Run Input Set without restaging"
    )]
    input_set_id: Option<String>,

    #[command(flatten)]
    inputs: super::NamedInputArgs,

    #[arg(
        long,
        value_name = "PATH",
        help = "Read private immutable integration context from a JSON file, or - for standard input"
    )]
    integration_context_file: Option<PathBuf>,

    #[command(flatten)]
    options: RunOptions,
}

#[derive(Debug, Args)]
struct RunReference {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

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

    #[command(flatten)]
    wait: super::WaitTimeoutArgs,

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
            Some(RunCommand::InputSet(command)) => command.execute(),
            Some(RunCommand::Input(command)) => command.execute(),
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum CreateInputSetOwnership {
    Explicit(String),
    Allocated(String),
}

impl CreateInputSetOwnership {
    fn id(&self) -> &str {
        match self {
            Self::Explicit(input_set_id) | Self::Allocated(input_set_id) => input_set_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CreateRecoveryState {
    BeforeRunDispatch(Option<CreateInputSetOwnership>),
    InputSetAllocationDispatched,
    RunDispatched(Option<CreateInputSetOwnership>),
}

impl CreateRecoveryState {
    fn new(explicit_input_set_id: Option<&str>) -> Self {
        Self::BeforeRunDispatch(
            explicit_input_set_id
                .map(|input_set_id| CreateInputSetOwnership::Explicit(input_set_id.to_owned())),
        )
    }

    fn input_set_allocation_dispatched() -> Self {
        Self::InputSetAllocationDispatched
    }

    fn allocated_input_set(input_set_id: &str) -> Self {
        Self::BeforeRunDispatch(Some(CreateInputSetOwnership::Allocated(
            input_set_id.to_owned(),
        )))
    }

    fn run_dispatched(&self) -> Self {
        match self {
            Self::BeforeRunDispatch(input_set) | Self::RunDispatched(input_set) => {
                Self::RunDispatched(input_set.clone())
            }
            Self::InputSetAllocationDispatched => Self::RunDispatched(None),
        }
    }

    fn input_set_id(&self) -> Option<&str> {
        match self {
            Self::BeforeRunDispatch(Some(input_set)) | Self::RunDispatched(Some(input_set)) => {
                Some(input_set.id())
            }
            Self::BeforeRunDispatch(None)
            | Self::InputSetAllocationDispatched
            | Self::RunDispatched(None) => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CreateSignalRecovery {
    None,
    InputSetUnknown,
    InputSet(String),
    Run(Option<String>),
}

fn create_signal_recovery(
    snapshot: super::SignalSnapshot<CreateRecoveryState>,
) -> CreateSignalRecovery {
    if !snapshot.dispatched {
        return CreateSignalRecovery::None;
    }
    match snapshot.recovery {
        CreateRecoveryState::InputSetAllocationDispatched => CreateSignalRecovery::InputSetUnknown,
        CreateRecoveryState::BeforeRunDispatch(Some(CreateInputSetOwnership::Allocated(
            input_set_id,
        ))) => CreateSignalRecovery::InputSet(input_set_id),
        CreateRecoveryState::BeforeRunDispatch(_) => CreateSignalRecovery::None,
        CreateRecoveryState::RunDispatched(input_set) => {
            CreateSignalRecovery::Run(input_set.map(|input_set| input_set.id().to_owned()))
        }
    }
}

fn finish_operation<R>(
    control: &super::OperationControl<R>,
    write_result: impl FnOnce() -> anyhow::Result<ExitCode>,
) -> super::CommandResult {
    super::complete_operation(control, || write_result().map_err(Into::into))
}

fn write_api_outcome<T>(
    result: Result<T, RunFailure>,
    write_success: impl FnOnce(T) -> anyhow::Result<()>,
    write_failure: impl FnOnce(&RunFailure) -> anyhow::Result<ExitCode>,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(value) => {
            write_success(value)?;
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(&failure),
    }
}

fn finish_create(
    deployment: &Deployment,
    organization: &str,
    input_set_id: Option<&str>,
    result: Result<scherzo_cloud_api::RunCreationAcceptance, RunFailure>,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
    control: &super::OperationControl<CreateRecoveryState>,
) -> super::CommandResult {
    finish_operation(control, || {
        write_create(
            deployment.fingerprint().api_url(),
            organization,
            input_set_id,
            result,
            authentication,
            json,
        )
    })
}

impl CreateCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        let recovery = CreateRecoveryState::new(self.input_set_id.as_deref());
        let signal_deployment = deployment.clone();
        let signal_organization = self.organization.clone();
        let signal_json = self.options.json;
        super::execute_mutation_with_signals(
            "Cloud run creation",
            recovery,
            move |control| self.execute_blocking(&deployment, control),
            move |signal, snapshot| match create_signal_recovery(snapshot) {
                CreateSignalRecovery::Run(input_set_id) => write_create_unknown(
                    signal_deployment.fingerprint().api_url(),
                    &signal_organization,
                    input_set_id.as_deref(),
                    signal_json,
                    signal,
                )
                .map_err(Into::into),
                CreateSignalRecovery::InputSetUnknown => write_resource_mutation_unknown(
                    "Run Input Set creation",
                    signal_deployment.fingerprint().api_url(),
                    &signal_organization,
                    "input set",
                    None,
                    signal_json,
                    signal,
                )
                .map_err(Into::into),
                CreateSignalRecovery::InputSet(input_set_id) => write_input_set_recovery(
                    signal_deployment.fingerprint().api_url(),
                    &signal_organization,
                    &input_set_id,
                    signal_json,
                    signal,
                )
                .map_err(Into::into),
                CreateSignalRecovery::None => Ok(signal),
            },
        )
    }

    fn execute_blocking(
        self,
        deployment: &Deployment,
        control: &super::OperationControl<CreateRecoveryState>,
    ) -> super::CommandResult {
        if let Err(error) = acquisition::validate_standard_input_claims(
            &self.inputs,
            self.integration_context_file.as_deref(),
            self.options.authentication.uses_stdin(),
        ) {
            return finish_operation(control, || {
                write_input_acquisition_failure(
                    deployment.fingerprint().api_url(),
                    &self.organization,
                    &error,
                    self.options.json,
                )
            });
        }
        let integration_context = match acquisition::acquire_integration_context(
            self.integration_context_file.as_deref(),
        ) {
            Ok(context) => context,
            Err(error) => {
                return finish_operation(control, || {
                    write_input_acquisition_failure(
                        deployment.fingerprint().api_url(),
                        &self.organization,
                        &error,
                        self.options.json,
                    )
                });
            }
        };
        let acquired = if self.inputs.is_empty() {
            None
        } else {
            match acquisition::acquire(&self.inputs) {
                Ok(acquired) => Some(acquired),
                Err(error) => {
                    return finish_operation(control, || {
                        write_input_acquisition_failure(
                            deployment.fingerprint().api_url(),
                            &self.organization,
                            &error,
                            self.options.json,
                        )
                    });
                }
            }
        };
        let run_idempotency_key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate Cloud run request identity")?;
        if control.is_cancelled() {
            return Ok(ExitCode::GeneralFailure);
        }

        let input_set_id = if let Some(acquired) = acquired.as_ref() {
            let staged = input_set::stage_and_seal(
                deployment,
                self.options.http.transport_policy(),
                &self.options.authentication,
                &self.organization,
                &self.project_id,
                acquired,
                control,
            )?;
            match staged {
                Ok(sealed) => sealed.id,
                Err(failure) => {
                    let recovery = control.recovery();
                    return finish_create(
                        deployment,
                        &self.organization,
                        recovery.input_set_id(),
                        Err(failure),
                        self.options.authentication.kind(),
                        self.options.json,
                        control,
                    );
                }
            }
        } else {
            self.input_set_id.clone().unwrap_or_default()
        };
        let input_set_id = (!input_set_id.is_empty()).then_some(input_set_id);
        if control.is_cancelled() {
            return Ok(ExitCode::GeneralFailure);
        }

        let dispatch_recovery = control.recovery().run_dispatched();
        // Run dispatch owns cancellation recovery and a run-specific request envelope; it stays
        // explicit rather than sharing project creation's superficially similar API call.
        // jscpd:ignore-start
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                api.create(
                    &self.organization,
                    &run_idempotency_key,
                    CreateRunInput {
                        project_id: &self.project_id,
                        workflow_path: &self.workflow_path,
                        source_branch: self.source_branch.as_deref(),
                        display_name: self.display_name.as_deref(),
                        input_set_id: input_set_id.as_deref(),
                        integration_context: integration_context.as_ref(),
                    },
                    || control.begin_dispatch_with_recovery(dispatch_recovery.clone()),
                )
            },
        )?;
        // jscpd:ignore-end
        finish_create(
            deployment,
            &self.organization,
            input_set_id.as_deref(),
            result,
            self.options.authentication.kind(),
            self.options.json,
            control,
        )
    }
}

impl ShowCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        super::execute_read_only_with_signals("Cloud run show", move |control| {
            self.execute_blocking(&deployment, control)
        })
    }

    fn execute_blocking(
        self,
        deployment: &Deployment,
        control: &super::OperationControl<()>,
    ) -> super::CommandResult {
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.get(&self.run.organization, &self.run.run_id),
        )?;
        super::complete_read_only_output(control, || {
            write_show(
                deployment.fingerprint().api_url(),
                &self.run.organization,
                &self.run.run_id,
                result,
                self.options.authentication.kind(),
                self.options.json,
            )
            .map_err(Into::into)
        })
    }
}

impl WaitCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        let timeout = self.wait.timeout;
        let timeout_deployment = deployment.fingerprint().api_url().to_owned();
        let timeout_organization = self.run.organization.clone();
        let timeout_run_id = self.run.run_id.clone();
        let timeout_json = self.options.json;

        super::execute_observation_with_signals_and_timeout(
            "Cloud run wait",
            timeout,
            move |control| self.execute_blocking(&deployment, control),
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
        let clock = super::SystemObservationClock;
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                wait_for_terminal_run(
                    api,
                    &self.run.organization,
                    &self.run.run_id,
                    self.wait.timeout,
                    control,
                    &clock,
                )
            },
        )?;
        if !control.begin_completion() {
            return Ok(ExitCode::GeneralFailure);
        }
        match result {
            Ok(WaitObservation::Terminal { resource, state }) => write_wait_terminal(
                deployment.fingerprint().api_url(),
                &resource,
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
                self.options.authentication.kind(),
                self.options.json,
            ),
        }
        .map_err(Into::into)
    }
}

trait RunObservationApi {
    fn get_run(&self, organization: &str, run_id: &str) -> Result<RunRead, RunFailure>;
}

impl<'a> RunObservationApi for RunApi<'a> {
    fn get_run(&self, organization: &str, run_id: &str) -> Result<RunRead, RunFailure> {
        self.get(organization, run_id)
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

type WaitObservation = super::TerminalObservation<Run, TerminalRunState>;

fn wait_for_terminal_run(
    api: &impl RunObservationApi,
    organization: &str,
    run_id: &str,
    timeout: Option<Duration>,
    control: &super::BlockingObservationControl,
    clock: &impl super::ObservationClock,
) -> Result<WaitObservation, RunFailure> {
    match super::wait_for_terminal_observation(
        || api.get_run(organization, run_id),
        |read| match read {
            RunRead::Materialized(run) => terminal_run_state(run.state),
            RunRead::Pending(_) => None,
        },
        RunFailure::retryable_observation,
        timeout,
        control,
        clock,
    )? {
        super::TerminalObservation::Terminal { resource, state } => match *resource {
            RunRead::Materialized(run) => Ok(super::TerminalObservation::Terminal {
                resource: run,
                state,
            }),
            RunRead::Pending(_) => Err(RunFailure::Protocol {
                credential_rejected: false,
            }),
        },
        super::TerminalObservation::TimedOut => Ok(super::TerminalObservation::TimedOut),
        super::TerminalObservation::Stopped => Ok(super::TerminalObservation::Stopped),
    }
}

const fn terminal_run_state(state: RunState) -> Option<TerminalRunState> {
    match state {
        RunState::Queued
        | RunState::Assigning
        | RunState::Preparing
        | RunState::Assigned
        | RunState::Running
        | RunState::Cancelling => None,
        RunState::Succeeded => Some(TerminalRunState::Succeeded),
        RunState::Failed => Some(TerminalRunState::Failed),
        RunState::Cancelled => Some(TerminalRunState::Cancelled),
        RunState::Interrupted => Some(TerminalRunState::Interrupted),
        RunState::Rejected => Some(TerminalRunState::Rejected),
    }
}

fn parse_input_set_id(value: &str) -> Result<String, String> {
    if scherzo_cloud_support::valid_typed_id(value, "ris_") {
        Ok(value.to_owned())
    } else {
        Err(
            "must be an exact Run Input Set ID (ris_ followed by 26 lowercase ULID characters)"
                .to_owned(),
        )
    }
}

fn with_api<T>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    authentication: &super::PrincipalAuthenticationArgs,
    mut operation: impl FnMut(&RunApi<'_>) -> Result<T, RunFailure>,
) -> anyhow::Result<Result<T, RunFailure>> {
    let client = super::human_session_client(transport_policy)?;
    super::execute_selected_api_operation(
        super::principal_api_context(
            &client,
            deployment,
            authentication,
            "acquire human session for Cloud run operation",
        ),
        |access_token| {
            let api = RunApi::new(
                deployment.fingerprint().api_url(),
                access_token,
                transport_policy,
                &client,
            )
            .map_err(|error| anyhow!(error))
            .context("prepare Cloud run networking")?;
            Ok(operation(&api))
        },
        RunFailure::credential_rejected,
        || RunFailure::Unauthenticated,
        RunFailure::Unreachable,
    )
}

fn write_input_acquisition_failure(
    deployment: &str,
    organization: &str,
    failure: &acquisition::InputAcquisitionFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&FailureResult {
            schema_version: 1,
            deployment,
            outcome: "invalid_input",
            organization_ref: organization,
            run_id: None,
            input_set_id: None,
            category: None,
        })?;
    } else {
        writeln!(
            io::stderr().lock(),
            "error: acquire Cloud run request data: {}\n\nCorrect the named input sources, integration context, and limits, then try again.",
            visible_text(&failure.to_string())
        )?;
    }
    Ok(ExitCode::GeneralFailure)
}

fn write_create(
    deployment: &str,
    organization: &str,
    input_set_id: Option<&str>,
    result: Result<scherzo_cloud_api::RunCreationAcceptance, RunFailure>,
    authentication: super::PrincipalAuthenticationKind,
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
                    input_set_id,
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
                if let Some(input_set_id) = input_set_id {
                    writeln!(stdout, "input set: {input_set_id}")?;
                }
                writeln!(stdout, "organization: {organization}")?;
                writeln!(stdout, "deployment: {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure_with_input_set(
            deployment,
            organization,
            None,
            input_set_id,
            &failure,
            authentication,
            json,
        ),
    }
}

fn write_show(
    deployment: &str,
    organization: &str,
    requested_run_id: &str,
    result: Result<RunRead, RunFailure>,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(RunRead::Materialized(run)) => {
            if json {
                write_json(&ShowResult {
                    schema_version: 1,
                    deployment,
                    outcome: "found",
                    run: run.as_ref(),
                })?;
            } else {
                write_run_human(deployment, "✓ Run found.", run.as_ref())?;
            }
            Ok(ExitCode::Success)
        }
        Ok(RunRead::Pending(pending)) => {
            if json {
                write_json(&PendingShowResult {
                    schema_version: 1,
                    deployment,
                    outcome: "pending",
                    organization_ref: organization,
                    run_id: &pending.run_id,
                })?;
            } else {
                let stdout = io::stdout();
                let mut stdout = stdout.lock();
                writeln!(stdout, "✓ Run creation pending.\n")?;
                writeln!(stdout, "run: {}", pending.run_id)?;
                writeln!(stdout, "organization: {organization}")?;
                writeln!(stdout, "deployment: {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(
            deployment,
            organization,
            Some(requested_run_id),
            &failure,
            authentication,
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
        write_json(&super::ObservationResult {
            schema_version: 1,
            deployment,
            outcome: "timed_out",
            organization_ref: organization,
            run_id,
            publication_id: None,
            idempotency_key: None,
            category: None,
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
    writeln!(stdout, "  named values: {}", run.inputs.input_count)?;
    writeln!(
        stdout,
        "  attachment members: {}",
        run.inputs.attachment_count
    )?;
    writeln!(stdout, "  bytes: {}", run.inputs.aggregate_bytes)?;
    writeln!(
        stdout,
        "  availability: {}",
        enum_text(&run.inputs.availability)?
    )?;
    writeln!(stdout, "\nintegration context:")?;
    if run.integration_context.is_empty() {
        writeln!(stdout, "  none")?;
    } else {
        let mut entries = run.integration_context.iter().collect::<Vec<_>>();
        entries.sort_by(|(first, _), (second, _)| first.as_bytes().cmp(second.as_bytes()));
        for (key, value) in entries {
            writeln!(stdout, "  {}: {}", visible_text(key), visible_text(value))?;
        }
    }
    writeln!(stdout, "\ncancellation:")?;
    if let Some(cancellation) = run.cancellation.as_deref() {
        writeln!(stdout, "  mode: {}", enum_text(&cancellation.mode)?)?;
        writeln!(
            stdout,
            "  graceful request: {}",
            cancellation
                .graceful_request_id
                .as_deref()
                .unwrap_or("none")
        )?;
        writeln!(
            stdout,
            "  force request: {}",
            cancellation.force_request_id.as_deref().unwrap_or("none")
        )?;
    } else {
        writeln!(stdout, "  none")?;
    }
    writeln!(stdout, "\ninterruption:")?;
    if let Some(interruption) = run.interruption.as_deref() {
        writeln!(stdout, "  phase: {}", enum_text(&interruption.phase)?)?;
        writeln!(stdout, "  cause: {}", enum_text(&interruption.cause)?)?;
        writeln!(
            stdout,
            "  executor fault: {}",
            interruption
                .executor_fault
                .as_ref()
                .map(enum_text)
                .transpose()?
                .as_deref()
                .unwrap_or("none")
        )?;
        writeln!(
            stdout,
            "  stop confirmed: {}",
            if interruption.stop_confirmed {
                "yes"
            } else {
                "no"
            }
        )?;
    } else {
        writeln!(stdout, "  none")?;
    }
    writeln!(stdout, "\nartifact delivery:")?;
    match run.artifact_delivery.as_deref() {
        None => writeln!(stdout, "  none")?,
        Some(RunArtifactDelivery::RunArtifactDeliverySucceeded(delivery)) => {
            writeln!(stdout, "  state: succeeded")?;
            writeln!(stdout, "  artifact set: {}", delivery.artifact_set_id)?;
        }
        Some(RunArtifactDelivery::RunArtifactDeliveryRegistrationFailed(delivery)) => {
            writeln!(stdout, "  state: failed")?;
            writeln!(stdout, "  phase: {}", enum_text(&delivery.phase)?)?;
            writeln!(stdout, "  code: {}", enum_text(&delivery.code)?)?;
        }
        Some(RunArtifactDelivery::RunArtifactDeliveryUploadFailed(delivery)) => {
            writeln!(stdout, "  state: failed")?;
            writeln!(stdout, "  phase: {}", enum_text(&delivery.phase)?)?;
            writeln!(stdout, "  code: {}", enum_text(&delivery.code)?)?;
        }
        Some(RunArtifactDelivery::RunArtifactDeliveryPreparationFailed(delivery)) => {
            writeln!(stdout, "  state: failed")?;
            writeln!(stdout, "  phase: {}", enum_text(&delivery.phase)?)?;
            writeln!(stdout, "  code: {}", enum_text(&delivery.code)?)?;
        }
    }
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
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_failure_with_input_set(
        deployment,
        organization,
        run_id,
        None,
        failure,
        authentication,
        json,
    )
}

fn write_failure_with_input_set(
    deployment: &str,
    organization: &str,
    run_id: Option<&str>,
    input_set_id: Option<&str>,
    failure: &RunFailure,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (outcome, category, human, class) = match failure {
        RunFailure::Unauthenticated => (
            "unauthenticated",
            None,
            authentication
                .rejected_error(
                    "error: Cloud run access requires sign-in\n\nSign in first:\n  scherzo-cloud auth login",
                )
                .to_owned(),
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
            "error: Cloud run request conflicts with current state\n\nCheck the resource state and try again.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunFailure::CreationRejected => (
            "creation_rejected",
            None,
            "error: Cloud run creation was rejected before the run materialized\n\nInspect the run request and create a new run after correcting the rejection cause.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunFailure::Gone => (
            "gone",
            None,
            "error: Cloud run input content is no longer available\n\nStart a new input set or run instead.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunFailure::Unreachable(category) => (
            "unreachable",
            Some(category.as_str()),
            format!("error: contact Cloud run API at {deployment}: {}\n\nCheck network access to the deployment and try again.", category.as_str()),
            super::unreachable_outcome_class(*category),
        ),
        RunFailure::InputUploadRejected => (
            "conflict",
            None,
            "error: Cloud run input upload was not accepted\n\nInspect the input set and upload the selected member again.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunFailure::InputDownloadRejected => (
            "integrity_mismatch",
            None,
            "error: retained input download did not match its manifest\n\nNo downloaded result was committed. Try again later.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunFailure::Interrupted => (
            "interrupted",
            None,
            "error: retained input operation was interrupted\n\nRun the command again to start with fresh capabilities.".to_owned(),
            OutcomeClass::Interrupted,
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
            input_set_id,
            category,
        })?;
    } else if let Some(input_set_id) = input_set_id {
        writeln!(
            io::stderr().lock(),
            "{human}\n\ninput set: {input_set_id}\n\nContinue explicitly with `scherzo-cloud run input-set show` before creating another set."
        )?;
    } else {
        writeln!(io::stderr().lock(), "{human}")?;
    }
    Ok(class.exit_code())
}

fn write_create_unknown(
    deployment: &str,
    organization: &str,
    input_set_id: Option<&str>,
    json: bool,
    exit_code: ExitCode,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&UnknownCreateResult {
            schema_version: 1,
            deployment,
            outcome: "unknown",
            organization_ref: organization,
            input_set_id,
            commitment: "unknown",
        })?;
    } else {
        writeln!(
            io::stderr().lock(),
            "error: run acceptance is unknown after interruption\n\norganization: {organization}\ninput set: {}\ncommitment: unknown\n\nThe CLI cannot safely determine whether the run was accepted. Inspect the deployment before creating another run.",
            input_set_id.unwrap_or("none")
        )?;
    }
    Ok(exit_code)
}

fn write_input_set_recovery(
    deployment: &str,
    organization: &str,
    input_set_id: &str,
    json: bool,
    exit_code: ExitCode,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&InputSetRecoveryResult {
            schema_version: 1,
            deployment,
            outcome: "input_set_incomplete",
            organization_ref: organization,
            input_set_id,
        })?;
    } else {
        writeln!(
            io::stderr().lock(),
            "error: Run Input Set preparation was interrupted\n\ninput set: {input_set_id}\norganization: {organization}\n\nInspect and resume this input set explicitly before creating another set."
        )?;
    }
    Ok(exit_code)
}

fn write_resource_mutation_unknown(
    operation: &str,
    deployment: &str,
    organization: &str,
    resource_kind: &str,
    resource_id: Option<&str>,
    json: bool,
    exit_code: ExitCode,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&UnknownResourceMutationResult {
            schema_version: 1,
            deployment,
            outcome: "unknown",
            organization_ref: organization,
            operation,
            resource_kind,
            resource_id,
            commitment: "unknown",
        })?;
    } else {
        let resource = resource_id
            .map(|resource_id| format!("\n{resource_kind}: {resource_id}"))
            .unwrap_or_default();
        writeln!(
            io::stderr().lock(),
            "error: {operation} is unconfirmed after interruption\n{resource}\norganization: {organization}\ncommitment: unknown\n\nInspect the resource before repeating this operation."
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
    #[serde(skip_serializing_if = "Option::is_none")]
    input_set_id: Option<&'a str>,
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
struct PendingShowResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    run_id: &'a str,
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
struct FailureResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_set_id: Option<&'a str>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    input_set_id: Option<&'a str>,
    commitment: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InputSetRecoveryResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    input_set_id: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UnknownResourceMutationResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    operation: &'a str,
    resource_kind: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    resource_id: Option<&'a str>,
    commitment: &'static str,
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{BTreeMap, VecDeque};
    use std::io::{BufRead as _, BufReader, Write as _};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, mpsc};

    use super::super::observation_test_support::ControlledObservationClock as ControlledWaitClock;
    use super::*;
    use scherzo_cloud_api::{
        HttpTransportPolicy, InputScalarMetadata, NamedInputMetadata, RunCreationAcceptance,
        RunInputManifest, UnreachableCategory,
    };

    struct ScriptedObservationApi {
        responses: RefCell<VecDeque<Result<RunRead, RunFailure>>>,
    }

    impl ScriptedObservationApi {
        fn new(responses: impl IntoIterator<Item = Result<Run, RunFailure>>) -> Self {
            Self {
                responses: RefCell::new(
                    responses
                        .into_iter()
                        .map(|response| response.map(Box::new).map(RunRead::Materialized))
                        .collect(),
                ),
            }
        }
    }

    impl RunObservationApi for ScriptedObservationApi {
        fn get_run(&self, _organization: &str, _run_id: &str) -> Result<RunRead, RunFailure> {
            self.responses
                .borrow_mut()
                .pop_front()
                .expect("the polling scenario should provide another response")
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
            timeout,
            &super::super::BlockingObservationControl::new(),
            clock,
        )
    }

    fn assert_succeeded_after_single_poll(
        result: Result<WaitObservation, RunFailure>,
        clock: ControlledWaitClock,
        context: &str,
    ) {
        assert!(matches!(
            result.unwrap_or_else(|failure| panic!("{context}: {failure:?}")),
            WaitObservation::Terminal {
                state: TerminalRunState::Succeeded,
                ..
            }
        ));
        clock.assert_single_poll();
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
            "sourceDisplaySnapshot": null,
            "inputs": {
                "inputSetId": null,
                "inputCount": 0,
                "attachmentCount": 0,
                "aggregateBytes": 0,
                "availability": "available"
            },
            "integrationContext": {},
            "publication": null,
            "cancellation": null,
            "interruption": null,
            "artifactDelivery": null,
            "createdAt": "2026-08-10T12:00:00Z",
            "updatedAt": "2026-08-10T12:00:00Z"
        }))
        .expect("run fixture should match the generated model")
    }

    fn dispatch_test_api<'a>(api_url: &str, client: &'a HttpClient) -> RunApi<'a> {
        RunApi::new(
            api_url,
            "test-token",
            HttpTransportPolicy::AllowInsecureHttp,
            client,
        )
        .unwrap()
    }

    fn create_dispatch_test_run(
        api: &RunApi<'_>,
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<RunCreationAcceptance, RunFailure> {
        api.create(
            "acme-research",
            "create-run-key",
            CreateRunInput {
                project_id: "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
                workflow_path: "workflows/build.yaml",
                source_branch: None,
                display_name: None,
                input_set_id: Some("ris_explicit"),
                integration_context: None,
            },
            begin_dispatch,
        )
    }

    fn assert_interrupted<T>(result: Result<T, RunFailure>) {
        assert!(matches!(result, Err(RunFailure::Interrupted)));
    }

    fn assert_no_pending_request(listener: &TcpListener) {
        assert!(matches!(
            listener.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn create_signal_snapshot_preserves_dispatch_and_input_set_provenance() {
        let allocated = super::super::OperationControl::new(CreateRecoveryState::new(None));
        assert!(
            allocated.begin_dispatch_with_recovery(
                CreateRecoveryState::input_set_allocation_dispatched()
            )
        );
        assert!(
            allocated.update_recovery(CreateRecoveryState::allocated_input_set("ris_allocated"))
        );
        let snapshot = allocated.claim_signal().unwrap();
        assert_eq!(
            create_signal_recovery(snapshot),
            CreateSignalRecovery::InputSet("ris_allocated".to_owned())
        );
        assert!(!allocated.begin_dispatch());

        let explicit =
            super::super::OperationControl::new(CreateRecoveryState::new(Some("ris_explicit")));
        assert!(explicit.begin_dispatch_with_recovery(explicit.recovery().run_dispatched()));
        let snapshot = explicit.claim_signal().unwrap();
        assert_eq!(
            snapshot.recovery,
            CreateRecoveryState::RunDispatched(Some(CreateInputSetOwnership::Explicit(
                "ris_explicit".to_owned()
            )))
        );
        assert_eq!(
            create_signal_recovery(snapshot),
            CreateSignalRecovery::Run(Some("ris_explicit".to_owned()))
        );

        let inputless = super::super::OperationControl::new(CreateRecoveryState::new(None));
        assert!(inputless.begin_dispatch_with_recovery(inputless.recovery().run_dispatched()));
        assert_eq!(
            create_signal_recovery(inputless.claim_signal().unwrap()),
            CreateSignalRecovery::Run(None)
        );
    }

    fn assert_signal_before_dispatch_sends_no_request<T: Send>(
        recovery: CreateRecoveryState,
        dispatch_recovery: CreateRecoveryState,
        operation: impl FnOnce(&RunApi<'_>, &dyn Fn() -> bool) -> Result<T, RunFailure> + Send,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let api_url = format!("http://{}", listener.local_addr().unwrap());
        let control = Arc::new(super::super::OperationControl::new(recovery));
        let before_claim = Arc::new(Barrier::new(2));
        let release_claim = Arc::new(Barrier::new(2));

        std::thread::scope(|scope| {
            let worker_control = Arc::clone(&control);
            let worker_before_claim = Arc::clone(&before_claim);
            let worker_release_claim = Arc::clone(&release_claim);
            let worker = scope.spawn(move || {
                let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).unwrap();
                let api = dispatch_test_api(&api_url, &client);
                let begin_dispatch = || {
                    worker_before_claim.wait();
                    worker_release_claim.wait();
                    worker_control.begin_dispatch_with_recovery(dispatch_recovery.clone())
                };
                operation(&api, &begin_dispatch)
            });

            before_claim.wait();
            let snapshot = control.claim_signal().unwrap();
            assert!(!snapshot.dispatched);
            release_claim.wait();
            assert_interrupted(worker.join().unwrap());
        });

        assert_no_pending_request(&listener);
    }

    #[test]
    fn cancellation_wins_at_real_run_and_input_set_dispatch_boundaries() {
        assert_signal_before_dispatch_sends_no_request(
            CreateRecoveryState::new(Some("ris_explicit")),
            CreateRecoveryState::new(Some("ris_explicit")).run_dispatched(),
            |api, begin_dispatch| create_dispatch_test_run(api, begin_dispatch),
        );

        let manifest = RunInputManifest {
            inputs: BTreeMap::from([(
                "request".to_owned(),
                NamedInputMetadata::Text(InputScalarMetadata {
                    size_bytes: 0,
                    sha256: ring::digest::digest(&ring::digest::SHA256, b"")
                        .as_ref()
                        .try_into()
                        .unwrap(),
                }),
            )]),
        };
        assert_signal_before_dispatch_sends_no_request(
            CreateRecoveryState::new(None),
            CreateRecoveryState::input_set_allocation_dispatched(),
            |api, begin_dispatch| {
                api.create_input_set(
                    "acme-research",
                    "create-input-set-key",
                    "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
                    &manifest,
                    begin_dispatch,
                )
            },
        );
    }

    #[test]
    fn cancellation_blocks_a_run_create_transport_retry_after_dispatch() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server_listener = listener.try_clone().unwrap();
        let api_url = format!("http://{}", listener.local_addr().unwrap());
        let control = Arc::new(super::super::OperationControl::new(
            CreateRecoveryState::new(Some("ris_explicit")),
        ));
        let (retry_ready, observe_retry) = mpsc::sync_channel(0);
        let (release_retry, retry_released) = mpsc::sync_channel(0);

        std::thread::scope(|scope| {
            let server = scope.spawn(move || {
                let (mut stream, _) = server_listener.accept().unwrap();
                let mut reader = BufReader::new(&mut stream);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert!(line.starts_with("POST "));
                while line != "\r\n" {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                }
                drop(reader);
                stream
                    .write_all(
                        b"HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nIdempotency-Key: create-run-key\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{",
                    )
                    .unwrap();
            });

            let worker_control = Arc::clone(&control);
            let worker = scope.spawn(move || {
                let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).unwrap();
                let api = dispatch_test_api(&api_url, &client);
                let dispatches = AtomicUsize::new(0);
                let dispatch_recovery = worker_control.recovery().run_dispatched();
                create_dispatch_test_run(&api, || {
                    if dispatches.fetch_add(1, Ordering::AcqRel) == 1 {
                        retry_ready.send(()).unwrap();
                        retry_released.recv().unwrap();
                    }
                    worker_control.begin_dispatch_with_recovery(dispatch_recovery.clone())
                })
            });

            observe_retry.recv().unwrap();
            let snapshot = control.claim_signal().unwrap();
            assert_eq!(
                create_signal_recovery(snapshot),
                CreateSignalRecovery::Run(Some("ris_explicit".to_owned()))
            );
            release_retry.send(()).unwrap();
            assert_interrupted(worker.join().unwrap());
            server.join().unwrap();
        });

        listener.set_nonblocking(true).unwrap();
        assert_no_pending_request(&listener);
    }

    #[test]
    fn wait_polls_every_nonterminal_state_until_each_terminal_state() {
        let nonterminal = [
            RunState::Queued,
            RunState::Assigning,
            RunState::Preparing,
            RunState::Assigned,
            RunState::Running,
            RunState::Cancelling,
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
            let started_at = scherzo_cloud_support::monotonic_now();
            let clock = ControlledWaitClock::new(started_at);

            let result = observe(&api, None, &clock).expect("the polling scenario should complete");

            match result {
                WaitObservation::Terminal { resource, state } => {
                    assert_eq!(resource.state, terminal_state);
                    assert_eq!(state, expected);
                }
                WaitObservation::TimedOut | WaitObservation::Stopped => {
                    panic!("the polling scenario did not observe its terminal run")
                }
            }
            assert_eq!(
                clock.into_sleeps(),
                vec![super::super::OBSERVATION_POLL_INTERVAL; nonterminal.len()]
            );
        }
    }

    #[test]
    fn wait_treats_admitted_creation_as_nonterminal() {
        let api = ScriptedObservationApi {
            responses: RefCell::new(VecDeque::from([
                Ok(RunRead::Pending(
                    scherzo_cloud_api::RunCreationPending::new(
                        "run_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    ),
                )),
                Ok(RunRead::Materialized(Box::new(run(RunState::Succeeded)))),
            ])),
        };
        let clock = ControlledWaitClock::new(scherzo_cloud_support::monotonic_now());

        assert_succeeded_after_single_poll(
            observe(&api, None, &clock),
            clock,
            "pending creation should remain observable",
        );
    }

    #[test]
    fn wait_recovers_from_one_retryable_observation_failure() {
        let api = ScriptedObservationApi::new([
            Err(RunFailure::Unreachable(UnreachableCategory::Server)),
            Ok(run(RunState::Succeeded)),
        ]);
        let started_at = scherzo_cloud_support::monotonic_now();
        let clock = ControlledWaitClock::new(started_at);

        assert_succeeded_after_single_poll(
            observe(&api, None, &clock),
            clock,
            "one recoverable failure should not end observation",
        );
    }

    #[test]
    fn wait_timeout_uses_the_remaining_duration_without_an_extra_request() {
        let api = ScriptedObservationApi::new([
            Ok(run(RunState::Queued)),
            Ok(run(RunState::Assigned)),
            Ok(run(RunState::Running)),
        ]);
        let started_at = scherzo_cloud_support::monotonic_now();
        let clock = ControlledWaitClock::new(started_at);

        let result = observe(&api, Some(Duration::from_secs(5)), &clock)
            .expect("timeout is a local wait outcome");

        assert!(matches!(result, WaitObservation::TimedOut));
        clock.assert_timeout_schedule();
        assert!(api.responses.into_inner().is_empty());
    }

    #[test]
    fn wait_bounds_retries_and_preserves_the_transport_failure() {
        let failure = RunFailure::Unreachable(UnreachableCategory::Connection);
        let api = ScriptedObservationApi::new([Err(failure), Err(failure)]);
        let started_at = scherzo_cloud_support::monotonic_now();
        let clock = ControlledWaitClock::new(started_at);

        let result = observe(&api, None, &clock);

        assert_eq!(result.err(), Some(failure));
        clock.assert_single_poll();
    }

    #[test]
    fn wait_timeout_parser_accepts_documented_units_and_rejects_zero() {
        assert_eq!(
            super::super::parse_wait_timeout("250ms"),
            Ok(Duration::from_millis(250))
        );
        assert_eq!(
            super::super::parse_wait_timeout("30"),
            Ok(Duration::from_secs(30))
        );
        assert_eq!(
            super::super::parse_wait_timeout("10m"),
            Ok(Duration::from_secs(600))
        );
        assert_eq!(
            super::super::parse_wait_timeout("2h"),
            Ok(Duration::from_secs(7_200))
        );
        assert!(super::super::parse_wait_timeout("0s").is_err());
        assert!(super::super::parse_wait_timeout("1.5s").is_err());
    }
}
