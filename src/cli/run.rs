use std::io::{self, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand, builder::NonEmptyStringValueParser};
use serde::Serialize;

use crate::exit_code::{ExitCode, OutcomeClass};
use scherzo_cloud_api::{
    CreateRunInput, HttpTransportPolicy, Run, RunApi, RunArtifactDelivery, RunCancellationMode,
    RunCancellationResolutionKind, RunFailure, RunList, RunListFilter, RunObservation, RunState,
};
#[cfg(test)]
use scherzo_cloud_api::{HttpClient, RunRead};
use scherzo_cloud_execution::visible_text;
use scherzo_cloud_human_auth::Deployment;

use super::{OrganizationArg, ProjectArg};

mod acquisition;
mod input_set;
mod inputs;
mod list;
mod observation;
mod output;
use output::{CloudOutput, CloudSnapshot};

// Keep the closed envelope fields grouped at the rendering boundary, without
// repeating its context at every signal, timeout and completion call site.
macro_rules! write_cloud {
    ($operation:expr, $deployment:expr, $organization:expr, $snapshot:expr,
     $outcome:expr, $error:expr, $key:expr, $mode:expr, $json:expr, $exit:expr $(,)?) => {
        output::write_cloud(
            CloudOutput {
                operation: $operation,
                deployment: $deployment,
                organization: $organization,
                snapshot: $snapshot,
                json: $json,
            },
            $outcome,
            $error,
            $key,
            $mode,
            $exit,
        )
    };
}

pub(super) const ABOUT: &str = "Manage Scherzo Cloud runs";
const NAME: &str = "run";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<RunCommand>,
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    #[command(about = "Request cancellation of a Scherzo Cloud run")]
    Cancel(CancelCommand),
    #[command(about = "Create a Scherzo Cloud run")]
    Create(CreateCommand),
    #[command(about = inputs::ABOUT)]
    Input(inputs::Command),
    #[command(about = input_set::ABOUT)]
    InputSet(input_set::Command),
    #[command(about = "List Scherzo Cloud runs")]
    List(ListCommand),
    #[command(about = "Show a Scherzo Cloud run")]
    Show(ShowCommand),
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
    wait: RunWaitArgs,

    #[command(flatten)]
    options: RunOptions,
}

#[derive(Debug, Args)]
struct RunWaitArgs {
    #[arg(long, help = "Observe the run until it settles")]
    wait: bool,
    #[arg(long, requires = "wait", value_name = "DURATION", value_parser = super::parse_wait_timeout,
        help = "Stop waiting after a positive duration (units: ms, s, m, or h)")]
    timeout: Option<Duration>,
}

#[derive(Debug, Args)]
struct RunReference {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(value_name = "RUN", help = "Exact Run ID")]
    run_id: String,
}

#[derive(Debug, Args)]
struct ListCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,
    #[arg(long, value_name = ProjectArg::VALUE_NAME, help = ProjectArg::HELP)]
    project_id: Option<ProjectArg>,
    #[arg(long, value_parser = ["active", "terminal"], help = "Filter by run state group")]
    state_group: Option<String>,
    #[arg(
        long,
        value_name = "RFC3339",
        help = "List runs created strictly after this time"
    )]
    created_after: Option<String>,
    #[arg(
        long = "integration-context",
        value_name = "KEY=VALUE",
        help = "Match an exact integration context entry (repeatable)"
    )]
    integration_context: Vec<String>,
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..=100), help = "Maximum runs to return (1-100)")]
    limit: Option<u16>,
    #[arg(long, help = "Opaque continuation cursor")]
    cursor: Option<String>,
    #[command(flatten)]
    options: RunOptions,
}

#[derive(Debug, Args)]
struct ShowCommand {
    #[command(flatten)]
    run: RunReference,
    #[command(flatten)]
    wait: RunWaitArgs,
    #[command(flatten)]
    options: RunOptions,
}

#[derive(Debug, Args)]
struct CancelCommand {
    #[command(flatten)]
    run: RunReference,
    #[arg(
        long,
        help = "Request immediate containment instead of graceful stopping"
    )]
    force: bool,
    #[arg(long, value_name = "KEY", value_parser = super::publication::parse_idempotency_key,
        help = "Reuse this key to reconcile an uncertain request")]
    idempotency_key: Option<String>,
    #[command(flatten)]
    wait: RunWaitArgs,
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
            Some(RunCommand::List(command)) => super::execute_deployment_command(
                Some(command),
                &[NAME],
                "configure Scherzo Cloud run access",
                |command, deployment| command.execute(deployment.clone()),
            ),
            Some(RunCommand::Cancel(command)) => super::execute_deployment_command(
                Some(command),
                &[NAME],
                "configure Scherzo Cloud run cancellation",
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

#[derive(Clone, Debug, PartialEq)]
enum CreateRecoveryState {
    BeforeRunDispatch(Option<CreateInputSetOwnership>),
    InputSetAllocationDispatched,
    RunDispatched(Option<CreateInputSetOwnership>, Option<String>),
    Accepted(CloudSnapshot),
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
            Self::BeforeRunDispatch(input_set) | Self::RunDispatched(input_set, _) => {
                Self::RunDispatched(input_set.clone(), None)
            }
            Self::InputSetAllocationDispatched => Self::RunDispatched(None, None),
            Self::Accepted(snapshot) => Self::Accepted(snapshot.clone()),
        }
    }

    fn with_run_key(self, key: &str) -> Self {
        match self {
            Self::RunDispatched(input_set, _) => {
                Self::RunDispatched(input_set, Some(key.to_owned()))
            }
            other => other,
        }
    }

    fn input_set_id(&self) -> Option<&str> {
        match self {
            Self::BeforeRunDispatch(Some(input_set)) | Self::RunDispatched(Some(input_set), _) => {
                Some(input_set.id())
            }
            Self::BeforeRunDispatch(None)
            | Self::InputSetAllocationDispatched
            | Self::RunDispatched(None, _)
            | Self::Accepted(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum CreateSignalRecovery {
    None,
    InputSetUnknown,
    InputSet(String),
    Run(Option<String>, Option<String>),
    Accepted(CloudSnapshot),
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
        CreateRecoveryState::RunDispatched(input_set, key) => {
            CreateSignalRecovery::Run(input_set.map(|input_set| input_set.id().to_owned()), key)
        }
        CreateRecoveryState::Accepted(snapshot) => CreateSignalRecovery::Accepted(snapshot),
    }
}

fn finish_operation<R>(
    control: &super::OperationControl<R>,
    write_result: impl FnOnce() -> anyhow::Result<ExitCode>,
) -> super::CommandResult {
    super::complete_operation(control, || write_result().map_err(Into::into))
}

fn start_cloud_observation(
    timeout_start: &super::DeferredObservationTimeoutStart,
) -> (super::SystemObservationClock, std::time::Instant) {
    timeout_start.start();
    (
        super::SystemObservationClock,
        scherzo_cloud_support::monotonic_now(),
    )
}

fn finish_accepted<R>(
    control: &super::OperationControl<R>,
    operation: &'static str,
    deployment: &Deployment,
    organization: &str,
    snapshot: &CloudSnapshot,
    json: bool,
) -> super::CommandResult {
    finish_operation(control, || {
        write_cloud!(
            operation,
            deployment.fingerprint().api_url(),
            organization,
            snapshot,
            "accepted",
            None,
            None,
            None,
            json,
            ExitCode::Success,
        )
    })
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

impl CreateCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        let recovery = CreateRecoveryState::new(self.input_set_id.as_deref());
        let signal_deployment = deployment.clone();
        let signal_organization = self.organization.clone();
        let signal_json = self.options.json;
        let timeout = self.wait.timeout.filter(|_| self.wait.wait);
        let timeout_deployment = signal_deployment.clone();
        let timeout_organization = signal_organization.clone();
        super::execute_mutation_with_signals_and_deferred_timeout(
            "Cloud run creation",
            recovery,
            timeout,
            move |control, timeout_start| {
                self.execute_blocking(&deployment, control, timeout_start)
            },
            move |signal, snapshot| match create_signal_recovery(snapshot) {
                CreateSignalRecovery::Run(input_set_id, key) => write_create_unknown(
                    signal_deployment.fingerprint().api_url(),
                    &signal_organization,
                    input_set_id.as_deref(),
                    key.as_deref(),
                    signal_json,
                    signal,
                )
                .map_err(Into::into),
                recovery @ (CreateSignalRecovery::InputSet(_)
                | CreateSignalRecovery::InputSetUnknown
                | CreateSignalRecovery::None) => {
                    if let CreateSignalRecovery::InputSet(input_set_id) = recovery {
                        write_staging_recovery(&signal_organization, &input_set_id)?;
                        if !signal_json {
                            return Ok(signal);
                        }
                    }
                    write_cloud!(
                        "create",
                        signal_deployment.fingerprint().api_url(),
                        &signal_organization,
                        &CloudSnapshot::default(),
                        "acceptance_unknown",
                        Some("acceptance_unknown"),
                        None,
                        None,
                        signal_json,
                        signal,
                    )
                    .map_err(Into::into)
                }
                CreateSignalRecovery::Accepted(snapshot) => write_cloud!(
                    "create",
                    signal_deployment.fingerprint().api_url(),
                    &signal_organization,
                    &snapshot,
                    "observation_stopped",
                    Some("observation_stopped"),
                    None,
                    None,
                    signal_json,
                    signal,
                )
                .map_err(Into::into),
            },
            move |snapshot| match snapshot.recovery {
                CreateRecoveryState::Accepted(snapshot) => write_cloud!(
                    "create",
                    timeout_deployment.fingerprint().api_url(),
                    &timeout_organization,
                    &snapshot,
                    "timed_out",
                    Some("wait_timed_out"),
                    None,
                    None,
                    signal_json,
                    ExitCode::GeneralFailure,
                )
                .map_err(Into::into),
                _ => Ok(ExitCode::GeneralFailure),
            },
        )
    }

    fn execute_blocking(
        self,
        deployment: &Deployment,
        control: &super::OperationControl<CreateRecoveryState>,
        timeout_start: &super::DeferredObservationTimeoutStart,
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
            let staged = match input_set::stage_and_seal(
                deployment,
                self.options.http.transport_policy(),
                &self.options.authentication,
                &self.organization,
                &self.project_id,
                acquired,
                control,
            ) {
                Ok(staged) => staged,
                Err(error) => {
                    let recovery = control.recovery();
                    return finish_operation(control, || {
                        writeln!(
                            io::stderr().lock(),
                            "error: stage Run Input Set: {}\n\nResolve this error before creating another run.",
                            visible_text(&format!("{error:#}"))
                        )?;
                        if let Some(input_set_id) = recovery.input_set_id() {
                            write_staging_guidance(&self.organization, input_set_id)?;
                        }
                        write_cloud!(
                            "create",
                            deployment.fingerprint().api_url(),
                            &self.organization,
                            &CloudSnapshot::default(),
                            "error",
                            Some("submission_failed"),
                            None,
                            None,
                            self.options.json,
                            ExitCode::GeneralFailure,
                        )
                    });
                }
            };
            match staged {
                Ok(sealed) => sealed.id,
                Err(failure) => {
                    let recovery = control.recovery();
                    return finish_operation(control, || {
                        write_create(
                            deployment.fingerprint().api_url(),
                            &self.organization,
                            recovery.input_set_id(),
                            (None, false),
                            Err(failure),
                            self.options.authentication.kind(),
                            self.options.json,
                        )
                    });
                }
            }
        } else {
            self.input_set_id.clone().unwrap_or_default()
        };
        let input_set_id = (!input_set_id.is_empty()).then_some(input_set_id);
        if control.is_cancelled() {
            return Ok(ExitCode::GeneralFailure);
        }

        let dispatch_recovery = control
            .recovery()
            .run_dispatched()
            .with_run_key(&run_idempotency_key);
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
        );
        // jscpd:ignore-end
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                let recovery = control.recovery();
                let dispatched = matches!(recovery, CreateRecoveryState::RunDispatched(_, _));
                return finish_operation(control, || {
                    if let Some(input_set_id) = recovery.input_set_id() {
                        write_staging_guidance(&self.organization, input_set_id)?;
                    }
                    write_cloud!(
                        "create",
                        deployment.fingerprint().api_url(),
                        &self.organization,
                        &CloudSnapshot::default(),
                        if dispatched {
                            "acceptance_unknown"
                        } else {
                            "error"
                        },
                        Some(if dispatched {
                            "acceptance_unknown"
                        } else {
                            "submission_failed"
                        }),
                        dispatched.then_some(run_idempotency_key.as_str()),
                        None,
                        self.options.json,
                        if dispatched {
                            ExitCode::Unavailable
                        } else {
                            ExitCode::GeneralFailure
                        },
                    )
                });
            }
        };
        let acceptance = match result {
            Ok(acceptance) => acceptance,
            Err(failure) => {
                return finish_operation(control, || {
                    write_create(
                        deployment.fingerprint().api_url(),
                        &self.organization,
                        input_set_id.as_deref(),
                        (
                            Some(&run_idempotency_key),
                            matches!(control.recovery(), CreateRecoveryState::RunDispatched(_, _)),
                        ),
                        Err(failure),
                        self.options.authentication.kind(),
                        self.options.json,
                    )
                });
            }
        };
        let mut snapshot = CloudSnapshot::for_run(acceptance.run_id);
        snapshot.replayed = Some(acceptance.replayed);
        if !control.update_recovery(CreateRecoveryState::Accepted(snapshot.clone())) {
            return Ok(ExitCode::GeneralFailure);
        }
        if !self.wait.wait {
            return finish_accepted(
                control,
                "create",
                deployment,
                &self.organization,
                &snapshot,
                self.options.json,
            );
        }
        let (clock, started) = start_cloud_observation(timeout_start);
        let result = observation::wait_run(
            observation::ObservationContext {
                deployment,
                options: &self.options,
                organization: &self.organization,
                snapshot: &snapshot,
                timeout: self.wait.timeout,
                started,
            },
            control,
            &clock,
            |latest| {
                control.update_recovery(CreateRecoveryState::Accepted(latest));
            },
        );
        finish_cloud_observation(
            deployment.fingerprint().api_url(),
            "create",
            &self.organization,
            result,
            control,
            || {
                (
                    match control.recovery() {
                        CreateRecoveryState::Accepted(latest) => latest,
                        _ => snapshot.clone(),
                    },
                    None,
                    None,
                )
            },
            self.options.json,
        )
    }
}

impl ShowCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        let initial = CloudSnapshot::for_run(self.run.run_id.clone());
        let signal_deployment = deployment.clone();
        let timeout_deployment = deployment.clone();
        let organization = self.run.organization.clone();
        let signal_organization = organization.clone();
        let timeout_organization = organization.clone();
        let json = self.options.json;
        let timeout = self.wait.timeout.filter(|_| self.wait.wait);
        super::execute_mutation_with_signals_and_deferred_timeout(
            "Cloud run show",
            initial,
            timeout,
            move |control, timeout_start| {
                timeout_start.start();
                let clock = super::SystemObservationClock;
                let started = scherzo_cloud_support::monotonic_now();
                let snapshot = control.recovery();
                if self.wait.wait {
                    let result = observation::wait_run(
                        observation::ObservationContext {
                            deployment: &deployment,
                            options: &self.options,
                            organization: &self.run.organization,
                            snapshot: &snapshot,
                            timeout: self.wait.timeout,
                            started,
                        },
                        control,
                        &clock,
                        |latest| {
                            control.update_recovery(latest);
                        },
                    );
                    finish_cloud_observation(
                        deployment.fingerprint().api_url(),
                        "show",
                        &self.run.organization,
                        result,
                        control,
                        || (control.recovery(), None, None),
                        self.options.json,
                    )
                } else {
                    let result = observation::show_once(
                        &deployment,
                        &self.options,
                        &self.run.organization,
                        &self.run.run_id,
                    );
                    if let Ok(snapshot) = &result {
                        control.update_recovery(snapshot.clone());
                    }
                    finish_operation(control, || match result {
                        Ok(snapshot) => write_cloud!(
                            "show",
                            deployment.fingerprint().api_url(),
                            &self.run.organization,
                            &snapshot,
                            if snapshot.run.is_some() {
                                "found"
                            } else {
                                "accepted"
                            },
                            None,
                            None,
                            None,
                            self.options.json,
                            ExitCode::Success,
                        ),
                        Err(failure) => {
                            let (code, exit) =
                                failure_code(&failure, self.options.authentication.kind());
                            write_cloud!(
                                "show",
                                deployment.fingerprint().api_url(),
                                &self.run.organization,
                                &CloudSnapshot::for_run(&self.run.run_id),
                                "error",
                                Some(code),
                                None,
                                None,
                                self.options.json,
                                exit,
                            )
                        }
                    })
                }
            },
            move |signal, snapshot| {
                write_cloud!(
                    "show",
                    signal_deployment.fingerprint().api_url(),
                    &signal_organization,
                    &snapshot.recovery,
                    "observation_stopped",
                    Some("observation_stopped"),
                    None,
                    None,
                    json,
                    signal,
                )
                .map_err(Into::into)
            },
            move |snapshot| {
                write_cloud!(
                    "show",
                    timeout_deployment.fingerprint().api_url(),
                    &timeout_organization,
                    &snapshot.recovery,
                    "timed_out",
                    Some("wait_timed_out"),
                    None,
                    None,
                    json,
                    ExitCode::GeneralFailure,
                )
                .map_err(Into::into)
            },
        )
    }
}

#[derive(Clone, Debug)]
struct CancelRecovery {
    key: String,
    mode: &'static str,
    accepted: bool,
    snapshot: CloudSnapshot,
}

impl CancelCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        let key = match self.idempotency_key.clone() {
            Some(key) => key,
            None => scherzo_cloud_support::generate_idempotency_key()
                .context("generate Cloud cancellation request identity")?,
        };
        let mode = if self.force { "force" } else { "graceful" };
        let recovery = CancelRecovery {
            key,
            mode,
            accepted: false,
            snapshot: CloudSnapshot::for_run(self.run.run_id.clone()),
        };
        let signal_deployment = deployment.clone();
        let timeout_deployment = deployment.clone();
        let signal_organization = self.run.organization.clone();
        let timeout_organization = signal_organization.clone();
        let json = self.options.json;
        let timeout = self.wait.timeout.filter(|_| self.wait.wait);
        super::execute_mutation_with_signals_and_deferred_timeout(
            "Cloud run cancellation",
            recovery,
            timeout,
            move |control, timeout_start| {
                self.execute_blocking(&deployment, control, timeout_start)
            },
            move |signal, snapshot| {
                let recovery = snapshot.recovery;
                let (outcome, code) = if recovery.accepted {
                    ("observation_stopped", "observation_stopped")
                } else {
                    ("acceptance_unknown", "acceptance_unknown")
                };
                write_cloud!(
                    "cancel",
                    signal_deployment.fingerprint().api_url(),
                    &signal_organization,
                    &recovery.snapshot,
                    outcome,
                    Some(code),
                    Some(&recovery.key),
                    Some(recovery.mode),
                    json,
                    signal,
                )
                .map_err(Into::into)
            },
            move |snapshot| {
                let recovery = snapshot.recovery;
                write_cloud!(
                    "cancel",
                    timeout_deployment.fingerprint().api_url(),
                    &timeout_organization,
                    &recovery.snapshot,
                    "timed_out",
                    Some("wait_timed_out"),
                    Some(&recovery.key),
                    Some(recovery.mode),
                    json,
                    ExitCode::GeneralFailure,
                )
                .map_err(Into::into)
            },
        )
    }

    fn execute_blocking(
        self,
        deployment: &Deployment,
        control: &super::OperationControl<CancelRecovery>,
        timeout_start: &super::DeferredObservationTimeoutStart,
    ) -> super::CommandResult {
        let recovery = control.recovery();
        let mode = if self.force {
            RunCancellationMode::Force
        } else {
            RunCancellationMode::Graceful
        };
        let result = with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                api.cancel(
                    &self.run.organization,
                    &self.run.run_id,
                    &recovery.key,
                    mode,
                    || control.begin_dispatch(),
                )
            },
        );
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                let dispatched = control.dispatched();
                return finish_operation(control, || {
                    write_cloud!(
                        "cancel",
                        deployment.fingerprint().api_url(),
                        &self.run.organization,
                        &recovery.snapshot,
                        if dispatched {
                            "acceptance_unknown"
                        } else {
                            "error"
                        },
                        Some(if dispatched {
                            "acceptance_unknown"
                        } else {
                            "submission_failed"
                        }),
                        Some(&recovery.key),
                        Some(recovery.mode),
                        self.options.json,
                        if dispatched {
                            ExitCode::Unavailable
                        } else {
                            ExitCode::GeneralFailure
                        },
                    )
                });
            }
        };
        let envelope = match result {
            Ok(envelope) => envelope,
            Err(failure) => {
                return finish_operation(control, || {
                    write_cancel_failure(
                        deployment.fingerprint().api_url(),
                        &self.run.organization,
                        &recovery,
                        &failure,
                        control.dispatched(),
                        self.options.authentication.kind(),
                        self.options.json,
                    )
                });
            }
        };
        let mut snapshot = recovery.snapshot.clone();
        snapshot.run = envelope.run;
        snapshot.cancellation_request = Some(envelope.request);
        let accepted = CancelRecovery {
            accepted: true,
            snapshot: snapshot.clone(),
            ..recovery
        };
        if !control.update_recovery(accepted.clone()) {
            return Ok(ExitCode::GeneralFailure);
        }
        if !self.wait.wait {
            return finish_accepted(
                control,
                "cancel",
                deployment,
                &self.run.organization,
                &snapshot,
                self.options.json,
            );
        }
        let (clock, started) = start_cloud_observation(timeout_start);
        let result = observation::wait_cancellation(
            observation::ObservationContext {
                deployment,
                options: &self.options,
                organization: &self.run.organization,
                snapshot: &snapshot,
                timeout: self.wait.timeout,
                started,
            },
            control,
            &clock,
            |latest| {
                control.update_recovery(CancelRecovery {
                    snapshot: latest,
                    ..accepted.clone()
                });
            },
        );
        finish_cloud_observation(
            deployment.fingerprint().api_url(),
            "cancel",
            &self.run.organization,
            result,
            control,
            || {
                let recovery = control.recovery();
                (recovery.snapshot, Some(recovery.key), Some(recovery.mode))
            },
            self.options.json,
        )
    }
}

fn finish_cloud_observation<R, S>(
    deployment: &str,
    operation: &'static str,
    organization: &str,
    result: Result<super::TerminalObservation<CloudSnapshot, S>, RunFailure>,
    control: &super::OperationControl<R>,
    latest: impl Fn() -> (CloudSnapshot, Option<String>, Option<&'static str>),
    json: bool,
) -> super::CommandResult {
    finish_operation(control, || match result {
        Ok(super::TerminalObservation::Terminal { resource, .. }) => {
            let rejected = operation == "cancel"
                && resource
                    .cancellation_request
                    .as_deref()
                    .and_then(|receipt| receipt.resolution.as_deref())
                    .is_some_and(|resolution| {
                        resolution.kind == RunCancellationResolutionKind::CreationRejected
                    });
            if rejected {
                let (_, key, mode) = latest();
                return write_cloud!(
                    operation,
                    deployment,
                    organization,
                    &resource,
                    "error",
                    Some("creation_rejected"),
                    key.as_deref(),
                    mode,
                    json,
                    ExitCode::GeneralFailure,
                );
            }
            let exit = if operation == "create"
                && resource.run.as_deref().is_some_and(|run| {
                    run.state != RunState::Succeeded
                        || run.publication.as_deref().is_some_and(|handoff| {
                            handoff.state == scherzo_cloud_api::RunPublicationHandoffState::Failed
                        })
                        || resource.publication.as_deref().is_some_and(|publication| {
                            publication.state != scherzo_cloud_api::PublicationState::Succeeded
                        })
                }) {
                ExitCode::GeneralFailure
            } else {
                ExitCode::Success
            };
            write_cloud!(
                operation,
                deployment,
                organization,
                &resource,
                "settled",
                None,
                None,
                None,
                json,
                exit,
            )
        }
        Ok(super::TerminalObservation::TimedOut) => {
            let (snapshot, key, mode) = latest();
            write_cloud!(
                operation,
                deployment,
                organization,
                &snapshot,
                "timed_out",
                Some("wait_timed_out"),
                key.as_deref(),
                mode,
                json,
                ExitCode::GeneralFailure
            )
        }
        Ok(super::TerminalObservation::Stopped) => Ok(ExitCode::GeneralFailure),
        Err(failure) => {
            let (code, exit) =
                failure_code(&failure, super::PrincipalAuthenticationKind::HumanSession);
            let code = if matches!(failure, RunFailure::Unreachable(_)) {
                "observation_failed"
            } else {
                code
            };
            let (snapshot, key, mode) = latest();
            write_cloud!(
                operation,
                deployment,
                organization,
                &snapshot,
                "error",
                Some(code),
                key.as_deref(),
                mode,
                json,
                exit
            )
        }
    })
}

fn failure_code(
    failure: &RunFailure,
    _authentication: super::PrincipalAuthenticationKind,
) -> (&'static str, ExitCode) {
    match failure {
        RunFailure::Unauthenticated => {
            ("authentication_required", ExitCode::AuthenticationRequired)
        }
        RunFailure::Forbidden => ("forbidden", ExitCode::GeneralFailure),
        RunFailure::InvalidInput => ("invalid_input", ExitCode::GeneralFailure),
        RunFailure::NotFound => ("not_found", ExitCode::GeneralFailure),
        RunFailure::IdempotencyConflict => ("idempotency_conflict", ExitCode::GeneralFailure),
        RunFailure::Conflict => ("submission_failed", ExitCode::GeneralFailure),
        RunFailure::CreationRejected => ("creation_rejected", ExitCode::GeneralFailure),
        RunFailure::Unreachable(_) => ("unavailable", ExitCode::Unavailable),
        RunFailure::Protocol { .. } => ("protocol_error", ExitCode::GeneralFailure),
        RunFailure::Gone | RunFailure::InputUploadRejected | RunFailure::InputDownloadRejected => {
            ("submission_failed", ExitCode::GeneralFailure)
        }
        RunFailure::Interrupted => ("acceptance_unknown", ExitCode::Interrupted),
    }
}

fn write_cancel_failure(
    deployment: &str,
    organization: &str,
    recovery: &CancelRecovery,
    failure: &RunFailure,
    dispatched: bool,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (outcome, code, exit) = if dispatched
        && (matches!(
            failure,
            RunFailure::Unreachable(category) if *category != scherzo_cloud_api::UnreachableCategory::RateLimited
        ) || matches!(failure, RunFailure::Protocol { .. }))
    {
        (
            "acceptance_unknown",
            "acceptance_unknown",
            ExitCode::Unavailable,
        )
    } else {
        let (code, exit) = failure_code(failure, authentication);
        ("error", code, exit)
    };
    write_cloud!(
        "cancel",
        deployment,
        organization,
        &recovery.snapshot,
        outcome,
        Some(code),
        Some(&recovery.key),
        Some(recovery.mode),
        json,
        exit,
    )
}

#[cfg(test)]
trait RunObservationApi {
    fn get_run(&self, organization: &str, run_id: &str) -> Result<RunRead, RunFailure>;
}

#[cfg(test)]
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

#[cfg(test)]
type WaitObservation = super::TerminalObservation<Run, TerminalRunState>;

#[cfg(test)]
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

fn run_api<'a>(
    client: &'a scherzo_cloud_api::HttpClient,
    deployment: &Deployment,
    access_token: &str,
    transport_policy: HttpTransportPolicy,
) -> anyhow::Result<RunApi<'a>> {
    RunApi::new(
        deployment.fingerprint().api_url(),
        access_token,
        transport_policy,
        client,
    )
    .map_err(|error| anyhow!(error))
    .context("prepare Cloud run networking")
}

fn with_api_until<T>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    authentication: &super::PrincipalAuthenticationArgs,
    deadline: Option<Instant>,
    mut operation: impl FnMut(&RunApi<'_>, Option<Duration>) -> Result<T, RunFailure>,
) -> anyhow::Result<Result<T, RunFailure>> {
    let client = super::human_session_client(transport_policy)?;
    super::execute_selected_api_observation(
        super::principal_api_context(
            &client,
            deployment,
            authentication,
            "acquire human session for Cloud run observation",
        ),
        |access_token, _remaining| {
            let api = run_api(&client, deployment, access_token, transport_policy)?;
            let remaining = match super::observation_http_budget(deadline) {
                Ok(remaining) => remaining,
                Err(category) => return Ok(Err(RunFailure::Unreachable(category))),
            };
            Ok(operation(&api, remaining))
        },
        RunFailure::credential_rejected,
        || RunFailure::Unauthenticated,
        RunFailure::Unreachable,
        deadline,
    )
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
            let api = run_api(&client, deployment, access_token, transport_policy)?;
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
        return write_cloud!(
            "create",
            deployment,
            organization,
            &CloudSnapshot::default(),
            "error",
            Some("invalid_input"),
            None,
            None,
            true,
            ExitCode::GeneralFailure,
        );
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
    submission: (Option<&str>, bool),
    result: Result<scherzo_cloud_api::RunCreationAcceptance, RunFailure>,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (key, run_dispatched) = submission;
    match result {
        Ok(acceptance) => {
            if json {
                let mut snapshot = CloudSnapshot::for_run(&acceptance.run_id);
                snapshot.replayed = Some(acceptance.replayed);
                return write_cloud!(
                    "create",
                    deployment,
                    organization,
                    &snapshot,
                    "accepted",
                    None,
                    None,
                    None,
                    true,
                    ExitCode::Success,
                );
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
        Err(failure)
            if json
                || (run_dispatched
                    && matches!(failure,
                        RunFailure::Unreachable(category) if category != scherzo_cloud_api::UnreachableCategory::RateLimited
                    ))
                || (run_dispatched && matches!(failure, RunFailure::Protocol { .. })) =>
        {
            let (code, exit) = failure_code(&failure, authentication);
            let uncertain = run_dispatched
                && (matches!(failure,
                RunFailure::Unreachable(category) if category != scherzo_cloud_api::UnreachableCategory::RateLimited)
                    || matches!(failure, RunFailure::Protocol { .. }));
            if let Some(input_set_id) = input_set_id {
                write_staging_guidance(organization, input_set_id)?;
            }
            write_cloud!(
                "create",
                deployment,
                organization,
                &CloudSnapshot::default(),
                if uncertain {
                    "acceptance_unknown"
                } else {
                    "error"
                },
                Some(if uncertain {
                    "acceptance_unknown"
                } else {
                    code
                }),
                key,
                None,
                json,
                if uncertain {
                    ExitCode::Unavailable
                } else {
                    exit
                },
            )
        }
        Err(failure) => write_failure_with_input_set(
            deployment,
            organization,
            None,
            input_set_id,
            &failure,
            authentication,
            false,
        ),
    }
}

fn write_observation_human(
    out: &mut impl io::Write,
    observation: &RunObservation,
) -> anyhow::Result<()> {
    writeln!(out, "\nobserved at: {}", observation.observed_at)?;
    writeln!(out, "placement (current attempt):")?;
    if let Some(place) = observation.placement.as_deref() {
        writeln!(out, "  runner: {} ({})", place.runner_name, place.runner_id)?;
        writeln!(out, "  pool: {} ({})", place.pool_name, place.pool_id)?;
    } else {
        writeln!(out, "  none")?;
    }
    writeln!(out, "Cloud assignment and runner connection:")?;
    if let Some(assignment) = observation.assignment.as_deref() {
        writeln!(
            out,
            "  assignment: {} ({})",
            assignment.id,
            enum_text(&assignment.state)?
        )?;
        writeln!(
            out,
            "  target: {} / {} / generation {}",
            assignment.runner_id, assignment.boot_id, assignment.presence_generation
        )?;
        writeln!(
            out,
            "  Cloud lease: sequence {}, expires {} ({})",
            assignment
                .lease_sequence
                .map_or_else(|| "none".to_owned(), |n| n.to_string()),
            assignment.lease_expires_at.as_deref().unwrap_or("none"),
            if assignment.lease_valid {
                "valid"
            } else {
                "not valid"
            }
        )?;
        writeln!(
            out,
            "  matching runner connected: {}",
            assignment.runner_connected
        )?;
        writeln!(
            out,
            "  matching runner last seen: {}",
            assignment.runner_last_seen_at.as_deref().unwrap_or("none")
        )?;
    } else {
        writeln!(out, "  no current assignment")?;
    }
    writeln!(out, "last coordinator-accepted workflow transition:")?;
    if let Some(progress) = observation.last_transition.as_deref() {
        writeln!(
            out,
            "  recorded: {} (attempt {})",
            progress.recorded_at, progress.attempt_id
        )?;
        writeln!(
            out,
            "  kind: {} (transition {}, event {})",
            enum_text(&progress.kind)?,
            progress.transition_sequence,
            progress.event_sequence
        )?;
        writeln!(
            out,
            "  step: {}",
            progress.step_id.as_deref().unwrap_or("none")
        )?;
        writeln!(
            out,
            "  target state: {}",
            progress.target_state.as_deref().unwrap_or("unavailable")
        )?;
    } else {
        writeln!(out, "  none reported")?;
    }
    Ok(())
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
    writeln!(stdout, "\nautomatic publication handoff:")?;
    if let Some(handoff) = run.publication.as_deref() {
        writeln!(stdout, "  export: {}", visible_text(&handoff.export_name))?;
        writeln!(stdout, "  state: {}", enum_text(&handoff.state)?)?;
        writeln!(
            stdout,
            "  publication: {}",
            handoff.publication_id.as_deref().unwrap_or("none")
        )?;
        if let Some(failure) = handoff.failure.as_deref() {
            writeln!(stdout, "  failure: {}", enum_text(&failure.code)?)?;
            writeln!(stdout, "  phase: {}", enum_text(&failure.phase)?)?;
            writeln!(stdout, "  retryable: {}", failure.retryable)?;
        }
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
    if let Some(observation) = run.observation.as_deref() {
        write_observation_human(&mut stdout, observation)?;
    } else {
        writeln!(stdout, "\nassignment and reported progress: unavailable")?;
    }
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
        RunFailure::Conflict | RunFailure::IdempotencyConflict => (
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

fn write_staging_guidance(organization: &str, input_set_id: &str) -> anyhow::Result<()> {
    writeln!(
        io::stderr().lock(),
        "input set: {input_set_id}\norganization: {organization}\n\nInspect with `scherzo-cloud run input-set show`; if open, resume upload and sealing, or delete the set explicitly. Reconcile any uncertain run acceptance before creating another run."
    )?;
    Ok(())
}

fn write_staging_recovery(organization: &str, input_set_id: &str) -> anyhow::Result<()> {
    writeln!(
        io::stderr().lock(),
        "error: Run Input Set preparation was interrupted"
    )?;
    write_staging_guidance(organization, input_set_id)
}

fn write_create_unknown(
    deployment: &str,
    organization: &str,
    input_set_id: Option<&str>,
    key: Option<&str>,
    json: bool,
    exit_code: ExitCode,
) -> anyhow::Result<ExitCode> {
    if json {
        if let Some(input_set_id) = input_set_id {
            write_staging_guidance(organization, input_set_id)?;
        }
        return write_cloud!(
            "create",
            deployment,
            organization,
            &CloudSnapshot::default(),
            "acceptance_unknown",
            Some("acceptance_unknown"),
            key,
            None,
            true,
            exit_code,
        );
    } else {
        writeln!(
            io::stderr().lock(),
            "error: run acceptance is unknown after interruption\n\norganization: {organization}\ninput set: {}\nidempotency key: {}\ncommitment: unknown\n\nReconcile this key with the deployment before creating another run.",
            input_set_id.unwrap_or("none"),
            key.unwrap_or("not established")
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

    #[test]
    fn run_show_keeps_placement_and_reported_progress_separate() {
        let observation: RunObservation = serde_json::from_value(serde_json::json!({
            "observedAt": "2026-08-03T12:00:00Z",
            "placement": {"runnerId": "runner-b", "runnerName": "second",
                "poolId": "pool-b", "poolName": "work"},
            "assignment": {"id": "assignment-b", "state": "active", "runnerId": "runner-b",
                "bootId": "boot-b", "presenceGeneration": 2, "leaseSequence": 9,
                "leaseExpiresAt": "2026-08-03T13:00:00Z", "leaseValid": false,
                "runnerConnected": true, "runnerLastSeenAt": null},
            "lastTransition": {"attemptId": "attempt-a", "eventSequence": 3,
                "transitionSequence": 2, "recordedAt": "2026-08-03T11:00:00Z",
                "kind": "step_state_changed", "stepId": "build", "targetState": "running"}
        }))
        .expect("valid run observation");
        let mut output = Vec::new();
        write_observation_human(&mut output, &observation).expect("render observation");
        let output = String::from_utf8(output).expect("human report is UTF-8");
        for identifier in ["runner-b", "pool-b", "assignment-b", "attempt-a", "build"] {
            assert!(output.contains(identifier), "missing {identifier}");
        }
        // The public projection is serialized by the run-show command; the human
        // rendering must keep placement separate from coordinator progress.
    }

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
            CreateRecoveryState::RunDispatched(
                Some(CreateInputSetOwnership::Explicit("ris_explicit".to_owned())),
                None
            )
        );
        assert_eq!(
            create_signal_recovery(snapshot),
            CreateSignalRecovery::Run(Some("ris_explicit".to_owned()), None)
        );

        let inputless = super::super::OperationControl::new(CreateRecoveryState::new(None));
        assert!(inputless.begin_dispatch_with_recovery(inputless.recovery().run_dispatched()));
        assert_eq!(
            create_signal_recovery(inputless.claim_signal().unwrap()),
            CreateSignalRecovery::Run(None, None)
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
                CreateSignalRecovery::Run(Some("ris_explicit".to_owned()), None)
            );
            release_retry.send(()).unwrap();
            assert_interrupted(worker.join().unwrap());
            server.join().unwrap();
        });

        listener.set_nonblocking(true).unwrap();
        assert_no_pending_request(&listener);
    }

    fn receipt(state: &str, run: Option<Run>) -> scherzo_cloud_api::RunCancellationEnvelope {
        let resolution = if state == "resolved" {
            serde_json::json!({"kind":"applied", "resolvedAt":"2026-08-10T12:05:00Z",
                "effectiveRequestId":"cmd_01k0z6r1w8f4jy2m7q9v3x5abc", "runVersion":1})
        } else {
            serde_json::Value::Null
        };
        serde_json::from_value(serde_json::json!({
            "request": {"id":"cmd_01k0z6r1w8f4jy2m7q9v3x5abc",
                "organizationId":"org_01k0z6r1w8f4jy2m7q9v3x5abc",
                "runId":"run_01k0z6r1w8f4jy2m7q9v3x5abc", "attemptId":null,
                "mode":"graceful", "acceptedAt":"2026-08-10T12:00:00Z",
                "state": state, "resolution": resolution},
            "run": run
        }))
        .unwrap()
    }

    fn scripted_cancellation(
        responses: impl IntoIterator<
            Item = Result<scherzo_cloud_api::RunCancellationEnvelope, RunFailure>,
        >,
        mut record: impl FnMut(CloudSnapshot),
    ) -> (
        Result<super::super::TerminalObservation<CloudSnapshot, ()>, RunFailure>,
        Vec<Duration>,
        usize,
    ) {
        let mut snapshot = CloudSnapshot::for_run("run_01k0z6r1w8f4jy2m7q9v3x5abc");
        snapshot.cancellation_request = Some(receipt("pending", None).request);
        let responses = RefCell::new(responses.into_iter().collect::<VecDeque<_>>());
        let clock = ControlledWaitClock::new(scherzo_cloud_support::monotonic_now());
        let result = observation::wait_receipt_with(
            |_| responses.borrow_mut().pop_front().unwrap(),
            &snapshot,
            None,
            scherzo_cloud_support::monotonic_now(),
            &super::super::BlockingObservationControl::new(),
            &clock,
            &mut record,
        );
        (result, clock.into_sleeps(), responses.into_inner().len())
    }

    #[test]
    fn cancellation_observation_requires_both_resolved_receipt_and_terminal_run() {
        let recorded = RefCell::new(Vec::new());
        let (result, sleeps, remaining) = scripted_cancellation(
            [
                Ok(receipt("pending", Some(run(RunState::Succeeded)))),
                Ok(receipt("resolved", Some(run(RunState::Running)))),
                Ok(receipt("resolved", Some(run(RunState::Cancelled)))),
            ],
            |latest| recorded.borrow_mut().push(latest),
        );
        let result = result.unwrap();
        assert!(
            matches!(result, super::super::TerminalObservation::Terminal { resource, .. }
            if resource.run.as_deref().is_some_and(|run| run.state == RunState::Cancelled))
        );
        assert_eq!(recorded.borrow().len(), 3);
        assert_eq!(remaining, 0);
        assert_eq!(sleeps, vec![Duration::from_secs(2); 2]);
    }

    #[test]
    fn cancellation_observation_fails_on_second_consecutive_retryable_error() {
        let (result, sleeps, remaining) = scripted_cancellation(
            [
                Err(RunFailure::Unreachable(UnreachableCategory::Server)),
                Err(RunFailure::Unreachable(UnreachableCategory::Server)),
            ],
            |_| {},
        );
        assert!(matches!(
            result,
            Err(RunFailure::Unreachable(UnreachableCategory::Server))
        ));
        assert_eq!(remaining, 0);
        assert_eq!(sleeps, vec![Duration::from_secs(2)]);
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
