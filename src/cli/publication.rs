use std::io::{self, Write};
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::deployment::Deployment;
use scherzo_cloud_api::{
    HttpTransportPolicy, Publication, PublicationApi, PublicationFailure, PublicationList,
    PublicationState,
};

use super::OrganizationArg;

pub(super) const ABOUT: &str = "Work with Scherzo Cloud publications";
const NAME: &str = "publication";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<PublicationCommand>,
}

#[derive(Debug, Subcommand)]
enum PublicationCommand {
    #[command(about = "Create a Scherzo Cloud publication")]
    Create(CreateCommand),
    #[command(about = "List Scherzo Cloud publications")]
    List(ListCommand),
    #[command(about = "Show a Scherzo Cloud publication")]
    Show(ShowCommand),
}

#[derive(Debug, Args)]
struct PublicationRunReference {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(value_name = "RUN", value_parser = parse_run_id, help = "Exact Run ID")]
    run_id: String,
}

type Options = super::CommonArgs<super::PublicationJson, super::PrincipalAuthenticationArgs>;

#[derive(Debug, Args)]
struct PublicationWaitArgs {
    #[arg(long, help = "Wait for a terminal publication")]
    wait: bool,

    #[arg(
        long,
        requires = "wait",
        value_name = "DURATION",
        value_parser = super::parse_wait_timeout,
        help = "Stop waiting after a positive duration (units: ms, s, m, or h)"
    )]
    timeout: Option<Duration>,
}

#[derive(Debug, Args)]
struct CreateCommand {
    #[command(flatten)]
    run: PublicationRunReference,

    #[arg(
        long,
        value_name = "NAME",
        value_parser = parse_export_name,
        help = "Exact available Git branch export name"
    )]
    export: String,

    #[arg(
        long,
        value_name = "KEY",
        value_parser = parse_idempotency_key,
        help = "Opaque request identity (generated when omitted)"
    )]
    idempotency_key: Option<String>,

    #[command(flatten)]
    wait: PublicationWaitArgs,

    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct PublicationReference {
    #[command(flatten)]
    run: PublicationRunReference,

    #[arg(
        value_name = "PUBLICATION",
        value_parser = parse_publication_id,
        help = "Exact Publication ID"
    )]
    publication_id: String,
}

#[derive(Debug, Args)]
struct ShowCommand {
    #[command(flatten)]
    publication: PublicationReference,

    #[command(flatten)]
    wait: PublicationWaitArgs,

    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct ListCommand {
    #[command(flatten)]
    run: PublicationRunReference,

    #[command(flatten)]
    pagination: super::PaginationArgs,

    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(PublicationCommand::Create(command)) => super::execute_deployment_command(
                Some(command),
                &[NAME],
                "configure Scherzo Cloud publication creation",
                |command, deployment| command.execute(deployment.clone()),
            ),
            Some(PublicationCommand::Show(command)) => super::execute_deployment_command(
                Some(command),
                &[NAME],
                "configure Scherzo Cloud publication access",
                |command, deployment| command.execute(deployment.clone()),
            ),
            Some(PublicationCommand::List(command)) => super::execute_deployment_command(
                Some(command),
                &[NAME],
                "configure Scherzo Cloud publication access",
                |command, deployment| command.execute(deployment.clone()),
            ),
        }
    }
}

#[derive(Clone)]
struct CreateRecovery {
    idempotency_key: String,
    publication_id: Option<String>,
}

impl CreateCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        let idempotency_key = match self.idempotency_key.clone() {
            Some(key) => key,
            None => scherzo_cloud_support::generate_idempotency_key()
                .context("generate Cloud publication request identity")?,
        };
        if self.wait.wait {
            self.execute_waiting(deployment, idempotency_key)
        } else {
            self.execute_without_wait(deployment, idempotency_key)
        }
    }

    fn execute_without_wait(
        self,
        deployment: Deployment,
        idempotency_key: String,
    ) -> super::CommandResult {
        let signal_deployment = deployment.fingerprint().api_url().to_owned();
        let signal_organization = self.run.organization.clone();
        let signal_run_id = self.run.run_id.clone();
        let signal_export = self.export.clone();
        let signal_json = self.options.json;
        let operation_key = idempotency_key.clone();
        super::execute_mutation_with_signals(
            "Cloud publication creation",
            idempotency_key,
            move |control| self.execute_blocking(&deployment, &operation_key, control),
            move |signal, snapshot| {
                super::report_dispatched_signal(signal, snapshot, |idempotency_key| {
                    write_unknown(
                        &signal_deployment,
                        &signal_organization,
                        &signal_run_id,
                        &signal_export,
                        &idempotency_key,
                        signal_json,
                        signal,
                    )
                    .map_err(Into::into)
                })
            },
        )
    }

    fn execute_waiting(
        self,
        deployment: Deployment,
        idempotency_key: String,
    ) -> super::CommandResult {
        let timeout = self.wait.timeout;
        let signal_deployment = deployment.fingerprint().api_url().to_owned();
        let signal_organization = self.run.organization.clone();
        let signal_run_id = self.run.run_id.clone();
        let signal_export = self.export.clone();
        let signal_json = self.options.json;
        let timeout_deployment = signal_deployment.clone();
        let timeout_organization = signal_organization.clone();
        let timeout_run_id = signal_run_id.clone();
        let timeout_json = signal_json;
        let operation_key = idempotency_key.clone();
        super::execute_mutation_with_signals_and_deferred_timeout(
            "Cloud publication creation and observation",
            CreateRecovery {
                idempotency_key,
                publication_id: None,
            },
            timeout,
            move |control, timeout_start| {
                self.execute_waiting_blocking(&deployment, &operation_key, control, timeout_start)
            },
            move |signal, snapshot| {
                if snapshot.recovery.publication_id.is_some() {
                    Ok(signal)
                } else {
                    super::report_dispatched_signal(signal, snapshot, |recovery| {
                        write_unknown(
                            &signal_deployment,
                            &signal_organization,
                            &signal_run_id,
                            &signal_export,
                            &recovery.idempotency_key,
                            signal_json,
                            signal,
                        )
                        .map_err(Into::into)
                    })
                }
            },
            move |snapshot| {
                let Some(publication_id) = snapshot.recovery.publication_id else {
                    return Ok(ExitCode::GeneralFailure);
                };
                write_wait_timeout(&WaitOutputContext {
                    deployment: timeout_deployment,
                    organization: String::from(&*timeout_organization),
                    run_id: timeout_run_id,
                    publication_id,
                    idempotency_key: Some(snapshot.recovery.idempotency_key),
                    json: timeout_json,
                })
                .map_err(Into::into)
            },
        )
    }

    fn submit(
        &self,
        deployment: &Deployment,
        idempotency_key: &str,
        begin_dispatch: impl Fn() -> bool,
    ) -> anyhow::Result<Result<Publication, PublicationFailure>> {
        with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                api.create(
                    &self.run.organization,
                    &self.run.run_id,
                    &self.export,
                    idempotency_key,
                    &begin_dispatch,
                )
            },
        )
    }

    fn execute_blocking(
        self,
        deployment: &Deployment,
        idempotency_key: &str,
        control: &super::OperationControl<String>,
    ) -> super::CommandResult {
        let result = self.submit(deployment, idempotency_key, || control.begin_dispatch());
        super::complete_operation(control, || match result {
            Ok(result) => write_create(
                &CreateOutputContext {
                    deployment: deployment.fingerprint().api_url(),
                    organization: &self.run.organization,
                    run_id: &self.run.run_id,
                    export_name: &self.export,
                    idempotency_key,
                    authentication: self.options.authentication.kind(),
                    json: self.options.json,
                    dispatched: control.dispatched(),
                },
                result,
            )
            .map_err(Into::into),
            Err(_) if control.dispatched() => write_unknown(
                deployment.fingerprint().api_url(),
                &self.run.organization,
                &self.run.run_id,
                &self.export,
                idempotency_key,
                self.options.json,
                ExitCode::GeneralFailure,
            )
            .map_err(Into::into),
            Err(error) => Err(error.into()),
        })
    }

    fn execute_waiting_blocking(
        self,
        deployment: &Deployment,
        idempotency_key: &str,
        control: &super::OperationControl<CreateRecovery>,
        timeout_start: &super::DeferredObservationTimeoutStart,
    ) -> super::CommandResult {
        let result = self.submit(deployment, idempotency_key, || control.begin_dispatch());
        let publication = match result {
            Ok(Ok(publication)) => publication,
            Ok(Err(failure)) => {
                return super::complete_operation(control, || {
                    write_create(
                        &CreateOutputContext {
                            deployment: deployment.fingerprint().api_url(),
                            organization: &self.run.organization,
                            run_id: &self.run.run_id,
                            export_name: &self.export,
                            idempotency_key,
                            authentication: self.options.authentication.kind(),
                            json: self.options.json,
                            dispatched: control.dispatched(),
                        },
                        Err(failure),
                    )
                    .map_err(Into::into)
                });
            }
            Err(_) if control.dispatched() => {
                return super::complete_operation(control, || {
                    write_unknown(
                        deployment.fingerprint().api_url(),
                        &self.run.organization,
                        &self.run.run_id,
                        &self.export,
                        idempotency_key,
                        self.options.json,
                        ExitCode::GeneralFailure,
                    )
                    .map_err(Into::into)
                });
            }
            Err(error) => {
                return super::complete_operation(control, || Err(error.into()));
            }
        };

        if !control.update_recovery(CreateRecovery {
            idempotency_key: idempotency_key.to_owned(),
            publication_id: Some(publication.id.clone()),
        }) {
            return Ok(ExitCode::GeneralFailure);
        }
        timeout_start.start();

        let publication_id = publication.id.clone();
        let result = match terminal_publication_state(&publication) {
            Some(state) => Ok(WaitObservation::Terminal {
                resource: Box::new(publication),
                state,
            }),
            None => {
                let clock = super::SystemObservationClock;
                let reference = PublicationObservationReference {
                    organization: &self.run.organization,
                    run_id: &self.run.run_id,
                    publication_id: &publication_id,
                };
                let transport_policy = self.options.http.transport_policy();
                wait_for_terminal_publication(
                    || {
                        observe_publication(
                            deployment,
                            transport_policy,
                            &self.options.authentication,
                            reference,
                        )
                    },
                    PublicationObservationFailure::retryable,
                    self.wait.timeout,
                    control,
                    &clock,
                )
            }
        };
        if !control.begin_completion() {
            return Ok(ExitCode::GeneralFailure);
        }
        let read_context = ReadOutputContext {
            deployment: deployment.fingerprint().api_url(),
            organization: &self.run.organization,
            run_id: &self.run.run_id,
            publication_id: Some(&publication_id),
            idempotency_key: Some(idempotency_key),
            authentication: self.options.authentication.kind(),
            json: self.options.json,
        };
        write_wait_observation(
            result,
            &read_context,
            &WaitOutputContext {
                deployment: read_context.deployment.to_owned(),
                organization: read_context.organization.to_owned(),
                run_id: read_context.run_id.to_owned(),
                publication_id: publication_id.clone(),
                idempotency_key: Some(idempotency_key.to_owned()),
                json: read_context.json,
            },
            TerminalPublicationState::exit_code,
        )
    }
}

impl ShowCommand {
    fn output_context(&self, deployment: &Deployment) -> WaitOutputContext {
        WaitOutputContext {
            deployment: deployment.fingerprint().api_url().to_owned(),
            organization: String::from(&*self.publication.run.organization),
            run_id: self.publication.run.run_id.clone(),
            publication_id: self.publication.publication_id.clone(),
            idempotency_key: None,
            json: self.options.json,
        }
    }

    fn execute(self, deployment: Deployment) -> super::CommandResult {
        if self.wait.wait {
            self.execute_waiting(deployment)
        } else {
            self.execute_without_wait(deployment)
        }
    }

    fn execute_without_wait(self, deployment: Deployment) -> super::CommandResult {
        super::execute_read_only_with_signals("Cloud publication show", move |control| {
            let result = with_api(
                &deployment,
                self.options.http.transport_policy(),
                &self.options.authentication,
                |api| {
                    api.get(
                        &self.publication.run.organization,
                        &self.publication.run.run_id,
                        &self.publication.publication_id,
                    )
                },
            )?;
            super::complete_read_only_output(control, || {
                write_show(
                    &ReadOutputContext {
                        deployment: deployment.fingerprint().api_url(),
                        organization: &self.publication.run.organization,
                        run_id: &self.publication.run.run_id,
                        publication_id: Some(&self.publication.publication_id),
                        idempotency_key: None,
                        authentication: self.options.authentication.kind(),
                        json: self.options.json,
                    },
                    result,
                )
                .map_err(Into::into)
            })
        })
    }

    fn execute_waiting(self, deployment: Deployment) -> super::CommandResult {
        let timeout = self.wait.timeout;
        let timeout_context = self.output_context(&deployment);

        super::execute_observation_with_signals_and_timeout(
            "Cloud publication show --wait",
            timeout,
            move |control| self.execute_waiting_blocking(&deployment, control),
            // Run and Publication retain separate typed API/session and timeout renderers; only
            // their polling state machine is shared.
            // jscpd:ignore-start
            move || write_wait_timeout(&timeout_context).map_err(Into::into),
        )
    }

    fn execute_waiting_blocking(
        self,
        deployment: &Deployment,
        control: &super::BlockingObservationControl,
    ) -> super::CommandResult {
        let clock = super::SystemObservationClock;
        let reference = PublicationObservationReference {
            organization: &self.publication.run.organization,
            run_id: &self.publication.run.run_id,
            publication_id: &self.publication.publication_id,
        };
        let transport_policy = self.options.http.transport_policy();
        let result = wait_for_terminal_publication(
            // jscpd:ignore-end
            || {
                observe_publication(
                    deployment,
                    transport_policy,
                    &self.options.authentication,
                    reference,
                )
            },
            PublicationObservationFailure::retryable,
            self.wait.timeout,
            control,
            &clock,
        );
        if !control.begin_completion() {
            return Ok(ExitCode::GeneralFailure);
        }
        let wait_context = self.output_context(deployment);
        let read_context = ReadOutputContext {
            deployment: &wait_context.deployment,
            organization: &wait_context.organization,
            run_id: &wait_context.run_id,
            publication_id: Some(&wait_context.publication_id),
            idempotency_key: None,
            authentication: self.options.authentication.kind(),
            json: wait_context.json,
        };
        write_wait_observation(result, &read_context, &wait_context, |_| ExitCode::Success)
    }
}

impl ListCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        super::execute_read_only_with_signals("Cloud publication list", move |control| {
            let result = with_api(
                &deployment,
                self.options.http.transport_policy(),
                &self.options.authentication,
                |api| {
                    api.list(
                        &self.run.organization,
                        &self.run.run_id,
                        self.pagination.limit,
                        self.pagination.cursor.as_deref(),
                    )
                },
            )?;
            super::complete_read_only_output(control, || {
                // List and show retain separate result envelopes despite sharing run coordinates.
                // jscpd:ignore-start
                write_list(
                    &ReadOutputContext {
                        deployment: deployment.fingerprint().api_url(),
                        organization: &self.run.organization,
                        run_id: &self.run.run_id,
                        publication_id: None,
                        idempotency_key: None,
                        authentication: self.options.authentication.kind(),
                        json: self.options.json,
                    },
                    result,
                )
                // jscpd:ignore-end
                .map_err(Into::into)
            })
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalPublicationState {
    Succeeded,
    Failed,
}

impl TerminalPublicationState {
    const fn outcome(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    const fn heading(self) -> &'static str {
        match self {
            Self::Succeeded => "✓ Publication succeeded.",
            Self::Failed => "✗ Publication failed.",
        }
    }

    const fn exit_code(self) -> ExitCode {
        match self {
            Self::Succeeded => ExitCode::Success,
            Self::Failed => ExitCode::GeneralFailure,
        }
    }
}

type WaitObservation = super::TerminalObservation<Publication, TerminalPublicationState>;

#[derive(Clone, Copy)]
struct PublicationObservationReference<'a> {
    organization: &'a str,
    run_id: &'a str,
    publication_id: &'a str,
}

enum PublicationObservationFailure {
    Api(PublicationFailure),
    Command(anyhow::Error),
}

impl PublicationObservationFailure {
    fn retryable(&self) -> bool {
        matches!(self, Self::Api(failure) if failure.retryable_observation())
    }
}

fn observe_publication(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    authentication: &super::PrincipalAuthenticationArgs,
    reference: PublicationObservationReference<'_>,
) -> Result<Publication, PublicationObservationFailure> {
    with_api(deployment, transport_policy, authentication, |api| {
        api.get(
            reference.organization,
            reference.run_id,
            reference.publication_id,
        )
    })
    .map_err(PublicationObservationFailure::Command)?
    .map_err(PublicationObservationFailure::Api)
}

fn wait_for_terminal_publication<E>(
    observe: impl FnMut() -> Result<Publication, E>,
    retryable_failure: impl Fn(&E) -> bool,
    timeout: Option<Duration>,
    control: &impl super::ObservationControl,
    clock: &impl super::ObservationClock,
) -> Result<WaitObservation, E> {
    super::wait_for_terminal_observation(
        observe,
        terminal_publication_state,
        retryable_failure,
        timeout,
        control,
        clock,
    )
}

fn terminal_publication_state(publication: &Publication) -> Option<TerminalPublicationState> {
    match publication.state {
        PublicationState::Queued | PublicationState::Running => None,
        PublicationState::Succeeded => Some(TerminalPublicationState::Succeeded),
        PublicationState::Failed => Some(TerminalPublicationState::Failed),
    }
}

fn parse_run_id(value: &str) -> Result<String, String> {
    if scherzo_cloud_support::valid_typed_id(value, "run_") {
        Ok(value.to_owned())
    } else {
        Err("must be an exact Run ID (run_ followed by 26 lowercase ULID characters)".to_owned())
    }
}

fn parse_publication_id(value: &str) -> Result<String, String> {
    if scherzo_cloud_support::valid_typed_id(value, "pub_") {
        Ok(value.to_owned())
    } else {
        Err(
            "must be an exact Publication ID (pub_ followed by 26 lowercase ULID characters)"
                .to_owned(),
        )
    }
}

fn parse_export_name(value: &str) -> Result<String, String> {
    if scherzo_cloud_support::is_identifier(value) {
        Ok(value.to_owned())
    } else {
        Err("must be a lower-camel identifier of at most 64 ASCII characters".to_owned())
    }
}

pub(super) fn parse_idempotency_key(value: &str) -> Result<String, String> {
    if (1..=255).contains(&value.len()) && value.bytes().all(|byte| (b'!'..=b'~').contains(&byte)) {
        Ok(value.to_owned())
    } else {
        Err("must contain 1-255 visible ASCII characters without whitespace".to_owned())
    }
}

fn with_api<T>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    authentication: &super::PrincipalAuthenticationArgs,
    mut operation: impl FnMut(&PublicationApi) -> Result<T, PublicationFailure>,
) -> anyhow::Result<Result<T, PublicationFailure>> {
    let client = super::human_session_client(transport_policy)?;
    super::execute_selected_api_operation(
        super::principal_api_context(
            &client,
            deployment,
            authentication,
            "acquire human session for Cloud publication",
        ),
        |access_token| {
            let api = PublicationApi::new(
                deployment.fingerprint().api_url(),
                access_token,
                transport_policy,
            )
            .map_err(|error| anyhow!(error))
            .context("prepare Cloud publication networking")?;
            Ok(operation(&api))
        },
        PublicationFailure::credential_rejected,
        || PublicationFailure::Unauthenticated,
        PublicationFailure::Unreachable,
    )
}

struct CreateOutputContext<'a> {
    deployment: &'a str,
    organization: &'a str,
    run_id: &'a str,
    export_name: &'a str,
    idempotency_key: &'a str,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
    dispatched: bool,
}

struct ReadOutputContext<'a> {
    deployment: &'a str,
    organization: &'a str,
    run_id: &'a str,
    publication_id: Option<&'a str>,
    idempotency_key: Option<&'a str>,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
}

struct WaitOutputContext {
    deployment: String,
    organization: String,
    run_id: String,
    publication_id: String,
    idempotency_key: Option<String>,
    json: bool,
}

fn write_create(
    context: &CreateOutputContext<'_>,
    result: Result<Publication, PublicationFailure>,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(publication) => {
            if context.json {
                super::write_pretty_json(&CreateResult {
                    schema_version: 1,
                    deployment: context.deployment,
                    idempotency_key: context.idempotency_key,
                    publication: &publication,
                })
                .context("write Cloud publication result")?;
            } else {
                let mut output = io::stdout().lock();
                writeln!(output, "✓ Publication accepted.\n")?;
                write_publication_human(
                    &mut output,
                    &publication,
                    HumanPublicationLayout::Details,
                )?;
                writeln!(output, "idempotency key: {}", context.idempotency_key)?;
                writeln!(output, "deployment: {}", context.deployment)?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(context, &failure),
    }
}

fn write_show(
    context: &ReadOutputContext<'_>,
    result: Result<Publication, PublicationFailure>,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(publication) => write_publication_result(
            context,
            &publication,
            "found",
            "✓ Publication found.",
            ExitCode::Success,
        ),
        Err(failure) => write_read_failure(context, &failure),
    }
}

fn write_wait_terminal(
    context: &ReadOutputContext<'_>,
    publication: &Publication,
    state: TerminalPublicationState,
    exit_code: ExitCode,
) -> anyhow::Result<ExitCode> {
    write_publication_result(
        context,
        publication,
        state.outcome(),
        state.heading(),
        exit_code,
    )
}

fn write_wait_observation(
    result: Result<WaitObservation, PublicationObservationFailure>,
    read_context: &ReadOutputContext<'_>,
    wait_context: &WaitOutputContext,
    terminal_exit: impl Fn(TerminalPublicationState) -> ExitCode,
) -> super::CommandResult {
    match result {
        Ok(WaitObservation::Terminal { resource, state }) => {
            write_wait_terminal(read_context, &resource, state, terminal_exit(state))
        }
        Ok(WaitObservation::TimedOut) => write_wait_timeout(wait_context),
        Ok(WaitObservation::Stopped) => Ok(ExitCode::GeneralFailure),
        Err(PublicationObservationFailure::Api(failure)) => {
            write_read_failure(read_context, &failure)
        }
        Err(PublicationObservationFailure::Command(error)) => return Err(error.into()),
    }
    .map_err(Into::into)
}

fn write_publication_result(
    context: &ReadOutputContext<'_>,
    publication: &Publication,
    outcome: &'static str,
    heading: &str,
    exit_code: ExitCode,
) -> anyhow::Result<ExitCode> {
    if context.json {
        super::write_pretty_json(&PublicationResult {
            schema_version: 1,
            deployment: context.deployment,
            outcome,
            idempotency_key: context.idempotency_key,
            publication,
        })
        .context("write Cloud publication result")?;
    } else {
        let mut output = io::stdout().lock();
        writeln!(output, "{heading}\n")?;
        write_publication_human(&mut output, publication, HumanPublicationLayout::Details)?;
        if let Some(idempotency_key) = context.idempotency_key {
            writeln!(output, "idempotency key: {idempotency_key}")?;
        }
        writeln!(output, "deployment: {}", context.deployment)?;
    }
    Ok(exit_code)
}

fn write_wait_timeout(context: &WaitOutputContext) -> anyhow::Result<ExitCode> {
    if context.json {
        super::write_pretty_json(&super::ObservationResult {
            schema_version: 1,
            deployment: &context.deployment,
            outcome: "timed_out",
            organization_ref: &context.organization,
            run_id: &context.run_id,
            publication_id: Some(&context.publication_id),
            idempotency_key: context.idempotency_key.as_deref(),
            category: None,
        })
        .context("write Cloud publication timeout")?;
    } else {
        writeln!(
            io::stderr().lock(),
            "error: Cloud publication observation reached its timeout\n\npublication: {}\nrun: {}\norganization: {}{}\n\nRun the command again with --wait and a longer --timeout, or omit --timeout.",
            context.publication_id,
            context.run_id,
            context.organization,
            context
                .idempotency_key
                .as_ref()
                .map(|key| format!("\nidempotency key: {key}"))
                .unwrap_or_default()
        )?;
    }
    Ok(ExitCode::GeneralFailure)
}

fn write_list(
    context: &ReadOutputContext<'_>,
    result: Result<PublicationList, PublicationFailure>,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(page) => {
            if context.json {
                super::write_cloud_list_json(
                    context.deployment,
                    &page.items,
                    page.next_cursor.as_deref(),
                )
                .context("write Cloud publication list")?;
            } else {
                let mut output = io::stdout().lock();
                writeln!(output, "✓ Publications listed.\n")?;
                for publication in &page.items {
                    write_publication_human(
                        &mut output,
                        publication,
                        HumanPublicationLayout::Summary,
                    )?;
                }
                if !page.items.is_empty() {
                    writeln!(output)?;
                }
                if let Some(cursor) = page.next_cursor {
                    writeln!(output, "next cursor: {}", cursor.escape_default())?;
                }
                writeln!(output, "deployment: {}", context.deployment)?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_read_failure(context, &failure),
    }
}

#[derive(Clone, Copy)]
enum HumanPublicationLayout {
    Details,
    Summary,
}

fn write_publication_human(
    output: &mut impl Write,
    publication: &Publication,
    layout: HumanPublicationLayout,
) -> anyhow::Result<()> {
    let state = enum_text(&publication.state)?;
    if matches!(layout, HumanPublicationLayout::Summary) {
        writeln!(
            output,
            "publication: {} · export: {} · state: {state}",
            publication.id, publication.export_name
        )?;
        if let Some(outcome) = publication.outcome {
            writeln!(output, "  outcome: {}", enum_text(&outcome)?)?;
        }
        if let Some(failure) = publication.failure.as_deref() {
            writeln!(
                output,
                "  failure: {} · phase: {} · retryable: {}",
                enum_text(&failure.code)?,
                enum_text(&failure.phase)?,
                failure.retryable
            )?;
        }
        if let Some(pull_request) = publication.pull_request.as_deref() {
            let url = redacted_human_url(&pull_request.url)?;
            writeln!(output, "  pull request: {}", url.escape_default())?;
        }
        return Ok(());
    }

    writeln!(output, "publication: {}", publication.id)?;
    writeln!(output, "run: {}", publication.run_id)?;
    writeln!(output, "export: {}", publication.export_name)?;
    writeln!(output, "state: {state}")?;
    writeln!(output, "version: {}", publication.version)?;
    writeln!(output, "repository: {}", publication.target.full_name)?;
    writeln!(
        output,
        "base branch: {}",
        publication.target.base_branch.escape_default()
    )?;
    writeln!(
        output,
        "destination branch: {}",
        publication.target.destination_branch
    )?;
    if let Some(outcome) = publication.outcome {
        writeln!(output, "outcome: {}", enum_text(&outcome)?)?;
    }
    if let Some(branch) = publication.branch.as_deref() {
        writeln!(output, "branch: {}", enum_text(&branch.disposition)?)?;
        let url = redacted_human_url(&branch.url)?;
        writeln!(output, "branch url: {}", url.escape_default())?;
    }
    if let Some(pull_request) = publication.pull_request.as_deref() {
        writeln!(output, "pull request: {}", pull_request.number)?;
        writeln!(
            output,
            "pull request state: {}",
            enum_text(&pull_request.state)?
        )?;
        let url = redacted_human_url(&pull_request.url)?;
        writeln!(output, "pull request url: {}", url.escape_default())?;
    }
    if let Some(failure) = publication.failure.as_deref() {
        writeln!(output, "failure: {}", enum_text(&failure.code)?)?;
        writeln!(output, "failure phase: {}", enum_text(&failure.phase)?)?;
        writeln!(output, "retryable: {}", failure.retryable)?;
    }
    writeln!(output, "created: {}", publication.created_at)?;
    writeln!(output, "updated: {}", publication.updated_at)?;
    if let Some(started_at) = publication.started_at.as_deref() {
        writeln!(output, "started: {started_at}")?;
    }
    if let Some(terminal_at) = publication.terminal_at.as_deref() {
        writeln!(output, "terminal: {terminal_at}")?;
    }
    Ok(())
}

pub(super) fn redacted_human_url(value: &str) -> anyhow::Result<String> {
    let mut url = url::Url::parse(value).context("parse validated Cloud publication URL")?;
    url.set_password(None)
        .map_err(|()| anyhow!("redact Cloud publication URL password"))?;
    url.set_username("")
        .map_err(|()| anyhow!("redact Cloud publication URL username"))?;
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.into())
}

fn enum_text(value: &impl Serialize) -> anyhow::Result<String> {
    match serde_json::to_value(value).context("serialize Cloud publication field")? {
        serde_json::Value::String(value) => Ok(value),
        _ => Err(anyhow!(
            "Cloud publication field is not a contracted string"
        )),
    }
}

fn write_failure(
    context: &CreateOutputContext<'_>,
    failure: &PublicationFailure,
) -> anyhow::Result<ExitCode> {
    let (outcome, category, human, class) = match failure {
        PublicationFailure::Unauthenticated => (
            "unauthenticated",
            None,
            context
                .authentication
                .rejected_error(if context.dispatched {
                    "error: Cloud publication access requires sign-in\n\nSign in first, then retry with the same --idempotency-key:\n  scherzo-cloud auth login"
                } else {
                    "error: Cloud publication access requires sign-in\n\nSign in first:\n  scherzo-cloud auth login"
                })
                .to_owned(),
            OutcomeClass::Unauthenticated,
        ),
        PublicationFailure::Forbidden => (
            "forbidden",
            None,
            "error: Cloud publication operation is not permitted for this account\n\nAsk an organization owner to check your access.".to_owned(),
            OutcomeClass::Forbidden,
        ),
        PublicationFailure::InvalidInput => (
            "invalid_input",
            None,
            format!("error: Cloud publication input rejected by {}\n\nCheck the organization, run, export, and idempotency key, then try again.", context.deployment),
            OutcomeClass::GeneralFailure,
        ),
        PublicationFailure::NotFound => (
            "not_found",
            None,
            "error: Cloud publication parent run not found or unavailable\n\nCheck the organization and run identifier, then try again.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        PublicationFailure::Conflict => (
            "conflict",
            None,
            "error: Cloud publication request conflicts with current state\n\nCheck the run, export, repository binding, and idempotency key before trying again.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        PublicationFailure::Gone => (
            "gone",
            None,
            "error: Cloud publication artifact is no longer available\n\nCreate a new run to produce another publishable export.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        PublicationFailure::Unreachable(category) => (
            "unreachable",
            Some(category.as_str()),
            if context.dispatched {
                format!("error: contact Cloud publication API at {}: {}\n\nRetry with the same --idempotency-key after network access is restored.", context.deployment, category.as_str())
            } else {
                format!("error: contact Cloud publication API at {}: {}\n\nTry again after network access is restored.", context.deployment, category.as_str())
            },
            super::unreachable_outcome_class(*category),
        ),
        PublicationFailure::Interrupted => (
            "interrupted",
            None,
            "error: Cloud publication creation was interrupted\n\nRetry with the same --idempotency-key to resolve the request.".to_owned(),
            OutcomeClass::Interrupted,
        ),
        PublicationFailure::Protocol { .. } => (
            "invalid_response",
            None,
            "error: Cloud publication API response does not match the public contract\n\nRetry with the same --idempotency-key later.".to_owned(),
            OutcomeClass::Protocol,
        ),
    };
    let human = if context.dispatched {
        with_recovery_key(human, context.idempotency_key)
    } else {
        human
    };
    if context.json {
        super::write_pretty_json(&FailureResult {
            schema_version: 1,
            deployment: context.deployment,
            outcome,
            organization_ref: context.organization,
            run_id: context.run_id,
            export_name: context.export_name,
            idempotency_key: context.idempotency_key,
            category,
        })
        .context("write Cloud publication failure")?;
    } else {
        writeln!(io::stderr().lock(), "{human}")?;
    }
    Ok(class.exit_code())
}

// Read failures intentionally omit creation-only retry coordinates. Keeping this projection
// separate prevents a show or list error from implying that a mutation may have committed.
// jscpd:ignore-start
fn write_read_failure(
    context: &ReadOutputContext<'_>,
    failure: &PublicationFailure,
) -> anyhow::Result<ExitCode> {
    let (outcome, category, human, class) = match failure {
        PublicationFailure::Unauthenticated => (
            "unauthenticated",
            None,
            context
                .authentication
                .rejected_error(
                    "error: Cloud publication access requires sign-in\n\nSign in first:\n  scherzo-cloud auth login",
                )
                .to_owned(),
            OutcomeClass::Unauthenticated,
        ),
        PublicationFailure::Forbidden => (
            "forbidden",
            None,
            "error: Cloud publication access is not permitted for this account\n\nAsk an organization owner to check your access."
                .to_owned(),
            OutcomeClass::Forbidden,
        ),
        PublicationFailure::InvalidInput => (
            "invalid_input",
            None,
            format!(
                "error: Cloud publication input rejected by {}\n\nCheck the organization, run, publication, limit, and cursor values, then try again.",
                context.deployment
            ),
            OutcomeClass::GeneralFailure,
        ),
        PublicationFailure::NotFound => (
            "not_found",
            None,
            "error: Cloud publication history not found or unavailable\n\nCheck the organization, run, and publication identifiers, then try again."
                .to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        PublicationFailure::Unreachable(category) => (
            "unreachable",
            Some(category.as_str()),
            format!(
                "error: contact Cloud publication API at {}: {}\n\nTry again after network access is restored.",
                context.deployment,
                category.as_str()
            ),
            super::unreachable_outcome_class(*category),
        ),
        PublicationFailure::Interrupted => (
            "interrupted",
            None,
            "error: Cloud publication read was interrupted\n\nRun the command again.".to_owned(),
            OutcomeClass::Interrupted,
        ),
        PublicationFailure::Conflict
        | PublicationFailure::Gone
        | PublicationFailure::Protocol { .. } => (
            "invalid_response",
            None,
            "error: Cloud publication API response does not match the public contract\n\nTry again later."
                .to_owned(),
            OutcomeClass::Protocol,
        ),
    };
    let human = match context.idempotency_key {
        Some(idempotency_key) => with_recovery_key(human, idempotency_key),
        None => human,
    };
    if context.json {
        super::write_pretty_json(&super::ObservationResult {
            schema_version: 1,
            deployment: context.deployment,
            outcome,
            organization_ref: context.organization,
            run_id: context.run_id,
            publication_id: context.publication_id,
            idempotency_key: context.idempotency_key,
            category,
        })
        .context("write Cloud publication failure")?;
    } else {
        writeln!(io::stderr().lock(), "{human}")?;
    }
    Ok(class.exit_code())
}
// jscpd:ignore-end

fn with_recovery_key(human: String, idempotency_key: &str) -> String {
    if let Some((diagnostic, remedy)) = human.split_once("\n\n") {
        format!("{diagnostic}\nidempotency key: {idempotency_key}\n\n{remedy}")
    } else {
        format!("{human}\nidempotency key: {idempotency_key}")
    }
}

fn write_unknown(
    deployment: &str,
    organization: &str,
    run_id: &str,
    export_name: &str,
    idempotency_key: &str,
    json: bool,
    exit_code: ExitCode,
) -> anyhow::Result<ExitCode> {
    if json {
        super::write_pretty_json(&UnknownResult {
            schema_version: 1,
            deployment,
            outcome: "unknown",
            organization_ref: organization,
            run_id,
            export_name,
            idempotency_key,
            commitment: "unknown",
        })
        .context("write unresolved Cloud publication result")?;
    } else {
        writeln!(
            io::stderr().lock(),
            "error: publication acceptance is unknown\n\nrun: {run_id}\nexport: {export_name}\norganization: {organization}\nidempotency key: {idempotency_key}\ncommitment: unknown\n\nRetry with the same --idempotency-key to resolve the request."
        )?;
    }
    Ok(exit_code)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    idempotency_key: &'a str,
    publication: &'a Publication,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PublicationResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotency_key: Option<&'a str>,
    publication: &'a Publication,
}

// Publication failures keep their run/export/key recovery coordinates explicit; sharing the
// shorter artifact failure envelope would make retry ambiguity invisible.
// jscpd:ignore-start
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FailureResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    run_id: &'a str,
    export_name: &'a str,
    idempotency_key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'a str>,
}
// jscpd:ignore-end

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UnknownResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    run_id: &'a str,
    export_name: &'a str,
    idempotency_key: &'a str,
    commitment: &'static str,
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::time::Duration;

    use super::super::observation_test_support::ControlledObservationClock as ControlledClock;
    use super::*;

    struct ScriptedObservationApi {
        responses: RefCell<VecDeque<Result<Publication, PublicationFailure>>>,
        requests: RefCell<Vec<(String, String, String)>>,
    }

    impl ScriptedObservationApi {
        fn new(
            responses: impl IntoIterator<Item = Result<Publication, PublicationFailure>>,
        ) -> Self {
            Self {
                responses: RefCell::new(responses.into_iter().collect()),
                requests: RefCell::new(Vec::new()),
            }
        }
    }

    impl ScriptedObservationApi {
        fn get_publication(
            &self,
            organization: &str,
            run_id: &str,
            publication_id: &str,
        ) -> Result<Publication, PublicationFailure> {
            self.requests.borrow_mut().push((
                organization.to_owned(),
                run_id.to_owned(),
                publication_id.to_owned(),
            ));
            self.responses
                .borrow_mut()
                .pop_front()
                .expect("the polling scenario should provide another response")
        }
    }

    fn publication(state: PublicationState) -> Publication {
        Publication {
            state,
            ..Publication::default()
        }
    }

    fn observe(
        api: &ScriptedObservationApi,
        timeout: Option<Duration>,
        clock: &ControlledClock,
    ) -> Result<WaitObservation, PublicationFailure> {
        let reference = PublicationObservationReference {
            organization: "acme-research",
            run_id: "run_01k0z6r1w8f4jy2m7q9v3x5abc",
            publication_id: "pub_01k0z6r1w8f4jy2m7q9v3x5abc",
        };
        wait_for_terminal_publication(
            || {
                api.get_publication(
                    reference.organization,
                    reference.run_id,
                    reference.publication_id,
                )
            },
            PublicationFailure::retryable_observation,
            timeout,
            &super::super::BlockingObservationControl::new(),
            clock,
        )
    }

    #[test]
    fn wait_polls_queued_and_running_until_each_terminal_state() {
        for (terminal, expected) in [
            (
                PublicationState::Succeeded,
                TerminalPublicationState::Succeeded,
            ),
            (PublicationState::Failed, TerminalPublicationState::Failed),
        ] {
            let api = ScriptedObservationApi::new([
                Ok(publication(PublicationState::Queued)),
                Ok(publication(PublicationState::Running)),
                Ok(publication(terminal)),
            ]);
            let clock = ControlledClock::new(scherzo_cloud_support::monotonic_now());

            let result = observe(&api, None, &clock)
                .expect("the scripted publication should reach terminal state");

            assert!(matches!(
                result,
                WaitObservation::Terminal { state, .. } if state == expected
            ));
            assert_eq!(
                clock.into_sleeps(),
                vec![super::super::OBSERVATION_POLL_INTERVAL; 2]
            );
            assert_eq!(api.requests.into_inner().len(), 3);
        }
    }

    #[test]
    fn wait_timeout_uses_the_remaining_duration_without_an_extra_request() {
        let api = ScriptedObservationApi::new([
            Ok(publication(PublicationState::Queued)),
            Ok(publication(PublicationState::Running)),
            Ok(publication(PublicationState::Running)),
        ]);
        let clock = ControlledClock::new(scherzo_cloud_support::monotonic_now());

        let result = observe(&api, Some(Duration::from_secs(5)), &clock)
            .expect("timeout should be a local observation result");

        assert!(matches!(result, WaitObservation::TimedOut));
        clock.assert_timeout_schedule();
        assert_eq!(api.requests.into_inner().len(), 3);
    }

    #[test]
    fn wait_bounds_retryable_observation_failures() {
        let failure =
            PublicationFailure::Unreachable(scherzo_cloud_api::UnreachableCategory::Connection);
        let api = ScriptedObservationApi::new([Err(failure), Err(failure)]);
        let clock = ControlledClock::new(scherzo_cloud_support::monotonic_now());

        let result = observe(&api, None, &clock);

        assert_eq!(result.err(), Some(failure));
        clock.assert_single_poll();
        assert_eq!(api.requests.into_inner().len(), 2);
    }
}
