use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand, builder::NonEmptyStringValueParser};
use serde::Serialize;

use crate::api::{HttpClient, HttpTransportPolicy, Run, RunApi, RunFailure};
use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::deployment::Deployment;
use crate::human_auth::session::{self, RequiredOperation};

pub(super) const ABOUT: &str = "Work with Scherzo Cloud runs";
const NAME: &str = "run";

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
struct ShowCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(value_name = "RUN", help = "Exact Run ID")]
    run_id: String,

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
            api.get(&self.organization, &self.run_id)
        })?;
        if cancelled.load(Ordering::Acquire) {
            return Ok(ExitCode::GeneralFailure);
        }
        completed.store(true, Ordering::Release);
        write_show(
            deployment.fingerprint().api_url(),
            &self.organization,
            &self.run_id,
            result,
            self.options.json,
        )
        .map_err(Into::into)
    }
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
                write_run_human(deployment, &run)?;
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

fn write_run_human(deployment: &str, run: &Run) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✓ Run found.\n")?;
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
