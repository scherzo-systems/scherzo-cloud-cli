use std::io::{self, Write};

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::api::{
    HttpTransportPolicy, Publication, PublicationApi, PublicationFailure, PublicationList,
};
use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::deployment::Deployment;

use super::OrganizationRef;

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
    #[command(about = "Show a Scherzo Cloud publication")]
    Show(ShowCommand),
    #[command(about = "List Scherzo Cloud publications")]
    List(ListCommand),
}

#[derive(Debug, Args)]
struct PublicationRunReference {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: OrganizationRef,

    #[arg(value_name = "RUN", value_parser = parse_run_id, help = "Exact Run ID")]
    run_id: String,
}

#[derive(Debug, Args)]
struct Options {
    #[arg(long, help = "Print the publication result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::HttpOptions,
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
    options: Options,
}

#[derive(Debug, Args)]
struct ShowCommand {
    #[command(flatten)]
    run: PublicationRunReference,

    #[arg(
        value_name = "PUBLICATION",
        value_parser = parse_publication_id,
        help = "Exact Publication ID"
    )]
    publication_id: String,

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

impl CreateCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        let idempotency_key = match self.idempotency_key.clone() {
            Some(key) => key,
            None => crate::idempotency::generate_idempotency_key()
                .context("generate Cloud publication request identity")?,
        };
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

    fn execute_blocking(
        self,
        deployment: &Deployment,
        idempotency_key: &str,
        control: &super::OperationControl<String>,
    ) -> super::CommandResult {
        let result = with_api(deployment, self.options.http.transport_policy(), |api| {
            api.create(
                &self.run.organization,
                &self.run.run_id,
                &self.export,
                idempotency_key,
                || control.begin_dispatch(),
            )
        });
        super::complete_operation(control, || match result {
            Ok(result) => write_create(
                &CreateOutputContext {
                    deployment: deployment.fingerprint().api_url(),
                    organization: &self.run.organization,
                    run_id: &self.run.run_id,
                    export_name: &self.export,
                    idempotency_key,
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
}

impl ShowCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        super::execute_read_only_with_signals("Cloud publication show", move |control| {
            let result = with_api(&deployment, self.options.http.transport_policy(), |api| {
                api.get(
                    &self.run.organization,
                    &self.run.run_id,
                    &self.publication_id,
                )
            })?;
            super::complete_read_only_output(control, || {
                write_show(
                    &ReadOutputContext {
                        deployment: deployment.fingerprint().api_url(),
                        organization: &self.run.organization,
                        run_id: &self.run.run_id,
                        publication_id: Some(&self.publication_id),
                        json: self.options.json,
                    },
                    result,
                )
                .map_err(Into::into)
            })
        })
    }
}

impl ListCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        super::execute_read_only_with_signals("Cloud publication list", move |control| {
            let result = with_api(&deployment, self.options.http.transport_policy(), |api| {
                api.list(
                    &self.run.organization,
                    &self.run.run_id,
                    self.pagination.limit,
                    self.pagination.cursor.as_deref(),
                )
            })?;
            super::complete_read_only_output(control, || {
                write_list(
                    &ReadOutputContext {
                        deployment: deployment.fingerprint().api_url(),
                        organization: &self.run.organization,
                        run_id: &self.run.run_id,
                        publication_id: None,
                        json: self.options.json,
                    },
                    result,
                )
                .map_err(Into::into)
            })
        })
    }
}

fn parse_run_id(value: &str) -> Result<String, String> {
    if crate::public_id::valid_typed_id(value, "run_") {
        Ok(value.to_owned())
    } else {
        Err("must be an exact Run ID (run_ followed by 26 lowercase ULID characters)".to_owned())
    }
}

fn parse_publication_id(value: &str) -> Result<String, String> {
    if crate::public_id::valid_typed_id(value, "pub_") {
        Ok(value.to_owned())
    } else {
        Err(
            "must be an exact Publication ID (pub_ followed by 26 lowercase ULID characters)"
                .to_owned(),
        )
    }
}

fn parse_export_name(value: &str) -> Result<String, String> {
    if crate::workflow_contract::is_identifier(value) {
        Ok(value.to_owned())
    } else {
        Err("must be a lower-camel identifier of at most 64 ASCII characters".to_owned())
    }
}

fn parse_idempotency_key(value: &str) -> Result<String, String> {
    if (1..=255).contains(&value.len()) && value.bytes().all(|byte| (b'!'..=b'~').contains(&byte)) {
        Ok(value.to_owned())
    } else {
        Err("must contain 1-255 visible ASCII characters without whitespace".to_owned())
    }
}

fn with_api<T>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    mut operation: impl FnMut(&PublicationApi) -> Result<T, PublicationFailure>,
) -> anyhow::Result<Result<T, PublicationFailure>> {
    let client = super::human_session_client(transport_policy)?;
    super::execute_required_api_operation(
        &client,
        deployment,
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
        "acquire human session for Cloud publication",
    )
}

struct CreateOutputContext<'a> {
    deployment: &'a str,
    organization: &'a str,
    run_id: &'a str,
    export_name: &'a str,
    idempotency_key: &'a str,
    json: bool,
    dispatched: bool,
}

struct ReadOutputContext<'a> {
    deployment: &'a str,
    organization: &'a str,
    run_id: &'a str,
    publication_id: Option<&'a str>,
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
        Ok(publication) => {
            if context.json {
                super::write_pretty_json(&PublicationResult {
                    schema_version: 1,
                    deployment: context.deployment,
                    outcome: "found",
                    publication: &publication,
                })
                .context("write Cloud publication result")?;
            } else {
                let mut output = io::stdout().lock();
                writeln!(output, "✓ Publication found.\n")?;
                write_publication_human(
                    &mut output,
                    &publication,
                    HumanPublicationLayout::Details,
                )?;
                writeln!(output, "deployment: {}", context.deployment)?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_read_failure(context, &failure),
    }
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

fn redacted_human_url(value: &str) -> anyhow::Result<String> {
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
            if context.dispatched {
                "error: Cloud publication access requires sign-in\n\nSign in first, then retry with the same --idempotency-key:\n  scherzo-cloud auth login".to_owned()
            } else {
                "error: Cloud publication access requires sign-in\n\nSign in first:\n  scherzo-cloud auth login".to_owned()
            },
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
            "error: Cloud publication access requires sign-in\n\nSign in first:\n  scherzo-cloud auth login"
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
    if context.json {
        super::write_pretty_json(&ReadFailureResult {
            schema_version: 1,
            deployment: context.deployment,
            outcome,
            organization_ref: context.organization,
            run_id: context.run_id,
            publication_id: context.publication_id,
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
    publication: &'a Publication,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadFailureResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    run_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    publication_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'a str>,
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
