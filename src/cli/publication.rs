use std::io::{self, Write};

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::api::{HttpTransportPolicy, Publication, PublicationApi, PublicationFailure};
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
}

#[derive(Debug, Args)]
struct CreateCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: OrganizationRef,

    #[arg(value_name = "RUN", value_parser = parse_run_id, help = "Exact Run ID")]
    run_id: String,

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

    #[arg(long, help = "Print the publication result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::HttpOptions,
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
        let signal_organization = self.organization.clone();
        let signal_run_id = self.run_id.clone();
        let signal_export = self.export.clone();
        let signal_json = self.json;
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
        let result = with_api(deployment, self.http.transport_policy(), |api| {
            api.create(
                &self.organization,
                &self.run_id,
                &self.export,
                idempotency_key,
                || control.begin_dispatch(),
            )
        });
        super::complete_operation(control, || match result {
            Ok(result) => write_create(
                &CreateOutputContext {
                    deployment: deployment.fingerprint().api_url(),
                    organization: &self.organization,
                    run_id: &self.run_id,
                    export_name: &self.export,
                    idempotency_key,
                    json: self.json,
                    dispatched: control.dispatched(),
                },
                result,
            )
            .map_err(Into::into),
            Err(_) if control.dispatched() => write_unknown(
                deployment.fingerprint().api_url(),
                &self.organization,
                &self.run_id,
                &self.export,
                idempotency_key,
                self.json,
                ExitCode::GeneralFailure,
            )
            .map_err(Into::into),
            Err(error) => Err(error.into()),
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
                write_publication_human(context.deployment, context.idempotency_key, &publication)?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(context, &failure),
    }
}

fn write_publication_human(
    deployment: &str,
    idempotency_key: &str,
    publication: &Publication,
) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✓ Publication accepted.\n")?;
    writeln!(stdout, "publication: {}", publication.id)?;
    writeln!(stdout, "run: {}", publication.run_id)?;
    writeln!(stdout, "export: {}", publication.export_name)?;
    writeln!(stdout, "state: {}", enum_text(&publication.state)?)?;
    writeln!(stdout, "version: {}", publication.version)?;
    writeln!(stdout, "repository: {}", publication.target.full_name)?;
    writeln!(stdout, "base branch: {}", publication.target.base_branch)?;
    writeln!(
        stdout,
        "destination branch: {}",
        publication.target.destination_branch
    )?;
    writeln!(stdout, "idempotency key: {idempotency_key}")?;
    writeln!(stdout, "deployment: {deployment}")?;
    Ok(())
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
