use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;
use std::io::{self, Write};
use zeroize::Zeroizing;

use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::deployment::Deployment;
use crate::service_auth::{ApiKeyCleanup, ApiKeyDestination, ServiceApiKey, ServiceApiKeyError};
use scherzo_cloud_api::{
    CreateServicePrincipalOutcome, HttpClient, IssueServiceCredentialOutcome,
    IssuedServiceCredential, ListServiceCredentialsOutcome, RevokeServiceCredentialOutcome,
    ServiceCredential, ServiceCredentialPage, ServicePrincipalApiError, create_service_principal,
    issue_service_credential, list_service_credentials, revoke_service_credential,
};

use super::write_api_failure as write_failure;

pub(super) const ABOUT: &str = "Manage Scherzo Cloud service principals";
const NAME: &str = "service-principal";
const ERROR_CONTEXT: &str = "configure Scherzo Cloud service-principal access";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<ServicePrincipalCommand>,
}

#[derive(Debug, Subcommand)]
enum ServicePrincipalCommand {
    #[command(about = "Create a service principal and initial API key")]
    Create(CreateCommand),
    #[command(about = "Manage service API credentials")]
    Credential(CredentialCommand),
}

#[derive(Debug, Args)]
struct CredentialCommand {
    #[command(subcommand)]
    command: Option<CredentialSubcommand>,
}

#[derive(Debug, Subcommand)]
enum CredentialSubcommand {
    #[command(about = "Issue a service credential")]
    Issue(IssueCommand),
    #[command(about = "List active service credentials")]
    List(ListCommand),
    #[command(about = "Revoke a service credential")]
    Revoke(RevokeCommand),
}

type ServiceOptions = super::CommonArgs<super::ServicePrincipalJson, super::NoAuthenticationArgs>;
type RequiredServiceOptions =
    super::CommonArgs<super::ServicePrincipalJson, super::RequiredServiceAuthenticationArgs>;

#[derive(Debug, Args)]
struct CreateCommand {
    #[arg(long, value_name = "NAME", help = "Set the service display name")]
    display_name: String,

    #[arg(
        long,
        value_name = "PATH|-",
        help = "Create the protected API-key file, or write only the API key to standard output"
    )]
    api_key_file: String,

    #[command(flatten)]
    options: ServiceOptions,
}

#[derive(Debug, Args)]
struct ListCommand {
    #[command(flatten)]
    pagination: super::PaginationArgs,

    #[command(flatten)]
    options: RequiredServiceOptions,
}

#[derive(Debug, Args)]
struct IssueCommand {
    #[arg(
        long,
        value_name = "PATH|-",
        help = "Create the protected API-key file, or write only the API key to standard output"
    )]
    api_key_file: String,

    #[command(flatten)]
    options: RequiredServiceOptions,
}

#[derive(Debug, Args)]
struct RevokeCommand {
    #[arg(
        value_name = "CREDENTIAL",
        value_parser = parse_credential_id,
        help = "Exact service credential ID"
    )]
    credential_id: String,

    #[command(flatten)]
    confirmation: super::ConfirmationArgs,

    #[command(flatten)]
    options: RequiredServiceOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(ServicePrincipalCommand::Create(command)) => super::execute_deployment_command(
                Some(command),
                &[NAME],
                ERROR_CONTEXT,
                |command, deployment| command.execute(deployment.clone()),
            ),
            Some(ServicePrincipalCommand::Credential(command)) => command.execute(),
        }
    }
}

impl CredentialCommand {
    fn execute(self) -> super::CommandResult {
        let Some(command) = self.command else {
            return super::print_help(&[NAME, "credential"]);
        };
        match command {
            CredentialSubcommand::List(command) => super::execute_deployment_leaf(
                command,
                &[NAME, "credential"],
                ERROR_CONTEXT,
                ListCommand::execute,
            ),
            CredentialSubcommand::Issue(command) => super::execute_deployment_command(
                Some(command),
                &[NAME, "credential"],
                ERROR_CONTEXT,
                |command, deployment| command.execute(deployment.clone()),
            ),
            CredentialSubcommand::Revoke(command) => super::execute_deployment_leaf(
                command,
                &[NAME, "credential"],
                ERROR_CONTEXT,
                RevokeCommand::execute,
            ),
        }
    }
}

enum CreateAttemptOutcome {
    Completed(CreateServicePrincipalOutcome),
    NotDispatched,
}

impl super::HumanCredentialOutcome for CreateAttemptOutcome {
    type Error = ServicePrincipalApiError;

    fn unauthenticated() -> Self {
        Self::Completed(CreateServicePrincipalOutcome::Unauthenticated)
    }

    fn unreachable(category: scherzo_cloud_api::UnreachableCategory) -> Self {
        Self::Completed(CreateServicePrincipalOutcome::Unreachable(category))
    }

    fn is_unauthenticated(&self) -> bool {
        matches!(
            self,
            Self::Completed(CreateServicePrincipalOutcome::Unauthenticated)
        )
    }

    fn credential_rejected(error: &Self::Error) -> bool {
        error.credential_rejected()
    }
}

impl CreateCommand {
    // Unlike run creation, this wrapper protects commitment through one-time secret delivery.
    // Keep the command-specific signal policy visible rather than sharing a dispatch adapter.
    // jscpd:ignore-start
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        validate_secret_output(&self.api_key_file, self.options.json)?;
        super::execute_bounded_mutation_with_signals("service-principal creation", move |control| {
            self.execute_blocking(&deployment, control)
        })
    }
    // jscpd:ignore-end

    fn execute_blocking(
        self,
        deployment: &Deployment,
        control: &super::OperationControl<()>,
    ) -> super::CommandResult {
        let mut destination = ApiKeyDestination::prepare(&self.api_key_file)
            .context("prepare initial service API-key destination")?;
        let idempotency_key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate service-principal creation request identity")?;
        let attempt = super::execute_with_human_credential(
            deployment,
            self.options.http.transport_policy(),
            "prepare service-principal creation networking",
            "create service principal through",
            |client, api_url, access_token| {
                if !control.begin_bounded_dispatch() {
                    return Ok(CreateAttemptOutcome::NotDispatched);
                }
                create_service_principal(
                    client,
                    api_url,
                    access_token,
                    &idempotency_key,
                    &self.display_name,
                )
                .map(CreateAttemptOutcome::Completed)
            },
        )?;
        let CreateAttemptOutcome::Completed(outcome) = attempt else {
            return Ok(ExitCode::GeneralFailure);
        };
        let delivered = match deliver_created_key(&mut destination, &outcome) {
            Ok(delivered) => delivered,
            Err(ApiKeyDeliveryFailure::Invalid(error)) => {
                return Err(invalid_returned_api_key(error).into());
            }
            Err(ApiKeyDeliveryFailure::Delivery(error)) => {
                let CreateServicePrincipalOutcome::Created(created) = &outcome else {
                    return Err(anyhow!(error).into());
                };
                return finish_failed_delivery(
                    deployment.fingerprint().api_url(),
                    Some(&created.principal),
                    &created.initial_credential,
                    destination,
                    &error,
                    self.options.json,
                )
                .map_err(Into::into);
            }
        };
        let write_outcome = || {
            write_create_outcome(
                deployment.fingerprint().api_url(),
                &outcome,
                delivered.then_some(destination.display_path()),
                destination.writes_stdout(),
                self.options.json,
            )
            .map_err(Into::into)
        };
        if control.dispatched() {
            write_outcome()
        } else {
            super::complete_operation(control, write_outcome)
        }
    }
}

impl ListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let api_key = self.options.authentication.api_key()?;
        let client = service_client(deployment, &self.options)?;
        let outcome = list_service_credentials(
            &client,
            deployment.fingerprint().api_url(),
            api_key.expose(),
            self.pagination.limit,
            self.pagination.cursor.as_deref(),
        )
        .context("list service credentials")?;
        write_list_outcome(
            deployment.fingerprint().api_url(),
            &outcome,
            self.options.json,
        )
    }
}

impl IssueCommand {
    fn execute(self, deployment: Deployment) -> super::CommandResult {
        validate_secret_output(&self.api_key_file, self.options.json)?;
        // Read a potentially blocking stdin credential before installing the bounded mutation
        // owner. No output destination or server mutation exists yet if a signal stops this read.
        let api_key = self.options.authentication.api_key()?;
        super::execute_bounded_mutation_with_signals(
            "service-credential issuance",
            move |control| self.execute_blocking(&deployment, control, &api_key),
        )
    }

    fn execute_blocking(
        self,
        deployment: &Deployment,
        control: &super::OperationControl<()>,
        api_key: &ServiceApiKey,
    ) -> super::CommandResult {
        let mut destination = ApiKeyDestination::prepare(&self.api_key_file)
            .context("prepare issued service API-key destination")?;
        let idempotency_key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate service-credential issuance request identity")?;
        let client = service_client(deployment, &self.options)?;
        if !control.begin_bounded_dispatch() {
            return Ok(ExitCode::GeneralFailure);
        }
        let outcome = issue_service_credential(
            &client,
            deployment.fingerprint().api_url(),
            api_key.expose(),
            &idempotency_key,
        )
        .context("issue service credential")?;
        let delivered = match deliver_issued_key(&mut destination, &outcome) {
            Ok(delivered) => delivered,
            Err(ApiKeyDeliveryFailure::Invalid(error)) => {
                return Err(invalid_returned_api_key(error).into());
            }
            Err(ApiKeyDeliveryFailure::Delivery(error)) => {
                let IssueServiceCredentialOutcome::Issued(issued) = &outcome else {
                    return Err(anyhow!(error).into());
                };
                return finish_failed_delivery(
                    deployment.fingerprint().api_url(),
                    None,
                    issued,
                    destination,
                    &error,
                    self.options.json,
                )
                .map_err(Into::into);
            }
        };
        write_issue_outcome(
            deployment.fingerprint().api_url(),
            &outcome,
            delivered.then_some(destination.display_path()),
            destination.writes_stdout(),
            self.options.json,
        )
        .map_err(Into::into)
    }
}

impl RevokeCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let api_key = self.options.authentication.api_key()?;
        let idempotency_key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate service-credential revocation request identity")?;
        let client = service_client(deployment, &self.options)?;
        let outcome = revoke_service_credential(
            &client,
            deployment.fingerprint().api_url(),
            api_key.expose(),
            &self.credential_id,
            &idempotency_key,
        )
        .context("revoke service credential")?;
        write_revoke_outcome(
            deployment.fingerprint().api_url(),
            &self.credential_id,
            &outcome,
            self.options.json,
        )
    }
}

fn service_client<A: Args>(
    _deployment: &Deployment,
    options: &super::CommonArgs<super::ServicePrincipalJson, A>,
) -> anyhow::Result<HttpClient> {
    HttpClient::new(options.http.transport_policy())
        .map_err(|error| anyhow!(error))
        .context("prepare service API-key networking")
}

fn validate_secret_output(destination: &str, json: bool) -> anyhow::Result<()> {
    if destination == "-" && json {
        return Err(anyhow!("--json cannot be combined with --api-key-file -"));
    }
    Ok(())
}

trait ApiKeyDelivery {
    fn deliver(&mut self, api_key: &ServiceApiKey) -> Result<(), ServiceApiKeyError>;
}

impl ApiKeyDelivery for ApiKeyDestination {
    fn deliver(&mut self, api_key: &ServiceApiKey) -> Result<(), ServiceApiKeyError> {
        self.write(api_key)
    }
}

enum ApiKeyDeliveryFailure {
    Invalid(ServiceApiKeyError),
    Delivery(ServiceApiKeyError),
}

fn invalid_returned_api_key(error: ServiceApiKeyError) -> anyhow::Error {
    anyhow!(error).context("validate returned service API key")
}

fn deliver_api_key(
    destination: &mut impl ApiKeyDelivery,
    api_key: &scherzo_cloud_api::IssuedServiceApiKey,
) -> Result<(), ApiKeyDeliveryFailure> {
    let api_key = ServiceApiKey::parse(Zeroizing::new(api_key.expose().to_owned()))
        .map_err(ApiKeyDeliveryFailure::Invalid)?;
    destination
        .deliver(&api_key)
        .map_err(ApiKeyDeliveryFailure::Delivery)
}

fn deliver_created_key(
    destination: &mut impl ApiKeyDelivery,
    outcome: &CreateServicePrincipalOutcome,
) -> Result<bool, ApiKeyDeliveryFailure> {
    let CreateServicePrincipalOutcome::Created(created) = outcome else {
        return Ok(false);
    };
    let Some(api_key) = &created.initial_credential.api_key else {
        return Ok(false);
    };
    deliver_api_key(destination, api_key)?;
    Ok(true)
}

fn deliver_issued_key(
    destination: &mut impl ApiKeyDelivery,
    outcome: &IssueServiceCredentialOutcome,
) -> Result<bool, ApiKeyDeliveryFailure> {
    let IssueServiceCredentialOutcome::Issued(issued) = outcome else {
        return Ok(false);
    };
    let Some(api_key) = &issued.api_key else {
        return Ok(false);
    };
    deliver_api_key(destination, api_key)?;
    Ok(true)
}

fn parse_credential_id(value: &str) -> Result<String, String> {
    if scherzo_cloud_support::valid_typed_id(value, "crd_") {
        Ok(value.to_owned())
    } else {
        Err("must be an exact service credential ID".to_owned())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    principal: &'a scherzo_cloud_api::ServicePrincipal,
    initial_credential: &'a ServiceCredential,
    api_key_file: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CredentialResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    credential: &'a ServiceCredential,
    api_key_file: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CredentialListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    items: &'a [ServiceCredential],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RevokeResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    credential_id: &'a str,
}

// Service secret failures can carry non-secret principal and credential metadata, so this
// envelope remains separate from the generic API failure envelope.
// jscpd:ignore-start
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FailureResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    principal: Option<&'a scherzo_cloud_api::ServicePrincipal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    credential: Option<&'a ServiceCredential>,
    #[serde(skip_serializing_if = "Option::is_none")]
    api_key_file: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cleanup: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recovery: Option<&'static str>,
}
// jscpd:ignore-end

fn write_create_outcome(
    deployment: &str,
    outcome: &CreateServicePrincipalOutcome,
    destination: Option<&str>,
    secret_stdout: bool,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        CreateServicePrincipalOutcome::Created(created) => {
            let Some(destination) = destination else {
                return write_secret_unavailable(
                    deployment,
                    Some(&created.principal),
                    &created.initial_credential,
                    json,
                );
            };
            if secret_stdout {
                writeln!(
                    io::stderr().lock(),
                    "✓ Service principal {} created; the API key was written to standard output.",
                    created.principal.id
                )?;
            } else if json {
                super::write_pretty_json(&CreateResult {
                    schema_version: 1,
                    deployment,
                    outcome: "created",
                    principal: &created.principal,
                    initial_credential: &created.initial_credential.credential,
                    api_key_file: destination,
                })?;
            } else {
                let mut output = io::stdout().lock();
                writeln!(output, "✓ Service principal created.\n")?;
                writeln!(output, "  Principal:   {}", created.principal.id)?;
                writeln!(
                    output,
                    "  Credential:  {}",
                    created.initial_credential.credential.id
                )?;
                writeln!(output, "  API-key file: {destination}")?;
                writeln!(output, "  Deployment:  {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        CreateServicePrincipalOutcome::InvalidDisplayName => write_failure(
            deployment,
            "invalid_display_name",
            None,
            None,
            "error: service display name rejected\n\nUse a name that normalizes to 1 through 200 Unicode scalar values without control characters.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        CreateServicePrincipalOutcome::Unauthenticated => authentication_failure(deployment, json),
        CreateServicePrincipalOutcome::Forbidden => write_failure(
            deployment,
            "forbidden",
            None,
            None,
            "error: service-principal creation is not permitted\n\nUse an active human account with permission to create a service principal.",
            OutcomeClass::Forbidden,
            json,
        ),
        CreateServicePrincipalOutcome::QuantityLimitReached => write_failure(
            deployment,
            "quantity_limit_reached",
            None,
            None,
            "error: service-principal quantity limit reached\n\nDelete an unused service principal or ask the deployment operator to raise the limit.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        CreateServicePrincipalOutcome::RateLimited { retry_after } => write_failure(
            deployment,
            "rate_limited",
            None,
            Some(*retry_after),
            "error: service-principal creation rate limited\n\nTry again after the reported retry interval.",
            OutcomeClass::RateLimited,
            json,
        ),
        CreateServicePrincipalOutcome::IdempotencyConflict => idempotency_failure(deployment, json),
        CreateServicePrincipalOutcome::RequestTooLarge => write_failure(
            deployment,
            "request_too_large",
            None,
            None,
            "error: service-principal creation request is too large\n\nUse a shorter display name.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        CreateServicePrincipalOutcome::Unreachable(category) => {
            unreachable_failure(deployment, *category, json)
        }
    }
}

fn finish_failed_delivery(
    deployment: &str,
    principal: Option<&scherzo_cloud_api::ServicePrincipal>,
    issued: &IssuedServiceCredential,
    destination: ApiKeyDestination,
    error: &ServiceApiKeyError,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let destination_path = destination.display_path().to_owned();
    let cleanup = destination.cleanup_after_delivery_failure();
    write_delivery_failure(
        DeliveryFailure {
            deployment,
            principal,
            issued,
            destination: &destination_path,
            recovery: if principal.is_some() {
                "operator_recovery_required"
            } else {
                "issue_replacement"
            },
            delivery_error: error,
            cleanup: &cleanup,
        },
        json,
    )
}

struct DeliveryFailure<'a> {
    deployment: &'a str,
    principal: Option<&'a scherzo_cloud_api::ServicePrincipal>,
    issued: &'a IssuedServiceCredential,
    destination: &'a str,
    recovery: &'static str,
    delivery_error: &'a ServiceApiKeyError,
    cleanup: &'a ApiKeyCleanup,
}

fn delivery_failure_result<'a>(failure: &DeliveryFailure<'a>) -> FailureResult<'a> {
    FailureResult {
        schema_version: 1,
        deployment: failure.deployment,
        outcome: "api_key_delivery_failed",
        category: None,
        retry_after: None,
        principal: failure.principal,
        credential: Some(&failure.issued.credential),
        api_key_file: Some(failure.destination),
        cleanup: Some(failure.cleanup.status()),
        recovery: Some(failure.recovery),
    }
}

fn write_delivery_failure(failure: DeliveryFailure<'_>, json: bool) -> anyhow::Result<ExitCode> {
    if json {
        super::write_pretty_json(&delivery_failure_result(&failure))?;
        return Ok(ExitCode::GeneralFailure);
    }

    let mut output = io::stderr().lock();
    let kind = if failure.principal.is_some() {
        "initial"
    } else {
        "issued"
    };
    writeln!(
        output,
        "error: deliver {kind} service API key to {}: {}",
        failure.destination, failure.delivery_error
    )?;
    if let Some(principal) = failure.principal {
        writeln!(output, "principal: {}", principal.id)?;
    }
    writeln!(output, "credential: {}", failure.issued.credential.id)?;
    writeln!(output, "destination: {}", failure.destination)?;
    writeln!(output, "cleanup: {}", failure.cleanup.status())?;
    if let Some(error) = failure.cleanup.error() {
        writeln!(output, "cleanup error: {error}")?;
    }
    writeln!(output)?;

    match (failure.principal.is_some(), failure.cleanup) {
        (true, ApiKeyCleanup::Removed) => writeln!(
            output,
            "The service principal and credential were created, but one-time API-key delivery did not complete. The destination file was removed, and the CLI did not revoke the credential. Ask the deployment operator to recover access or revoke credential {} if it should not remain active.",
            failure.issued.credential.id
        )?,
        (true, ApiKeyCleanup::NotApplicable) => writeln!(
            output,
            "The service principal and credential were created, but one-time API-key delivery did not complete. Cleanup does not apply to standard output, and the API key may have been written there. Protect or remove any captured output. The CLI did not revoke the credential; ask the deployment operator to recover access or revoke credential {} if exposure is possible.",
            failure.issued.credential.id
        )?,
        (true, ApiKeyCleanup::Uncertain(_)) => writeln!(
            output,
            "The service principal and credential were created, but one-time API-key delivery did not complete. The API key may remain at {}. Protect the destination, ask the deployment operator to inspect and remove it, and revoke credential {} if it should not remain active or exposure is possible. The CLI did not revoke the credential.",
            failure.destination, failure.issued.credential.id
        )?,
        (false, ApiKeyCleanup::Removed) => writeln!(
            output,
            "The credential was issued, but one-time API-key delivery did not complete. The destination file was removed. The existing service API key remains active, and the CLI did not revoke the issued credential. Revoke credential {} with a different active key if it should not remain active, then issue a replacement as needed.",
            failure.issued.credential.id
        )?,
        (false, ApiKeyCleanup::NotApplicable) => writeln!(
            output,
            "The credential was issued, but one-time API-key delivery did not complete. Cleanup does not apply to standard output, and the API key may have been written there. Protect or remove any captured output. The existing service API key remains active, and the CLI did not revoke credential {}; revoke it with a different active key if exposure is possible.",
            failure.issued.credential.id
        )?,
        (false, ApiKeyCleanup::Uncertain(_)) => writeln!(
            output,
            "The credential was issued, but one-time API-key delivery did not complete. The API key may remain at {}. Protect the destination, have an operator inspect and remove it, and revoke credential {} with a different active key if it will not be used or exposure is possible. The existing service API key remains active, and the CLI did not revoke the issued credential.",
            failure.destination, failure.issued.credential.id
        )?,
    }
    Ok(ExitCode::GeneralFailure)
}

fn write_secret_unavailable(
    deployment: &str,
    principal: Option<&scherzo_cloud_api::ServicePrincipal>,
    issued: &IssuedServiceCredential,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        super::write_pretty_json(&FailureResult {
            schema_version: 1,
            deployment,
            outcome: "api_key_unavailable",
            category: None,
            retry_after: None,
            principal,
            credential: Some(&issued.credential),
            api_key_file: None,
            cleanup: None,
            recovery: None,
        })?;
    } else if let Some(principal) = principal {
        writeln!(
            io::stderr().lock(),
            "error: completed service credential replay omitted its one-time API key\n\nprincipal: {}\ncredential: {}\n\nThe deployment does not retain this key. Use an existing service credential to issue a replacement, or ask the deployment operator for recovery.",
            principal.id,
            issued.credential.id,
        )?;
    } else {
        writeln!(
            io::stderr().lock(),
            "error: completed service credential replay omitted its one-time API key\n\ncredential: {}\n\nThe deployment does not retain this key. Run the issue command again with a new request identity.",
            issued.credential.id,
        )?;
    }
    Ok(ExitCode::GeneralFailure)
}

fn write_list_outcome(
    deployment: &str,
    outcome: &ListServiceCredentialsOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        ListServiceCredentialsOutcome::Listed(page) => {
            write_credential_page(deployment, page, json)
        }
        ListServiceCredentialsOutcome::InvalidInput => write_failure(
            deployment,
            "invalid_input",
            None,
            None,
            "error: service credential pagination rejected\n\nUse a valid limit and a cursor returned by this operation.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        ListServiceCredentialsOutcome::Unauthenticated => {
            service_credential_rejected(deployment, json)
        }
        ListServiceCredentialsOutcome::Forbidden => service_forbidden(deployment, json),
        ListServiceCredentialsOutcome::Unreachable(category) => {
            unreachable_failure(deployment, *category, json)
        }
    }
}

fn write_credential_page(
    deployment: &str,
    page: &ServiceCredentialPage,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        super::write_pretty_json(&CredentialListResult {
            schema_version: 1,
            deployment,
            outcome: "listed",
            items: &page.items,
            next_cursor: page.next_cursor.as_deref(),
        })?;
    } else {
        let mut output = io::stdout().lock();
        writeln!(output, "✓ Service credentials listed.\n")?;
        for credential in &page.items {
            writeln!(
                output,
                "  Credential: {}  Current: {}  Created: {}",
                credential.id,
                credential.current.unwrap_or(false),
                credential.created_at,
            )?;
        }
        if let Some(cursor) = &page.next_cursor {
            writeln!(output, "\n  Next cursor: {cursor}")?;
        }
        writeln!(output, "  Deployment: {deployment}")?;
    }
    Ok(ExitCode::Success)
}

fn write_issue_outcome(
    deployment: &str,
    outcome: &IssueServiceCredentialOutcome,
    destination: Option<&str>,
    secret_stdout: bool,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        IssueServiceCredentialOutcome::Issued(issued) => {
            let Some(destination) = destination else {
                return write_secret_unavailable(deployment, None, issued, json);
            };
            if secret_stdout {
                writeln!(
                    io::stderr().lock(),
                    "✓ Service credential {} issued; the API key was written to standard output.",
                    issued.credential.id
                )?;
            } else if json {
                super::write_pretty_json(&CredentialResult {
                    schema_version: 1,
                    deployment,
                    outcome: "issued",
                    credential: &issued.credential,
                    api_key_file: destination,
                })?;
            } else {
                writeln!(
                    io::stdout().lock(),
                    "✓ Service credential issued.\n\n  Credential:   {}\n  API-key file: {destination}\n  Deployment:   {deployment}",
                    issued.credential.id,
                )?;
            }
            Ok(ExitCode::Success)
        }
        IssueServiceCredentialOutcome::InvalidInput => write_failure(
            deployment,
            "invalid_input",
            None,
            None,
            "error: service credential issuance request rejected\n\nUse a CLI version supported by this deployment.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        IssueServiceCredentialOutcome::Unauthenticated => {
            service_credential_rejected(deployment, json)
        }
        IssueServiceCredentialOutcome::Forbidden => service_forbidden(deployment, json),
        IssueServiceCredentialOutcome::QuantityLimitReached => write_failure(
            deployment,
            "quantity_limit_reached",
            None,
            None,
            "error: active service credential limit reached\n\nRevoke an unused credential before issuing another.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        IssueServiceCredentialOutcome::IdempotencyConflict => idempotency_failure(deployment, json),
        IssueServiceCredentialOutcome::Unreachable(category) => {
            unreachable_failure(deployment, *category, json)
        }
    }
}

fn write_revoke_outcome(
    deployment: &str,
    credential_id: &str,
    outcome: &RevokeServiceCredentialOutcome,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        RevokeServiceCredentialOutcome::Revoked => {
            if json {
                super::write_pretty_json(&RevokeResult {
                    schema_version: 1,
                    deployment,
                    outcome: "revoked",
                    credential_id,
                })?;
            } else {
                writeln!(
                    io::stdout().lock(),
                    "✓ Service credential revoked.\n\n  Credential: {credential_id}\n  Deployment: {deployment}"
                )?;
            }
            Ok(ExitCode::Success)
        }
        RevokeServiceCredentialOutcome::InvalidInput => write_failure(
            deployment,
            "invalid_input",
            None,
            None,
            "error: service credential revocation request rejected\n\nUse an exact credential ID returned by the list command.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        RevokeServiceCredentialOutcome::Unauthenticated => {
            service_credential_rejected(deployment, json)
        }
        RevokeServiceCredentialOutcome::Forbidden => service_forbidden(deployment, json),
        RevokeServiceCredentialOutcome::NotFound => write_failure(
            deployment,
            "not_found",
            None,
            None,
            "error: service credential not found or unavailable\n\nList active credentials and choose an exact ID.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        RevokeServiceCredentialOutcome::RemovalUnavailable => write_failure(
            deployment,
            "credential_removal_unavailable",
            None,
            None,
            "error: a service credential cannot revoke itself\n\nAuthenticate with a different active service credential.",
            OutcomeClass::GeneralFailure,
            json,
        ),
        RevokeServiceCredentialOutcome::IdempotencyConflict => {
            idempotency_failure(deployment, json)
        }
        RevokeServiceCredentialOutcome::Unreachable(category) => {
            unreachable_failure(deployment, *category, json)
        }
    }
}

fn authentication_failure(deployment: &str, json: bool) -> anyhow::Result<ExitCode> {
    write_failure(
        deployment,
        "unauthenticated",
        None,
        None,
        "error: service-principal creation requires sign-in\n\nSign in first:\n  scherzo-cloud auth login",
        OutcomeClass::Unauthenticated,
        json,
    )
}

fn service_credential_rejected(deployment: &str, json: bool) -> anyhow::Result<ExitCode> {
    write_failure(
        deployment,
        "unauthenticated",
        None,
        None,
        "error: service API key rejected\n\nUse a different active service API key.",
        OutcomeClass::Unauthenticated,
        json,
    )
}

fn service_forbidden(deployment: &str, json: bool) -> anyhow::Result<ExitCode> {
    write_failure(
        deployment,
        "forbidden",
        None,
        None,
        "error: service credential operation is not permitted\n\nUse an active platform API key belonging to this service principal.",
        OutcomeClass::Forbidden,
        json,
    )
}

fn idempotency_failure(deployment: &str, json: bool) -> anyhow::Result<ExitCode> {
    write_failure(
        deployment,
        "idempotency_conflict",
        None,
        None,
        "error: service request identity conflicted with another request\n\nRun the command again to use a new request identity.",
        OutcomeClass::GeneralFailure,
        json,
    )
}

fn unreachable_failure(
    deployment: &str,
    category: scherzo_cloud_api::UnreachableCategory,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_failure(
        deployment,
        "unreachable",
        Some(category.as_str()),
        None,
        &format!(
            "error: contact service-principal API at {deployment}: {}\n\nCheck network access to the deployment and try again.",
            category.as_str()
        ),
        super::unreachable_outcome_class(category),
        json,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const CREDENTIAL_ID: &str = "crd_01k0z6r1w8f4jy2m7q9v3x5abc";
    const API_KEY: &str =
        "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    struct FaultingDelivery;

    impl ApiKeyDelivery for FaultingDelivery {
        fn deliver(&mut self, _api_key: &ServiceApiKey) -> Result<(), ServiceApiKeyError> {
            Err(ServiceApiKeyError::Io {
                source_name: crate::service_auth::SecretSource::File("next.key".into()),
                operation: "sync",
                source: io::Error::other("injected delivery fault"),
            })
        }
    }

    #[test]
    fn uncertain_delivery_cleanup_retains_metadata_without_the_secret() {
        let outcome = IssueServiceCredentialOutcome::Issued(IssuedServiceCredential {
            credential: ServiceCredential {
                id: CREDENTIAL_ID.to_owned(),
                created_at: "2026-01-02T03:04:05Z".to_owned(),
                current: None,
            },
            api_key: Some(scherzo_cloud_api::IssuedServiceApiKey::new(Zeroizing::new(
                API_KEY.to_owned(),
            ))),
        });
        let ApiKeyDeliveryFailure::Delivery(error) =
            deliver_issued_key(&mut FaultingDelivery, &outcome)
                .expect_err("injected delivery should fail")
        else {
            panic!("fixture should fail during delivery");
        };
        let IssueServiceCredentialOutcome::Issued(issued) = &outcome else {
            panic!("fixture outcome should be issued");
        };
        let principal = scherzo_cloud_api::ServicePrincipal {
            id: "prn_service".to_owned(),
            r#type: "service",
            state: "active",
            display_name: "Build agent".to_owned(),
        };
        let cleanup = ApiKeyCleanup::Uncertain(ServiceApiKeyError::Io {
            source_name: crate::service_auth::SecretSource::File("next.key".into()),
            operation: "remove incomplete",
            source: io::Error::other("injected cleanup fault"),
        });
        let failure = DeliveryFailure {
            deployment: "https://api.example/",
            principal: Some(&principal),
            issued,
            destination: "next.key",
            recovery: "operator_recovery_required",
            delivery_error: &error,
            cleanup: &cleanup,
        };

        let document = serde_json::to_value(delivery_failure_result(&failure)).unwrap();

        assert!(matches!(&error, ServiceApiKeyError::Io { .. }));
        assert_eq!(document["outcome"], "api_key_delivery_failed");
        assert_eq!(document["principal"]["id"], "prn_service");
        assert_eq!(document["credential"]["id"], CREDENTIAL_ID);
        assert_eq!(document["apiKeyFile"], "next.key");
        assert_eq!(document["cleanup"], "uncertain");
        assert_eq!(document["recovery"], "operator_recovery_required");
        let receipt = document.to_string();
        assert!(!receipt.contains(API_KEY));
        assert!(!receipt.contains("injected cleanup fault"));
    }
}
