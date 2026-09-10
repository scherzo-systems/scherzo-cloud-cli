use std::io::{self, Write};

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;
use time::OffsetDateTime;

use crate::api::{
    CancelDeletionOutcome, CommonLifecycleFailure, DeletionSchedule, HttpClient,
    HttpTransportPolicy, LifecycleApiError, LifecycleTransition, RequestDeletionOutcome,
    UnreachableCategory, cancel_current_principal_deletion, cancel_organization_deletion,
    request_current_principal_deletion, request_organization_deletion,
};
use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::cancellation::Cancellation;
use crate::human_auth::deployment::Deployment;
use crate::human_auth::device_authorization::{AuthorizationError, DeviceAuthorization};
use crate::human_auth::device_flow::{self, DeviceFlowError, DeviceFlowOutcome, DeviceFlowPhase};
use crate::human_auth::session::{
    self, LocalCredentialState, RequiredOperationWithBinding, SessionBinding,
};

const ACCOUNT_ABOUT: &str = "Manage your account deletion schedule";
const ORGANIZATION_ABOUT: &str = "Manage an organization deletion schedule";
const ACCOUNT_REQUEST_AFTER_HELP: &str = "Account access:\n  A confirmed request removes this deployment's local human credential.\n  Cancellation requires a fresh browser proof from the same linked identity before the deadline.";
const ORGANIZATION_REQUEST_AFTER_HELP: &str = "Authorization:\n  Only a current active human owner can request organization deletion.\n\nAccount access:\n  A confirmed organization request leaves the local human credential unchanged.";
const ACCOUNT_CANCEL_AFTER_HELP: &str = "Proof:\n  This command starts a fresh browser sign-in for the same linked identity.\n  The proof is sent only as the cancellation bearer and is not stored as a local session.";
const ORGANIZATION_CANCEL_AFTER_HELP: &str = "Authorization:\n  Cancellation requires a current active human owner using the same linked identity that requested deletion.\n\nProof:\n  This command starts a fresh browser sign-in. The proof is not stored as a local session.";

#[derive(Debug, Args)]
pub(super) struct AccountCommand {
    #[command(subcommand)]
    command: Option<AccountDeletionCommand>,
}

#[derive(Debug, Subcommand)]
enum AccountDeletionCommand {
    #[command(about = "Request account deletion", after_help = ACCOUNT_REQUEST_AFTER_HELP)]
    Request(AccountRequestCommand),
    #[command(about = "Cancel account deletion", after_help = ACCOUNT_CANCEL_AFTER_HELP)]
    Cancel(CancelCommand),
}

#[derive(Debug, Args)]
pub(super) struct OrganizationCommand {
    #[command(subcommand)]
    command: Option<OrganizationDeletionCommand>,
}

#[derive(Debug, Subcommand)]
enum OrganizationDeletionCommand {
    #[command(
        about = "Request organization deletion",
        after_help = ORGANIZATION_REQUEST_AFTER_HELP
    )]
    Request(OrganizationRequestCommand),
    #[command(
        about = "Cancel organization deletion",
        after_help = ORGANIZATION_CANCEL_AFTER_HELP
    )]
    Cancel(OrganizationCancelCommand),
}

#[derive(Debug, Args)]
struct AccountRequestCommand {
    #[command(flatten)]
    request: RequestOptions,
}

#[derive(Debug, Args)]
struct OrganizationRequestCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization_ref: super::OrganizationRef,

    #[command(flatten)]
    request: RequestOptions,
}

#[derive(Debug, Args)]
struct CancelCommand {
    #[command(flatten)]
    options: CancellationOptions,
}

#[derive(Debug, Args)]
struct OrganizationCancelCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization_ref: super::OrganizationRef,

    #[command(flatten)]
    options: CancellationOptions,
}

#[derive(Debug, Args)]
struct RequestOptions {
    #[arg(
        long,
        required = true,
        action = clap::ArgAction::SetTrue,
        help = "Confirm the 30-day deletion schedule"
    )]
    yes: bool,

    #[arg(long, help = "Print the deletion result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::HttpOptions,
}

#[derive(Debug, Args)]
struct CancellationOptions {
    #[arg(long, help = "Emit newline-delimited JSON events")]
    json: bool,

    #[command(flatten)]
    http: super::HttpOptions,
}

// Account deletion keeps its own nested command path and credential cleanup while organization
// deletion retains target-bound authorization, so the two family dispatchers stay explicit.
// jscpd:ignore-start
impl AccountCommand {
    pub(super) fn execute(self) -> super::CommandResult {
        super::execute_deployment_command(
            self.command,
            &["account", "deletion"],
            "configure Scherzo Cloud account deletion",
            |command, deployment| match command {
                AccountDeletionCommand::Request(command) => command
                    .execute(deployment)
                    .map_err(super::CommandFailure::from),
                AccountDeletionCommand::Cancel(command) => {
                    command.execute(deployment, DeletionTarget::Account)
                }
            },
        )
    }
}
// jscpd:ignore-end

impl OrganizationCommand {
    pub(super) fn execute(self) -> super::CommandResult {
        super::execute_deployment_command(
            self.command,
            &["organization", "deletion"],
            "configure Scherzo Cloud organization deletion",
            |command, deployment| match command {
                OrganizationDeletionCommand::Request(command) => command
                    .execute(deployment)
                    .map_err(super::CommandFailure::from),
                OrganizationDeletionCommand::Cancel(command) => command.options.execute(
                    deployment,
                    DeletionTarget::Organization(command.organization_ref.0),
                ),
            },
        )
    }
}

impl AccountRequestCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let idempotency_key = crate::idempotency::generate_idempotency_key()
            .context("generate account deletion request identity")?;
        let client = HttpClient::new(self.request.http.transport_policy())
            .map_err(|error| anyhow!(error))
            .context("prepare account deletion networking")?;
        let (outcome, binding) = request_deletion_with_session(
            &client,
            deployment,
            format!(
                "request account deletion through {}",
                deployment.fingerprint().api_url()
            ),
            |client, api_url, access_token| {
                request_current_principal_deletion(client, api_url, access_token, &idempotency_key)
            },
        )?;
        let (credential, cleanup_error) = if matches!(outcome, RequestDeletionOutcome::Scheduled(_))
        {
            match binding {
                Some(binding) => match session::remove_bound_credential(deployment, &binding) {
                    Ok(LocalCredentialState::Removed) => {
                        (Some(LocalCredentialDisposition::Removed), None)
                    }
                    Ok(LocalCredentialState::Retained) => {
                        (Some(LocalCredentialDisposition::Changed), None)
                    }
                    Err(error) => (
                        Some(LocalCredentialDisposition::RemovalUnconfirmed),
                        Some(anyhow!(error).context(
                            "remove the local credential after scheduling account deletion",
                        )),
                    ),
                },
                None => (
                    Some(LocalCredentialDisposition::RemovalUnconfirmed),
                    Some(anyhow!(
                        "the requesting human session binding is unavailable"
                    )),
                ),
            }
        } else {
            (None, None)
        };
        let exit_code = write_request_outcome(
            &DeletionTarget::Account,
            deployment.fingerprint().api_url(),
            &outcome,
            credential,
            self.request.json,
        )?;
        if let Some(error) = cleanup_error {
            let mut stderr = io::stderr().lock();
            writeln!(stderr, "error: {error:#}")?;
            writeln!(
                stderr,
                "\nRemove the deployment's local credential before continuing:\n  scherzo-cloud auth logout"
            )?;
            return Ok(ExitCode::GeneralFailure);
        }
        Ok(exit_code)
    }
}

impl OrganizationRequestCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let organization_ref = self.organization_ref.0;
        let idempotency_key = crate::idempotency::generate_idempotency_key()
            .context("generate organization deletion request identity")?;
        let client = HttpClient::new(self.request.http.transport_policy())
            .map_err(|error| anyhow!(error))
            .context("prepare organization deletion networking")?;
        let (outcome, _binding) = request_deletion_with_session(
            &client,
            deployment,
            format!(
                "request organization deletion through {}",
                deployment.fingerprint().api_url()
            ),
            |client, api_url, access_token| {
                request_organization_deletion(
                    client,
                    api_url,
                    access_token,
                    &organization_ref,
                    &idempotency_key,
                )
            },
        )?;
        write_request_outcome(
            &DeletionTarget::Organization(organization_ref),
            deployment.fingerprint().api_url(),
            &outcome,
            matches!(outcome, RequestDeletionOutcome::Scheduled(_))
                .then_some(LocalCredentialDisposition::Unchanged),
            self.request.json,
        )
    }
}

impl CancelCommand {
    fn execute(self, deployment: &Deployment, target: DeletionTarget) -> super::CommandResult {
        self.options.execute(deployment, target)
    }
}

impl CancellationOptions {
    fn execute(self, deployment: &Deployment, target: DeletionTarget) -> super::CommandResult {
        let deployment = deployment.clone();
        let context = target.signal_context();
        super::execute_cancellable_with_signals(context, move |cancellation| {
            execute_cancellation(
                &deployment,
                target,
                self.http.transport_policy(),
                self.json,
                cancellation,
            )
        })
    }
}

fn request_deletion_with_session(
    client: &HttpClient,
    deployment: &Deployment,
    api_context: String,
    mut operation: impl FnMut(
        &HttpClient,
        &str,
        &str,
    ) -> Result<RequestDeletionOutcome, LifecycleApiError>,
) -> anyhow::Result<(RequestDeletionOutcome, Option<SessionBinding>)> {
    match session::execute_required_with_binding(
        client,
        deployment,
        |access_token| {
            operation(
                client,
                deployment.fingerprint().api_url(),
                access_token.expose(),
            )
        },
        lifecycle_credential_rejected,
    ) {
        Ok(RequiredOperationWithBinding::Completed { result, binding }) => result
            .or_else(|error| {
                if error.invalid_response() {
                    Ok(RequestDeletionOutcome::Common(
                        CommonLifecycleFailure::InvalidResponse,
                    ))
                } else {
                    Err(error)
                }
            })
            .map(|outcome| (outcome, Some(binding)))
            .map_err(|error| anyhow!(error))
            .context(api_context),
        Ok(RequiredOperationWithBinding::Unauthenticated) => Ok((
            RequestDeletionOutcome::Common(CommonLifecycleFailure::Unauthenticated),
            None,
        )),
        Err(error) => match error.unreachable_category() {
            Some(category) => Ok((
                RequestDeletionOutcome::Common(CommonLifecycleFailure::Unreachable(category)),
                None,
            )),
            None => Err(anyhow!(error).context("acquire human session")),
        },
    }
}

fn lifecycle_credential_rejected(
    result: &Result<RequestDeletionOutcome, LifecycleApiError>,
) -> bool {
    result.as_ref().is_ok_and(|outcome| {
        matches!(
            outcome,
            RequestDeletionOutcome::Common(CommonLifecycleFailure::Unauthenticated)
        )
    }) || result
        .as_ref()
        .is_err_and(LifecycleApiError::credential_rejected)
}

#[derive(Clone, Debug)]
enum DeletionTarget {
    Account,
    Organization(String),
}

impl DeletionTarget {
    const fn noun(&self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Organization(_) => "organization",
        }
    }

    const fn signal_context(&self) -> &'static str {
        match self {
            Self::Account => "account deletion cancellation",
            Self::Organization(_) => "organization deletion cancellation",
        }
    }

    const fn activation_operation(&self) -> &'static str {
        match self {
            Self::Account => "account_deletion_cancellation",
            Self::Organization(_) => "organization_deletion_cancellation",
        }
    }

    fn organization_ref(&self) -> Option<&str> {
        match self {
            Self::Account => None,
            Self::Organization(organization_ref) => Some(organization_ref),
        }
    }

    fn cancellation_command(&self) -> String {
        match self {
            Self::Account => "scherzo-cloud account deletion cancel".to_owned(),
            Self::Organization(organization_ref) => {
                format!("scherzo-cloud organization deletion cancel {organization_ref}")
            }
        }
    }

    fn status_command(&self) -> String {
        match self {
            Self::Account => "scherzo-cloud auth status".to_owned(),
            Self::Organization(organization_ref) => {
                format!("scherzo-cloud organization show {organization_ref}")
            }
        }
    }
}

fn execute_cancellation(
    deployment: &Deployment,
    target: DeletionTarget,
    transport_policy: HttpTransportPolicy,
    json: bool,
    cancellation: &Cancellation,
) -> super::CommandResult {
    let client = HttpClient::new(transport_policy)
        .map_err(|error| anyhow!(error))
        .context("prepare deletion cancellation networking")?;
    let mut output = CancellationOutput::new(json, &target);
    let proof = device_flow::identity_proof(
        &client,
        deployment,
        cancellation,
        |authorization, expires_at| output.activation(deployment, authorization, expires_at),
    );
    let proof = match proof {
        Ok(DeviceFlowOutcome::Issued(proof)) => proof,
        Ok(DeviceFlowOutcome::Denied) => {
            return output.browser_failure(
                deployment.fingerprint().api_url(),
                "denied",
                "token_polling",
                None,
                "error: deletion cancellation was not authorized\n\nStart the command again and authorize with the required linked identity.",
                OutcomeClass::GeneralFailure,
            );
        }
        Ok(DeviceFlowOutcome::Expired) => {
            return output.browser_failure(
                deployment.fingerprint().api_url(),
                "expired",
                "token_polling",
                None,
                "error: deletion cancellation authorization expired\n\nStart the command again.",
                OutcomeClass::GeneralFailure,
            );
        }
        Ok(DeviceFlowOutcome::Cancelled) => {
            return output.cancelled(deployment.fingerprint().api_url());
        }
        Err(error) => {
            return handle_device_flow_error(&mut output, deployment, cancellation, error);
        }
    };

    let idempotency_key = crate::idempotency::generate_idempotency_key()
        .context("generate deletion cancellation request identity")?;
    let result = match &target {
        DeletionTarget::Account => cancel_current_principal_deletion(
            &client,
            deployment.fingerprint().api_url(),
            proof.expose(),
            &idempotency_key,
        ),
        DeletionTarget::Organization(organization_ref) => cancel_organization_deletion(
            &client,
            deployment.fingerprint().api_url(),
            proof.expose(),
            organization_ref,
            &idempotency_key,
        ),
    };
    let outcome = match result {
        Ok(outcome) => outcome,
        Err(error) if error.invalid_response() => {
            CancelDeletionOutcome::Common(CommonLifecycleFailure::InvalidResponse)
        }
        Err(error) => {
            return Err(anyhow!(error)
                .context(format!(
                    "cancel {} deletion through {}",
                    target.noun(),
                    deployment.fingerprint().api_url()
                ))
                .into());
        }
    };
    output.api_outcome(deployment.fingerprint().api_url(), &outcome)
}

// Cancellation and identity linking deliberately retain distinct terminal event and diagnostic
// contracts even though both consume the shared browser-proof state machine.
// jscpd:ignore-start
fn handle_device_flow_error(
    output: &mut CancellationOutput<'_>,
    deployment: &Deployment,
    cancellation: &Cancellation,
    error: DeviceFlowError,
) -> super::CommandResult {
    if cancellation.is_cancelled() {
        return output.cancelled(deployment.fingerprint().api_url());
    }
    match error {
        DeviceFlowError::Authorization { phase, error } => {
            handle_authorization_error(output, deployment, phase, error)
        }
        DeviceFlowError::ExpirationOutOfRange => handle_protocol_error(
            output,
            deployment,
            DeviceFlowPhase::DeviceAuthorization,
            anyhow!("the device-authorization expiration is out of range"),
        ),
        DeviceFlowError::ActivationOutput(error) => Err(error.into()),
    }
}
// jscpd:ignore-end

// Browser-proof authorization failures are projected into deletion-specific outcomes and remedies.
// jscpd:ignore-start
fn handle_authorization_error(
    output: &mut CancellationOutput<'_>,
    deployment: &Deployment,
    phase: DeviceFlowPhase,
    error: AuthorizationError,
) -> super::CommandResult {
    match error {
        AuthorizationError::Local(error) => Err(anyhow!(error)
            .context(cancellation_flow_context(deployment, phase))
            .into()),
        AuthorizationError::Unreachable(category) => output.browser_failure(
            deployment.fingerprint().api_url(),
            "unreachable",
            phase_name(phase),
            Some(category),
            "error: authorization server unavailable during deletion cancellation\n\nStart the command again when the authorization server is available.",
            super::unreachable_outcome_class(category),
        ),
        error @ AuthorizationError::Protocol { .. } => {
            handle_protocol_error(output, deployment, phase, anyhow!(error))
        }
    }
}
// jscpd:ignore-end

fn handle_protocol_error(
    output: &mut CancellationOutput<'_>,
    deployment: &Deployment,
    phase: DeviceFlowPhase,
    error: anyhow::Error,
) -> super::CommandResult {
    if output.json {
        output.browser_failure(
            deployment.fingerprint().api_url(),
            "protocol_error",
            phase_name(phase),
            None,
            "error: deletion cancellation authorization response invalid\n\nUse a CLI version supported by this deployment.",
            OutcomeClass::Protocol,
        )
    } else {
        Err(super::CommandFailure::for_outcome(
            error.context(cancellation_flow_context(deployment, phase)),
            OutcomeClass::Protocol,
        ))
    }
}

// Phase names are shared machine vocabulary, while deletion owns separate operation context.
// jscpd:ignore-start
const fn phase_name(phase: DeviceFlowPhase) -> &'static str {
    match phase {
        DeviceFlowPhase::DeviceAuthorization => "device_authorization",
        DeviceFlowPhase::TokenPolling => "token_polling",
    }
}

fn cancellation_flow_context(deployment: &Deployment, phase: DeviceFlowPhase) -> String {
    let action = match phase {
        DeviceFlowPhase::DeviceAuthorization => "request deletion cancellation authorization from",
        DeviceFlowPhase::TokenPolling => "request deletion cancellation proof from",
    };
    format!(
        "{action} OAuth issuer {}",
        deployment.fingerprint().issuer()
    )
}
// jscpd:ignore-end

#[derive(Clone, Copy)]
enum LocalCredentialDisposition {
    Removed,
    Changed,
    Unchanged,
    RemovalUnconfirmed,
}

impl LocalCredentialDisposition {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Removed => "removed",
            Self::Changed => "changed",
            Self::Unchanged => "unchanged",
            Self::RemovalUnconfirmed => "removal_unconfirmed",
        }
    }
}

fn write_request_outcome(
    target: &DeletionTarget,
    deployment: &str,
    outcome: &RequestDeletionOutcome,
    credential: Option<LocalCredentialDisposition>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        RequestDeletionOutcome::Scheduled(schedule) => {
            if json {
                write_json(&DeletionResult {
                    schema_version: 1,
                    deployment,
                    outcome: "scheduled",
                    organization_ref: target.organization_ref(),
                    schedule: Some(schedule),
                    category: None,
                    local_credential: credential.map(LocalCredentialDisposition::as_str),
                })?;
            } else {
                write_schedule_human(target, deployment, schedule, credential)?;
            }
            Ok(ExitCode::Success)
        }
        RequestDeletionOutcome::Common(common) => {
            let failure = common_lifecycle_failure(
                target,
                deployment,
                common,
                FailureOperation::Request,
            );
            write_request_failure(target, deployment, failure, json)
        }
        RequestDeletionOutcome::NotFound => write_request_failure(
            target,
            deployment,
            FailurePresentation::new(
                "not_found",
                format!(
                    "error: {} not found or unavailable\n\nCheck the organization reference and your access, then try again.",
                    target.noun()
                ),
                OutcomeClass::GeneralFailure,
            ),
            json,
        ),
        RequestDeletionOutcome::HumanOwnerRequired => write_request_failure(
            target,
            deployment,
            FailurePresentation::new(
                "human_owner_required",
                "error: account deletion would leave an organization without an active human owner\n\nMake another active human member an owner in every affected organization, then try again.".to_owned(),
                OutcomeClass::GeneralFailure,
            ),
            json,
        ),
        RequestDeletionOutcome::TransitionUnavailable => write_request_failure(
            target,
            deployment,
            FailurePresentation::new(
                "transition_unavailable",
                format!(
                    "error: {} deletion request unavailable in the current state\n\nDeletion may already be scheduled. Cancel the pending deletion only if you want to keep the {}:\n  {}",
                    target.noun(),
                    target.noun(),
                    target.cancellation_command()
                ),
                OutcomeClass::GeneralFailure,
            ),
            json,
        ),
        RequestDeletionOutcome::IdempotencyConflict => write_request_failure(
            target,
            deployment,
            FailurePresentation::new(
                "idempotency_conflict",
                format!(
                    "error: {} deletion request identity conflicted with another request\n\nRun the command again to create a new request identity.",
                    target.noun()
                ),
                OutcomeClass::GeneralFailure,
            ),
            json,
        ),
    }
}

fn write_request_failure(
    target: &DeletionTarget,
    deployment: &str,
    failure: FailurePresentation,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&DeletionResult {
            schema_version: 1,
            deployment,
            outcome: failure.outcome,
            organization_ref: target.organization_ref(),
            schedule: None,
            category: failure.category,
            local_credential: None,
        })?;
    } else {
        writeln!(io::stderr().lock(), "{}", failure.human)?;
    }
    Ok(failure.class.exit_code())
}

fn write_schedule_human(
    target: &DeletionTarget,
    deployment: &str,
    schedule: &DeletionSchedule,
    credential: Option<LocalCredentialDisposition>,
) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(
        stdout,
        "✓ {} deletion scheduled.\n",
        title_case(target.noun())
    )?;
    write_schedule_fields(&mut stdout, schedule)?;
    if let Some(credential) = credential {
        writeln!(stdout, "local credential: {}", credential.as_str())?;
    }
    writeln!(stdout, "deployment: {deployment}")?;
    writeln!(
        stdout,
        "\nCancel before the deadline with fresh browser proof:\n  {}",
        target.cancellation_command()
    )?;
    Ok(())
}

fn write_schedule_fields(output: &mut impl Write, schedule: &DeletionSchedule) -> io::Result<()> {
    writeln!(output, "{}: {}", resource_key(schedule.kind), schedule.id)?;
    writeln!(output, "state: deletion_pending")?;
    writeln!(output, "requested: {}", schedule.requested_at)?;
    writeln!(output, "deadline: {}", schedule.deadline)?;
    writeln!(output, "updated: {}", schedule.updated_at)
}

const fn resource_key(kind: crate::api::LifecycleResourceKind) -> &'static str {
    match kind {
        crate::api::LifecycleResourceKind::Principal => "principal",
        crate::api::LifecycleResourceKind::Organization => "organization",
    }
}

const fn lifecycle_state(state: crate::api::LifecycleState) -> &'static str {
    match state {
        crate::api::LifecycleState::Active => "active",
        crate::api::LifecycleState::Suspended => "suspended",
        crate::api::LifecycleState::DeletionPending => "deletion_pending",
    }
}

fn title_case(noun: &str) -> &str {
    match noun {
        "account" => "Account",
        "organization" => "Organization",
        _ => noun,
    }
}

struct CancellationOutput<'a> {
    json: bool,
    target: &'a DeletionTarget,
}

struct CancellationTerminal<'a> {
    outcome: &'static str,
    phase: Option<&'static str>,
    category: Option<&'static str>,
    transition: Option<&'a LifecycleTransition>,
    human: String,
    class: OutcomeClass,
}

impl<'a> CancellationOutput<'a> {
    const fn new(json: bool, target: &'a DeletionTarget) -> Self {
        Self { json, target }
    }

    fn activation(
        &mut self,
        deployment: &Deployment,
        authorization: &DeviceAuthorization,
        expires_at: OffsetDateTime,
    ) -> anyhow::Result<()> {
        if self.json {
            let event = device_flow::activation_event(
                deployment,
                authorization,
                expires_at,
                Some(self.target.activation_operation()),
            )
            .context("format deletion cancellation activation expiration")?;
            write_json_line(&event)
        } else {
            let mut stdout = io::stdout().lock();
            writeln!(stdout, "Cancel {} deletion\n", self.target.noun())?;
            writeln!(stdout, "open: {}", authorization.activation_uri())?;
            writeln!(stdout, "code: {}", authorization.user_code())?;
            writeln!(
                stdout,
                "\nSign in with the same linked identity used to request deletion."
            )?;
            stdout
                .flush()
                .context("write deletion cancellation activation")?;
            let mut stderr = io::stderr().lock();
            writeln!(stderr, "Waiting for authorization...")?;
            stderr
                .flush()
                .context("write deletion cancellation progress")
        }
    }

    fn cancelled(&mut self, deployment: &str) -> super::CommandResult {
        self.write_terminal(
            deployment,
            CancellationTerminal {
                outcome: "cancelled",
                phase: None,
                category: None,
                transition: None,
                human: "error: deletion cancellation interrupted\n\nStart the command again."
                    .to_owned(),
                class: OutcomeClass::Interrupted,
            },
        )
        .map_err(Into::into)
    }

    // Browser failures remain deletion result events rather than adopting identity-link fields.
    // jscpd:ignore-start
    fn browser_failure(
        &mut self,
        deployment: &str,
        outcome: &'static str,
        phase: &'static str,
        category: Option<UnreachableCategory>,
        human: &str,
        class: OutcomeClass,
    ) -> super::CommandResult {
        self.write_terminal(
            deployment,
            CancellationTerminal {
                outcome,
                phase: Some(phase),
                category: category.map(UnreachableCategory::as_str),
                transition: None,
                human: human.to_owned(),
                class,
            },
        )
        .map_err(Into::into)
    }
    // jscpd:ignore-end

    fn api_outcome(
        &mut self,
        deployment: &str,
        outcome: &CancelDeletionOutcome,
    ) -> super::CommandResult {
        let (name, transition, category, human, class) = match outcome {
            CancelDeletionOutcome::Cancelled(transition) => (
                "cancelled",
                Some(transition),
                None,
                format!("✓ {} deletion cancelled.", title_case(self.target.noun())),
                OutcomeClass::Success,
            ),
            CancelDeletionOutcome::Common(common) => {
                let failure = common_lifecycle_failure(
                    self.target,
                    deployment,
                    common,
                    FailureOperation::Cancellation,
                );
                (
                    failure.outcome,
                    None,
                    failure.category,
                    failure.human,
                    failure.class,
                )
            }
            CancelDeletionOutcome::ReauthenticationRequired => (
                "reauthentication_required",
                None,
                None,
                reauthentication_message(self.target),
                OutcomeClass::Forbidden,
            ),
            CancelDeletionOutcome::NotFound => (
                "not_found",
                None,
                None,
                "error: organization not found or unavailable\n\nCheck the organization reference and your access, then try again.".to_owned(),
                OutcomeClass::GeneralFailure,
            ),
            CancelDeletionOutcome::TransitionUnavailable => (
                "transition_unavailable",
                None,
                None,
                format!(
                    "error: {} deletion cancellation unavailable in the current state\n\nThe deletion may already be canceled or past its deadline. Check current access:\n  {}",
                    self.target.noun(),
                    self.target.status_command()
                ),
                OutcomeClass::GeneralFailure,
            ),
            CancelDeletionOutcome::IdempotencyConflict => (
                "idempotency_conflict",
                None,
                None,
                format!(
                    "error: {} deletion cancellation identity conflicted with another request\n\nStart the command again to create a new request identity and fresh proof.",
                    self.target.noun()
                ),
                OutcomeClass::GeneralFailure,
            ),
        };
        self.write_terminal(
            deployment,
            CancellationTerminal {
                outcome: name,
                phase: None,
                category,
                transition,
                human,
                class,
            },
        )
        .map_err(Into::into)
    }

    fn write_terminal(
        &mut self,
        deployment: &str,
        terminal: CancellationTerminal<'_>,
    ) -> anyhow::Result<ExitCode> {
        if self.json {
            write_json_line(&CancellationResultEvent {
                schema_version: 1,
                event: "result",
                deployment,
                outcome: terminal.outcome,
                organization_ref: self.target.organization_ref(),
                transition: terminal.transition,
                category: terminal.category,
                phase: terminal.phase,
                local_credential: "unchanged",
                proof_credential_stored: false,
            })?;
        } else if let Some(transition) = terminal.transition {
            let mut stdout = io::stdout().lock();
            writeln!(stdout, "\n{}\n", terminal.human)?;
            writeln!(
                stdout,
                "{}: {}",
                resource_key(transition.kind),
                transition.id
            )?;
            writeln!(stdout, "state: {}", lifecycle_state(transition.state))?;
            writeln!(stdout, "updated: {}", transition.updated_at)?;
            writeln!(stdout, "local credential: unchanged")?;
            writeln!(stdout, "cancellation proof credential: not stored")?;
            writeln!(stdout, "deployment: {deployment}")?;
            if matches!(self.target, DeletionTarget::Account) {
                writeln!(
                    stdout,
                    "\nSign in to establish a renewable local session:\n  scherzo-cloud auth login"
                )?;
            }
        } else {
            writeln!(io::stderr().lock(), "{}", terminal.human)?;
        }
        Ok(terminal.class.exit_code())
    }
}

#[derive(Clone, Copy)]
enum FailureOperation {
    Request,
    Cancellation,
}

impl FailureOperation {
    const fn name(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Cancellation => "cancellation",
        }
    }
}

fn common_lifecycle_failure(
    target: &DeletionTarget,
    deployment: &str,
    common: &CommonLifecycleFailure,
    operation: FailureOperation,
) -> FailurePresentation {
    match common {
        CommonLifecycleFailure::Unauthenticated => match operation {
            FailureOperation::Request => unauthenticated_failure(
                target,
                "requires sign-in",
                "Sign in first:\n  scherzo-cloud auth login",
            ),
            FailureOperation::Cancellation => unauthenticated_failure(
                target,
                "cancellation proof was not authenticated",
                "Start the command again and authorize with the same linked identity.",
            ),
        },
        CommonLifecycleFailure::Forbidden => FailurePresentation::new(
            "forbidden",
            match (operation, target) {
                (FailureOperation::Request, DeletionTarget::Account) => "error: account deletion not permitted\n\nResolve active ownership requirements before trying again.".to_owned(),
                (FailureOperation::Request, DeletionTarget::Organization(_)) => "error: organization deletion not permitted\n\nUse a current active human owner account.".to_owned(),
                (FailureOperation::Cancellation, _) => format!(
                    "error: {} deletion cancellation not permitted\n\nUse the required linked identity and active owner account.",
                    target.noun()
                ),
            },
            OutcomeClass::Forbidden,
        ),
        CommonLifecycleFailure::InvalidInput => {
            invalid_input_failure(target, deployment, operation.name())
        }
        CommonLifecycleFailure::InvalidResponse => FailurePresentation::new(
            "invalid_response",
            match operation {
                FailureOperation::Request => format!(
                    "error: {} deletion request response from {deployment} does not match the public contract\n\nDo not issue another deletion request until you confirm the lifecycle state with the deployment operator.",
                    target.noun()
                ),
                FailureOperation::Cancellation => format!(
                    "error: {} deletion cancellation response from {deployment} does not match the public contract\n\nCheck current access before starting another cancellation:\n  {}",
                    target.noun(),
                    target.status_command()
                ),
            },
            OutcomeClass::Protocol,
        ),
        CommonLifecycleFailure::Unreachable(category) => unreachable_failure(
            target,
            *category,
            operation.name(),
            match operation {
                FailureOperation::Request => "Do not issue another deletion request until you confirm the lifecycle state with the deployment operator.".to_owned(),
                FailureOperation::Cancellation => format!(
                    "Check current access before starting another cancellation:\n  {}",
                    target.status_command()
                ),
            },
        ),
    }
}

fn unauthenticated_failure(
    target: &DeletionTarget,
    diagnostic: &str,
    remedy: &str,
) -> FailurePresentation {
    FailurePresentation::new(
        "unauthenticated",
        format!("error: {} deletion {diagnostic}\n\n{remedy}", target.noun()),
        OutcomeClass::Unauthenticated,
    )
}

fn invalid_input_failure(
    target: &DeletionTarget,
    deployment: &str,
    operation: &str,
) -> FailurePresentation {
    FailurePresentation::new(
        "invalid_input",
        format!(
            "error: {} deletion {operation} rejected by {deployment}\n\nCheck the target and use a CLI version supported by this deployment.",
            target.noun()
        ),
        OutcomeClass::GeneralFailure,
    )
}

fn unreachable_failure(
    target: &DeletionTarget,
    category: UnreachableCategory,
    operation: &str,
    remedy: String,
) -> FailurePresentation {
    FailurePresentation::with_category(
        "unreachable",
        format!(
            "error: {} deletion {operation} unconfirmed: {}\n\n{remedy}",
            target.noun(),
            category.as_str()
        ),
        super::unreachable_outcome_class(category),
        category.as_str(),
    )
}

fn reauthentication_message(target: &DeletionTarget) -> String {
    match target {
        DeletionTarget::Account => "error: account deletion cancellation requires a different later bearer from the same linked identity\n\nStart the command again and choose the same linked identity used to request deletion.".to_owned(),
        DeletionTarget::Organization(_) => "error: organization deletion cancellation proof rejected\n\nStart the command again as a current active owner using the same linked identity that requested deletion.".to_owned(),
    }
}

struct FailurePresentation {
    outcome: &'static str,
    category: Option<&'static str>,
    human: String,
    class: OutcomeClass,
}

impl FailurePresentation {
    const fn new(outcome: &'static str, human: String, class: OutcomeClass) -> Self {
        Self {
            outcome,
            category: None,
            human,
            class,
        }
    }

    const fn with_category(
        outcome: &'static str,
        human: String,
        class: OutcomeClass,
        category: &'static str,
    ) -> Self {
        Self {
            outcome,
            category: Some(category),
            human,
            class,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeletionResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization_ref: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schedule: Option<&'a DeletionSchedule>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_credential: Option<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancellationResultEvent<'a> {
    schema_version: u8,
    event: &'static str,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization_ref: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transition: Option<&'a LifecycleTransition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<&'static str>,
    local_credential: &'static str,
    proof_credential_stored: bool,
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    super::write_pretty_json(value).context("write JSON deletion result")
}

fn write_json_line(value: &impl Serialize) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value).context("serialize JSON deletion event")?;
    writeln!(stdout).context("write JSON deletion event")?;
    stdout.flush().context("write JSON deletion event")
}

pub(super) const fn account_about() -> &'static str {
    ACCOUNT_ABOUT
}

pub(super) const fn organization_about() -> &'static str {
    ORGANIZATION_ABOUT
}
