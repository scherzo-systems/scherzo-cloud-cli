// Command modules keep domain imports local; an import facade would obscure API ownership.
// jscpd:ignore-start
use std::io::{self, Write};

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::exit_code::{ExitCode, OutcomeClass};
use scherzo_cloud_api::{
    AcceptDelegationOutcome, CommonDelegationFailure, Delegation, DelegationApiError,
    DelegationPage, DelegationState, DelegationTerminalReason, EndDelegationOutcome,
    GetDelegationOutcome, HttpClient, ListDelegationsOutcome, ProposeDelegationOutcome,
    accept_delegation, end_delegation, get_delegation, list_current_principal_delegations,
    propose_delegation,
};
use scherzo_cloud_human_auth::Deployment;
// jscpd:ignore-end

pub(super) const ABOUT: &str = "Manage Scherzo Cloud delegations";
const NAME: &str = "delegation";
const ERROR_CONTEXT: &str = "configure Scherzo Cloud delegation access";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<DelegationCommand>,
}

#[derive(Debug, Subcommand)]
enum DelegationCommand {
    #[command(about = "Accept a delegation as its service principal")]
    Accept(AcceptCommand),
    #[command(about = "End a delegation")]
    End(EndCommand),
    #[command(about = "List your delegation history")]
    List(ListCommand),
    #[command(about = "Propose delegation to a service principal")]
    Propose(ProposeCommand),
    #[command(about = "Show a delegation")]
    Show(ShowCommand),
}

type DelegationOptions =
    super::CommonArgs<super::DelegationJson, super::PrincipalAuthenticationArgs>;

// Participant-selected reads and mutations share one credential decision, while mutations add a
// non-optional request identity. Keeping both paths here makes that distinction explicit.
// jscpd:ignore-start
impl DelegationOptions {
    fn execute<O>(
        self,
        deployment: &Deployment,
        operation: impl FnMut(&HttpClient, &str, &str) -> Result<O, DelegationApiError>,
        write: impl FnOnce(
            &str,
            &O,
            super::PrincipalAuthenticationKind,
            bool,
        ) -> anyhow::Result<ExitCode>,
    ) -> anyhow::Result<ExitCode>
    where
        O: super::HumanCredentialOutcome<Error = DelegationApiError>,
    {
        let outcome = super::execute_with_principal_credential(
            deployment,
            self.http.transport_policy(),
            &self.authentication,
            "prepare delegation networking",
            "contact delegation API at",
            operation,
        )?;
        write(
            deployment.fingerprint().api_url(),
            &outcome,
            self.authentication.kind(),
            self.json,
        )
        .context("write delegation result")
    }

    fn execute_mutation<O>(
        self,
        deployment: &Deployment,
        mut operation: impl FnMut(&HttpClient, &str, &str, &str) -> Result<O, DelegationApiError>,
        write: impl FnOnce(
            &str,
            &O,
            super::PrincipalAuthenticationKind,
            bool,
        ) -> anyhow::Result<ExitCode>,
    ) -> anyhow::Result<ExitCode>
    where
        O: super::HumanCredentialOutcome<Error = DelegationApiError>,
    {
        let idempotency_key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate delegation mutation request identity")?;
        self.execute(
            deployment,
            |client, api_url, credential| operation(client, api_url, credential, &idempotency_key),
            write,
        )
    }
}
// jscpd:ignore-end

type DelegationOutputOptions =
    super::CommonArgs<super::DelegationJson, super::NoAuthenticationArgs>;
type RequiredDelegationOptions =
    super::CommonArgs<super::DelegationJson, super::RequiredServiceAuthenticationArgs>;

#[derive(Debug, Args)]
struct ListCommand {
    #[command(flatten)]
    pagination: super::PaginationArgs,

    #[command(flatten)]
    options: DelegationOptions,
}

#[derive(Debug, Args)]
struct ProposeCommand {
    #[arg(
        value_name = "SERVICE_PRINCIPAL",
        value_parser = parse_principal_id,
        help = "Service principal ID"
    )]
    service_principal_id: String,

    #[command(flatten)]
    options: DelegationOutputOptions,
}

#[derive(Debug, Args)]
struct DelegationTarget {
    #[arg(
        value_name = "DELEGATION",
        value_parser = parse_delegation_id,
        help = "Delegation ID"
    )]
    delegation_id: String,
}

#[derive(Debug, Args)]
struct ShowCommand {
    #[command(flatten)]
    target: DelegationTarget,

    #[command(flatten)]
    options: DelegationOptions,
}

#[derive(Debug, Args)]
struct AcceptCommand {
    #[command(flatten)]
    target: DelegationTarget,

    #[command(flatten)]
    options: RequiredDelegationOptions,
}

#[derive(Debug, Args)]
struct EndCommand {
    #[command(flatten)]
    target: DelegationTarget,

    #[command(flatten)]
    confirmation: super::ConfirmationArgs,

    #[command(flatten)]
    options: DelegationOptions,
}

// Delegation dispatch remains local so the actor restriction on each leaf is visible beside the
// command spelling instead of hidden in a generic family dispatcher.
// jscpd:ignore-start
impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        super::execute_deployment_command(
            self.command,
            &[NAME],
            ERROR_CONTEXT,
            |command, deployment| match command {
                DelegationCommand::List(command) => command.execute(deployment).map_err(Into::into),
                DelegationCommand::Propose(command) => {
                    command.execute(deployment).map_err(Into::into)
                }
                DelegationCommand::Show(command) => command.execute(deployment).map_err(Into::into),
                DelegationCommand::Accept(command) => {
                    command.execute(deployment).map_err(Into::into)
                }
                DelegationCommand::End(command) => command.execute(deployment).map_err(Into::into),
            },
        )
    }
}
// jscpd:ignore-end

impl ListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        self.options.execute(
            deployment,
            |client, api_url, credential| {
                list_current_principal_delegations(
                    client,
                    api_url,
                    credential,
                    self.pagination.limit,
                    self.pagination.cursor.as_deref(),
                )
            },
            write_list,
        )
    }
}

impl ProposeCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let idempotency_key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate delegation proposal request identity")?;
        let outcome = super::execute_with_human_credential(
            deployment,
            self.options.http.transport_policy(),
            "prepare delegation networking",
            "contact delegation API at",
            |client, api_url, access_token| {
                propose_delegation(
                    client,
                    api_url,
                    access_token,
                    &self.service_principal_id,
                    &idempotency_key,
                )
            },
        )?;
        write_propose(
            deployment.fingerprint().api_url(),
            &outcome,
            super::PrincipalAuthenticationKind::HumanSession,
            self.options.json,
        )
        .context("write delegation proposal result")
    }
}

impl ShowCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let delegation_id = self.target.delegation_id;
        self.options.execute(
            deployment,
            |client, api_url, credential| {
                get_delegation(client, api_url, credential, &delegation_id)
            },
            write_show,
        )
    }
}

// Acceptance deliberately bypasses the human-session adapter and requires one explicit service
// key, so its direct client path must remain separate from human and participant-selected leaves.
// jscpd:ignore-start
impl AcceptCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let api_key = self.options.authentication.api_key()?;
        let idempotency_key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate delegation acceptance request identity")?;
        let client = HttpClient::new(self.options.http.transport_policy())
            .map_err(|error| anyhow!(error))
            .context("prepare delegation networking")?;
        let outcome = accept_delegation(
            &client,
            deployment.fingerprint().api_url(),
            api_key.expose(),
            &self.target.delegation_id,
            &idempotency_key,
        )
        .context("accept delegation")?;
        write_accept(
            deployment.fingerprint().api_url(),
            &outcome,
            super::PrincipalAuthenticationKind::ServiceApiKey,
            self.options.json,
        )
        .context("write delegation acceptance result")
    }
}
// jscpd:ignore-end

// Ending retains participant-selected authentication and a terminal confirmation, so it remains
// separate from create-style human-only mutations with superficially similar closure plumbing.
// jscpd:ignore-start
impl EndCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let delegation_id = self.target.delegation_id;
        self.options.execute_mutation(
            deployment,
            |client, api_url, credential, idempotency_key| {
                end_delegation(client, api_url, credential, &delegation_id, idempotency_key)
            },
            |deployment, outcome, authentication, json| {
                write_end(deployment, &delegation_id, outcome, authentication, json)
            },
        )
    }
}
// jscpd:ignore-end

fn parse_principal_id(value: &str) -> Result<String, String> {
    if scherzo_cloud_support::valid_typed_id(value, "prn_") {
        Ok(value.to_owned())
    } else {
        Err("must be an exact service principal ID".to_owned())
    }
}

fn parse_delegation_id(value: &str) -> Result<String, String> {
    if scherzo_cloud_support::valid_typed_id(value, "dlg_") {
        Ok(value.to_owned())
    } else {
        Err("must be an exact delegation ID".to_owned())
    }
}

macro_rules! impl_delegation_credential_outcome {
    ($($outcome:ty),+ $(,)?) => {
        $(
            impl super::HumanCredentialOutcome for $outcome {
                type Error = DelegationApiError;

                fn unauthenticated() -> Self {
                    Self::Common(CommonDelegationFailure::Unauthenticated)
                }

                fn unreachable(category: scherzo_cloud_api::UnreachableCategory) -> Self {
                    Self::Common(CommonDelegationFailure::Unreachable(category))
                }

                fn is_unauthenticated(&self) -> bool {
                    matches!(self, Self::Common(CommonDelegationFailure::Unauthenticated))
                }

                fn credential_rejected(error: &Self::Error) -> bool {
                    error.credential_rejected()
                }
            }
        )+
    };
}

impl_delegation_credential_outcome!(
    ListDelegationsOutcome,
    GetDelegationOutcome,
    ProposeDelegationOutcome,
    AcceptDelegationOutcome,
    EndDelegationOutcome,
);

fn write_list(
    deployment: &str,
    outcome: &ListDelegationsOutcome,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        ListDelegationsOutcome::Listed(page) => {
            write_page(deployment, page, json)?;
            Ok(ExitCode::Success)
        }
        ListDelegationsOutcome::Common(failure) => {
            write_common(deployment, failure, authentication, json)
        }
    }
}

fn write_propose(
    deployment: &str,
    outcome: &ProposeDelegationOutcome,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        ProposeDelegationOutcome::Proposed(delegation) => {
            write_delegation_result(deployment, "proposed", delegation, json)?;
            Ok(ExitCode::Success)
        }
        ProposeDelegationOutcome::Common(failure) => {
            write_common(deployment, failure, authentication, json)
        }
        ProposeDelegationOutcome::TransitionUnavailable => {
            write_transition_unavailable(deployment, json)
        }
        ProposeDelegationOutcome::IdempotencyConflict => {
            write_idempotency_conflict(deployment, json)
        }
        ProposeDelegationOutcome::RetryableConflict { retry_after } => write_failure(
            deployment,
            "retryable_conflict",
            None,
            Some(*retry_after),
            &format!(
                "error: delegation proposal temporarily conflicted\n\nTry again in {retry_after} seconds."
            ),
            OutcomeClass::Unreachable,
            json,
        ),
    }
}

fn write_show(
    deployment: &str,
    outcome: &GetDelegationOutcome,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        GetDelegationOutcome::Found(delegation) => {
            write_delegation_result(deployment, "shown", delegation, json)?;
            Ok(ExitCode::Success)
        }
        GetDelegationOutcome::Common(failure) => {
            write_common(deployment, failure, authentication, json)
        }
        GetDelegationOutcome::NotFound => write_not_found(deployment, json),
    }
}

fn write_accept(
    deployment: &str,
    outcome: &AcceptDelegationOutcome,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        AcceptDelegationOutcome::Accepted(delegation) => {
            write_delegation_result(deployment, "accepted", delegation, json)?;
            Ok(ExitCode::Success)
        }
        AcceptDelegationOutcome::Common(failure) => {
            write_common(deployment, failure, authentication, json)
        }
        AcceptDelegationOutcome::NotFound => write_not_found(deployment, json),
        AcceptDelegationOutcome::TransitionUnavailable => {
            write_transition_unavailable(deployment, json)
        }
        AcceptDelegationOutcome::IdempotencyConflict => {
            write_idempotency_conflict(deployment, json)
        }
    }
}

fn write_end(
    deployment: &str,
    delegation_id: &str,
    outcome: &EndDelegationOutcome,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match outcome {
        EndDelegationOutcome::Ended => {
            if json {
                super::write_pretty_json(&EndResult {
                    schema_version: 1,
                    deployment,
                    outcome: "ended",
                    delegation_id,
                })?;
            } else {
                writeln!(
                    io::stdout().lock(),
                    "✓ Delegation ended.\n\ndelegation: {delegation_id}\ndeployment: {deployment}"
                )?;
            }
            Ok(ExitCode::Success)
        }
        EndDelegationOutcome::Common(failure) => {
            write_common(deployment, failure, authentication, json)
        }
        EndDelegationOutcome::NotFound => write_not_found(deployment, json),
        EndDelegationOutcome::TransitionUnavailable => {
            write_transition_unavailable(deployment, json)
        }
        EndDelegationOutcome::IdempotencyConflict => write_idempotency_conflict(deployment, json),
    }
}

// Delegation remedies distinguish exact participants and nominated services; retaining this
// mapping beside delegation output is clearer than sharing invitation or membership prose.
// jscpd:ignore-start
fn write_common(
    deployment: &str,
    failure: &CommonDelegationFailure,
    authentication: super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (outcome, category, message, class) = match failure {
        CommonDelegationFailure::InvalidInput => (
            "invalid_input",
            None,
            "error: delegation input rejected\n\nUse exact principal and delegation IDs, plus a cursor returned by the list command."
                .to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        CommonDelegationFailure::Unauthenticated => (
            "unauthenticated",
            None,
            authentication
                .rejected_error(
                    "error: delegation management requires sign-in\n\nSign in first:\n  scherzo-cloud auth login",
                )
                .to_owned(),
            OutcomeClass::Unauthenticated,
        ),
        CommonDelegationFailure::Forbidden => (
            "forbidden",
            None,
            "error: delegation operation not permitted\n\nUse the exact active participant required by this operation and check the nominated service principal."
                .to_owned(),
            OutcomeClass::Forbidden,
        ),
        CommonDelegationFailure::Unreachable(category) => (
            "unreachable",
            Some(category.as_str()),
            format!(
                "error: contact delegation API at {deployment}: {}\n\nCheck network access to the deployment and try again.",
                category.as_str()
            ),
            super::unreachable_outcome_class(*category),
        ),
    };
    write_failure(deployment, outcome, category, None, &message, class, json)
}
// jscpd:ignore-end

// Delegation terminal, hidden-resource, and replay failures have domain-specific stable outcomes
// and remedies, so they remain separate from invitation lifecycle failures.
// jscpd:ignore-start
fn write_not_found(deployment: &str, json: bool) -> anyhow::Result<ExitCode> {
    write_failure(
        deployment,
        "not_found",
        None,
        None,
        "error: delegation not found or unavailable\n\nCheck the exact delegation ID and use one of its participants.",
        OutcomeClass::GeneralFailure,
        json,
    )
}

fn write_transition_unavailable(deployment: &str, json: bool) -> anyhow::Result<ExitCode> {
    write_failure(
        deployment,
        "delegation_transition_unavailable",
        None,
        None,
        "error: delegation transition unavailable\n\nThe relationship may already be ended or may not be in the state required by this command. List delegation history before choosing another action.",
        OutcomeClass::GeneralFailure,
        json,
    )
}

fn write_idempotency_conflict(deployment: &str, json: bool) -> anyhow::Result<ExitCode> {
    write_failure(
        deployment,
        "idempotency_conflict",
        None,
        None,
        "error: delegation request identity conflicted with another request\n\nRun the command again to use a new request identity.",
        OutcomeClass::GeneralFailure,
        json,
    )
}

fn write_failure(
    deployment: &str,
    outcome: &'static str,
    category: Option<&'static str>,
    retry_after: Option<u64>,
    human: &str,
    class: OutcomeClass,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        super::write_cloud_failure_json(deployment, outcome, category, retry_after)
            .context("write JSON delegation failure")?;
    } else {
        writeln!(io::stderr().lock(), "{human}")?;
    }
    Ok(class.exit_code())
}
// jscpd:ignore-end

fn write_page(deployment: &str, page: &DelegationPage, json: bool) -> anyhow::Result<()> {
    if json {
        super::write_cloud_list_json(deployment, &page.items, page.next_cursor.as_deref())
            .context("write JSON delegation list")
    } else {
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "✓ Delegation history listed.\n")?;
        for delegation in &page.items {
            write_delegation_fields(&mut stdout, delegation)?;
            writeln!(stdout)?;
        }
        super::write_page_footer(&mut stdout, deployment, page.next_cursor.as_deref())?;
        Ok(())
    }
}

fn write_delegation_result(
    deployment: &str,
    outcome: &'static str,
    delegation: &Delegation,
    json: bool,
) -> anyhow::Result<()> {
    if json {
        super::write_pretty_json(&DelegationResult {
            schema_version: 1,
            deployment,
            outcome,
            delegation,
        })
        .context("write JSON delegation result")
    } else {
        let heading = match outcome {
            "proposed" => "✓ Delegation proposed.",
            "accepted" => "✓ Delegation accepted.",
            _ => "✓ Delegation shown.",
        };
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "{heading}\n")?;
        write_delegation_fields(&mut stdout, delegation)?;
        writeln!(stdout, "deployment: {deployment}")?;
        Ok(())
    }
}

fn write_delegation_fields(output: &mut impl Write, delegation: &Delegation) -> io::Result<()> {
    writeln!(
        output,
        "delegation: {} · state: {}",
        delegation.id,
        delegation_state(delegation.state)
    )?;
    writeln!(output, "human principal: {}", delegation.human_principal_id)?;
    writeln!(
        output,
        "service principal: {}",
        delegation.service_principal_id
    )?;
    writeln!(output, "proposed: {}", delegation.proposed_at)?;
    if let Some(accepted_at) = &delegation.accepted_at {
        writeln!(output, "accepted: {accepted_at}")?;
    }
    if let Some(ended_at) = &delegation.ended_at {
        writeln!(output, "ended: {ended_at}")?;
    }
    if let Some(reason) = delegation.terminal_reason {
        writeln!(output, "terminal reason: {}", terminal_reason(reason))?;
    }
    Ok(())
}

const fn delegation_state(state: DelegationState) -> &'static str {
    match state {
        DelegationState::Pending => "pending",
        DelegationState::Active => "active",
        DelegationState::Ended => "ended",
    }
}

const fn terminal_reason(reason: DelegationTerminalReason) -> &'static str {
    match reason {
        DelegationTerminalReason::ParticipantEnded => "participant_ended",
        DelegationTerminalReason::ParticipantDeleted => "participant_deleted",
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DelegationResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    delegation: &'a Delegation,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EndResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    delegation_id: &'a str,
}
