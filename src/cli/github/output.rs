use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use crate::api::{
    GitHubAccountType, GitHubFailure, GitHubInstallation, GitHubInstallationState,
    GitHubRepository, GitHubRepositoryList, GitHubSetupSession,
};
use crate::exit_code::{ExitCode, OutcomeClass};

#[derive(Clone, Copy)]
pub(super) enum InstallationAction {
    SetupCompleted,
    Disconnected,
}

#[derive(Clone, Copy)]
enum FailureAction {
    BeginSetup,
    CompleteSetup,
    ListInstallations,
    DisconnectInstallation,
    ListRepositories,
}

pub(super) fn write_setup_begin(
    deployment: &str,
    organization: &str,
    result: &Result<GitHubSetupSession, GitHubFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(session) => {
            if json {
                write_json(&SetupResult {
                    schema_version: 1,
                    deployment,
                    organization_ref: organization,
                    outcome: "pending",
                    session,
                })?;
            } else {
                let mut output = io::stdout().lock();
                writeln!(output, "✓ GitHub setup started.\n")?;
                writeln!(output, "  Organization: {organization}")?;
                writeln!(output, "  Setup session: {}", session.id)?;
                writeln!(output, "  Expires:       {}", session.expires_at)?;
                writeln!(output, "  Deployment:    {deployment}\n")?;
                writeln!(
                    output,
                    "Open this URL in a browser and approve the repositories:\n  {}",
                    session.setup_url
                )?;
                writeln!(
                    output,
                    "\nThen copy the decimal installation ID from the browser return URL and run:\n  scherzo-cloud github setup complete {organization} {} --provider-installation-id <INSTALLATION_ID>",
                    session.id
                )?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(
            deployment,
            organization,
            failure,
            FailureAction::BeginSetup,
            json,
        ),
    }
}

pub(super) fn write_installation_list(
    deployment: &str,
    organization: &str,
    result: &Result<Vec<GitHubInstallation>, GitHubFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(installations) => {
            if json {
                write_json(&InstallationListResult {
                    schema_version: 1,
                    deployment,
                    organization_ref: organization,
                    outcome: "listed",
                    items: installations,
                })?;
            } else {
                let mut output = io::stdout().lock();
                writeln!(output, "✓ GitHub installations listed.\n")?;
                for installation in installations {
                    writeln!(
                        output,
                        "  Installation: {}  State: {}  Account: {} {}  Provider installation: {}",
                        installation.id,
                        installation_state(installation.state),
                        account_type(installation.provider_account_type),
                        installation.provider_account_id,
                        installation.provider_installation_id,
                    )?;
                }
                if !installations.is_empty() {
                    writeln!(output)?;
                }
                writeln!(output, "  Organization: {organization}")?;
                writeln!(output, "  Deployment:   {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(
            deployment,
            organization,
            failure,
            FailureAction::ListInstallations,
            json,
        ),
    }
}

pub(super) fn write_installation(
    deployment: &str,
    organization: &str,
    result: &Result<GitHubInstallation, GitHubFailure>,
    action: InstallationAction,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(installation) => {
            let (outcome, heading) = match action {
                InstallationAction::SetupCompleted => ("completed", "✓ GitHub setup completed."),
                InstallationAction::Disconnected => {
                    ("disconnected", "✓ GitHub installation disconnected.")
                }
            };
            if json {
                write_json(&InstallationResult {
                    schema_version: 1,
                    deployment,
                    organization_ref: organization,
                    outcome,
                    installation,
                })?;
            } else {
                let mut output = io::stdout().lock();
                writeln!(output, "{heading}\n")?;
                write_installation_fields(&mut output, installation)?;
                writeln!(output, "  Organization:          {organization}")?;
                writeln!(output, "  Deployment:            {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(
            deployment,
            organization,
            failure,
            match action {
                InstallationAction::SetupCompleted => FailureAction::CompleteSetup,
                InstallationAction::Disconnected => FailureAction::DisconnectInstallation,
            },
            json,
        ),
    }
}

pub(super) fn write_repository_list(
    deployment: &str,
    organization: &str,
    result: &Result<GitHubRepositoryList, GitHubFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(page) => {
            if json {
                write_json(&RepositoryListResult {
                    schema_version: 1,
                    deployment,
                    organization_ref: organization,
                    outcome: "listed",
                    installation: &page.installation,
                    items: &page.items,
                })?;
            } else {
                write_repositories_human(deployment, organization, page)?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(
            deployment,
            organization,
            failure,
            FailureAction::ListRepositories,
            json,
        ),
    }
}

fn write_installation_fields(
    output: &mut impl Write,
    installation: &GitHubInstallation,
) -> anyhow::Result<()> {
    writeln!(output, "  Installation:          {}", installation.id)?;
    writeln!(
        output,
        "  Provider installation: {}",
        installation.provider_installation_id
    )?;
    writeln!(
        output,
        "  Provider account:      {} ({})",
        installation.provider_account_id,
        account_type(installation.provider_account_type)
    )?;
    writeln!(
        output,
        "  State:                 {}",
        installation_state(installation.state)
    )?;
    writeln!(
        output,
        "  Created:               {}",
        installation.created_at
    )?;
    writeln!(
        output,
        "  Updated:               {}",
        installation.updated_at
    )?;
    Ok(())
}

fn write_repositories_human(
    deployment: &str,
    organization: &str,
    page: &GitHubRepositoryList,
) -> anyhow::Result<()> {
    let mut output = io::stdout().lock();
    writeln!(output, "✓ GitHub repositories listed.\n")?;
    for repository in &page.items {
        write_repository_row(&mut output, repository)?;
    }
    if !page.items.is_empty() {
        writeln!(output)?;
    }
    writeln!(output, "  Installation: {}", page.installation.id)?;
    writeln!(output, "  Organization: {organization}")?;
    writeln!(output, "  Deployment:   {deployment}")?;
    Ok(())
}

fn write_repository_row(
    output: &mut impl Write,
    repository: &GitHubRepository,
) -> anyhow::Result<()> {
    writeln!(
        output,
        "  Repository: {}  Provider repository: {}  Default branch: {}",
        repository.full_name, repository.provider_repository_id, repository.default_branch
    )?;
    Ok(())
}

fn write_failure(
    deployment: &str,
    organization: &str,
    failure: &GitHubFailure,
    action: FailureAction,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (outcome, category, human, class) = match failure {
        GitHubFailure::Unauthenticated => (
            "unauthenticated",
            None,
            "error: GitHub connection management requires sign-in\n\nSign in first:\n  scherzo-cloud auth login".to_owned(),
            OutcomeClass::Unauthenticated,
        ),
        GitHubFailure::Forbidden => (
            "forbidden",
            None,
            "error: GitHub connection operation is not permitted for this account\n\nAsk an active organization owner to perform this operation.".to_owned(),
            OutcomeClass::Forbidden,
        ),
        GitHubFailure::InvalidInput => (
            "invalid_input",
            None,
            format!(
                "error: GitHub connection input rejected by {deployment}\n\n{}",
                action.invalid_input_remedy()
            ),
            OutcomeClass::GeneralFailure,
        ),
        GitHubFailure::NotFound => (
            "not_found",
            None,
            "error: GitHub connection not found or unavailable\n\nCheck the organization and connection identifiers, then try again.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        GitHubFailure::SourceConnectionConflict => (
            "source_connection_conflict",
            None,
            action.conflict_message().to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        GitHubFailure::Unreachable(category) => (
            "unreachable",
            Some(category.as_str()),
            format!(
                "error: contact GitHub connection API at {deployment}: {}\n\nCheck network access to the deployment and try again.",
                category.as_str()
            ),
            super::super::unreachable_outcome_class(*category),
        ),
        GitHubFailure::Protocol { .. } => (
            "invalid_response",
            None,
            "error: GitHub connection API response does not match the public contract\n\nTry again later.".to_owned(),
            OutcomeClass::Protocol,
        ),
    };
    if json {
        write_json(&FailureResult {
            schema_version: 1,
            deployment,
            organization_ref: organization,
            outcome,
            category,
        })?;
    } else {
        writeln!(io::stderr().lock(), "{human}")?;
    }
    Ok(class.exit_code())
}

impl FailureAction {
    const fn invalid_input_remedy(self) -> &'static str {
        match self {
            Self::BeginSetup | Self::ListInstallations => {
                "Check the organization reference, then try again."
            }
            Self::CompleteSetup => {
                "Check the organization, setup session, and decimal installation ID, then try again."
            }
            Self::DisconnectInstallation | Self::ListRepositories => {
                "Check the organization and installation binding ID, then try again."
            }
        }
    }

    const fn conflict_message(self) -> &'static str {
        match self {
            Self::CompleteSetup => {
                "error: GitHub setup conflicts with the current source connection\n\nCheck that the setup session is still pending and that the GitHub installation belongs to this organization. If the earlier session expired, begin a new one:\n  scherzo-cloud github setup begin <ORGANIZATION>"
            }
            Self::DisconnectInstallation => {
                "error: GitHub installation is unavailable for disconnection\n\nList installation bindings. Reconnect a disconnected binding through browser setup; a revoked installation requires a new GitHub installation."
            }
            Self::ListRepositories => {
                "error: GitHub installation is unavailable for repository discovery\n\nList installation bindings. Reconnect a disconnected binding through browser setup; a revoked installation requires a new GitHub installation."
            }
            Self::BeginSetup | Self::ListInstallations => {
                "error: GitHub connection state conflicts with this operation\n\nList installation bindings and begin a new setup session if needed."
            }
        }
    }
}

const fn account_type(account_type: GitHubAccountType) -> &'static str {
    match account_type {
        GitHubAccountType::Organization => "organization",
        GitHubAccountType::User => "user",
    }
}

const fn installation_state(state: GitHubInstallationState) -> &'static str {
    match state {
        GitHubInstallationState::Active => "active",
        GitHubInstallationState::Disconnected => "disconnected",
        GitHubInstallationState::Revoked => "revoked",
    }
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer_pretty(&mut output, value)
        .context("serialize JSON GitHub connection result")?;
    writeln!(output).context("write GitHub connection result")
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SetupResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    organization_ref: &'a str,
    outcome: &'static str,
    session: &'a GitHubSetupSession,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InstallationListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    organization_ref: &'a str,
    outcome: &'static str,
    items: &'a [GitHubInstallation],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InstallationResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    organization_ref: &'a str,
    outcome: &'static str,
    installation: &'a GitHubInstallation,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RepositoryListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    organization_ref: &'a str,
    outcome: &'static str,
    installation: &'a GitHubInstallation,
    items: &'a [GitHubRepository],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FailureResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    organization_ref: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'a str>,
}
