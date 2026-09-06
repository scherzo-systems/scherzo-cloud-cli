use std::io::{self, Write};

use anyhow::{Context, anyhow};
use serde::Serialize;

use crate::api::{
    OrganizationMembershipList, Project, ProjectFailure,
    ProjectGitHubInstallation as GitHubInstallation,
    ProjectGitHubInstallationList as GitHubInstallationList,
    ProjectGitHubRepository as GitHubRepository,
    ProjectGitHubRepositoryList as GitHubRepositoryList, ProjectList, ProjectReadinessBlocker,
    ProjectRepository,
};
use crate::exit_code::{ExitCode, OutcomeClass};

pub(super) fn write_project(
    deployment: &str,
    result: Result<Project, ProjectFailure>,
    outcome: &'static str,
    heading: &'static str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(project) => {
            if json {
                write_json(&ProjectResult {
                    schema_version: 1,
                    deployment,
                    outcome,
                    project: &project,
                })?;
            } else {
                write_project_human(deployment, heading, &project)?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(deployment, failure, json),
    }
}

pub(super) fn write_project_list(
    deployment: &str,
    result: Result<ProjectList, ProjectFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(page) => {
            if json {
                write_paginated_list_json(deployment, &page.items, page.next_cursor.as_deref())?;
            } else {
                let mut stdout = io::stdout().lock();
                writeln!(stdout, "✓ Projects listed.\n")?;
                for project in &page.items {
                    writeln!(
                        stdout,
                        "project: {} · name: {} · readiness: {}",
                        project.id,
                        project.name,
                        readiness(project)
                    )?;
                    if !project.execution_readiness.blockers.is_empty() {
                        writeln!(
                            stdout,
                            "  blockers: {}",
                            blocker_list(&project.execution_readiness.blockers)
                        )?;
                    }
                }
                write_list_footer(
                    &mut stdout,
                    !page.items.is_empty(),
                    page.next_cursor.as_deref(),
                    deployment,
                )?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(deployment, failure, json),
    }
}

pub(super) fn write_repository(
    deployment: &str,
    result: Result<ProjectRepository, ProjectFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(repository) => {
            if json {
                write_json(&RepositoryResult {
                    schema_version: 1,
                    deployment,
                    outcome: "found",
                    repository: &repository,
                })?;
            } else {
                let mut stdout = io::stdout().lock();
                writeln!(stdout, "✓ Project repository found.\n")?;
                writeln!(
                    stdout,
                    "repository connection: {}",
                    repository.connection_id
                )?;
                writeln!(
                    stdout,
                    "installation: {}",
                    repository.installation_binding_id
                )?;
                writeln!(stdout, "repository: {}", repository.full_name)?;
                writeln!(
                    stdout,
                    "provider repository: {}",
                    repository.provider_repository_id
                )?;
                writeln!(stdout, "default branch: {}", repository.default_branch)?;
                writeln!(
                    stdout,
                    "availability: {}",
                    enum_text(&repository.availability)?
                )?;
                writeln!(stdout, "deployment: {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(deployment, failure, json),
    }
}

pub(super) fn write_installations(
    deployment: &str,
    result: Result<GitHubInstallationList, ProjectFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(list) => {
            if json {
                write_json(&InstallationListResult {
                    schema_version: 1,
                    deployment,
                    outcome: "listed",
                    items: &list.items,
                })?;
            } else {
                let mut stdout = io::stdout().lock();
                writeln!(stdout, "✓ GitHub installations listed.\n")?;
                for installation in &list.items {
                    writeln!(
                        stdout,
                        "installation: {} · account: {} · type: {} · state: {}",
                        installation.id,
                        installation.provider_account_id,
                        enum_text(&installation.provider_account_type)?,
                        enum_text(&installation.state)?
                    )?;
                }
                write_list_footer(&mut stdout, !list.items.is_empty(), None, deployment)?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(deployment, failure, json),
    }
}

pub(super) fn write_repositories(
    deployment: &str,
    result: Result<GitHubRepositoryList, ProjectFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(list) => {
            if json {
                write_json(&RepositoryListResult {
                    schema_version: 1,
                    deployment,
                    outcome: "listed",
                    installation: &list.installation,
                    items: &list.items,
                })?;
            } else {
                let mut stdout = io::stdout().lock();
                writeln!(stdout, "✓ GitHub repositories listed.\n")?;
                writeln!(stdout, "installation: {}\n", list.installation.id)?;
                for repository in &list.items {
                    writeln!(
                        stdout,
                        "repository: {} · provider repository: {} · default branch: {}",
                        repository.full_name,
                        repository.provider_repository_id,
                        repository.default_branch
                    )?;
                }
                // Repository discovery has an installation-bearing envelope distinct from
                // installation discovery even though both reports share a list footer.
                // jscpd:ignore-start
                write_list_footer(&mut stdout, !list.items.is_empty(), None, deployment)?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(deployment, failure, json),
    }
    // jscpd:ignore-end
}

// Organization membership discovery intentionally renders membership lifecycle fields;
// project listing renders readiness, so their similar pagination shells remain separate.
// jscpd:ignore-start
pub(in crate::cli) fn write_organization_list(
    deployment: &str,
    result: Result<OrganizationMembershipList, ProjectFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(page) => {
            if json {
                write_paginated_list_json(deployment, &page.items, page.next_cursor.as_deref())?;
            } else {
                let mut stdout = io::stdout().lock();
                writeln!(stdout, "✓ Organization memberships listed.\n")?;
                for membership in &page.items {
                    writeln!(
                        stdout,
                        "organization: {} · slug: {} · role: {} · membership: {}",
                        membership.organization_id,
                        membership
                            .organization_slug
                            .as_deref()
                            .unwrap_or("unavailable"),
                        enum_text(&membership.role)?,
                        enum_text(&membership.state)?
                    )?;
                    if let Some(display_name) = &membership.organization_display_name {
                        writeln!(stdout, "  name: {display_name}")?;
                    }
                }
                write_list_footer(
                    &mut stdout,
                    !page.items.is_empty(),
                    page.next_cursor.as_deref(),
                    deployment,
                )?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(deployment, failure, json),
    }
}
// jscpd:ignore-end

fn write_paginated_list_json(
    deployment: &str,
    items: &[impl Serialize],
    next_cursor: Option<&str>,
) -> anyhow::Result<()> {
    write_json(&PaginatedListResult {
        schema_version: 1,
        deployment,
        outcome: "listed",
        items,
        next_cursor,
    })
}

fn write_list_footer(
    output: &mut impl Write,
    has_items: bool,
    next_cursor: Option<&str>,
    deployment: &str,
) -> io::Result<()> {
    if has_items {
        writeln!(output)?;
    }
    if let Some(cursor) = next_cursor {
        writeln!(output, "next cursor: {cursor}")?;
    }
    writeln!(output, "deployment: {deployment}")
}

fn write_project_human(deployment: &str, heading: &str, project: &Project) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{heading}\n")?;
    writeln!(stdout, "project: {}", project.id)?;
    writeln!(stdout, "name: {}", project.name)?;
    writeln!(stdout, "organization: {}", project.organization_id)?;
    match project.runner_pool.as_deref() {
        Some(pool) => writeln!(stdout, "runner pool: {} ({})", pool.name, pool.id)?,
        None => writeln!(stdout, "runner pool: none")?,
    }
    match project.repository.as_deref() {
        Some(repository) => {
            writeln!(stdout, "repository: {}", repository.full_name)?;
            writeln!(
                stdout,
                "repository connection: {}",
                repository.connection_id
            )?;
            writeln!(stdout, "default branch: {}", repository.default_branch)?;
            writeln!(
                stdout,
                "repository availability: {}",
                enum_text(&repository.availability)?
            )?;
        }
        None => writeln!(stdout, "repository: none")?,
    }
    writeln!(stdout, "readiness: {}", readiness(project))?;
    writeln!(
        stdout,
        "blockers: {}",
        if project.execution_readiness.blockers.is_empty() {
            "none".to_owned()
        } else {
            blocker_list(&project.execution_readiness.blockers)
        }
    )?;
    writeln!(stdout, "created: {}", project.created_at)?;
    writeln!(stdout, "updated: {}", project.updated_at)?;
    writeln!(stdout, "deployment: {deployment}")?;
    Ok(())
}

fn readiness(project: &Project) -> &'static str {
    if project.execution_readiness.ready {
        "ready"
    } else {
        "blocked"
    }
}

fn blocker_list(blockers: &[ProjectReadinessBlocker]) -> String {
    blockers
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn enum_text(value: &impl Serialize) -> anyhow::Result<String> {
    match serde_json::to_value(value).context("serialize project field")? {
        serde_json::Value::String(value) => Ok(value),
        _ => Err(anyhow!("project field is not a contracted string")),
    }
}

fn write_failure(
    deployment: &str,
    failure: ProjectFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (outcome, category, human, class) = match failure {
        ProjectFailure::Unauthenticated => (
            "unauthenticated",
            None,
            "error: Scherzo Cloud access requires sign-in\n\nSign in first:\n  scherzo-cloud auth login".to_owned(),
            OutcomeClass::Unauthenticated,
        ),
        ProjectFailure::Forbidden => (
            "forbidden",
            None,
            "error: Scherzo Cloud operation is not permitted for this account\n\nAsk an organization owner to perform this operation.".to_owned(),
            OutcomeClass::Forbidden,
        ),
        ProjectFailure::InvalidInput => (
            "invalid_input",
            None,
            format!("error: request input rejected by {deployment}\n\nCheck the organization, project, repository, pool, and option values, then try again."),
            OutcomeClass::GeneralFailure,
        ),
        ProjectFailure::NotFound => (
            "not_found",
            None,
            "error: Scherzo Cloud resource not found or unavailable\n\nCheck the organization and resource identifier, then try again.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        ProjectFailure::RepositoryNotBound => (
            "repository_not_bound",
            None,
            "error: project has no repository binding\n\nBind a repository first:\n  scherzo-cloud project repository set <ORGANIZATION> <PROJECT> --installation-id <INSTALLATION> --repository-id <REPOSITORY>".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        ProjectFailure::NameUnavailable => (
            "name_unavailable",
            None,
            "error: project name unavailable\n\nChoose another name and try again.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        ProjectFailure::QuantityLimitReached => (
            "quantity_limit_reached",
            None,
            "error: project quantity limit reached\n\nAsk the deployment operator to raise the limit.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        ProjectFailure::RateLimited { retry_after } => (
            "rate_limited",
            None,
            format!(
                "error: project creation rate limited\n\nTry again in {retry_after} seconds."
            ),
            OutcomeClass::RateLimited,
        ),
        ProjectFailure::IdempotencyConflict => (
            "idempotency_conflict",
            None,
            "error: project request identity conflicted with another request\n\nRun the command again to use a new request identity.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        ProjectFailure::SourceConflict => (
            "source_conflict",
            None,
            "error: selected GitHub repository or branch is unavailable\n\nList the installation repositories and choose an available repository and branch.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        ProjectFailure::Conflict => (
            "conflict",
            None,
            "error: project request conflicts with current state\n\nShow the project and try again with its current configuration.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        ProjectFailure::Unreachable(category) => (
            "unreachable",
            Some(category.as_str()),
            format!(
                "error: contact Scherzo Cloud API at {deployment}: {}\n\nCheck network access to the deployment and try again.",
                category.as_str()
            ),
            super::super::unreachable_outcome_class(category),
        ),
        ProjectFailure::Protocol { .. } => (
            "invalid_response",
            None,
            "error: Scherzo Cloud API response does not match the public contract\n\nTry again later.".to_owned(),
            OutcomeClass::Protocol,
        ),
    };
    let retry_after = match failure {
        ProjectFailure::RateLimited { retry_after } => Some(retry_after),
        _ => None,
    };
    if json {
        write_json(&super::super::CloudFailureResult {
            schema_version: 1,
            deployment,
            outcome,
            category,
            retry_after,
        })?;
    } else {
        writeln!(io::stderr().lock(), "{human}")?;
    }
    Ok(class.exit_code())
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::write_pretty_json(value).context("write JSON project result")
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProjectResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    project: &'a Project,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PaginatedListResult<'a, T> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    items: &'a [T],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RepositoryResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    repository: &'a ProjectRepository,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InstallationListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    items: &'a [GitHubInstallation],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RepositoryListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    installation: &'a GitHubInstallation,
    items: &'a [GitHubRepository],
}
