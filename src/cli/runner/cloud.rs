use std::io::{self, Write};

use anyhow::{Context, anyhow};
use serde::Serialize;

use crate::api::{
    HttpTransportPolicy, RunnerApi, RunnerFailure, RunnerPool, RunnerPoolList, RunnerRegistration,
    RunnerRegistrationList,
};
use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::deployment::Deployment;
use crate::human_auth::session::{self, RequiredOperation};

pub(super) fn with_api<T>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    operation: impl FnMut(&RunnerApi) -> Result<T, RunnerFailure>,
) -> anyhow::Result<Result<T, RunnerFailure>> {
    with_api_retrying_rejected_result(deployment, transport_policy, operation, |_| false)
}

pub(super) fn with_api_retrying_rejected_result<T>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    mut operation: impl FnMut(&RunnerApi) -> Result<T, RunnerFailure>,
    result_credential_rejected: impl Fn(&T) -> bool,
) -> anyhow::Result<Result<T, RunnerFailure>> {
    let client = crate::api::HttpClient::new(transport_policy)
        .map_err(|error| anyhow!(error))
        .context("prepare human session networking")?;
    match session::execute_required(
        &client,
        deployment,
        |access_token| {
            let api = RunnerApi::new(
                deployment.fingerprint().api_url(),
                access_token.expose(),
                transport_policy,
            )
            .map_err(|error| anyhow!(error))
            .context("prepare runner administration networking")?;
            Ok(operation(&api))
        },
        |result| {
            result.as_ref().is_ok_and(|operation| {
                operation
                    .as_ref()
                    .is_err_and(RunnerFailure::credential_rejected)
                    || operation.as_ref().is_ok_and(&result_credential_rejected)
            })
        },
    ) {
        Ok(RequiredOperation::Unauthenticated) => Ok(Err(RunnerFailure::Unauthenticated)),
        Ok(RequiredOperation::Completed(result)) => result,
        Err(error) => match error.unreachable_category() {
            Some(category) => Ok(Err(RunnerFailure::Unreachable(category))),
            None => Err(anyhow!(error).context("acquire human session")),
        },
    }
}

pub(super) fn write_pool_create(
    deployment: &str,
    result: &Result<RunnerPool, RunnerFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_pool(
        deployment,
        result,
        "created",
        "✓ Runner pool created.",
        json,
    )
}

pub(super) fn write_pool_show(
    deployment: &str,
    result: &Result<RunnerPool, RunnerFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_pool(deployment, result, "found", "✓ Runner pool found.", json)
}

// Each command wrapper binds one stable machine outcome to its human verdict;
// spelling out that binding is clearer than a second layer of callback indirection.
// jscpd:ignore-start
pub(super) fn write_pool_rename(
    deployment: &str,
    result: &Result<RunnerPool, RunnerFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_pool(
        deployment,
        result,
        "renamed",
        "✓ Runner pool renamed.",
        json,
    )
}
// jscpd:ignore-end

fn write_pool(
    deployment: &str,
    result: &Result<RunnerPool, RunnerFailure>,
    outcome: &'static str,
    heading: &'static str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(pool) => {
            if json {
                write_json(&PoolResult {
                    schema_version: 1,
                    deployment,
                    outcome,
                    pool,
                })?;
            } else {
                let stdout = io::stdout();
                let mut stdout = stdout.lock();
                writeln!(stdout, "{heading}\n")?;
                writeln!(stdout, "  Pool:         {}", pool.id)?;
                writeln!(stdout, "  Name:         {}", pool.name)?;
                writeln!(stdout, "  Organization: {}", pool.organization_id)?;
                writeln!(stdout, "  Deployment:   {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(deployment, failure, json),
    }
}

// Pool pages have a deliberately compact presentation and a pool-specific JSON
// envelope; sharing organization or runner row rendering would couple output contracts.
// jscpd:ignore-start
pub(super) fn write_pool_list(
    deployment: &str,
    result: &Result<RunnerPoolList, RunnerFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(page) => {
            if json {
                write_json(&PoolListResult {
                    schema_version: 1,
                    deployment,
                    outcome: "listed",
                    items: &page.items,
                    next_cursor: page.next_cursor.as_deref(),
                })?;
            } else {
                let stdout = io::stdout();
                let mut stdout = stdout.lock();
                writeln!(stdout, "✓ Runner pools listed.\n")?;
                for pool in &page.items {
                    writeln!(stdout, "  Pool: {}  Name: {}", pool.id, pool.name)?;
                }
                if !page.items.is_empty() {
                    writeln!(stdout)?;
                }
                if let Some(cursor) = &page.next_cursor {
                    writeln!(stdout, "  Next cursor: {cursor}")?;
                }
                writeln!(stdout, "  Deployment: {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(deployment, failure, json),
    }
}
// jscpd:ignore-end

// Registration rows own the independent projection summary and JSON envelope;
// a generic page renderer would hide that product-specific field contract.
// jscpd:ignore-start
pub(super) fn write_runner_list(
    deployment: &str,
    result: &Result<RunnerRegistrationList, RunnerFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(page) => {
            if json {
                write_json(&RunnerListResult {
                    schema_version: 1,
                    deployment,
                    outcome: "listed",
                    items: &page.items,
                    next_cursor: page.next_cursor.as_deref(),
                })?;
            } else {
                let stdout = io::stdout();
                let mut stdout = stdout.lock();
                writeln!(stdout, "✓ Runners listed.\n")?;
                for runner in &page.items {
                    writeln!(
                        stdout,
                        "  Runner: {}  Name: {}  Pool: {}  Mode: {}  Enrollment: {}  Connectivity: {}  Activity: {}",
                        runner.id,
                        runner.name,
                        runner.runner_pool.name,
                        enum_text(&runner.administration.mode)?,
                        enum_text(&runner.enrollment.state)?,
                        enum_text(&runner.connectivity.state)?,
                        enum_text(&runner.activity.state)?,
                    )?;
                }
                if !page.items.is_empty() {
                    writeln!(stdout)?;
                }
                if let Some(cursor) = &page.next_cursor {
                    writeln!(stdout, "  Next cursor: {cursor}")?;
                }
                writeln!(stdout, "  Deployment: {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(deployment, failure, json),
    }
}
// jscpd:ignore-end

pub(super) fn write_runner_show(
    deployment: &str,
    result: &Result<RunnerRegistration, RunnerFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_runner(deployment, result, "found", "✓ Runner found.", json)
}

pub(super) fn write_runner_rename(
    deployment: &str,
    result: &Result<RunnerRegistration, RunnerFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_runner(deployment, result, "renamed", "✓ Runner renamed.", json)
}

pub(super) fn write_runner_transition(
    deployment: &str,
    result: &Result<RunnerRegistration, RunnerFailure>,
    outcome: &'static str,
    heading: &'static str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_runner(deployment, result, outcome, heading, json)
}

fn write_runner(
    deployment: &str,
    result: &Result<RunnerRegistration, RunnerFailure>,
    outcome: &'static str,
    heading: &'static str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(runner) => {
            if json {
                write_json(&RunnerResult {
                    schema_version: 1,
                    deployment,
                    outcome,
                    runner,
                })?;
            } else {
                write_runner_human(deployment, heading, runner)?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_failure(deployment, failure, json),
    }
}

fn write_runner_human(
    deployment: &str,
    heading: &str,
    runner: &RunnerRegistration,
) -> anyhow::Result<()> {
    let stdout = io::stdout();
    write_runner_human_to(&mut stdout.lock(), deployment, heading, runner)
}

fn write_runner_human_to(
    output: &mut impl Write,
    deployment: &str,
    heading: &str,
    runner: &RunnerRegistration,
) -> anyhow::Result<()> {
    writeln!(output, "{heading}\n")?;
    writeln!(output, "  Runner:       {}", runner.id)?;
    writeln!(output, "  Name:         {}", runner.name)?;
    writeln!(output, "  Organization: {}", runner.organization_id)?;
    writeln!(
        output,
        "  Pool:         {} ({})",
        runner.runner_pool.name, runner.runner_pool.id
    )?;
    writeln!(output, "\n  Administration")?;
    writeln!(
        output,
        "    Mode:       {}",
        enum_text(&runner.administration.mode)?
    )?;
    writeln!(
        output,
        "    Created:    {}",
        runner.administration.created_at
    )?;
    writeln!(
        output,
        "    Updated:    {}",
        runner.administration.updated_at
    )?;
    writeln!(output, "\n  Enrollment")?;
    writeln!(
        output,
        "    State:      {}",
        enum_text(&runner.enrollment.state)?
    )?;
    writeln!(
        output,
        "    Credentials: {} valid",
        runner.enrollment.valid_credential_count
    )?;
    if let Some(first_enrolled_at) = &runner.enrollment.first_enrolled_at {
        writeln!(output, "    First:      {first_enrolled_at}")?;
    }
    writeln!(output, "\n  Connectivity")?;
    writeln!(
        output,
        "    State:      {}",
        enum_text(&runner.connectivity.state)?
    )?;
    if let Some(connected_at) = &runner.connectivity.connected_at {
        writeln!(output, "    Connected:  {connected_at}")?;
    }
    if let Some(last_seen_at) = &runner.connectivity.last_seen_at {
        writeln!(output, "    Last seen:  {last_seen_at}")?;
    }
    writeln!(output, "\n  Activity")?;
    writeln!(
        output,
        "    State:      {}",
        enum_text(&runner.activity.state)?
    )?;
    writeln!(
        output,
        "    Assignments: {} current",
        runner.activity.current_assignment_count
    )?;
    writeln!(output, "\n  Advertised metadata (informational)")?;
    if let Some(metadata) = &runner.advertised_metadata {
        writeln!(output, "    Runner version: {}", metadata.runner_version)?;
        writeln!(output, "    Protocol:       {}", metadata.protocol_version)?;
    } else {
        writeln!(output, "    Not reported")?;
    }
    writeln!(output, "\n  Deployment: {deployment}")?;
    Ok(())
}

fn enum_text(value: &impl Serialize) -> anyhow::Result<String> {
    match serde_json::to_value(value).context("serialize runner state")? {
        serde_json::Value::String(value) => Ok(value),
        _ => Err(anyhow!("runner state is not a contracted string")),
    }
}

pub(super) fn write_failure(
    deployment: &str,
    failure: &RunnerFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_failure_with_context(deployment, failure, json, None)
}

pub(super) fn write_activation_failure(
    deployment: &str,
    failure: &RunnerFailure,
    organization: &str,
    runner_id: &str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_failure_with_context(
        deployment,
        failure,
        json,
        Some(CreatedRegistration {
            organization,
            runner_id,
        }),
    )
}

#[derive(Clone, Copy)]
struct CreatedRegistration<'a> {
    organization: &'a str,
    runner_id: &'a str,
}

fn write_failure_with_context(
    deployment: &str,
    failure: &RunnerFailure,
    json: bool,
    created: Option<CreatedRegistration<'_>>,
) -> anyhow::Result<ExitCode> {
    let (outcome, category, human, outcome_class) = match failure {
        RunnerFailure::Unauthenticated => (
            "unauthenticated",
            None,
            "error: runner administration requires sign-in\n\nSign in first:\n  scherzo-cloud auth login".to_owned(),
            OutcomeClass::Unauthenticated,
        ),
        RunnerFailure::Forbidden => (
            "forbidden",
            None,
            "error: runner operation is not permitted for this account\n\nAsk an organization owner to perform this operation.".to_owned(),
            OutcomeClass::Forbidden,
        ),
        RunnerFailure::InvalidInput => (
            "invalid_input",
            None,
            format!("error: runner input rejected by {deployment}\n\nCheck the organization, resource identifier, and name, then try again."),
            OutcomeClass::GeneralFailure,
        ),
        RunnerFailure::NotFound => (
            "not_found",
            None,
            "error: runner resource not found or unavailable\n\nCheck the organization and resource reference, then try again.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunnerFailure::NameUnavailable => (
            "name_unavailable",
            None,
            "error: runner resource name unavailable\n\nChoose another name and try again.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunnerFailure::QuantityLimitReached => (
            "quantity_limit_reached",
            None,
            "error: runner resource quantity limit reached\n\nRemove an unused resource or ask the deployment operator to raise the limit.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunnerFailure::RateLimited => (
            "rate_limited",
            None,
            "error: runner resource creation rate limited\n\nTry again later.".to_owned(),
            OutcomeClass::RateLimited,
        ),
        RunnerFailure::IdempotencyConflict => (
            "idempotency_conflict",
            None,
            "error: runner request identity conflicted with another request\n\nRun the command again to use a new request identity.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunnerFailure::CredentialLimit => (
            "credential_limit_reached",
            None,
            "error: runner credential limit reached\n\nRevoke a credential or wait for retirement before issuing another activation.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunnerFailure::ActivationUnavailable => (
            "activation_unavailable",
            None,
            "error: runner activation is no longer available\n\nList activations and issue a replacement when needed.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunnerFailure::CredentialTransitionUnavailable => (
            "credential_transition_unavailable",
            None,
            "error: runner credential transition is unavailable\n\nList credentials and choose an active or retiring credential.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunnerFailure::PoolMoveUnavailable => (
            "pool_move_unavailable",
            None,
            "error: runner cannot move while it has active work\n\nDrain or disable the runner, then wait for its reservation and assignments to finish.".to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        RunnerFailure::Unreachable(category) => (
            "unreachable",
            Some(category.as_str()),
            format!(
                "error: contact runner API at {deployment}: {}\n\nCheck network access to the deployment and try again.",
                category.as_str()
            ),
            super::super::unreachable_outcome_class(*category),
        ),
        RunnerFailure::Protocol => (
            "invalid_response",
            None,
            "error: runner API response does not match the public contract\n\nTry again later.".to_owned(),
            OutcomeClass::Protocol,
        ),
    };
    // Runner failures own a closed machine vocabulary distinct from organization
    // failures, so the small stream-selection adapter remains domain-local.
    // jscpd:ignore-start
    if json {
        write_json(&FailureResult {
            schema_version: 1,
            deployment,
            outcome,
            category,
            runner_id: created.map(|registration| registration.runner_id),
        })?;
    } else {
        let stderr = io::stderr();
        let mut stderr = stderr.lock();
        writeln!(stderr, "{human}")?;
        if let Some(created) = created {
            writeln!(
                stderr,
                "\nRunner {} was created without an activation. Issue one without creating another runner:\n  scherzo-cloud runner activation create {} {} --activation-file <PATH>",
                created.runner_id, created.organization, created.runner_id
            )?;
        }
    }
    // jscpd:ignore-end
    Ok(outcome_class.exit_code())
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, value).context("serialize JSON runner result")?;
    writeln!(stdout).context("write runner result")
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PoolResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    pool: &'a RunnerPool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PoolListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    items: &'a [RunnerPool],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunnerResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    runner: &'a RunnerRegistration,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunnerListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    items: &'a [RunnerRegistration],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
}

// This schema intentionally excludes organization-only retry metadata and must
// remain an independently reviewable machine contract.
// jscpd:ignore-start
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FailureResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runner_id: Option<&'a str>,
}
// jscpd:ignore-end

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runner_show_keeps_cloud_projections_separate_from_advertised_metadata() {
        let runner: RunnerRegistration = serde_json::from_value(serde_json::json!({
            "id": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
            "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
            "runnerPool": {
                "id": "rpl_01k0z6r1w8f4jy2m7q9v3x5abc",
                "name": "builders"
            },
            "name": "builder-one",
            "administration": {
                "mode": "draining",
                "createdAt": "2026-08-09T12:00:00Z",
                "updatedAt": "2026-08-09T12:01:00Z"
            },
            "enrollment": {
                "state": "credentialed",
                "firstEnrolledAt": "2026-08-09T12:02:00Z",
                "validCredentialCount": 1
            },
            "connectivity": {
                "state": "online",
                "connectedAt": "2026-08-09T12:03:00Z",
                "lastSeenAt": "2026-08-09T12:04:00Z"
            },
            "activity": {"state": "assigned", "currentAssignmentCount": 1},
            "advertisedMetadata": {
                "runnerVersion": "1.2.3",
                "protocolVersion": 1
            }
        }))
        .expect("runner fixture should match the generated API model");

        let mut output = Vec::new();
        write_runner_human_to(
            &mut output,
            "https://api.scherzo.dev",
            "✓ Runner found.",
            &runner,
        )
        .expect("runner presentation should render");
        let output = String::from_utf8(output).expect("runner presentation should be UTF-8");

        for expected in [
            "  Administration\n    Mode:       draining",
            "  Enrollment\n    State:      credentialed",
            "  Connectivity\n    State:      online",
            "  Activity\n    State:      assigned",
            "  Advertised metadata (informational)\n    Runner version: 1.2.3",
        ] {
            assert!(
                output.contains(expected),
                "missing {expected:?} in {output:?}"
            );
        }
        assert!(
            !output.contains("Status:"),
            "runner output must not overload status"
        );
    }
}
